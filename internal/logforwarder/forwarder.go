package logforwarder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
	"vectis/internal/resolver"
)

const (
	defaultBatchSize       = 100
	defaultMaxChunksPerSec = 10000
	defaultScanInterval    = 5 * time.Second
)

// Forwarder receives log chunks from local workers over a Unix socket,
// batches them, and forwards to vectis-log via gRPC.
// When vectis-log is unavailable it writes to a shared spool directory
// for later retry.
type Forwarder struct {
	logClient       interfaces.LogClient
	logger          interfaces.Logger
	chunkCh         <-chan *api.LogChunk
	spoolDir        string
	batchSize       int
	maxChunksPerSec int
	scanInterval    time.Duration
	shutdownCh      chan struct{}
	shutdownOnce    sync.Once
	spoolCounter    uint64
}

// NewForwarder creates a forwarder that reads from the provided chunk channel.
func NewForwarder(
	chunkCh <-chan *api.LogChunk,
	logger interfaces.Logger,
	spoolDir string,
	batchSize int,
	maxChunksPerSec int,
) *Forwarder {
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	if maxChunksPerSec <= 0 {
		maxChunksPerSec = defaultMaxChunksPerSec
	}

	if spoolDir == "" {
		spoolDir = defaultSpoolDir()
	}

	return &Forwarder{
		chunkCh:         chunkCh,
		logger:          logger,
		spoolDir:        spoolDir,
		batchSize:       batchSize,
		maxChunksPerSec: maxChunksPerSec,
		scanInterval:    defaultScanInterval,
		shutdownCh:      make(chan struct{}),
	}
}

// SetLogClient configures the gRPC client used to reach vectis-log.
func (f *Forwarder) SetLogClient(client interfaces.LogClient) {
	f.logClient = client
}

// SetScanInterval configures how often the spool scanner polls the spool
// directory.  It is intended for tests.
func (f *Forwarder) SetScanInterval(d time.Duration) {
	f.scanInterval = d
}

// Run starts the forwarder loop.  It blocks until Shutdown is called.
func (f *Forwarder) Run(ctx context.Context) {
	// Ensure spool directory exists.
	_ = os.MkdirAll(f.spoolDir, 0o755)

	var wg sync.WaitGroup
	wg.Go(func() {
		f.spoolScanner(ctx)
	})

	batch := make([]*api.LogChunk, 0, f.batchSize)
	ticker := time.NewTicker(time.Second / time.Duration(f.maxChunksPerSec))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			batch = f.drainAndFlush(ctx, batch)
			wg.Wait()
			return
		case <-f.shutdownCh:
			batch = f.drainAndFlush(ctx, batch)
			wg.Wait()
			return
		case chunk, ok := <-f.chunkCh:
			if !ok {
				f.flushBatch(ctx, batch)
				wg.Wait()
				return
			}

			batch = append(batch, chunk)
			if len(batch) >= f.batchSize {
				batch = f.flushBatch(ctx, batch)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				batch = f.flushBatch(ctx, batch)
			}
		}
	}
}

// drainAndFlush reads any remaining chunks from the channel and flushes them.
func (f *Forwarder) drainAndFlush(ctx context.Context, batch []*api.LogChunk) []*api.LogChunk {
	for {
		select {
		case chunk, ok := <-f.chunkCh:
			if !ok {
				return f.flushBatch(ctx, batch)
			}

			batch = append(batch, chunk)
			if len(batch) >= f.batchSize {
				batch = f.flushBatch(ctx, batch)
			}
		default:
			return f.flushBatch(ctx, batch)
		}
	}
}

// Shutdown signals the forwarder to stop.
func (f *Forwarder) Shutdown() {
	f.shutdownOnce.Do(func() { close(f.shutdownCh) })
}

func (f *Forwarder) flushBatch(ctx context.Context, batch []*api.LogChunk) []*api.LogChunk {
	if len(batch) == 0 {
		return batch
	}

	if f.logClient == nil {
		if spoolErr := f.spoolBatch(batch); spoolErr != nil {
			if f.logger != nil {
				f.logger.Error("Log batch lost: log client nil and spool failed: %v", spoolErr)
			}
		}

		return batch[:0]
	}

	if err := f.sendBatch(ctx, batch); err != nil {
		if f.logger != nil {
			f.logger.Warn("Failed to forward log batch (%d chunks): %v; spooling", len(batch), err)
		}

		if spoolErr := f.spoolBatch(batch); spoolErr != nil {
			if f.logger != nil {
				f.logger.Error("Log batch lost: forward failed and spool failed: forward=%v spool=%v", err, spoolErr)
			}
		}
	}

	return batch[:0]
}

func (f *Forwarder) sendBatch(parentCtx context.Context, batch []*api.LogChunk) error {
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	stream, err := f.logClient.StreamLogs(ctx)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	defer func() {
		if s, ok := stream.(interface{ CloseAndRecv() error }); ok {
			_ = s.CloseAndRecv()
		} else {
			_ = stream.CloseSend()
		}
	}()

	for _, chunk := range batch {
		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("send chunk: %w", err)
		}
	}

	return nil
}

func (f *Forwarder) spoolBatch(batch []*api.LogChunk) error {
	if len(batch) == 0 {
		return nil
	}

	ts := time.Now().UnixNano()
	seq := atomic.AddUint64(&f.spoolCounter, 1)
	tmpPath := filepath.Join(f.spoolDir, fmt.Sprintf("batch-%d-%d%s", ts, seq, spoolTmpExt))
	finalPath := filepath.Join(f.spoolDir, fmt.Sprintf("batch-%d-%d%s", ts, seq, spoolExt))

	w, err := NewSpoolWriter(tmpPath, len(batch))
	if err != nil {
		if f.logger != nil {
			f.logger.Error("Failed to create spool writer: %v", err)
		}

		return fmt.Errorf("create spool writer: %w", err)
	}

	for _, chunk := range batch {
		if err := w.Append(chunk); err != nil {
			if f.logger != nil {
				f.logger.Warn("Spool append failed: %v", err)
			}
		}
	}

	if err := w.Close(); err != nil {
		if f.logger != nil {
			f.logger.Error("Failed to close spool writer: %v", err)
		}

		os.Remove(tmpPath)
		return fmt.Errorf("close spool writer: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		if f.logger != nil {
			f.logger.Error("Failed to finalize spool file: %v", err)
		}

		os.Remove(tmpPath)
		return fmt.Errorf("rename spool file: %w", err)
	}

	if f.logger != nil {
		f.logger.Info("Spooled %d chunks to %s", len(batch), finalPath)
	}

	return nil
}

func (f *Forwarder) spoolScanner(ctx context.Context) {
	ticker := time.NewTicker(f.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-f.shutdownCh:
			return
		case <-ticker.C:
			if err := f.scanSpool(ctx); err != nil && f.logger != nil {
				f.logger.Debug("Spool scanner error: %v", err)
			}
		}
	}
}

func (f *Forwarder) scanSpool(ctx context.Context) error {
	entries, err := os.ReadDir(f.spoolDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("read spool dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != spoolExt {
			continue
		}

		path := filepath.Join(f.spoolDir, name)
		if err := f.forwardSpoolFile(ctx, path); err != nil {
			if f.logger != nil {
				f.logger.Warn("Failed to forward spool %s: %v", name, err)
			}

			// Quarantine permanently corrupted files so they are not retried forever.
			if isPermanentSpoolError(err) {
				quarantinePath := path + ".quarantine"
				if renameErr := os.Rename(path, quarantinePath); renameErr == nil && f.logger != nil {
					f.logger.Warn("Quarantined corrupted spool file %s", name)
				}
			}

			continue
		}

		if err := os.Remove(path); err != nil {
			if f.logger != nil {
				f.logger.Warn("Failed to remove forwarded spool %s: %v", name, err)
			}
		}
	}

	return nil
}

func isPermanentSpoolError(err error) bool {
	if err == nil {
		return false
	}

	s := err.Error()
	return containsAny(s, []string{"crc mismatch", "unmarshal chunk", "unsupported spool version", "invalid spool magic", "spool batch count", "read magic"})
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}

	return false
}

func (f *Forwarder) forwardSpoolFile(parentCtx context.Context, path string) error {
	reader, err := NewSpoolReader(path)
	if err != nil {
		return fmt.Errorf("open spool reader: %w", err)
	}
	defer reader.Close()

	if f.logClient == nil {
		return fmt.Errorf("no log client available")
	}

	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	stream, err := f.logClient.StreamLogs(ctx)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	defer func() {
		if s, ok := stream.(interface{ CloseAndRecv() error }); ok {
			_ = s.CloseAndRecv()
		} else {
			_ = stream.CloseSend()
		}
	}()

	for {
		chunks, err := reader.ReadBatch()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("read batch: %w", err)
		}

		for _, chunk := range chunks {
			if err := stream.Send(chunk); err != nil {
				return fmt.Errorf("send chunk: %w", err)
			}
		}
	}

	return nil
}

func defaultSpoolDir() string {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, _ := os.UserHomeDir()

		if home != "" {
			dataHome = filepath.Join(home, ".local", "share")
		} else {
			dataHome = os.TempDir()
		}
	}

	return filepath.Join(dataHome, "vectis", "log-forwarder", "spool")
}

// ResolveLogClient creates a gRPC log client using the same discovery
// semantics as the worker.
func ResolveLogClient(ctx context.Context, logger interfaces.Logger) (interfaces.LogClient, func(), error) {
	pin := config.PinnedLogAddress()
	if pin != "" {
		conn, cleanup, err := resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_LOG, pin, logger, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("pinned log client: %w", err)
		}

		return interfaces.NewGRPCLogClient(conn), cleanup, nil
	}

	regClient, err := registry.New(ctx, config.WorkerRegistryDialAddress(), logger, interfaces.SystemClock{})
	if err != nil {
		return nil, nil, fmt.Errorf("registry client: %w", err)
	}

	conn, cleanup, err := resolver.NewClientWithRegistry(ctx, api.Component_COMPONENT_LOG, logger, regClient)
	if err != nil {
		regClient.Close()
		return nil, nil, fmt.Errorf("registry log client: %w", err)
	}

	return interfaces.NewGRPCLogClient(conn), func() {
		cleanup()
		regClient.Close()
	}, nil
}
