package logforwarder

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/logclient"
	"vectis/internal/logroute"
	"vectis/internal/logspool"
)

const (
	defaultBatchSize       = 100
	defaultMaxChunksPerSec = 10000
	defaultFlushInterval   = 10 * time.Millisecond
	defaultScanInterval    = 5 * time.Second

	maxBatchRPCPayloadBytes = 256 << 10
)

const (
	MetricsRouteHinted   = "hinted"
	MetricsRouteUnhinted = "unhinted"

	MetricsBatchSent    = "sent"
	MetricsBatchSpooled = "spooled"
	MetricsBatchLost    = "lost"
)

type Metrics interface {
	RecordChunkReceived(ctx context.Context, route string)
	RecordBatch(ctx context.Context, outcome string)
}

// Forwarder receives log chunks from local workers over a Unix socket,
// batches them, and forwards to vectis-log via gRPC.
// When vectis-log is unavailable it writes to a shared spool directory
// for later retry.
type Forwarder struct {
	logClient     interfaces.LogClient
	logger        interfaces.Logger
	chunkCh       <-chan *api.LogChunk
	spoolDir      string
	batchSize     int
	flushInterval time.Duration
	scanInterval  time.Duration
	shutdownCh    chan struct{}
	shutdownOnce  sync.Once
	spoolCounter  uint64
	sendMu        sync.Mutex
	metrics       Metrics
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
		chunkCh:       chunkCh,
		logger:        logger,
		spoolDir:      spoolDir,
		batchSize:     batchSize,
		flushInterval: forwarderFlushInterval(maxChunksPerSec),
		scanInterval:  defaultScanInterval,
		shutdownCh:    make(chan struct{}),
	}
}

func forwarderFlushInterval(maxChunksPerSec int) time.Duration {
	if maxChunksPerSec <= 0 {
		maxChunksPerSec = defaultMaxChunksPerSec
	}

	interval := time.Second / time.Duration(maxChunksPerSec)
	if interval < defaultFlushInterval {
		return defaultFlushInterval
	}

	return interval
}

// SetLogClient configures the gRPC client used to reach vectis-log.
func (f *Forwarder) SetLogClient(client interfaces.LogClient) {
	f.logClient = client
}

func (f *Forwarder) SetMetrics(metrics Metrics) {
	f.metrics = metrics
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
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = f.drainAndFlush(ctx, batch)
			wg.Wait()
			return
		case <-f.shutdownCh:
			_ = f.drainAndFlush(ctx, batch)
			wg.Wait()
			return
		case chunk, ok := <-f.chunkCh:
			if !ok {
				f.flushBatch(ctx, batch)
				wg.Wait()
				return
			}

			f.recordChunkReceived(ctx, chunk)
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

			f.recordChunkReceived(ctx, chunk)
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

	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	if f.hasSpoolBacklog() {
		if spoolErr := f.spoolBatch(batch); spoolErr != nil {
			if f.logger != nil {
				f.logger.Error("Log batch lost: spool backlog present and spool failed: %v", spoolErr)
			}
			f.recordBatch(ctx, MetricsBatchLost)
		} else {
			f.recordBatch(ctx, MetricsBatchSpooled)
		}

		return batch[:0]
	}

	if f.logClient == nil {
		if spoolErr := f.spoolBatch(batch); spoolErr != nil {
			if f.logger != nil {
				f.logger.Error("Log batch lost: log client nil and spool failed: %v", spoolErr)
			}
			f.recordBatch(ctx, MetricsBatchLost)
		} else {
			f.recordBatch(ctx, MetricsBatchSpooled)
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
			f.recordBatch(ctx, MetricsBatchLost)
		} else {
			f.recordBatch(ctx, MetricsBatchSpooled)
		}

		return batch[:0]
	}

	f.recordBatch(ctx, MetricsBatchSent)
	return batch[:0]
}

func (f *Forwarder) recordChunkReceived(ctx context.Context, chunk *api.LogChunk) {
	if f.metrics == nil || chunk == nil {
		return
	}

	route := MetricsRouteUnhinted
	if chunk.GetLogShardId() != "" {
		route = MetricsRouteHinted
	}

	f.metrics.RecordChunkReceived(ctx, route)
}

func (f *Forwarder) recordBatch(ctx context.Context, outcome string) {
	if f.metrics == nil {
		return
	}

	f.metrics.RecordBatch(ctx, outcome)
}

func (f *Forwarder) hasSpoolBacklog() bool {
	entries, err := os.ReadDir(f.spoolDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == spoolExt {
			return true
		}
	}

	return false
}

func (f *Forwarder) sendBatch(parentCtx context.Context, batch []*api.LogChunk) error {
	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	return f.sendChunkGroups(ctx, batch)
}

func (f *Forwarder) sendChunkGroups(ctx context.Context, chunks []*api.LogChunk) error {
	if f.preferUnscopedLogStream(chunks) {
		return f.sendChunkGroup(ctx, "", "", chunks)
	}

	groups := groupChunksByRoute(chunks)
	for _, group := range groups {
		if err := f.sendChunkGroup(ctx, group.runID, group.logShardID, group.chunks); err != nil {
			return err
		}
	}

	return nil
}

func (f *Forwarder) preferUnscopedLogStream(chunks []*api.LogChunk) bool {
	client, ok := f.logClient.(interface{ PreferUnscopedLogStream() bool })
	if !ok || !client.PreferUnscopedLogStream() {
		return false
	}

	for _, chunk := range chunks {
		if chunk.GetLogShardId() != "" {
			return false
		}
	}

	return true
}

func (f *Forwarder) sendChunkGroup(ctx context.Context, runID, logShardID string, chunks []*api.LogChunk) error {
	if preferBatchRPC(chunks) {
		if sent, err := f.sendChunkBatch(ctx, runID, logShardID, chunks); sent || err != nil {
			return err
		}
	}

	stream, err := f.openLogStream(ctx, runID, logShardID)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	for _, chunk := range chunks {
		if err := stream.Send(chunk); err != nil {
			_ = closeLogStream(stream)
			return fmt.Errorf("send chunk: %w", err)
		}
	}

	if err := closeLogStream(stream); err != nil {
		return fmt.Errorf("close log stream: %w", err)
	}

	return nil
}

func closeLogStream(stream interfaces.LogStream) error {
	if s, ok := stream.(interface{ CloseAndRecv() error }); ok {
		return s.CloseAndRecv()
	}

	return stream.CloseSend()
}

func (f *Forwarder) sendChunkBatch(ctx context.Context, runID, logShardID string, chunks []*api.LogChunk) (bool, error) {
	if assigned, ok := f.logClient.(interfaces.AssignedRunLogBatchClient); ok && runID != "" && logShardID != "" {
		return true, assigned.SendLogBatchForAssignedRun(ctx, runID, logShardID, chunks)
	}

	if scoped, ok := f.logClient.(interfaces.RunLogBatchClient); ok && runID != "" {
		return true, scoped.SendLogBatchForRun(ctx, runID, chunks)
	}

	if batch, ok := f.logClient.(interfaces.LogBatchClient); ok {
		return true, batch.SendLogBatch(ctx, chunks)
	}

	return false, nil
}

func preferBatchRPC(chunks []*api.LogChunk) bool {
	var payloadBytes int
	for _, chunk := range chunks {
		payloadBytes += len(chunk.GetRunId()) + len(chunk.GetData())
		if payloadBytes > maxBatchRPCPayloadBytes {
			return false
		}
	}

	return true
}

func (f *Forwarder) openLogStream(ctx context.Context, runID, logShardID string) (interfaces.LogStream, error) {
	if assigned, ok := f.logClient.(interfaces.AssignedRunLogClient); ok && runID != "" && logShardID != "" {
		return assigned.StreamLogsForAssignedRun(ctx, runID, logShardID)
	}

	if scoped, ok := f.logClient.(interfaces.RunLogClient); ok && runID != "" {
		return scoped.StreamLogsForRun(ctx, runID)
	}

	return f.logClient.StreamLogs(ctx)
}

type chunkGroup struct {
	runID      string
	logShardID string
	chunks     []*api.LogChunk
}

type chunkGroupKey struct {
	runID      string
	logShardID string
}

func groupChunksByRoute(chunks []*api.LogChunk) []chunkGroup {
	byRun := make(map[chunkGroupKey]int)
	groups := make([]chunkGroup, 0)
	for _, chunk := range chunks {
		route := logroute.FromChunk(chunk)
		key := chunkGroupKey{
			runID:      route.RunID,
			logShardID: route.LogShardID,
		}

		idx, ok := byRun[key]
		if !ok {
			idx = len(groups)
			byRun[key] = idx
			groups = append(groups, chunkGroup{runID: key.runID, logShardID: key.logShardID})
		}

		groups[idx].chunks = append(groups[idx].chunks, chunk)
	}

	return groups
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
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

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

			// Quarantine permanently unrecoverable files so they are not retried forever.
			if logspool.IsPermanentReplayError(err) {
				quarantinePath := path + ".quarantine"
				if renameErr := os.Rename(path, quarantinePath); renameErr == nil && f.logger != nil {
					f.logger.Warn("Quarantined unrecoverable spool file %s", name)
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

	for {
		chunks, err := reader.ReadBatch()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("read batch: %w", err)
		}

		if err := f.sendChunkGroups(ctx, chunks); err != nil {
			return err
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
func ResolveLogClient(ctx context.Context, logger interfaces.Logger, retryMetrics backoff.RetryMetrics, routingMetrics logclient.RoutingMetrics) (interfaces.LogClient, func(), error) {
	client, err := logclient.NewManagingLogClient(ctx, logger, logclient.PoolOptions{
		PinnedAddress:   config.PinnedLogAddress(),
		RegistryAddress: config.WorkerRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
		Metrics:         routingMetrics,
	})

	if err != nil {
		return nil, nil, fmt.Errorf("log client: %w", err)
	}

	return client, func() { _ = client.Close() }, nil
}
