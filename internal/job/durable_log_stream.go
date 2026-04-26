package job

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

var (
	logFlushTimeout = 30 * time.Second
	logRetryBase    = 150 * time.Millisecond
	logRetryMax     = 2 * time.Second
	logInitialProbe = 200 * time.Millisecond
	logTuneMu       sync.RWMutex
)

func LogFlushTimeoutForTest() time.Duration {
	logTuneMu.RLock()
	defer logTuneMu.RUnlock()
	return logFlushTimeout
}
func SetLogFlushTimeoutForTest(d time.Duration) {
	if d > 0 {
		logTuneMu.Lock()
		logFlushTimeout = d
		logTuneMu.Unlock()
	}
}

func LogRetryBaseForTest() time.Duration {
	logTuneMu.RLock()
	defer logTuneMu.RUnlock()
	return logRetryBase
}
func SetLogRetryBaseForTest(d time.Duration) {
	if d > 0 {
		logTuneMu.Lock()
		logRetryBase = d
		logTuneMu.Unlock()
	}
}

func LogRetryMaxForTest() time.Duration {
	logTuneMu.RLock()
	defer logTuneMu.RUnlock()
	return logRetryMax
}
func SetLogRetryMaxForTest(d time.Duration) {
	if d > 0 {
		logTuneMu.Lock()
		logRetryMax = d
		logTuneMu.Unlock()
	}
}

func LogInitialProbeForTest() time.Duration {
	logTuneMu.RLock()
	defer logTuneMu.RUnlock()
	return logInitialProbe
}
func SetLogInitialProbeForTest(d time.Duration) {
	if d > 0 {
		logTuneMu.Lock()
		logInitialProbe = d
		logTuneMu.Unlock()
	}
}

type durableLogStream struct {
	logger    interfaces.Logger
	logClient interfaces.LogClient

	mu           sync.Mutex
	cond         *sync.Cond
	spool        *os.File
	spoolPath    string
	writeOffset  int64
	maxSpoolSize int64
	closed       bool
	closeTime    time.Time

	done   chan struct{}
	stream interfaces.LogStream

	streamCtx    context.Context
	streamCancel context.CancelFunc
	senderErr    error
	degraded     bool
	closeWaitLog bool
}

func sanitizeRunIDForSpool(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "/", "-"), string(filepath.Separator), "-")
}

func newDurableLogStream(logClient interfaces.LogClient, logger interfaces.Logger, runID string) (*durableLogStream, error) {
	if logClient == nil {
		return nil, fmt.Errorf("log client is required")
	}

	baseDir := filepath.Join(os.TempDir(), "vectis-log-spool")
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create log spool dir: %w", err)
	}

	prefix := sanitizeRunIDForSpool(runID)
	if prefix == "" {
		prefix = "run"
	}

	spool, err := os.CreateTemp(baseDir, prefix+"-*.spool")
	if err != nil {
		return nil, fmt.Errorf("create spool file: %w", err)
	}

	maxSize := defaultMaxSpoolBytes()
	d := &durableLogStream{
		logger:       logger,
		logClient:    logClient,
		spool:        spool,
		spoolPath:    spool.Name(),
		maxSpoolSize: maxSize,
		done:         make(chan struct{}),
	}

	d.streamCtx, d.streamCancel = context.WithCancel(context.Background())
	d.cond = sync.NewCond(&d.mu)

	go d.senderLoop()
	d.probeInitialConnectivity()
	return d, nil
}

func (d *durableLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return nil
	}

	payload, err := proto.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("marshal log chunk: %w", err)
	}

	line := base64.RawStdEncoding.EncodeToString(payload) + "\n"

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return fmt.Errorf("log stream already closed")
	}

	if d.writeOffset+int64(len(line)) > d.maxSpoolSize {
		if d.logger != nil {
			d.logger.Warn("Log spool full for run; dropping log chunk (max %d bytes)", d.maxSpoolSize)
		}

		return nil
	}

	n, err := d.spool.WriteString(line)
	if err != nil {
		return fmt.Errorf("write spool chunk: %w", err)
	}

	d.writeOffset += int64(n)
	d.cond.Signal()
	return nil
}

func (d *durableLogStream) CloseSend() error {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		d.closeTime = time.Now()
		d.cond.Broadcast()
	}

	if d.degraded && !d.closeWaitLog {
		d.closeWaitLog = true
		if d.logger != nil {
			d.logger.Warn("Log aggregator still unavailable; flush will continue in background (run outcome is independent)")
		}
	}
	d.mu.Unlock()

	// Non-blocking: the senderLoop continues flushing in the background.
	return nil
}

// WaitForDone blocks until the background senderLoop exits or the timeout expires.
// It is intended for tests that need to verify async flush behavior.
func (d *durableLogStream) WaitForDone(timeout time.Duration) error {
	select {
	case <-d.done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timed out waiting for sender loop after %s", timeout)
	}
}

// SenderErr returns the last sender error, if any. Safe to call after WaitForDone.
func (d *durableLogStream) SenderErr() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.senderErr
}

func (d *durableLogStream) senderLoop() {
	defer close(d.done)
	defer func() {
		if d.streamCancel != nil {
			d.streamCancel()
		}

		if d.spool != nil {
			_ = d.spool.Close()
		}

		if d.spoolPath != "" {
			d.mu.Lock()
			save := d.senderErr != nil
			d.mu.Unlock()

			if save {
				if err := d.moveSpoolToPending(); err != nil && d.logger != nil {
					d.logger.Warn("Failed to move spool to pending: %v", err)
				}
			} else {
				_ = os.Remove(d.spoolPath)
			}
		}
	}()

	readFile, err := os.Open(d.spoolPath)
	if err != nil {
		d.setSenderErr(fmt.Errorf("open spool file for read: %w", err))
		if d.logger != nil {
			d.logger.Error("Failed to open spool file for read: %v", err)
		}

		return
	}
	defer readFile.Close()

	reader := bufio.NewReader(readFile)
	var readOffset int64
	retryAttempt := 0
	pending := ""

	for {
		d.mu.Lock()
		for !d.closed && readOffset >= d.writeOffset {
			d.cond.Wait()
		}

		shouldExit := d.closed && readOffset >= d.writeOffset
		flushTimeout := LogFlushTimeoutForTest()
		deadlineExceeded := d.closed && time.Since(d.closeTime) > flushTimeout
		d.mu.Unlock()

		if shouldExit && pending == "" {
			if err := d.finalizeCurrentStream(); err != nil {
				d.setSenderErr(fmt.Errorf("finalize log stream: %w", err))
			}

			return
		}

		if deadlineExceeded {
			if pending != "" {
				if d.logger != nil {
					d.logger.Warn("Log flush deadline exceeded with unsent chunks; giving up after %s", flushTimeout)
				}
			}

			d.setSenderErr(fmt.Errorf("log flush deadline exceeded after %s", flushTimeout))
			return
		}

		if shouldExit && pending != "" {
			chunk, err := decodeSpoolLine(pending)
			pending = ""
			if err != nil {
				d.setSenderErr(fmt.Errorf("decode trailing spool line: %w", err))
				if d.logger != nil {
					d.logger.Warn("Skipping invalid trailing spool line: %v", err)
				}

				continue
			}

			if err := d.sendWithRetry(chunk, &retryAttempt); err != nil {
				d.setSenderErr(err)
				return
			}

			continue
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				if line != "" {
					pending += line
					readOffset += int64(len(line))
				}

				time.Sleep(20 * time.Millisecond)
				continue
			}

			if d.logger != nil {
				d.logger.Warn("Spool read error: %v", err)
			}

			time.Sleep(20 * time.Millisecond)
			continue
		}

		readOffset += int64(len(line))
		if pending != "" {
			line = pending + line
			pending = ""
		}

		chunk, err := decodeSpoolLine(line)
		if err != nil {
			if d.logger != nil {
				d.logger.Warn("Skipping invalid spool line: %v", err)
			}

			continue
		}

		if err := d.sendWithRetry(chunk, &retryAttempt); err != nil {
			d.setSenderErr(err)
			return
		}
	}
}

func (d *durableLogStream) sendWithRetry(chunk *api.LogChunk, retryAttempt *int) error {
	for {
		d.mu.Lock()
		flushTimeout := LogFlushTimeoutForTest()
		deadlineExceeded := d.closed && time.Since(d.closeTime) > flushTimeout
		d.mu.Unlock()

		if deadlineExceeded {
			return fmt.Errorf("log flush deadline exceeded after %s", flushTimeout)
		}

		stream, err := d.ensureStream()
		if err != nil {
			d.noteAggregatorUnavailable(err)
			delay := backoff.ExponentialDelay(LogRetryBaseForTest(), *retryAttempt, LogRetryMaxForTest())
			*retryAttempt = *retryAttempt + 1
			time.Sleep(delay)
			continue
		}

		if err := stream.Send(chunk); err != nil {
			d.noteAggregatorUnavailable(err)

			_ = d.closeCurrentStream()
			delay := backoff.ExponentialDelay(LogRetryBaseForTest(), *retryAttempt, LogRetryMaxForTest())
			*retryAttempt = *retryAttempt + 1
			time.Sleep(delay)
			continue
		}

		d.noteAggregatorRecovered()
		*retryAttempt = 0
		return nil
	}
}

func (d *durableLogStream) ensureStream() (interfaces.LogStream, error) {
	d.mu.Lock()
	if d.stream != nil {
		stream := d.stream
		d.mu.Unlock()
		return stream, nil
	}

	flushTimeout := LogFlushTimeoutForTest()
	deadlineExceeded := d.closed && time.Since(d.closeTime) > flushTimeout
	d.mu.Unlock()

	if deadlineExceeded {
		return nil, fmt.Errorf("log flush deadline exceeded after %s", flushTimeout)
	}

	stream, err := d.logClient.StreamLogs(d.streamCtx)
	if err != nil {
		d.noteAggregatorUnavailable(err)
		return nil, err
	}

	d.mu.Lock()
	if d.stream == nil {
		d.stream = stream
		d.mu.Unlock()

		return stream, nil
	}
	d.mu.Unlock()
	_ = stream.CloseSend()

	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stream, nil
}

func (d *durableLogStream) probeInitialConnectivity() {
	probeTimeout := LogInitialProbeForTest()
	if probeTimeout <= 0 {
		return
	}

	probeCtx, cancel := context.WithTimeout(d.streamCtx, probeTimeout)
	defer cancel()

	stream, err := d.logClient.StreamLogs(probeCtx)
	if err != nil {
		d.noteAggregatorUnavailable(err)
		return
	}

	_ = stream.CloseSend()
}

func (d *durableLogStream) closeCurrentStream() error {
	d.mu.Lock()
	stream := d.stream
	d.stream = nil
	d.mu.Unlock()

	if stream == nil {
		return nil
	}

	return stream.CloseSend()
}

func (d *durableLogStream) finalizeCurrentStream() error {
	d.mu.Lock()
	stream := d.stream
	d.stream = nil
	d.mu.Unlock()

	if stream == nil {
		return nil
	}

	type closeAndReceiver interface {
		CloseAndRecv() error
	}

	if s, ok := stream.(closeAndReceiver); ok {
		return s.CloseAndRecv()
	}

	return stream.CloseSend()
}

func (d *durableLogStream) setSenderErr(err error) {
	if err == nil {
		return
	}

	d.mu.Lock()
	if d.senderErr == nil {
		d.senderErr = err
	}
	d.mu.Unlock()
}

func (d *durableLogStream) noteAggregatorUnavailable(err error) {
	d.mu.Lock()
	alreadyDegraded := d.degraded
	if !alreadyDegraded {
		d.degraded = true
	}
	d.mu.Unlock()

	if alreadyDegraded || d.logger == nil {
		return
	}

	d.logger.Warn("Log aggregator unavailable; spooling logs locally and retrying")
	if err != nil {
		d.logger.Debug("Log aggregator unavailable detail: %v", err)
	}
}

func (d *durableLogStream) noteAggregatorRecovered() {
	d.mu.Lock()
	wasDegraded := d.degraded
	d.degraded = false
	d.closeWaitLog = false
	d.mu.Unlock()

	if !wasDegraded || d.logger == nil {
		return
	}

	d.logger.Info("Log aggregator reconnected; resumed flushing spooled logs")
}

func defaultMaxSpoolBytes() int64 {
	return 10 * 1024 * 1024 // 10 MB
}

func pendingSpoolDir() string {
	return filepath.Join(os.TempDir(), "vectis-log-spool", "pending")
}

func (d *durableLogStream) moveSpoolToPending() error {
	dir := pendingSpoolDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}

	name := filepath.Base(d.spoolPath)
	pendingPath := filepath.Join(dir, name)
	if err := os.Rename(d.spoolPath, pendingPath); err != nil {
		return fmt.Errorf("rename spool to pending: %w", err)
	}

	if d.logger != nil {
		d.logger.Info("Moved unfinished spool to pending: %s", pendingPath)
	}
	return nil
}

func decodeSpoolLine(line string) (*api.LogChunk, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty line")
	}

	payload, err := base64.RawStdEncoding.DecodeString(line)
	if err != nil {
		return nil, fmt.Errorf("decode base64: %w", err)
	}

	var chunk api.LogChunk
	if err := proto.Unmarshal(payload, &chunk); err != nil {
		return nil, fmt.Errorf("unmarshal chunk: %w", err)
	}

	return &chunk, nil
}
