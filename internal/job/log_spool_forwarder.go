package job

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/interfaces"
)

// LogSpoolForwarder periodically scans the pending spool directory and retries
// sending unfinished log batches to the log service.
type LogSpoolForwarder struct {
	logClient interfaces.LogClient
	logger    interfaces.Logger
	interval  time.Duration
}

func NewLogSpoolForwarder(logClient interfaces.LogClient, logger interfaces.Logger, interval time.Duration) *LogSpoolForwarder {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	return &LogSpoolForwarder{
		logClient: logClient,
		logger:    logger,
		interval:  interval,
	}
}

func (f *LogSpoolForwarder) Run(ctx context.Context) {
	// On startup, move any orphaned spool files from previous crashes into pending.
	if err := f.moveOrphanedSpoolsToPending(); err != nil && f.logger != nil {
		f.logger.Debug("Log spool forwarder startup orphan scan error: %v", err)
	}

	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.scanAndForward(); err != nil {
				if f.logger != nil {
					f.logger.Debug("Log spool forwarder scan error: %v", err)
				}
			}
		}
	}
}

func (f *LogSpoolForwarder) moveOrphanedSpoolsToPending() error {
	baseDir := spoolBaseDir()
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read base spool dir: %w", err)
	}

	pendingDir := pendingSpoolDir()
	if err := os.MkdirAll(pendingDir, 0o755); err != nil {
		return fmt.Errorf("create pending dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".spool") {
			continue
		}

		oldPath := filepath.Join(baseDir, name)
		newPath := filepath.Join(pendingDir, name)
		if err := os.Rename(oldPath, newPath); err != nil {
			if f.logger != nil {
				f.logger.Warn("Failed to move orphaned spool %s to pending: %v", name, err)
			}
			continue
		}
		if f.logger != nil {
			f.logger.Info("Moved orphaned spool to pending: %s", name)
		}
	}

	return nil
}

func (f *LogSpoolForwarder) scanAndForward() error {
	dir := pendingSpoolDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read pending dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".spool") {
			continue
		}

		path := filepath.Join(dir, name)
		if err := f.forwardFile(path); err != nil {
			if f.logger != nil {
				f.logger.Warn("Failed to forward pending spool %s: %v", name, err)
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

func (f *LogSpoolForwarder) forwardFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open spool: %w", err)
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var stream interfaces.LogStream
	var streamRunID string
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && line == "" {
				break
			}

			return fmt.Errorf("read spool: %w", err)
		}

		chunk, err := decodeSpoolLine(line)
		if err != nil {
			if f.logger != nil {
				f.logger.Warn("Skipping invalid spool line in %s: %v", path, err)
			}

			continue
		}

		runID := chunk.GetRunId()
		if stream == nil || runID != streamRunID {
			if err := closeLogStream(stream); err != nil {
				return fmt.Errorf("close stream: %w", err)
			}

			stream, err = f.openLogStream(ctx, runID)
			if err != nil {
				return fmt.Errorf("create stream: %w", err)
			}

			streamRunID = runID
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("send chunk: %w", err)
		}
	}

	if err := closeLogStream(stream); err != nil {
		return fmt.Errorf("close stream: %w", err)
	}

	return nil
}

func (f *LogSpoolForwarder) openLogStream(ctx context.Context, runID string) (interfaces.LogStream, error) {
	if scoped, ok := f.logClient.(interfaces.RunLogClient); ok && runID != "" {
		return scoped.StreamLogsForRun(ctx, runID)
	}

	return f.logClient.StreamLogs(ctx)
}

func closeLogStream(stream interfaces.LogStream) error {
	if stream == nil {
		return nil
	}

	if s, ok := stream.(interface{ CloseAndRecv() error }); ok {
		return s.CloseAndRecv()
	}

	return stream.CloseSend()
}

// ForwardSpoolFile sends a single spool file to the log service.
// It is used by both the forwarder and direct recovery paths.
func ForwardSpoolFile(path string, logClient interfaces.LogClient, logger interfaces.Logger) error {
	f := &LogSpoolForwarder{logClient: logClient, logger: logger}
	return f.forwardFile(path)
}
