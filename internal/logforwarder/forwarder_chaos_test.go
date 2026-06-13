package logforwarder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

type closeAckLogClient struct {
	mu       sync.Mutex
	chunks   []*api.LogChunk
	closeErr error
}

func (c *closeAckLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &closeAckLogStream{client: c}, nil
}

func (c *closeAckLogClient) Close() error {
	return nil
}

func (c *closeAckLogClient) setCloseErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeErr = err
}

func (c *closeAckLogClient) snapshot() []*api.LogChunk {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]*api.LogChunk, len(c.chunks))
	copy(out, c.chunks)
	return out
}

type closeAckLogStream struct {
	client *closeAckLogClient
}

func (s *closeAckLogStream) Send(chunk *api.LogChunk) error {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()

	s.client.chunks = append(s.client.chunks, chunk)
	return nil
}

func (s *closeAckLogStream) CloseSend() error {
	return s.CloseAndRecv()
}

func (s *closeAckLogStream) CloseAndRecv() error {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()

	return s.client.closeErr
}

type replayErrLogClient struct {
	sendErr func(*api.LogChunk) error
}

func (c *replayErrLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &replayErrLogStream{client: c}, nil
}

func (c *replayErrLogClient) Close() error {
	return nil
}

type replayErrLogStream struct {
	client *replayErrLogClient
}

func (s *replayErrLogStream) Send(chunk *api.LogChunk) error {
	if s.client.sendErr != nil {
		return s.client.sendErr(chunk)
	}

	return nil
}

func (s *replayErrLogStream) CloseSend() error {
	return nil
}

func TestForwarderChaos_SpoolRetainedWhenStreamCloseAckFails(t *testing.T) {
	spoolDir := t.TempDir()
	client := &closeAckLogClient{closeErr: errors.New("log service rejected stream")}
	fwd := NewForwarder(nil, interfaces.NewLogger("test"), spoolDir, 10, 10000)
	fwd.SetLogClient(client)

	chunks := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("one")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("two")},
	}
	if err := fwd.spoolBatch(chunks); err != nil {
		t.Fatalf("spool batch: %v", err)
	}

	if err := fwd.scanSpool(context.Background()); err != nil {
		t.Fatalf("scan spool with close failure: %v", err)
	}

	if got := countSpoolFiles(t, spoolDir); got != 1 {
		t.Fatalf("spool file count after failed close ack = %d, want 1", got)
	}

	sentAfterFailure := client.snapshot()
	if len(sentAfterFailure) != len(chunks) {
		t.Fatalf("sent chunks after failed close ack = %d, want %d", len(sentAfterFailure), len(chunks))
	}

	client.setCloseErr(nil)
	if err := fwd.scanSpool(context.Background()); err != nil {
		t.Fatalf("scan spool after recovery: %v", err)
	}

	if got := countSpoolFiles(t, spoolDir); got != 0 {
		t.Fatalf("spool file count after close ack recovery = %d, want 0", got)
	}

	sentAfterRecovery := client.snapshot()
	if len(sentAfterRecovery) != len(chunks)*2 {
		t.Fatalf("sent chunks after retry = %d, want %d", len(sentAfterRecovery), len(chunks)*2)
	}
}

func TestForwarderChaos_UnrecoverableSpoolQuarantined(t *testing.T) {
	tests := []struct {
		name    string
		chunks  []*api.LogChunk
		sendErr func(*api.LogChunk) error
	}{
		{
			name: "missing run id",
			chunks: []*api.LogChunk{
				{Sequence: proto.Int64(1), Data: []byte("orphan")},
			},
			sendErr: func(chunk *api.LogChunk) error {
				if chunk.GetRunId() == "" {
					return errors.New("run id is required")
				}

				return nil
			},
		},
		{
			name: "stale run assignment",
			chunks: []*api.LogChunk{
				{RunId: proto.String("stale-run"), Sequence: proto.Int64(1), Data: []byte("stale")},
			},
			sendErr: func(*api.LogChunk) error {
				return errors.New("not found: run stale-run")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spoolDir := t.TempDir()
			client := &replayErrLogClient{sendErr: tt.sendErr}
			fwd := NewForwarder(nil, interfaces.NewLogger("test"), spoolDir, 10, 10000)
			fwd.SetLogClient(client)

			if err := fwd.spoolBatch(tt.chunks); err != nil {
				t.Fatalf("spool batch: %v", err)
			}

			if err := fwd.scanSpool(context.Background()); err != nil {
				t.Fatalf("scan spool: %v", err)
			}

			if got := countSpoolFiles(t, spoolDir); got != 0 {
				t.Fatalf("retryable spool file count = %d, want 0", got)
			}

			if got := countQuarantineFiles(t, spoolDir); got != 1 {
				t.Fatalf("quarantine file count = %d, want 1", got)
			}
		})
	}
}

func countSpoolFiles(t *testing.T, dir string) int {
	return countFilesWithExt(t, dir, spoolExt)
}

func countQuarantineFiles(t *testing.T, dir string) int {
	return countFilesWithExt(t, dir, ".quarantine")
}

func countFilesWithExt(t *testing.T, dir, ext string) int {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read spool dir: %v", err)
	}

	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) == ext {
			count++
		}
	}

	return count
}
