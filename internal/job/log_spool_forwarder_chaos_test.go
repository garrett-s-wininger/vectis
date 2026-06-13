package job

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

type pendingOpenErrLogClient struct {
	err error
}

func (c *pendingOpenErrLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return nil, c.err
}

func (c *pendingOpenErrLogClient) StreamLogsForRun(context.Context, string) (interfaces.LogStream, error) {
	return nil, c.err
}

func (c *pendingOpenErrLogClient) Close() error {
	return nil
}

type pendingSendErrLogClient struct {
	sendErr func(*api.LogChunk) error
}

func (c *pendingSendErrLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &pendingSendErrLogStream{client: c}, nil
}

func (c *pendingSendErrLogClient) Close() error {
	return nil
}

type pendingSendErrLogStream struct {
	client *pendingSendErrLogClient
}

func (s *pendingSendErrLogStream) Send(chunk *api.LogChunk) error {
	if s.client.sendErr != nil {
		return s.client.sendErr(chunk)
	}

	return nil
}

func (s *pendingSendErrLogStream) CloseSend() error {
	return nil
}

func TestLogSpoolForwarderChaos_StaleRunSpoolQuarantined(t *testing.T) {
	resetPendingLogSpools(t)

	runID := "stale-run"
	path := writePendingLogSpool(t, "stale-run.spool", &api.LogChunk{
		RunId:    proto.String(runID),
		Sequence: proto.Int64(1),
		Data:     []byte("cannot route"),
	})

	forwarder := NewLogSpoolForwarder(
		&pendingOpenErrLogClient{err: errors.New("not found: run stale-run")},
		interfaces.NewLogger("test"),
		time.Second,
	)
	if err := forwarder.scanAndForward(); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathMissing(t, path)
	assertPathExists(t, path+".quarantine")
}

func TestLogSpoolForwarderChaos_MissingRunIDSpoolQuarantined(t *testing.T) {
	resetPendingLogSpools(t)

	path := writePendingLogSpool(t, "missing-run.spool", &api.LogChunk{
		Sequence: proto.Int64(1),
		Data:     []byte("orphan"),
	})

	forwarder := NewLogSpoolForwarder(
		&pendingSendErrLogClient{sendErr: func(chunk *api.LogChunk) error {
			if chunk.GetRunId() == "" {
				return errors.New("run id is required")
			}

			return nil
		}},
		interfaces.NewLogger("test"),
		time.Second,
	)
	if err := forwarder.scanAndForward(); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathMissing(t, path)
	assertPathExists(t, path+".quarantine")
}

func TestLogSpoolForwarderChaos_TransientSpoolFailureRetained(t *testing.T) {
	resetPendingLogSpools(t)

	runID := "retry-run"
	path := writePendingLogSpool(t, "retry-run.spool", &api.LogChunk{
		RunId:    proto.String(runID),
		Sequence: proto.Int64(1),
		Data:     []byte("retry"),
	})

	forwarder := NewLogSpoolForwarder(
		&pendingOpenErrLogClient{err: errors.New("temporary unavailable")},
		interfaces.NewLogger("test"),
		time.Second,
	)
	if err := forwarder.scanAndForward(); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathExists(t, path)
	assertPathMissing(t, path+".quarantine")
}

func resetPendingLogSpools(t *testing.T) {
	t.Helper()

	if err := os.RemoveAll(pendingSpoolDir()); err != nil {
		t.Fatalf("remove pending spool dir: %v", err)
	}
}

func writePendingLogSpool(t *testing.T, name string, chunks ...*api.LogChunk) string {
	t.Helper()

	dir := pendingSpoolDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create pending spool dir: %v", err)
	}

	var b strings.Builder
	for _, chunk := range chunks {
		payload, err := proto.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}

		b.WriteString(base64.RawStdEncoding.EncodeToString(payload))
		b.WriteByte('\n')
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("write pending spool: %v", err)
	}

	return path
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertPathMissing(t *testing.T, path string) {
	t.Helper()

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s to be absent, stat err=%v", path, err)
	}
}
