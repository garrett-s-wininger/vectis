package job

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/logrecord"

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

func TestLogSpoolForwarderFault_StaleRunSpoolQuarantined(t *testing.T) {
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
	if err := forwarder.scanAndForward(context.Background()); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathMissing(t, path)
	assertPathExists(t, path+".quarantine")
}

func TestLogSpoolForwarderFault_MissingRunIDSpoolQuarantined(t *testing.T) {
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
	if err := forwarder.scanAndForward(context.Background()); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathMissing(t, path)
	assertPathExists(t, path+".quarantine")
}

func TestLogSpoolForwarderFault_TransientSpoolFailureRetained(t *testing.T) {
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
	if err := forwarder.scanAndForward(context.Background()); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	assertPathExists(t, path)
	assertPathMissing(t, path+".quarantine")
}

func TestLogSpoolForwarderSecuresPendingDirectory(t *testing.T) {
	resetPendingLogSpools(t)

	dir := pendingSpoolDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create pending spool dir: %v", err)
	}

	if runtime.GOOS != "windows" {
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatalf("chmod pending spool dir: %v", err)
		}
	}

	forwarder := NewLogSpoolForwarder(&pendingOpenErrLogClient{err: errors.New("unused")}, interfaces.NewLogger("test"), time.Second)
	if err := forwarder.scanAndForward(); err != nil {
		t.Fatalf("scan pending spools: %v", err)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat pending spool dir: %v", err)
		}

		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("pending spool dir mode = %v, want 0700", got)
		}
	}
}

func TestEnsureLogSpoolDirRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}

	link := filepath.Join(root, "spool-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	err := ensureLogSpoolDir(link)
	if err == nil || !strings.Contains(err.Error(), "directory must not be a symlink") {
		t.Fatalf("ensureLogSpoolDir error = %v, want symlink rejection", err)
	}
}

func TestLogSpoolForwarderRejectsSymlinkSpoolFile(t *testing.T) {
	resetPendingLogSpools(t)

	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "outside.spool")
	if err := os.WriteFile(outsideFile, []byte("not a real spool"), 0o600); err != nil {
		t.Fatalf("write outside spool: %v", err)
	}

	dir := pendingSpoolDir()
	if err := ensureLogSpoolDir(dir); err != nil {
		t.Fatalf("create pending spool dir: %v", err)
	}

	link := filepath.Join(dir, "link.spool")
	if err := os.Symlink(outsideFile, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	forwarder := NewLogSpoolForwarder(&pendingOpenErrLogClient{err: errors.New("unused")}, interfaces.NewLogger("test"), time.Second)
	err := forwarder.forwardFile(link)
	if err == nil || !strings.Contains(err.Error(), "spool must not be a symlink") {
		t.Fatalf("forwardFile error = %v, want symlink spool rejection", err)
	}
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

	var records []byte
	for _, chunk := range chunks {
		payload, err := proto.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal chunk: %v", err)
		}

		records, err = logrecord.Append(records, payload)
		if err != nil {
			t.Fatalf("append spool record: %v", err)
		}
	}

	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, records, 0o600); err != nil {
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
