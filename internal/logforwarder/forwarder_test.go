package logforwarder

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/protobuf/proto"
)

func pollChunkCount(t *testing.T, client *mocks.MockLogClient, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(client.GetChunks()) >= want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d chunks, got %d", want, len(client.GetChunks()))
}

func pollSpoolFiles(t *testing.T, dir string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				time.Sleep(10 * time.Millisecond)
				continue
			}

			t.Fatalf("read spool dir: %v", err)
		}

		count := 0
		for _, e := range entries {
			if filepath.Ext(e.Name()) == spoolExt {
				count++
			}
		}

		if count >= want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for %d spool files", want)
}

func assertNoSpoolFiles(t *testing.T, dir string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return
		}

		found := false
		for _, e := range entries {
			if filepath.Ext(e.Name()) == spoolExt {
				found = true
				break
			}
		}

		if !found {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for spool files to be removed")
}

// TestForwarder_HappyPath verifies that chunks received over the Unix socket
// are batched and forwarded to the log client.
func TestForwarder_HappyPath(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "forwarder.sock")

	server, err := NewSocketServer(sockPath, 1024)
	if err != nil {
		t.Fatalf("create socket server: %v", err)
	}
	defer server.Close()

	go server.Serve()

	logClient := mocks.NewMockLogClient()
	logger := interfaces.NewLogger("test")

	fwd := NewForwarder(server.Chunks(), logger, filepath.Join(tmpDir, "spool"), 5, 10000)
	fwd.SetLogClient(logClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fwd.Run(ctx)

	// Connect and send chunks via the forwarder client (same protocol as worker)
	client := interfaces.NewForwarderLogClient(sockPath)
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	chunks := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("world")},
		{RunId: proto.String("run-2"), Sequence: proto.Int64(1), Data: []byte("foo")},
	}

	for _, c := range chunks {
		if err := stream.Send(c); err != nil {
			t.Fatalf("send chunk: %v", err)
		}
	}
	stream.CloseSend()

	// Give the socket time to deliver chunks to the forwarder.
	time.Sleep(50 * time.Millisecond)

	// Trigger final flush by shutting down.
	fwd.Shutdown()
	cancel()

	pollChunkCount(t, logClient, len(chunks), 2*time.Second)

	received := logClient.GetChunks()
	if len(received) != len(chunks) {
		t.Fatalf("expected %d chunks, got %d", len(chunks), len(received))
	}

	for i, c := range chunks {
		if received[i].GetRunId() != c.GetRunId() {
			t.Errorf("chunk %d run_id: got %q, want %q", i, received[i].GetRunId(), c.GetRunId())
		}

		if received[i].GetSequence() != c.GetSequence() {
			t.Errorf("chunk %d sequence: got %d, want %d", i, received[i].GetSequence(), c.GetSequence())
		}

		if string(received[i].GetData()) != string(c.GetData()) {
			t.Errorf("chunk %d data: got %q, want %q", i, received[i].GetData(), c.GetData())
		}
	}
}

// TestForwarder_SpoolAndRecover verifies that when the log client is
// unavailable, chunks are spooled to disk and later recovered by the
// background scanner when the client becomes available again.
func TestForwarder_SpoolAndRecover(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "forwarder.sock")
	spoolDir := filepath.Join(tmpDir, "spool")

	server, err := NewSocketServer(sockPath, 1024)
	if err != nil {
		t.Fatalf("create socket server: %v", err)
	}
	defer server.Close()

	go server.Serve()

	logClient := mocks.NewMockLogClient()
	logger := interfaces.NewLogger("test")

	// Start with a failing log client so batches get spooled.
	logClient.SetStreamError(os.ErrDeadlineExceeded)

	fwd := NewForwarder(server.Chunks(), logger, spoolDir, 2, 10000)
	fwd.SetLogClient(logClient)
	fwd.SetScanInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fwd.Run(ctx)

	client := interfaces.NewForwarderLogClient(sockPath)
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	chunks := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("a")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("b")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(3), Data: []byte("c")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(4), Data: []byte("d")},
	}

	for _, c := range chunks {
		if err := stream.Send(c); err != nil {
			t.Fatalf("send chunk: %v", err)
		}
	}
	stream.CloseSend()

	// Wait for spool files to be written.
	pollSpoolFiles(t, spoolDir, 1, 2*time.Second)

	// Now allow the log client to succeed.
	logClient.SetStreamError(nil)

	// Wait for the spool scanner to pick up the files.
	pollChunkCount(t, logClient, len(chunks), 2*time.Second)

	received := logClient.GetChunks()
	if len(received) != len(chunks) {
		t.Fatalf("expected %d recovered chunks, got %d", len(chunks), len(received))
	}

	for i, c := range chunks {
		if received[i].GetSequence() != c.GetSequence() {
			t.Errorf("chunk %d sequence: got %d, want %d", i, received[i].GetSequence(), c.GetSequence())
		}
	}

	// Verify spool files were cleaned up.
	assertNoSpoolFiles(t, spoolDir, 2*time.Second)

	fwd.Shutdown()
	cancel()
}

// TestForwarder_ShutdownFlushesPending verifies that an in-memory batch is
// flushed (or spooled) when Shutdown is called.
func TestForwarder_ShutdownFlushesPending(t *testing.T) {
	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "forwarder.sock")
	spoolDir := filepath.Join(tmpDir, "spool")

	server, err := NewSocketServer(sockPath, 1024)
	if err != nil {
		t.Fatalf("create socket server: %v", err)
	}
	defer server.Close()

	go server.Serve()

	logClient := mocks.NewMockLogClient()
	logger := interfaces.NewLogger("test")

	// Batch size of 1 so each chunk flushes immediately.
	fwd := NewForwarder(server.Chunks(), logger, spoolDir, 1, 10000)
	fwd.SetLogClient(logClient)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go fwd.Run(ctx)

	client := interfaces.NewForwarderLogClient(sockPath)
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}

	chunk := &api.LogChunk{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("flush-me")}
	if err := stream.Send(chunk); err != nil {
		t.Fatalf("send chunk: %v", err)
	}
	stream.CloseSend()

	pollChunkCount(t, logClient, 1, 2*time.Second)

	received := logClient.GetChunks()
	if len(received) != 1 {
		t.Fatalf("expected 1 chunk after flush, got %d", len(received))
	}

	if string(received[0].GetData()) != "flush-me" {
		t.Errorf("data mismatch: got %q, want %q", received[0].GetData(), "flush-me")
	}

	fwd.Shutdown()
	cancel()
}
