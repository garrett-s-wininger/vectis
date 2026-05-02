package logforwarder

import (
	"context"
	"os"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/testutil/socktest"

	"google.golang.org/protobuf/proto"
)

func TestSocketServerRoundtrip(t *testing.T) {
	sockPath := socktest.ShortPath(t, "test.sock")

	server, err := NewSocketServer(sockPath, 1024)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer server.Close()

	go server.Serve()

	client := interfaces.NewForwarderLogClient(sockPath)
	ctx := context.Background()
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	want := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("world")},
		{RunId: proto.String("run-2"), Sequence: proto.Int64(1), Data: []byte("!")},
	}

	for _, c := range want {
		if err := stream.Send(c); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	stream.CloseSend()

	var got []*api.LogChunk
	timeout := time.After(2 * time.Second)
	done := make(chan struct{})
	go func() {
		for {
			select {
			case chunk, ok := <-server.Chunks():
				if !ok {
					close(done)
					return
				}

				got = append(got, chunk)
				if len(got) == len(want) {
					close(done)
					return
				}
			case <-time.After(100 * time.Millisecond):
				close(done)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-timeout:
		t.Fatal("timed out waiting for chunks")
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d chunks, got %d", len(want), len(got))
	}

	for i := range want {
		if got[i].GetRunId() != want[i].GetRunId() {
			t.Errorf("chunk %d run_id: got %q, want %q", i, got[i].GetRunId(), want[i].GetRunId())
		}

		if got[i].GetSequence() != want[i].GetSequence() {
			t.Errorf("chunk %d sequence: got %d, want %d", i, got[i].GetSequence(), want[i].GetSequence())
		}

		if string(got[i].GetData()) != string(want[i].GetData()) {
			t.Errorf("chunk %d data: got %q, want %q", i, got[i].GetData(), want[i].GetData())
		}
	}
}

func TestSocketServerBackpressureDrops(t *testing.T) {
	sockPath := socktest.ShortPath(t, "test.sock")

	// Tiny buffer so backpressure kicks in quickly.
	server, err := NewSocketServer(sockPath, 1)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	defer server.Close()

	go server.Serve()

	client := interfaces.NewForwarderLogClient(sockPath)
	ctx := context.Background()
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	// Don't read from server.Chunks() — fill the buffer and observe that
	// the client does not block (drops are silent on the sender side).
	for i := range 50 {
		if err := stream.Send(&api.LogChunk{RunId: proto.String("r"), Sequence: proto.Int64(int64(i))}); err != nil {
			t.Fatalf("send chunk %d: %v", i, err)
		}
	}
	stream.CloseSend()

	// The server should have received at most a few chunks before dropping.
	time.Sleep(100 * time.Millisecond)

	count := 0
	for {
		select {
		case _, ok := <-server.Chunks():
			if !ok {
				goto check
			}

			count++
			if count >= 10 {
				goto check
			}
		default:
			goto check
		}
	}

check:
	// We sent 50 but the channel buffer is 1. Some should have been dropped.
	// We can't assert an exact number, but we should have received at least 1.
	if count == 0 {
		t.Fatal("expected at least some chunks to be received")
	}
}

func TestSocketServerStaleSocketRemoved(t *testing.T) {
	sockPath := socktest.ShortPath(t, "stale.sock")

	// Pre-create a stale socket file.
	if err := os.WriteFile(sockPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale socket: %v", err)
	}

	server, err := NewSocketServer(sockPath, 10)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	server.Close()
}
