package interfaces

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/testutil/socktest"

	"google.golang.org/protobuf/proto"
)

func TestForwarderLogClientRoundtrip(t *testing.T) {
	sockPath := socktest.ShortPath(t, "fwd.sock")

	// Stand up a minimal server that speaks the length-prefixed protocol.
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	defer os.Remove(sockPath)

	var received []*api.LogChunk
	done := make(chan struct{})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(done)
			return
		}
		defer conn.Close()

		for {
			var length uint32
			if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
				close(done)
				return
			}

			data := make([]byte, length)
			if _, err := io.ReadFull(conn, data); err != nil {
				close(done)
				return
			}

			var chunk api.LogChunk
			if err := proto.Unmarshal(data, &chunk); err != nil {
				t.Errorf("unmarshal: %v", err)
				close(done)
				return
			}

			received = append(received, &chunk)
		}
	}()

	client := NewForwarderLogClient(sockPath)
	ctx := context.Background()
	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}

	want := []*api.LogChunk{
		{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")},
		{RunId: proto.String("run-1"), Sequence: proto.Int64(2), Data: []byte("world")},
	}

	for _, c := range want {
		if err := stream.Send(c); err != nil {
			t.Fatalf("send: %v", err)
		}
	}

	// CloseSend closes the underlying connection, which will cause the server
	// to hit EOF and exit its loop.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server")
	}

	if len(received) != len(want) {
		t.Fatalf("expected %d chunks, got %d", len(want), len(received))
	}

	for i := range want {
		if received[i].GetRunId() != want[i].GetRunId() {
			t.Errorf("chunk %d run_id: got %q, want %q", i, received[i].GetRunId(), want[i].GetRunId())
		}

		if received[i].GetSequence() != want[i].GetSequence() {
			t.Errorf("chunk %d sequence: got %d, want %d", i, received[i].GetSequence(), want[i].GetSequence())
		}

		if string(received[i].GetData()) != string(want[i].GetData()) {
			t.Errorf("chunk %d data: got %q, want %q", i, received[i].GetData(), want[i].GetData())
		}
	}
}

func TestForwarderLogClientDialMissingSocket(t *testing.T) {
	sockPath := socktest.ShortPath(t, "missing.sock")

	client := NewForwarderLogClient(sockPath)
	_, err := client.StreamLogs(context.Background())
	if err == nil {
		t.Fatal("expected error dialing missing socket, got nil")
	}
}

func TestForwarderLogClientNilChunk(t *testing.T) {
	sockPath := socktest.ShortPath(t, "nil.sock")

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	defer os.Remove(sockPath)

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	client := NewForwarderLogClient(sockPath)
	stream, err := client.StreamLogs(context.Background())
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	// Sending nil should return an error to match gRPC behavior.
	if err := stream.Send(nil); err == nil {
		t.Fatal("expected error for nil chunk, got nil")
	}
}

func TestForwarderLogClientSendToActiveSocket(t *testing.T) {
	sockPath := socktest.ShortPath(t, "log-forwarder.sock")

	// Create a listening socket with an acceptor so Send does not rely on
	// kernel backlog buffering.
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen on unix socket: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, _ := ln.Accept()
		if conn != nil {
			// Drain the length-prefixed chunk so the client's Send succeeds.
			var length uint32
			binary.Read(conn, binary.BigEndian, &length)
			io.CopyN(io.Discard, conn, int64(length))
			conn.Close()
		}
	}()

	client := NewForwarderLogClient(sockPath)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("stream logs: %v", err)
	}
	defer stream.CloseSend()

	chunk := &api.LogChunk{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")}
	if err := stream.Send(chunk); err != nil {
		t.Fatalf("send chunk: %v", err)
	}
}
