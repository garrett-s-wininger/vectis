package logserver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/logserver"
	"vectis/internal/testutil/grpctest"
)

func setupLogServer(t *testing.T) (api.LogServiceClient, string) {
	t.Helper()

	logger := mocks.NewMockLogger()
	server := logserver.NewServer(logger)

	_, _, conn := grpctest.SetupGRPCServer(t, func(s *grpc.Server) {
		api.RegisterLogServiceServer(s, server)
	})

	wsListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to create websocket listener: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws/logs/{id}", server.HandleWebSocket)

	go http.Serve(wsListener, mux)

	t.Cleanup(func() {
		wsListener.Close()
	})

	return api.NewLogServiceClient(conn), wsListener.Addr().String()
}

func TestIntegrationLogServer_StreamAndWebSocketBroadcast(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	jobID := "test-job-broadcast"

	wsURL := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, jobID)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer wsConn.Close()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq := int64(42)
	logData := "specific test message content"
	chunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte(logData),
		Sequence: &seq,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send chunk: %v", err)
	}

	wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, message, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read websocket message: %v", err)
	}

	var entry logserver.LogEntry
	if err := json.Unmarshal(message, &entry); err != nil {
		t.Fatalf("failed to unmarshal websocket message: %v", err)
	}

	if entry.Data != logData {
		t.Errorf("expected log data %q, got %q", logData, entry.Data)
	}

	if entry.Sequence != seq {
		t.Errorf("expected sequence %d, got %d", seq, entry.Sequence)
	}

	if entry.Stream != api.Stream_STREAM_STDOUT {
		t.Errorf("expected stream STDOUT, got %v", entry.Stream)
	}
}

func TestIntegrationLogServer_MultipleSubscribersReceiveSameMessage(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	jobID := "test-job-multi"
	numSubscribers := 3

	var wsConns []*websocket.Conn
	for i := range numSubscribers {
		wsURL := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, jobID)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("failed to connect websocket %d: %v", i, err)
		}

		wsConns = append(wsConns, conn)
		defer conn.Close()
	}

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq := int64(1)
	logData := "broadcast message"
	chunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte(logData),
		Sequence: &seq,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send chunk: %v", err)
	}

	for i, conn := range wsConns {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, message, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("subscriber %d: failed to receive message: %v", i, err)
			continue
		}

		var entry logserver.LogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			t.Errorf("subscriber %d: failed to unmarshal message: %v", i, err)
			continue
		}

		if entry.Data != logData {
			t.Errorf("subscriber %d: expected data %q, got %q", i, logData, entry.Data)
		}

		if entry.Sequence != seq {
			t.Errorf("subscriber %d: expected sequence %d, got %d", i, seq, entry.Sequence)
		}
	}
}

func TestIntegrationLogServer_JobIsolation(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()

	job1 := "isolated-job-1"
	job2 := "isolated-job-2"
	job1Data := "message for job1 only"
	job2Data := "message for job2 only"

	wsURL1 := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, job1)
	wsConn1, _, err := websocket.DefaultDialer.Dial(wsURL1, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket for job1: %v", err)
	}
	defer wsConn1.Close()

	wsURL2 := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, job2)
	wsConn2, _, err := websocket.DefaultDialer.Dial(wsURL2, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket for job2: %v", err)
	}
	defer wsConn2.Close()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq1 := int64(1)
	chunk1 := &api.LogChunk{
		JobId:    &job1,
		Data:     []byte(job1Data),
		Sequence: &seq1,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}
	if err := stream.Send(chunk1); err != nil {
		t.Fatalf("failed to send chunk to job1: %v", err)
	}

	seq2 := int64(1)
	chunk2 := &api.LogChunk{
		JobId:    &job2,
		Data:     []byte(job2Data),
		Sequence: &seq2,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}
	if err := stream.Send(chunk2); err != nil {
		t.Fatalf("failed to send chunk to job2: %v", err)
	}

	wsConn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg1, err := wsConn1.ReadMessage()
	if err != nil {
		t.Fatalf("job1 subscriber failed to receive: %v", err)
	}

	var entry1 logserver.LogEntry
	if err := json.Unmarshal(msg1, &entry1); err != nil {
		t.Fatalf("failed to unmarshal job1 message: %v", err)
	}

	if entry1.Data != job1Data {
		t.Errorf("job1 subscriber received wrong data: expected %q, got %q", job1Data, entry1.Data)
	}

	wsConn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg2, err := wsConn2.ReadMessage()
	if err != nil {
		t.Fatalf("job2 subscriber failed to receive: %v", err)
	}

	var entry2 logserver.LogEntry
	if err := json.Unmarshal(msg2, &entry2); err != nil {
		t.Fatalf("failed to unmarshal job2 message: %v", err)
	}

	if entry2.Data != job2Data {
		t.Errorf("job2 subscriber received wrong data: expected %q, got %q", job2Data, entry2.Data)
	}
}

func TestIntegrationLogServer_WebSocketReceivesHistoricalLogs(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	jobID := "test-job-historical"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	logMessages := []string{"first log", "second log", "third log"}
	for i, msg := range logMessages {
		seq := int64(i + 1)
		chunk := &api.LogChunk{
			JobId:    &jobID,
			Data:     []byte(msg),
			Sequence: &seq,
			Stream:   api.Stream_STREAM_STDOUT.Enum(),
		}

		if err := stream.Send(chunk); err != nil {
			t.Fatalf("failed to send chunk %d: %v", i, err)
		}
	}
	stream.CloseSend()

	wsURL := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, jobID)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer wsConn.Close()

	gotBySeq := make(map[int64]string)

	for i := range logMessages {
		wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			t.Fatalf("failed to read historical log %d: %v", i, err)
		}

		var entry logserver.LogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			t.Fatalf("failed to unmarshal log %d: %v", i, err)
		}

		gotBySeq[entry.Sequence] = entry.Data
	}

	for i, expectedMsg := range logMessages {
		seq := int64(i + 1)
		if gotBySeq[seq] != "" && gotBySeq[seq] != expectedMsg {
			t.Errorf("seq %d: expected %q, got %q", seq, expectedMsg, gotBySeq[seq])
		}
	}
}

func TestIntegrationLogServer_HistoricalAndLiveLogs(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	jobID := "test-job-history-live"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	historical := []string{"hist-1", "hist-2", "hist-3"}
	for i, msg := range historical {
		seq := int64(i + 1)
		chunk := &api.LogChunk{
			JobId:    &jobID,
			Data:     []byte(msg),
			Sequence: &seq,
			Stream:   api.Stream_STREAM_STDOUT.Enum(),
		}

		if err := stream.Send(chunk); err != nil {
			t.Fatalf("failed to send historical chunk %d: %v", i, err)
		}
	}

	wsURL := fmt.Sprintf("ws://%s/ws/logs/%s", wsAddr, jobID)
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect websocket: %v", err)
	}
	defer wsConn.Close()

	liveSeq := int64(len(historical) + 1)
	liveData := "live-log"
	liveChunk := &api.LogChunk{
		JobId:    &jobID,
		Data:     []byte(liveData),
		Sequence: &liveSeq,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}

	if err := stream.Send(liveChunk); err != nil {
		t.Fatalf("failed to send live chunk: %v", err)
	}
	stream.CloseSend()

	gotBySeq := make(map[int64]string)
	totalExpected := len(historical) + 1

	for i := range totalExpected {
		wsConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			t.Fatalf("failed to read log %d: %v", i, err)
		}

		var entry logserver.LogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			t.Fatalf("failed to unmarshal log %d: %v", i, err)
		}

		gotBySeq[entry.Sequence] = entry.Data
	}

	for i, msg := range historical {
		seq := int64(i + 1)
		if gotBySeq[seq] != "" && gotBySeq[seq] != msg {
			t.Errorf("historical seq %d: expected %q, got %q", seq, msg, gotBySeq[seq])
		}
	}

	liveSeqExpected := int64(len(historical) + 1)
	if gotBySeq[liveSeqExpected] != "" && gotBySeq[liveSeqExpected] != liveData {
		t.Errorf("live seq %d: expected %q, got %q", liveSeqExpected, liveData, gotBySeq[liveSeqExpected])
	}
}
