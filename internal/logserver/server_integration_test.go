package logserver_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

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

	sseListener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to create sse listener: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sse/logs/{id}", server.HandleSSE)

	go http.Serve(sseListener, mux)

	t.Cleanup(func() {
		sseListener.Close()
	})

	return api.NewLogServiceClient(conn), sseListener.Addr().String()
}

func readNextSSEData(r *bufio.Reader) ([]byte, error) {
	var dataBuf strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if dataBuf.Len() == 0 {
				continue
			}
			return []byte(dataBuf.String()), nil
		}

		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			dataBuf.WriteString(data)
		}
	}
}

func TestIntegrationLogServer_StreamAndWebSocketBroadcast(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-broadcast"

	sseURL := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, runID)
	sseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		t.Fatalf("create sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect sse: %v", err)
	}
	defer sseResp.Body.Close()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq := int64(42)
	logData := "specific test message content"
	chunk := &api.LogChunk{
		RunId:    &runID,
		Data:     []byte(logData),
		Sequence: &seq,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send chunk: %v", err)
	}

	reader := bufio.NewReader(sseResp.Body)
	message, err := readNextSSEData(reader)
	if err != nil {
		t.Fatalf("failed to read sse message: %v", err)
	}

	var entry logserver.LogEntry
	if err := json.Unmarshal(message, &entry); err != nil {
		t.Fatalf("failed to unmarshal sse message: %v", err)
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
	runID := "test-run-multi"
	numSubscribers := 3

	sseReaders := make([]*bufio.Reader, 0, numSubscribers)
	sseCancels := make([]context.CancelFunc, 0, numSubscribers)
	for i := range numSubscribers {
		sseURL := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, runID)
		sseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		sseCancels = append(sseCancels, cancel)

		req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseURL, nil)
		if err != nil {
			t.Fatalf("create sse request: %v", err)
		}
		req.Header.Set("Accept", "text/event-stream")

		sseResp, err := http.DefaultClient.Do(req)
		if err != nil {
			cancel()
			t.Fatalf("failed to connect sse %d: %v", i, err)
		}
		sseReaders = append(sseReaders, bufio.NewReader(sseResp.Body))
		defer sseResp.Body.Close()
	}
	defer func() {
		for _, cancel := range sseCancels {
			cancel()
		}
	}()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq := int64(1)
	logData := "broadcast message"
	chunk := &api.LogChunk{
		RunId:    &runID,
		Data:     []byte(logData),
		Sequence: &seq,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send chunk: %v", err)
	}

	for i, reader := range sseReaders {
		message, err := readNextSSEData(reader)
		if err != nil {
			t.Errorf("subscriber %d: failed to receive sse message: %v", i, err)
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

	run1 := "isolated-run-1"
	run2 := "isolated-run-2"
	job1Data := "message for run1 only"
	job2Data := "message for run2 only"

	sseURL1 := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, run1)
	sseURL2 := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, run2)

	sseCtx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	sseReq1, err := http.NewRequestWithContext(sseCtx1, http.MethodGet, sseURL1, nil)
	if err != nil {
		t.Fatalf("create sse request1: %v", err)
	}
	sseReq1.Header.Set("Accept", "text/event-stream")
	sseResp1, err := http.DefaultClient.Do(sseReq1)
	if err != nil {
		t.Fatalf("connect sse1: %v", err)
	}
	defer sseResp1.Body.Close()

	sseCtx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	sseReq2, err := http.NewRequestWithContext(sseCtx2, http.MethodGet, sseURL2, nil)
	if err != nil {
		t.Fatalf("create sse request2: %v", err)
	}
	sseReq2.Header.Set("Accept", "text/event-stream")
	sseResp2, err := http.DefaultClient.Do(sseReq2)
	if err != nil {
		t.Fatalf("connect sse2: %v", err)
	}
	defer sseResp2.Body.Close()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq1 := int64(1)
	chunk1 := &api.LogChunk{
		RunId:    &run1,
		Data:     []byte(job1Data),
		Sequence: &seq1,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}
	if err := stream.Send(chunk1); err != nil {
		t.Fatalf("failed to send chunk to job1: %v", err)
	}

	seq2 := int64(1)
	chunk2 := &api.LogChunk{
		RunId:    &run2,
		Data:     []byte(job2Data),
		Sequence: &seq2,
		Stream:   api.Stream_STREAM_STDOUT.Enum(),
	}
	if err := stream.Send(chunk2); err != nil {
		t.Fatalf("failed to send chunk to job2: %v", err)
	}

	reader1 := bufio.NewReader(sseResp1.Body)
	msg1, err := readNextSSEData(reader1)
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

	reader2 := bufio.NewReader(sseResp2.Body)
	msg2, err := readNextSSEData(reader2)
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

func TestIntegrationLogServer_SSEReceivesHistoricalLogs(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-historical"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	logMessages := []string{"first log", "second log", "third log"}
	for i, msg := range logMessages {
		seq := int64(i + 1)
		chunk := &api.LogChunk{
			RunId:    &runID,
			Data:     []byte(msg),
			Sequence: &seq,
			Stream:   api.Stream_STREAM_STDOUT.Enum(),
		}

		if err := stream.Send(chunk); err != nil {
			t.Fatalf("failed to send chunk %d: %v", i, err)
		}
	}
	stream.CloseSend()

	sseURL := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, runID)
	sseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		t.Fatalf("create sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect sse: %v", err)
	}
	defer sseResp.Body.Close()

	reader := bufio.NewReader(sseResp.Body)

	gotBySeq := make(map[int64]string)

	for i := range logMessages {
		message, err := readNextSSEData(reader)
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
	runID := "test-run-history-live"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	historical := []string{"hist-1", "hist-2", "hist-3"}
	for i, msg := range historical {
		seq := int64(i + 1)
		chunk := &api.LogChunk{
			RunId:    &runID,
			Data:     []byte(msg),
			Sequence: &seq,
			Stream:   api.Stream_STREAM_STDOUT.Enum(),
		}

		if err := stream.Send(chunk); err != nil {
			t.Fatalf("failed to send historical chunk %d: %v", i, err)
		}
	}

	sseURL := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, runID)
	sseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		t.Fatalf("create sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect sse: %v", err)
	}
	defer sseResp.Body.Close()

	reader := bufio.NewReader(sseResp.Body)

	liveSeq := int64(len(historical) + 1)
	liveData := "live-log"
	liveChunk := &api.LogChunk{
		RunId:    &runID,
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
		message, err := readNextSSEData(reader)
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

func TestIntegrationLogServer_ServerClosesConnectionAfterCompleted(t *testing.T) {
	client, wsAddr := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-completed-close"

	sseURL := fmt.Sprintf("http://%s/sse/logs/%s", wsAddr, runID)
	sseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		t.Fatalf("create sse request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to connect sse: %v", err)
	}
	defer sseResp.Body.Close()

	reader := bufio.NewReader(sseResp.Body)

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	seq := int64(1)
	completedData := `{"event":"completed","status":"success"}`
	chunk := &api.LogChunk{
		RunId:    &runID,
		Data:     []byte(completedData),
		Sequence: &seq,
		Stream:   api.Stream_STREAM_CONTROL.Enum(),
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send completed chunk: %v", err)
	}

	stream.CloseSend()

	message, err := readNextSSEData(reader)
	if err != nil {
		t.Fatalf("failed to read completed sse message: %v", err)
	}

	var entry logserver.LogEntry
	if err := json.Unmarshal(message, &entry); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if entry.Stream != api.Stream_STREAM_CONTROL || entry.Data != completedData {
		t.Errorf("expected control completed message, got stream=%v data=%q", entry.Stream, entry.Data)
	}

	_, err = readNextSSEData(reader)
	if err == nil {
		t.Fatal("expected SSE response to be closed by server after completed, got no error")
	}
}
