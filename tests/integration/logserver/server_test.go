//go:build integration

package logserver_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"google.golang.org/grpc"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/logserver"
	"vectis/internal/testutil/grpctest"
)

func setupLogServer(t *testing.T) api.LogServiceClient {
	t.Helper()

	logger := mocks.NewMockLogger()
	server := logserver.NewServer(logger)

	_, _, conn := grpctest.SetupGRPCServer(t, func(s *grpc.Server) {
		api.RegisterLogServiceServer(s, server)
	})

	return api.NewLogServiceClient(conn)
}

func sendChunk(t *testing.T, stream api.LogService_StreamLogsClient, runID, data string, seq int64, streamType api.Stream, completed api.RunOutcome) {
	t.Helper()

	chunk := &api.LogChunk{
		RunId:     &runID,
		Data:      []byte(data),
		Sequence:  &seq,
		Stream:    &streamType,
		Completed: completed.Enum(),
		Timestamp: nil,
	}

	if err := stream.Send(chunk); err != nil {
		t.Fatalf("failed to send chunk: %v", err)
	}
}

func recvGetLogsChunk(t *testing.T, getStream api.LogService_GetLogsClient) *api.LogChunk {
	t.Helper()

	chunk, err := getStream.Recv()
	if err != nil {
		t.Fatalf("failed to receive from GetLogs: %v", err)
	}

	return chunk
}

func TestIntegrationLogServer_StreamAndGetLogsBroadcast(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-broadcast"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	seq := int64(42)
	logData := "specific test message content"
	sendChunk(t, stream, runID, logData, seq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)

	chunk := recvGetLogsChunk(t, getStream)

	if string(chunk.GetData()) != logData {
		t.Errorf("expected log data %q, got %q", logData, string(chunk.GetData()))
	}

	if chunk.GetSequence() != seq {
		t.Errorf("expected sequence %d, got %d", seq, chunk.GetSequence())
	}

	if chunk.GetStream() != api.Stream_STREAM_STDOUT {
		t.Errorf("expected stream STDOUT, got %v", chunk.GetStream())
	}
}

func TestIntegrationLogServer_MultipleSubscribersReceiveSameMessage(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-multi"
	numSubscribers := 3

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	getStreams := make([]api.LogService_GetLogsClient, 0, numSubscribers)
	for i := range numSubscribers {
		getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
		if err != nil {
			t.Fatalf("subscriber %d: failed to start GetLogs: %v", i, err)
		}

		getStreams = append(getStreams, getStream)
	}

	seq := int64(1)
	logData := "broadcast message"
	sendChunk(t, stream, runID, logData, seq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)

	for i, getStream := range getStreams {
		chunk := recvGetLogsChunk(t, getStream)
		if string(chunk.GetData()) != logData {
			t.Errorf("subscriber %d: expected data %q, got %q", i, logData, string(chunk.GetData()))
		}

		if chunk.GetSequence() != seq {
			t.Errorf("subscriber %d: expected sequence %d, got %d", i, seq, chunk.GetSequence())
		}
	}
}

func TestIntegrationLogServer_JobIsolation(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()

	run1 := "isolated-run-1"
	run2 := "isolated-run-2"
	run1Data := "message for run1 only"
	run2Data := "message for run2 only"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	getStream1, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &run1})
	if err != nil {
		t.Fatalf("get logs for run1: %v", err)
	}

	getStream2, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &run2})
	if err != nil {
		t.Fatalf("get logs for run2: %v", err)
	}

	sendChunk(t, stream, run1, run1Data, 1, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	sendChunk(t, stream, run2, run2Data, 1, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)

	chunk1 := recvGetLogsChunk(t, getStream1)
	if string(chunk1.GetData()) != run1Data {
		t.Errorf("run1 subscriber received wrong data: expected %q, got %q", run1Data, string(chunk1.GetData()))
	}

	chunk2 := recvGetLogsChunk(t, getStream2)
	if string(chunk2.GetData()) != run2Data {
		t.Errorf("run2 subscriber received wrong data: expected %q, got %q", run2Data, string(chunk2.GetData()))
	}
}

func TestIntegrationLogServer_GetLogsReceivesHistoricalLogs(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-historical"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	logMessages := []string{"first log", "second log", "third log"}
	for i, msg := range logMessages {
		seq := int64(i + 1)
		sendChunk(t, stream, runID, msg, seq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	gotBySeq := make(map[int64]string)
	for range logMessages {
		chunk := recvGetLogsChunk(t, getStream)
		gotBySeq[chunk.GetSequence()] = string(chunk.GetData())
	}

	for i, expectedMsg := range logMessages {
		seq := int64(i + 1)
		if gotBySeq[seq] != "" && gotBySeq[seq] != expectedMsg {
			t.Errorf("seq %d: expected %q, got %q", seq, expectedMsg, gotBySeq[seq])
		}
	}
}

func TestIntegrationLogServer_HistoricalAndLiveLogs(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-history-live"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	historical := []string{"hist-1", "hist-2", "hist-3"}
	for i, msg := range historical {
		seq := int64(i + 1)
		sendChunk(t, stream, runID, msg, seq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	}

	// Start GetLogs before sending live chunks
	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	liveSeq := int64(len(historical) + 1)
	liveData := "live-log"
	sendChunk(t, stream, runID, liveData, liveSeq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	stream.CloseSend()

	gotBySeq := make(map[int64]string)
	totalExpected := len(historical) + 1

	for range totalExpected {
		chunk := recvGetLogsChunk(t, getStream)
		gotBySeq[chunk.GetSequence()] = string(chunk.GetData())
	}

	for i, msg := range historical {
		seq := int64(i + 1)
		if gotBySeq[seq] != "" && gotBySeq[seq] != msg {
			t.Errorf("historical seq %d: expected %q, got %q", seq, msg, gotBySeq[seq])
		}
	}

	if gotBySeq[liveSeq] != "" && gotBySeq[liveSeq] != liveData {
		t.Errorf("live seq %d: expected %q, got %q", liveSeq, liveData, gotBySeq[liveSeq])
	}
}

func TestIntegrationLogServer_ServerClosesStreamAfterCompleted(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-completed-close"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	defer stream.CloseSend()

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	completedData := `{"event":"completed","status":"success"}`
	sendChunk(t, stream, runID, completedData, 1, api.Stream_STREAM_CONTROL, api.RunOutcome_RUN_OUTCOME_SUCCESS)
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	chunk := recvGetLogsChunk(t, getStream)
	if chunk.GetStream() != api.Stream_STREAM_CONTROL || string(chunk.GetData()) != completedData {
		t.Errorf("expected control completed message, got stream=%v data=%q", chunk.GetStream(), string(chunk.GetData()))
	}
	if chunk.GetCompleted() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Errorf("expected Completed=RUN_OUTCOME_SUCCESS, got %v", chunk.GetCompleted())
	}

	// After completion, GetLogs stream should end
	_, err = getStream.Recv()
	if err != io.EOF {
		t.Fatalf("expected GetLogs stream to end after completion, got error: %v", err)
	}
}

func TestIntegrationLogServer_WorkerCrashSyntheticCompletion(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-worker-crash"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send one regular log, then close without completion.
	// CloseAndRecv blocks until the server processes EOF and injects the
	// synthetic completion, avoiding a race with the GetLogs call below.
	sendChunk(t, stream, runID, "some log output", 1, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	// First chunk is the regular log
	chunk1 := recvGetLogsChunk(t, getStream)
	if string(chunk1.GetData()) != "some log output" {
		t.Errorf("expected log output, got %q", string(chunk1.GetData()))
	}

	// Next chunk should be the synthetic completion event
	chunk2, err := getStream.Recv()
	if err != nil {
		t.Fatalf("expected synthetic completion chunk, got error: %v", err)
	}

	if chunk2.GetStream() != api.Stream_STREAM_CONTROL {
		t.Errorf("expected control stream, got %v", chunk2.GetStream())
	}

	var meta struct {
		Event     string `json:"event"`
		Synthetic bool   `json:"synthetic"`
	}

	if err := json.Unmarshal(chunk2.GetData(), &meta); err != nil {
		t.Fatalf("failed to parse control event: %v", err)
	}

	if meta.Event != "completed" {
		t.Errorf("expected completed event, got %q", meta.Event)
	}

	if !meta.Synthetic {
		t.Error("expected synthetic:true flag")
	}

	// Verify the proto Completed field is set to RUN_OUTCOME_UNKNOWN on synthetic completion
	if chunk2.GetCompleted() != api.RunOutcome_RUN_OUTCOME_UNKNOWN {
		t.Errorf("expected Completed=RUN_OUTCOME_UNKNOWN on synthetic completion, got %v", chunk2.GetCompleted())
	}

	// Stream should end after synthetic completion
	_, err = getStream.Recv()
	if err != io.EOF {
		t.Fatalf("expected GetLogs stream to end after synthetic completion, got error: %v", err)
	}
}

func TestIntegrationLogServer_GetLogsSinceSequence(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-run-since-seq"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	for i := range 5 {
		seq := int64(i + 1)
		sendChunk(t, stream, runID, "log-"+string(rune('A'+i)), seq, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	// Resume from sequence 2, should only get logs with seq > 2
	sinceSeq := int64(2)
	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID, SinceSequence: &sinceSeq})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	var sequences []int64
	for {
		chunk, err := getStream.Recv()
		if err != nil {
			break
		}

		sequences = append(sequences, chunk.GetSequence())
		if chunk.GetSequence() <= 2 {
			t.Errorf("received chunk with sequence %d (should be > 2)", chunk.GetSequence())
		}
	}

	if len(sequences) != 3 {
		t.Errorf("expected 3 chunks since sequence 2, got %d", len(sequences))
	}
}

func TestIntegrationLogServer_CompletedFieldPropagatesThroughGetLogs(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-completed-propagation"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send a completion chunk with proto Completed=RUN_OUTCOME_FAILURE
	completedData := `{"event":"completed","status":"failure"}`
	sendChunk(t, stream, runID, completedData, 1, api.Stream_STREAM_CONTROL, api.RunOutcome_RUN_OUTCOME_FAILURE)
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	chunk := recvGetLogsChunk(t, getStream)
	if chunk.GetCompleted() != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Errorf("expected Completed=RUN_OUTCOME_FAILURE propagated through GetLogs, got %v", chunk.GetCompleted())
	}

	if string(chunk.GetData()) != completedData {
		t.Errorf("expected data %q, got %q", completedData, string(chunk.GetData()))
	}
}

func TestIntegrationLogServer_UnspecifiedCompletedDoesNotLeak(t *testing.T) {
	client := setupLogServer(t)
	ctx := context.Background()
	runID := "test-unspecified-propagation"

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	// Send a regular stdout chunk with UNSPECIFIED completed
	sendChunk(t, stream, runID, "stdout line", 1, api.Stream_STREAM_STDOUT, api.RunOutcome_RUN_OUTCOME_UNSPECIFIED)
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	getStream, err := client.GetLogs(ctx, &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		t.Fatalf("failed to start GetLogs: %v", err)
	}

	chunk := recvGetLogsChunk(t, getStream)
	if chunk.GetCompleted() != api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
		t.Errorf("expected Completed=UNSPECIFIED for regular chunk, got %v", chunk.GetCompleted())
	}
}
