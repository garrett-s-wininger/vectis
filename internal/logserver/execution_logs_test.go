package logserver

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

func TestExecutionLogPersistenceAndReplay(t *testing.T) {
	for _, size := range []int{20, minVectoredLogEntryRecordWriteSize + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			store := newLocalRunLogStoreForTest(t)
			entries := []LogEntry{
				{Timestamp: time.Now().UTC(), Sequence: 1, Data: []byte("legacy")},
				{Timestamp: time.Now().UTC(), Sequence: 2, Data: bytes.Repeat([]byte("x"), size), ExecutionID: "execution-a"},
				{Timestamp: time.Now().UTC(), Sequence: 3, Stream: api.Stream_STREAM_CONTROL, Data: []byte(`{"event":"completed"}`), Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS, ExecutionID: "execution-a"},
				{Timestamp: time.Now().UTC(), Sequence: 4, Data: []byte("later task"), ExecutionID: "execution-b"},
			}

			if err := store.AppendBatch("run", entries); err != nil {
				t.Fatal(err)
			}

			got, err := store.List("run")
			if err != nil {
				t.Fatal(err)
			}

			if len(got) != len(entries) {
				t.Fatalf("entries = %d", len(got))
			}

			for i := range entries {
				if got[i].ExecutionID != entries[i].ExecutionID || !bytes.Equal(got[i].Data, entries[i].Data) {
					t.Fatalf("record %d changed", i)
				}
			}

			replay, err := store.Replay("run", LogReplayOptions{Tail: 2})
			if err != nil {
				t.Fatal(err)
			}

			if len(replay.Entries) != 2 || replay.Entries[1].ExecutionID != "execution-b" || replay.TerminalAlreadyConsumed {
				t.Fatalf("replay = %+v", replay)
			}
		})
	}
}

func TestExecutionCompletionDoesNotTerminateRunLogs(t *testing.T) {
	entry := LogEntry{ExecutionID: "attempt-1", Stream: api.Stream_STREAM_CONTROL, Data: []byte(`{"event":"completed"}`), Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS}
	if isCompletedEvent(entry) {
		t.Fatal("task completion terminated run logs")
	}

	entry.ExecutionID = ""
	if !isCompletedEvent(entry) {
		t.Fatal("run completion was ignored")
	}

	stream := &fakeGetLogsServer{}
	entry.ExecutionID = "attempt-1"
	if err := sendLogEntryChunk(stream, "run", entry); err != nil {
		t.Fatal(err)
	}

	if len(stream.chunks) != 1 || stream.chunks[0].GetExecutionId() != "attempt-1" {
		t.Fatal("gRPC replay lost execution identity")
	}
}

func TestExecutionStreamEOFDoesNotSynthesizeRunCompletion(t *testing.T) {
	server := NewServer(mocks.NewMockLogger())
	stream := &fakeStreamLogsServer{
		ctx:    metadata.NewIncomingContext(t.Context(), metadata.Pairs(interfaces.LogSyntheticCompletionMetadata, "true")),
		chunks: []*api.LogChunk{{RunId: proto.String("run"), ExecutionId: proto.String("execution-1"), Stream: api.Stream_STREAM_STDOUT.Enum(), Data: []byte("task output")}},
	}

	if err := server.StreamLogs(stream); err != nil {
		t.Fatal(err)
	}

	buffer := server.getOrCreateBuffer("run")
	if buffer.IsTerminal() {
		t.Fatal("closing a task stream prematurely terminated the run")
	}
}

func TestReplayRetriedRunDoesNotCloseBeforeLatestAttemptOutput(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)
	entries := []LogEntry{
		{Timestamp: time.Now(), Sequence: 1, ExecutionID: "attempt-1", Data: []byte("first attempt\n")},
		{Timestamp: time.Now(), Sequence: 2, Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_FAILURE, Data: []byte(`{"event":"completed","status":"failure"}`)},
		{Timestamp: time.Now(), Sequence: 3, ExecutionID: "attempt-2", Data: []byte("second attempt\n")},
		{Timestamp: time.Now(), Sequence: 4, Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS, Data: []byte(`{"event":"completed","status":"success"}`)},
	}

	if err := store.AppendBatch("retry-run", entries); err != nil {
		t.Fatal(err)
	}

	server := NewServerWithStore(mocks.NewMockLogger(), store, nil)
	for _, tail := range []string{"0", "500"} {
		t.Run(tail, func(t *testing.T) {
			stream := &fakeGetLogsServer{ctx: metadata.NewIncomingContext(t.Context(), metadata.Pairs(GetLogsTailMetadata, tail))}
			if err := server.GetLogs(&api.GetLogsRequest{RunId: proto.String("retry-run")}, stream); err != nil {
				t.Fatal(err)
			}

			if len(stream.chunks) != 3 {
				t.Fatalf("replay chunks: %d", len(stream.chunks))
			}

			if stream.chunks[0].GetExecutionId() != "attempt-1" || stream.chunks[1].GetExecutionId() != "attempt-2" || stream.chunks[2].GetCompleted() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
				t.Fatalf("completion cut off latest attempt: %+v", stream.chunks)
			}
		})
	}
}

func TestActiveRetryReplayIgnoresOldCompletionAndFollowsNewOutput(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)
	entries := []LogEntry{
		{Sequence: 1, Timestamp: time.Now(), ExecutionID: "attempt-1", Data: []byte("old output")},
		{Sequence: 2, Timestamp: time.Now(), Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_FAILURE, Data: []byte(`{"event":"completed"}`)},
		{Sequence: 3, Timestamp: time.Now(), ExecutionID: "attempt-2", Data: []byte("new output")},
	}

	if err := store.AppendBatch("active-retry", entries); err != nil {
		t.Fatal(err)
	}

	server := NewServerWithStore(mocks.NewMockLogger(), store, nil)
	buffer := server.getOrCreateBuffer("active-retry")
	buffer.AddBatch(entries)
	stream := &fakeGetLogsServer{ctx: metadata.NewIncomingContext(t.Context(), metadata.Pairs(GetLogsActiveRunMetadata, "true"))}
	stream.onSend = func(chunk *api.LogChunk) {
		if chunk.GetSequence() != 3 {
			return
		}

		completed := LogEntry{Sequence: 4, Timestamp: time.Now(), Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS, Data: []byte(`{"event":"completed"}`)}
		buffer.Add(completed)
		buffer.Broadcast(t.Context(), "active-retry", completed)
	}

	if err := server.GetLogs(&api.GetLogsRequest{RunId: proto.String("active-retry")}, stream); err != nil {
		t.Fatal(err)
	}

	if len(stream.chunks) != 3 || stream.chunks[1].GetExecutionId() != "attempt-2" || stream.chunks[2].GetSequence() != 4 {
		t.Fatalf("old completion interrupted active retry: %+v", stream.chunks)
	}
}

func TestTaggedCompletionFencesOldAttemptEvenWhenCatalogStillSaysRunning(t *testing.T) {
	store := newLocalRunLogStoreForTest(t)
	entries := []LogEntry{
		{Sequence: 1, Timestamp: time.Now(), ExecutionID: "child-1", Data: []byte("first")},
		{Sequence: 2, Timestamp: time.Now(), Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_FAILURE, Data: []byte(`{"event":"completed","root_execution_id":"root-1"}`)},
		{Sequence: 3, Timestamp: time.Now(), ExecutionID: "child-2", Data: []byte("second")},
		{Sequence: 4, Timestamp: time.Now(), Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS, Data: []byte(`{"event":"completed","root_execution_id":"root-2"}`)},
		{Sequence: 5, Timestamp: time.Now(), Stream: api.Stream_STREAM_CONTROL, Completed: api.RunOutcome_RUN_OUTCOME_FAILURE, Data: []byte(`{"event":"completed","root_execution_id":"root-1"}`)},
	}

	if err := store.AppendBatch("tagged-retry", entries); err != nil {
		t.Fatal(err)
	}

	server := NewServerWithStore(mocks.NewMockLogger(), store, nil)
	stream := &fakeGetLogsServer{ctx: metadata.NewIncomingContext(t.Context(), metadata.Pairs(GetLogsActiveRunMetadata, "true", GetLogsRootExecutionMetadata, "root-2", GetLogsTailMetadata, "500"))}
	if err := server.GetLogs(&api.GetLogsRequest{RunId: proto.String("tagged-retry")}, stream); err != nil {
		t.Fatal(err)
	}

	if len(stream.chunks) != 3 || stream.chunks[1].GetExecutionId() != "child-2" || stream.chunks[2].GetSequence() != 4 {
		t.Fatalf("wrong attempt completion replayed: %+v", stream.chunks)
	}
}
