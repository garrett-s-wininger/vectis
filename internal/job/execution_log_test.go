package job

import (
	"testing"

	"google.golang.org/protobuf/proto"
	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces/mocks"
)

func TestExecutionLogStreamAttributesWithoutMutatingInput(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, err := client.StreamLogs(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	chunk := &api.LogChunk{RunId: proto.String("run"), ExecutionId: proto.String("spoofed"), Data: []byte("output")}
	wrapped := &executionLogStream{LogStream: stream, executionID: "execution-1"}
	if err := wrapped.Send(chunk); err != nil {
		t.Fatal(err)
	}

	got := client.GetChunks()
	if len(got) != 1 || got[0].GetExecutionId() != "execution-1" || string(got[0].GetData()) != "output" {
		t.Fatalf("chunks = %v", got)
	}

	if chunk.GetExecutionId() != "spoofed" {
		t.Fatal("input chunk was mutated")
	}
}

func TestExecutorMessagesEndWithNewlinesAndControlsStayUnchanged(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, err := client.StreamLogs(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	state := &action.ExecutionState{LogStream: stream}
	sendLog(state, api.Stream_STREAM_STDOUT, "Starting task")
	sendLog(state, api.Stream_STREAM_STDERR, "Failed task\n")
	sendCompletionLog(state, api.Stream_STREAM_CONTROL, `{"event":"completed"}`, api.RunOutcome_RUN_OUTCOME_FAILURE)
	chunks := client.GetChunks()

	if len(chunks) != 3 || string(chunks[0].GetData()) != "Starting task\n" || string(chunks[1].GetData()) != "Failed task\n" || string(chunks[2].GetData()) != `{"event":"completed"}` {
		t.Fatalf("chunks = %v", chunks)
	}
}
