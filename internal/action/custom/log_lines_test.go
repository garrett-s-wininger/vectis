package custom

import (
	"io"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces/mocks"
)

func TestFrameworkLinesAndPartialProcessWrites(t *testing.T) {
	client := mocks.NewMockLogClient()
	stream, err := client.StreamLogs(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	state := &action.ExecutionState{LogStream: stream}
	logLine(state, api.Stream_STREAM_STDOUT, "Starting")
	streamOutput(io.MultiReader(strings.NewReader("hel"), strings.NewReader("lo\n")), state, api.Stream_STREAM_STDOUT)
	logLine(state, api.Stream_STREAM_STDOUT, "Finished\n")
	var output strings.Builder
	for _, chunk := range client.GetChunks() {
		output.Write(chunk.GetData())
	}

	if got := output.String(); got != "Starting\nhello\nFinished\n" {
		t.Fatalf("output = %q", got)
	}
}
