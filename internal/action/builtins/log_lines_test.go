package builtins

import (
	"io"
	"strings"
	"testing"

	api "vectis/api/gen/go"
)

func TestFrameworkLinesAndPartialProcessWrites(t *testing.T) {
	stream := &mockLogStream{}
	state := createTestState(stream)
	sendLog(state, api.Stream_STREAM_STDOUT, "Starting")
	streamOutput(io.MultiReader(strings.NewReader("hel"), strings.NewReader("lo\n")), state, api.Stream_STREAM_STDOUT)
	sendLog(state, api.Stream_STREAM_STDOUT, "Finished\n")

	var output strings.Builder
	for _, chunk := range stream.GetChunks() {
		output.Write(chunk.GetData())
	}

	if got := output.String(); got != "Starting\nhello\nFinished\n" {
		t.Fatalf("output = %q", got)
	}
}
