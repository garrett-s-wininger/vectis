package main

import (
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestRunLogCompletionFollowsRunFinalization(t *testing.T) {
	client := mocks.NewMockLogClient()
	w := &worker{logClient: client, logger: mocks.NewMockLogger()}
	for _, status := range []string{dal.RunStatusRunning, dal.RunStatusQueued} {
		w.publishRunLogCompletion(t.Context(), dal.RunStatusUpdate{RunID: "run", Status: status})
	}

	if len(client.GetChunks()) != 0 {
		t.Fatal("nonterminal run emitted completion")
	}

	w.publishRunLogCompletion(t.Context(), dal.RunStatusUpdate{RunID: "run", Status: dal.RunStatusSucceeded, RootExecutionID: "root-2"})
	chunks := client.GetChunks()
	if len(chunks) != 1 || chunks[0].GetRunId() != "run" || chunks[0].GetExecutionId() != "" || chunks[0].GetCompleted() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Fatalf("chunks = %v", chunks)
	}

	if !strings.Contains(string(chunks[0].GetData()), `"root_execution_id":"root-2"`) {
		t.Fatal("run completion lost dispatch identity")
	}
}
