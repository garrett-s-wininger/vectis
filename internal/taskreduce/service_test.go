package taskreduce_test

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/taskreduce"
)

type recordingReduceRunner struct {
	runID    string
	decision taskreduce.Decision
	err      error
}

func (r *recordingReduceRunner) Reduce(_ context.Context, runID string) (taskreduce.Decision, error) {
	r.runID = runID
	return r.decision, r.err
}

func TestServiceProcessRunsReducer(t *testing.T) {
	t.Parallel()

	runner := &recordingReduceRunner{
		decision: taskreduce.Decision{
			Outcome: taskreduce.OutcomeSucceeded,
			Summary: dal.RunTaskCompletion{RunID: "run-1", Total: 1, Succeeded: 1},
		},
	}

	decision, err := taskreduce.NewService(runner).Process(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if runner.runID != "run-1" {
		t.Fatalf("runner run id: got %q, want run-1", runner.runID)
	}

	if decision.Outcome != taskreduce.OutcomeSucceeded || decision.Summary.RunID != "run-1" {
		t.Fatalf("decision: %+v", decision)
	}
}

func TestServiceProcessPropagatesReducerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("db unavailable")
	runner := &recordingReduceRunner{err: wantErr}

	if _, err := taskreduce.NewService(runner).Process(context.Background(), "run-1"); !errors.Is(err, wantErr) {
		t.Fatalf("Process error: got %v, want %v", err, wantErr)
	}
}

func TestServiceProcessRejectsMissingRunner(t *testing.T) {
	t.Parallel()

	if _, err := taskreduce.NewService(nil).Process(context.Background(), "run-1"); err == nil {
		t.Fatal("Process should reject missing runner")
	}
}
