package taskreduce_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/taskreduce"
)

func TestDecide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		summary dal.RunTaskCompletion
		want    taskreduce.Outcome
	}{
		{
			name:    "waiting when tasks are incomplete",
			summary: dal.RunTaskCompletion{RunID: "run-1", Total: 3, Succeeded: 2, Incomplete: 1},
			want:    taskreduce.OutcomeWaiting,
		},
		{
			name:    "succeeded when all tasks succeeded",
			summary: dal.RunTaskCompletion{RunID: "run-1", Total: 3, Succeeded: 3},
			want:    taskreduce.OutcomeSucceeded,
		},
		{
			name:    "failed when any task terminal failed",
			summary: dal.RunTaskCompletion{RunID: "run-1", Total: 3, Succeeded: 2, TerminalFailed: 1},
			want:    taskreduce.OutcomeFailed,
		},
		{
			name:    "waiting when no tasks exist",
			summary: dal.RunTaskCompletion{RunID: "run-1"},
			want:    taskreduce.OutcomeWaiting,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := taskreduce.Decide(tt.summary).Outcome; got != tt.want {
				t.Fatalf("outcome: got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReducerReduceLoadsTaskCompletion(t *testing.T) {
	t.Parallel()

	runs := mocks.NewMockRunsRepository()
	runs.TaskCompletion = dal.RunTaskCompletion{Total: 1, Succeeded: 1}

	decision, err := taskreduce.New(runs).Reduce(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Reduce: %v", err)
	}

	if decision.Outcome != taskreduce.OutcomeSucceeded {
		t.Fatalf("outcome: got %q, want %q", decision.Outcome, taskreduce.OutcomeSucceeded)
	}

	if decision.Summary.RunID != "run-1" {
		t.Fatalf("summary run id: got %q, want run-1", decision.Summary.RunID)
	}
}
