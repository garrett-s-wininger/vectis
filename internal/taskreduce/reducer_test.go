package taskreduce_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/taskreduce"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/runfixture"
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
			name:    "failed when terminal failure exists with incomplete siblings",
			summary: dal.RunTaskCompletion{RunID: "run-1", Total: 4, Succeeded: 1, TerminalFailed: 1, Incomplete: 2},
			want:    taskreduce.OutcomeFailed,
		},
		{
			name:    "waiting when no tasks exist",
			summary: dal.RunTaskCompletion{RunID: "run-1"},
			want:    taskreduce.OutcomeWaiting,
		},
	}

	for _, tt := range tests {
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

func TestReducerReduceFailureDominatesIncompleteSibling(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "task-reduce-branch-failure", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-reduce-branch-failure"
	def := `{"id":"job-task-reduce-branch-failure","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	failedBranch, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: root.TaskID,
		TaskKey:      "failed-branch",
		SpecHash:     "sha256:failed-branch",
	})

	if err != nil {
		t.Fatalf("ensure failed branch: %v", err)
	}

	incompleteBranch, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: root.TaskID,
		TaskKey:      "incomplete-branch",
		SpecHash:     "sha256:incomplete-branch",
	})

	if err != nil {
		t.Fatalf("ensure incomplete branch: %v", err)
	}

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, root.ExecutionID, dal.ExecutionStatusSucceeded)
	if activated, children := result.Activated, result.Children; activated != 2 || len(children) != 2 {
		t.Fatalf("root success fan-out activated=%d children=%+v", activated, children)
	}

	runfixture.FinalizeExecutionByClaimWithFailure(t, ctx, repos, failedBranch.ExecutionID, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "failed branch")

	decision, err := taskreduce.New(repos.Runs()).Reduce(ctx, runID)
	if err != nil {
		t.Fatalf("Reduce: %v", err)
	}

	if decision.Outcome != taskreduce.OutcomeFailed {
		t.Fatalf("outcome: got %q, want %q; summary=%+v", decision.Outcome, taskreduce.OutcomeFailed, decision.Summary)
	}

	if decision.Summary.Total != 3 || decision.Summary.Succeeded != 1 || decision.Summary.TerminalFailed != 1 || decision.Summary.Incomplete != 1 {
		t.Fatalf("summary should include succeeded root, failed branch, and incomplete sibling: %+v", decision.Summary)
	}

	pendingSibling, err := repos.Runs().GetExecutionDispatch(ctx, incompleteBranch.ExecutionID)
	if err != nil {
		t.Fatalf("incomplete sibling should still be pending: %v", err)
	}

	if pendingSibling.ExecutionID != incompleteBranch.ExecutionID || pendingSibling.TaskKey != "incomplete-branch" {
		t.Fatalf("pending sibling dispatch mismatch: %+v", pendingSibling)
	}
}
