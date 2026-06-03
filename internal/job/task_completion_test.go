package job_test

import (
	"context"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
)

func TestCompleteTaskExecutionSucceededActivatesChildren(t *testing.T) {
	t.Parallel()

	runs := mocks.NewMockRunsRepository()
	runs.TaskExecutions = []dal.TaskExecutionRecord{{
		RunID:       "run-1",
		TaskID:      "run-1:child",
		TaskKey:     "child",
		ExecutionID: "run-1:child:attempt:1:execution",
	}}
	runs.TaskActivatedN = 1

	result, err := job.CompleteTaskExecution(context.Background(), runs, "execution-root", dal.ExecutionStatusSucceeded)
	if err != nil {
		t.Fatalf("CompleteTaskExecution succeeded: %v", err)
	}

	if result.Activated != 1 || len(result.Children) != 1 || result.Children[0].TaskKey != "child" {
		t.Fatalf("completion result: %+v", result)
	}

	if runs.LastSucceededExecID != "execution-root" {
		t.Fatalf("last succeeded execution: got %q, want execution-root", runs.LastSucceededExecID)
	}

	if len(runs.ExecutionTransitions) != 1 || runs.ExecutionTransitions[0] != "execution-root:"+dal.ExecutionStatusSucceeded {
		t.Fatalf("execution transitions: %+v", runs.ExecutionTransitions)
	}
}

func TestCompleteTaskExecutionNonSuccessTerminalDoesNotActivateChildren(t *testing.T) {
	t.Parallel()

	runs := mocks.NewMockRunsRepository()
	runs.TaskExecutions = []dal.TaskExecutionRecord{{
		TaskID:  "run-1:child",
		TaskKey: "child",
	}}
	runs.TaskActivatedN = 1

	result, err := job.CompleteTaskExecution(context.Background(), runs, "execution-root", dal.ExecutionStatusFailed)
	if err != nil {
		t.Fatalf("CompleteTaskExecution failed: %v", err)
	}

	if result.Activated != 0 || len(result.Children) != 0 {
		t.Fatalf("failed completion should not activate children: %+v", result)
	}

	if runs.LastSucceededExecID != "" {
		t.Fatalf("last succeeded execution should be empty, got %q", runs.LastSucceededExecID)
	}

	if len(runs.ExecutionTransitions) != 1 || runs.ExecutionTransitions[0] != "execution-root:"+dal.ExecutionStatusFailed {
		t.Fatalf("execution transitions: %+v", runs.ExecutionTransitions)
	}
}

func TestCompleteTaskExecutionRejectsNonTerminalStatus(t *testing.T) {
	t.Parallel()

	runs := mocks.NewMockRunsRepository()
	if _, err := job.CompleteTaskExecution(context.Background(), runs, "execution-root", dal.ExecutionStatusRunning); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for running status, got %v", err)
	}
}

func TestCompleteTaskExecutionRejectsMissingInputs(t *testing.T) {
	t.Parallel()

	if _, err := job.CompleteTaskExecution(context.Background(), nil, "execution-root", dal.ExecutionStatusSucceeded); err == nil || !strings.Contains(err.Error(), "runs repository is required") {
		t.Fatalf("expected runs repository error, got %v", err)
	}

	if _, err := job.CompleteTaskExecution(context.Background(), mocks.NewMockRunsRepository(), "", dal.ExecutionStatusSucceeded); !dal.IsNotFound(err) {
		t.Fatalf("expected not found for missing execution id, got %v", err)
	}
}

func TestTaskCompletionServiceRejectsMissingDependencies(t *testing.T) {
	t.Parallel()

	var service *job.TaskCompletionService
	if _, err := service.CompleteTaskExecution(context.Background(), "execution-root", dal.ExecutionStatusSucceeded); err == nil || !strings.Contains(err.Error(), "task completion service is required") {
		t.Fatalf("expected service error, got %v", err)
	}

	service = job.NewTaskCompletionService(nil)
	if _, err := service.CompleteTaskExecution(context.Background(), "execution-root", dal.ExecutionStatusSucceeded); err == nil || !strings.Contains(err.Error(), "runs repository is required") {
		t.Fatalf("expected runs repository error, got %v", err)
	}
}
