package job

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type TaskCompletionResult struct {
	Children  []dal.TaskExecutionRecord
	Activated int
}

type TaskCompleter interface {
	CompleteTaskExecution(ctx context.Context, executionID, status string) (TaskCompletionResult, error)
}

type TaskCompletionService struct {
	runs dal.RunsRepository
}

func NewTaskCompletionService(runs dal.RunsRepository) *TaskCompletionService {
	return &TaskCompletionService{runs: runs}
}

func (s *TaskCompletionService) CompleteTaskExecution(ctx context.Context, executionID, status string) (TaskCompletionResult, error) {
	if s == nil {
		return TaskCompletionResult{}, fmt.Errorf("task completion service is required")
	}

	return CompleteTaskExecution(ctx, s.runs, executionID, status)
}

func CompleteTaskExecution(ctx context.Context, runs dal.RunsRepository, executionID, status string) (TaskCompletionResult, error) {
	if runs == nil {
		return TaskCompletionResult{}, fmt.Errorf("runs repository is required")
	}

	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return TaskCompletionResult{}, fmt.Errorf("%w: execution_id is required", dal.ErrNotFound)
	}

	status = strings.TrimSpace(status)
	switch status {
	case dal.ExecutionStatusSucceeded:
		children, activated, err := runs.MarkExecutionSucceededAndActivateChildren(ctx, executionID)
		if err != nil {
			return TaskCompletionResult{}, err
		}

		return TaskCompletionResult{
			Children:  children,
			Activated: activated,
		}, nil
	case dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		if err := runs.MarkExecutionTerminal(ctx, executionID, status); err != nil {
			return TaskCompletionResult{}, err
		}

		return TaskCompletionResult{}, nil
	default:
		return TaskCompletionResult{}, fmt.Errorf("%w: unsupported terminal execution status %s", dal.ErrConflict, status)
	}
}
