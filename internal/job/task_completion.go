package job

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type TaskCompletionResult struct {
	ExecutionID string
	Status      string
	Children    []dal.TaskExecutionRecord
	Activated   int
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

	result, err := CompleteTaskExecution(ctx, s.runs, executionID, status)
	if err != nil {
		return TaskCompletionResult{}, err
	}

	RecordTaskCompletion(ctx, result)
	return result, nil
}

func RecordTaskCompletion(ctx context.Context, result TaskCompletionResult) {
	span := trace.SpanFromContext(ctx)
	attrs := TaskCompletionAttributes(result)
	span.SetAttributes(attrs...)
	span.AddEvent("task.complete", trace.WithAttributes(attrs...))
}

func TaskCompletionAttributes(result TaskCompletionResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("vectis.execution.id", result.ExecutionID),
		attribute.String("vectis.execution.status", result.Status),
		attribute.Int("vectis.task.children.activated", result.Activated),
		attribute.Int("vectis.task.children.dispatchable", len(result.Children)),
		attribute.Bool("vectis.task.fanout.activation_point", len(result.Children) > 0),
	}
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
			ExecutionID: executionID,
			Status:      status,
			Children:    children,
			Activated:   activated,
		}, nil
	case dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		if err := runs.MarkExecutionTerminal(ctx, executionID, status); err != nil {
			return TaskCompletionResult{}, err
		}

		return TaskCompletionResult{
			ExecutionID: executionID,
			Status:      status,
		}, nil
	default:
		return TaskCompletionResult{}, fmt.Errorf("%w: unsupported terminal execution status %s", dal.ErrConflict, status)
	}
}
