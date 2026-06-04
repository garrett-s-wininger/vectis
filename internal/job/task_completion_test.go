package job_test

import (
	"context"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

func TestTaskCompletionServiceRecordsTraceCompletion(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("task-completion-test").Start(context.Background(), "complete")
	runs := mocks.NewMockRunsRepository()
	runs.TaskExecutions = []dal.TaskExecutionRecord{
		{TaskID: "run-1:build", TaskKey: "build"},
		{TaskID: "run-1:test", TaskKey: "test"},
	}
	runs.TaskActivatedN = 2

	result, err := job.NewTaskCompletionService(runs).CompleteTaskExecution(ctx, " execution-root ", " "+dal.ExecutionStatusSucceeded+" ")
	if err != nil {
		t.Fatalf("CompleteTaskExecution: %v", err)
	}
	span.End()

	if result.ExecutionID != "execution-root" || result.Status != dal.ExecutionStatusSucceeded {
		t.Fatalf("normalized completion result: %+v", result)
	}

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	if got := spanAttributeString(spans[0].Attributes(), "vectis.execution.id"); got != "execution-root" {
		t.Fatalf("execution id attr: got %q, want execution-root", got)
	}

	if got := spanAttributeInt(spans[0].Attributes(), "vectis.task.children.dispatchable"); got != 2 {
		t.Fatalf("dispatchable children attr: got %d, want 2", got)
	}

	if got := spanAttributeBool(spans[0].Attributes(), "vectis.task.fanout.activation_point"); !got {
		t.Fatalf("fanout activation point attr: got false, want true")
	}

	events := spans[0].Events()
	if len(events) != 1 || events[0].Name != "task.complete" {
		t.Fatalf("events: %+v", events)
	}

	if got := spanAttributeInt(events[0].Attributes, "vectis.task.children.activated"); got != 2 {
		t.Fatalf("event activated children attr: got %d, want 2", got)
	}
}

func spanAttributeString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}

	return ""
}

func spanAttributeInt(attrs []attribute.KeyValue, key string) int64 {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsInt64()
		}
	}

	return 0
}

func spanAttributeBool(attrs []attribute.KeyValue, key string) bool {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsBool()
		}
	}

	return false
}
