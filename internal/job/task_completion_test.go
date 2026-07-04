package job_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/job"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRecordTaskCompletionRecordsTraceCompletion(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("task-completion-test").Start(context.Background(), "complete")
	job.RecordTaskCompletion(ctx, job.TaskCompletionResult{
		ExecutionID: "execution-root",
		Status:      dal.ExecutionStatusSucceeded,
		Children: []dal.TaskExecutionRecord{
			{TaskID: "run-1:build", TaskKey: "build"},
			{TaskID: "run-1:test", TaskKey: "test"},
		},
		Activated: 2,
	})

	span.End()
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
