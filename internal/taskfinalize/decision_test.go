package taskfinalize_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestDecide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		continued bool
		reduce    taskreduce.Decision
		want      taskfinalize.Outcome
	}{
		{
			name:      "continuation queued",
			continued: true,
			want:      taskfinalize.OutcomeContinue,
		},
		{
			name:   "reduced succeeded",
			reduce: taskreduce.Decision{Outcome: taskreduce.OutcomeSucceeded},
			want:   taskfinalize.OutcomeReduceSucceeded,
		},
		{
			name:   "reduced failed",
			reduce: taskreduce.Decision{Outcome: taskreduce.OutcomeFailed},
			want:   taskfinalize.OutcomeReduceFailed,
		},
		{
			name:   "incomplete",
			reduce: taskreduce.Decision{Outcome: taskreduce.OutcomeWaiting},
			want:   taskfinalize.OutcomeIncomplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := taskfinalize.Decide(tt.continued, tt.reduce).Outcome; got != tt.want {
				t.Fatalf("outcome: got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExecutionTerminalDecisions(t *testing.T) {
	t.Parallel()

	reduce := taskreduce.Decision{
		Outcome: taskreduce.OutcomeFailed,
		Summary: dal.RunTaskCompletion{RunID: "run-failed", Total: 1, TerminalFailed: 1},
	}

	failed := taskfinalize.ExecutionFailed(reduce)
	if failed.Outcome != taskfinalize.OutcomeExecutionFailed || failed.Reduce.Summary.RunID != "run-failed" {
		t.Fatalf("failed decision: %+v", failed)
	}

	aborted := taskfinalize.ExecutionAborted()
	if aborted.Outcome != taskfinalize.OutcomeExecutionAborted || aborted.Reduce.Outcome != "" {
		t.Fatalf("aborted decision: %+v", aborted)
	}
}

func TestFailureReason(t *testing.T) {
	t.Parallel()

	decision := taskfinalize.Decision{
		Outcome: taskfinalize.OutcomeReduceFailed,
		Reduce: taskreduce.Decision{
			Outcome: taskreduce.OutcomeFailed,
			Summary: dal.RunTaskCompletion{TerminalFailed: 3},
		},
	}

	if got, want := taskfinalize.FailureReason(decision), "3 task execution(s) ended in a terminal failure"; got != want {
		t.Fatalf("FailureReason: got %q, want %q", got, want)
	}
}

func TestRecordDecision(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("taskfinalize-test").Start(context.Background(), "finalize")
	decision := taskfinalize.Decision{
		Outcome: taskfinalize.OutcomeReduceFailed,
		Reduce: taskreduce.Decision{
			Outcome: taskreduce.OutcomeFailed,
			Summary: dal.RunTaskCompletion{Total: 4, Succeeded: 2, TerminalFailed: 1, Incomplete: 1},
		},
	}

	taskfinalize.RecordDecision(ctx, decision)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	if got := spanAttributeString(spans[0].Attributes(), "vectis.task.finalize.outcome"); got != string(taskfinalize.OutcomeReduceFailed) {
		t.Fatalf("finalize outcome attr: got %q, want %q", got, taskfinalize.OutcomeReduceFailed)
	}

	events := spans[0].Events()
	if len(events) != 1 || events[0].Name != "task.finalize" {
		t.Fatalf("events: %+v", events)
	}

	if got := spanAttributeInt(events[0].Attributes, "vectis.task.terminal_failed"); got != 1 {
		t.Fatalf("event terminal_failed attr: got %d, want 1", got)
	}
}

func TestRecordDecisionWithoutReduceOmitsReduceAttributes(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("taskfinalize-test").Start(context.Background(), "finalize")
	taskfinalize.RecordDecision(ctx, taskfinalize.ExecutionAborted())
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	if got := spanAttributeString(spans[0].Attributes(), "vectis.task.finalize.outcome"); got != string(taskfinalize.OutcomeExecutionAborted) {
		t.Fatalf("finalize outcome attr: got %q, want %q", got, taskfinalize.OutcomeExecutionAborted)
	}

	if got := spanAttributeString(spans[0].Attributes(), "vectis.task.reduce.outcome"); got != "" {
		t.Fatalf("reduce outcome attr should be omitted, got %q", got)
	}

	events := spans[0].Events()
	if len(events) != 1 || events[0].Name != "task.finalize" {
		t.Fatalf("events: %+v", events)
	}

	if got := spanAttributeString(events[0].Attributes, "vectis.task.reduce.outcome"); got != "" {
		t.Fatalf("event reduce outcome attr should be omitted, got %q", got)
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
