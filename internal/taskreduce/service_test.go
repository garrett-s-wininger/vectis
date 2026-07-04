package taskreduce_test

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/taskreduce"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

func TestServiceProcessRecordsTraceDecision(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("taskreduce-test").Start(context.Background(), "reduce")
	runner := &recordingReduceRunner{
		decision: taskreduce.Decision{
			Outcome: taskreduce.OutcomeFailed,
			Summary: dal.RunTaskCompletion{RunID: "run-1", Total: 3, Succeeded: 1, TerminalFailed: 1, Incomplete: 1},
		},
	}

	if _, err := taskreduce.NewService(runner).Process(ctx, "run-1"); err != nil {
		t.Fatalf("Process: %v", err)
	}
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	if got := spanAttributeString(spans[0].Attributes(), "vectis.task.reduce.outcome"); got != string(taskreduce.OutcomeFailed) {
		t.Fatalf("reduce outcome attr: got %q, want %q", got, taskreduce.OutcomeFailed)
	}

	if got := spanAttributeInt(spans[0].Attributes(), "vectis.task.total"); got != 3 {
		t.Fatalf("task total attr: got %d, want 3", got)
	}

	events := spans[0].Events()
	if len(events) != 1 || events[0].Name != "task.reduce" {
		t.Fatalf("events: %+v", events)
	}

	if got := spanAttributeInt(events[0].Attributes, "vectis.task.terminal_failed"); got != 1 {
		t.Fatalf("event terminal_failed attr: got %d, want 1", got)
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
