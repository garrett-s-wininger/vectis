package taskfinalize

import (
	"context"
	"fmt"

	"vectis/internal/taskreduce"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Outcome string

const (
	OutcomeContinue        Outcome = "continue"
	OutcomeReduceSucceeded Outcome = "reduce_succeeded"
	OutcomeReduceFailed    Outcome = "reduce_failed"
	OutcomeIncomplete      Outcome = "incomplete"
)

type Decision struct {
	Outcome Outcome
	Reduce  taskreduce.Decision
}

func Decide(continued bool, reduce taskreduce.Decision) Decision {
	if continued {
		return Decision{Outcome: OutcomeContinue, Reduce: reduce}
	}

	switch reduce.Outcome {
	case taskreduce.OutcomeSucceeded:
		return Decision{Outcome: OutcomeReduceSucceeded, Reduce: reduce}
	case taskreduce.OutcomeFailed:
		return Decision{Outcome: OutcomeReduceFailed, Reduce: reduce}
	default:
		return Decision{Outcome: OutcomeIncomplete, Reduce: reduce}
	}
}

func FailureReason(decision Decision) string {
	return fmt.Sprintf("%d task execution(s) ended in a terminal failure", decision.Reduce.Summary.TerminalFailed)
}

func RecordDecision(ctx context.Context, decision Decision) {
	span := trace.SpanFromContext(ctx)
	attrs := DecisionAttributes(decision)
	span.SetAttributes(attrs...)
	span.AddEvent("task.finalize", trace.WithAttributes(attrs...))
}

func DecisionAttributes(decision Decision) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("vectis.task.finalize.outcome", string(decision.Outcome)),
	}

	if decision.Outcome == OutcomeContinue {
		return attrs
	}

	return append(attrs,
		attribute.String("vectis.task.reduce.outcome", string(decision.Reduce.Outcome)),
		attribute.Int("vectis.task.total", decision.Reduce.Summary.Total),
		attribute.Int("vectis.task.succeeded", decision.Reduce.Summary.Succeeded),
		attribute.Int("vectis.task.terminal_failed", decision.Reduce.Summary.TerminalFailed),
		attribute.Int("vectis.task.incomplete", decision.Reduce.Summary.Incomplete),
	)
}
