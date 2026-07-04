package observability

import (
	"context"
	"fmt"

	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	TaskReduceOutcomeError = "error"

	taskFinalizeReduceOutcomeNone = "none"
)

type TaskFinalizeMetrics struct {
	reduceDecisions   metric.Int64Counter
	finalizeDecisions metric.Int64Counter
}

func NewTaskFinalizeMetrics() (*TaskFinalizeMetrics, error) {
	m := otel.Meter("vectis/task-finalize")

	reduceDecisions, err := m.Int64Counter("vectis_task_reduce_decisions_total",
		metric.WithDescription("Total task reduce decisions by outcome"),
		metric.WithUnit("{decision}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_task_reduce_decisions_total: %w", err)
	}

	finalizeDecisions, err := m.Int64Counter("vectis_task_finalize_decisions_total",
		metric.WithDescription("Total task finalize decisions by outcome and reduce outcome"),
		metric.WithUnit("{decision}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_task_finalize_decisions_total: %w", err)
	}

	return &TaskFinalizeMetrics{
		reduceDecisions:   reduceDecisions,
		finalizeDecisions: finalizeDecisions,
	}, nil
}

func (m *TaskFinalizeMetrics) RecordReduce(ctx context.Context, decision taskreduce.Decision, err error) {
	if m == nil {
		return
	}

	outcome := string(decision.Outcome)
	if err != nil {
		outcome = TaskReduceOutcomeError
	}

	if outcome == "" {
		outcome = TaskReduceOutcomeError
	}

	m.reduceDecisions.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func (m *TaskFinalizeMetrics) RecordFinalize(ctx context.Context, decision taskfinalize.Decision) {
	if m == nil {
		return
	}

	reduceOutcome := string(decision.Reduce.Outcome)
	if reduceOutcome == "" {
		reduceOutcome = taskFinalizeReduceOutcomeNone
	}

	m.finalizeDecisions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", string(decision.Outcome)),
		attribute.String("reduce_outcome", reduceOutcome),
	))
}
