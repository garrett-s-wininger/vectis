package observability

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/taskdispatch"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	TaskDispatchDrainOutcomeSuccess        = "success"
	TaskDispatchDrainOutcomePartialFailure = "partial_failure"
	TaskDispatchDrainOutcomeError          = "error"

	TaskDispatchIntentOutcomeListed   = "listed"
	TaskDispatchIntentOutcomeEnqueued = "enqueued"
	TaskDispatchIntentOutcomeFailed   = "failed"

	taskDispatchScopeCell = "cell"
	taskDispatchScopeRun  = "run"
)

type TaskDispatchMetrics struct {
	drains  metric.Int64Counter
	intents metric.Int64Counter
}

func NewTaskDispatchMetrics() (*TaskDispatchMetrics, error) {
	m := otel.Meter("vectis/task-dispatch")

	drains, err := m.Int64Counter("vectis_task_dispatch_drains_total",
		metric.WithDescription("Total task dispatch drain attempts by scope, target cell, and outcome"),
		metric.WithUnit("{drain}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_task_dispatch_drains_total: %w", err)
	}

	intents, err := m.Int64Counter("vectis_task_dispatch_intents_total",
		metric.WithDescription("Total task dispatch intents observed by scope, target cell, and outcome"),
		metric.WithUnit("{intent}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_task_dispatch_intents_total: %w", err)
	}

	return &TaskDispatchMetrics{
		drains:  drains,
		intents: intents,
	}, nil
}

func (m *TaskDispatchMetrics) RecordDrain(ctx context.Context, opts taskdispatch.DrainOptions, result taskdispatch.DrainResult, drainErr error) {
	if m == nil {
		return
	}

	attrs := metric.WithAttributes(
		attribute.String("scope", taskDispatchScope(opts)),
		attribute.String("target_cell", taskDispatchTargetCell(opts.CellID)),
	)

	m.drains.Add(ctx, 1, metric.WithAttributes(
		attribute.String("scope", taskDispatchScope(opts)),
		attribute.String("target_cell", taskDispatchTargetCell(opts.CellID)),
		attribute.String("outcome", taskDispatchDrainOutcome(result, drainErr)),
	))

	if result.Listed > 0 {
		m.intents.Add(ctx, int64(result.Listed), attrs, metric.WithAttributes(attribute.String("outcome", TaskDispatchIntentOutcomeListed)))
	}

	if result.Enqueued > 0 {
		m.intents.Add(ctx, int64(result.Enqueued), attrs, metric.WithAttributes(attribute.String("outcome", TaskDispatchIntentOutcomeEnqueued)))
	}

	if result.Failed > 0 {
		m.intents.Add(ctx, int64(result.Failed), attrs, metric.WithAttributes(attribute.String("outcome", TaskDispatchIntentOutcomeFailed)))
	}
}

func taskDispatchScope(opts taskdispatch.DrainOptions) string {
	if strings.TrimSpace(opts.RunID) != "" {
		return taskDispatchScopeRun
	}

	return taskDispatchScopeCell
}

func taskDispatchTargetCell(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return "unknown"
	}

	return cellID
}

func taskDispatchDrainOutcome(result taskdispatch.DrainResult, drainErr error) string {
	if drainErr != nil {
		return TaskDispatchDrainOutcomeError
	}

	if result.Failed > 0 {
		return TaskDispatchDrainOutcomePartialFailure
	}

	return TaskDispatchDrainOutcomeSuccess
}
