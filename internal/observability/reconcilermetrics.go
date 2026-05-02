package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	ReconcilerOutcomeSuccess              = "success"
	ReconcilerOutcomeSkippedPolicy        = "skipped_policy"
	ReconcilerOutcomeSkippedMissingJobDef = "skipped_missing_job_definition"
	ReconcilerOutcomeFailedLoadJobDef     = "failed_load_job_definition"
	ReconcilerOutcomeFailedParseJobDef    = "failed_parse_job_definition"
	ReconcilerOutcomeFailedEnqueue        = "failed_enqueue"
	ReconcilerOutcomeFailedTouchRun       = "failed_touch_dispatched"
)

type ReconcilerMetrics struct {
	scanned   metric.Int64Counter
	reenqueue metric.Int64Counter
}

func NewReconcilerMetrics() (*ReconcilerMetrics, error) {
	m := otel.Meter("vectis/reconciler")

	scanned, err := m.Int64Counter("vectis_reconciler_runs_scanned_total",
		metric.WithDescription("Total queued runs considered for reconcile dispatch"),
		metric.WithUnit("{run}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_reconciler_runs_scanned_total: %w", err)
	}

	reenqueue, err := m.Int64Counter("vectis_reconciler_reenqueue_total",
		metric.WithDescription("Total reconcile redispatch outcomes by result"),
		metric.WithUnit("{run}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_reconciler_reenqueue_total: %w", err)
	}

	return &ReconcilerMetrics{
		scanned:   scanned,
		reenqueue: reenqueue,
	}, nil
}

func (rm *ReconcilerMetrics) RecordRunsScanned(ctx context.Context, count int) {
	if rm == nil || count <= 0 {
		return
	}
	rm.scanned.Add(ctx, int64(count))
}

func (rm *ReconcilerMetrics) RecordReenqueueOutcome(ctx context.Context, outcome string) {
	if rm == nil || outcome == "" {
		return
	}
	rm.reenqueue.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
