package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	APIEnqueueRunKindStored    = "stored"
	APIEnqueueRunKindEphemeral = "ephemeral"

	APIEnqueueOutcomeAccepted            = "accepted"
	APIEnqueueOutcomeAttempt             = "attempt"
	APIEnqueueOutcomeSuccess             = "success"
	APIEnqueueOutcomeFailedEnqueue       = "failed_enqueue"
	APIEnqueueOutcomeFailedTouchDispatch = "failed_touch_dispatched"
)

type APIDispatchMetrics struct {
	enqueue metric.Int64Counter
}

func NewAPIDispatchMetrics() (*APIDispatchMetrics, error) {
	m := otel.Meter("vectis/api")

	enqueue, err := m.Int64Counter("vectis_api_run_enqueue_total",
		metric.WithDescription("Total API run enqueue transitions by run kind and outcome"),
		metric.WithUnit("{run}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_api_run_enqueue_total: %w", err)
	}

	return &APIDispatchMetrics{enqueue: enqueue}, nil
}

func (m *APIDispatchMetrics) RecordRunEnqueue(ctx context.Context, runKind, outcome string) {
	if m == nil || runKind == "" || outcome == "" {
		return
	}

	m.enqueue.Add(ctx, 1, metric.WithAttributes(
		attribute.String("run_kind", runKind),
		attribute.String("outcome", outcome),
	))
}
