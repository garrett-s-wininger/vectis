package observability

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	WorkerOutcomeSuccess          = "success"
	WorkerOutcomeFailed           = "failed"
	WorkerOutcomeSkippedUnclaimed = "skipped_unclaimed"
)

type WorkerMetrics struct {
	received metric.Int64Counter
	duration metric.Float64Histogram
}

func NewWorkerMetrics() (*WorkerMetrics, error) {
	m := otel.Meter("vectis/worker")

	received, err := m.Int64Counter("vectis_worker_jobs_received_total",
		metric.WithDescription("Jobs dequeued and passed to the worker handler"),
		metric.WithUnit("{job}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_jobs_received_total: %w", err)
	}

	duration, err := m.Float64Histogram("vectis_worker_job_duration_seconds",
		metric.WithDescription("Wall time from job handler entry to terminal outcome"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300))

	if err != nil {
		return nil, fmt.Errorf("vectis_worker_job_duration_seconds: %w", err)
	}

	return &WorkerMetrics{received: received, duration: duration}, nil
}

func (wm *WorkerMetrics) RecordJobReceived(ctx context.Context) {
	if wm == nil {
		return
	}

	wm.received.Add(ctx, 1)
}

func (wm *WorkerMetrics) RecordJobFinished(ctx context.Context, outcome string, d time.Duration) {
	if wm == nil || outcome == "" {
		return
	}

	attrs := metric.WithAttributes(attribute.String("outcome", outcome))
	wm.duration.Record(ctx, d.Seconds(), attrs)
}
