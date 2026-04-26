package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type QueueMetrics struct {
	enqueued    metric.Int64Counter
	dequeued    metric.Int64Counter
	dlqMoved    metric.Int64Counter
	dlqRequeued metric.Int64Counter
}

func NewQueueMetrics() (*QueueMetrics, error) {
	m := otel.Meter("vectis/queue")

	enqueued, err := m.Int64Counter("vectis_queue_enqueued_total",
		metric.WithDescription("Total jobs enqueued"),
		metric.WithUnit("{job}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_queue_enqueued_total: %w", err)
	}

	dequeued, err := m.Int64Counter("vectis_queue_dequeued_total",
		metric.WithDescription("Total jobs dequeued"),
		metric.WithUnit("{job}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_queue_dequeued_total: %w", err)
	}

	dlqMoved, err := m.Int64Counter("vectis_queue_dlq_moved_total",
		metric.WithDescription("Total deliveries moved to dead letter queue after max requeue attempts"),
		metric.WithUnit("{delivery}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_queue_dlq_moved_total: %w", err)
	}

	dlqRequeued, err := m.Int64Counter("vectis_queue_dlq_requeued_total",
		metric.WithDescription("Total dead letter items requeued back to the main queue"),
		metric.WithUnit("{delivery}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_queue_dlq_requeued_total: %w", err)
	}

	return &QueueMetrics{
		enqueued:    enqueued,
		dequeued:    dequeued,
		dlqMoved:    dlqMoved,
		dlqRequeued: dlqRequeued,
	}, nil
}

func (qm *QueueMetrics) RecordEnqueued(ctx context.Context) {
	if qm == nil {
		return
	}
	qm.enqueued.Add(ctx, 1)
}

func (qm *QueueMetrics) RecordDequeued(ctx context.Context) {
	if qm == nil {
		return
	}
	qm.dequeued.Add(ctx, 1)
}

func (qm *QueueMetrics) RecordDLQMoved(ctx context.Context) {
	if qm == nil {
		return
	}
	qm.dlqMoved.Add(ctx, 1)
}

func (qm *QueueMetrics) RecordDLQRequeued(ctx context.Context) {
	if qm == nil {
		return
	}
	qm.dlqRequeued.Add(ctx, 1)
}

func RegisterQueueGauges(snapshot func() (pending int64, inflight int64, dlq int64)) error {
	if snapshot == nil {
		return fmt.Errorf("RegisterQueueGauges: snapshot is nil")
	}

	m := otel.Meter("vectis/queue")

	pendingG, err := m.Int64ObservableGauge("vectis_queue_jobs_pending",
		metric.WithDescription("Jobs waiting in the in-memory queue (not yet delivered to a worker)"),
		metric.WithUnit("{job}"))
	if err != nil {
		return fmt.Errorf("vectis_queue_jobs_pending: %w", err)
	}

	inflightG, err := m.Int64ObservableGauge("vectis_queue_deliveries_inflight",
		metric.WithDescription("Deliveries handed to workers but not yet acked (lease not expired)"),
		metric.WithUnit("{delivery}"))
	if err != nil {
		return fmt.Errorf("vectis_queue_deliveries_inflight: %w", err)
	}

	dlqG, err := m.Int64ObservableGauge("vectis_queue_dlq_size",
		metric.WithDescription("Items currently in the dead letter queue"),
		metric.WithUnit("{item}"))
	if err != nil {
		return fmt.Errorf("vectis_queue_dlq_size: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		pending, inflight, dlq := snapshot()
		o.ObserveInt64(pendingG, pending)
		o.ObserveInt64(inflightG, inflight)
		o.ObserveInt64(dlqG, dlq)
		return nil
	}, pendingG, inflightG, dlqG)

	if err != nil {
		return fmt.Errorf("register queue gauge callback: %w", err)
	}

	return nil
}
