package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func RegisterQueueGauges(snapshot func() (pending int64, inflight int64)) error {
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

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		pending, inflight := snapshot()
		o.ObserveInt64(pendingG, pending)
		o.ObserveInt64(inflightG, inflight)
		return nil
	}, pendingG, inflightG)

	if err != nil {
		return fmt.Errorf("register queue gauge callback: %w", err)
	}

	return nil
}
