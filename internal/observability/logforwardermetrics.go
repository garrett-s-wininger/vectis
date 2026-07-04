package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type LogForwarderMetrics struct {
	chunksReceived metric.Int64Counter
	batches        metric.Int64Counter
}

func NewLogForwarderMetrics() (*LogForwarderMetrics, error) {
	m := otel.Meter("vectis/log-forwarder")

	chunks, err := m.Int64Counter("vectis_log_forwarder_chunks_received_total",
		metric.WithDescription("Log chunks received from local producers by shard-route hint state"),
		metric.WithUnit("{chunk}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_log_forwarder_chunks_received_total: %w", err)
	}

	batches, err := m.Int64Counter("vectis_log_forwarder_batches_total",
		metric.WithDescription("Log-forwarder batch outcomes"),
		metric.WithUnit("{batch}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_log_forwarder_batches_total: %w", err)
	}

	return &LogForwarderMetrics{
		chunksReceived: chunks,
		batches:        batches,
	}, nil
}

func (m *LogForwarderMetrics) RecordChunkReceived(ctx context.Context, route string) {
	if m == nil {
		return
	}

	if route == "" {
		route = "unknown"
	}

	m.chunksReceived.Add(ctx, 1, metric.WithAttributes(attribute.String("route", route)))
}

func (m *LogForwarderMetrics) RecordBatch(ctx context.Context, outcome string) {
	if m == nil {
		return
	}

	if outcome == "" {
		outcome = "unknown"
	}

	m.batches.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

func RegisterLogForwarderSpoolGauges(snapshot func() (files int64, oldestAgeSeconds int64)) error {
	if snapshot == nil {
		return fmt.Errorf("RegisterLogForwarderSpoolGauges: snapshot is nil")
	}

	m := otel.Meter("vectis/log-forwarder")

	filesG, err := m.Int64ObservableGauge("vectis_log_forwarder_spool_files",
		metric.WithDescription("Log-forwarder durable spool files waiting to drain"),
		metric.WithUnit("{file}"))
	if err != nil {
		return fmt.Errorf("vectis_log_forwarder_spool_files: %w", err)
	}

	oldestG, err := m.Int64ObservableGauge("vectis_log_forwarder_spool_oldest_age_seconds",
		metric.WithDescription("Age of the oldest log-forwarder durable spool file"),
		metric.WithUnit("s"))
	if err != nil {
		return fmt.Errorf("vectis_log_forwarder_spool_oldest_age_seconds: %w", err)
	}

	_, err = m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		files, oldestAge := snapshot()
		o.ObserveInt64(filesG, files)
		o.ObserveInt64(oldestG, oldestAge)
		return nil
	}, filesG, oldestG)
	if err != nil {
		return fmt.Errorf("register log-forwarder spool gauge callback: %w", err)
	}

	return nil
}
