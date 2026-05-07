package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type AuditMetrics struct {
	dropped       metric.Int64Counter
	flushFailures metric.Int64Counter
}

func NewAuditMetrics() (*AuditMetrics, error) {
	m := otel.Meter("vectis/api/audit")

	dropped, err := m.Int64Counter("vectis_audit_events_dropped_total",
		metric.WithDescription("Audit events dropped before persistence"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_audit_events_dropped_total: %w", err)
	}

	flushFailures, err := m.Int64Counter("vectis_audit_flush_failures_total",
		metric.WithDescription("Failed audit flush attempts"),
		metric.WithUnit("{failure}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_audit_flush_failures_total: %w", err)
	}

	return &AuditMetrics{
		dropped:       dropped,
		flushFailures: flushFailures,
	}, nil
}

func (m *AuditMetrics) RecordDropped(ctx context.Context, eventType string) {
	if m == nil {
		return
	}

	m.dropped.Add(ctx, 1, metric.WithAttributes(attribute.String("event_type", eventType)))
}

func (m *AuditMetrics) RecordFlushFailure(ctx context.Context, eventCount int) {
	if m == nil {
		return
	}

	m.flushFailures.Add(ctx, 1, metric.WithAttributes(attribute.Int("event_count", eventCount)))
}
