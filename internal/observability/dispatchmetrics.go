package observability

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type DispatchMetrics struct {
	events metric.Int64Counter
}

func NewDispatchMetrics() (*DispatchMetrics, error) {
	m := otel.Meter("vectis/dispatch")

	events, err := m.Int64Counter("vectis_run_dispatch_events_total",
		metric.WithDescription("Total run dispatch events recorded by source, event type, and target cell"),
		metric.WithUnit("{event}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_run_dispatch_events_total: %w", err)
	}

	return &DispatchMetrics{events: events}, nil
}

func (dm *DispatchMetrics) RecordDispatchEvent(ctx context.Context, source, eventType, targetCell string) {
	if dm == nil || strings.TrimSpace(source) == "" || strings.TrimSpace(eventType) == "" {
		return
	}

	targetCell = strings.TrimSpace(targetCell)
	if targetCell == "" {
		targetCell = "unknown"
	}

	dm.events.Add(ctx, 1, metric.WithAttributes(
		attribute.String("source", source),
		attribute.String("event_type", eventType),
		attribute.String("target_cell", targetCell),
	))
}
