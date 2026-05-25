package observability

import (
	"context"
	"fmt"

	"vectis/internal/cell"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type CatalogMetrics struct {
	read          metric.Int64Counter
	applied       metric.Int64Counter
	failed        metric.Int64Counter
	processErrors metric.Int64Counter
}

func NewCatalogMetrics() (*CatalogMetrics, error) {
	m := otel.Meter("vectis/catalog")

	read, err := m.Int64Counter("vectis_catalog_events_read_total",
		metric.WithDescription("Total pending cell catalog events read from the inbox"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_events_read_total: %w", err)
	}

	applied, err := m.Int64Counter("vectis_catalog_events_applied_total",
		metric.WithDescription("Total cell catalog events applied to the global catalog"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_events_applied_total: %w", err)
	}

	failed, err := m.Int64Counter("vectis_catalog_events_failed_total",
		metric.WithDescription("Total cell catalog events marked failed while applying to the global catalog"),
		metric.WithUnit("{event}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_events_failed_total: %w", err)
	}

	processErrors, err := m.Int64Counter("vectis_catalog_process_errors_total",
		metric.WithDescription("Total catalog inbox process attempts that failed before event-level status updates completed"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_process_errors_total: %w", err)
	}

	return &CatalogMetrics{
		read:          read,
		applied:       applied,
		failed:        failed,
		processErrors: processErrors,
	}, nil
}

func (cm *CatalogMetrics) RecordProcessResult(ctx context.Context, result cell.CatalogInboxProcessResult) {
	if cm == nil {
		return
	}

	if result.Read > 0 {
		cm.read.Add(ctx, int64(result.Read))
	}

	if result.Applied > 0 {
		cm.applied.Add(ctx, int64(result.Applied))
	}

	if result.Failed > 0 {
		cm.failed.Add(ctx, int64(result.Failed))
	}
}

func (cm *CatalogMetrics) RecordProcessError(ctx context.Context) {
	if cm == nil {
		return
	}

	cm.processErrors.Add(ctx, 1)
}
