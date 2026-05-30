package observability

import (
	"context"
	"fmt"

	"vectis/internal/catalog"
	"vectis/internal/cell"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type CatalogMetrics struct {
	read            metric.Int64Counter
	applied         metric.Int64Counter
	failed          metric.Int64Counter
	processErrors   metric.Int64Counter
	fanInRead       metric.Int64Counter
	fanInCopied     metric.Int64Counter
	fanInBackfilled metric.Int64Counter
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

	fanInRead, err := m.Int64Counter("vectis_catalog_fanin_events_read_total",
		metric.WithDescription("Total pending cell catalog events read from fan-in source databases"),
		metric.WithUnit("{event}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_fanin_events_read_total: %w", err)
	}

	fanInCopied, err := m.Int64Counter("vectis_catalog_fanin_events_copied_total",
		metric.WithDescription("Total cell catalog events copied from fan-in source databases into the global inbox"),
		metric.WithUnit("{event}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_fanin_events_copied_total: %w", err)
	}

	fanInBackfilled, err := m.Int64Counter("vectis_catalog_fanin_events_backfilled_total",
		metric.WithDescription("Total missing cell catalog events synthesized before fan-in copy"),
		metric.WithUnit("{event}"))

	if err != nil {
		return nil, fmt.Errorf("vectis_catalog_fanin_events_backfilled_total: %w", err)
	}

	return &CatalogMetrics{
		read:            read,
		applied:         applied,
		failed:          failed,
		processErrors:   processErrors,
		fanInRead:       fanInRead,
		fanInCopied:     fanInCopied,
		fanInBackfilled: fanInBackfilled,
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

func (cm *CatalogMetrics) RecordFanInSourceResult(ctx context.Context, result catalog.FanInSourceResult) {
	if cm == nil {
		return
	}

	attrs := metric.WithAttributes(attribute.String("source_cell", result.CellID))
	if result.Read > 0 {
		cm.fanInRead.Add(ctx, int64(result.Read), attrs)
	}

	if result.Copied > 0 {
		cm.fanInCopied.Add(ctx, int64(result.Copied), attrs)
	}

	if result.Backfilled > 0 {
		cm.fanInBackfilled.Add(ctx, int64(result.Backfilled), attrs)
	}
}
