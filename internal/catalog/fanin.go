package catalog

import (
	"context"
	"errors"
	"fmt"

	"vectis/internal/dal"
)

type FanInSource struct {
	CellID   string
	Events   dal.CatalogEventsRepository
	Backfill Backfill
}

type FanInResult struct {
	Sources    int
	Backfilled int
	Read       int
	Copied     int
}

type FanInProcessor struct {
	target  dal.CatalogEventsRepository
	sources []FanInSource
}

func NewFanInProcessor(target dal.CatalogEventsRepository, sources []FanInSource) *FanInProcessor {
	return &FanInProcessor{
		target:  target,
		sources: append([]FanInSource(nil), sources...),
	}
}

func (p *FanInProcessor) IngestPending(ctx context.Context, limit int) (FanInResult, error) {
	if p == nil || len(p.sources) == 0 {
		return FanInResult{}, nil
	}

	if p.target == nil {
		return FanInResult{}, errors.New("catalog fan-in target is required")
	}

	if limit <= 0 {
		limit = DefaultBatchSize
	}

	remaining := limit
	result := FanInResult{Sources: len(p.sources)}
	for _, source := range p.sources {
		if remaining <= 0 {
			break
		}

		if source.Backfill != nil {
			backfillResult, err := source.Backfill.RepairMissing(ctx, remaining)
			if err != nil {
				return result, fmt.Errorf("backfill missing catalog events for cell %q: %w", source.CellID, err)
			}

			result.Backfilled += backfillResult.Total()
		}

		if source.Events == nil {
			return result, fmt.Errorf("catalog fan-in source %q has no events repository", source.CellID)
		}

		records, err := source.Events.ListPending(ctx, remaining)
		if err != nil {
			return result, fmt.Errorf("list pending catalog events for cell %q: %w", source.CellID, err)
		}

		result.Read += len(records)
		for _, rec := range records {
			if _, created, err := p.target.Record(ctx, rec.SourceCell, rec.EventKey, rec.EventType, rec.Payload); err != nil {
				return result, fmt.Errorf("copy catalog event %q from cell %q: %w", rec.EventKey, source.CellID, err)
			} else if created {
				result.Copied++
			}

			if err := source.Events.MarkApplied(ctx, rec.ID); err != nil {
				return result, fmt.Errorf("mark source catalog event %d from cell %q applied: %w", rec.ID, source.CellID, err)
			}

			remaining--
			if remaining <= 0 {
				break
			}
		}
	}

	return result, nil
}
