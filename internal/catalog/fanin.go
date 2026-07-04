package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

type FanInSourceResult struct {
	CellID     string
	Backfilled int
	Read       int
	Copied     int
}

type FanInMetrics interface {
	RecordFanInSourceResult(ctx context.Context, result FanInSourceResult)
}

type FanInProcessor struct {
	target  dal.CatalogEventsRepository
	sources []FanInSource
	metrics FanInMetrics
}

func NewFanInProcessor(target dal.CatalogEventsRepository, sources []FanInSource) *FanInProcessor {
	return &FanInProcessor{
		target:  target,
		sources: append([]FanInSource(nil), sources...),
	}
}

func (p *FanInProcessor) SetMetrics(metrics FanInMetrics) {
	p.metrics = metrics
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

		sourceCellID := strings.TrimSpace(source.CellID)
		if sourceCellID == "" {
			return result, errors.New("catalog fan-in source cell_id is required")
		}

		sourceResult := FanInSourceResult{CellID: source.CellID}
		if source.Backfill != nil {
			backfillResult, err := source.Backfill.RepairMissing(ctx, remaining)
			if err != nil {
				return result, fmt.Errorf("backfill missing catalog events for cell %q: %w", source.CellID, err)
			}

			backfilled := backfillResult.Total()
			result.Backfilled += backfilled
			sourceResult.Backfilled += backfilled
		}

		if source.Events == nil {
			return result, fmt.Errorf("catalog fan-in source %q has no events repository", source.CellID)
		}

		records, err := source.Events.ListPending(ctx, remaining)
		if err != nil {
			return result, fmt.Errorf("list pending catalog events for cell %q: %w", source.CellID, err)
		}

		result.Read += len(records)
		sourceResult.Read += len(records)
		for _, rec := range records {
			if err := validateSourceCatalogEventForFanIn(sourceCellID, rec); err != nil {
				if markErr := source.Events.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
					return result, fmt.Errorf("mark spoofed source catalog event %d from cell %q failed: %w", rec.ID, source.CellID, markErr)
				}

				remaining--
				if remaining <= 0 {
					break
				}

				continue
			}

			target, created, err := p.target.Record(ctx, rec.SourceCell, rec.EventKey, rec.EventType, rec.Payload)
			if err != nil {
				return result, fmt.Errorf("copy catalog event %q from cell %q: %w", rec.EventKey, source.CellID, err)
			} else if created {
				result.Copied++
				sourceResult.Copied++
			}

			if err := validateTargetCatalogEventForSourceAck(target); err != nil {
				return result, fmt.Errorf("target catalog event %q from cell %q is not ackable: %w", rec.EventKey, source.CellID, err)
			}

			if err := source.Events.MarkApplied(ctx, rec.ID); err != nil {
				return result, fmt.Errorf("mark source catalog event %d from cell %q applied: %w", rec.ID, source.CellID, err)
			}

			remaining--
			if remaining <= 0 {
				break
			}
		}

		if p.metrics != nil && (sourceResult.Backfilled > 0 || sourceResult.Read > 0 || sourceResult.Copied > 0) {
			p.metrics.RecordFanInSourceResult(ctx, sourceResult)
		}
	}

	return result, nil
}

func validateSourceCatalogEventForFanIn(sourceCellID string, rec dal.CatalogEventRecord) error {
	sourceCellID = strings.TrimSpace(sourceCellID)
	eventSourceCell := strings.TrimSpace(rec.SourceCell)
	if eventSourceCell == "" {
		return errors.New("catalog event source_cell is required")
	}

	if eventSourceCell != sourceCellID {
		return fmt.Errorf("catalog event source_cell %q does not match configured source %q", eventSourceCell, sourceCellID)
	}

	return nil
}

func validateTargetCatalogEventForSourceAck(rec dal.CatalogEventRecord) error {
	switch rec.Status {
	case dal.CatalogEventStatusPending, dal.CatalogEventStatusApplied:
		return nil
	case dal.CatalogEventStatusFailed:
		return fmt.Errorf("status %q", rec.Status)
	default:
		return fmt.Errorf("unknown status %q", rec.Status)
	}
}
