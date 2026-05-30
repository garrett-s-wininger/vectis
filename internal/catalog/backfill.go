package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"vectis/internal/cell"
	"vectis/internal/dal"
)

type BackfillResult struct {
	RunEvents       int
	ExecutionEvents int
}

func (r BackfillResult) Total() int {
	return r.RunEvents + r.ExecutionEvents
}

type BackfillProcessor struct {
	sourceCellID string
	state        dal.CatalogStatusBackfillRepository
	publisher    cell.CatalogEventPublisher
}

func NewBackfillProcessor(sourceCellID string, state dal.CatalogStatusBackfillRepository, publisher cell.CatalogEventPublisher) *BackfillProcessor {
	return &BackfillProcessor{
		sourceCellID: strings.TrimSpace(sourceCellID),
		state:        state,
		publisher:    publisher,
	}
}

func (p *BackfillProcessor) RepairMissing(ctx context.Context, limit int) (BackfillResult, error) {
	if p == nil {
		return BackfillResult{}, nil
	}

	if p.state == nil {
		return BackfillResult{}, errors.New("catalog backfill state repository is required")
	}

	if strings.TrimSpace(p.sourceCellID) == "" {
		return BackfillResult{}, errors.New("catalog backfill source cell is required")
	}

	if limit <= 0 {
		limit = DefaultBatchSize
	}

	result := BackfillResult{}
	remaining := limit
	runEvents, err := p.state.ListMissingRunStatusCatalogEvents(ctx, p.sourceCellID, remaining)
	if err != nil {
		return result, fmt.Errorf("list missing run status catalog events: %w", err)
	}

	for _, update := range runEvents {
		if err := p.publisher.RecordRunStatus(ctx, update); err != nil {
			return result, fmt.Errorf("record run status catalog event for %s: %w", update.RunID, err)
		}

		result.RunEvents++
		remaining--
		if remaining <= 0 {
			return result, nil
		}
	}

	executionEvents, err := p.state.ListMissingExecutionStatusCatalogEvents(ctx, p.sourceCellID, remaining)
	if err != nil {
		return result, fmt.Errorf("list missing execution status catalog events: %w", err)
	}

	for _, update := range executionEvents {
		if err := p.publisher.RecordExecutionStatus(ctx, update); err != nil {
			return result, fmt.Errorf("record execution status catalog event for %s: %w", update.ExecutionID, err)
		}

		result.ExecutionEvents++
		remaining--
		if remaining <= 0 {
			break
		}
	}

	return result, nil
}
