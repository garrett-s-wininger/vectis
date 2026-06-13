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
	RunEvents               int
	ExecutionEvents         int
	ExecutionSecurityEvents int
}

func (r BackfillResult) Total() int {
	return r.RunEvents + r.ExecutionEvents + r.ExecutionSecurityEvents
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

	if remaining <= 0 {
		return result, nil
	}

	securityEvents, err := p.state.ListMissingExecutionSecurityCatalogEvents(ctx, p.sourceCellID, remaining)
	if err != nil {
		return result, fmt.Errorf("list missing execution security catalog events: %w", err)
	}

	for _, event := range securityEvents {
		if err := p.publisher.RecordExecutionSecurity(ctx, executionSecurityEventRecordParams(event)); err != nil {
			return result, fmt.Errorf("record execution security catalog event for %s: %w", event.ExecutionID, err)
		}

		result.ExecutionSecurityEvents++
		remaining--
		if remaining <= 0 {
			break
		}
	}

	return result, nil
}

func executionSecurityEventRecordParams(event dal.ExecutionSecurityEvent) dal.RecordExecutionSecurityEventParams {
	provider := ""
	if event.Provider != nil {
		provider = *event.Provider
	}

	return dal.RecordExecutionSecurityEventParams{
		EventKey:      event.EventKey,
		RunID:         event.RunID,
		TaskID:        event.TaskID,
		TaskAttemptID: event.TaskAttemptID,
		ExecutionID:   event.ExecutionID,
		EventType:     event.EventType,
		Outcome:       event.Outcome,
		Reason:        event.Reason,
		Provider:      provider,
		SecretCount:   event.SecretCount,
		FileCount:     event.FileCount,
		CreatedAt:     event.CreatedAt,
	}
}
