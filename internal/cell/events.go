package cell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

var ErrInvalidCatalogEvent = errors.New("invalid catalog event")

const (
	CatalogEventTypeRunStatus       = "run.status"
	CatalogEventTypeExecutionStatus = "execution.status"
)

type CatalogEvent struct {
	SourceCellID    string
	RunStatus       *dal.RunStatusUpdate
	ExecutionStatus *dal.ExecutionStatusUpdate
}

type CatalogEventConsumer struct {
	updater dal.RunCatalogUpdater
}

type CatalogEventPublisher struct {
	sourceCellID string
	events       dal.CatalogEventsRepository
}

type CatalogInboxProcessor struct {
	events   dal.CatalogEventsRepository
	consumer CatalogEventConsumer
}

type CatalogInboxProcessResult struct {
	Read    int
	Applied int
	Failed  int
}

func NewCatalogEventConsumer(updater dal.RunCatalogUpdater) CatalogEventConsumer {
	return CatalogEventConsumer{updater: updater}
}

func NewCatalogEventPublisher(sourceCellID string, events dal.CatalogEventsRepository) CatalogEventPublisher {
	return CatalogEventPublisher{
		sourceCellID: strings.TrimSpace(sourceCellID),
		events:       events,
	}
}

func NewCatalogInboxProcessor(events dal.CatalogEventsRepository, updater dal.RunCatalogUpdater) CatalogInboxProcessor {
	return CatalogInboxProcessor{
		events:   events,
		consumer: NewCatalogEventConsumer(updater),
	}
}

func (p CatalogEventPublisher) RecordRunStatus(ctx context.Context, update dal.RunStatusUpdate) error {
	if p.events == nil {
		return nil
	}

	payload, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal run status catalog event: %w", err)
	}

	if strings.TrimSpace(update.RunID) == "" || strings.TrimSpace(update.Status) == "" {
		return fmt.Errorf("%w: run_id and status are required", ErrInvalidCatalogEvent)
	}

	return p.record(ctx, CatalogRunStatusEventKey(update.RunID, update.Status), CatalogEventTypeRunStatus, payload)
}

func (p CatalogEventPublisher) RecordExecutionStatus(ctx context.Context, update dal.ExecutionStatusUpdate) error {
	if p.events == nil {
		return nil
	}

	payload, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal execution status catalog event: %w", err)
	}

	if strings.TrimSpace(update.ExecutionID) == "" || strings.TrimSpace(update.Status) == "" {
		return fmt.Errorf("%w: execution_id and status are required", ErrInvalidCatalogEvent)
	}

	return p.record(ctx, CatalogExecutionStatusEventKey(update.ExecutionID, update.Status), CatalogEventTypeExecutionStatus, payload)
}

func (p CatalogEventPublisher) record(ctx context.Context, eventKey, eventType string, payload []byte) error {
	sourceCellID := strings.TrimSpace(p.sourceCellID)
	if sourceCellID == "" {
		return fmt.Errorf("%w: source cell is required", ErrInvalidCatalogEvent)
	}

	_, _, err := p.events.Record(ctx, sourceCellID, eventKey, eventType, payload)
	return err
}

func CatalogRunStatusEventKey(runID, status string) string {
	return "run:" + strings.TrimSpace(runID) + ":" + strings.TrimSpace(status)
}

func CatalogExecutionStatusEventKey(executionID, status string) string {
	return "execution:" + strings.TrimSpace(executionID) + ":" + strings.TrimSpace(status)
}

func (p CatalogInboxProcessor) ProcessPending(ctx context.Context, limit int) (CatalogInboxProcessResult, error) {
	if p.events == nil {
		return CatalogInboxProcessResult{}, errors.New("catalog events repository is required")
	}

	records, err := p.events.ListPending(ctx, limit)
	if err != nil {
		return CatalogInboxProcessResult{}, err
	}

	result := CatalogInboxProcessResult{Read: len(records)}
	for _, rec := range records {
		event, err := CatalogEventFromRecord(rec)
		if err == nil {
			err = p.consumer.Apply(ctx, event)
		}

		if err != nil {
			result.Failed++
			if markErr := p.events.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
				return result, fmt.Errorf("mark catalog event %d failed: %w", rec.ID, markErr)
			}

			continue
		}

		if err := p.events.MarkApplied(ctx, rec.ID); err != nil {
			return result, fmt.Errorf("mark catalog event %d applied: %w", rec.ID, err)
		}

		result.Applied++
	}

	return result, nil
}

func CatalogEventFromRecord(rec dal.CatalogEventRecord) (CatalogEvent, error) {
	event := CatalogEvent{SourceCellID: rec.SourceCell}
	switch strings.TrimSpace(rec.EventType) {
	case CatalogEventTypeRunStatus:
		var update dal.RunStatusUpdate
		if err := json.Unmarshal(rec.Payload, &update); err != nil {
			return CatalogEvent{}, fmt.Errorf("%w: decode run status payload: %v", ErrInvalidCatalogEvent, err)
		}

		event.RunStatus = &update
	case CatalogEventTypeExecutionStatus:
		var update dal.ExecutionStatusUpdate
		if err := json.Unmarshal(rec.Payload, &update); err != nil {
			return CatalogEvent{}, fmt.Errorf("%w: decode execution status payload: %v", ErrInvalidCatalogEvent, err)
		}

		event.ExecutionStatus = &update
	default:
		return CatalogEvent{}, fmt.Errorf("%w: unsupported event type %q", ErrInvalidCatalogEvent, rec.EventType)
	}

	if err := event.Validate(); err != nil {
		return CatalogEvent{}, err
	}

	return event, nil
}

func (c CatalogEventConsumer) ApplyBatch(ctx context.Context, events []CatalogEvent) error {
	for i, event := range events {
		if err := c.Apply(ctx, event); err != nil {
			return fmt.Errorf("apply catalog event %d: %w", i, err)
		}
	}

	return nil
}

func (c CatalogEventConsumer) Apply(ctx context.Context, event CatalogEvent) error {
	if c.updater == nil {
		return errors.New("run catalog updater is required")
	}

	if err := event.Validate(); err != nil {
		return err
	}

	if event.RunStatus != nil {
		return c.updater.ApplyRunStatusUpdate(ctx, *event.RunStatus)
	}

	return c.updater.ApplyExecutionStatusUpdate(ctx, *event.ExecutionStatus)
}

func (e CatalogEvent) Validate() error {
	if _, err := e.normalizedSourceCellID(); err != nil {
		return err
	}

	updateCount := 0
	if e.RunStatus != nil {
		updateCount++
	}

	if e.ExecutionStatus != nil {
		updateCount++
	}

	if updateCount != 1 {
		return fmt.Errorf("%w: exactly one status update is required", ErrInvalidCatalogEvent)
	}

	return nil
}

func (e CatalogEvent) normalizedSourceCellID() (string, error) {
	sourceCellID := strings.TrimSpace(e.SourceCellID)
	if sourceCellID == "" {
		return "", fmt.Errorf("%w: source cell is required", ErrInvalidCatalogEvent)
	}

	return sourceCellID, nil
}
