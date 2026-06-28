package cell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"vectis/internal/dal"
)

var ErrInvalidCatalogEvent = errors.New("invalid catalog event")

const (
	CatalogEventTypeRunStatus         = "run.status"
	CatalogEventTypeExecutionStatus   = "execution.status"
	CatalogEventTypeArtifactRecord    = "artifact.record"
	CatalogEventTypeExecutionSecurity = "execution.security"
	CatalogEventTypeTerminalSnapshot  = "execution.terminal_snapshot"
)

type CatalogEvent struct {
	SourceCellID      string
	RunStatus         *dal.RunStatusUpdate
	ExecutionStatus   *dal.ExecutionStatusUpdate
	Artifact          *dal.ArtifactCreate
	ExecutionSecurity *dal.RecordExecutionSecurityEventParams
	TerminalSnapshot  *dal.TerminalExecutionSnapshotUpdate
}

type CatalogEventConsumer struct {
	updater        dal.RunCatalogUpdater
	artifacts      dal.ArtifactsRepository
	securityEvents interface {
		RecordExecutionSecurityEvent(ctx context.Context, event dal.RecordExecutionSecurityEventParams) error
	}
}

type terminalSnapshotCatalogUpdater interface {
	ApplyTerminalExecutionSnapshot(ctx context.Context, update dal.TerminalExecutionSnapshotUpdate) error
}

type catalogSourceOwnershipVerifier interface {
	VerifyCatalogRunSourceCell(ctx context.Context, runID, sourceCell string) error
	VerifyCatalogExecutionSourceCell(ctx context.Context, executionID, sourceCell string) error
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
	Read      int
	Applied   int
	Failed    int
	Retryable int
}

func NewCatalogEventConsumer(updater dal.RunCatalogUpdater, artifacts ...dal.ArtifactsRepository) CatalogEventConsumer {
	consumer := CatalogEventConsumer{updater: updater}
	if securityEvents, ok := updater.(interface {
		RecordExecutionSecurityEvent(ctx context.Context, event dal.RecordExecutionSecurityEventParams) error
	}); ok {
		consumer.securityEvents = securityEvents
	}
	if len(artifacts) > 0 {
		consumer.artifacts = artifacts[0]
	}

	return consumer
}

func NewCatalogEventPublisher(sourceCellID string, events dal.CatalogEventsRepository) CatalogEventPublisher {
	return CatalogEventPublisher{
		sourceCellID: strings.TrimSpace(sourceCellID),
		events:       events,
	}
}

func NewCatalogInboxProcessor(events dal.CatalogEventsRepository, updater dal.RunCatalogUpdater, artifacts ...dal.ArtifactsRepository) CatalogInboxProcessor {
	return CatalogInboxProcessor{
		events:   events,
		consumer: NewCatalogEventConsumer(updater, artifacts...),
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

func (p CatalogEventPublisher) RecordArtifact(ctx context.Context, create dal.ArtifactCreate) error {
	if p.events == nil {
		return nil
	}

	payload, err := json.Marshal(create)
	if err != nil {
		return fmt.Errorf("marshal artifact catalog event: %w", err)
	}

	if strings.TrimSpace(create.RunID) == "" || strings.TrimSpace(create.Name) == "" {
		return fmt.Errorf("%w: run_id and artifact name are required", ErrInvalidCatalogEvent)
	}

	return p.record(ctx, CatalogArtifactEventKey(create.RunID, create.Name), CatalogEventTypeArtifactRecord, payload)
}

func (p CatalogEventPublisher) RecordExecutionSecurity(ctx context.Context, event dal.RecordExecutionSecurityEventParams) error {
	if p.events == nil {
		return nil
	}

	if event.CreatedAt <= 0 {
		event.CreatedAt = time.Now().Unix()
	}

	if strings.TrimSpace(event.EventKey) == "" {
		event.EventKey = dal.ExecutionSecurityEventKey(event)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal execution security catalog event: %w", err)
	}

	if strings.TrimSpace(event.RunID) == "" || strings.TrimSpace(event.EventType) == "" || strings.TrimSpace(event.Outcome) == "" {
		return fmt.Errorf("%w: run_id, event_type, and outcome are required", ErrInvalidCatalogEvent)
	}

	return p.record(ctx, event.EventKey, CatalogEventTypeExecutionSecurity, payload)
}

func (p CatalogEventPublisher) RecordTerminalExecutionSnapshot(ctx context.Context, update dal.TerminalExecutionSnapshotUpdate) error {
	if p.events == nil {
		return nil
	}

	payload, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("marshal terminal execution snapshot catalog event: %w", err)
	}

	if strings.TrimSpace(update.RunID) == "" || strings.TrimSpace(string(update.Outcome)) == "" || len(update.Executions) == 0 {
		return fmt.Errorf("%w: run_id, outcome, and executions are required", ErrInvalidCatalogEvent)
	}

	return p.record(ctx, CatalogTerminalSnapshotEventKey(update.RunID), CatalogEventTypeTerminalSnapshot, payload)
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

func CatalogArtifactEventKey(runID, name string) string {
	return "artifact:" + strings.TrimSpace(runID) + ":" + strings.TrimSpace(name)
}

func CatalogTerminalSnapshotEventKey(runID string) string {
	return "run:" + strings.TrimSpace(runID) + ":terminal-snapshot"
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
			if catalogEventErrorIsTerminal(err) {
				result.Failed++
				if markErr := p.events.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
					return result, fmt.Errorf("mark catalog event %d failed: %w", rec.ID, markErr)
				}
			} else {
				result.Retryable++
				if markErr := p.events.MarkRetryable(ctx, rec.ID, err.Error()); markErr != nil {
					return result, fmt.Errorf("mark catalog event %d retryable: %w", rec.ID, markErr)
				}
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

func catalogEventErrorIsTerminal(err error) bool {
	return errors.Is(err, ErrInvalidCatalogEvent) || dal.IsConflict(err)
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
	case CatalogEventTypeArtifactRecord:
		var create dal.ArtifactCreate
		if err := json.Unmarshal(rec.Payload, &create); err != nil {
			return CatalogEvent{}, fmt.Errorf("%w: decode artifact payload: %v", ErrInvalidCatalogEvent, err)
		}

		event.Artifact = &create
	case CatalogEventTypeExecutionSecurity:
		var securityEvent dal.RecordExecutionSecurityEventParams
		if err := json.Unmarshal(rec.Payload, &securityEvent); err != nil {
			return CatalogEvent{}, fmt.Errorf("%w: decode execution security payload: %v", ErrInvalidCatalogEvent, err)
		}

		if strings.TrimSpace(securityEvent.EventKey) == "" {
			securityEvent.EventKey = rec.EventKey
		}

		event.ExecutionSecurity = &securityEvent
	case CatalogEventTypeTerminalSnapshot:
		var update dal.TerminalExecutionSnapshotUpdate
		if err := json.Unmarshal(rec.Payload, &update); err != nil {
			return CatalogEvent{}, fmt.Errorf("%w: decode terminal execution snapshot payload: %v", ErrInvalidCatalogEvent, err)
		}

		event.TerminalSnapshot = &update
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

	if err := c.verifySourceOwnership(ctx, event); err != nil {
		return err
	}

	if event.RunStatus != nil {
		return c.updater.ApplyRunStatusUpdate(ctx, *event.RunStatus)
	}

	if event.ExecutionStatus != nil {
		return c.updater.ApplyExecutionStatusUpdate(ctx, *event.ExecutionStatus)
	}

	if event.TerminalSnapshot != nil {
		updater, ok := c.updater.(terminalSnapshotCatalogUpdater)
		if !ok {
			return errors.New("terminal snapshot catalog updater is required")
		}

		return updater.ApplyTerminalExecutionSnapshot(ctx, *event.TerminalSnapshot)
	}

	if event.Artifact != nil {
		if c.artifacts == nil {
			return errors.New("artifact catalog updater is required")
		}

		_, err := c.artifacts.Record(ctx, *event.Artifact)
		return err
	}

	if c.securityEvents == nil {
		return errors.New("execution security catalog updater is required")
	}

	return c.securityEvents.RecordExecutionSecurityEvent(ctx, *event.ExecutionSecurity)
}

func (c CatalogEventConsumer) verifySourceOwnership(ctx context.Context, event CatalogEvent) error {
	verifier, ok := c.updater.(catalogSourceOwnershipVerifier)
	if !ok {
		return nil
	}

	sourceCellID, err := event.normalizedSourceCellID()
	if err != nil {
		return err
	}

	if event.RunStatus != nil {
		return verifier.VerifyCatalogRunSourceCell(ctx, event.RunStatus.RunID, sourceCellID)
	}

	if event.ExecutionStatus != nil {
		return verifier.VerifyCatalogExecutionSourceCell(ctx, event.ExecutionStatus.ExecutionID, sourceCellID)
	}

	if event.TerminalSnapshot != nil {
		return verifier.VerifyCatalogRunSourceCell(ctx, event.TerminalSnapshot.RunID, sourceCellID)
	}

	if event.Artifact != nil {
		if artifactCellID := strings.TrimSpace(event.Artifact.CellID); artifactCellID != "" && artifactCellID != sourceCellID {
			return fmt.Errorf("%w: catalog source_cell %s cannot record artifact from cell %s", dal.ErrConflict, sourceCellID, artifactCellID)
		}

		if executionID := strings.TrimSpace(event.Artifact.ExecutionID); executionID != "" {
			return verifier.VerifyCatalogExecutionSourceCell(ctx, executionID, sourceCellID)
		}

		return verifier.VerifyCatalogRunSourceCell(ctx, event.Artifact.RunID, sourceCellID)
	}

	if event.ExecutionSecurity != nil {
		if executionID := strings.TrimSpace(event.ExecutionSecurity.ExecutionID); executionID != "" {
			return verifier.VerifyCatalogExecutionSourceCell(ctx, executionID, sourceCellID)
		}

		return verifier.VerifyCatalogRunSourceCell(ctx, event.ExecutionSecurity.RunID, sourceCellID)
	}

	return nil
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

	if e.Artifact != nil {
		updateCount++
	}

	if e.ExecutionSecurity != nil {
		updateCount++
		if strings.TrimSpace(e.ExecutionSecurity.RunID) == "" || strings.TrimSpace(e.ExecutionSecurity.EventType) == "" || strings.TrimSpace(e.ExecutionSecurity.Outcome) == "" {
			return fmt.Errorf("%w: execution security run_id, event_type, and outcome are required", ErrInvalidCatalogEvent)
		}
	}

	if e.TerminalSnapshot != nil {
		updateCount++
	}

	if updateCount != 1 {
		return fmt.Errorf("%w: exactly one catalog update is required", ErrInvalidCatalogEvent)
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
