package reaction

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type ManualNotice struct {
	EventID     string
	NamespaceID int64
	JobID       string
	RunID       string
	TargetIDs   []string
	Actor       string
	Severity    string
	Message     string
	Reason      string
	SourceCell  string
	CreatedAt   int64
}

type RunCompleted struct {
	EventID       string
	NamespaceID   int64
	JobID         string
	RunID         string
	Status        string
	FailureCode   string
	FailureReason string
	TriggerType   string
	OwningCell    string
	CreatedAt     int64
}

type DefinitionValidationFailed struct {
	EventID        string
	NamespaceID    int64
	JobID          string
	Actor          string
	Message        string
	Reason         string
	DefinitionHash string
	SourceCell     string
	CreatedAt      int64
}

func (p *Publisher) PublishManualNotice(ctx context.Context, notice ManualNotice) (Publication, error) {
	message := strings.TrimSpace(notice.Message)
	if message == "" {
		return Publication{}, fmt.Errorf("%w: manual notice message is required", dal.ErrConflict)
	}

	severity := strings.TrimSpace(notice.Severity)
	if severity == "" {
		severity = "info"
	}

	payload, err := marshalPayload(manualNoticePayload{
		Message:  message,
		Reason:   strings.TrimSpace(notice.Reason),
		Severity: severity,
	})

	if err != nil {
		return Publication{}, err
	}

	return p.publish(ctx, dal.ReactionEventCreate{
		EventID:     strings.TrimSpace(notice.EventID),
		Source:      dal.ReactionEventSourceManual,
		EventType:   dal.ReactionEventTypeManualNotice,
		NamespaceID: notice.NamespaceID,
		JobID:       strings.TrimSpace(notice.JobID),
		RunID:       strings.TrimSpace(notice.RunID),
		Actor:       strings.TrimSpace(notice.Actor),
		PayloadJSON: payload,
		SourceCell:  strings.TrimSpace(notice.SourceCell),
		CreatedAt:   notice.CreatedAt,
	}, notice.TargetIDs)
}

func (p *Publisher) PublishRunCompleted(ctx context.Context, completed RunCompleted) (Publication, error) {
	runID := strings.TrimSpace(completed.RunID)
	if runID == "" {
		return Publication{}, fmt.Errorf("%w: run completed event requires run_id", dal.ErrConflict)
	}

	status := strings.TrimSpace(completed.Status)
	if status == "" {
		return Publication{}, fmt.Errorf("%w: run completed event requires status", dal.ErrConflict)
	}

	if !isRunCompletedStatus(status) {
		return Publication{}, fmt.Errorf("%w: run completed event requires terminal status, got %q", dal.ErrConflict, status)
	}

	jobID := strings.TrimSpace(completed.JobID)
	triggerType := strings.TrimSpace(completed.TriggerType)
	payload, err := marshalPayload(runCompletedPayload{
		JobID:         jobID,
		RunID:         runID,
		Status:        status,
		FailureCode:   strings.TrimSpace(completed.FailureCode),
		FailureReason: strings.TrimSpace(completed.FailureReason),
		TriggerType:   triggerType,
	})

	if err != nil {
		return Publication{}, err
	}

	return p.Publish(ctx, dal.ReactionEventCreate{
		EventID:     strings.TrimSpace(completed.EventID),
		Source:      dal.ReactionEventSourceLifecycle,
		EventType:   dal.ReactionEventTypeRunCompleted,
		NamespaceID: completed.NamespaceID,
		JobID:       jobID,
		RunID:       runID,
		PayloadJSON: payload,
		SourceCell:  strings.TrimSpace(completed.OwningCell),
		CreatedAt:   completed.CreatedAt,
	})
}

func (p *Publisher) PublishDefinitionValidationFailed(ctx context.Context, failure DefinitionValidationFailed) (Publication, error) {
	message := strings.TrimSpace(failure.Message)
	reason := strings.TrimSpace(failure.Reason)
	if message == "" && reason == "" {
		return Publication{}, fmt.Errorf("%w: definition validation failed event requires message or reason", dal.ErrConflict)
	}

	jobID := strings.TrimSpace(failure.JobID)
	if jobID == "" {
		return Publication{}, fmt.Errorf("%w: definition validation failed event requires job_id", dal.ErrConflict)
	}

	payload, err := marshalPayload(definitionValidationFailedPayload{
		JobID:          jobID,
		Message:        message,
		Reason:         reason,
		DefinitionHash: strings.TrimSpace(failure.DefinitionHash),
	})

	if err != nil {
		return Publication{}, err
	}

	return p.Publish(ctx, dal.ReactionEventCreate{
		EventID:     strings.TrimSpace(failure.EventID),
		Source:      dal.ReactionEventSourceLifecycle,
		EventType:   dal.ReactionEventTypeDefinitionValidationFailed,
		NamespaceID: failure.NamespaceID,
		JobID:       jobID,
		Actor:       strings.TrimSpace(failure.Actor),
		PayloadJSON: payload,
		SourceCell:  strings.TrimSpace(failure.SourceCell),
		CreatedAt:   failure.CreatedAt,
	})
}

type manualNoticePayload struct {
	Message  string `json:"message"`
	Reason   string `json:"reason,omitempty"`
	Severity string `json:"severity"`
}

type runCompletedPayload struct {
	JobID         string `json:"job_id,omitempty"`
	RunID         string `json:"run_id"`
	Status        string `json:"status"`
	FailureCode   string `json:"failure_code,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	TriggerType   string `json:"trigger_type,omitempty"`
}

type definitionValidationFailedPayload struct {
	JobID          string `json:"job_id,omitempty"`
	Message        string `json:"message,omitempty"`
	Reason         string `json:"reason,omitempty"`
	DefinitionHash string `json:"definition_hash,omitempty"`
}

func marshalPayload(payload any) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return raw, nil
}

func isRunCompletedStatus(status string) bool {
	switch status {
	case dal.RunStatusSucceeded,
		dal.RunStatusFailed,
		dal.RunStatusOrphaned,
		dal.RunStatusCancelled,
		dal.RunStatusAbandoned,
		dal.RunStatusAborted:
		return true
	default:
		return false
	}
}
