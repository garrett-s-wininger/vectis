package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (r *SQLRunsRepository) RecordExecutionSecurityEvent(ctx context.Context, event RecordExecutionSecurityEventParams) error {
	event.RunID = strings.TrimSpace(event.RunID)
	event.TaskID = strings.TrimSpace(event.TaskID)
	event.TaskAttemptID = strings.TrimSpace(event.TaskAttemptID)
	event.ExecutionID = strings.TrimSpace(event.ExecutionID)
	event.EventType = strings.TrimSpace(event.EventType)
	event.Outcome = strings.TrimSpace(event.Outcome)
	event.Reason = strings.TrimSpace(event.Reason)
	event.Provider = strings.TrimSpace(event.Provider)

	if event.RunID == "" {
		return fmt.Errorf("%w: run_id is required", ErrConflict)
	}
	if event.EventType == "" {
		return fmt.Errorf("%w: security event type is required", ErrConflict)
	}
	if event.Outcome == "" {
		return fmt.Errorf("%w: security event outcome is required", ErrConflict)
	}

	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO execution_security_events (
			run_id,
			task_id,
			task_attempt_id,
			execution_id,
			event_type,
			outcome,
			reason,
			provider,
			secret_count,
			file_count,
			created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`),
		event.RunID,
		nullableString(event.TaskID),
		nullableString(event.TaskAttemptID),
		nullableString(event.ExecutionID),
		event.EventType,
		event.Outcome,
		event.Reason,
		nullableString(event.Provider),
		nullableIntPtr(event.SecretCount),
		nullableIntPtr(event.FileCount),
		time.Now().Unix(),
	)

	return normalizeSQLError(err)
}

func nullableIntPtr(value *int) any {
	if value == nil {
		return nil
	}

	return *value
}

func scanExecutionSecurityEvent(scanner interface {
	Scan(dest ...any) error
}) (ExecutionSecurityEvent, error) {
	var rec ExecutionSecurityEvent
	var taskID, taskAttemptID, executionID, provider sql.NullString
	var secretCount, fileCount sql.NullInt64
	if err := scanner.Scan(
		&rec.ID,
		&rec.RunID,
		&taskID,
		&taskAttemptID,
		&executionID,
		&rec.EventType,
		&rec.Outcome,
		&rec.Reason,
		&provider,
		&secretCount,
		&fileCount,
		&rec.CreatedAt,
	); err != nil {
		return ExecutionSecurityEvent{}, normalizeSQLError(err)
	}

	rec.TaskID = nullStringValue(taskID)
	rec.TaskAttemptID = nullStringValue(taskAttemptID)
	rec.ExecutionID = nullStringValue(executionID)
	rec.Provider = nullStringPtr(provider)
	rec.SecretCount = nullIntPtr(secretCount)
	rec.FileCount = nullIntPtr(fileCount)

	return rec, nil
}

func nullIntPtr(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}

	v := int(value.Int64)
	return &v
}
