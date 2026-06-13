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
	event.EventKey = strings.TrimSpace(event.EventKey)
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

	if event.CreatedAt <= 0 {
		event.CreatedAt = time.Now().Unix()
	}

	if event.EventKey == "" {
		event.EventKey = ExecutionSecurityEventKey(event)
	}

	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO execution_security_events (
			event_key,
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
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_key) DO NOTHING
	`),
		nullableString(event.EventKey),
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
		event.CreatedAt,
	)

	return normalizeSQLError(err)
}

func ExecutionSecurityEventKey(event RecordExecutionSecurityEventParams) string {
	parts := []string{
		"security",
		strings.TrimSpace(event.RunID),
		strings.TrimSpace(event.TaskAttemptID),
		strings.TrimSpace(event.ExecutionID),
		strings.TrimSpace(event.EventType),
		strings.TrimSpace(event.Outcome),
		strings.TrimSpace(event.Reason),
		strings.TrimSpace(event.Provider),
		fmt.Sprintf("%d", event.CreatedAt),
	}

	if event.SecretCount != nil {
		parts = append(parts, fmt.Sprintf("secrets=%d", *event.SecretCount))
	}

	if event.FileCount != nil {
		parts = append(parts, fmt.Sprintf("files=%d", *event.FileCount))
	}

	return strings.Join(parts, ":")
}

func (r *SQLRunsRepository) LatestRunSecurityEvent(ctx context.Context, runID string, failedOnly bool) (*ExecutionSecurityEvent, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	where := "run_id = ?"
	args := []any{runID}
	if failedOnly {
		where += " AND outcome <> ?"
		args = append(args, "success")
	}

	query := `
		SELECT id, event_key, run_id, task_id, task_attempt_id, execution_id, event_type, outcome, reason, provider, secret_count, file_count, created_at
		FROM execution_security_events
		WHERE ` + where + `
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`

	rec, err := scanExecutionSecurityEvent(r.db.QueryRowContext(ctx, rebindQueryForPgx(query), args...))
	if err != nil {
		if IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	return &rec, nil
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
	var eventKey, taskID, taskAttemptID, executionID, provider sql.NullString
	var secretCount, fileCount sql.NullInt64
	if err := scanner.Scan(
		&rec.ID,
		&eventKey,
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

	rec.EventKey = nullStringValue(eventKey)
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
