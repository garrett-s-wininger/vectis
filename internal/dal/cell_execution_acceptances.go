package dal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type SQLCellExecutionAcceptancesRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLCellExecutionAcceptancesRepository) AcceptExecution(ctx context.Context, acceptance CellExecutionAcceptance) (bool, error) {
	normalized, err := normalizeCellExecutionAcceptance(acceptance, r.cellID)
	if err != nil {
		return false, err
	}

	acceptanceHash := cellExecutionAcceptanceHash(normalized)
	executionPayloadHash := ExecutionPayloadHash(normalized.RequestJSON)
	if created, err := r.acceptanceAlreadyRecorded(ctx, normalized.ExecutionID, acceptanceHash); err != nil || !created {
		return created, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureJobDefinitionTx(ctx, tx, normalized); err != nil {
		return false, err
	}

	if err := ensureExecutionPayloadTx(ctx, tx, normalized, executionPayloadHash); err != nil {
		return false, err
	}

	if err := ensureJobRunTx(ctx, tx, normalized, executionPayloadHash); err != nil {
		return false, err
	}

	if err := ensureAcceptedTaskAttemptTx(ctx, tx, normalized); err != nil {
		return false, err
	}

	if err := ensureRunSegmentTx(ctx, tx, normalized); err != nil {
		return false, err
	}

	if err := ensureSegmentExecutionTx(ctx, tx, normalized); err != nil {
		return false, err
	}

	result, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO cell_execution_acceptances
			(execution_id, acceptance_hash, run_id, job_id, run_index, segment_id, segment_name, cell_id, attempt, definition_version, definition_hash, execution_payload_hash, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(execution_id) DO NOTHING
	`),
		normalized.ExecutionID,
		acceptanceHash,
		normalized.RunID,
		normalized.JobID,
		normalized.RunIndex,
		normalized.SegmentID,
		normalized.SegmentName,
		normalized.CellID,
		normalized.Attempt,
		normalized.DefinitionVersion,
		normalized.DefinitionHash,
		executionPayloadHash,
	)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	inserted, err := insertedReceipt(result)
	if err != nil {
		return false, err
	}

	if !inserted {
		if err := assertRecordedAcceptanceHashTx(ctx, tx, normalized.ExecutionID, acceptanceHash); err != nil {
			return false, err
		}

		if err := tx.Commit(); err != nil {
			return false, err
		}

		return false, nil
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func insertedReceipt(result sql.Result) (bool, error) {
	if result == nil {
		return true, nil
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return true, nil
	}

	return rows > 0, nil
}

func assertRecordedAcceptanceHashTx(ctx context.Context, tx *sql.Tx, executionID, acceptanceHash string) error {
	var existingHash string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT acceptance_hash FROM cell_execution_acceptances WHERE execution_id = ?"),
		executionID,
	).Scan(&existingHash); err != nil {
		return normalizeSQLError(err)
	}

	if existingHash != acceptanceHash {
		return fmt.Errorf("%w: execution %s was already accepted with different payload", ErrConflict, executionID)
	}

	return nil
}

func (r *SQLCellExecutionAcceptancesRepository) acceptanceAlreadyRecorded(ctx context.Context, executionID, acceptanceHash string) (bool, error) {
	var existingHash string
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT acceptance_hash FROM cell_execution_acceptances WHERE execution_id = ?"),
		executionID,
	).Scan(&existingHash)

	if err == nil {
		if existingHash != acceptanceHash {
			return false, fmt.Errorf("%w: execution %s was already accepted with different payload", ErrConflict, executionID)
		}

		return false, nil
	}

	if err == sql.ErrNoRows {
		return true, nil
	}

	return false, normalizeSQLError(err)
}

func (r *SQLCellExecutionAcceptancesRepository) ListPendingQueueHandoffs(ctx context.Context, cutoffUnixNano int64, limit int) ([]CellExecutionQueueHandoff, error) {
	if limit <= 0 {
		limit = 100
	}

	cellID := normalizeTargetCellID("", r.cellID)
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT ca.execution_id, ca.run_id, ep.payload_json, ca.enqueue_attempts
		FROM cell_execution_acceptances ca
		JOIN execution_payloads ep ON ep.payload_hash = ca.execution_payload_hash
		JOIN segment_executions se ON se.execution_id = ca.execution_id
		WHERE ca.cell_id = ?
			AND ca.enqueued_at IS NULL
			AND se.status = ?
			AND (ca.last_enqueue_attempt_at IS NULL OR ca.last_enqueue_attempt_at <= ?)
		ORDER BY ca.id ASC
		LIMIT ?
	`), cellID, ExecutionStatusAccepted, cutoffUnixNano, limit)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CellExecutionQueueHandoff
	for rows.Next() {
		var rec CellExecutionQueueHandoff
		if err := rows.Scan(&rec.ExecutionID, &rec.RunID, &rec.RequestJSON, &rec.EnqueueAttempts); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLCellExecutionAcceptancesRepository) MarkEnqueued(ctx context.Context, executionID string, enqueuedAtUnixNano int64) error {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if enqueuedAtUnixNano <= 0 {
		enqueuedAtUnixNano = time.Now().UnixNano()
	}

	result, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE cell_execution_acceptances
		SET enqueued_at = ?,
			last_enqueue_attempt_at = ?,
			enqueue_attempts = enqueue_attempts + 1,
			last_enqueue_error = NULL,
			updated_at = CURRENT_TIMESTAMP
		WHERE execution_id = ?
	`), enqueuedAtUnixNano, enqueuedAtUnixNano, executionID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireRowsAffected(result, "cell execution acceptance", executionID)
}

func (r *SQLCellExecutionAcceptancesRepository) MarkEnqueueFailed(ctx context.Context, executionID string, attemptedAtUnixNano int64, message string) error {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if attemptedAtUnixNano <= 0 {
		attemptedAtUnixNano = time.Now().UnixNano()
	}

	result, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE cell_execution_acceptances
		SET last_enqueue_attempt_at = ?,
			enqueue_attempts = enqueue_attempts + 1,
			last_enqueue_error = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE execution_id = ?
	`), attemptedAtUnixNano, truncateCellEnqueueError(message), executionID)

	if err != nil {
		return normalizeSQLError(err)
	}

	return requireRowsAffected(result, "cell execution acceptance", executionID)
}

func requireRowsAffected(result sql.Result, label, id string) error {
	if result == nil {
		return nil
	}

	rows, err := result.RowsAffected()
	if err != nil || rows > 0 {
		return nil
	}

	return fmt.Errorf("%w: %s %s", ErrNotFound, label, id)
}

func truncateCellEnqueueError(message string) string {
	message = strings.TrimSpace(message)
	const maxLen = 1000
	if len(message) <= maxLen {
		return message
	}

	return message[:maxLen]
}

func normalizeCellExecutionAcceptance(a CellExecutionAcceptance, fallbackCellID string) (CellExecutionAcceptance, error) {
	a.ExecutionID = strings.TrimSpace(a.ExecutionID)
	a.RunID = strings.TrimSpace(a.RunID)
	a.JobID = strings.TrimSpace(a.JobID)
	a.TaskID = strings.TrimSpace(a.TaskID)
	a.TaskKey = strings.TrimSpace(a.TaskKey)
	a.TaskName = strings.TrimSpace(a.TaskName)
	a.TaskAttemptID = strings.TrimSpace(a.TaskAttemptID)
	a.SegmentID = strings.TrimSpace(a.SegmentID)
	a.SegmentName = strings.TrimSpace(a.SegmentName)
	a.CellID = normalizeTargetCellID(a.CellID, fallbackCellID)
	a.DefinitionHash = strings.TrimSpace(a.DefinitionHash)
	a.DefinitionJSON = strings.TrimSpace(a.DefinitionJSON)
	a.RequestJSON = strings.TrimSpace(a.RequestJSON)

	if a.ExecutionID == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: execution_id is required", ErrConflict)
	}

	if a.RunID == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if a.JobID == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: job_id is required", ErrConflict)
	}

	if a.RunIndex <= 0 {
		a.RunIndex = 1
	}

	if a.TaskKey == "" {
		a.TaskKey = RootTaskKey
	}

	if a.TaskName == "" {
		a.TaskName = a.TaskKey
	}

	if a.TaskID == "" {
		a.TaskID = a.RunID + ":" + a.TaskKey
	}

	if a.Attempt <= 0 {
		a.Attempt = 1
	}

	if a.TaskAttemptID == "" {
		a.TaskAttemptID = taskAttemptID(a.TaskID, a.Attempt)
	}

	if a.SegmentID == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: segment_id is required", ErrConflict)
	}

	if a.SegmentName == "" {
		a.SegmentName = "root"
	}

	if a.DefinitionVersion <= 0 {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: definition_version must be positive", ErrConflict)
	}

	if a.DefinitionHash == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: definition_hash is required", ErrConflict)
	}

	if a.DefinitionJSON == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: definition_json is required", ErrConflict)
	}

	if a.RequestJSON == "" {
		return CellExecutionAcceptance{}, fmt.Errorf("%w: request_json is required", ErrConflict)
	}

	if a.AcceptedAtUnixNano <= 0 {
		a.AcceptedAtUnixNano = time.Now().UnixNano()
	}

	return a, nil
}

func cellExecutionAcceptanceHash(a CellExecutionAcceptance) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		a.ExecutionID,
		a.RunID,
		a.JobID,
		fmt.Sprintf("%d", a.RunIndex),
		a.TaskID,
		a.TaskKey,
		a.TaskName,
		a.TaskAttemptID,
		a.SegmentID,
		a.SegmentName,
		a.CellID,
		fmt.Sprintf("%d", a.Attempt),
		fmt.Sprintf("%d", a.DefinitionVersion),
		a.DefinitionHash,
		a.DefinitionJSON,
		ExecutionPayloadHash(a.RequestJSON),
	}, "\x00")))

	return hex.EncodeToString(sum[:])
}

func ensureJobDefinitionTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance) error {
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(job_id, version) DO NOTHING
	`), newGlobalID(), a.JobID, a.DefinitionVersion, a.DefinitionJSON, a.DefinitionHash); err != nil {
		return normalizeSQLError(err)
	}

	var existingHash string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = ?"),
		a.JobID,
		a.DefinitionVersion,
	).Scan(&existingHash); err != nil {
		return normalizeSQLError(err)
	}

	if existingHash != a.DefinitionHash {
		return fmt.Errorf("%w: job %s version %d has different definition hash", ErrConflict, a.JobID, a.DefinitionVersion)
	}

	return nil
}

func ensureExecutionPayloadTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance, executionPayloadHash string) error {
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO execution_payloads (payload_hash, payload_json, definition_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(payload_hash) DO NOTHING
	`), executionPayloadHash, a.RequestJSON, a.DefinitionHash); err != nil {
		return normalizeSQLError(err)
	}

	var existingPayload string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT payload_json FROM execution_payloads WHERE payload_hash = ?"),
		executionPayloadHash,
	).Scan(&existingPayload); err != nil {
		return normalizeSQLError(err)
	}

	if existingPayload != a.RequestJSON {
		return fmt.Errorf("%w: execution payload hash %s has different payload", ErrConflict, executionPayloadHash)
	}

	return nil
}

func ensureJobRunTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance, executionPayloadHash string) error {
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO job_runs
			(run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, execution_payload_hash)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?)
		ON CONFLICT(run_id) DO NOTHING
	`), a.RunID, a.JobID, a.RunIndex, RunStatusQueued, a.DefinitionVersion, a.DefinitionHash, a.CellID, executionPayloadHash); err != nil {
		return normalizeSQLError(err)
	}

	var jobID, definitionHash, owningCell string
	var storedPayloadHash string
	var runIndex, definitionVersion int

	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT job_id, run_index, definition_version, definition_hash, owning_cell, execution_payload_hash
		FROM job_runs
		WHERE run_id = ?
	`), a.RunID).Scan(&jobID, &runIndex, &definitionVersion, &definitionHash, &owningCell, &storedPayloadHash); err != nil {
		return normalizeSQLError(err)
	}

	if jobID != a.JobID || runIndex != a.RunIndex || definitionVersion != a.DefinitionVersion || definitionHash != a.DefinitionHash || owningCell != a.CellID {
		return fmt.Errorf("%w: run %s has different accepted payload", ErrConflict, a.RunID)
	}

	if storedPayloadHash == "" {
		if _, err := tx.ExecContext(ctx,
			rebindQueryForPgx("UPDATE job_runs SET execution_payload_hash = ? WHERE run_id = ? AND execution_payload_hash = ''"),
			executionPayloadHash,
			a.RunID,
		); err != nil {
			return normalizeSQLError(err)
		}

		return nil
	}

	if storedPayloadHash != executionPayloadHash {
		return fmt.Errorf("%w: run %s has different execution payload", ErrConflict, a.RunID)
	}

	return nil
}

func ensureAcceptedTaskAttemptTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance) error {
	taskID := a.TaskID
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO run_tasks (task_id, run_id, task_key, name, status)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(task_id) DO NOTHING
	`), taskID, a.RunID, a.TaskKey, a.TaskName, TaskStatusAccepted); err != nil {
		return normalizeSQLError(err)
	}

	var runID, taskKey, name, status string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, task_key, name, status FROM run_tasks WHERE task_id = ?"),
		taskID,
	).Scan(&runID, &taskKey, &name, &status); err != nil {
		return normalizeSQLError(err)
	}

	if runID != a.RunID || taskKey != a.TaskKey || name != a.TaskName {
		return fmt.Errorf("%w: task %s has different accepted payload", ErrConflict, taskID)
	}

	if isPreDispatchStatus(status) {
		if _, err := tx.ExecContext(ctx,
			rebindQueryForPgx("UPDATE run_tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?"),
			TaskStatusAccepted,
			taskID,
		); err != nil {
			return normalizeSQLError(err)
		}
	}

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO task_attempts
			(attempt_id, task_id, run_id, cell_id, attempt, status, accepted_at, last_observed_at, event_sequence)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 1)
		ON CONFLICT(task_id, attempt) DO NOTHING
	`), a.TaskAttemptID, taskID, a.RunID, a.CellID, a.Attempt, TaskStatusAccepted, a.AcceptedAtUnixNano); err != nil {
		return normalizeSQLError(err)
	}

	var storedAttemptID, attemptRunID, attemptTaskID, cellID, attemptStatus string
	var attempt int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT attempt_id, run_id, task_id, cell_id, attempt, status
		FROM task_attempts
		WHERE task_id = ? AND attempt = ?
	`), taskID, a.Attempt).Scan(&storedAttemptID, &attemptRunID, &attemptTaskID, &cellID, &attempt, &attemptStatus); err != nil {
		return normalizeSQLError(err)
	}

	if storedAttemptID != a.TaskAttemptID || attemptRunID != a.RunID || attemptTaskID != taskID || cellID != a.CellID || attempt != a.Attempt {
		return fmt.Errorf("%w: task attempt %s attempt %d has different accepted payload", ErrConflict, taskID, a.Attempt)
	}

	if isPreDispatchStatus(attemptStatus) {
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE task_attempts
			SET status = ?, accepted_at = COALESCE(accepted_at, CURRENT_TIMESTAMP), last_observed_at = ?, event_sequence = event_sequence + 1, updated_at = CURRENT_TIMESTAMP
			WHERE task_id = ? AND attempt = ?
		`), TaskStatusAccepted, a.AcceptedAtUnixNano, taskID, a.Attempt); err != nil {
			return normalizeSQLError(err)
		}
	}

	return nil
}

func ensureRunSegmentTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance) error {
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO run_segments (segment_id, run_id, name, status)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(segment_id) DO NOTHING
	`), a.SegmentID, a.RunID, a.SegmentName, SegmentStatusAccepted); err != nil {
		return normalizeSQLError(err)
	}

	var runID, name, status string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, name, status FROM run_segments WHERE segment_id = ?"),
		a.SegmentID,
	).Scan(&runID, &name, &status); err != nil {
		return normalizeSQLError(err)
	}

	if runID != a.RunID || name != a.SegmentName {
		return fmt.Errorf("%w: segment %s has different accepted payload", ErrConflict, a.SegmentID)
	}

	if isPreDispatchStatus(status) {
		if _, err := tx.ExecContext(ctx,
			rebindQueryForPgx("UPDATE run_segments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ?"),
			SegmentStatusAccepted,
			a.SegmentID,
		); err != nil {
			return normalizeSQLError(err)
		}
	}

	return nil
}

func ensureSegmentExecutionTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance) error {
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO segment_executions
			(execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt, accepted_at, last_observed_at, event_sequence)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, 1)
		ON CONFLICT(execution_id) DO NOTHING
	`), a.ExecutionID, a.SegmentID, a.RunID, a.TaskID, a.TaskAttemptID, a.CellID, ExecutionStatusAccepted, a.Attempt, a.AcceptedAtUnixNano); err != nil {
		return normalizeSQLError(err)
	}

	var segmentID, runID, taskID, taskAttemptID, cellID, status string
	var attempt int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT segment_id, run_id, task_id, task_attempt_id, cell_id, attempt, status
		FROM segment_executions
		WHERE execution_id = ?
	`), a.ExecutionID).Scan(&segmentID, &runID, &taskID, &taskAttemptID, &cellID, &attempt, &status); err != nil {
		return normalizeSQLError(err)
	}

	if segmentID != a.SegmentID || runID != a.RunID || taskID != a.TaskID || taskAttemptID != a.TaskAttemptID || cellID != a.CellID || attempt != a.Attempt {
		return fmt.Errorf("%w: execution %s has different accepted payload", ErrConflict, a.ExecutionID)
	}

	if isPreDispatchStatus(status) {
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE segment_executions
			SET status = ?, accepted_at = COALESCE(accepted_at, CURRENT_TIMESTAMP), last_observed_at = ?, event_sequence = event_sequence + 1, updated_at = CURRENT_TIMESTAMP
			WHERE execution_id = ?
		`), ExecutionStatusAccepted, a.AcceptedAtUnixNano, a.ExecutionID); err != nil {
			return normalizeSQLError(err)
		}
	}

	return nil
}

func isPreDispatchStatus(status string) bool {
	return status == TaskStatusPlanned || status == TaskStatusPending
}

var _ CellExecutionAcceptancesRepository = (*SQLCellExecutionAcceptancesRepository)(nil)
