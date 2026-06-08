package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

type SQLRunsRepository struct {
	db     *sql.DB
	cellID string
}

func (r *SQLRunsRepository) currentCellID() string {
	return normalizeCellID(r.cellID)
}

func (r *SQLRunsRepository) ApplyRunStatusUpdate(ctx context.Context, update RunStatusUpdate) error {
	runID := strings.TrimSpace(update.RunID)
	if runID == "" {
		return fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	switch update.Status {
	case RunStatusRunning:
		return r.MarkRunRunning(ctx, runID)
	case RunStatusSucceeded:
		return r.MarkRunSucceeded(ctx, runID)
	case RunStatusFailed:
		return r.MarkRunFailed(ctx, runID, update.FailureCode, update.Reason)
	case RunStatusCancelled:
		return r.MarkRunCancelled(ctx, runID, update.Reason)
	case RunStatusAborted:
		return r.MarkRunAborted(ctx, runID, update.Reason)
	case RunStatusOrphaned:
		return r.MarkRunOrphaned(ctx, runID, update.Reason)
	default:
		return fmt.Errorf("%w: unsupported run status %s", ErrConflict, update.Status)
	}
}

func (r *SQLRunsRepository) ApplyExecutionStatusUpdate(ctx context.Context, update ExecutionStatusUpdate) error {
	executionID := strings.TrimSpace(update.ExecutionID)
	if executionID == "" {
		return fmt.Errorf("%w: execution_id is required", ErrNotFound)
	}

	switch update.Status {
	case ExecutionStatusAccepted:
		return r.markExecutionAccepted(ctx, executionID)
	case ExecutionStatusRunning:
		return r.MarkExecutionStarted(ctx, executionID)
	case ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusCancelled, ExecutionStatusAborted:
		return r.markExecutionTerminal(ctx, executionID, update.Status)
	default:
		return fmt.Errorf("%w: unsupported execution status %s", ErrConflict, update.Status)
	}
}

func (r *SQLRunsRepository) MarkRunRunning(ctx context.Context, runID string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("UPDATE job_runs SET status = ?, orphan_reason = '', failure_code = '', started_at = COALESCE(started_at, CURRENT_TIMESTAMP) WHERE run_id = ?"),
		"running", runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) MarkRunSucceeded(ctx context.Context, runID string) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP,
		orphan_reason = '', failure_code = '', failure_reason = NULL, lease_owner = NULL, lease_until = NULL,
		cancel_token = NULL, cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?
	`), "succeeded", runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) MarkRunFailed(ctx context.Context, runID, failureCode, reason string) error {
	if failureCode == "" {
		failureCode = FailureCodeExecution
	}

	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_code = ?, failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL,
		cancel_token = NULL, cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?
	`), "failed", failureCode, reason, runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) MarkRunAborted(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = CancelReasonAPI
	}

	return r.MarkRunCancelled(ctx, runID, reason)
}

func (r *SQLRunsRepository) MarkRunCancelled(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = CancelReasonAPI
	}

	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_code = '', failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL, cancel_token = NULL,
		cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?
	`), RunStatusCancelled, reason, runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error {
	return r.repairMarkTerminal(ctx, runID, RunStatusSucceeded, "", reason)
}

func (r *SQLRunsRepository) RepairMarkRunFailed(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = RepairReasonManual
	}

	return r.RepairMarkRunFailedWithCode(ctx, runID, FailureCodeForceFailed, reason)
}

func (r *SQLRunsRepository) RepairMarkRunFailedWithCode(ctx context.Context, runID, failureCode, reason string) error {
	if failureCode == "" {
		failureCode = FailureCodeForceFailed
	}
	if reason == "" {
		reason = RepairReasonManual
	}

	return r.repairMarkTerminal(ctx, runID, RunStatusFailed, failureCode, reason)
}

func (r *SQLRunsRepository) RepairMarkRunCancelled(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = RepairReasonManual
	}

	return r.repairMarkTerminal(ctx, runID, RunStatusCancelled, "", reason)
}

func (r *SQLRunsRepository) RepairMarkRunAbandoned(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = RepairReasonManual
	}

	return r.repairMarkTerminal(ctx, runID, RunStatusAbandoned, "", reason)
}

func (r *SQLRunsRepository) repairMarkTerminal(ctx context.Context, runID, status, failureCode, reason string) error {
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET status = ?,
			orphan_reason = '',
			failure_code = ?,
			finished_at = CURRENT_TIMESTAMP,
			failure_reason = ?,
			lease_owner = NULL,
			lease_until = NULL,
			cancel_token = NULL,
			cancel_requested_at = NULL,
			cancel_reason = NULL
		WHERE run_id = ?
			AND status = 'orphaned'
	`), status, failureCode, nullableReason(reason), runID)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 1 {
		return nil
	}

	var current string
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`SELECT status FROM job_runs WHERE run_id = ?`), runID).Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return normalizeSQLError(err)
	}

	return fmt.Errorf("%w: run %s in status %s cannot be repair-marked %s", ErrConflict, runID, current, status)
}

func nullableReason(reason string) any {
	if reason == "" {
		return nil
	}

	return reason
}

func (r *SQLRunsRepository) MarkRunOrphaned(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = "unknown"
	}
	orphanReason := classifyOrphanReason(reason)

	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs SET status = ?, failure_reason = ?,
		orphan_reason = ?, failure_code = '', lease_owner = NULL, lease_until = NULL WHERE run_id = ?
	`), "orphaned", reason, orphanReason, runID)

	return normalizeSQLError(err)
}

func classifyOrphanReason(reason string) string {
	switch reason {
	case OrphanReasonLeaseExpired, OrphanReasonAckUncertain:
		return reason
	default:
		return "unknown"
	}
}

func (r *SQLRunsRepository) RequeueRunForRetry(ctx context.Context, runID string) error {
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET status = 'queued',
			orphan_reason = '',
			failure_code = '',
			finished_at = NULL,
			failure_reason = NULL,
			lease_owner = NULL,
			lease_until = NULL,
			cancel_token = NULL,
			cancel_requested_at = NULL,
			cancel_reason = NULL,
			last_dispatched_at = NULL
		WHERE run_id = ?
			AND status IN ('queued', 'failed', 'orphaned', 'aborted', 'cancelled', 'abandoned')
	`), runID)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 1 {
		return nil
	}

	var status string
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`SELECT status FROM job_runs WHERE run_id = ?`), runID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return normalizeSQLError(err)
	}

	return fmt.Errorf("%w: run %s in status %s cannot be requeued", ErrConflict, runID, status)
}

func (r *SQLRunsRepository) MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT jr.run_id
		FROM job_runs jr
		WHERE jr.status = 'running'
			AND (jr.lease_until IS NULL OR jr.lease_until < ?)
			AND NOT EXISTS (
				SELECT 1
				FROM segment_executions se
				WHERE se.run_id = jr.run_id
					AND se.status IN (?, ?)
					AND se.lease_until IS NOT NULL
					AND se.lease_until >= ?
			)
		ORDER BY id ASC
	`), cutoffUnix, ExecutionStatusAccepted, ExecutionStatusRunning, cutoffUnix)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	candidates := make([]string, 0, 16)
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, normalizeSQLError(err)
		}

		candidates = append(candidates, runID)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	out := make([]string, 0, len(candidates))
	for _, runID := range candidates {
		res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE job_runs
			SET status = 'orphaned',
				orphan_reason = ?,
				failure_code = ''
			WHERE run_id = ?
				AND status = 'running'
				AND (lease_until IS NULL OR lease_until < ?)
				AND NOT EXISTS (
					SELECT 1
					FROM segment_executions se
					WHERE se.run_id = job_runs.run_id
						AND se.status IN (?, ?)
						AND se.lease_until IS NOT NULL
						AND se.lease_until >= ?
				)
		`), OrphanReasonLeaseExpired, runID, cutoffUnix, ExecutionStatusAccepted, ExecutionStatusRunning, cutoffUnix)

		if err != nil {
			return nil, normalizeSQLError(err)
		}

		n, err := res.RowsAffected()
		if err != nil {
			return nil, normalizeSQLError(err)
		}

		if n == 1 {
			out = append(out, runID)
		}
	}

	return out, nil
}

func (r *SQLRunsRepository) GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error) {
	if runID == "" {
		return "", false, nil
	}

	err = r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT status FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&status)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}

		return "", false, normalizeSQLError(err)
	}

	return status, true, nil
}

func (r *SQLRunsRepository) RequestRunCancel(ctx context.Context, runID, reason string) (RunForCancel, error) {
	if reason == "" {
		reason = CancelReasonAPI
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET cancel_requested_at = COALESCE(cancel_requested_at, ?),
			cancel_reason = CASE
				WHEN cancel_reason IS NULL OR cancel_reason = '' THEN ?
				ELSE cancel_reason
			END
		WHERE run_id = ?
			AND status = ?
	`), time.Now().Unix(), reason, runID, RunStatusRunning)

	if err != nil {
		return RunForCancel{}, normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return RunForCancel{}, err
	}

	rec, err := r.GetRunForCancel(ctx, runID)
	if err != nil {
		return RunForCancel{}, err
	}

	if n != 1 {
		return rec, fmt.Errorf("%w: run %s in status %s cannot be cancelled", ErrConflict, runID, rec.Status)
	}

	return rec, nil
}

func (r *SQLRunsRepository) RunCancelRequested(ctx context.Context, runID string) (bool, error) {
	if runID == "" {
		return false, nil
	}

	var requestedAt sql.NullInt64
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT cancel_requested_at
		FROM job_runs
		WHERE run_id = ?
			AND status IN (?, ?)
	`), runID, RunStatusRunning, RunStatusOrphaned).Scan(&requestedAt); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return requestedAt.Valid && requestedAt.Int64 > 0, nil
}

func (r *SQLRunsRepository) TouchDispatched(ctx context.Context, runID string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE job_runs SET last_dispatched_at = ? WHERE run_id = ?`),
		time.Now().Unix(), runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) GetLogShard(ctx context.Context, runID string) (string, bool, error) {
	var shardID string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`SELECT log_shard_id FROM job_runs WHERE run_id = ?`),
		runID,
	).Scan(&shardID); err != nil {
		if err == sql.ErrNoRows {
			return "", false, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return "", false, normalizeSQLError(err)
	}

	if shardID == "" {
		return "", false, nil
	}

	return shardID, true, nil
}

func (r *SQLRunsRepository) AssignLogShard(ctx context.Context, runID, shardID string) (string, error) {
	if shardID == "" {
		return "", fmt.Errorf("log shard id is required")
	}

	if _, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET log_shard_id = ?,
			log_shard_assigned_at = ?
		WHERE run_id = ?
			AND log_shard_id = ''
	`), shardID, time.Now().Unix(), runID); err != nil {
		return "", normalizeSQLError(err)
	}

	assigned, ok, err := r.GetLogShard(ctx, runID)
	if err != nil {
		return "", err
	}

	if !ok {
		return "", fmt.Errorf("assign log shard: run %s has no assigned shard after update", runID)
	}

	return assigned, nil
}

func (r *SQLRunsRepository) CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error) {
	return r.CreateRunInCell(ctx, jobID, runIndex, definitionVersion, r.currentCellID())
}

func (r *SQLRunsRepository) CreateRunInCell(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellID string) (runID string, runIndexOut int, err error) {
	runs, err := r.CreateRunsInCells(ctx, jobID, runIndex, definitionVersion, []string{targetCellID})
	if err != nil {
		return "", 0, err
	}

	if len(runs) == 0 {
		return "", 0, fmt.Errorf("%w: no runs created", ErrNotFound)
	}

	return runs[0].RunID, runs[0].RunIndex, nil
}

func (r *SQLRunsRepository) CreateRunsInCells(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string) ([]CreatedRun, error) {
	return r.CreateRunsInCellsWithAudit(ctx, jobID, runIndex, definitionVersion, targetCellIDs, RunAuditMetadata{})
}

func (r *SQLRunsRepository) CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit RunAuditMetadata) ([]CreatedRun, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var idx int
	if runIndex != nil {
		idx = *runIndex
	} else {
		err = tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"), jobID).Scan(&idx)
		if err != nil {
			return nil, normalizeSQLError(err)
		}
	}

	definitionHash, err := lookupDefinitionHashTx(ctx, tx, jobID, definitionVersion)
	if err != nil {
		return nil, err
	}

	if len(targetCellIDs) == 0 {
		targetCellIDs = []string{r.currentCellID()}
	}

	triggerInvocationID := strings.TrimSpace(audit.TriggerInvocationID)
	executionPayloadHash := strings.TrimSpace(audit.ExecutionPayloadHash)
	replayOfRunID := strings.TrimSpace(audit.ReplayOfRunID)

	createdRuns := make([]CreatedRun, 0, len(targetCellIDs))
	for i, targetCellID := range targetCellIDs {
		targetCellID = normalizeTargetCellID(targetCellID, r.currentCellID())
		runID := uuid.New().String()
		runIndexOut := idx + i

		_, err = tx.ExecContext(ctx,
			rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?, ?, ?)`),
			runID,
			jobID,
			runIndexOut,
			"queued",
			definitionVersion,
			definitionHash,
			targetCellID,
			nullableString(replayOfRunID),
			nullableString(triggerInvocationID),
			executionPayloadHash,
		)

		if err != nil {
			return nil, normalizeSQLError(err)
		}

		if err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID); err != nil {
			return nil, err
		}

		createdRuns = append(createdRuns, CreatedRun{
			RunID:        runID,
			RunIndex:     runIndexOut,
			TargetCellID: targetCellID,
		})
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return createdRuns, nil
}

func (r *SQLRunsRepository) CreateScheduledRun(ctx context.Context, scheduleID int64, scheduledFor time.Time, jobID string, definitionVersion int, audit RunAuditMetadata) (runID string, runIndexOut int, created bool, err error) {
	if scheduleID <= 0 {
		return "", 0, false, fmt.Errorf("schedule id is required")
	}

	scheduledForKey := scheduledFor.UTC().Format(time.RFC3339)

	for range 3 {
		runID, runIndexOut, found, err := r.findScheduledRun(ctx, scheduleID, scheduledForKey)
		if err != nil {
			return "", 0, false, err
		}
		if found {
			return runID, runIndexOut, false, nil
		}

		runID, runIndexOut, created, err := r.tryCreateScheduledRun(ctx, scheduleID, scheduledForKey, jobID, definitionVersion, audit)
		if err != nil {
			return "", 0, false, err
		}

		if created {
			return runID, runIndexOut, true, nil
		}
	}

	runID, runIndexOut, found, err := r.findScheduledRun(ctx, scheduleID, scheduledForKey)
	if err != nil {
		return "", 0, false, err
	}

	if found {
		return runID, runIndexOut, false, nil
	}

	return "", 0, false, fmt.Errorf("%w: scheduled run for schedule %d at %s was not created", ErrConflict, scheduleID, scheduledForKey)
}

func (r *SQLRunsRepository) findScheduledRun(ctx context.Context, scheduleID int64, scheduledForKey string) (runID string, runIndexOut int, found bool, err error) {
	err = r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT f.run_id, r.run_index
		FROM cron_schedule_fires f
		JOIN job_runs r ON r.run_id = f.run_id
		WHERE f.schedule_id = ? AND f.scheduled_for = ?
	`), scheduleID, scheduledForKey).Scan(&runID, &runIndexOut)

	if err == nil {
		return runID, runIndexOut, true, nil
	}

	if err != sql.ErrNoRows {
		return "", 0, false, normalizeSQLError(err)
	}

	return "", 0, false, nil
}

func (r *SQLRunsRepository) tryCreateScheduledRun(ctx context.Context, scheduleID int64, scheduledForKey, jobID string, definitionVersion int, audit RunAuditMetadata) (runID string, runIndexOut int, created bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, false, err
	}
	defer func() { _ = tx.Rollback() }()

	runID = uuid.New().String()
	runIndexOut, err = createRunTx(ctx, tx, runID, jobID, nil, definitionVersion, r.currentCellID(), audit)
	if err != nil {
		return "", 0, false, err
	}

	res, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO cron_schedule_fires (schedule_id, scheduled_for, run_id)
		VALUES (?, ?, ?)
		ON CONFLICT(schedule_id, scheduled_for) DO NOTHING
	`), scheduleID, scheduledForKey, runID)

	if err != nil {
		return "", 0, false, normalizeSQLError(err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return "", 0, false, err
	}

	if rows == 0 {
		return "", 0, false, nil
	}

	if err = tx.Commit(); err != nil {
		return "", 0, false, err
	}

	return runID, runIndexOut, true, nil
}

func createRunTx(ctx context.Context, tx *sql.Tx, runID, jobID string, runIndex *int, definitionVersion int, targetCellID string, audit RunAuditMetadata) (int, error) {
	var idx int
	if runIndex != nil {
		idx = *runIndex
	} else {
		if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"), jobID).Scan(&idx); err != nil {
			return 0, normalizeSQLError(err)
		}
	}

	definitionHash, err := lookupDefinitionHashTx(ctx, tx, jobID, definitionVersion)
	if err != nil {
		return 0, err
	}

	targetCellID = normalizeTargetCellID(targetCellID, DefaultCellID)
	triggerInvocationID := strings.TrimSpace(audit.TriggerInvocationID)
	executionPayloadHash := strings.TrimSpace(audit.ExecutionPayloadHash)
	replayOfRunID := strings.TrimSpace(audit.ReplayOfRunID)

	_, err = tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?, ?, ?)`),
		runID,
		jobID,
		idx,
		RunStatusQueued,
		definitionVersion,
		definitionHash,
		targetCellID,
		nullableString(replayOfRunID),
		nullableString(triggerInvocationID),
		executionPayloadHash,
	)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	if err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID); err != nil {
		return 0, err
	}

	return idx, nil
}

func (r *SQLRunsRepository) CreateReplayRun(ctx context.Context, sourceRunID string, targetCellID string, audit RunAuditMetadata) (CreatedRun, error) {
	sourceRunID = strings.TrimSpace(sourceRunID)
	if sourceRunID == "" {
		return CreatedRun{}, fmt.Errorf("%w: source_run_id is required", ErrNotFound)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return CreatedRun{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var jobID, sourceStatus, sourceOwningCell string
	var definitionVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT job_id, status, definition_version, owning_cell FROM job_runs WHERE run_id = ?"),
		sourceRunID,
	).Scan(&jobID, &sourceStatus, &definitionVersion, &sourceOwningCell); err != nil {
		if err == sql.ErrNoRows {
			return CreatedRun{}, fmt.Errorf("%w: source run %s", ErrNotFound, sourceRunID)
		}

		return CreatedRun{}, normalizeSQLError(err)
	}

	if sourceStatus == RunStatusQueued || sourceStatus == RunStatusRunning {
		return CreatedRun{}, fmt.Errorf("%w: source run %s in status %s cannot be replayed", ErrConflict, sourceRunID, sourceStatus)
	}

	targetCellID = normalizeTargetCellID(targetCellID, sourceOwningCell)
	if targetCellID == "" {
		targetCellID = r.currentCellID()
	}

	definitionHash, err := lookupDefinitionHashTx(ctx, tx, jobID, definitionVersion)
	if err != nil {
		return CreatedRun{}, err
	}

	if definitionHash == "" {
		return CreatedRun{}, fmt.Errorf("%w: definition version %d for job %s", ErrNotFound, definitionVersion, jobID)
	}

	var idx int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"), jobID).Scan(&idx); err != nil {
		return CreatedRun{}, normalizeSQLError(err)
	}

	runID := uuid.New().String()
	replayOfRunID := strings.TrimSpace(audit.ReplayOfRunID)
	if replayOfRunID == "" {
		replayOfRunID = sourceRunID
	}

	_, err = tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?, ?, ?)`),
		runID,
		jobID,
		idx,
		RunStatusQueued,
		definitionVersion,
		definitionHash,
		targetCellID,
		replayOfRunID,
		nullableString(audit.TriggerInvocationID),
		strings.TrimSpace(audit.ExecutionPayloadHash),
	)

	if err != nil {
		return CreatedRun{}, normalizeSQLError(err)
	}

	if err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID); err != nil {
		return CreatedRun{}, err
	}

	if err := tx.Commit(); err != nil {
		return CreatedRun{}, err
	}

	return CreatedRun{RunID: runID, RunIndex: idx, TargetCellID: targetCellID}, nil
}

func (r *SQLRunsRepository) RecordExecutionPayload(ctx context.Context, runID, payloadJSON, definitionHash string) (string, string, error) {
	runID = strings.TrimSpace(runID)
	definitionHash = strings.TrimSpace(definitionHash)
	if runID == "" {
		return "", "", fmt.Errorf("%w: run_id is required", ErrConflict)
	}

	if strings.TrimSpace(payloadJSON) == "" {
		return "", "", fmt.Errorf("%w: payload_json is required", ErrConflict)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback() }()

	lookupPayload := func(payloadHash string) (string, error) {
		var existingPayload string
		if err := tx.QueryRowContext(ctx,
			rebindQueryForPgx("SELECT payload_json FROM execution_payloads WHERE payload_hash = ?"),
			payloadHash,
		).Scan(&existingPayload); err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("%w: execution payload %s", ErrNotFound, payloadHash)
			}

			return "", normalizeSQLError(err)
		}

		return existingPayload, nil
	}

	var currentPayloadHash string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT execution_payload_hash FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&currentPayloadHash); err != nil {
		if err == sql.ErrNoRows {
			return "", "", fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return "", "", normalizeSQLError(err)
	}

	if currentPayloadHash != "" {
		recordedPayloadJSON, err := lookupPayload(currentPayloadHash)
		if err != nil {
			return "", "", err
		}

		if err := tx.Commit(); err != nil {
			return "", "", err
		}

		return currentPayloadHash, recordedPayloadJSON, nil
	}

	payloadHash := ExecutionPayloadHash(payloadJSON)
	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO execution_payloads (payload_hash, payload_json, definition_hash)
		VALUES (?, ?, ?)
		ON CONFLICT(payload_hash) DO NOTHING
	`), payloadHash, payloadJSON, definitionHash); err != nil {
		return "", "", normalizeSQLError(err)
	}

	existingPayload, err := lookupPayload(payloadHash)
	if err != nil {
		return "", "", err
	}

	if existingPayload != payloadJSON {
		return "", "", fmt.Errorf("%w: execution payload hash %s has different payload", ErrConflict, payloadHash)
	}

	result, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE job_runs SET execution_payload_hash = ? WHERE run_id = ? AND execution_payload_hash = ''"),
		payloadHash,
		runID,
	)
	if err != nil {
		return "", "", normalizeSQLError(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return "", "", normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		if err := tx.QueryRowContext(ctx,
			rebindQueryForPgx("SELECT execution_payload_hash FROM job_runs WHERE run_id = ?"),
			runID,
		).Scan(&currentPayloadHash); err != nil {
			return "", "", normalizeSQLError(err)
		}

		if currentPayloadHash == "" {
			return "", "", fmt.Errorf("%w: execution payload not recorded for run %s", ErrConflict, runID)
		}

		recordedPayloadJSON, err := lookupPayload(currentPayloadHash)
		if err != nil {
			return "", "", err
		}

		if err := tx.Commit(); err != nil {
			return "", "", err
		}

		return currentPayloadHash, recordedPayloadJSON, nil
	}

	if err := tx.Commit(); err != nil {
		return "", "", err
	}

	return payloadHash, payloadJSON, nil
}

func (r *SQLRunsRepository) GetExecutionPayloadForRun(ctx context.Context, runID string) (ExecutionPayloadRecord, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ExecutionPayloadRecord{}, fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	var rec ExecutionPayloadRecord
	var payloadJSON, definitionHash sql.NullString
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT jr.run_id, jr.execution_payload_hash, ep.payload_json, ep.definition_hash
		FROM job_runs jr
		LEFT JOIN execution_payloads ep ON ep.payload_hash = jr.execution_payload_hash
		WHERE jr.run_id = ?
	`), runID).Scan(&rec.RunID, &rec.PayloadHash, &payloadJSON, &definitionHash); err != nil {

		if err == sql.ErrNoRows {
			return ExecutionPayloadRecord{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return ExecutionPayloadRecord{}, normalizeSQLError(err)
	}

	if strings.TrimSpace(rec.PayloadHash) == "" || !payloadJSON.Valid {
		return ExecutionPayloadRecord{}, fmt.Errorf("%w: execution payload for run %s", ErrNotFound, runID)
	}

	rec.PayloadJSON = payloadJSON.String
	if definitionHash.Valid {
		rec.DefinitionHash = definitionHash.String
	}

	return rec, nil
}

func (r *SQLRunsRepository) GetExecutionPayloadByHash(ctx context.Context, payloadHash string) (ExecutionPayloadRecord, error) {
	payloadHash = strings.TrimSpace(payloadHash)
	if payloadHash == "" {
		return ExecutionPayloadRecord{}, fmt.Errorf("%w: payload_hash is required", ErrNotFound)
	}

	var rec ExecutionPayloadRecord
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT payload_hash, payload_json, definition_hash
		FROM execution_payloads
		WHERE payload_hash = ?
	`), payloadHash).Scan(&rec.PayloadHash, &rec.PayloadJSON, &rec.DefinitionHash); err != nil {

		if err == sql.ErrNoRows {
			return ExecutionPayloadRecord{}, fmt.Errorf("%w: execution payload %s", ErrNotFound, payloadHash)
		}

		return ExecutionPayloadRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	return value
}

func lookupDefinitionHashTx(ctx context.Context, tx *sql.Tx, jobID string, version int) (string, error) {
	var hash string
	var definitionJSON string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_hash, definition_json FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&hash, &definitionJSON); err == nil {
		if hash == "" {
			hash = DefinitionHash(definitionJSON)
		}

		return hash, nil
	} else if err == sql.ErrNoRows {
		return "", nil
	} else {
		return "", normalizeSQLError(err)
	}
}

func (r *SQLRunsRepository) ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]RunRecord, int64, error) {
	query := `
		SELECT
			jr.id,
			jr.run_id,
			jr.run_index,
			jr.status,
			jr.orphan_reason,
			jr.failure_code,
			CAST(jr.created_at AS TEXT),
			CAST(jr.started_at AS TEXT),
			CAST(jr.finished_at AS TEXT),
			jr.failure_reason,
			jr.definition_version,
			jr.definition_hash,
			jr.owning_cell,
			jr.replay_of_run_id,
			jr.trigger_invocation_id,
			jr.execution_payload_hash,
			ti.trigger_id,
			ti.trigger_type,
			ti.trigger_payload_hash,
			ti.requested_cells
		FROM job_runs jr
		LEFT JOIN trigger_invocations ti ON ti.invocation_id = jr.trigger_invocation_id
		WHERE jr.job_id = ?`
	args := []any{jobID}

	if afterIndex != nil {
		query += " AND jr.run_index > ?"
		args = append(args, *afterIndex)
	}

	if since != nil {
		query += " AND jr.created_at >= ?"
		args = append(args, since.UTC().Format("2006-01-02 15:04:05"))
	}

	if owningCell = strings.TrimSpace(owningCell); owningCell != "" {
		query += " AND jr.owning_cell = ?"
		args = append(args, owningCell)
	}

	if cursor > 0 {
		query += " AND jr.id > ?"
		args = append(args, cursor)
	}

	query += " ORDER BY jr.id ASC LIMIT ?"
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RunRecord
	var lastID int64
	for rows.Next() {
		var rec RunRecord
		var id int64
		var orphanReason, failureCode, createdAt, startedAt, finishedAt, failureReason sql.NullString
		var replayOfRunID, triggerInvocationID, triggerType, triggerPayloadHash, requestedCells sql.NullString
		var triggerID sql.NullInt64
		if err := rows.Scan(&id, &rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &createdAt, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell, &replayOfRunID, &triggerInvocationID, &rec.ExecutionPayloadHash, &triggerID, &triggerType, &triggerPayloadHash, &requestedCells); err != nil {
			return nil, 0, normalizeSQLError(err)
		}

		if err := applyRunAuditFields(&rec, replayOfRunID, triggerInvocationID, triggerID, triggerType, triggerPayloadHash, requestedCells); err != nil {
			return nil, 0, err
		}

		lastID = id
		if orphanReason.Valid && orphanReason.String != "" {
			rec.OrphanReason = &orphanReason.String
		}

		if createdAt.Valid {
			rec.CreatedAt = &createdAt.String
		}

		if startedAt.Valid {
			rec.StartedAt = &startedAt.String
		}

		if finishedAt.Valid {
			rec.FinishedAt = &finishedAt.String
		}

		if failureCode.Valid && failureCode.String != "" {
			rec.FailureCode = &failureCode.String
		}

		if failureReason.Valid {
			rec.FailureReason = &failureReason.String
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, normalizeSQLError(err)
	}

	var nextCursor int64
	if len(out) > limit {
		out = out[:limit]
		nextCursor = lastID
	}

	return out, nextCursor, nil
}

func (r *SQLRunsRepository) ListRunTasks(ctx context.Context, runID string, cursor int64, limit int) ([]TaskRecord, int64, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, 0, fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		WITH page_tasks AS (
			SELECT
				id,
				task_id,
				run_id,
				parent_task_id,
				task_key,
				name,
				status,
				spec_hash,
				created_at,
				updated_at
			FROM run_tasks
			WHERE run_id = ?
				AND (? <= 0 OR id > ?)
			ORDER BY id ASC
			LIMIT ?
		)
		SELECT
			rt.id,
			rt.task_id,
			rt.run_id,
			rt.parent_task_id,
			rt.task_key,
			rt.name,
			rt.status,
			rt.spec_hash,
			CAST(rt.created_at AS TEXT),
			CAST(rt.updated_at AS TEXT),
			ta.attempt_id,
			ta.task_id,
			ta.run_id,
			se.execution_id,
			se.status,
			ta.cell_id,
			se.lease_owner,
			se.lease_until,
			ta.attempt,
			ta.status,
			CAST(ta.accepted_at AS TEXT),
			CAST(ta.started_at AS TEXT),
			CAST(ta.finished_at AS TEXT),
			ta.last_observed_at,
			ta.event_sequence,
			CAST(ta.created_at AS TEXT),
			CAST(ta.updated_at AS TEXT)
		FROM page_tasks rt
		LEFT JOIN task_attempts ta ON ta.task_id = rt.task_id
		LEFT JOIN segment_executions se ON se.task_attempt_id = ta.attempt_id AND se.task_id = ta.task_id AND se.run_id = ta.run_id AND se.attempt = ta.attempt
		ORDER BY rt.id ASC, ta.attempt ASC, ta.id ASC
	`), runID, cursor, cursor, limit+1)

	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	byTaskID := map[string]int{}
	var out []TaskRecord
	for rows.Next() {
		var rec TaskRecord
		var parentTaskID, createdAt, updatedAt sql.NullString
		var attemptID, attemptTaskID, attemptRunID, executionID, executionStatus, cellID, leaseOwner, attemptStatus sql.NullString
		var attempt sql.NullInt64
		var acceptedAt, startedAt, finishedAt, attemptCreatedAt, attemptUpdatedAt sql.NullString
		var leaseUntil, lastObservedAt, eventSequence sql.NullInt64
		if err := rows.Scan(
			&rec.ID,
			&rec.TaskID,
			&rec.RunID,
			&parentTaskID,
			&rec.TaskKey,
			&rec.Name,
			&rec.Status,
			&rec.SpecHash,
			&createdAt,
			&updatedAt,
			&attemptID,
			&attemptTaskID,
			&attemptRunID,
			&executionID,
			&executionStatus,
			&cellID,
			&leaseOwner,
			&leaseUntil,
			&attempt,
			&attemptStatus,
			&acceptedAt,
			&startedAt,
			&finishedAt,
			&lastObservedAt,
			&eventSequence,
			&attemptCreatedAt,
			&attemptUpdatedAt,
		); err != nil {
			return nil, 0, normalizeSQLError(err)
		}

		idx, ok := byTaskID[rec.TaskID]
		if !ok {
			rec.ParentTaskID = nullStringPtr(parentTaskID)
			rec.CreatedAt = nullStringPtr(createdAt)
			rec.UpdatedAt = nullStringPtr(updatedAt)
			out = append(out, rec)
			idx = len(out) - 1
			byTaskID[rec.TaskID] = idx
		}

		if !attemptID.Valid {
			continue
		}

		attemptRec := TaskAttemptRecord{
			AttemptID:       attemptID.String,
			TaskID:          attemptTaskID.String,
			RunID:           attemptRunID.String,
			ExecutionID:     executionID.String,
			ExecutionStatus: executionStatus.String,
			CellID:          cellID.String,
			LeaseOwner:      nullStringPtr(leaseOwner),
			LeaseUntil:      nullInt64Ptr(leaseUntil),
			Status:          attemptStatus.String,
			AcceptedAt:      nullStringPtr(acceptedAt),
			StartedAt:       nullStringPtr(startedAt),
			FinishedAt:      nullStringPtr(finishedAt),
			LastObservedAt:  nullInt64Ptr(lastObservedAt),
			EventSequence:   eventSequence.Int64,
			CreatedAt:       nullStringPtr(attemptCreatedAt),
			UpdatedAt:       nullStringPtr(attemptUpdatedAt),
		}
		if attempt.Valid {
			attemptRec.Attempt = int(attempt.Int64)
		}

		out[idx].Attempts = append(out[idx].Attempts, attemptRec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, normalizeSQLError(err)
	}

	if len(out) == 0 {
		var existingRunID string
		err := r.db.QueryRowContext(ctx, rebindQueryForPgx("SELECT run_id FROM job_runs WHERE run_id = ?"), runID).Scan(&existingRunID)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, 0, fmt.Errorf("%w: run %s", ErrNotFound, runID)
			}

			return nil, 0, normalizeSQLError(err)
		}

		return []TaskRecord{}, 0, nil
	}

	var nextCursor int64
	if len(out) > limit {
		nextCursor = out[limit-1].ID
		out = out[:limit]
	}

	return out, nextCursor, nil
}

func (r *SQLRunsRepository) EnsurePlannedTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error) {
	return r.ensureTaskExecution(ctx, create, TaskStatusPlanned, SegmentStatusPlanned, ExecutionStatusPlanned)
}

func (r *SQLRunsRepository) EnsurePendingTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error) {
	return r.ensureTaskExecution(ctx, create, TaskStatusPending, SegmentStatusPending, ExecutionStatusPending)
}

type taskExecutionStatusSnapshot struct {
	Record          TaskExecutionRecord
	TaskStatus      string
	AttemptStatus   string
	SegmentStatus   string
	ExecutionStatus string
}

func (r *SQLRunsRepository) ActivatePlannedTaskExecution(ctx context.Context, taskID string) (TaskExecutionRecord, bool, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: task_id is required", ErrNotFound)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	snapshot, err := taskExecutionStatusSnapshotByTaskIDTx(ctx, tx, taskID)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}

	if snapshot.hasStatuses(TaskStatusPending, SegmentStatusPending, ExecutionStatusPending) {
		return snapshot.Record, false, nil
	}

	activated, err := activatePlannedTaskExecutionSnapshotTx(ctx, tx, snapshot)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return TaskExecutionRecord{}, false, err
	}

	return snapshot.Record, activated, nil
}

func (r *SQLRunsRepository) ActivatePlannedChildTaskExecutions(ctx context.Context, parentTaskID string) ([]TaskExecutionRecord, int, error) {
	parentTaskID = strings.TrimSpace(parentTaskID)
	if parentTaskID == "" {
		return nil, 0, fmt.Errorf("%w: parent_task_id is required", ErrNotFound)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	records, activatedCount, err := activatePlannedChildTaskExecutionsTx(ctx, tx, parentTaskID)
	if err != nil {
		return nil, 0, err
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}

	return records, activatedCount, nil
}

func (r *SQLRunsRepository) GetRunTaskCompletion(ctx context.Context, runID string) (RunTaskCompletion, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RunTaskCompletion{}, fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	return getRunTaskCompletion(ctx, r.db, runID)
}

type runTaskCompletionQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getRunTaskCompletionTx(ctx context.Context, tx *sql.Tx, runID string) (RunTaskCompletion, error) {
	return getRunTaskCompletion(ctx, tx, runID)
}

func getRunTaskCompletion(ctx context.Context, q runTaskCompletionQueryer, runID string) (RunTaskCompletion, error) {
	var summary RunTaskCompletion
	summary.RunID = runID
	if err := q.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			COUNT(rt.task_id),
			COALESCE(SUM(CASE WHEN rt.status = ? THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN rt.status IN (?, ?, ?) THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN rt.status NOT IN (?, ?, ?, ?) THEN 1 ELSE 0 END), 0)
		FROM job_runs jr
		LEFT JOIN run_tasks rt ON rt.run_id = jr.run_id
		WHERE jr.run_id = ?
		GROUP BY jr.run_id
	`),
		TaskStatusSucceeded,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		TaskStatusSucceeded, TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		runID,
	).Scan(&summary.Total, &summary.Succeeded, &summary.TerminalFailed, &summary.Incomplete); err != nil {
		if err == sql.ErrNoRows {
			return RunTaskCompletion{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return RunTaskCompletion{}, normalizeSQLError(err)
	}

	return summary, nil
}

func (r *SQLRunsRepository) ListOrphanedTaskFinalizationCandidates(ctx context.Context, limit int) ([]RunTaskCompletion, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT run_id, total, succeeded, terminal_failed, incomplete
		FROM (
			SELECT
				jr.id,
				jr.run_id,
				COUNT(rt.task_id) AS total,
				COALESCE(SUM(CASE WHEN rt.status = ? THEN 1 ELSE 0 END), 0) AS succeeded,
				COALESCE(SUM(CASE WHEN rt.status IN (?, ?, ?) THEN 1 ELSE 0 END), 0) AS terminal_failed,
				COALESCE(SUM(CASE WHEN rt.status NOT IN (?, ?, ?, ?) THEN 1 ELSE 0 END), 0) AS incomplete
			FROM job_runs jr
			JOIN run_tasks rt ON rt.run_id = jr.run_id
			WHERE jr.status = ?
			GROUP BY jr.id, jr.run_id
		) summary
		WHERE total > 0
			AND (terminal_failed > 0 OR succeeded = total)
		ORDER BY id ASC
		LIMIT ?
	`),
		TaskStatusSucceeded,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		TaskStatusSucceeded, TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		RunStatusOrphaned,
		limit,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RunTaskCompletion
	for rows.Next() {
		var summary RunTaskCompletion
		if err := rows.Scan(&summary.RunID, &summary.Total, &summary.Succeeded, &summary.TerminalFailed, &summary.Incomplete); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, summary)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLRunsRepository) CountOrphanedTaskFinalizationCandidates(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT COUNT(*)
		FROM (
			SELECT jr.id
			FROM job_runs jr
			JOIN run_tasks rt ON rt.run_id = jr.run_id
			WHERE jr.status = ?
			GROUP BY jr.id
			HAVING COUNT(rt.task_id) > 0
				AND (
					COALESCE(SUM(CASE WHEN rt.status IN (?, ?, ?) THEN 1 ELSE 0 END), 0) > 0
					OR COALESCE(SUM(CASE WHEN rt.status = ? THEN 1 ELSE 0 END), 0) = COUNT(rt.task_id)
				)
		) candidates
	`),
		RunStatusOrphaned,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		TaskStatusSucceeded,
	).Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLRunsRepository) CountOrphanedTaskFinalizationCandidatesByCell(ctx context.Context) ([]RunCountByCell, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT owning_cell, COUNT(*)
		FROM (
			SELECT jr.id, jr.owning_cell
			FROM job_runs jr
			JOIN run_tasks rt ON rt.run_id = jr.run_id
			WHERE jr.status = ?
			GROUP BY jr.id, jr.owning_cell
			HAVING COUNT(rt.task_id) > 0
				AND (
					COALESCE(SUM(CASE WHEN rt.status IN (?, ?, ?) THEN 1 ELSE 0 END), 0) > 0
					OR COALESCE(SUM(CASE WHEN rt.status = ? THEN 1 ELSE 0 END), 0) = COUNT(rt.task_id)
				)
		) candidates
		GROUP BY owning_cell
		ORDER BY owning_cell ASC
	`),
		RunStatusOrphaned,
		TaskStatusFailed, TaskStatusCancelled, TaskStatusAborted,
		TaskStatusSucceeded,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var counts []RunCountByCell
	for rows.Next() {
		var count RunCountByCell
		if err := rows.Scan(&count.CellID, &count.Count); err != nil {
			return nil, normalizeSQLError(err)
		}

		counts = append(counts, count)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return counts, nil
}

func activatePlannedChildTaskExecutionsTx(ctx context.Context, tx *sql.Tx, parentTaskID string) ([]TaskExecutionRecord, int, error) {
	if err := ensureTaskExistsTx(ctx, tx, parentTaskID); err != nil {
		return nil, 0, err
	}

	snapshots, err := taskExecutionStatusSnapshotsByParentTaskIDTx(ctx, tx, parentTaskID)
	if err != nil {
		return nil, 0, err
	}

	records := make([]TaskExecutionRecord, 0, len(snapshots))
	activatedCount := 0
	for _, snapshot := range snapshots {
		switch {
		case snapshot.hasStatuses(TaskStatusPlanned, SegmentStatusPlanned, ExecutionStatusPlanned):
			activated, err := activatePlannedTaskExecutionSnapshotTx(ctx, tx, snapshot)
			if err != nil {
				return nil, 0, err
			}

			if activated {
				activatedCount++
			}
			records = append(records, snapshot.Record)
		case snapshot.hasStatuses(TaskStatusPending, SegmentStatusPending, ExecutionStatusPending):
			records = append(records, snapshot.Record)
		case snapshot.hasConsistentAdvancedStatus():
			continue
		default:
			return nil, 0, fmt.Errorf(
				"%w: child task %s statuses task=%s attempt=%s segment=%s execution=%s cannot activate",
				ErrConflict,
				snapshot.Record.TaskID,
				snapshot.TaskStatus,
				snapshot.AttemptStatus,
				snapshot.SegmentStatus,
				snapshot.ExecutionStatus,
			)
		}
	}

	return records, activatedCount, nil
}

func activatePlannedTaskExecutionSnapshotTx(ctx context.Context, tx *sql.Tx, snapshot taskExecutionStatusSnapshot) (bool, error) {
	if !snapshot.hasStatuses(TaskStatusPlanned, SegmentStatusPlanned, ExecutionStatusPlanned) {
		return false, fmt.Errorf(
			"%w: task %s statuses task=%s attempt=%s segment=%s execution=%s cannot activate",
			ErrConflict,
			snapshot.Record.TaskID,
			snapshot.TaskStatus,
			snapshot.AttemptStatus,
			snapshot.SegmentStatus,
			snapshot.ExecutionStatus,
		)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE run_tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?"),
		TaskStatusPending,
		snapshot.Record.TaskID,
	); err != nil {
		return false, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE task_attempts SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE attempt_id = ?"),
		TaskStatusPending,
		snapshot.Record.TaskAttemptID,
	); err != nil {
		return false, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE run_segments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ?"),
		SegmentStatusPending,
		snapshot.Record.SegmentID,
	); err != nil {
		return false, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE segment_executions SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE execution_id = ?"),
		ExecutionStatusPending,
		snapshot.Record.ExecutionID,
	); err != nil {
		return false, normalizeSQLError(err)
	}

	return true, nil
}

func taskExecutionStatusSnapshotByTaskIDTx(ctx context.Context, tx *sql.Tx, taskID string) (taskExecutionStatusSnapshot, error) {
	snapshot, err := scanTaskExecutionStatusSnapshot(tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			rt.run_id,
			rt.task_id,
			rt.parent_task_id,
			rt.task_key,
			rt.name,
			ta.attempt_id,
			rs.segment_id,
			rs.name,
			se.execution_id,
			se.cell_id,
			se.attempt,
			rt.status,
			ta.status,
			rs.status,
			se.status
		FROM run_tasks rt
		JOIN task_attempts ta ON ta.task_id = rt.task_id AND ta.run_id = rt.run_id
		JOIN segment_executions se ON se.task_id = rt.task_id AND se.task_attempt_id = ta.attempt_id AND se.run_id = rt.run_id AND se.attempt = ta.attempt
		JOIN run_segments rs ON rs.segment_id = se.segment_id AND rs.run_id = rt.run_id
		WHERE rt.task_id = ?
		ORDER BY ta.attempt ASC
		LIMIT 1
	`), taskID))
	if err != nil {
		if err == sql.ErrNoRows {
			return taskExecutionStatusSnapshot{}, fmt.Errorf("%w: task execution %s", ErrNotFound, taskID)
		}

		return taskExecutionStatusSnapshot{}, normalizeSQLError(err)
	}

	return snapshot, nil
}

func taskExecutionStatusSnapshotsByParentTaskIDTx(ctx context.Context, tx *sql.Tx, parentTaskID string) ([]taskExecutionStatusSnapshot, error) {
	rows, err := tx.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			rt.run_id,
			rt.task_id,
			rt.parent_task_id,
			rt.task_key,
			rt.name,
			ta.attempt_id,
			rs.segment_id,
			rs.name,
			se.execution_id,
			se.cell_id,
			se.attempt,
			rt.status,
			ta.status,
			rs.status,
			se.status
		FROM run_tasks rt
		JOIN task_attempts ta ON ta.task_id = rt.task_id AND ta.run_id = rt.run_id
		JOIN segment_executions se ON se.task_id = rt.task_id AND se.task_attempt_id = ta.attempt_id AND se.run_id = rt.run_id AND se.attempt = ta.attempt
		JOIN run_segments rs ON rs.segment_id = se.segment_id AND rs.run_id = rt.run_id
		WHERE rt.parent_task_id = ?
		ORDER BY rt.id ASC, ta.attempt ASC
	`), parentTaskID)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []taskExecutionStatusSnapshot
	for rows.Next() {
		snapshot, err := scanTaskExecutionStatusSnapshot(rows)
		if err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, snapshot)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

type taskExecutionStatusScanner interface {
	Scan(dest ...any) error
}

func scanTaskExecutionStatusSnapshot(scanner taskExecutionStatusScanner) (taskExecutionStatusSnapshot, error) {
	var snapshot taskExecutionStatusSnapshot
	var parentTaskID sql.NullString
	if err := scanner.Scan(
		&snapshot.Record.RunID,
		&snapshot.Record.TaskID,
		&parentTaskID,
		&snapshot.Record.TaskKey,
		&snapshot.Record.Name,
		&snapshot.Record.TaskAttemptID,
		&snapshot.Record.SegmentID,
		&snapshot.Record.SegmentName,
		&snapshot.Record.ExecutionID,
		&snapshot.Record.CellID,
		&snapshot.Record.Attempt,
		&snapshot.TaskStatus,
		&snapshot.AttemptStatus,
		&snapshot.SegmentStatus,
		&snapshot.ExecutionStatus,
	); err != nil {
		return taskExecutionStatusSnapshot{}, err
	}

	snapshot.Record.ParentTaskID = nullStringValue(parentTaskID)
	return snapshot, nil
}

func ensureTaskExistsTx(ctx context.Context, tx *sql.Tx, taskID string) error {
	var found string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT task_id FROM run_tasks WHERE task_id = ?"), taskID).Scan(&found); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: task %s", ErrNotFound, taskID)
		}

		return normalizeSQLError(err)
	}

	return nil
}

func (s taskExecutionStatusSnapshot) hasStatuses(taskStatus, segmentStatus, executionStatus string) bool {
	return s.TaskStatus == taskStatus &&
		s.AttemptStatus == taskStatus &&
		s.SegmentStatus == segmentStatus &&
		s.ExecutionStatus == executionStatus
}

func (s taskExecutionStatusSnapshot) hasConsistentAdvancedStatus() bool {
	if !s.hasStatuses(s.TaskStatus, s.TaskStatus, s.TaskStatus) {
		return false
	}

	return s.TaskStatus != TaskStatusPlanned && s.TaskStatus != TaskStatusPending
}

func (r *SQLRunsRepository) ensureTaskExecution(ctx context.Context, create TaskExecutionCreate, taskStatus, segmentStatus, executionStatus string) (TaskExecutionRecord, bool, error) {
	normalized, err := normalizeTaskExecutionCreate(create)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	var owningCell string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT owning_cell FROM job_runs WHERE run_id = ?"), normalized.RunID).Scan(&owningCell); err != nil {
		if err == sql.ErrNoRows {
			return TaskExecutionRecord{}, false, fmt.Errorf("%w: run %s", ErrNotFound, normalized.RunID)
		}

		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	parentTaskID := normalized.ParentTaskID
	if parentTaskID == "" {
		parentTaskID = rootTaskID(normalized.RunID)
	}

	var parentRunID string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT run_id FROM run_tasks WHERE task_id = ?"), parentTaskID).Scan(&parentRunID); err != nil {
		if err == sql.ErrNoRows {
			return TaskExecutionRecord{}, false, fmt.Errorf("%w: parent task %s", ErrNotFound, parentTaskID)
		}

		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	if parentRunID != normalized.RunID {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: parent task %s belongs to run %s", ErrConflict, parentTaskID, parentRunID)
	}

	cellID := normalizeTargetCellID(normalized.TargetCellID, owningCell)
	taskID := taskIDForKey(normalized.RunID, normalized.TaskKey)
	attempt := 1
	attemptID := taskAttemptID(taskID, attempt)
	segmentID := taskSegmentID(taskID)
	executionID := taskExecutionID(attemptID)

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO run_tasks (task_id, run_id, parent_task_id, task_key, name, status, spec_hash)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_id) DO NOTHING
	`), taskID, normalized.RunID, parentTaskID, normalized.TaskKey, normalized.Name, taskStatus, normalized.SpecHash); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	var storedRunID, storedTaskKey, storedName, storedSpecHash string
	var storedParentTaskID sql.NullString
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT run_id, parent_task_id, task_key, name, spec_hash
		FROM run_tasks
		WHERE task_id = ?
	`), taskID).Scan(&storedRunID, &storedParentTaskID, &storedTaskKey, &storedName, &storedSpecHash); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	if storedRunID != normalized.RunID || nullStringValue(storedParentTaskID) != parentTaskID || storedTaskKey != normalized.TaskKey || storedName != normalized.Name || storedSpecHash != normalized.SpecHash {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: task %s has different payload", ErrConflict, taskID)
	}

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO task_attempts (attempt_id, task_id, run_id, cell_id, status, attempt)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_id, attempt) DO NOTHING
	`), attemptID, taskID, normalized.RunID, cellID, taskStatus, attempt); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	var storedAttemptID, attemptRunID, attemptTaskID, attemptCellID string
	var storedAttempt int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT attempt_id, run_id, task_id, cell_id, attempt
		FROM task_attempts
		WHERE task_id = ? AND attempt = ?
	`), taskID, attempt).Scan(&storedAttemptID, &attemptRunID, &attemptTaskID, &attemptCellID, &storedAttempt); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	if storedAttemptID != attemptID || attemptRunID != normalized.RunID || attemptTaskID != taskID || attemptCellID != cellID || storedAttempt != attempt {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: task attempt %s has different payload", ErrConflict, attemptID)
	}

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO run_segments (segment_id, run_id, name, status)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(segment_id) DO NOTHING
	`), segmentID, normalized.RunID, normalized.Name, segmentStatus); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	var segmentRunID, segmentName string
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT run_id, name
		FROM run_segments
		WHERE segment_id = ?
	`), segmentID).Scan(&segmentRunID, &segmentName); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	if segmentRunID != normalized.RunID || segmentName != normalized.Name {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: segment %s has different payload", ErrConflict, segmentID)
	}

	result, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO segment_executions (execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(task_attempt_id) DO NOTHING
	`), executionID, segmentID, normalized.RunID, taskID, attemptID, cellID, executionStatus, attempt)
	if err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	created, err := insertedReceipt(result)
	if err != nil {
		return TaskExecutionRecord{}, false, err
	}

	var storedExecutionID, executionSegmentID, executionRunID, executionTaskID, executionTaskAttemptID, executionCellID string
	var executionAttempt int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, attempt
		FROM segment_executions
		WHERE task_attempt_id = ?
	`), attemptID).Scan(&storedExecutionID, &executionSegmentID, &executionRunID, &executionTaskID, &executionTaskAttemptID, &executionCellID, &executionAttempt); err != nil {
		return TaskExecutionRecord{}, false, normalizeSQLError(err)
	}

	if storedExecutionID != executionID || executionSegmentID != segmentID || executionRunID != normalized.RunID || executionTaskID != taskID || executionTaskAttemptID != attemptID || executionCellID != cellID || executionAttempt != attempt {
		return TaskExecutionRecord{}, false, fmt.Errorf("%w: execution %s has different payload", ErrConflict, executionID)
	}

	if err := tx.Commit(); err != nil {
		return TaskExecutionRecord{}, false, err
	}

	return TaskExecutionRecord{
		RunID:         normalized.RunID,
		TaskID:        taskID,
		ParentTaskID:  parentTaskID,
		TaskKey:       normalized.TaskKey,
		Name:          normalized.Name,
		TaskAttemptID: attemptID,
		SegmentID:     segmentID,
		SegmentName:   normalized.Name,
		ExecutionID:   executionID,
		CellID:        cellID,
		Attempt:       attempt,
	}, created, nil
}

func normalizeTaskExecutionCreate(create TaskExecutionCreate) (TaskExecutionCreate, error) {
	create.RunID = strings.TrimSpace(create.RunID)
	create.ParentTaskID = strings.TrimSpace(create.ParentTaskID)
	create.TaskKey = strings.TrimSpace(create.TaskKey)
	create.Name = strings.TrimSpace(create.Name)
	create.SpecHash = strings.TrimSpace(create.SpecHash)
	create.TargetCellID = strings.TrimSpace(create.TargetCellID)

	if create.RunID == "" {
		return TaskExecutionCreate{}, fmt.Errorf("%w: run_id is required", ErrNotFound)
	}

	if create.TaskKey == "" {
		return TaskExecutionCreate{}, fmt.Errorf("%w: task_key is required", ErrConflict)
	}

	if create.TaskKey == RootTaskKey {
		return TaskExecutionCreate{}, fmt.Errorf("%w: task_key %q is reserved", ErrConflict, RootTaskKey)
	}

	if create.Name == "" {
		create.Name = create.TaskKey
	}

	return create, nil
}

func nullStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}

	return value.String
}

func nullStringPtr(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}

	v := value.String
	return &v
}

func nullInt64Ptr(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}

	v := value.Int64
	return &v
}

func (r *SQLRunsRepository) ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT run_id, job_id, definition_version, definition_hash, owning_cell
		FROM job_runs
		WHERE status = 'queued'
			AND (last_dispatched_at IS NULL OR last_dispatched_at < ?)
			AND NOT EXISTS (
				SELECT 1
				FROM task_dispatch_intents tdi
				WHERE tdi.run_id = job_runs.run_id
					AND tdi.enqueued_at IS NULL
			)
		ORDER BY id ASC
	`), cutoffUnix)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []QueuedRun
	for rows.Next() {
		var rec QueuedRun
		if err := rows.Scan(&rec.RunID, &rec.JobID, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLRunsRepository) GetPendingExecution(ctx context.Context, runID string) (ExecutionDispatchRecord, error) {
	rec, err := scanExecutionDispatchRecord(r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			jr.run_id,
			jr.job_id,
			COALESCE(ns.path, '/'),
			jr.run_index,
			rt.task_id,
			rt.task_key,
			rt.name,
			ta.attempt_id,
			rs.segment_id,
			rs.name,
			rs.status,
			se.execution_id,
			se.status,
			se.cell_id,
			se.attempt,
			jr.definition_version,
			jr.definition_hash,
			jr.owning_cell
		FROM job_runs jr
		JOIN run_segments rs ON rs.run_id = jr.run_id
		JOIN segment_executions se ON se.segment_id = rs.segment_id
		JOIN run_tasks rt ON rt.task_id = se.task_id AND rt.run_id = jr.run_id
		JOIN task_attempts ta ON ta.attempt_id = se.task_attempt_id AND ta.task_id = rt.task_id AND ta.run_id = jr.run_id AND ta.attempt = se.attempt
		LEFT JOIN stored_jobs sj ON sj.job_id = jr.job_id
		LEFT JOIN namespaces ns ON ns.id = sj.namespace_id
		WHERE jr.run_id = ?
			AND rs.status = ?
			AND se.status = ?
			AND rt.status = ?
			AND ta.status = ?
		ORDER BY rs.id ASC, se.attempt ASC, se.id ASC
		LIMIT 1
	`), runID, SegmentStatusPending, ExecutionStatusPending, TaskStatusPending, TaskStatusPending))

	if err != nil {
		if err == sql.ErrNoRows {
			return ExecutionDispatchRecord{}, fmt.Errorf("%w: pending execution for run %s", ErrNotFound, runID)
		}

		return ExecutionDispatchRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

func (r *SQLRunsRepository) GetExecutionDispatch(ctx context.Context, executionID string) (ExecutionDispatchRecord, error) {
	executionID = strings.TrimSpace(executionID)
	if executionID == "" {
		return ExecutionDispatchRecord{}, fmt.Errorf("%w: execution_id is required", ErrNotFound)
	}

	rec, err := scanExecutionDispatchRecord(r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			jr.run_id,
			jr.job_id,
			COALESCE(ns.path, '/'),
			jr.run_index,
			rt.task_id,
			rt.task_key,
			rt.name,
			ta.attempt_id,
			rs.segment_id,
			rs.name,
			rs.status,
			se.execution_id,
			se.status,
			se.cell_id,
			se.attempt,
			jr.definition_version,
			jr.definition_hash,
			jr.owning_cell
		FROM segment_executions se
		JOIN job_runs jr ON jr.run_id = se.run_id
		JOIN run_segments rs ON rs.segment_id = se.segment_id AND rs.run_id = jr.run_id
		JOIN run_tasks rt ON rt.task_id = se.task_id AND rt.run_id = jr.run_id
		JOIN task_attempts ta ON ta.attempt_id = se.task_attempt_id AND ta.task_id = rt.task_id AND ta.run_id = jr.run_id AND ta.attempt = se.attempt
		LEFT JOIN stored_jobs sj ON sj.job_id = jr.job_id
		LEFT JOIN namespaces ns ON ns.id = sj.namespace_id
		WHERE se.execution_id = ?
			AND rs.status = ?
			AND se.status = ?
			AND rt.status = ?
			AND ta.status = ?
		LIMIT 1
	`), executionID, SegmentStatusPending, ExecutionStatusPending, TaskStatusPending, TaskStatusPending))

	if err != nil {
		if err == sql.ErrNoRows {
			return ExecutionDispatchRecord{}, fmt.Errorf("%w: pending execution %s", ErrNotFound, executionID)
		}

		return ExecutionDispatchRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

type executionDispatchRecordScanner interface {
	Scan(dest ...any) error
}

func scanExecutionDispatchRecord(scanner executionDispatchRecordScanner) (ExecutionDispatchRecord, error) {
	var rec ExecutionDispatchRecord
	if err := scanner.Scan(
		&rec.RunID,
		&rec.JobID,
		&rec.NamespacePath,
		&rec.RunIndex,
		&rec.TaskID,
		&rec.TaskKey,
		&rec.TaskName,
		&rec.TaskAttemptID,
		&rec.SegmentID,
		&rec.SegmentName,
		&rec.SegmentStatus,
		&rec.ExecutionID,
		&rec.ExecutionStatus,
		&rec.CellID,
		&rec.Attempt,
		&rec.DefinitionVersion,
		&rec.DefinitionHash,
		&rec.OwningCell,
	); err != nil {
		return ExecutionDispatchRecord{}, err
	}

	return rec, nil
}

func (r *SQLRunsRepository) TryClaimExecution(ctx context.Context, executionID, owner string, leaseUntil time.Time) (ExecutionClaimResult, error) {
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	if executionID == "" || owner == "" {
		return ExecutionClaimResult{}, nil
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecutionClaimResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var runID string
	var segmentID string
	var taskID string
	var taskAttemptID string
	var attempt int
	var currentStatus string
	var runStatus string
	var currentLeaseUntil sql.NullInt64
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT se.run_id, se.segment_id, se.task_id, se.task_attempt_id, se.attempt, se.status, se.lease_until, jr.status
		FROM segment_executions se
		JOIN job_runs jr ON jr.run_id = se.run_id
		WHERE se.execution_id = ?
	`), executionID).Scan(&runID, &segmentID, &taskID, &taskAttemptID, &attempt, &currentStatus, &currentLeaseUntil, &runStatus); err != nil {
		if err == sql.ErrNoRows {
			return ExecutionClaimResult{}, nil
		}

		return ExecutionClaimResult{}, normalizeSQLError(err)
	}

	if !statusIn(currentStatus, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}) {
		return ExecutionClaimResult{}, nil
	}

	if !statusIn(runStatus, []string{RunStatusQueued, RunStatusRunning, RunStatusOrphaned}) {
		return ExecutionClaimResult{}, nil
	}

	now := time.Now().UTC()
	nowUnix := now.Unix()
	if currentLeaseUntil.Valid && currentLeaseUntil.Int64 >= nowUnix {
		return ExecutionClaimResult{}, nil
	}

	claimToken := uuid.NewString()
	transitionedToAccepted := currentStatus == ExecutionStatusPending
	setParts := []string{"lease_owner = ?", "lease_until = ?", "claim_token = ?"}
	args := []any{owner, leaseUntil.UTC().Unix(), claimToken}
	if transitionedToAccepted {
		setParts = append(setParts,
			"status = ?",
			"accepted_at = COALESCE(accepted_at, CURRENT_TIMESTAMP)",
			"last_observed_at = ?",
			"event_sequence = event_sequence + 1",
		)

		args = append(args, ExecutionStatusAccepted, now.UnixNano())
	}

	setParts = append(setParts, "updated_at = CURRENT_TIMESTAMP")
	args = append(args, executionID, currentStatus, nowUnix)
	res, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE segment_executions
		SET `+strings.Join(setParts, ", ")+`
		WHERE execution_id = ?
			AND status = ?
			AND (lease_until IS NULL OR lease_until < ?)
	`), args...)

	if err != nil {
		return ExecutionClaimResult{}, normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return ExecutionClaimResult{}, err
	}

	if n != 1 {
		return ExecutionClaimResult{}, nil
	}

	if transitionedToAccepted {
		if _, err := tx.ExecContext(ctx,
			rebindQueryForPgx("UPDATE run_segments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ?"),
			SegmentStatusAccepted,
			segmentID,
		); err != nil {
			return ExecutionClaimResult{}, normalizeSQLError(err)
		}

		if err := transitionTaskAttemptTx(ctx, tx, taskID, taskAttemptID, attempt, TaskStatusAccepted, TaskStatusAccepted, true, false, false); err != nil {
			return ExecutionClaimResult{}, err
		}
	}

	runRes, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET lease_owner = ?,
			lease_until = ?,
			cancel_token = ?,
			cancel_requested_at = CASE WHEN status = ? THEN cancel_requested_at ELSE NULL END,
			cancel_reason = CASE WHEN status = ? THEN cancel_reason ELSE NULL END,
			attempt = CASE WHEN status = ? THEN attempt + 1 ELSE attempt END,
			orphan_reason = '',
			failure_code = '',
			status = ?,
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
		WHERE run_id = ?
			AND status IN (?, ?, ?)
	`), owner, leaseUntil.UTC().Unix(), claimToken, RunStatusRunning, RunStatusRunning, RunStatusQueued, RunStatusRunning, runID, RunStatusQueued, RunStatusRunning, RunStatusOrphaned)
	if err != nil {
		return ExecutionClaimResult{}, normalizeSQLError(err)
	}
	runUpdated, err := runRes.RowsAffected()
	if err != nil {
		return ExecutionClaimResult{}, err
	}
	if runUpdated != 1 {
		return ExecutionClaimResult{}, fmt.Errorf("%w: run %s cannot be promoted for execution claim", ErrConflict, runID)
	}

	if err := tx.Commit(); err != nil {
		return ExecutionClaimResult{}, err
	}

	return ExecutionClaimResult{
		Claimed:                true,
		ClaimToken:             claimToken,
		TransitionedToAccepted: transitionedToAccepted,
	}, nil
}

func (r *SQLRunsRepository) RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	claimToken = strings.TrimSpace(claimToken)
	if executionID == "" || owner == "" || claimToken == "" {
		return fmt.Errorf("%w: execution_id, owner, and claim_token are required", ErrConflict)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE segment_executions
		SET lease_until = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE execution_id = ?
			AND lease_owner = ?
			AND claim_token = ?
			AND status IN (?, ?)
	`), leaseUntil.UTC().Unix(), executionID, owner, claimToken, ExecutionStatusAccepted, ExecutionStatusRunning)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: renew execution lease: no matching active row for execution_id=%q owner=%q claim_token=%q", ErrConflict, executionID, owner, claimToken)
	}

	return nil
}

func (r *SQLRunsRepository) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (ExecutionFinalizationResult, error) {
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	claimToken = strings.TrimSpace(claimToken)
	status = strings.TrimSpace(status)
	if executionID == "" || owner == "" || claimToken == "" {
		return ExecutionFinalizationResult{}, fmt.Errorf("%w: execution_id, owner, and claim_token are required", ErrConflict)
	}

	if !isTerminalExecutionStatus(status) {
		return ExecutionFinalizationResult{}, fmt.Errorf("%w: unsupported terminal execution status %s", ErrConflict, status)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecutionFinalizationResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	runID, err := validateExecutionClaimForCompletionTx(ctx, tx, executionID, owner, claimToken, time.Now().UTC().Unix())
	if err != nil {
		return ExecutionFinalizationResult{}, err
	}

	var children []TaskExecutionRecord
	var activated int
	if status == ExecutionStatusSucceeded {
		var taskID string
		taskID, err = transitionExecutionTx(ctx, tx, executionID, ExecutionStatusSucceeded, SegmentStatusSucceeded, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}, true, false, true)
		if err != nil {
			return ExecutionFinalizationResult{}, err
		}

		children, activated, err = activatePlannedChildTaskExecutionsTx(ctx, tx, taskID)
		if err != nil {
			return ExecutionFinalizationResult{}, err
		}

		for _, child := range children {
			if _, err := ensureTaskDispatchIntentTx(ctx, tx, TaskDispatchIntentCreate{
				ExecutionID:       child.ExecutionID,
				RunID:             child.RunID,
				TaskID:            child.TaskID,
				TaskAttemptID:     child.TaskAttemptID,
				SourceExecutionID: executionID,
				CellID:            child.CellID,
			}); err != nil {
				return ExecutionFinalizationResult{}, err
			}
		}
	} else {
		if _, err := transitionExecutionTx(ctx, tx, executionID, status, status, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}, true, false, true); err != nil {
			return ExecutionFinalizationResult{}, err
		}
	}

	summary, err := getRunTaskCompletionTx(ctx, tx, runID)
	if err != nil {
		return ExecutionFinalizationResult{}, err
	}

	result := ExecutionFinalizationResult{
		ExecutionID: executionID,
		RunID:       runID,
		Outcome:     ExecutionFinalizationOutcomeWaiting,
		Summary:     summary,
		Children:    children,
		Activated:   activated,
	}

	switch {
	case statusIn(status, []string{ExecutionStatusCancelled, ExecutionStatusAborted}):
		if reason == "" {
			reason = CancelReasonAPI
		}

		if err := markRunTerminalTx(ctx, tx, runID, RunStatusCancelled, "", reason); err != nil {
			return ExecutionFinalizationResult{}, err
		}

		result.Outcome = ExecutionFinalizationOutcomeRunCancelled
	case summary.TerminalFailed > 0:
		if failureCode == "" {
			failureCode = FailureCodeExecution
		}

		if reason == "" {
			reason = fmt.Sprintf("%d task execution(s) ended in a terminal failure", summary.TerminalFailed)
		}

		if err := markRunTerminalTx(ctx, tx, runID, RunStatusFailed, failureCode, reason); err != nil {
			return ExecutionFinalizationResult{}, err
		}

		result.Outcome = ExecutionFinalizationOutcomeRunFailed
	case summary.AllSucceeded():
		if err := markRunTerminalTx(ctx, tx, runID, RunStatusSucceeded, "", ""); err != nil {
			return ExecutionFinalizationResult{}, err
		}

		result.Outcome = ExecutionFinalizationOutcomeRunSucceeded
	case len(children) > 0:
		if err := markRunQueuedForContinuationTx(ctx, tx, runID); err != nil {
			return ExecutionFinalizationResult{}, err
		}

		result.Outcome = ExecutionFinalizationOutcomeContinued
	default:
		if err := markRunQueuedForContinuationTx(ctx, tx, runID); err != nil {
			return ExecutionFinalizationResult{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return ExecutionFinalizationResult{}, err
	}

	return result, nil
}

func validateExecutionClaimForCompletionTx(ctx context.Context, tx *sql.Tx, executionID, owner, claimToken string, nowUnix int64) (string, error) {
	var runID string
	var runStatus string
	var executionStatus string
	var leaseOwner, storedClaimToken sql.NullString
	var leaseUntil sql.NullInt64
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT se.run_id, jr.status, se.status, se.lease_owner, se.claim_token, se.lease_until
		FROM segment_executions se
		JOIN job_runs jr ON jr.run_id = se.run_id
		WHERE se.execution_id = ?
	`), executionID).Scan(&runID, &runStatus, &executionStatus, &leaseOwner, &storedClaimToken, &leaseUntil); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: execution %s", ErrNotFound, executionID)
		}

		return "", normalizeSQLError(err)
	}

	if !statusIn(runStatus, []string{RunStatusRunning, RunStatusOrphaned}) {
		return "", fmt.Errorf("%w: run %s status %s cannot be finalized by execution claim", ErrConflict, runID, runStatus)
	}

	if !statusIn(executionStatus, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}) {
		return "", fmt.Errorf("%w: execution %s status %s cannot be finalized by execution claim", ErrConflict, executionID, executionStatus)
	}

	if !leaseOwner.Valid || leaseOwner.String != owner || !storedClaimToken.Valid || storedClaimToken.String != claimToken || !leaseUntil.Valid || leaseUntil.Int64 < nowUnix {
		return "", fmt.Errorf("%w: execution %s claim is not active for owner %q", ErrConflict, executionID, owner)
	}

	return runID, nil
}

func markRunQueuedForContinuationTx(ctx context.Context, tx *sql.Tx, runID string) error {
	res, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET status = ?,
			orphan_reason = '',
			failure_code = '',
			failure_reason = NULL,
			lease_owner = NULL,
			lease_until = NULL,
			cancel_token = NULL,
			cancel_requested_at = NULL,
			cancel_reason = NULL,
			last_dispatched_at = NULL
		WHERE run_id = ?
			AND status IN (?, ?)
	`), RunStatusQueued, runID, RunStatusRunning, RunStatusOrphaned)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return fmt.Errorf("%w: run %s cannot be queued for continuation from active status", ErrConflict, runID)
	}

	return nil
}

func markRunTerminalTx(ctx context.Context, tx *sql.Tx, runID, status, failureCode, reason string) error {
	res, err := tx.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET status = ?,
			finished_at = CURRENT_TIMESTAMP,
			orphan_reason = '',
			failure_code = ?,
			failure_reason = ?,
			lease_owner = NULL,
			lease_until = NULL,
			cancel_token = NULL,
			cancel_requested_at = NULL,
			cancel_reason = NULL
		WHERE run_id = ?
			AND status IN (?, ?)
	`), status, failureCode, nullableReason(reason), runID, RunStatusRunning, RunStatusOrphaned)
	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n != 1 {
		return fmt.Errorf("%w: run %s cannot be finalized from active status", ErrConflict, runID)
	}

	return nil
}

func (r *SQLRunsRepository) markExecutionAccepted(ctx context.Context, executionID string) error {
	return r.transitionExecution(ctx, executionID, ExecutionStatusAccepted, SegmentStatusAccepted, []string{ExecutionStatusPending}, true, false, false)
}

func (r *SQLRunsRepository) MarkExecutionStarted(ctx context.Context, executionID string) error {
	return r.transitionExecution(ctx, executionID, ExecutionStatusRunning, SegmentStatusRunning, []string{ExecutionStatusPending, ExecutionStatusAccepted}, true, true, false)
}

func (r *SQLRunsRepository) markExecutionTerminal(ctx context.Context, executionID, status string) error {
	if !isTerminalExecutionStatus(status) {
		return fmt.Errorf("%w: unsupported terminal execution status %s", ErrConflict, status)
	}

	return r.transitionExecution(ctx, executionID, status, status, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}, true, false, true)
}

func (r *SQLRunsRepository) transitionExecution(
	ctx context.Context,
	executionID, targetStatus, targetSegmentStatus string,
	allowedFrom []string,
	markAccepted, markStarted, markFinished bool,
) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := transitionExecutionTx(ctx, tx, executionID, targetStatus, targetSegmentStatus, allowedFrom, markAccepted, markStarted, markFinished); err != nil {
		return err
	}

	return tx.Commit()
}

func transitionExecutionTx(
	ctx context.Context,
	tx *sql.Tx,
	executionID, targetStatus, targetSegmentStatus string,
	allowedFrom []string,
	markAccepted, markStarted, markFinished bool,
) (string, error) {
	var segmentID string
	var runID string
	var taskID string
	var taskAttemptID string
	var attempt int
	var currentStatus string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT segment_id, run_id, task_id, task_attempt_id, attempt, status FROM segment_executions WHERE execution_id = ?"),
		executionID,
	).Scan(&segmentID, &runID, &taskID, &taskAttemptID, &attempt, &currentStatus); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: execution %s", ErrNotFound, executionID)
		}

		return "", normalizeSQLError(err)
	}

	if currentStatus == targetStatus {
		return taskID, nil
	}

	if !statusIn(currentStatus, allowedFrom) {
		return "", fmt.Errorf("%w: execution %s status %s cannot transition to %s", ErrConflict, executionID, currentStatus, targetStatus)
	}

	setParts := []string{"status = ?"}
	args := []any{targetStatus}
	if markAccepted {
		setParts = append(setParts, "accepted_at = COALESCE(accepted_at, CURRENT_TIMESTAMP)")
	}

	if markStarted {
		setParts = append(setParts, "started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")
	}

	if markFinished {
		setParts = append(setParts,
			"finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)",
			"lease_owner = NULL",
			"lease_until = NULL",
			"claim_token = NULL",
		)
	}

	setParts = append(setParts, "last_observed_at = ?", "event_sequence = event_sequence + 1", "updated_at = CURRENT_TIMESTAMP")
	args = append(args, time.Now().UnixNano(), executionID)

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE segment_executions SET "+strings.Join(setParts, ", ")+" WHERE execution_id = ?"),
		args...,
	); err != nil {
		return "", normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE run_segments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ?"),
		targetSegmentStatus,
		segmentID,
	); err != nil {
		return "", normalizeSQLError(err)
	}

	if err := transitionTaskAttemptTx(ctx, tx, taskID, taskAttemptID, attempt, targetStatus, targetSegmentStatus, markAccepted, markStarted, markFinished); err != nil {
		return "", err
	}

	return taskID, nil
}

func transitionTaskAttemptTx(
	ctx context.Context,
	tx *sql.Tx,
	taskID string,
	taskAttemptID string,
	attempt int,
	targetAttemptStatus string,
	targetTaskStatus string,
	markAccepted bool,
	markStarted bool,
	markFinished bool,
) error {
	setParts := []string{"status = ?"}
	args := []any{targetAttemptStatus}
	if markAccepted {
		setParts = append(setParts, "accepted_at = COALESCE(accepted_at, CURRENT_TIMESTAMP)")
	}

	if markStarted {
		setParts = append(setParts, "started_at = COALESCE(started_at, CURRENT_TIMESTAMP)")
	}

	if markFinished {
		setParts = append(setParts, "finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)")
	}

	setParts = append(setParts, "last_observed_at = ?", "event_sequence = event_sequence + 1", "updated_at = CURRENT_TIMESTAMP")
	args = append(args, time.Now().UnixNano(), taskAttemptID, taskID, attempt)

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE task_attempts SET "+strings.Join(setParts, ", ")+" WHERE attempt_id = ? AND task_id = ? AND attempt = ?"),
		args...,
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE run_tasks SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE task_id = ?"),
		targetTaskStatus,
		taskID,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func statusIn(status string, statuses []string) bool {
	return slices.Contains(statuses, status)
}

func isTerminalExecutionStatus(status string) bool {
	switch status {
	case ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusCancelled, ExecutionStatusAborted:
		return true
	default:
		return false
	}
}

func (r *SQLRunsRepository) GetRunForCancel(ctx context.Context, runID string) (RunForCancel, error) {
	var rec RunForCancel
	var leaseOwner, cancelToken, cancelReason sql.NullString
	var cancelRequestedAt sql.NullInt64
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, status, lease_owner, cancel_token, cancel_requested_at, cancel_reason FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&rec.RunID, &rec.Status, &leaseOwner, &cancelToken, &cancelRequestedAt, &cancelReason); err != nil {
		if err == sql.ErrNoRows {
			return RunForCancel{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return RunForCancel{}, normalizeSQLError(err)
	}

	if leaseOwner.Valid {
		rec.LeaseOwner = leaseOwner.String
	}

	if cancelToken.Valid {
		rec.CancelToken = cancelToken.String
	}

	if cancelRequestedAt.Valid {
		v := cancelRequestedAt.Int64
		rec.CancelRequestedAt = &v
	}

	if cancelReason.Valid {
		rec.CancelReason = cancelReason.String
	}

	return rec, nil
}

func (r *SQLRunsRepository) GetRunJobID(ctx context.Context, runID string) (string, error) {
	var jobID string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT job_id FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&jobID); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return "", normalizeSQLError(err)
	}

	return jobID, nil
}

func (r *SQLRunsRepository) CountByStatus(ctx context.Context, status string) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COUNT(*) FROM job_runs WHERE status = ?"),
		status,
	).Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLRunsRepository) CountByStatusByCell(ctx context.Context, status string) ([]RunCountByCell, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT owning_cell, COUNT(*)
		FROM job_runs
		WHERE status = ?
		GROUP BY owning_cell
		ORDER BY owning_cell ASC
	`), status)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var counts []RunCountByCell
	for rows.Next() {
		var count RunCountByCell
		if err := rows.Scan(&count.CellID, &count.Count); err != nil {
			return nil, normalizeSQLError(err)
		}

		counts = append(counts, count)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return counts, nil
}

func (r *SQLRunsRepository) CountStuckBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) (int64, error) {
	var count int64
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT COUNT(*)
		FROM job_runs
		WHERE status = 'queued'
			AND (last_dispatched_at IS NULL OR last_dispatched_at < ?)
			AND NOT EXISTS (
				SELECT 1
				FROM task_dispatch_intents tdi
				WHERE tdi.run_id = job_runs.run_id
					AND tdi.enqueued_at IS NULL
			)
	`), cutoffUnix).Scan(&count)

	if err != nil {
		return 0, normalizeSQLError(err)
	}

	return count, nil
}

func (r *SQLRunsRepository) CountStuckBeforeDispatchCutoffByCell(ctx context.Context, cutoffUnix int64) ([]RunCountByCell, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT owning_cell, COUNT(*)
		FROM job_runs
		WHERE status = 'queued'
			AND (last_dispatched_at IS NULL OR last_dispatched_at < ?)
			AND NOT EXISTS (
				SELECT 1
				FROM task_dispatch_intents tdi
				WHERE tdi.run_id = job_runs.run_id
					AND tdi.enqueued_at IS NULL
			)
		GROUP BY owning_cell
		ORDER BY owning_cell ASC
	`), cutoffUnix)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var counts []RunCountByCell
	for rows.Next() {
		var count RunCountByCell
		if err := rows.Scan(&count.CellID, &count.Count); err != nil {
			return nil, normalizeSQLError(err)
		}

		counts = append(counts, count)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return counts, nil
}

func (r *SQLRunsRepository) GetRun(ctx context.Context, runID string) (RunRecord, error) {
	var rec RunRecord
	var orphanReason, failureCode, createdAt, startedAt, finishedAt, failureReason sql.NullString
	var replayOfRunID, triggerInvocationID, triggerType, triggerPayloadHash, requestedCells sql.NullString
	var triggerID sql.NullInt64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx(`
			SELECT
				jr.run_id,
				jr.run_index,
				jr.status,
				jr.orphan_reason,
				jr.failure_code,
				CAST(jr.created_at AS TEXT),
				CAST(jr.started_at AS TEXT),
				CAST(jr.finished_at AS TEXT),
				jr.failure_reason,
				jr.definition_version,
				jr.definition_hash,
				jr.owning_cell,
				jr.replay_of_run_id,
				jr.trigger_invocation_id,
				jr.execution_payload_hash,
				ti.trigger_id,
				ti.trigger_type,
				ti.trigger_payload_hash,
				ti.requested_cells
			FROM job_runs jr
			LEFT JOIN trigger_invocations ti ON ti.invocation_id = jr.trigger_invocation_id
			WHERE jr.run_id = ?
		`),
		runID,
	).Scan(&rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &createdAt, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell, &replayOfRunID, &triggerInvocationID, &rec.ExecutionPayloadHash, &triggerID, &triggerType, &triggerPayloadHash, &requestedCells)

	if err != nil {
		if err == sql.ErrNoRows {
			return RunRecord{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return RunRecord{}, normalizeSQLError(err)
	}

	if orphanReason.Valid && orphanReason.String != "" {
		rec.OrphanReason = &orphanReason.String
	}

	if createdAt.Valid {
		rec.CreatedAt = &createdAt.String
	}

	if startedAt.Valid {
		rec.StartedAt = &startedAt.String
	}

	if finishedAt.Valid {
		rec.FinishedAt = &finishedAt.String
	}

	if failureCode.Valid && failureCode.String != "" {
		rec.FailureCode = &failureCode.String
	}

	if failureReason.Valid {
		rec.FailureReason = &failureReason.String
	}

	if err := applyRunAuditFields(&rec, replayOfRunID, triggerInvocationID, triggerID, triggerType, triggerPayloadHash, requestedCells); err != nil {
		return RunRecord{}, err
	}

	return rec, nil
}

func applyRunAuditFields(rec *RunRecord, replayOfRunID, triggerInvocationID sql.NullString, triggerID sql.NullInt64, triggerType, triggerPayloadHash, requestedCells sql.NullString) error {
	if replayOfRunID.Valid && strings.TrimSpace(replayOfRunID.String) != "" {
		rec.ReplayOfRunID = &replayOfRunID.String
	}

	if triggerInvocationID.Valid && strings.TrimSpace(triggerInvocationID.String) != "" {
		rec.TriggerInvocationID = &triggerInvocationID.String
	}

	if triggerID.Valid {
		v := triggerID.Int64
		rec.TriggerID = &v
	}

	if triggerType.Valid && strings.TrimSpace(triggerType.String) != "" {
		rec.TriggerType = &triggerType.String
	}

	if triggerPayloadHash.Valid && strings.TrimSpace(triggerPayloadHash.String) != "" {
		rec.TriggerPayloadHash = &triggerPayloadHash.String
	}

	if requestedCells.Valid && strings.TrimSpace(requestedCells.String) != "" {
		var cells []string
		if err := json.Unmarshal([]byte(requestedCells.String), &cells); err != nil {
			return fmt.Errorf("parse trigger requested cells: %w", err)
		}

		rec.RequestedCells = cells
	}

	return nil
}
