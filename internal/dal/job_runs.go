package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		return r.MarkRunSucceeded(ctx, runID, update.ClaimToken)
	case RunStatusFailed:
		return r.MarkRunFailed(ctx, runID, update.ClaimToken, update.FailureCode, update.Reason)
	case RunStatusCancelled:
		return r.MarkRunCancelled(ctx, runID, update.ClaimToken, update.Reason)
	case RunStatusAborted:
		return r.MarkRunAborted(ctx, runID, update.ClaimToken, update.Reason)
	case RunStatusOrphaned:
		return r.MarkRunOrphaned(ctx, runID, update.ClaimToken, update.Reason)
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
		return r.MarkExecutionAccepted(ctx, executionID)
	case ExecutionStatusRunning:
		return r.MarkExecutionStarted(ctx, executionID)
	case ExecutionStatusSucceeded, ExecutionStatusFailed, ExecutionStatusCancelled, ExecutionStatusAborted:
		return r.MarkExecutionTerminal(ctx, executionID, update.Status)
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

func (r *SQLRunsRepository) MarkRunSucceeded(ctx context.Context, runID, claimToken string) error {
	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP,
		orphan_reason = '', failure_code = '', failure_reason = NULL, lease_owner = NULL, lease_until = NULL,
		claim_token = NULL, cancel_token = NULL, cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?`
	args := []any{"succeeded", runID}
	if claimToken != "" {
		query += ` AND status IN ('running', 'orphaned') AND claim_token = ?`
		args = append(args, claimToken)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return normalizeSQLError(err)
	}

	if claimToken == "" {
		return nil
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("mark run succeeded: no matching active row for run_id=%q claim_token=%q", runID, claimToken)
	}

	return nil
}

func (r *SQLRunsRepository) MarkRunFailed(ctx context.Context, runID, claimToken, failureCode, reason string) error {
	if failureCode == "" {
		failureCode = FailureCodeExecution
	}

	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_code = ?, failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL,
		claim_token = NULL, cancel_token = NULL, cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?`

	args := []any{"failed", failureCode, reason, runID}
	if claimToken != "" {
		query += ` AND status IN ('running', 'orphaned') AND claim_token = ?`
		args = append(args, claimToken)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return normalizeSQLError(err)
	}

	if claimToken == "" {
		return nil
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("mark run failed: no matching active row for run_id=%q claim_token=%q", runID, claimToken)
	}

	return nil
}

func (r *SQLRunsRepository) MarkRunAborted(ctx context.Context, runID, claimToken, reason string) error {
	if reason == "" {
		reason = CancelReasonAPI
	}

	return r.MarkRunCancelled(ctx, runID, claimToken, reason)
}

func (r *SQLRunsRepository) MarkRunCancelled(ctx context.Context, runID, claimToken, reason string) error {
	if reason == "" {
		reason = CancelReasonAPI
	}

	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_code = '', failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL, claim_token = NULL, cancel_token = NULL,
		cancel_requested_at = NULL, cancel_reason = NULL WHERE run_id = ?`

	args := []any{RunStatusCancelled, reason, runID}
	if claimToken != "" {
		query += ` AND status IN ('running', 'orphaned') AND claim_token = ?`
		args = append(args, claimToken)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return normalizeSQLError(err)
	}

	if claimToken == "" {
		return nil
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("mark run cancelled: no matching active row for run_id=%q claim_token=%q", runID, claimToken)
	}

	return nil
}

func (r *SQLRunsRepository) RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error {
	return r.repairMarkTerminal(ctx, runID, RunStatusSucceeded, "", reason)
}

func (r *SQLRunsRepository) RepairMarkRunFailed(ctx context.Context, runID, reason string) error {
	if reason == "" {
		reason = RepairReasonManual
	}

	return r.repairMarkTerminal(ctx, runID, RunStatusFailed, FailureCodeForceFailed, reason)
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
			claim_token = NULL,
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

func (r *SQLRunsRepository) MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error {
	if reason == "" {
		reason = "unknown"
	}
	orphanReason := classifyOrphanReason(reason)

	query := `UPDATE job_runs SET status = ?, failure_reason = ?,
		orphan_reason = ?, failure_code = '', lease_owner = NULL, lease_until = NULL, claim_token = NULL WHERE run_id = ?`
	args := []any{"orphaned", reason, orphanReason, runID}
	if claimToken != "" {
		query += ` AND status IN ('running', 'orphaned') AND claim_token = ?`
		args = append(args, claimToken)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return normalizeSQLError(err)
	}

	if claimToken == "" {
		return nil
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("mark run orphaned: no matching active row for run_id=%q claim_token=%q", runID, claimToken)
	}

	return nil
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
			claim_token = NULL,
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
		SELECT run_id
		FROM job_runs
		WHERE status = 'running'
			AND lease_until IS NOT NULL
			AND lease_until < ?
		ORDER BY id ASC
	`), cutoffUnix)

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
				AND lease_until IS NOT NULL
				AND lease_until < ?
		`), OrphanReasonLeaseExpired, runID, cutoffUnix)

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

func (r *SQLRunsRepository) TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (bool, string, error) {
	now := time.Now().UTC()
	nowUnix := now.Unix()
	claimToken := uuid.NewString()
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs SET
			lease_owner = ?,
			lease_until = ?,
			claim_token = ?,
			cancel_token = ?,
			cancel_requested_at = NULL,
			cancel_reason = NULL,
			attempt = attempt + 1,
			orphan_reason = '',
			failure_code = '',
			status = 'running',
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
		WHERE run_id = ?
			AND status = 'queued'
			AND (lease_until IS NULL OR lease_until < ?)
	`), owner, leaseUntil.Unix(), claimToken, claimToken, runID, nowUnix)

	if err != nil {
		return false, "", normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return false, "", err
	}

	if n != 1 {
		return false, "", nil
	}

	return true, claimToken, nil
}

func (r *SQLRunsRepository) RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error {
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET lease_until = ?, orphan_reason = '', status = 'running'
			, failure_code = ''
		WHERE run_id = ?
			AND lease_owner = ?
			AND claim_token = ?
			AND status IN ('running', 'orphaned')
	`), leaseUntil.Unix(), runID, owner, claimToken)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("renew lease: no matching active row for run_id=%q owner=%q claim_token=%q", runID, owner, claimToken)
	}

	return nil
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

func (r *SQLRunsRepository) RunCancelRequested(ctx context.Context, runID, claimToken string) (bool, error) {
	if runID == "" || claimToken == "" {
		return false, nil
	}

	var requestedAt sql.NullInt64
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT cancel_requested_at
		FROM job_runs
		WHERE run_id = ?
			AND claim_token = ?
			AND status IN (?, ?)
	`), runID, claimToken, RunStatusRunning, RunStatusOrphaned).Scan(&requestedAt); err != nil {
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

	for attempt := 0; attempt < 3; attempt++ {
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
			ta.cell_id,
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
		var attemptID, attemptTaskID, attemptRunID, cellID, attemptStatus sql.NullString
		var attempt sql.NullInt64
		var acceptedAt, startedAt, finishedAt, attemptCreatedAt, attemptUpdatedAt sql.NullString
		var lastObservedAt, eventSequence sql.NullInt64
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
			&cellID,
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
			AttemptID:      attemptID.String,
			TaskID:         attemptTaskID.String,
			RunID:          attemptRunID.String,
			CellID:         cellID.String,
			Status:         attemptStatus.String,
			AcceptedAt:     nullStringPtr(acceptedAt),
			StartedAt:      nullStringPtr(startedAt),
			FinishedAt:     nullStringPtr(finishedAt),
			LastObservedAt: nullInt64Ptr(lastObservedAt),
			EventSequence:  eventSequence.Int64,
			CreatedAt:      nullStringPtr(attemptCreatedAt),
			UpdatedAt:      nullStringPtr(attemptUpdatedAt),
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
	var rec ExecutionDispatchRecord
	err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			jr.run_id,
			jr.job_id,
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
		WHERE jr.run_id = ?
			AND rs.status = ?
			AND se.status = ?
			AND rt.status = ?
			AND ta.status = ?
		ORDER BY rs.id ASC, se.attempt ASC, se.id ASC
		LIMIT 1
	`), runID, SegmentStatusPending, ExecutionStatusPending, TaskStatusPending, TaskStatusPending).Scan(
		&rec.RunID,
		&rec.JobID,
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
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return ExecutionDispatchRecord{}, fmt.Errorf("%w: pending execution for run %s", ErrNotFound, runID)
		}

		return ExecutionDispatchRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

func (r *SQLRunsRepository) MarkExecutionAccepted(ctx context.Context, executionID string) error {
	return r.transitionExecution(ctx, executionID, ExecutionStatusAccepted, SegmentStatusAccepted, []string{ExecutionStatusPending}, true, false, false)
}

func (r *SQLRunsRepository) MarkExecutionStarted(ctx context.Context, executionID string) error {
	return r.transitionExecution(ctx, executionID, ExecutionStatusRunning, SegmentStatusRunning, []string{ExecutionStatusPending, ExecutionStatusAccepted}, true, true, false)
}

func (r *SQLRunsRepository) MarkExecutionTerminal(ctx context.Context, executionID, status string) error {
	if !isTerminalExecutionStatus(status) {
		return fmt.Errorf("%w: unsupported terminal execution status %s", ErrConflict, status)
	}

	return r.transitionExecution(ctx, executionID, status, status, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}, true, false, true)
}

func (r *SQLRunsRepository) MarkExecutionSucceededAndActivateChildren(ctx context.Context, executionID string) ([]TaskExecutionRecord, int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	taskID, err := transitionExecutionTx(ctx, tx, executionID, ExecutionStatusSucceeded, SegmentStatusSucceeded, []string{ExecutionStatusPending, ExecutionStatusAccepted, ExecutionStatusRunning}, true, false, true)
	if err != nil {
		return nil, 0, err
	}

	children, activated, err := activatePlannedChildTaskExecutionsTx(ctx, tx, taskID)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}

	return children, activated, nil
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
		setParts = append(setParts, "finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP)")
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
	for _, candidate := range statuses {
		if status == candidate {
			return true
		}
	}

	return false
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
