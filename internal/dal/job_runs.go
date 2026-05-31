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
		orphan_reason = '', failure_code = '', failure_reason = NULL, lease_owner = NULL, lease_until = NULL WHERE run_id = ?`
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
		orphan_reason = '', lease_owner = NULL, lease_until = NULL WHERE run_id = ?`
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
		orphan_reason = '', lease_owner = NULL, lease_until = NULL, claim_token = NULL, cancel_token = NULL WHERE run_id = ?`

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
			cancel_token = NULL
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

func (r *SQLRunsRepository) TouchDispatched(ctx context.Context, runID string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx(`UPDATE job_runs SET last_dispatched_at = ? WHERE run_id = ?`),
		time.Now().Unix(), runID)

	return normalizeSQLError(err)
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
			return nil, err
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

	createdRuns := make([]CreatedRun, 0, len(targetCellIDs))
	for i, targetCellID := range targetCellIDs {
		targetCellID = normalizeTargetCellID(targetCellID, r.currentCellID())
		runID := uuid.New().String()
		runIndexOut := idx + i

		_, err = tx.ExecContext(ctx,
			rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, trigger_invocation_id, execution_payload_hash) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?, ?)`),
			runID,
			jobID,
			runIndexOut,
			"queued",
			definitionVersion,
			definitionHash,
			targetCellID,
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
		var triggerInvocationID, triggerType, triggerPayloadHash, requestedCells sql.NullString
		var triggerID sql.NullInt64
		if err := rows.Scan(&id, &rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &createdAt, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell, &triggerInvocationID, &rec.ExecutionPayloadHash, &triggerID, &triggerType, &triggerPayloadHash, &requestedCells); err != nil {
			return nil, 0, normalizeSQLError(err)
		}

		if err := applyRunAuditFields(&rec, triggerInvocationID, triggerID, triggerType, triggerPayloadHash, requestedCells); err != nil {
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
		WHERE jr.run_id = ?
			AND rs.status = ?
			AND se.status = ?
		ORDER BY rs.id ASC, se.attempt ASC, se.id ASC
		LIMIT 1
	`), runID, SegmentStatusPending, ExecutionStatusPending).Scan(
		&rec.RunID,
		&rec.JobID,
		&rec.RunIndex,
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

	var segmentID string
	var currentStatus string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT segment_id, status FROM segment_executions WHERE execution_id = ?"),
		executionID,
	).Scan(&segmentID, &currentStatus); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: execution %s", ErrNotFound, executionID)
		}

		return normalizeSQLError(err)
	}

	if currentStatus == targetStatus {
		return nil
	}

	if !statusIn(currentStatus, allowedFrom) {
		return fmt.Errorf("%w: execution %s status %s cannot transition to %s", ErrConflict, executionID, currentStatus, targetStatus)
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
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("UPDATE run_segments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE segment_id = ?"),
		targetSegmentStatus,
		segmentID,
	); err != nil {
		return normalizeSQLError(err)
	}

	return tx.Commit()
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
	var leaseOwner, cancelToken sql.NullString
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, status, lease_owner, cancel_token FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&rec.RunID, &rec.Status, &leaseOwner, &cancelToken); err != nil {
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
	var triggerInvocationID, triggerType, triggerPayloadHash, requestedCells sql.NullString
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
	).Scan(&rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &createdAt, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell, &triggerInvocationID, &rec.ExecutionPayloadHash, &triggerID, &triggerType, &triggerPayloadHash, &requestedCells)

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

	if err := applyRunAuditFields(&rec, triggerInvocationID, triggerID, triggerType, triggerPayloadHash, requestedCells); err != nil {
		return RunRecord{}, err
	}

	return rec, nil
}

func applyRunAuditFields(rec *RunRecord, triggerInvocationID sql.NullString, triggerID sql.NullInt64, triggerType, triggerPayloadHash, requestedCells sql.NullString) error {
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
