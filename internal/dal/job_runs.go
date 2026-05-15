package dal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type SQLRunsRepository struct {
	db *sql.DB
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
		reason = AbortReasonCancelled
	}

	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_code = '', failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL, claim_token = NULL, cancel_token = NULL WHERE run_id = ?`

	args := []any{RunStatusAborted, reason, runID}
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
		return fmt.Errorf("mark run aborted: no matching active row for run_id=%q claim_token=%q", runID, claimToken)
	}

	return nil
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
			AND status IN ('queued', 'failed', 'orphaned', 'aborted')
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
	runID = uuid.New().String()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var idx int
	if runIndex != nil {
		idx = *runIndex
	} else {
		err = tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"), jobID).Scan(&idx)
		if err != nil {
			return "", 0, err
		}
	}

	definitionHash, err := lookupDefinitionHashTx(ctx, tx, jobID, definitionVersion)
	if err != nil {
		return "", 0, err
	}

	_, err = tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, started_at, definition_version, definition_hash, owning_cell) VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`),
		runID,
		jobID,
		idx,
		"queued",
		definitionVersion,
		definitionHash,
		DefaultCellID,
	)

	if err != nil {
		return "", 0, normalizeSQLError(err)
	}

	if err = tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}

func lookupDefinitionHashTx(ctx context.Context, tx *sql.Tx, jobID string, version int) (string, error) {
	var hash string
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&hash); err == nil {
		return hash, nil
	} else if err != sql.ErrNoRows {
		return "", normalizeSQLError(err)
	}

	var currentVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_hash, version FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&hash, &currentVersion); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}

		return "", normalizeSQLError(err)
	}

	if currentVersion == version {
		return hash, nil
	}

	return "", nil
}

func (r *SQLRunsRepository) ListByJob(ctx context.Context, jobID string, since *int, cursor int64, limit int) ([]RunRecord, int64, error) {
	query := "SELECT id, run_id, run_index, status, orphan_reason, failure_code, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), failure_reason, definition_version, definition_hash, owning_cell FROM job_runs WHERE job_id = ?"
	args := []any{jobID}

	if since != nil {
		query += " AND run_index > ?"
		args = append(args, *since)
	}

	if cursor > 0 {
		query += " AND id > ?"
		args = append(args, cursor)
	}

	query += " ORDER BY id ASC LIMIT ?"
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
		var orphanReason, failureCode, startedAt, finishedAt, failureReason sql.NullString
		if err := rows.Scan(&id, &rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell); err != nil {
			return nil, 0, normalizeSQLError(err)
		}

		lastID = id
		if orphanReason.Valid && orphanReason.String != "" {
			rec.OrphanReason = &orphanReason.String
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

func (r *SQLRunsRepository) GetRun(ctx context.Context, runID string) (RunRecord, error) {
	var rec RunRecord
	var orphanReason, failureCode, startedAt, finishedAt, failureReason sql.NullString
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, run_index, status, orphan_reason, failure_code, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), failure_reason, definition_version, definition_hash, owning_cell FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &startedAt, &finishedAt, &failureReason, &rec.DefinitionVersion, &rec.DefinitionHash, &rec.OwningCell)

	if err != nil {
		if err == sql.ErrNoRows {
			return RunRecord{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		}

		return RunRecord{}, normalizeSQLError(err)
	}

	if orphanReason.Valid && orphanReason.String != "" {
		rec.OrphanReason = &orphanReason.String
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

	return rec, nil
}
