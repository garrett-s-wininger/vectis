package dal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"vectis/internal/database"
)

func rebindQueryForPgx(query string) string {
	if os.Getenv(database.EnvDatabaseDriver) != "pgx" {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 8)

	argNum := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(argNum))
			argNum++
			continue
		}

		b.WriteByte(query[i])
	}

	return b.String()
}

const (
	DefaultLeaseTTL          = 15 * time.Minute
	DefaultRenewInterval     = 5 * time.Minute
	OrphanReasonLeaseExpired = "lease_expired"
	OrphanReasonAckUncertain = "ack_uncertain"
)

type JobRecord struct {
	JobID          string
	DefinitionJSON string
}

type RunRecord struct {
	RunID         string
	RunIndex      int
	Status        string
	OrphanReason  *string
	StartedAt     *string
	FinishedAt    *string
	FailureReason *string
}

type QueuedRun struct {
	RunID             string
	JobID             string
	DefinitionVersion int
}

type EphemeralRunStarter interface {
	CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error)
}

type CronSchedule struct {
	ID        int64
	JobID     string
	CronSpec  string
	NextRunAt time.Time
}

type JobsRepository interface {
	Create(ctx context.Context, jobID, definitionJSON string) error
	Delete(ctx context.Context, jobID string) error
	List(ctx context.Context) ([]JobRecord, error)
	GetDefinition(ctx context.Context, jobID string) (string, error)
	GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error)
	UpdateDefinition(ctx context.Context, jobID, definitionJSON string) error
}

type RunsRepository interface {
	MarkRunRunning(ctx context.Context, runID string) error
	MarkRunSucceeded(ctx context.Context, runID, claimToken string) error
	MarkRunFailed(ctx context.Context, runID, claimToken, reason string) error
	MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error
	RequeueRunForRetry(ctx context.Context, runID string) error
	MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error)
	GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error)
	TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (bool, string, error)
	RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error
	TouchDispatched(ctx context.Context, runID string) error
	CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error)
	ListByJob(ctx context.Context, jobID string, since *int) ([]RunRecord, error)
	ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error)
}

type SchedulesRepository interface {
	GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error)
	UpdateNextRun(ctx context.Context, scheduleID int64, nextRun time.Time) error
}

type SQLRepositories struct {
	db        *sql.DB
	jobs      *SQLJobsRepository
	runs      *SQLRunsRepository
	schedules *SQLSchedulesRepository
}

func NewSQLRepositories(db *sql.DB) *SQLRepositories {
	return &SQLRepositories{
		db:        db,
		jobs:      &SQLJobsRepository{db: db},
		runs:      &SQLRunsRepository{db: db},
		schedules: &SQLSchedulesRepository{db: db},
	}
}

var _ EphemeralRunStarter = (*SQLRepositories)(nil)

func (r *SQLRepositories) CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_definitions (job_id, version, definition_json) VALUES (?, 1, ?)`),
		jobID, definitionJSON,
	); err != nil {
		return "", 0, normalizeSQLError(err)
	}

	runID = uuid.New().String()

	var idx int
	if runIndex != nil {
		idx = *runIndex
	} else {
		if err := tx.QueryRowContext(ctx,
			rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"),
			jobID,
		).Scan(&idx); err != nil {
			return "", 0, err
		}
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, started_at, definition_version) VALUES (?, ?, ?, ?, NULL, 1)`),
		runID,
		jobID,
		idx,
		"queued",
	); err != nil {
		return "", 0, normalizeSQLError(err)
	}

	if err := tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}

func (r *SQLRepositories) Jobs() JobsRepository {
	return r.jobs
}

func (r *SQLRepositories) Runs() RunsRepository {
	return r.runs
}

func (r *SQLRepositories) Schedules() SchedulesRepository {
	return r.schedules
}

type SQLJobsRepository struct {
	db *sql.DB
}

func (r *SQLJobsRepository) Create(ctx context.Context, jobID, definitionJSON string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)"),
		jobID,
		definitionJSON,
	)

	return normalizeSQLError(err)
}

func (r *SQLJobsRepository) Delete(ctx context.Context, jobID string) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx("DELETE FROM stored_jobs WHERE job_id = ?"), jobID)
	return normalizeSQLError(err)
}

func (r *SQLJobsRepository) List(ctx context.Context) ([]JobRecord, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT job_id, definition_json FROM stored_jobs")
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []JobRecord
	for rows.Next() {
		var rec JobRecord
		if err := rows.Scan(&rec.JobID, &rec.DefinitionJSON); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLJobsRepository) GetDefinition(ctx context.Context, jobID string) (string, error) {
	var definitionJSON string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&definitionJSON); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return "", normalizeSQLError(err)
	}

	return definitionJSON, nil
}

func (r *SQLJobsRepository) UpdateDefinition(ctx context.Context, jobID, definitionJSON string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("UPDATE stored_jobs SET definition_json = ? WHERE job_id = ?"),
		definitionJSON,
		jobID,
	)

	return normalizeSQLError(err)
}

func (r *SQLJobsRepository) GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error) {
	var definitionJSON string
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&definitionJSON); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("%w: job %s version %d", ErrNotFound, jobID, version)
		}

		return "", normalizeSQLError(err)
	}

	return definitionJSON, nil
}

type SQLRunsRepository struct {
	db *sql.DB
}

func (r *SQLRunsRepository) MarkRunRunning(ctx context.Context, runID string) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("UPDATE job_runs SET status = ?, orphan_reason = '', started_at = COALESCE(started_at, CURRENT_TIMESTAMP) WHERE run_id = ?"),
		"running", runID)

	return normalizeSQLError(err)
}

func (r *SQLRunsRepository) MarkRunSucceeded(ctx context.Context, runID, claimToken string) error {
	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL WHERE run_id = ?`
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

func (r *SQLRunsRepository) MarkRunFailed(ctx context.Context, runID, claimToken, reason string) error {
	query := `UPDATE job_runs SET status = ?, finished_at = CURRENT_TIMESTAMP, failure_reason = ?,
		orphan_reason = '', lease_owner = NULL, lease_until = NULL WHERE run_id = ?`
	args := []any{"failed", reason, runID}
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

func (r *SQLRunsRepository) MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error {
	if reason == "" {
		reason = "unknown"
	}
	orphanReason := classifyOrphanReason(reason)

	query := `UPDATE job_runs SET status = ?, failure_reason = ?,
		orphan_reason = ?, lease_owner = NULL, lease_until = NULL, claim_token = NULL WHERE run_id = ?`
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
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE job_runs
		SET status = 'queued',
			orphan_reason = '',
			finished_at = NULL,
			failure_reason = NULL,
			lease_owner = NULL,
			lease_until = NULL,
			claim_token = NULL,
			last_dispatched_at = NULL
		WHERE run_id = ?
	`), runID)
	return normalizeSQLError(err)
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
			return nil, err
		}

		candidates = append(candidates, runID)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(candidates))
	for _, runID := range candidates {
		res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
			UPDATE job_runs
			SET status = 'orphaned',
				orphan_reason = ?
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
			return nil, err
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
			attempt = attempt + 1,
			orphan_reason = '',
			status = 'running',
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
		WHERE run_id = ?
			AND status = 'queued'
			AND (lease_until IS NULL OR lease_until < ?)
	`), owner, leaseUntil.Unix(), claimToken, runID, nowUnix)

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

	_, err = tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, started_at, definition_version) VALUES (?, ?, ?, ?, NULL, ?)`),
		runID,
		jobID,
		idx,
		"queued",
		definitionVersion,
	)

	if err != nil {
		return "", 0, normalizeSQLError(err)
	}

	if err = tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}

func (r *SQLRunsRepository) ListByJob(ctx context.Context, jobID string, since *int) ([]RunRecord, error) {
	query := "SELECT run_id, run_index, status, orphan_reason, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), failure_reason FROM job_runs WHERE job_id = ?"
	args := []any{jobID}

	if since != nil {
		query += " AND run_index > ?"
		args = append(args, *since)
	}

	query += " ORDER BY run_index ASC"
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RunRecord
	for rows.Next() {
		var rec RunRecord
		var orphanReason, startedAt, finishedAt, failureReason sql.NullString
		if err := rows.Scan(&rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &startedAt, &finishedAt, &failureReason); err != nil {
			return nil, err
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

		if failureReason.Valid {
			rec.FailureReason = &failureReason.String
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLRunsRepository) ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT run_id, job_id, definition_version
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
		if err := rows.Scan(&rec.RunID, &rec.JobID, &rec.DefinitionVersion); err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

type SQLSchedulesRepository struct {
	db *sql.DB
}

func (r *SQLSchedulesRepository) GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT id, job_id, cron_spec, next_run_at
		FROM job_cron_schedules
		WHERE next_run_at <= ?
	`), at.Format(time.RFC3339))

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []CronSchedule
	for rows.Next() {
		var sched CronSchedule
		var nextRunAt string
		if err := rows.Scan(&sched.ID, &sched.JobID, &sched.CronSpec, &nextRunAt); err != nil {
			return nil, err
		}

		parsedTime, err := time.Parse(time.RFC3339, nextRunAt)
		if err != nil {
			return nil, fmt.Errorf("parse next_run_at %q: %w", nextRunAt, err)
		}

		sched.NextRunAt = parsedTime
		out = append(out, sched)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLSchedulesRepository) UpdateNextRun(ctx context.Context, scheduleID int64, nextRun time.Time) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("UPDATE job_cron_schedules SET next_run_at = ? WHERE id = ?"),
		nextRun.Format(time.RFC3339),
		scheduleID,
	)

	return normalizeSQLError(err)
}
