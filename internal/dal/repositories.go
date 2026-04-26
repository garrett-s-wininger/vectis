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
	FailureCodeExecution     = "execution_error"
	FailureCodeForceFailed   = "force_failed"
)

type JobRecord struct {
	JobID          string
	NamespaceID    int64
	DefinitionJSON string
	Version        int
}

type RunRecord struct {
	RunID         string
	RunIndex      int
	Status        string
	OrphanReason  *string
	FailureCode   *string
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

type RunForCancel struct {
	RunID       string
	Status      string
	LeaseOwner  string
	CancelToken string
}

type RunsRepository interface {
	MarkRunRunning(ctx context.Context, runID string) error
	MarkRunSucceeded(ctx context.Context, runID, claimToken string) error
	MarkRunFailed(ctx context.Context, runID, claimToken, failureCode, reason string) error
	MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error
	RequeueRunForRetry(ctx context.Context, runID string) error
	MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error)
	GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error)
	TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (bool, string, error)
	RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error
	TouchDispatched(ctx context.Context, runID string) error
	CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error)
	ListByJob(ctx context.Context, jobID string, since *int, cursor int64, limit int) ([]RunRecord, int64, error)
	ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error)
	GetRunJobID(ctx context.Context, runID string) (string, error)
	GetRunForCancel(ctx context.Context, runID string) (RunForCancel, error)
	GetRun(ctx context.Context, runID string) (RunRecord, error)
}

type SchedulesRepository interface {
	GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error)
	UpdateNextRun(ctx context.Context, scheduleID int64, nextRun time.Time) error
}

type JobsRepository interface {
	Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error
	Delete(ctx context.Context, jobID string) error
	List(ctx context.Context, cursor int64, limit int) ([]JobRecord, int64, error)
	ListByNamespace(ctx context.Context, namespaceID int64) ([]JobRecord, error)
	GetDefinition(ctx context.Context, jobID string) (definitionJSON string, version int, err error)
	GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error)
	GetNamespaceID(ctx context.Context, jobID string) (int64, error)
	UpdateDefinition(ctx context.Context, jobID, definitionJSON string) (newVersion int, err error)
}

type SQLRepositories struct {
	db           *sql.DB
	jobs         *SQLJobsRepository
	runs         *SQLRunsRepository
	schedules    *SQLSchedulesRepository
	auth         *SQLAuthRepository
	namespaces   *SQLNamespacesRepository
	roleBindings *SQLRoleBindingsRepository
}

func NewSQLRepositories(db *sql.DB) *SQLRepositories {
	return &SQLRepositories{
		db:           db,
		jobs:         &SQLJobsRepository{db: db},
		runs:         &SQLRunsRepository{db: db},
		schedules:    &SQLSchedulesRepository{db: db},
		auth:         NewSQLAuthRepository(db),
		namespaces:   NewSQLNamespacesRepository(db),
		roleBindings: NewSQLRoleBindingsRepository(db),
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

func (r *SQLRepositories) Auth() AuthRepository {
	return r.auth
}

func (r *SQLRepositories) Namespaces() NamespacesRepository {
	return r.namespaces
}

func (r *SQLRepositories) RoleBindings() RoleBindingsRepository {
	return r.roleBindings
}

type SQLJobsRepository struct {
	db *sql.DB
}

func (r *SQLJobsRepository) Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error {
	_, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)"),
		jobID,
		namespaceID,
		definitionJSON,
	)

	return normalizeSQLError(err)
}

func (r *SQLJobsRepository) Delete(ctx context.Context, jobID string) error {
	res, err := r.db.ExecContext(ctx, rebindQueryForPgx("DELETE FROM stored_jobs WHERE job_id = ?"), jobID)
	if err != nil {
		return normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("%w: job %s", ErrNotFound, jobID)
	}

	return nil
}

func (r *SQLJobsRepository) List(ctx context.Context, cursor int64, limit int) ([]JobRecord, int64, error) {
	query := "SELECT id, job_id, namespace_id, definition_json, version FROM stored_jobs"
	args := []any{}
	if cursor > 0 {
		query += " WHERE id > ?"
		args = append(args, cursor)
	}
	query += " ORDER BY id ASC LIMIT ?"
	args = append(args, limit+1)

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(query), args...)
	if err != nil {
		return nil, 0, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []JobRecord
	var lastID int64
	for rows.Next() {
		var rec JobRecord
		var id int64
		if err := rows.Scan(&id, &rec.JobID, &rec.NamespaceID, &rec.DefinitionJSON, &rec.Version); err != nil {
			return nil, 0, err
		}
		lastID = id
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	var nextCursor int64
	if len(out) > limit {
		out = out[:limit]
		nextCursor = lastID
	}

	return out, nextCursor, nil
}

func (r *SQLJobsRepository) ListByNamespace(ctx context.Context, namespaceID int64) ([]JobRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT job_id, namespace_id, definition_json, version FROM stored_jobs WHERE namespace_id = ?"),
		namespaceID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []JobRecord
	for rows.Next() {
		var rec JobRecord
		if err := rows.Scan(&rec.JobID, &rec.NamespaceID, &rec.DefinitionJSON, &rec.Version); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLJobsRepository) GetDefinition(ctx context.Context, jobID string) (string, int, error) {
	var definitionJSON string
	var version int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT definition_json, version FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&definitionJSON, &version); err != nil {
		if err == sql.ErrNoRows {
			return "", 0, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return "", 0, normalizeSQLError(err)
	}

	return definitionJSON, version, nil
}

func (r *SQLJobsRepository) GetNamespaceID(ctx context.Context, jobID string) (int64, error) {
	var namespaceID int64
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT namespace_id FROM stored_jobs WHERE job_id = ?"),
		jobID,
	).Scan(&namespaceID); err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("%w: job %s", ErrNotFound, jobID)
		}

		return 0, normalizeSQLError(err)
	}

	return namespaceID, nil
}

func (r *SQLJobsRepository) UpdateDefinition(ctx context.Context, jobID, definitionJSON string) (int, error) {
	var newVersion int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("UPDATE stored_jobs SET definition_json = ?, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE job_id = ? RETURNING version"),
		definitionJSON,
		jobID,
	).Scan(&newVersion); err != nil {
		return 0, normalizeSQLError(err)
	}

	return newVersion, nil
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
			AND status IN ('queued', 'failed', 'orphaned')
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
	cancelToken := uuid.NewString()
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
	`), owner, leaseUntil.Unix(), claimToken, cancelToken, runID, nowUnix)

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

func (r *SQLRunsRepository) ListByJob(ctx context.Context, jobID string, since *int, cursor int64, limit int) ([]RunRecord, int64, error) {
	query := "SELECT id, run_id, run_index, status, orphan_reason, failure_code, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), failure_reason FROM job_runs WHERE job_id = ?"
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
		if err := rows.Scan(&id, &rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &startedAt, &finishedAt, &failureReason); err != nil {
			return nil, 0, err
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
		return nil, 0, err
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

func (r *SQLRunsRepository) GetRun(ctx context.Context, runID string) (RunRecord, error) {
	var rec RunRecord
	var orphanReason, failureCode, startedAt, finishedAt, failureReason sql.NullString
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT run_id, run_index, status, orphan_reason, failure_code, CAST(started_at AS TEXT), CAST(finished_at AS TEXT), failure_reason FROM job_runs WHERE run_id = ?"),
		runID,
	).Scan(&rec.RunID, &rec.RunIndex, &rec.Status, &orphanReason, &failureCode, &startedAt, &finishedAt, &failureReason)
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
