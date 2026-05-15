package dal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
	RunStatusQueued    = "queued"
	RunStatusRunning   = "running"
	RunStatusSucceeded = "succeeded"
	RunStatusFailed    = "failed"
	RunStatusOrphaned  = "orphaned"
	RunStatusCancelled = "cancelled"
	RunStatusAbandoned = "abandoned"
	RunStatusAborted   = "aborted"

	DefaultLeaseTTL          = 15 * time.Minute
	DefaultRenewInterval     = 5 * time.Minute
	OrphanReasonLeaseExpired = "lease_expired"
	OrphanReasonAckUncertain = "ack_uncertain"
	CancelReasonAPI          = "api_cancelled"
	RepairReasonManual       = "manual_repair"
	FailureCodeExecution     = "execution_error"
	FailureCodeForceFailed   = "force_failed"
	DefaultCellID            = "local"

	DispatchSourceAPI        = "api"
	DispatchSourceCron       = "cron"
	DispatchSourceReconciler = "reconciler"

	DispatchEventAttempt = "attempt"
	DispatchEventSuccess = "success"
	DispatchEventFailure = "failure"
)

type JobRecord struct {
	GlobalID       string
	JobID          string
	NamespaceID    int64
	DefinitionJSON string
	DefinitionHash string
	Version        int
	HomeCell       string
}

type RunRecord struct {
	RunID             string
	RunIndex          int
	Status            string
	OrphanReason      *string
	FailureCode       *string
	CreatedAt         *string
	StartedAt         *string
	FinishedAt        *string
	FailureReason     *string
	DefinitionVersion int
	DefinitionHash    string
	OwningCell        string
}

type QueuedRun struct {
	RunID             string
	JobID             string
	DefinitionVersion int
	DefinitionHash    string
	OwningCell        string
}

type EphemeralRunStarter interface {
	CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error)
}

type IdempotencyRecord struct {
	Scope        string
	Key          string
	RequestHash  string
	ResponseJSON *string
}

type DispatchEvent struct {
	ID        int64
	RunID     string
	Source    string
	EventType string
	Message   *string
	CreatedAt int64
}

type IdempotencyRepository interface {
	Reserve(ctx context.Context, scope, key, requestHash string) (record IdempotencyRecord, created bool, err error)
	Complete(ctx context.Context, scope, key, responseJSON string) error
	Release(ctx context.Context, scope, key string) error
}

type DispatchEventsRepository interface {
	Record(ctx context.Context, runID, source, eventType string, message *string) error
	ListByRun(ctx context.Context, runID string) ([]DispatchEvent, error)
	LastReconcilerActivity(ctx context.Context) (*int64, error)
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
	MarkRunCancelled(ctx context.Context, runID, claimToken, reason string) error
	MarkRunAborted(ctx context.Context, runID, claimToken, reason string) error
	MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error
	RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error
	RepairMarkRunFailed(ctx context.Context, runID, reason string) error
	RepairMarkRunCancelled(ctx context.Context, runID, reason string) error
	RepairMarkRunAbandoned(ctx context.Context, runID, reason string) error
	RequeueRunForRetry(ctx context.Context, runID string) error
	MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error)
	GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error)
	TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (bool, string, error)
	RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error
	TouchDispatched(ctx context.Context, runID string) error
	CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error)
	ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, cursor int64, limit int) ([]RunRecord, int64, error)
	ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error)
	CountByStatus(ctx context.Context, status string) (int64, error)
	CountStuckBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) (int64, error)
	GetRunJobID(ctx context.Context, runID string) (string, error)
	GetRunForCancel(ctx context.Context, runID string) (RunForCancel, error)
	GetRun(ctx context.Context, runID string) (RunRecord, error)
}

type SchedulesRepository interface {
	GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error)
	ClaimDue(ctx context.Context, scheduleID int64, observedNextRun time.Time, claimToken string, claimedUntil, now time.Time) (bool, error)
	CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error)
	ReleaseClaim(ctx context.Context, scheduleID int64, claimToken string) error
	CountCronSchedules(ctx context.Context) (int64, error)
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
	idempotency  *SQLIdempotencyRepository
	dispatch     *SQLDispatchEventsRepository
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
		idempotency:  &SQLIdempotencyRepository{db: db},
		dispatch:     &SQLDispatchEventsRepository{db: db},
	}
}

var _ EphemeralRunStarter = (*SQLRepositories)(nil)

func DefinitionHash(definitionJSON string) string {
	sum := sha256.Sum256([]byte(definitionJSON))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func newGlobalID() string {
	return uuid.NewString()
}

func (r *SQLRepositories) CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	definitionHash := DefinitionHash(definitionJSON)
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash, home_cell) VALUES (?, ?, 1, ?, ?, ?)`),
		newGlobalID(), jobID, definitionJSON, definitionHash, DefaultCellID,
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
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, 1, ?, ?)`),
		runID,
		jobID,
		idx,
		"queued",
		definitionHash,
		DefaultCellID,
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

func (r *SQLRepositories) Idempotency() IdempotencyRepository {
	return r.idempotency
}

func (r *SQLRepositories) DispatchEvents() DispatchEventsRepository {
	return r.dispatch
}
