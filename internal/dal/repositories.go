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
	SegmentStatusPending     = "pending"
	SegmentStatusAccepted    = "accepted"
	SegmentStatusRunning     = "running"
	SegmentStatusSucceeded   = "succeeded"
	SegmentStatusFailed      = "failed"
	SegmentStatusCancelled   = "cancelled"
	SegmentStatusAborted     = "aborted"
	ExecutionStatusPending   = "pending"
	ExecutionStatusAccepted  = "accepted"
	ExecutionStatusRunning   = "running"
	ExecutionStatusSucceeded = "succeeded"
	ExecutionStatusFailed    = "failed"
	ExecutionStatusCancelled = "cancelled"
	ExecutionStatusAborted   = "aborted"

	DispatchSourceAPI        = "api"
	DispatchSourceCron       = "cron"
	DispatchSourceReconciler = "reconciler"

	DispatchEventAttempt = "attempt"
	DispatchEventSuccess = "success"
	DispatchEventFailure = "failure"

	CatalogEventStatusPending = "pending"
	CatalogEventStatusApplied = "applied"
	CatalogEventStatusFailed  = "failed"
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

type RunCountByCell struct {
	CellID string
	Count  int64
}

type CreatedRun struct {
	RunID        string
	RunIndex     int
	TargetCellID string
}

type ExecutionDispatchRecord struct {
	RunID             string
	JobID             string
	RunIndex          int
	SegmentID         string
	SegmentName       string
	SegmentStatus     string
	ExecutionID       string
	ExecutionStatus   string
	CellID            string
	Attempt           int
	DefinitionVersion int
	DefinitionHash    string
	OwningCell        string
}

type CellExecutionAcceptance struct {
	ExecutionID        string
	RunID              string
	JobID              string
	RunIndex           int
	SegmentID          string
	SegmentName        string
	CellID             string
	Attempt            int
	DefinitionVersion  int
	DefinitionHash     string
	DefinitionJSON     string
	RequestJSON        string
	AcceptedAtUnixNano int64
}

type CellExecutionQueueHandoff struct {
	ExecutionID     string
	RunID           string
	RequestJSON     string
	EnqueueAttempts int
}

type CellExecutionAcceptancesRepository interface {
	AcceptExecution(ctx context.Context, acceptance CellExecutionAcceptance) (created bool, err error)
	ListPendingQueueHandoffs(ctx context.Context, cutoffUnixNano int64, limit int) ([]CellExecutionQueueHandoff, error)
	MarkEnqueued(ctx context.Context, executionID string, enqueuedAtUnixNano int64) error
	MarkEnqueueFailed(ctx context.Context, executionID string, attemptedAtUnixNano int64, message string) error
}

type EphemeralRunStarter interface {
	CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error)
	CreateDefinitionAndRunInCell(ctx context.Context, jobID, definitionJSON string, runIndex *int, targetCellID string) (runID string, runIndexOut int, err error)
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

type CatalogEventRecord struct {
	ID         int64
	SourceCell string
	EventKey   string
	EventType  string
	Payload    []byte
	Status     string
	Attempts   int
	LastError  *string
	ReceivedAt int64
	AppliedAt  *int64
	UpdatedAt  int64
}

type CatalogEventSummary struct {
	Pending          int64
	Applied          int64
	Failed           int64
	Total            int64
	LastReceivedUnix *int64
	LastAppliedUnix  *int64
}

type CatalogEventSourceSummary struct {
	SourceCell       string
	Pending          int64
	Applied          int64
	Failed           int64
	Total            int64
	LastReceivedUnix *int64
	LastAppliedUnix  *int64
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

type CatalogEventsRepository interface {
	Record(ctx context.Context, sourceCell, eventKey, eventType string, payload []byte) (CatalogEventRecord, bool, error)
	ListPending(ctx context.Context, limit int) ([]CatalogEventRecord, error)
	MarkApplied(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64, message string) error
	Summary(ctx context.Context) (CatalogEventSummary, error)
	SummaryBySource(ctx context.Context) ([]CatalogEventSourceSummary, error)
}

type CatalogStatusBackfillRepository interface {
	ListMissingRunStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]RunStatusUpdate, error)
	ListMissingExecutionStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionStatusUpdate, error)
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

type RunStatusUpdate struct {
	RunID       string `json:"run_id"`
	Status      string `json:"status"`
	ClaimToken  string `json:"claim_token,omitempty"`
	FailureCode string `json:"failure_code,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type ExecutionStatusUpdate struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
}

type RunCatalogUpdater interface {
	ApplyRunStatusUpdate(ctx context.Context, update RunStatusUpdate) error
	ApplyExecutionStatusUpdate(ctx context.Context, update ExecutionStatusUpdate) error
}

type RunsRepository interface {
	RunCatalogUpdater

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
	CreateRunInCell(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellID string) (runID string, runIndexOut int, err error)
	CreateRunsInCells(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string) ([]CreatedRun, error)
	ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]RunRecord, int64, error)
	ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error)
	GetPendingExecution(ctx context.Context, runID string) (ExecutionDispatchRecord, error)
	MarkExecutionAccepted(ctx context.Context, executionID string) error
	MarkExecutionStarted(ctx context.Context, executionID string) error
	MarkExecutionTerminal(ctx context.Context, executionID, status string) error
	CountByStatus(ctx context.Context, status string) (int64, error)
	CountByStatusByCell(ctx context.Context, status string) ([]RunCountByCell, error)
	CountStuckBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) (int64, error)
	CountStuckBeforeDispatchCutoffByCell(ctx context.Context, cutoffUnix int64) ([]RunCountByCell, error)
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
	cellID       string
	jobs         *SQLJobsRepository
	runs         *SQLRunsRepository
	schedules    *SQLSchedulesRepository
	auth         *SQLAuthRepository
	namespaces   *SQLNamespacesRepository
	roleBindings *SQLRoleBindingsRepository
	idempotency  *SQLIdempotencyRepository
	dispatch     *SQLDispatchEventsRepository
	catalog      *SQLCatalogEventsRepository
	catalogState *SQLCatalogStatusBackfillRepository
	cellAccept   *SQLCellExecutionAcceptancesRepository
}

func NewSQLRepositories(db *sql.DB) *SQLRepositories {
	return NewSQLRepositoriesWithCellID(db, DefaultCellID)
}

func NewSQLRepositoriesWithCellID(db *sql.DB, cellID string) *SQLRepositories {
	cellID = normalizeCellID(cellID)
	return &SQLRepositories{
		db:           db,
		cellID:       cellID,
		jobs:         &SQLJobsRepository{db: db, cellID: cellID},
		runs:         &SQLRunsRepository{db: db, cellID: cellID},
		schedules:    &SQLSchedulesRepository{db: db},
		auth:         NewSQLAuthRepository(db),
		namespaces:   NewSQLNamespacesRepositoryWithCellID(db, cellID),
		roleBindings: NewSQLRoleBindingsRepository(db),
		idempotency:  &SQLIdempotencyRepository{db: db},
		dispatch:     &SQLDispatchEventsRepository{db: db},
		catalog:      &SQLCatalogEventsRepository{db: db},
		catalogState: &SQLCatalogStatusBackfillRepository{db: db},
		cellAccept:   &SQLCellExecutionAcceptancesRepository{db: db, cellID: cellID},
	}
}

var _ EphemeralRunStarter = (*SQLRepositories)(nil)

func DefinitionHash(definitionJSON string) string {
	sum := sha256.Sum256([]byte(definitionJSON))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ExecutionPayloadHash(payloadJSON string) string {
	sum := sha256.Sum256([]byte(payloadJSON))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func newGlobalID() string {
	return uuid.NewString()
}

func newSegmentID() string {
	return uuid.NewString()
}

func newExecutionID() string {
	return uuid.NewString()
}

func normalizeCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return DefaultCellID
	}

	return cellID
}

func normalizeTargetCellID(cellID, fallback string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return normalizeCellID(fallback)
	}

	return cellID
}

func createInitialSegmentExecutionTx(ctx context.Context, tx *sql.Tx, runID, cellID string) error {
	segmentID := newSegmentID()
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO run_segments (segment_id, run_id, name, status) VALUES (?, ?, ?, ?)"),
		segmentID,
		runID,
		"root",
		SegmentStatusPending,
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO segment_executions (execution_id, segment_id, run_id, cell_id, status, attempt) VALUES (?, ?, ?, ?, ?, ?)"),
		newExecutionID(),
		segmentID,
		runID,
		normalizeCellID(cellID),
		ExecutionStatusPending,
		1,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func (r *SQLRepositories) CreateDefinitionAndRun(ctx context.Context, jobID, definitionJSON string, runIndex *int) (runID string, runIndexOut int, err error) {
	return r.CreateDefinitionAndRunInCell(ctx, jobID, definitionJSON, runIndex, r.cellID)
}

func (r *SQLRepositories) CreateDefinitionAndRunInCell(ctx context.Context, jobID, definitionJSON string, runIndex *int, targetCellID string) (runID string, runIndexOut int, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	targetCellID = normalizeTargetCellID(targetCellID, r.cellID)
	definitionHash := DefinitionHash(definitionJSON)
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash) VALUES (?, ?, 1, ?, ?)`),
		newGlobalID(), jobID, definitionJSON, definitionHash,
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
		targetCellID,
	); err != nil {
		return "", 0, normalizeSQLError(err)
	}

	if err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID); err != nil {
		return "", 0, err
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

func (r *SQLRepositories) CatalogEvents() CatalogEventsRepository {
	return r.catalog
}

func (r *SQLRepositories) CatalogStatusBackfill() CatalogStatusBackfillRepository {
	return r.catalogState
}

func (r *SQLRepositories) CellExecutionAcceptances() CellExecutionAcceptancesRepository {
	return r.cellAccept
}
