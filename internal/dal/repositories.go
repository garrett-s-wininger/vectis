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
	SegmentStatusPlanned     = "planned"
	SegmentStatusPending     = "pending"
	SegmentStatusAccepted    = "accepted"
	SegmentStatusRunning     = "running"
	SegmentStatusSucceeded   = "succeeded"
	SegmentStatusFailed      = "failed"
	SegmentStatusCancelled   = "cancelled"
	SegmentStatusAborted     = "aborted"
	RootTaskKey              = "root"
	TaskStatusPlanned        = "planned"
	TaskStatusPending        = "pending"
	TaskStatusAccepted       = "accepted"
	TaskStatusRunning        = "running"
	TaskStatusSucceeded      = "succeeded"
	TaskStatusFailed         = "failed"
	TaskStatusCancelled      = "cancelled"
	TaskStatusAborted        = "aborted"
	ExecutionStatusPlanned   = "planned"
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

	DispatchEventAccepted = "accepted"
	DispatchEventAttempt  = "attempt"
	DispatchEventSuccess  = "success"
	DispatchEventFailure  = "failure"

	TriggerTypeManual  = "manual"
	TriggerTypeCron    = "cron"
	TriggerTypeReplay  = "replay"
	TriggerTypeWebhook = "webhook"

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
	RunID                string
	RunIndex             int
	Status               string
	OrphanReason         *string
	FailureCode          *string
	CreatedAt            *string
	StartedAt            *string
	FinishedAt           *string
	FailureReason        *string
	DefinitionVersion    int
	DefinitionHash       string
	OwningCell           string
	ReplayOfRunID        *string
	TriggerInvocationID  *string
	TriggerID            *int64
	TriggerType          *string
	TriggerPayloadHash   *string
	RequestedCells       []string
	ExecutionPayloadHash string
}

type TaskRecord struct {
	ID           int64
	TaskID       string
	RunID        string
	ParentTaskID *string
	TaskKey      string
	Name         string
	Status       string
	SpecHash     string
	CreatedAt    *string
	UpdatedAt    *string
	Attempts     []TaskAttemptRecord
}

type TaskAttemptRecord struct {
	AttemptID      string
	TaskID         string
	RunID          string
	CellID         string
	Attempt        int
	Status         string
	AcceptedAt     *string
	StartedAt      *string
	FinishedAt     *string
	LastObservedAt *int64
	EventSequence  int64
	CreatedAt      *string
	UpdatedAt      *string
}

type TaskExecutionCreate struct {
	RunID        string
	ParentTaskID string
	TaskKey      string
	Name         string
	SpecHash     string
	TargetCellID string
}

type TaskExecutionRecord struct {
	RunID         string
	TaskID        string
	ParentTaskID  string
	TaskKey       string
	Name          string
	TaskAttemptID string
	SegmentID     string
	SegmentName   string
	ExecutionID   string
	CellID        string
	Attempt       int
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

type RunAuditMetadata struct {
	TriggerInvocationID  string
	ExecutionPayloadHash string
	ReplayOfRunID        string
}

type ExecutionPayloadRecord struct {
	RunID          string
	PayloadHash    string
	PayloadJSON    string
	DefinitionHash string
}

type ExecutionDispatchRecord struct {
	RunID             string
	JobID             string
	RunIndex          int
	TaskID            string
	TaskKey           string
	TaskName          string
	TaskAttemptID     string
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
	TaskID             string
	TaskKey            string
	TaskName           string
	TaskAttemptID      string
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

type TriggerInvocation struct {
	InvocationID       string
	TriggerID          *int64
	JobID              string
	TriggerType        string
	TriggerPayloadHash string
	RequestedCells     []string
}

type TriggerInvocationRecord struct {
	ID                 int64
	InvocationID       string
	TriggerID          *int64
	JobID              string
	TriggerType        string
	TriggerPayloadHash string
	RequestedCellsJSON string
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

type TriggerInvocationsRepository interface {
	Record(ctx context.Context, invocation TriggerInvocation) (TriggerInvocationRecord, error)
}

type CatalogStatusBackfillRepository interface {
	ListMissingRunStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]RunStatusUpdate, error)
	ListMissingExecutionStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionStatusUpdate, error)
}

type CronSchedule struct {
	ID        int64
	TriggerID int64
	JobID     string
	CronSpec  string
	NextRunAt time.Time
}

type RunForCancel struct {
	RunID             string
	Status            string
	LeaseOwner        string
	CancelToken       string
	CancelRequestedAt *int64
	CancelReason      string
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
	RequestRunCancel(ctx context.Context, runID, reason string) (RunForCancel, error)
	RunCancelRequested(ctx context.Context, runID, claimToken string) (bool, error)
	TouchDispatched(ctx context.Context, runID string) error
	GetLogShard(ctx context.Context, runID string) (shardID string, assigned bool, err error)
	AssignLogShard(ctx context.Context, runID, shardID string) (assignedShardID string, err error)
	CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error)
	CreateRunInCell(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellID string) (runID string, runIndexOut int, err error)
	CreateRunsInCells(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string) ([]CreatedRun, error)
	CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit RunAuditMetadata) ([]CreatedRun, error)
	CreateScheduledRun(ctx context.Context, scheduleID int64, scheduledFor time.Time, jobID string, definitionVersion int, audit RunAuditMetadata) (runID string, runIndexOut int, created bool, err error)
	CreateReplayRun(ctx context.Context, sourceRunID string, targetCellID string, audit RunAuditMetadata) (CreatedRun, error)
	RecordExecutionPayload(ctx context.Context, runID, payloadJSON, definitionHash string) (payloadHash string, recordedPayloadJSON string, err error)
	GetExecutionPayloadForRun(ctx context.Context, runID string) (ExecutionPayloadRecord, error)
	GetExecutionPayloadByHash(ctx context.Context, payloadHash string) (ExecutionPayloadRecord, error)
	ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]RunRecord, int64, error)
	ListRunTasks(ctx context.Context, runID string, cursor int64, limit int) ([]TaskRecord, int64, error)
	EnsurePlannedTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error)
	EnsurePendingTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error)
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

type ServiceLeasesRepository interface {
	TryAcquire(ctx context.Context, name, owner string, now, leaseUntil time.Time) (bool, error)
	Release(ctx context.Context, name, owner string) error
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

type EphemeralRunStarterWithAudit interface {
	EphemeralRunStarter
	CreateDefinitionAndRunInCellWithAudit(ctx context.Context, jobID, definitionJSON string, runIndex *int, targetCellID string, audit RunAuditMetadata) (runID string, runIndexOut int, err error)
}

type SQLRepositories struct {
	db            *sql.DB
	cellID        string
	jobs          *SQLJobsRepository
	runs          *SQLRunsRepository
	schedules     *SQLSchedulesRepository
	auth          *SQLAuthRepository
	namespaces    *SQLNamespacesRepository
	roleBindings  *SQLRoleBindingsRepository
	idempotency   *SQLIdempotencyRepository
	dispatch      *SQLDispatchEventsRepository
	triggers      *SQLTriggerInvocationsRepository
	catalog       *SQLCatalogEventsRepository
	catalogState  *SQLCatalogStatusBackfillRepository
	cellAccept    *SQLCellExecutionAcceptancesRepository
	serviceLeases *SQLServiceLeasesRepository
}

func NewSQLRepositories(db *sql.DB) *SQLRepositories {
	return NewSQLRepositoriesWithCellID(db, DefaultCellID)
}

func NewSQLRepositoriesWithCellID(db *sql.DB, cellID string) *SQLRepositories {
	cellID = normalizeCellID(cellID)
	return &SQLRepositories{
		db:            db,
		cellID:        cellID,
		jobs:          &SQLJobsRepository{db: db, cellID: cellID},
		runs:          &SQLRunsRepository{db: db, cellID: cellID},
		schedules:     &SQLSchedulesRepository{db: db},
		auth:          NewSQLAuthRepository(db),
		namespaces:    NewSQLNamespacesRepositoryWithCellID(db, cellID),
		roleBindings:  NewSQLRoleBindingsRepository(db),
		idempotency:   &SQLIdempotencyRepository{db: db},
		dispatch:      &SQLDispatchEventsRepository{db: db},
		triggers:      &SQLTriggerInvocationsRepository{db: db},
		catalog:       &SQLCatalogEventsRepository{db: db},
		catalogState:  &SQLCatalogStatusBackfillRepository{db: db},
		cellAccept:    &SQLCellExecutionAcceptancesRepository{db: db, cellID: cellID},
		serviceLeases: &SQLServiceLeasesRepository{db: db},
	}
}

var _ EphemeralRunStarter = (*SQLRepositories)(nil)
var _ EphemeralRunStarterWithAudit = (*SQLRepositories)(nil)

func DefinitionHash(definitionJSON string) string {
	return PayloadHash(definitionJSON)
}

func PayloadHash(payload string) string {
	sum := sha256.Sum256([]byte(payload))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func ExecutionPayloadHash(payloadJSON string) string {
	return PayloadHash(payloadJSON)
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

func rootTaskID(runID string) string {
	return taskIDForKey(runID, RootTaskKey)
}

func taskIDForKey(runID, taskKey string) string {
	return runID + ":" + taskKey
}

func rootTaskAttemptID(runID string, attempt int) string {
	return taskAttemptID(rootTaskID(runID), attempt)
}

func taskAttemptID(taskID string, attempt int) string {
	if attempt <= 0 {
		attempt = 1
	}

	return taskID + ":attempt:" + strconv.Itoa(attempt)
}

func taskSegmentID(taskID string) string {
	return taskID + ":segment"
}

func taskExecutionID(taskAttemptID string) string {
	return taskAttemptID + ":execution"
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
	if err := createInitialRootTaskAttemptTx(ctx, tx, runID, cellID); err != nil {
		return err
	}

	taskID := rootTaskID(runID)
	taskAttemptID := rootTaskAttemptID(runID, 1)
	segmentID := newSegmentID()
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO run_segments (segment_id, run_id, name, status) VALUES (?, ?, ?, ?)"),
		segmentID,
		runID,
		RootTaskKey,
		SegmentStatusPending,
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO segment_executions (execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"),
		newExecutionID(),
		segmentID,
		runID,
		taskID,
		taskAttemptID,
		normalizeCellID(cellID),
		ExecutionStatusPending,
		1,
	); err != nil {
		return normalizeSQLError(err)
	}

	return nil
}

func createInitialRootTaskAttemptTx(ctx context.Context, tx *sql.Tx, runID, cellID string) error {
	taskID := rootTaskID(runID)
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO run_tasks (task_id, run_id, task_key, name, status) VALUES (?, ?, ?, ?, ?)"),
		taskID,
		runID,
		RootTaskKey,
		RootTaskKey,
		TaskStatusPending,
	); err != nil {
		return normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO task_attempts (attempt_id, task_id, run_id, cell_id, status, attempt) VALUES (?, ?, ?, ?, ?, ?)"),
		rootTaskAttemptID(runID, 1),
		taskID,
		runID,
		normalizeCellID(cellID),
		TaskStatusPending,
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
	return r.CreateDefinitionAndRunInCellWithAudit(ctx, jobID, definitionJSON, runIndex, targetCellID, RunAuditMetadata{})
}

func (r *SQLRepositories) CreateDefinitionAndRunInCellWithAudit(ctx context.Context, jobID, definitionJSON string, runIndex *int, targetCellID string, audit RunAuditMetadata) (runID string, runIndexOut int, err error) {
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
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, 1, ?, ?, ?, ?, ?)`),
		runID,
		jobID,
		idx,
		"queued",
		definitionHash,
		targetCellID,
		nullableString(audit.ReplayOfRunID),
		nullableString(audit.TriggerInvocationID),
		strings.TrimSpace(audit.ExecutionPayloadHash),
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

func (r *SQLRepositories) TriggerInvocations() TriggerInvocationsRepository {
	return r.triggers
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

func (r *SQLRepositories) ServiceLeases() ServiceLeasesRepository {
	return r.serviceLeases
}
