package dal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
	RunStatusQueued    = "queued"
	RunStatusRunning   = "running"
	RunStatusSucceeded = "succeeded"
	RunStatusFailed    = "failed"
	RunStatusOrphaned  = "orphaned"
	RunStatusCancelled = "cancelled"
	RunStatusAbandoned = "abandoned"
	RunStatusAborted   = "aborted"

	DefaultLeaseTTL               = 15 * time.Minute
	DefaultRenewInterval          = 5 * time.Minute
	OrphanReasonLeaseExpired      = "lease_expired"
	OrphanReasonAckUncertain      = "ack_uncertain"
	OrphanReasonWorkerCoreUnknown = "worker_core_unknown"
	CancelReasonAPI               = "api_cancelled"
	RepairReasonManual            = "manual_repair"
	FailureCodeExecution          = "execution_error"
	FailureCodeForceFailed        = "force_failed"
	FailureCodeDispatchExpired    = "dispatch_expired"
	DefaultCellID                 = "local"
	SegmentStatusPlanned          = "planned"
	SegmentStatusPending          = "pending"
	SegmentStatusAccepted         = "accepted"
	SegmentStatusRunning          = "running"
	SegmentStatusSucceeded        = "succeeded"
	SegmentStatusFailed           = "failed"
	SegmentStatusCancelled        = "cancelled"
	SegmentStatusAborted          = "aborted"
	RootTaskKey                   = "root"
	TaskStatusPlanned             = "planned"
	TaskStatusPending             = "pending"
	TaskStatusAccepted            = "accepted"
	TaskStatusRunning             = "running"
	TaskStatusSucceeded           = "succeeded"
	TaskStatusFailed              = "failed"
	TaskStatusCancelled           = "cancelled"
	TaskStatusAborted             = "aborted"
	ExecutionStatusPlanned        = "planned"
	ExecutionStatusPending        = "pending"
	ExecutionStatusAccepted       = "accepted"
	ExecutionStatusRunning        = "running"
	ExecutionStatusSucceeded      = "succeeded"
	ExecutionStatusFailed         = "failed"
	ExecutionStatusCancelled      = "cancelled"
	ExecutionStatusAborted        = "aborted"

	ExecutionSecurityEventSVIDCheck        = "svid_check"
	ExecutionSecurityEventSecretResolution = "secret_resolution"

	DispatchSourceAPI        = "api"
	DispatchSourceCron       = "cron"
	DispatchSourceReconciler = "reconciler"

	DispatchEventAccepted = "accepted"
	DispatchEventAttempt  = "attempt"
	DispatchEventSuccess  = "success"
	DispatchEventFailure  = "failure"

	TriggerTypeManual   = "manual"
	TriggerTypeCron     = "cron"
	TriggerTypeReaction = "reaction"
	TriggerTypeReplay   = "replay"
	TriggerTypeWebhook  = "webhook"

	SourceKindLocalCheckout = "local_checkout"

	SourceCheckoutModeExternal = "external"
	SourceCheckoutModeManaged  = "managed"

	SourceAuthoringModeReadOnly              = "read_only"
	SourceAuthoringModeLocalCommit           = "local_commit"
	SourceAuthoringModeExternalChangeRequest = "external_change_request"

	SourceSyncStatusNever     = "never"
	SourceSyncStatusRunning   = "running"
	SourceSyncStatusSucceeded = "succeeded"
	SourceSyncStatusFailed    = "failed"

	CatalogEventStatusPending = "pending"
	CatalogEventStatusApplied = "applied"
	CatalogEventStatusFailed  = "failed"

	ReactionInvocationLastErrorMaxLength = 4096

	ReactionEventTypeManualNotice               = "manual.notice"
	ReactionEventTypeRunCompleted               = "run.completed"
	ReactionEventTypeDefinitionValidationFailed = "definition.validation_failed"

	ReactionEventSourceLifecycle = "lifecycle"
	ReactionEventSourceManual    = "manual"

	ReactionInvocationStatusPending   = "pending"
	ReactionInvocationStatusRunning   = "running"
	ReactionInvocationStatusSucceeded = "succeeded"
	ReactionInvocationStatusFailed    = "failed"

	ReactionTargetKindLocal = "local"
	ReactionTargetKindJob   = "job"

	ReactionActionNotifyLocal = "builtins/notify-local"
	ReactionActionTriggerJob  = "builtins/trigger-job"
)

const FailureCodeInvalidEnvelope = "invalid_execution_envelope"

type JobRecord struct {
	GlobalID       string
	JobID          string
	NamespaceID    int64
	DefinitionJSON string
	DefinitionHash string
	Version        int
	HomeCell       string
}

type SourceRepositoryRecord struct {
	ID                     int64
	GlobalID               string
	RepositoryID           string
	NamespaceID            int64
	SourceKind             string
	CheckoutPath           string
	CheckoutMode           string
	AuthoringMode          string
	CanonicalURL           string
	FallbackRemoteURLs     []string
	DefaultRef             string
	CredentialRef          string
	Enabled                bool
	SyncStatus             string
	LastSyncStartedAtUnix  int64
	LastSyncFinishedAtUnix int64
	LastSyncRef            string
	LastSyncCommit         string
	LastSyncError          string
}

type SourceRepositorySyncRecord struct {
	RepositoryID           string
	Status                 string
	StartedAtUnix          int64
	FinishedAtUnix         int64
	Ref                    string
	Commit                 string
	Error                  string
	RunningStaleBeforeUnix int64
}

type SourceRepositoryCountSummary struct {
	Total         int
	Enabled       int
	Disabled      int
	Declared      int
	StaleEnabled  int
	StaleDisabled int
	SyncSucceeded int
	SyncFailed    int
	SyncRunning   int
	SyncNever     int
}

type JobDefinitionSourceRecord struct {
	JobID          string
	Version        int
	RepositoryID   string
	RequestedRef   string
	ResolvedCommit string
	DefinitionPath string
	BlobSHA        string
}

type RunRecord struct {
	RunID                string
	JobID                string
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
	AttemptID       string
	TaskID          string
	RunID           string
	ExecutionID     string
	ExecutionStatus string
	CellID          string
	LeaseOwner      *string
	LeaseUntil      *int64
	Attempt         int
	Status          string
	AcceptedAt      *string
	StartedAt       *string
	FinishedAt      *string
	LastObservedAt  *int64
	EventSequence   int64
	CreatedAt       *string
	UpdatedAt       *string
	SecurityEvents  []ExecutionSecurityEvent
}

type TaskExecutionCreate struct {
	RunID                 string
	ParentTaskID          string
	TaskKey               string
	Name                  string
	SpecHash              string
	TargetCellID          string
	StartDeadlineUnixNano int64
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

type TaskExecutionSnapshot struct {
	Record               TaskExecutionRecord
	Status               string
	LeaseOwner           string
	ClaimToken           string
	LeaseUntilUnix       int64
	AcceptedAtUnixNano   int64
	StartedAtUnixNano    int64
	FinishedAtUnixNano   int64
	LastObservedUnixNano int64
	EventSequence        int64
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
	RunID             string
	JobID             string
	RunIndex          int
	TargetCellID      string
	DefinitionVersion int
	DefinitionHash    string
	Source            *JobDefinitionSourceRecord
	RootDispatch      ExecutionDispatchRecord
}

type RunAuditMetadata struct {
	TriggerInvocationID   string
	ExecutionPayloadHash  string
	ReplayOfRunID         string
	NamespacePath         string
	StartDeadlineUnixNano int64
}

type ExecutionPayloadRecord struct {
	RunID          string
	PayloadHash    string
	PayloadJSON    string
	DefinitionHash string
}

type ExecutionDispatchRecord struct {
	RunID                 string
	JobID                 string
	NamespacePath         string
	RunIndex              int
	TaskID                string
	TaskKey               string
	TaskName              string
	TaskAttemptID         string
	SegmentID             string
	SegmentName           string
	SegmentStatus         string
	ExecutionID           string
	ExecutionStatus       string
	CellID                string
	Attempt               int
	DefinitionVersion     int
	DefinitionHash        string
	OwningCell            string
	StartDeadlineUnixNano int64
}

type ExpiredExecution struct {
	RunID       string
	ExecutionID string
}

type CellExecutionAcceptance struct {
	ExecutionID           string
	RunID                 string
	JobID                 string
	RunIndex              int
	TaskID                string
	TaskKey               string
	TaskName              string
	TaskAttemptID         string
	SegmentID             string
	SegmentName           string
	CellID                string
	Attempt               int
	DefinitionVersion     int
	DefinitionHash        string
	DefinitionJSON        string
	RequestJSON           string
	AcceptedAtUnixNano    int64
	StartDeadlineUnixNano int64
}

type CellExecutionQueueHandoff struct {
	ExecutionID       string
	RunID             string
	JobID             string
	RunIndex          int
	TaskID            string
	TaskKey           string
	TaskName          string
	TaskAttemptID     string
	SegmentID         string
	SegmentName       string
	CellID            string
	Attempt           int
	DefinitionVersion int
	DefinitionHash    string
	RequestJSON       string
	EnqueueAttempts   int
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
	ResourceType string
	ResourceID   string
}

type DispatchEvent struct {
	ID        int64
	RunID     string
	Source    string
	EventType string
	Message   *string
	CreatedAt int64
}

type ExecutionSecurityEvent struct {
	ID            int64   `json:"id"`
	EventKey      string  `json:"event_key,omitempty"`
	RunID         string  `json:"run_id"`
	TaskID        string  `json:"task_id,omitempty"`
	TaskAttemptID string  `json:"task_attempt_id,omitempty"`
	ExecutionID   string  `json:"execution_id,omitempty"`
	EventType     string  `json:"event_type"`
	Outcome       string  `json:"outcome"`
	Reason        string  `json:"reason,omitempty"`
	Provider      *string `json:"provider,omitempty"`
	SecretCount   *int    `json:"secret_count,omitempty"`
	FileCount     *int    `json:"file_count,omitempty"`
	CreatedAt     int64   `json:"created_at"`
}

type RecordExecutionSecurityEventParams struct {
	EventKey      string `json:"event_key,omitempty"`
	RunID         string `json:"run_id"`
	TaskID        string `json:"task_id,omitempty"`
	TaskAttemptID string `json:"task_attempt_id,omitempty"`
	ExecutionID   string `json:"execution_id,omitempty"`
	EventType     string `json:"event_type"`
	Outcome       string `json:"outcome"`
	Reason        string `json:"reason,omitempty"`
	Provider      string `json:"provider,omitempty"`
	SecretCount   *int   `json:"secret_count,omitempty"`
	FileCount     *int   `json:"file_count,omitempty"`
	CreatedAt     int64  `json:"created_at,omitempty"`
}

type ArtifactCreate struct {
	RunID           string
	TaskID          string
	TaskAttemptID   string
	ExecutionID     string
	CellID          string
	Name            string
	Path            string
	ContentType     string
	BlobKey         string
	BlobAlgorithm   string
	BlobDigest      string
	SizeBytes       int64
	ArtifactShardID string
	MetadataJSON    *string
}

type ArtifactRecord struct {
	ID              int64
	RunID           string
	TaskID          *string
	TaskAttemptID   *string
	ExecutionID     *string
	CellID          string
	Name            string
	Path            string
	ContentType     string
	BlobKey         string
	BlobAlgorithm   string
	BlobDigest      string
	SizeBytes       int64
	ArtifactShardID string
	MetadataJSON    *string
	CreatedAt       int64
	UpdatedAt       int64
}

type ArtifactListFilter struct {
	TaskID        string
	TaskAttemptID string
	ExecutionID   string
}

type ArtifactRunUsage struct {
	Count     int64
	SizeBytes int64
}

type RunTaskCompletion struct {
	RunID          string
	Total          int
	Succeeded      int
	TerminalFailed int
	Incomplete     int
}

func (s RunTaskCompletion) AllSucceeded() bool {
	return s.Total > 0 && s.Succeeded == s.Total && s.TerminalFailed == 0 && s.Incomplete == 0
}

type ExecutionFinalizationOutcome string

const (
	ExecutionFinalizationOutcomeContinued    ExecutionFinalizationOutcome = "continued"
	ExecutionFinalizationOutcomeWaiting      ExecutionFinalizationOutcome = "waiting"
	ExecutionFinalizationOutcomeRunSucceeded ExecutionFinalizationOutcome = "run_succeeded"
	ExecutionFinalizationOutcomeRunFailed    ExecutionFinalizationOutcome = "run_failed"
	ExecutionFinalizationOutcomeRunCancelled ExecutionFinalizationOutcome = "run_cancelled"
)

type ExecutionFinalizationResult struct {
	ExecutionID string
	RunID       string
	Outcome     ExecutionFinalizationOutcome
	Summary     RunTaskCompletion
	Children    []TaskExecutionRecord
	Executions  []TaskExecutionSnapshot
	Activated   int
}

type TerminalExecutionSnapshotUpdate struct {
	RunID       string
	Outcome     ExecutionFinalizationOutcome
	FailureCode string
	Reason      string
	Executions  []TaskExecutionSnapshot
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

type ReactionEventCreate struct {
	EventID     string
	Source      string
	EventType   string
	NamespaceID int64
	JobID       string
	RunID       string
	Actor       string
	PayloadJSON []byte
	SourceCell  string
	CreatedAt   int64
}

type ReactionEventRecord struct {
	ID          int64
	EventID     string
	Source      string
	EventType   string
	NamespaceID *int64
	JobID       string
	RunID       string
	Actor       string
	PayloadJSON []byte
	SourceCell  string
	CreatedAt   int64
}

type ReactionTargetCreate struct {
	TargetID       string
	NamespaceID    int64
	Name           string
	Kind           string
	Uses           string
	ConfigJSON     []byte
	SecretRefsJSON []byte
	CreatedAt      int64
}

type ReactionTargetRecord struct {
	ID             int64
	TargetID       string
	NamespaceID    *int64
	Name           string
	Kind           string
	Uses           string
	ConfigJSON     []byte
	SecretRefsJSON []byte
	Enabled        bool
	CreatedAt      int64
	UpdatedAt      int64
}

type ReactionSubscriptionCreate struct {
	SubscriptionID string
	NamespaceID    int64
	TargetID       string
	Name           string
	EventType      string
	JobID          string
	RunStatus      string
	TriggerType    string
	OwningCell     string
	CreatedAt      int64
}

type ReactionSubscriptionRecord struct {
	ID             int64
	SubscriptionID string
	NamespaceID    *int64
	TargetID       string
	Name           string
	EventType      string
	JobID          string
	RunStatus      string
	TriggerType    string
	OwningCell     string
	Enabled        bool
	CreatedAt      int64
	UpdatedAt      int64
}

type ReactionSubscriptionMatch struct {
	Subscription ReactionSubscriptionRecord
	Target       ReactionTargetRecord
}

type ReactionInvocationCreate struct {
	InvocationID         string
	EventID              string
	TargetID             string
	ActionUses           string
	ActionDescriptorJSON []byte
	ActionDigest         string
	TargetConfigJSON     []byte
	MaxAttempts          int
	NextAttemptAt        int64
	CreatedAt            int64
}

type ReactionInvocationRecord struct {
	ID                   int64
	InvocationID         string
	EventID              string
	TargetID             string
	Status               string
	ActionUses           string
	ActionDescriptorJSON []byte
	ActionDigest         string
	TargetConfigJSON     []byte
	Attempts             int
	MaxAttempts          int
	NextAttemptAt        int64
	ClaimedBy            string
	ClaimUntil           *int64
	LastError            *string
	CreatedAt            int64
	UpdatedAt            int64
	CompletedAt          *int64
}

type ReactionLocalMessageCreate struct {
	MessageID    string
	EventID      string
	InvocationID string
	Mailbox      string
	PayloadJSON  []byte
	CreatedAt    int64
}

type ReactionLocalMessageRecord struct {
	ID           int64
	MessageID    string
	EventID      string
	InvocationID string
	Mailbox      string
	PayloadJSON  []byte
	CreatedAt    int64
}

type ReactionJobTriggerEdge struct {
	SubscriptionID string
	TargetID       string
	SourceJobID    string
	TargetJobID    string
	EventType      string
	RunStatus      string
	TriggerType    string
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
	AttachResource(ctx context.Context, scope, key, resourceType, resourceID string) error
	Complete(ctx context.Context, scope, key, responseJSON string) error
	Release(ctx context.Context, scope, key string) error
}

type DispatchEventsRepository interface {
	Record(ctx context.Context, runID, source, eventType string, message *string) error
	RecordDispatchSuccess(ctx context.Context, runID, source string) error
	RecordDispatchAttemptOutcome(ctx context.Context, runID, source, outcomeEventType string, message *string) error
	ListByRun(ctx context.Context, runID string) ([]DispatchEvent, error)
	LastReconcilerActivity(ctx context.Context) (*int64, error)
}

type ArtifactsRepository interface {
	Record(ctx context.Context, create ArtifactCreate) (ArtifactRecord, error)
	GetByRunAndName(ctx context.Context, runID, name string) (ArtifactRecord, error)
	ListByRun(ctx context.Context, runID string, cursor int64, limit int) ([]ArtifactRecord, int64, error)
	ListByRunFiltered(ctx context.Context, runID string, cursor int64, limit int, filter ArtifactListFilter) ([]ArtifactRecord, int64, error)
	GetRunUsageExcludingName(ctx context.Context, runID, name string) (ArtifactRunUsage, error)
}

type CatalogEventsRepository interface {
	Record(ctx context.Context, sourceCell, eventKey, eventType string, payload []byte) (CatalogEventRecord, bool, error)
	ListPending(ctx context.Context, limit int) ([]CatalogEventRecord, error)
	MarkApplied(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64, message string) error
	MarkRetryable(ctx context.Context, id int64, message string) error
	Summary(ctx context.Context) (CatalogEventSummary, error)
	SummaryBySource(ctx context.Context) ([]CatalogEventSourceSummary, error)
}

type ReactionsRepository interface {
	RecordEvent(ctx context.Context, create ReactionEventCreate) (ReactionEventRecord, error)
	GetEvent(ctx context.Context, eventID string) (ReactionEventRecord, error)
	CreateTarget(ctx context.Context, create ReactionTargetCreate) (ReactionTargetRecord, error)
	GetTarget(ctx context.Context, targetID string) (ReactionTargetRecord, error)
	SetTargetEnabled(ctx context.Context, targetID string, enabled bool) (ReactionTargetRecord, error)
	CreateSubscription(ctx context.Context, create ReactionSubscriptionCreate) (ReactionSubscriptionRecord, error)
	SetSubscriptionEnabled(ctx context.Context, subscriptionID string, enabled bool) (ReactionSubscriptionRecord, error)
	ListMatchingSubscriptions(ctx context.Context, event ReactionEventRecord) ([]ReactionSubscriptionMatch, error)
	CreateInvocation(ctx context.Context, create ReactionInvocationCreate) (ReactionInvocationRecord, error)
	GetInvocation(ctx context.Context, invocationID string) (ReactionInvocationRecord, error)
	ListReadyInvocations(ctx context.Context, nowUnixNano int64, limit int) ([]ReactionInvocationRecord, error)
	MarkExpiredInvocationsFailed(ctx context.Context, nowUnixNano int64) (int, error)
	MarkInvocationRunning(ctx context.Context, invocationID, owner string, claimUntilUnixNano int64) (bool, error)
	MarkInvocationSucceeded(ctx context.Context, invocationID, owner string, completedAtUnixNano int64) error
	MarkInvocationFailed(ctx context.Context, invocationID, owner, message string, nextAttemptAtUnixNano int64) error
	RecordLocalMessage(ctx context.Context, create ReactionLocalMessageCreate) (ReactionLocalMessageRecord, error)
	ListLocalMessages(ctx context.Context, mailbox string, cursor int64, limit int) ([]ReactionLocalMessageRecord, int64, error)
	ListJobTriggerEdges(ctx context.Context) ([]ReactionJobTriggerEdge, error)
}

type TriggerInvocationsRepository interface {
	Record(ctx context.Context, invocation TriggerInvocation) (TriggerInvocationRecord, error)
}

type CatalogStatusBackfillRepository interface {
	ListMissingRunStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]RunStatusUpdate, error)
	ListMissingExecutionStatusCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionStatusUpdate, error)
	ListMissingExecutionSecurityCatalogEvents(ctx context.Context, sourceCell string, limit int) ([]ExecutionSecurityEvent, error)
}

type CronSchedule struct {
	ID                          int64
	TriggerID                   int64
	ScheduleID                  string
	JobID                       string
	CronSpec                    string
	NextRunAt                   time.Time
	SourceRepositoryID          string
	SourceRef                   string
	SourcePath                  string
	SourceOverrideRef           string
	SourceOverridePath          string
	SourceOverrideReason        string
	SourceOverrideCreatedAtUnix int64
}

type CronScheduleSummary struct {
	ScheduleCount int64
	DueCount      int64
	ClaimedCount  int64
	OldestDueAt   *time.Time
}

type CronScheduleRecord struct {
	ID                          int64
	TriggerID                   int64
	ScheduleID                  string
	JobID                       string
	CronSpec                    string
	NextRunAt                   time.Time
	SourceRepositoryID          string
	SourceRef                   string
	SourcePath                  string
	SourceOverrideRef           string
	SourceOverridePath          string
	SourceOverrideReason        string
	SourceOverrideCreatedAtUnix int64
	Enabled                     bool
}

type SourceScheduleOverride struct {
	Ref           string
	Path          string
	Reason        string
	CreatedAtUnix int64
}

type SourceCronScheduleCountSummary struct {
	Total           int
	Enabled         int
	Disabled        int
	Declared        int
	StaleEnabled    int
	StaleDisabled   int
	ActiveOverrides int
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
	FailureCode string `json:"failure_code,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type ExecutionStatusUpdate struct {
	ExecutionID string `json:"execution_id"`
	Status      string `json:"status"`
}

// ExecutionClaimResult describes an execution lease claim and whether it also
// performed the pending-to-accepted state transition.
type ExecutionClaimResult struct {
	Claimed                bool
	ClaimToken             string
	TransitionedToAccepted bool
	ExecutionStarted       bool
	Expired                bool
	RunID                  string
	ExecutionID            string
}

type RunHotStateOwnerUpdate struct {
	RunID        string
	CellID       string
	OwnerID      string
	OwnerEpoch   string
	LeaseUntil   time.Time
	LastSequence int64
}

type RunHotStateOwnerRecord struct {
	RunID        string
	CellID       string
	OwnerID      string
	OwnerEpoch   string
	LeaseUntil   time.Time
	LastSequence int64
}

type RunCatalogUpdater interface {
	ApplyRunStatusUpdate(ctx context.Context, update RunStatusUpdate) error
	ApplyExecutionStatusUpdate(ctx context.Context, update ExecutionStatusUpdate) error
}

type RunsRepository interface {
	RunCatalogUpdater

	MarkRunRunning(ctx context.Context, runID string) error
	MarkRunSucceeded(ctx context.Context, runID string) error
	MarkRunFailed(ctx context.Context, runID, failureCode, reason string) error
	MarkRunCancelled(ctx context.Context, runID, reason string) error
	MarkRunAborted(ctx context.Context, runID, reason string) error
	MarkRunOrphaned(ctx context.Context, runID, reason string) error
	RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error
	RepairMarkRunFailed(ctx context.Context, runID, reason string) error
	RepairMarkRunFailedWithCode(ctx context.Context, runID, failureCode, reason string) error
	RepairMarkRunCancelled(ctx context.Context, runID, reason string) error
	RepairMarkRunAbandoned(ctx context.Context, runID, reason string) error
	RequeueRunForRetry(ctx context.Context, runID string) error
	MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error)
	MarkExpiredQueuedExecutionsFailed(ctx context.Context, cutoffUnixNano int64, limit int) ([]ExpiredExecution, error)
	GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error)
	RequestRunCancel(ctx context.Context, runID, reason string) (RunForCancel, error)
	RunCancelRequested(ctx context.Context, runID string) (bool, error)
	TouchDispatched(ctx context.Context, runID string) error
	GetLogShard(ctx context.Context, runID string) (shardID string, assigned bool, err error)
	AssignLogShard(ctx context.Context, runID, shardID string) (assignedShardID string, err error)
	CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (runID string, runIndexOut int, err error)
	CreateRunInCell(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellID string) (runID string, runIndexOut int, err error)
	CreateRunsInCells(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string) ([]CreatedRun, error)
	CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit RunAuditMetadata) ([]CreatedRun, error)
	ListRunsByTriggerInvocation(ctx context.Context, triggerInvocationID string) ([]CreatedRun, error)
	CreateScheduledSourceDefinitionRun(ctx context.Context, scheduleID int64, scheduledFor time.Time, jobID, definitionJSON string, source JobDefinitionSourceRecord, audit RunAuditMetadata) (runID string, runIndexOut int, definitionVersion int, created bool, err error)
	CreateReplayRun(ctx context.Context, sourceRunID string, targetCellID string, audit RunAuditMetadata) (CreatedRun, error)
	ListCreatedByTriggerInvocation(ctx context.Context, invocationID string) ([]CreatedRun, error)
	RecordExecutionPayload(ctx context.Context, runID, payloadJSON, definitionHash string) (payloadHash string, recordedPayloadJSON string, err error)
	GetExecutionPayloadHashForRun(ctx context.Context, runID string) (string, error)
	GetExecutionPayloadForRun(ctx context.Context, runID string) (ExecutionPayloadRecord, error)
	GetExecutionPayloadByHash(ctx context.Context, payloadHash string) (ExecutionPayloadRecord, error)
	UpsertRunHotStateOwner(ctx context.Context, update RunHotStateOwnerUpdate) error
	ClearRunHotStateOwner(ctx context.Context, runID string) error
	GetRunHotStateOwner(ctx context.Context, runID string) (RunHotStateOwnerRecord, bool, error)
	ListAll(ctx context.Context, cursor int64, limit int) ([]RunRecord, int64, error)
	ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]RunRecord, int64, error)
	ListRunTasks(ctx context.Context, runID string, cursor int64, limit int) ([]TaskRecord, int64, error)
	GetRunNamespacePath(ctx context.Context, runID string) (string, error)
	RecordExecutionSecurityEvent(ctx context.Context, event RecordExecutionSecurityEventParams) error
	LatestRunSecurityEvent(ctx context.Context, runID string, failedOnly bool) (*ExecutionSecurityEvent, error)
	EnsurePlannedTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error)
	EnsurePendingTaskExecution(ctx context.Context, create TaskExecutionCreate) (TaskExecutionRecord, bool, error)
	ApplyTerminalExecutionSnapshot(ctx context.Context, update TerminalExecutionSnapshotUpdate) error
	ActivatePlannedTaskExecution(ctx context.Context, taskID string) (TaskExecutionRecord, bool, error)
	ActivatePlannedChildTaskExecutions(ctx context.Context, parentTaskID string) ([]TaskExecutionRecord, int, error)
	MarkRunQueuedForContinuation(ctx context.Context, runID string) error
	GetRunTaskCompletion(ctx context.Context, runID string) (RunTaskCompletion, error)
	ListOrphanedTaskFinalizationCandidates(ctx context.Context, limit int) ([]RunTaskCompletion, error)
	CountOrphanedTaskFinalizationCandidates(ctx context.Context) (int64, error)
	CountOrphanedTaskFinalizationCandidatesByCell(ctx context.Context) ([]RunCountByCell, error)
	CountPendingTaskContinuations(ctx context.Context) (int64, error)
	CountPendingTaskContinuationsByCell(ctx context.Context) ([]RunCountByCell, error)
	GetQueuedRunForDispatch(ctx context.Context, runID string) (QueuedRun, bool, error)
	ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]QueuedRun, error)
	ListQueuedBeforeDispatchCutoffLimit(ctx context.Context, cutoffUnix int64, limit int) ([]QueuedRun, error)
	GetPendingExecution(ctx context.Context, runID string) (ExecutionDispatchRecord, error)
	ListPendingExecutions(ctx context.Context, runID string) ([]ExecutionDispatchRecord, error)
	GetExecutionDispatch(ctx context.Context, executionID string) (ExecutionDispatchRecord, error)
	EnsureExecutionStartDeadline(ctx context.Context, executionID string, deadlineUnixNano int64) (int64, error)
	GetActiveExecutionDispatch(ctx context.Context, runID, executionID string) (ExecutionDispatchRecord, error)
	TryClaimExecution(ctx context.Context, executionID, owner string, leaseUntil time.Time) (ExecutionClaimResult, error)
	MirrorExecutionClaim(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error
	RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error
	ValidateActiveExecutionClaim(ctx context.Context, runID, executionID, claimToken string) error
	CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (ExecutionFinalizationResult, error)
	MarkExecutionStarted(ctx context.Context, executionID string) error
	CountByStatus(ctx context.Context, status string) (int64, error)
	CountByStatusByCell(ctx context.Context, status string) ([]RunCountByCell, error)
	CountStuckBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) (int64, error)
	CountStuckBeforeDispatchCutoffByCell(ctx context.Context, cutoffUnix int64) ([]RunCountByCell, error)
	GetRunJobID(ctx context.Context, runID string) (string, error)
	GetRunForCancel(ctx context.Context, runID string) (RunForCancel, error)
	GetRun(ctx context.Context, runID string) (RunRecord, error)
}

type SourceRepositoryRunLister interface {
	ListBySourceRepositoryJob(ctx context.Context, repositoryID, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]RunRecord, int64, error)
}

type SchedulesRepository interface {
	CreateCronSchedule(ctx context.Context, rec CronScheduleRecord) (CronScheduleRecord, error)
	UpdateCronSchedule(ctx context.Context, rec CronScheduleRecord) (CronScheduleRecord, error)
	GetCronScheduleByScheduleID(ctx context.Context, scheduleID string) (CronScheduleRecord, error)
	ListSourceCronSchedules(ctx context.Context, namespaceID int64, repositoryID string) ([]CronScheduleRecord, error)
	CountSourceCronSchedules(ctx context.Context, declaredScheduleIDs []string) (SourceCronScheduleCountSummary, error)
	SetSourceCronScheduleOverride(ctx context.Context, scheduleID string, override SourceScheduleOverride) (CronScheduleRecord, error)
	ClearSourceCronScheduleOverride(ctx context.Context, scheduleID string) (CronScheduleRecord, error)
	DeleteSourceCronSchedule(ctx context.Context, scheduleID string) error
	GetReady(ctx context.Context, at time.Time) ([]CronSchedule, error)
	ClaimDue(ctx context.Context, scheduleID int64, observedNextRun time.Time, claimToken string, claimedUntil, now time.Time) (bool, error)
	CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error)
	ReleaseClaim(ctx context.Context, scheduleID int64, claimToken string) error
	CountCronSchedules(ctx context.Context) (int64, error)
	CronScheduleSummary(ctx context.Context, at time.Time) (CronScheduleSummary, error)
}

type ServiceLeasesRepository interface {
	TryAcquire(ctx context.Context, name, owner string, now, leaseUntil time.Time) (bool, error)
	Release(ctx context.Context, name, owner string) error
}

type JobsRepository interface {
	CreateDefinitionSnapshot(ctx context.Context, jobID, definitionJSON string) error
	GetLatestDefinition(ctx context.Context, jobID string) (definitionJSON string, version int, err error)
	GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error)
}

type SourcesRepository interface {
	CreateRepository(ctx context.Context, rec SourceRepositoryRecord) (SourceRepositoryRecord, error)
	UpdateRepository(ctx context.Context, rec SourceRepositoryRecord) (SourceRepositoryRecord, error)
	DeleteRepository(ctx context.Context, repositoryID string) error
	BeginRepositorySync(ctx context.Context, rec SourceRepositorySyncRecord) (SourceRepositoryRecord, bool, error)
	UpdateRepositorySync(ctx context.Context, rec SourceRepositorySyncRecord) (SourceRepositoryRecord, error)
	GetRepository(ctx context.Context, repositoryID string) (SourceRepositoryRecord, error)
	ListRepositories(ctx context.Context, namespaceID int64) ([]SourceRepositoryRecord, error)
	CountRepositories(ctx context.Context, declaredRepositoryIDs []string) (SourceRepositoryCountSummary, error)
	RecordDefinitionSource(ctx context.Context, rec JobDefinitionSourceRecord) error
	GetDefinitionSource(ctx context.Context, jobID string, version int) (JobDefinitionSourceRecord, error)
	GetDefinitionSources(ctx context.Context, jobID string, versions []int) (map[int]JobDefinitionSourceRecord, error)
}

type EphemeralRunStarterWithAudit interface {
	EphemeralRunStarter
	CreateDefinitionAndRunInCellWithAudit(ctx context.Context, jobID, definitionJSON string, runIndex *int, targetCellID string, audit RunAuditMetadata) (runID string, runIndexOut int, err error)
}

type SourceDefinitionRunStarter interface {
	CreateSourceDefinitionAndRunInCellWithAudit(ctx context.Context, jobID, definitionJSON string, source JobDefinitionSourceRecord, targetCellID string, audit RunAuditMetadata) (runID string, runIndexOut int, definitionVersion int, err error)
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
	artifacts     *SQLArtifactsRepository
	triggers      *SQLTriggerInvocationsRepository
	catalog       *SQLCatalogEventsRepository
	catalogState  *SQLCatalogStatusBackfillRepository
	reactions     *SQLReactionsRepository
	cellAccept    *SQLCellExecutionAcceptancesRepository
	serviceLeases *SQLServiceLeasesRepository
	sources       *SQLSourcesRepository
}

func NewSQLRepositories(db *sql.DB) *SQLRepositories {
	return NewSQLRepositoriesWithCellID(db, DefaultCellID)
}

func NewSQLRepositoriesWithCellID(db *sql.DB, cellID string) *SQLRepositories {
	cellID = normalizeCellID(cellID)
	return &SQLRepositories{
		db:            db,
		cellID:        cellID,
		jobs:          &SQLJobsRepository{db: db},
		runs:          &SQLRunsRepository{db: db, cellID: cellID},
		schedules:     &SQLSchedulesRepository{db: db},
		auth:          NewSQLAuthRepository(db),
		namespaces:    NewSQLNamespacesRepositoryWithCellID(db, cellID),
		roleBindings:  NewSQLRoleBindingsRepository(db),
		idempotency:   &SQLIdempotencyRepository{db: db},
		dispatch:      &SQLDispatchEventsRepository{db: db},
		artifacts:     &SQLArtifactsRepository{db: db, cellID: cellID},
		triggers:      &SQLTriggerInvocationsRepository{db: db},
		catalog:       &SQLCatalogEventsRepository{db: db},
		catalogState:  &SQLCatalogStatusBackfillRepository{db: db},
		reactions:     &SQLReactionsRepository{db: db, cellID: cellID},
		cellAccept:    &SQLCellExecutionAcceptancesRepository{db: db, cellID: cellID},
		serviceLeases: &SQLServiceLeasesRepository{db: db},
		sources:       &SQLSourcesRepository{db: db},
	}
}

var _ EphemeralRunStarter = (*SQLRepositories)(nil)
var _ EphemeralRunStarterWithAudit = (*SQLRepositories)(nil)
var _ SourceDefinitionRunStarter = (*SQLRepositories)(nil)

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

func nullableInt64(value int64) any {
	if value <= 0 {
		return nil
	}

	return value
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

func createInitialSegmentExecutionTx(ctx context.Context, tx *sql.Tx, runID, cellID string, startDeadlineUnixNano int64) (TaskExecutionRecord, error) {
	if err := createInitialRootTaskAttemptTx(ctx, tx, runID, cellID); err != nil {
		return TaskExecutionRecord{}, err
	}

	taskID := rootTaskID(runID)
	taskAttemptID := rootTaskAttemptID(runID, 1)
	segmentID := newSegmentID()
	executionID := newExecutionID()
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO run_segments (segment_id, run_id, name, status) VALUES (?, ?, ?, ?)"),
		segmentID,
		runID,
		RootTaskKey,
		SegmentStatusPending,
	); err != nil {
		return TaskExecutionRecord{}, normalizeSQLError(err)
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx("INSERT INTO segment_executions (execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt, start_deadline_unix_nano) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
		executionID,
		segmentID,
		runID,
		taskID,
		taskAttemptID,
		normalizeCellID(cellID),
		ExecutionStatusPending,
		1,
		nullableInt64(startDeadlineUnixNano),
	); err != nil {
		return TaskExecutionRecord{}, normalizeSQLError(err)
	}

	return TaskExecutionRecord{
		RunID:         runID,
		TaskID:        taskID,
		TaskKey:       RootTaskKey,
		Name:          RootTaskKey,
		TaskAttemptID: taskAttemptID,
		SegmentID:     segmentID,
		SegmentName:   RootTaskKey,
		ExecutionID:   executionID,
		CellID:        normalizeCellID(cellID),
		Attempt:       1,
	}, nil
}

func rootDispatchRecord(
	runID string,
	jobID string,
	namespacePath string,
	runIndex int,
	root TaskExecutionRecord,
	definitionVersion int,
	definitionHash string,
	owningCell string,
	startDeadlineUnixNano int64,
) ExecutionDispatchRecord {
	namespacePath = strings.TrimSpace(namespacePath)
	if namespacePath == "" {
		namespacePath = "/"
	}

	return ExecutionDispatchRecord{
		RunID:                 runID,
		JobID:                 jobID,
		NamespacePath:         namespacePath,
		RunIndex:              runIndex,
		TaskID:                root.TaskID,
		TaskKey:               root.TaskKey,
		TaskName:              root.Name,
		TaskAttemptID:         root.TaskAttemptID,
		SegmentID:             root.SegmentID,
		SegmentName:           root.SegmentName,
		SegmentStatus:         SegmentStatusPending,
		ExecutionID:           root.ExecutionID,
		ExecutionStatus:       ExecutionStatusPending,
		CellID:                root.CellID,
		Attempt:               root.Attempt,
		DefinitionVersion:     definitionVersion,
		DefinitionHash:        definitionHash,
		OwningCell:            normalizeCellID(owningCell),
		StartDeadlineUnixNano: startDeadlineUnixNano,
	}
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

	namespacePath := strings.TrimSpace(audit.NamespacePath)
	if namespacePath == "" {
		namespacePath = RootNamespacePath
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash, namespace_path) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, 1, ?, ?, ?, ?, ?, ?)`),
		runID,
		jobID,
		idx,
		"queued",
		definitionHash,
		targetCellID,
		nullableString(audit.ReplayOfRunID),
		nullableString(audit.TriggerInvocationID),
		strings.TrimSpace(audit.ExecutionPayloadHash),
		namespacePath,
	); err != nil {
		return "", 0, normalizeSQLError(err)
	}

	if _, err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID, audit.StartDeadlineUnixNano); err != nil {
		return "", 0, err
	}

	if err := tx.Commit(); err != nil {
		return "", 0, err
	}

	return runID, idx, nil
}

func (r *SQLRepositories) CreateSourceDefinitionAndRunInCellWithAudit(ctx context.Context, jobID, definitionJSON string, source JobDefinitionSourceRecord, targetCellID string, audit RunAuditMetadata) (runID string, runIndexOut int, definitionVersion int, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return "", 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "", 0, 0, fmt.Errorf("%w: job_id is required", ErrConflict)
	}

	var nextVersion int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COALESCE(MAX(version), 0) + 1 FROM job_definitions WHERE job_id = ?"),
		jobID,
	).Scan(&nextVersion); err != nil {
		return "", 0, 0, normalizeSQLError(err)
	}

	definitionHash := DefinitionHash(definitionJSON)
	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_definitions (global_id, job_id, version, definition_json, definition_hash) VALUES (?, ?, ?, ?, ?)`),
		newGlobalID(), jobID, nextVersion, definitionJSON, definitionHash,
	); err != nil {
		return "", 0, 0, normalizeSQLError(err)
	}

	source.JobID = jobID
	source.Version = nextVersion
	if err := insertDefinitionSourceTx(ctx, tx, source); err != nil {
		return "", 0, 0, err
	}

	runID = uuid.New().String()
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?"),
		jobID,
	).Scan(&runIndexOut); err != nil {
		return "", 0, 0, normalizeSQLError(err)
	}

	targetCellID = normalizeTargetCellID(targetCellID, r.cellID)
	namespacePath := strings.TrimSpace(audit.NamespacePath)
	if namespacePath == "" {
		namespacePath = RootNamespacePath
	}

	if _, err := tx.ExecContext(ctx,
		rebindQueryForPgx(`INSERT INTO job_runs (run_id, job_id, run_index, status, created_at, started_at, definition_version, definition_hash, owning_cell, replay_of_run_id, trigger_invocation_id, execution_payload_hash, namespace_path) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?, ?, ?, ?, ?, ?, ?)`),
		runID,
		jobID,
		runIndexOut,
		RunStatusQueued,
		nextVersion,
		definitionHash,
		targetCellID,
		nullableString(audit.ReplayOfRunID),
		nullableString(audit.TriggerInvocationID),
		strings.TrimSpace(audit.ExecutionPayloadHash),
		namespacePath,
	); err != nil {
		return "", 0, 0, normalizeSQLError(err)
	}

	if _, err := createInitialSegmentExecutionTx(ctx, tx, runID, targetCellID, audit.StartDeadlineUnixNano); err != nil {
		return "", 0, 0, err
	}

	if err := tx.Commit(); err != nil {
		return "", 0, 0, err
	}

	return runID, runIndexOut, nextVersion, nil
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

func (r *SQLRepositories) Artifacts() ArtifactsRepository {
	return r.artifacts
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

func (r *SQLRepositories) Reactions() ReactionsRepository {
	return r.reactions
}

func (r *SQLRepositories) CellExecutionAcceptances() CellExecutionAcceptancesRepository {
	return r.cellAccept
}

func (r *SQLRepositories) ServiceLeases() ServiceLeasesRepository {
	return r.serviceLeases
}

func (r *SQLRepositories) Sources() SourcesRepository {
	return r.sources
}
