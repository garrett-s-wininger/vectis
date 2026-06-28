package mocks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vectis/internal/dal"
)

type StubEphemeralRunStarter struct{}

func (StubEphemeralRunStarter) CreateDefinitionAndRun(context.Context, string, string, *int) (string, int, error) {
	return "", 0, fmt.Errorf("stub ephemeral run starter: not configured")
}

func (StubEphemeralRunStarter) CreateDefinitionAndRunInCell(context.Context, string, string, *int, string) (string, int, error) {
	return "", 0, fmt.Errorf("stub ephemeral run starter: not configured")
}

func (StubEphemeralRunStarter) CreateDefinitionAndRunInCellWithAudit(context.Context, string, string, *int, string, dal.RunAuditMetadata) (string, int, error) {
	return "", 0, fmt.Errorf("stub ephemeral run starter: not configured")
}

type MockJobsRepository struct {
	Definitions        map[string]string
	DefinitionVersions map[string]int

	// Versions is jobID -> version -> definition JSON (ephemeral / versioned definitions).
	Versions map[string]map[int]string

	CreateErr error
	GetErr    error
}

func NewMockJobsRepository() *MockJobsRepository {
	return &MockJobsRepository{
		Definitions:        map[string]string{},
		DefinitionVersions: map[string]int{},
		Versions:           map[string]map[int]string{},
	}
}

func (m *MockJobsRepository) CreateDefinitionSnapshot(ctx context.Context, jobID, definitionJSON string) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}

	m.Definitions[jobID] = definitionJSON
	version := 1
	if m.Versions[jobID] == nil {
		m.Versions[jobID] = map[int]string{}
	} else {
		for existing := range m.Versions[jobID] {
			if existing >= version {
				version = existing + 1
			}
		}
	}

	m.DefinitionVersions[jobID] = version
	m.Versions[jobID][version] = definitionJSON
	return nil
}

func (m *MockJobsRepository) GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error) {
	if m.GetErr != nil {
		return "", m.GetErr
	}

	byVer, ok := m.Versions[jobID]
	if !ok {
		currentVersion := m.DefinitionVersions[jobID]
		if currentVersion <= 0 {
			currentVersion = 1
		}
		if currentVersion == version {
			if currentDef, currentOK := m.Definitions[jobID]; currentOK {
				return currentDef, nil
			}
		}

		return "", fmt.Errorf("%w: job %s version %d", dal.ErrNotFound, jobID, version)
	}

	def, ok := byVer[version]
	if !ok {
		currentVersion := m.DefinitionVersions[jobID]
		if currentVersion <= 0 {
			currentVersion = 1
		}

		if currentVersion == version {
			if currentDef, currentOK := m.Definitions[jobID]; currentOK {
				return currentDef, nil
			}
		}

		return "", fmt.Errorf("%w: job %s version %d", dal.ErrNotFound, jobID, version)
	}

	return def, nil
}

var _ dal.JobsRepository = (*MockJobsRepository)(nil)

type MockRunsRepository struct {
	mu sync.Mutex

	CreateRunID      string
	CreateRunIndex   int
	CreateRunCreated bool

	CreateRunErr                  error
	TouchDispatchedErr            error
	ListByJobErr                  error
	ListRunTasksErr               error
	RecordSecurityEventErr        error
	EnsureTaskExecutionErr        error
	ActivateTaskErr               error
	MarkQueuedContinuationErr     error
	TaskCompletionErr             error
	QueuedListErr                 error
	TryClaimExecutionErr          error
	MirrorExecutionClaimErr       error
	RenewExecutionLeaseErr        error
	RequestCancelErr              error
	CancelRequestedErr            error
	MarkRunRunningErr             error
	MarkRunSuccessErr             error
	MarkRunFailedErr              error
	MarkRunCancelledErr           error
	MarkRunAbortedErr             error
	MarkRunOrphanedErr            error
	RepairMarkErr                 error
	RequeueRunErr                 error
	MarkOrphanedErr               error
	GetRunStatusErr               error
	CountByStatusErr              error
	CountByStatusByCellErr        error
	CountStuckErr                 error
	CountStuckByCellErr           error
	CountTaskFinalizeErr          error
	CountTaskFinalizeByCellErr    error
	CountTaskContinuationErr      error
	CountTaskContinuationCellsErr error
	PendingExecutionErr           error
	MarkExecutionErr              error
	ExecutionFinalizationErr      error
	TerminalSnapshotErr           error
	ValidateExecutionClaimErr     error
	LogShardErr                   error
	EnsureExecutionDeadlineErr    error
	ExpireQueuedExecutionsErr     error
	HotStateOwnerErr              error

	CountByStatusResult         int64
	CountByStatusByCellResult   []dal.RunCountByCell
	CountStuckResult            int64
	CountStuckByCell            []dal.RunCountByCell
	CountTaskFinalizeResult     int64
	CountTaskFinalizeByCell     []dal.RunCountByCell
	CountTaskContinuationResult int64
	CountTaskContinuationByCell []dal.RunCountByCell

	TryClaimExecutionResult          bool
	TryClaimExecutionAlreadyAccepted bool
	TryClaimExecutionExpired         bool
	ExecutionClaimToken              string
	RunStatus                        string
	RunStatusFound                   bool
	CancelRequested                  bool
	OrphanedRunIDs                   []string
	LogShardID                       string
	LogShardSet                      bool

	ListByJobResults       []dal.RunRecord
	TaskRecords            []dal.TaskRecord
	LatestSecurityEvent    *dal.ExecutionSecurityEvent
	TaskExecution          dal.TaskExecutionRecord
	TaskCreated            bool
	TaskActivated          bool
	TaskExecutions         []dal.TaskExecutionRecord
	TaskActivatedN         int
	TaskCompletion         dal.RunTaskCompletion
	ExecutionFinalization  dal.ExecutionFinalizationResult
	TaskFinalizeCandidates []dal.RunTaskCompletion
	RunRecords             map[string]dal.RunRecord
	RunNamespacePaths      map[string]string
	QueuedRuns             []dal.QueuedRun
	PendingExecution       dal.ExecutionDispatchRecord
	PendingExecutions      []dal.ExecutionDispatchRecord
	ExecutionDispatches    map[string]dal.ExecutionDispatchRecord
	ExpiredExecutions      []dal.ExpiredExecution
	HotStateOwner          dal.RunHotStateOwnerRecord
	HotStateOwnerFound     bool

	TouchedRunIDs        []string
	ExecutionTransitions []string

	LastCreateJobID        string
	LastDefinitionVersion  int
	LastRunAudit           dal.RunAuditMetadata
	LastCreateTargetCell   string
	LastCreateTargetCells  []string
	RecordedPayloads       map[string]string
	ExecutionPayloads      map[string]dal.ExecutionPayloadRecord
	LastScheduleID         int64
	LastScheduledFor       time.Time
	LastSourceRecord       dal.JobDefinitionSourceRecord
	LastListJobID          string
	LastListAfterIndex     *int
	LastListSince          *time.Time
	LastListOwningCell     string
	LastListRunTasksRunID  string
	LastQueuedListLimit    int
	RecordedSecurityEvents []dal.RecordExecutionSecurityEventParams
	LastTaskExecution      dal.TaskExecutionCreate
	LastActivatedTaskID    string
	LastActivatedParentID  string
	LastQueuedRunID        string
	LastExecutionClaimID   string
	LastExecutionOwner     string
	LastMirroredExecID     string
	LastMirroredToken      string
	LastMirroredLease      time.Time
	LastExecutionRenewID   string
	LastEnsuredExecution   string
	LastEnsuredDeadline    int64
	LastExpiryCutoff       int64
	LastExpiryLimit        int
	LastFinalizedExecID    string
	LastFinalizedStatus    string
	LastRunStatusUpdate    dal.RunStatusUpdate
	LastExecStatusUpdate   dal.ExecutionStatusUpdate
	LastTerminalSnapshot   dal.TerminalExecutionSnapshotUpdate
	LastHotStateOwner      dal.RunHotStateOwnerUpdate
	ClearedHotStateRunIDs  []string
}

func NewMockRunsRepository() *MockRunsRepository {
	return &MockRunsRepository{
		CreateRunID:      "mock-run-id",
		CreateRunIndex:   1,
		CreateRunCreated: true,
	}
}

func (m *MockRunsRepository) MarkRunRunning(ctx context.Context, runID string) error {
	return m.MarkRunRunningErr
}

func (m *MockRunsRepository) ApplyRunStatusUpdate(ctx context.Context, update dal.RunStatusUpdate) error {
	m.mu.Lock()
	m.LastRunStatusUpdate = update
	m.mu.Unlock()

	switch update.Status {
	case dal.RunStatusRunning:
		return m.MarkRunRunning(ctx, update.RunID)
	case dal.RunStatusSucceeded:
		return m.MarkRunSucceeded(ctx, update.RunID)
	case dal.RunStatusFailed:
		return m.MarkRunFailed(ctx, update.RunID, update.FailureCode, update.Reason)
	case dal.RunStatusCancelled:
		return m.MarkRunCancelled(ctx, update.RunID, update.Reason)
	case dal.RunStatusAborted:
		return m.MarkRunAborted(ctx, update.RunID, update.Reason)
	case dal.RunStatusOrphaned:
		return m.MarkRunOrphaned(ctx, update.RunID, update.Reason)
	default:
		return fmt.Errorf("%w: unsupported run status %s", dal.ErrConflict, update.Status)
	}
}

func (m *MockRunsRepository) ApplyExecutionStatusUpdate(ctx context.Context, update dal.ExecutionStatusUpdate) error {
	m.mu.Lock()
	m.LastExecStatusUpdate = update
	m.mu.Unlock()

	switch update.Status {
	case dal.ExecutionStatusAccepted:
		if m.MarkExecutionErr != nil {
			return m.MarkExecutionErr
		}

		m.mu.Lock()
		m.ExecutionTransitions = append(m.ExecutionTransitions, update.ExecutionID+":"+dal.ExecutionStatusAccepted)
		m.mu.Unlock()

		return nil
	case dal.ExecutionStatusRunning:
		return m.MarkExecutionStarted(ctx, update.ExecutionID)
	case dal.ExecutionStatusSucceeded, dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		if m.MarkExecutionErr != nil {
			return m.MarkExecutionErr
		}

		m.mu.Lock()
		m.ExecutionTransitions = append(m.ExecutionTransitions, update.ExecutionID+":"+update.Status)
		m.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("%w: unsupported execution status %s", dal.ErrConflict, update.Status)
	}
}

func (m *MockRunsRepository) MarkRunSucceeded(ctx context.Context, runID string) error {
	if m.MarkRunSuccessErr != nil {
		return m.MarkRunSuccessErr
	}

	return m.ClearRunHotStateOwner(ctx, runID)
}

func (m *MockRunsRepository) MarkRunFailed(ctx context.Context, runID, failureCode, reason string) error {
	if m.MarkRunFailedErr != nil {
		return m.MarkRunFailedErr
	}

	return m.ClearRunHotStateOwner(ctx, runID)
}

func (m *MockRunsRepository) MarkRunCancelled(ctx context.Context, runID, reason string) error {
	if m.MarkRunCancelledErr != nil {
		return m.MarkRunCancelledErr
	}

	return m.ClearRunHotStateOwner(ctx, runID)
}

func (m *MockRunsRepository) MarkRunAborted(ctx context.Context, runID, reason string) error {
	if m.MarkRunAbortedErr != nil {
		return m.MarkRunAbortedErr
	}

	return m.ClearRunHotStateOwner(ctx, runID)
}

func (m *MockRunsRepository) MarkRunOrphaned(ctx context.Context, runID, reason string) error {
	if m.MarkRunOrphanedErr != nil {
		return m.MarkRunOrphanedErr
	}

	return m.ClearRunHotStateOwner(ctx, runID)
}

func (m *MockRunsRepository) RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RepairMarkRunFailed(ctx context.Context, runID, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RepairMarkRunFailedWithCode(ctx context.Context, runID, failureCode, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RepairMarkRunCancelled(ctx context.Context, runID, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RepairMarkRunAbandoned(ctx context.Context, runID, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RequeueRunForRetry(ctx context.Context, runID string) error {
	return m.RequeueRunErr
}

func (m *MockRunsRepository) MarkExpiredRunningAsOrphaned(ctx context.Context, cutoffUnix int64) ([]string, error) {
	if m.MarkOrphanedErr != nil {
		return nil, m.MarkOrphanedErr
	}

	return append([]string(nil), m.OrphanedRunIDs...), nil
}

func (m *MockRunsRepository) MarkExpiredQueuedExecutionsFailed(ctx context.Context, cutoffUnixNano int64, limit int) ([]dal.ExpiredExecution, error) {
	if m.ExpireQueuedExecutionsErr != nil {
		return nil, m.ExpireQueuedExecutionsErr
	}

	m.mu.Lock()
	m.LastExpiryCutoff = cutoffUnixNano
	m.LastExpiryLimit = limit
	m.mu.Unlock()

	return append([]dal.ExpiredExecution(nil), m.ExpiredExecutions...), nil
}

func (m *MockRunsRepository) GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error) {
	if m.GetRunStatusErr != nil {
		return "", false, m.GetRunStatusErr
	}
	return m.RunStatus, m.RunStatusFound, nil
}

func (m *MockRunsRepository) RequestRunCancel(ctx context.Context, runID, reason string) (dal.RunForCancel, error) {
	if m.RequestCancelErr != nil {
		return dal.RunForCancel{RunID: runID, Status: m.RunStatus}, m.RequestCancelErr
	}

	status := m.RunStatus
	if status == "" {
		status = dal.RunStatusRunning
	}

	return dal.RunForCancel{
		RunID:       runID,
		Status:      status,
		LeaseOwner:  "mock-worker",
		CancelToken: "mock-cancel-token",
	}, nil
}

func (m *MockRunsRepository) RunCancelRequested(ctx context.Context, runID string) (bool, error) {
	if m.CancelRequestedErr != nil {
		return false, m.CancelRequestedErr
	}

	return m.CancelRequested, nil
}

func (m *MockRunsRepository) TouchDispatched(ctx context.Context, runID string) error {
	if m.TouchDispatchedErr != nil {
		return m.TouchDispatchedErr
	}

	m.mu.Lock()
	m.TouchedRunIDs = append(m.TouchedRunIDs, runID)
	m.mu.Unlock()
	return nil
}

func (m *MockRunsRepository) GetLogShard(ctx context.Context, runID string) (string, bool, error) {
	if m.LogShardErr != nil {
		return "", false, m.LogShardErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LogShardID, m.LogShardSet, nil
}

func (m *MockRunsRepository) AssignLogShard(ctx context.Context, runID, shardID string) (string, error) {
	if m.LogShardErr != nil {
		return "", m.LogShardErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.LogShardSet {
		m.LogShardID = shardID
		m.LogShardSet = true
	}

	return m.LogShardID, nil
}

func (m *MockRunsRepository) CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (string, int, error) {
	return m.CreateRunInCell(ctx, jobID, runIndex, definitionVersion, dal.DefaultCellID)
}

func (m *MockRunsRepository) CreateRunInCell(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellID string) (string, int, error) {
	created, err := m.CreateRunsInCells(ctx, jobID, runIndex, definitionVersion, []string{targetCellID})
	if err != nil {
		return "", 0, err
	}

	if len(created) == 0 {
		return "", 0, fmt.Errorf("mock runs repository created no runs")
	}

	return created[0].RunID, created[0].RunIndex, nil
}

func (m *MockRunsRepository) CreateRunsInCells(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string) ([]dal.CreatedRun, error) {
	return m.CreateRunsInCellsWithAudit(ctx, jobID, runIndex, definitionVersion, targetCellIDs, dal.RunAuditMetadata{})
}

func (m *MockRunsRepository) CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit dal.RunAuditMetadata) ([]dal.CreatedRun, error) {
	if m.CreateRunErr != nil {
		return nil, m.CreateRunErr
	}

	if len(targetCellIDs) == 0 {
		targetCellIDs = []string{dal.DefaultCellID}
	}

	runIndexStart := m.CreateRunIndex
	if runIndex != nil {
		runIndexStart = *runIndex
	}
	if runIndexStart <= 0 {
		runIndexStart = 1
	}

	m.mu.Lock()
	m.LastCreateJobID = jobID
	m.LastDefinitionVersion = definitionVersion
	m.LastRunAudit = audit
	m.LastCreateTargetCell = targetCellIDs[0]
	m.LastCreateTargetCells = append([]string(nil), targetCellIDs...)
	definitionHash := m.PendingExecution.DefinitionHash
	m.mu.Unlock()

	baseRunID := m.CreateRunID
	if baseRunID == "" {
		baseRunID = "mock-run-id"
	}

	created := make([]dal.CreatedRun, 0, len(targetCellIDs))
	for i, targetCellID := range targetCellIDs {
		runID := baseRunID
		if len(targetCellIDs) > 1 {
			runID = fmt.Sprintf("%s-%d", baseRunID, i+1)
		}

		namespacePath := strings.TrimSpace(audit.NamespacePath)
		if namespacePath == "" {
			namespacePath = dal.RootNamespacePath
		}

		m.mu.Lock()
		if m.RunNamespacePaths == nil {
			m.RunNamespacePaths = map[string]string{}
		}
		m.RunNamespacePaths[runID] = namespacePath
		m.mu.Unlock()

		created = append(created, dal.CreatedRun{
			RunID:        runID,
			JobID:        jobID,
			RunIndex:     runIndexStart + i,
			TargetCellID: targetCellID,
			RootDispatch: mockRootDispatchRecord(
				runID,
				jobID,
				runIndexStart+i,
				targetCellID,
				definitionVersion,
				definitionHash,
				namespacePath,
				audit.StartDeadlineUnixNano,
			),
		})
	}

	return created, nil
}

func (m *MockRunsRepository) ListCreatedByTriggerInvocation(ctx context.Context, invocationID string) ([]dal.CreatedRun, error) {
	return nil, nil
}

func mockRootDispatchRecord(runID, jobID string, runIndex int, targetCellID string, definitionVersion int, definitionHash string, namespacePath string, startDeadlineUnixNano int64) dal.ExecutionDispatchRecord {
	targetCellID = strings.TrimSpace(targetCellID)
	if targetCellID == "" {
		targetCellID = dal.DefaultCellID
	}

	namespacePath = strings.TrimSpace(namespacePath)
	if namespacePath == "" {
		namespacePath = "/"
	}

	definitionHash = strings.TrimSpace(definitionHash)
	if definitionHash == "" {
		definitionHash = "mock-definition-hash"
	}

	return dal.ExecutionDispatchRecord{
		RunID:                 runID,
		JobID:                 jobID,
		NamespacePath:         namespacePath,
		RunIndex:              runIndex,
		TaskID:                runID + ":" + dal.RootTaskKey,
		TaskKey:               dal.RootTaskKey,
		TaskName:              dal.RootTaskKey,
		TaskAttemptID:         runID + ":" + dal.RootTaskKey + ":attempt:1",
		SegmentID:             runID + ":" + dal.RootTaskKey + ":segment",
		SegmentName:           dal.RootTaskKey,
		SegmentStatus:         dal.SegmentStatusPending,
		ExecutionID:           runID + ":" + dal.RootTaskKey + ":attempt:1:execution",
		ExecutionStatus:       dal.ExecutionStatusPending,
		CellID:                targetCellID,
		Attempt:               1,
		DefinitionVersion:     definitionVersion,
		DefinitionHash:        definitionHash,
		OwningCell:            targetCellID,
		StartDeadlineUnixNano: startDeadlineUnixNano,
	}
}

func (m *MockRunsRepository) CreateReplayRun(ctx context.Context, sourceRunID string, targetCellID string, audit dal.RunAuditMetadata) (dal.CreatedRun, error) {
	sourceRun, ok := m.RunRecords[sourceRunID]
	if ok && (sourceRun.Status == dal.RunStatusQueued || sourceRun.Status == dal.RunStatusRunning) {
		return dal.CreatedRun{}, fmt.Errorf("%w: source run %s in status %s cannot be replayed", dal.ErrConflict, sourceRunID, sourceRun.Status)
	}

	jobID := m.LastCreateJobID
	definitionVersion := m.LastDefinitionVersion
	if ok && sourceRun.DefinitionVersion > 0 {
		definitionVersion = sourceRun.DefinitionVersion
	}

	if definitionVersion <= 0 {
		definitionVersion = 1
	}

	if strings.TrimSpace(targetCellID) == "" && ok {
		targetCellID = sourceRun.OwningCell
	}

	if strings.TrimSpace(audit.ReplayOfRunID) == "" {
		audit.ReplayOfRunID = sourceRunID
	}

	created, err := m.CreateRunsInCellsWithAudit(ctx, jobID, nil, definitionVersion, []string{targetCellID}, audit)
	if err != nil {
		return dal.CreatedRun{}, err
	}

	if len(created) == 0 {
		return dal.CreatedRun{}, fmt.Errorf("mock runs repository created no replay run")
	}

	return created[0], nil
}

func (m *MockRunsRepository) RecordExecutionPayload(ctx context.Context, runID, payloadJSON, definitionHash string) (string, string, error) {
	payloadHash := dal.ExecutionPayloadHash(payloadJSON)
	m.mu.Lock()
	if m.RecordedPayloads == nil {
		m.RecordedPayloads = map[string]string{}
	}

	if recordedPayloadJSON, ok := m.RecordedPayloads[runID]; ok {
		m.mu.Unlock()
		return dal.ExecutionPayloadHash(recordedPayloadJSON), recordedPayloadJSON, nil
	}
	m.RecordedPayloads[runID] = payloadJSON
	m.mu.Unlock()

	return payloadHash, payloadJSON, nil
}

func (m *MockRunsRepository) GetExecutionPayloadForRun(ctx context.Context, runID string) (dal.ExecutionPayloadRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ExecutionPayloads != nil {
		if rec, ok := m.ExecutionPayloads[runID]; ok {
			return rec, nil
		}
	}

	if payloadJSON, ok := m.RecordedPayloads[runID]; ok {
		payloadHash := dal.ExecutionPayloadHash(payloadJSON)
		return dal.ExecutionPayloadRecord{
			RunID:       runID,
			PayloadHash: payloadHash,
			PayloadJSON: payloadJSON,
		}, nil
	}

	return dal.ExecutionPayloadRecord{}, fmt.Errorf("%w: execution payload for run %s", dal.ErrNotFound, runID)
}

func (m *MockRunsRepository) GetExecutionPayloadHashForRun(ctx context.Context, runID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ExecutionPayloads != nil {
		if rec, ok := m.ExecutionPayloads[runID]; ok && strings.TrimSpace(rec.PayloadHash) != "" {
			return strings.TrimSpace(rec.PayloadHash), nil
		}
	}

	if payloadJSON, ok := m.RecordedPayloads[runID]; ok {
		return dal.ExecutionPayloadHash(payloadJSON), nil
	}

	return "", fmt.Errorf("%w: execution payload for run %s", dal.ErrNotFound, runID)
}

func (m *MockRunsRepository) GetExecutionPayloadByHash(ctx context.Context, payloadHash string) (dal.ExecutionPayloadRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, rec := range m.ExecutionPayloads {
		if rec.PayloadHash == payloadHash {
			return rec, nil
		}
	}

	for runID, payloadJSON := range m.RecordedPayloads {
		if dal.ExecutionPayloadHash(payloadJSON) == payloadHash {
			return dal.ExecutionPayloadRecord{
				RunID:       runID,
				PayloadHash: payloadHash,
				PayloadJSON: payloadJSON,
			}, nil
		}
	}

	return dal.ExecutionPayloadRecord{}, fmt.Errorf("%w: execution payload %s", dal.ErrNotFound, payloadHash)
}

func (m *MockRunsRepository) UpsertRunHotStateOwner(ctx context.Context, update dal.RunHotStateOwnerUpdate) error {
	if m.HotStateOwnerErr != nil {
		return m.HotStateOwnerErr
	}

	m.mu.Lock()
	m.LastHotStateOwner = update
	m.HotStateOwner = dal.RunHotStateOwnerRecord(update)
	m.HotStateOwnerFound = true
	m.mu.Unlock()

	return nil
}

func (m *MockRunsRepository) ClearRunHotStateOwner(ctx context.Context, runID string) error {
	if m.HotStateOwnerErr != nil {
		return m.HotStateOwnerErr
	}

	m.mu.Lock()
	m.ClearedHotStateRunIDs = append(m.ClearedHotStateRunIDs, runID)
	if m.HotStateOwner.RunID == runID {
		m.HotStateOwner = dal.RunHotStateOwnerRecord{}
		m.HotStateOwnerFound = false
	}
	m.mu.Unlock()

	return nil
}

func (m *MockRunsRepository) GetRunHotStateOwner(ctx context.Context, runID string) (dal.RunHotStateOwnerRecord, bool, error) {
	if m.HotStateOwnerErr != nil {
		return dal.RunHotStateOwnerRecord{}, false, m.HotStateOwnerErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.HotStateOwnerFound || m.HotStateOwner.RunID != runID {
		return dal.RunHotStateOwnerRecord{}, false, nil
	}

	return m.HotStateOwner, true, nil
}

func (m *MockRunsRepository) CreateScheduledSourceDefinitionRun(ctx context.Context, scheduleID int64, scheduledFor time.Time, jobID, definitionJSON string, source dal.JobDefinitionSourceRecord, audit dal.RunAuditMetadata) (string, int, int, bool, error) {
	if m.CreateRunErr != nil {
		return "", 0, 0, false, m.CreateRunErr
	}

	definitionVersion := source.Version
	if definitionVersion <= 0 {
		definitionVersion = m.LastDefinitionVersion
	}
	if definitionVersion <= 0 {
		definitionVersion = 1
	}

	m.mu.Lock()
	m.LastCreateJobID = jobID
	m.LastDefinitionVersion = definitionVersion
	m.LastRunAudit = audit
	m.LastScheduleID = scheduleID
	m.LastScheduledFor = scheduledFor
	m.LastSourceRecord = source
	m.mu.Unlock()

	return m.CreateRunID, m.CreateRunIndex, definitionVersion, m.CreateRunCreated, nil
}

func (m *MockRunsRepository) ListByJob(ctx context.Context, jobID string, afterIndex *int, since *time.Time, owningCell string, cursor int64, limit int) ([]dal.RunRecord, int64, error) {
	if m.ListByJobErr != nil {
		return nil, 0, m.ListByJobErr
	}

	m.mu.Lock()
	m.LastListJobID = jobID
	if afterIndex != nil {
		v := *afterIndex
		m.LastListAfterIndex = &v
	} else {
		m.LastListAfterIndex = nil
	}

	if since != nil {
		v := *since
		m.LastListSince = &v
	} else {
		m.LastListSince = nil
	}
	m.LastListOwningCell = owningCell
	m.mu.Unlock()

	return append([]dal.RunRecord(nil), m.ListByJobResults...), 0, nil
}

func (m *MockRunsRepository) ListRunTasks(ctx context.Context, runID string, cursor int64, limit int) ([]dal.TaskRecord, int64, error) {
	if m.ListRunTasksErr != nil {
		return nil, 0, m.ListRunTasksErr
	}

	m.mu.Lock()
	m.LastListRunTasksRunID = runID
	m.mu.Unlock()

	return append([]dal.TaskRecord(nil), m.TaskRecords...), 0, nil
}

func (m *MockRunsRepository) RecordExecutionSecurityEvent(ctx context.Context, event dal.RecordExecutionSecurityEventParams) error {
	if m.RecordSecurityEventErr != nil {
		return m.RecordSecurityEventErr
	}

	m.mu.Lock()
	m.RecordedSecurityEvents = append(m.RecordedSecurityEvents, event)
	m.mu.Unlock()

	return nil
}

func (m *MockRunsRepository) LatestRunSecurityEvent(ctx context.Context, runID string, failedOnly bool) (*dal.ExecutionSecurityEvent, error) {
	if m.LatestSecurityEvent == nil {
		return nil, nil
	}

	rec := *m.LatestSecurityEvent
	return &rec, nil
}

func (m *MockRunsRepository) SnapshotExecutionSecurityEvents() []dal.RecordExecutionSecurityEventParams {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]dal.RecordExecutionSecurityEventParams(nil), m.RecordedSecurityEvents...)
}

func (m *MockRunsRepository) CountByStatus(ctx context.Context, status string) (int64, error) {
	return m.CountByStatusResult, m.CountByStatusErr
}

func (m *MockRunsRepository) CountByStatusByCell(ctx context.Context, status string) ([]dal.RunCountByCell, error) {
	return append([]dal.RunCountByCell(nil), m.CountByStatusByCellResult...), m.CountByStatusByCellErr
}

func (m *MockRunsRepository) CountStuckBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) (int64, error) {
	return m.CountStuckResult, m.CountStuckErr
}

func (m *MockRunsRepository) CountStuckBeforeDispatchCutoffByCell(ctx context.Context, cutoffUnix int64) ([]dal.RunCountByCell, error) {
	return append([]dal.RunCountByCell(nil), m.CountStuckByCell...), m.CountStuckByCellErr
}

func (m *MockRunsRepository) GetRun(ctx context.Context, runID string) (dal.RunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.RunRecords != nil {
		if rec, ok := m.RunRecords[runID]; ok {
			return rec, nil
		}
	}

	return dal.RunRecord{RunID: runID, Status: m.RunStatus}, nil
}

func (m *MockRunsRepository) SnapshotTouchedRunIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.TouchedRunIDs...)
}

func (m *MockRunsRepository) SnapshotLastCreate() (string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastCreateJobID, m.LastDefinitionVersion
}

func (m *MockRunsRepository) SnapshotLastRunAudit() dal.RunAuditMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastRunAudit
}

func (m *MockRunsRepository) SnapshotLastCreateTargetCell() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastCreateTargetCell
}

func (m *MockRunsRepository) SnapshotLastCreateTargetCells() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.LastCreateTargetCells...)
}

func (m *MockRunsRepository) SnapshotLastScheduled() (int64, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastScheduleID, m.LastScheduledFor
}

func (m *MockRunsRepository) SnapshotLastListSince() *int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LastListAfterIndex == nil {
		return nil
	}

	v := *m.LastListAfterIndex
	return &v
}

func (m *MockRunsRepository) SnapshotLastListAfterIndex() *int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LastListAfterIndex == nil {
		return nil
	}

	v := *m.LastListAfterIndex
	return &v
}

func (m *MockRunsRepository) SnapshotLastListSinceTime() *time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LastListSince == nil {
		return nil
	}
	v := *m.LastListSince
	return &v
}

func (m *MockRunsRepository) SnapshotLastListOwningCell() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastListOwningCell
}

func (m *MockRunsRepository) ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]dal.QueuedRun, error) {
	return m.ListQueuedBeforeDispatchCutoffLimit(ctx, cutoffUnix, 0)
}

func (m *MockRunsRepository) ListQueuedBeforeDispatchCutoffLimit(ctx context.Context, cutoffUnix int64, limit int) ([]dal.QueuedRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastQueuedListLimit = limit
	if m.QueuedListErr != nil {
		return nil, m.QueuedListErr
	}

	out := append([]dal.QueuedRun(nil), m.QueuedRuns...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	return out, nil
}

func (m *MockRunsRepository) GetPendingExecution(ctx context.Context, runID string) (dal.ExecutionDispatchRecord, error) {
	if m.PendingExecutionErr != nil {
		return dal.ExecutionDispatchRecord{}, m.PendingExecutionErr
	}

	if len(m.PendingExecutions) > 0 {
		return m.defaultPendingExecutionForRun(m.PendingExecutions[0], runID), nil
	}

	return m.defaultPendingExecutionForRun(m.PendingExecution, runID), nil
}

func (m *MockRunsRepository) defaultPendingExecutionForRun(rec dal.ExecutionDispatchRecord, runID string) dal.ExecutionDispatchRecord {
	if rec.RunID == "" {
		rec.RunID = runID
	}

	if rec.RunIndex == 0 {
		rec.RunIndex = 1
	}

	if rec.TaskKey == "" {
		rec.TaskKey = dal.RootTaskKey
	}

	if rec.TaskID == "" {
		rec.TaskID = rec.RunID + ":" + rec.TaskKey
	}

	if rec.TaskName == "" {
		rec.TaskName = rec.TaskKey
	}

	if rec.TaskAttemptID == "" {
		rec.TaskAttemptID = fmt.Sprintf("%s:attempt:%d", rec.TaskID, defaultPositive(rec.Attempt, 1))
	}

	if rec.SegmentID == "" {
		rec.SegmentID = rec.RunID + ":" + rec.TaskKey + ":segment"
	}

	if rec.SegmentName == "" {
		rec.SegmentName = rec.TaskKey
	}

	if rec.ExecutionID == "" {
		rec.ExecutionID = rec.RunID + ":" + rec.TaskKey + ":attempt:1:execution"
	}

	if rec.CellID == "" {
		rec.CellID = dal.DefaultCellID
	}

	if rec.Attempt == 0 {
		rec.Attempt = 1
	}

	if rec.DefinitionVersion == 0 {
		rec.DefinitionVersion = 1
	}

	if queued, ok := m.queuedRunByID(rec.RunID); ok {
		if rec.JobID == "" {
			rec.JobID = queued.JobID
		}

		if rec.DefinitionVersion == 1 && queued.DefinitionVersion > 0 {
			rec.DefinitionVersion = queued.DefinitionVersion
		}

		if rec.DefinitionHash == "" {
			rec.DefinitionHash = queued.DefinitionHash
		}

		if rec.OwningCell == "" {
			rec.OwningCell = queued.OwningCell
		}
	}

	if rec.DefinitionHash == "" {
		rec.DefinitionHash = "test-definition-hash"
	}

	return rec
}

func defaultPositive(value, fallback int) int {
	if value > 0 {
		return value
	}

	return fallback
}

func (m *MockRunsRepository) queuedRunByID(runID string) (dal.QueuedRun, bool) {
	for _, queued := range m.QueuedRuns {
		if queued.RunID == runID {
			return queued, true
		}
	}

	return dal.QueuedRun{}, false
}

func (m *MockRunsRepository) ListPendingExecutions(ctx context.Context, runID string) ([]dal.ExecutionDispatchRecord, error) {
	if m.PendingExecutionErr != nil {
		return nil, m.PendingExecutionErr
	}

	if len(m.PendingExecutions) > 0 {
		out := append([]dal.ExecutionDispatchRecord(nil), m.PendingExecutions...)
		for i := range out {
			out[i] = m.defaultPendingExecutionForRun(out[i], runID)
		}
		return out, nil
	}

	return []dal.ExecutionDispatchRecord{m.defaultPendingExecutionForRun(m.PendingExecution, runID)}, nil
}

func (m *MockRunsRepository) GetExecutionDispatch(ctx context.Context, executionID string) (dal.ExecutionDispatchRecord, error) {
	if m.PendingExecutionErr != nil {
		return dal.ExecutionDispatchRecord{}, m.PendingExecutionErr
	}

	if m.ExecutionDispatches != nil {
		if rec, ok := m.ExecutionDispatches[executionID]; ok {
			return rec, nil
		}
	}

	rec := m.PendingExecution
	if rec.ExecutionID == "" {
		rec.ExecutionID = executionID
	}

	return rec, nil
}

func (m *MockRunsRepository) EnsureExecutionStartDeadline(ctx context.Context, executionID string, deadlineUnixNano int64) (int64, error) {
	if m.EnsureExecutionDeadlineErr != nil {
		return 0, m.EnsureExecutionDeadlineErr
	}

	m.mu.Lock()
	m.LastEnsuredExecution = executionID
	m.LastEnsuredDeadline = deadlineUnixNano
	m.mu.Unlock()

	if m.ExecutionDispatches != nil {
		if rec, ok := m.ExecutionDispatches[executionID]; ok && rec.StartDeadlineUnixNano > 0 {
			return rec.StartDeadlineUnixNano, nil
		}
	}

	if m.PendingExecution.StartDeadlineUnixNano > 0 {
		return m.PendingExecution.StartDeadlineUnixNano, nil
	}

	return deadlineUnixNano, nil
}

func (m *MockRunsRepository) GetActiveExecutionDispatch(ctx context.Context, runID, executionID string) (dal.ExecutionDispatchRecord, error) {
	rec, err := m.GetExecutionDispatch(ctx, executionID)
	if err != nil {
		return dal.ExecutionDispatchRecord{}, err
	}

	if rec.RunID == "" {
		rec.RunID = runID
	}

	return rec, nil
}

func (m *MockRunsRepository) TryClaimExecution(ctx context.Context, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	if m.TryClaimExecutionErr != nil {
		return dal.ExecutionClaimResult{}, m.TryClaimExecutionErr
	}

	m.mu.Lock()
	m.LastExecutionClaimID = executionID
	m.LastExecutionOwner = owner
	m.mu.Unlock()

	if m.TryClaimExecutionExpired {
		runID := m.PendingExecution.RunID
		if runID == "" {
			runID = "mock-run-id"
		}

		return dal.ExecutionClaimResult{
			Expired:     true,
			RunID:       runID,
			ExecutionID: executionID,
		}, nil
	}

	if !m.TryClaimExecutionResult {
		return dal.ExecutionClaimResult{}, nil
	}

	token := m.ExecutionClaimToken
	if token == "" {
		token = "mock-execution-claim-token"
	}

	return dal.ExecutionClaimResult{
		Claimed:                true,
		ClaimToken:             token,
		TransitionedToAccepted: !m.TryClaimExecutionAlreadyAccepted,
	}, nil
}

func (m *MockRunsRepository) MirrorExecutionClaim(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	if m.MirrorExecutionClaimErr != nil {
		return m.MirrorExecutionClaimErr
	}

	m.mu.Lock()
	m.LastMirroredExecID = executionID
	m.LastExecutionOwner = owner
	m.LastMirroredToken = claimToken
	m.LastMirroredLease = leaseUntil
	m.mu.Unlock()

	return nil
}

func (m *MockRunsRepository) RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	if m.RenewExecutionLeaseErr != nil {
		return m.RenewExecutionLeaseErr
	}

	m.mu.Lock()
	m.LastExecutionRenewID = executionID
	m.LastExecutionOwner = owner
	m.mu.Unlock()

	return nil
}

func (m *MockRunsRepository) ValidateActiveExecutionClaim(ctx context.Context, runID, executionID, claimToken string) error {
	if m.ValidateExecutionClaimErr != nil {
		return m.ValidateExecutionClaimErr
	}

	return nil
}

func (m *MockRunsRepository) EnsurePendingTaskExecution(ctx context.Context, create dal.TaskExecutionCreate) (dal.TaskExecutionRecord, bool, error) {
	m.mu.Lock()
	m.LastTaskExecution = create
	m.mu.Unlock()

	if m.EnsureTaskExecutionErr != nil {
		return dal.TaskExecutionRecord{}, false, m.EnsureTaskExecutionErr
	}

	return m.TaskExecution, m.TaskCreated, nil
}

func (m *MockRunsRepository) EnsurePlannedTaskExecution(ctx context.Context, create dal.TaskExecutionCreate) (dal.TaskExecutionRecord, bool, error) {
	m.mu.Lock()
	m.LastTaskExecution = create
	m.mu.Unlock()

	if m.EnsureTaskExecutionErr != nil {
		return dal.TaskExecutionRecord{}, false, m.EnsureTaskExecutionErr
	}

	return m.TaskExecution, m.TaskCreated, nil
}

func (m *MockRunsRepository) ApplyTerminalExecutionSnapshot(ctx context.Context, update dal.TerminalExecutionSnapshotUpdate) error {
	m.mu.Lock()
	m.LastTerminalSnapshot = update
	m.mu.Unlock()

	if m.TerminalSnapshotErr != nil {
		return m.TerminalSnapshotErr
	}

	return m.ClearRunHotStateOwner(ctx, update.RunID)
}

func (m *MockRunsRepository) ActivatePlannedTaskExecution(ctx context.Context, taskID string) (dal.TaskExecutionRecord, bool, error) {
	m.mu.Lock()
	m.LastActivatedTaskID = taskID
	m.mu.Unlock()

	if m.ActivateTaskErr != nil {
		return dal.TaskExecutionRecord{}, false, m.ActivateTaskErr
	}

	return m.TaskExecution, m.TaskActivated, nil
}

func (m *MockRunsRepository) ActivatePlannedChildTaskExecutions(ctx context.Context, parentTaskID string) ([]dal.TaskExecutionRecord, int, error) {
	m.mu.Lock()
	m.LastActivatedParentID = parentTaskID
	m.mu.Unlock()

	if m.ActivateTaskErr != nil {
		return nil, 0, m.ActivateTaskErr
	}

	return append([]dal.TaskExecutionRecord(nil), m.TaskExecutions...), m.TaskActivatedN, nil
}

func (m *MockRunsRepository) MarkRunQueuedForContinuation(ctx context.Context, runID string) error {
	m.mu.Lock()
	m.LastQueuedRunID = runID
	m.mu.Unlock()

	return m.MarkQueuedContinuationErr
}

func (m *MockRunsRepository) MarkExecutionStarted(ctx context.Context, executionID string) error {
	if m.MarkExecutionErr != nil {
		return m.MarkExecutionErr
	}

	m.mu.Lock()
	m.ExecutionTransitions = append(m.ExecutionTransitions, executionID+":"+dal.ExecutionStatusRunning)
	m.mu.Unlock()
	return nil
}

func (m *MockRunsRepository) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	if m.ExecutionFinalizationErr != nil {
		return dal.ExecutionFinalizationResult{}, m.ExecutionFinalizationErr
	}

	m.mu.Lock()
	m.LastFinalizedExecID = executionID
	m.LastExecutionOwner = owner
	m.LastFinalizedStatus = status
	m.mu.Unlock()

	result := m.ExecutionFinalization
	if result.ExecutionID == "" {
		result.ExecutionID = executionID
	}

	return result, nil
}

func (m *MockRunsRepository) GetRunTaskCompletion(ctx context.Context, runID string) (dal.RunTaskCompletion, error) {
	if m.TaskCompletionErr != nil {
		return dal.RunTaskCompletion{}, m.TaskCompletionErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	summary := m.TaskCompletion
	if summary.RunID == "" {
		summary.RunID = runID
	}

	return summary, nil
}

func (m *MockRunsRepository) ListOrphanedTaskFinalizationCandidates(ctx context.Context, limit int) ([]dal.RunTaskCompletion, error) {
	if m.TaskCompletionErr != nil {
		return nil, m.TaskCompletionErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	candidates := append([]dal.RunTaskCompletion(nil), m.TaskFinalizeCandidates...)
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	return candidates, nil
}

func (m *MockRunsRepository) CountOrphanedTaskFinalizationCandidates(ctx context.Context) (int64, error) {
	if m.CountTaskFinalizeErr != nil {
		return 0, m.CountTaskFinalizeErr
	}

	return m.CountTaskFinalizeResult, nil
}

func (m *MockRunsRepository) CountOrphanedTaskFinalizationCandidatesByCell(ctx context.Context) ([]dal.RunCountByCell, error) {
	if m.CountTaskFinalizeByCellErr != nil {
		return nil, m.CountTaskFinalizeByCellErr
	}

	return append([]dal.RunCountByCell(nil), m.CountTaskFinalizeByCell...), nil
}

func (m *MockRunsRepository) CountPendingTaskContinuations(ctx context.Context) (int64, error) {
	if m.CountTaskContinuationErr != nil {
		return 0, m.CountTaskContinuationErr
	}

	return m.CountTaskContinuationResult, nil
}

func (m *MockRunsRepository) CountPendingTaskContinuationsByCell(ctx context.Context) ([]dal.RunCountByCell, error) {
	if m.CountTaskContinuationCellsErr != nil {
		return nil, m.CountTaskContinuationCellsErr
	}

	return append([]dal.RunCountByCell(nil), m.CountTaskContinuationByCell...), nil
}

func (m *MockRunsRepository) GetRunJobID(ctx context.Context, runID string) (string, error) {
	return m.LastCreateJobID, nil
}

func (m *MockRunsRepository) GetRunNamespacePath(ctx context.Context, runID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if namespacePath := strings.TrimSpace(m.RunNamespacePaths[runID]); namespacePath != "" {
		return namespacePath, nil
	}

	if namespacePath := strings.TrimSpace(m.LastRunAudit.NamespacePath); namespacePath != "" {
		return namespacePath, nil
	}

	return dal.RootNamespacePath, nil
}

func (m *MockRunsRepository) GetRunForCancel(ctx context.Context, runID string) (dal.RunForCancel, error) {
	return dal.RunForCancel{
		RunID:       runID,
		Status:      m.RunStatus,
		LeaseOwner:  "mock-worker",
		CancelToken: "mock-cancel-token",
	}, nil
}

var _ dal.RunsRepository = (*MockRunsRepository)(nil)

type ClaimDueCall struct {
	ID              int64
	ObservedNextRun time.Time
	ClaimToken      string
	ClaimedUntil    time.Time
	Now             time.Time
}

type CompleteClaimCall struct {
	ID         int64
	ClaimToken string
	Next       time.Time
}

type ReleaseClaimCall struct {
	ID         int64
	ClaimToken string
}

type MockSchedulesRepository struct {
	Ready         []dal.CronSchedule
	CronSchedules map[string]dal.CronScheduleRecord

	GetReadyErr                        error
	CreateCronScheduleErr              error
	UpdateCronScheduleErr              error
	GetCronScheduleErr                 error
	ListSourceCronSchedulesErr         error
	SetSourceCronScheduleOverrideErr   error
	ClearSourceCronScheduleOverrideErr error
	DeleteSourceCronScheduleErr        error
	ClaimDueErr                        error
	ClaimDueOK                         bool
	CompleteClaimErr                   error
	CompleteClaimOK                    bool
	ReleaseClaimErr                    error
	CountCronSchedulesErr              error
	CountCronSchedulesResult           int64
	CronScheduleSummaryErr             error
	CronScheduleSummaryResult          dal.CronScheduleSummary

	GetReadyCalled     int
	ClaimDueCalls      []ClaimDueCall
	CompleteClaimCalls []CompleteClaimCall
	ReleaseClaimCalls  []ReleaseClaimCall
}

func NewMockSchedulesRepository() *MockSchedulesRepository {
	return &MockSchedulesRepository{
		ClaimDueOK:      true,
		CompleteClaimOK: true,
		CronSchedules:   map[string]dal.CronScheduleRecord{},
	}
}

func (m *MockSchedulesRepository) CreateCronSchedule(ctx context.Context, rec dal.CronScheduleRecord) (dal.CronScheduleRecord, error) {
	if m.CreateCronScheduleErr != nil {
		return dal.CronScheduleRecord{}, m.CreateCronScheduleErr
	}

	if m.CronSchedules == nil {
		m.CronSchedules = map[string]dal.CronScheduleRecord{}
	}

	if rec.ID == 0 {
		rec.ID = int64(len(m.CronSchedules) + 1)
	}

	if rec.TriggerID == 0 {
		rec.TriggerID = rec.ID
	}

	m.CronSchedules[rec.ScheduleID] = rec
	return rec, nil
}

func (m *MockSchedulesRepository) UpdateCronSchedule(ctx context.Context, rec dal.CronScheduleRecord) (dal.CronScheduleRecord, error) {
	if m.UpdateCronScheduleErr != nil {
		return dal.CronScheduleRecord{}, m.UpdateCronScheduleErr
	}

	existing, err := m.GetCronScheduleByScheduleID(ctx, rec.ScheduleID)
	if err != nil {
		return dal.CronScheduleRecord{}, err
	}

	if rec.ID == 0 {
		rec.ID = existing.ID
	}

	if rec.TriggerID == 0 {
		rec.TriggerID = existing.TriggerID
	}

	if rec.NextRunAt.IsZero() {
		rec.NextRunAt = existing.NextRunAt
	}

	rec.SourceOverrideRef = existing.SourceOverrideRef
	rec.SourceOverridePath = existing.SourceOverridePath
	rec.SourceOverrideReason = existing.SourceOverrideReason
	rec.SourceOverrideCreatedAtUnix = existing.SourceOverrideCreatedAtUnix

	m.CronSchedules[rec.ScheduleID] = rec
	return rec, nil
}

func (m *MockSchedulesRepository) GetCronScheduleByScheduleID(ctx context.Context, scheduleID string) (dal.CronScheduleRecord, error) {
	if m.GetCronScheduleErr != nil {
		return dal.CronScheduleRecord{}, m.GetCronScheduleErr
	}

	if m.CronSchedules != nil {
		if rec, ok := m.CronSchedules[scheduleID]; ok {
			return rec, nil
		}
	}

	return dal.CronScheduleRecord{}, fmt.Errorf("%w: cron schedule %s", dal.ErrNotFound, scheduleID)
}

func (m *MockSchedulesRepository) ListSourceCronSchedules(ctx context.Context, namespaceID int64, repositoryID string) ([]dal.CronScheduleRecord, error) {
	if m.ListSourceCronSchedulesErr != nil {
		return nil, m.ListSourceCronSchedulesErr
	}

	var out []dal.CronScheduleRecord
	for _, rec := range m.CronSchedules {
		if rec.SourceRepositoryID == "" {
			continue
		}
		if repositoryID != "" && rec.SourceRepositoryID != repositoryID {
			continue
		}
		out = append(out, rec)
	}

	return out, nil
}

func (m *MockSchedulesRepository) CountSourceCronSchedules(ctx context.Context, declaredScheduleIDs []string) (dal.SourceCronScheduleCountSummary, error) {
	if m.ListSourceCronSchedulesErr != nil {
		return dal.SourceCronScheduleCountSummary{}, m.ListSourceCronSchedulesErr
	}

	declared := make(map[string]struct{}, len(declaredScheduleIDs))
	for _, id := range declaredScheduleIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			declared[id] = struct{}{}
		}
	}

	counts := dal.SourceCronScheduleCountSummary{}
	for _, rec := range m.CronSchedules {
		if rec.SourceRepositoryID == "" {
			continue
		}

		counts.Total++
		if rec.Enabled {
			counts.Enabled++
		} else {
			counts.Disabled++
		}

		if _, ok := declared[strings.TrimSpace(rec.ScheduleID)]; ok {
			counts.Declared++
		} else if rec.Enabled {
			counts.StaleEnabled++
		} else {
			counts.StaleDisabled++
		}

		if strings.TrimSpace(rec.SourceOverrideRef) != "" || strings.TrimSpace(rec.SourceOverridePath) != "" {
			counts.ActiveOverrides++
		}
	}

	return counts, nil
}

func (m *MockSchedulesRepository) SetSourceCronScheduleOverride(ctx context.Context, scheduleID string, override dal.SourceScheduleOverride) (dal.CronScheduleRecord, error) {
	if m.SetSourceCronScheduleOverrideErr != nil {
		return dal.CronScheduleRecord{}, m.SetSourceCronScheduleOverrideErr
	}

	rec, err := m.GetCronScheduleByScheduleID(ctx, scheduleID)
	if err != nil {
		return dal.CronScheduleRecord{}, err
	}

	if rec.SourceRepositoryID == "" {
		return dal.CronScheduleRecord{}, fmt.Errorf("%w: source cron schedule %s", dal.ErrNotFound, scheduleID)
	}

	rec.SourceOverrideRef = strings.TrimSpace(override.Ref)
	rec.SourceOverridePath = strings.TrimSpace(override.Path)
	rec.SourceOverrideReason = strings.TrimSpace(override.Reason)
	rec.SourceOverrideCreatedAtUnix = override.CreatedAtUnix
	if rec.SourceOverrideCreatedAtUnix == 0 {
		rec.SourceOverrideCreatedAtUnix = time.Now().UTC().Unix()
	}

	m.CronSchedules[scheduleID] = rec
	return rec, nil
}

func (m *MockSchedulesRepository) ClearSourceCronScheduleOverride(ctx context.Context, scheduleID string) (dal.CronScheduleRecord, error) {
	if m.ClearSourceCronScheduleOverrideErr != nil {
		return dal.CronScheduleRecord{}, m.ClearSourceCronScheduleOverrideErr
	}

	rec, err := m.GetCronScheduleByScheduleID(ctx, scheduleID)
	if err != nil {
		return dal.CronScheduleRecord{}, err
	}

	if rec.SourceRepositoryID == "" {
		return dal.CronScheduleRecord{}, fmt.Errorf("%w: source cron schedule %s", dal.ErrNotFound, scheduleID)
	}

	rec.SourceOverrideRef = ""
	rec.SourceOverridePath = ""
	rec.SourceOverrideReason = ""
	rec.SourceOverrideCreatedAtUnix = 0
	m.CronSchedules[scheduleID] = rec

	return rec, nil
}

func (m *MockSchedulesRepository) DeleteSourceCronSchedule(ctx context.Context, scheduleID string) error {
	if m.DeleteSourceCronScheduleErr != nil {
		return m.DeleteSourceCronScheduleErr
	}

	rec, err := m.GetCronScheduleByScheduleID(ctx, scheduleID)
	if err != nil {
		return err
	}

	if rec.SourceRepositoryID == "" {
		return fmt.Errorf("%w: source cron schedule %s", dal.ErrNotFound, scheduleID)
	}

	delete(m.CronSchedules, scheduleID)
	return nil
}

func (m *MockSchedulesRepository) GetReady(ctx context.Context, at time.Time) ([]dal.CronSchedule, error) {
	m.GetReadyCalled++
	if m.GetReadyErr != nil {
		return nil, m.GetReadyErr
	}
	return append([]dal.CronSchedule(nil), m.Ready...), nil
}

func (m *MockSchedulesRepository) ClaimDue(ctx context.Context, scheduleID int64, observedNextRun time.Time, claimToken string, claimedUntil, now time.Time) (bool, error) {
	if m.ClaimDueErr != nil {
		return false, m.ClaimDueErr
	}

	m.ClaimDueCalls = append(m.ClaimDueCalls, ClaimDueCall{
		ID:              scheduleID,
		ObservedNextRun: observedNextRun,
		ClaimToken:      claimToken,
		ClaimedUntil:    claimedUntil,
		Now:             now,
	})

	return m.ClaimDueOK, nil
}

func (m *MockSchedulesRepository) CompleteClaim(ctx context.Context, scheduleID int64, claimToken string, nextRun time.Time) (bool, error) {
	if m.CompleteClaimErr != nil {
		return false, m.CompleteClaimErr
	}

	m.CompleteClaimCalls = append(m.CompleteClaimCalls, CompleteClaimCall{
		ID:         scheduleID,
		ClaimToken: claimToken,
		Next:       nextRun,
	})

	return m.CompleteClaimOK, nil
}

func (m *MockSchedulesRepository) CountCronSchedules(ctx context.Context) (int64, error) {
	return m.CountCronSchedulesResult, m.CountCronSchedulesErr
}

func (m *MockSchedulesRepository) CronScheduleSummary(ctx context.Context, at time.Time) (dal.CronScheduleSummary, error) {
	return m.CronScheduleSummaryResult, m.CronScheduleSummaryErr
}

func (m *MockSchedulesRepository) ReleaseClaim(ctx context.Context, scheduleID int64, claimToken string) error {
	if m.ReleaseClaimErr != nil {
		return m.ReleaseClaimErr
	}

	m.ReleaseClaimCalls = append(m.ReleaseClaimCalls, ReleaseClaimCall{
		ID:         scheduleID,
		ClaimToken: claimToken,
	})

	return nil
}

var _ dal.SchedulesRepository = (*MockSchedulesRepository)(nil)
