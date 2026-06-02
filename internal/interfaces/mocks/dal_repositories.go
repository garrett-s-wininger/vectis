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
	DeleteErr error
	ListErr   error
	GetErr    error
	UpdateErr error
}

func NewMockJobsRepository() *MockJobsRepository {
	return &MockJobsRepository{
		Definitions:        map[string]string{},
		DefinitionVersions: map[string]int{},
		Versions:           map[string]map[int]string{},
	}
}

func (m *MockJobsRepository) Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error {
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

func (m *MockJobsRepository) Delete(ctx context.Context, jobID string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}

	delete(m.Definitions, jobID)
	delete(m.DefinitionVersions, jobID)
	return nil
}

func (m *MockJobsRepository) List(ctx context.Context, cursor int64, limit int) ([]dal.JobRecord, int64, error) {
	if m.ListErr != nil {
		return nil, 0, m.ListErr
	}

	out := make([]dal.JobRecord, 0, len(m.Definitions))
	for id, def := range m.Definitions {
		out = append(out, dal.JobRecord{
			JobID:          id,
			DefinitionJSON: def,
		})
	}

	return out, 0, nil
}

func (m *MockJobsRepository) GetDefinition(ctx context.Context, jobID string) (string, int, error) {
	if m.GetErr != nil {
		return "", 0, m.GetErr
	}

	def, ok := m.Definitions[jobID]
	if !ok {
		return "", 0, fmt.Errorf("%w: job %s", dal.ErrNotFound, jobID)
	}

	version := m.DefinitionVersions[jobID]
	if version <= 0 {
		version = 1
	}

	return def, version, nil
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

func (m *MockJobsRepository) UpdateDefinition(ctx context.Context, jobID, definitionJSON string) (int, error) {
	if m.UpdateErr != nil {
		return 0, m.UpdateErr
	}

	m.Definitions[jobID] = definitionJSON
	newVersion := m.DefinitionVersions[jobID] + 1
	if newVersion <= 1 {
		newVersion = 2
	}

	m.DefinitionVersions[jobID] = newVersion
	if m.Versions[jobID] == nil {
		m.Versions[jobID] = map[int]string{}
	}

	m.Versions[jobID][newVersion] = definitionJSON
	return newVersion, nil
}

func (m *MockJobsRepository) ListByNamespace(ctx context.Context, namespaceID int64) ([]dal.JobRecord, error) {
	if m.ListErr != nil {
		return nil, m.ListErr
	}

	out := make([]dal.JobRecord, 0, len(m.Definitions))
	for id, def := range m.Definitions {
		out = append(out, dal.JobRecord{
			JobID:          id,
			NamespaceID:    namespaceID,
			DefinitionJSON: def,
		})
	}

	return out, nil
}

func (m *MockJobsRepository) GetNamespaceID(ctx context.Context, jobID string) (int64, error) {
	if m.GetErr != nil {
		return 0, m.GetErr
	}

	if _, ok := m.Definitions[jobID]; !ok {
		return 0, fmt.Errorf("%w: job %s", dal.ErrNotFound, jobID)
	}

	return 1, nil
}

var _ dal.JobsRepository = (*MockJobsRepository)(nil)

type MockRunsRepository struct {
	mu sync.Mutex

	CreateRunID      string
	CreateRunIndex   int
	CreateRunCreated bool

	CreateRunErr           error
	TouchDispatchedErr     error
	ListByJobErr           error
	ListRunTasksErr        error
	EnsureTaskExecutionErr error
	QueuedListErr          error
	TryClaimErr            error
	RenewLeaseErr          error
	RequestCancelErr       error
	CancelRequestedErr     error
	MarkRunRunningErr      error
	MarkRunSuccessErr      error
	MarkRunFailedErr       error
	MarkRunCancelledErr    error
	MarkRunAbortedErr      error
	MarkRunOrphanedErr     error
	RepairMarkErr          error
	RequeueRunErr          error
	MarkOrphanedErr        error
	GetRunStatusErr        error
	CountByStatusErr       error
	CountByStatusByCellErr error
	CountStuckErr          error
	CountStuckByCellErr    error
	PendingExecutionErr    error
	MarkExecutionErr       error
	LogShardErr            error

	CountByStatusResult       int64
	CountByStatusByCellResult []dal.RunCountByCell
	CountStuckResult          int64
	CountStuckByCell          []dal.RunCountByCell

	TryClaimResult  bool
	ClaimToken      string
	RunStatus       string
	RunStatusFound  bool
	CancelRequested bool
	OrphanedRunIDs  []string
	LogShardID      string
	LogShardSet     bool

	ListByJobResults []dal.RunRecord
	TaskRecords      []dal.TaskRecord
	TaskExecution    dal.TaskExecutionRecord
	TaskCreated      bool
	RunRecords       map[string]dal.RunRecord
	QueuedRuns       []dal.QueuedRun
	PendingExecution dal.ExecutionDispatchRecord

	TouchedRunIDs        []string
	ExecutionTransitions []string

	LastCreateJobID       string
	LastDefinitionVersion int
	LastRunAudit          dal.RunAuditMetadata
	LastCreateTargetCell  string
	LastCreateTargetCells []string
	RecordedPayloads      map[string]string
	ExecutionPayloads     map[string]dal.ExecutionPayloadRecord
	LastScheduleID        int64
	LastScheduledFor      time.Time
	LastListJobID         string
	LastListAfterIndex    *int
	LastListSince         *time.Time
	LastListOwningCell    string
	LastListRunTasksRunID string
	LastTaskExecution     dal.TaskExecutionCreate
	LastRunStatusUpdate   dal.RunStatusUpdate
	LastExecStatusUpdate  dal.ExecutionStatusUpdate
}

func NewMockRunsRepository() *MockRunsRepository {
	return &MockRunsRepository{
		CreateRunID:      "mock-run-id",
		CreateRunIndex:   1,
		CreateRunCreated: true,
		TryClaimResult:   false,
		ClaimToken:       "mock-claim-token",
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
		return m.MarkRunSucceeded(ctx, update.RunID, update.ClaimToken)
	case dal.RunStatusFailed:
		return m.MarkRunFailed(ctx, update.RunID, update.ClaimToken, update.FailureCode, update.Reason)
	case dal.RunStatusCancelled:
		return m.MarkRunCancelled(ctx, update.RunID, update.ClaimToken, update.Reason)
	case dal.RunStatusAborted:
		return m.MarkRunAborted(ctx, update.RunID, update.ClaimToken, update.Reason)
	case dal.RunStatusOrphaned:
		return m.MarkRunOrphaned(ctx, update.RunID, update.ClaimToken, update.Reason)
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
		return m.MarkExecutionAccepted(ctx, update.ExecutionID)
	case dal.ExecutionStatusRunning:
		return m.MarkExecutionStarted(ctx, update.ExecutionID)
	case dal.ExecutionStatusSucceeded, dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		return m.MarkExecutionTerminal(ctx, update.ExecutionID, update.Status)
	default:
		return fmt.Errorf("%w: unsupported execution status %s", dal.ErrConflict, update.Status)
	}
}

func (m *MockRunsRepository) MarkRunSucceeded(ctx context.Context, runID, claimToken string) error {
	return m.MarkRunSuccessErr
}

func (m *MockRunsRepository) MarkRunFailed(ctx context.Context, runID, claimToken, failureCode, reason string) error {
	return m.MarkRunFailedErr
}

func (m *MockRunsRepository) MarkRunCancelled(ctx context.Context, runID, claimToken, reason string) error {
	return m.MarkRunCancelledErr
}

func (m *MockRunsRepository) MarkRunAborted(ctx context.Context, runID, claimToken, reason string) error {
	return m.MarkRunAbortedErr
}

func (m *MockRunsRepository) MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error {
	return m.MarkRunOrphanedErr
}

func (m *MockRunsRepository) RepairMarkRunSucceeded(ctx context.Context, runID, reason string) error {
	return m.RepairMarkErr
}

func (m *MockRunsRepository) RepairMarkRunFailed(ctx context.Context, runID, reason string) error {
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

func (m *MockRunsRepository) GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error) {
	if m.GetRunStatusErr != nil {
		return "", false, m.GetRunStatusErr
	}
	return m.RunStatus, m.RunStatusFound, nil
}

func (m *MockRunsRepository) TryClaim(ctx context.Context, runID, owner string, leaseUntil time.Time) (bool, string, error) {
	if m.TryClaimErr != nil {
		return false, "", m.TryClaimErr
	}
	return m.TryClaimResult, m.ClaimToken, nil
}

func (m *MockRunsRepository) RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error {
	return m.RenewLeaseErr
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

func (m *MockRunsRepository) RunCancelRequested(ctx context.Context, runID, claimToken string) (bool, error) {
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

		created = append(created, dal.CreatedRun{
			RunID:        runID,
			RunIndex:     runIndexStart + i,
			TargetCellID: targetCellID,
		})
	}

	return created, nil
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

func (m *MockRunsRepository) CreateScheduledRun(ctx context.Context, scheduleID int64, scheduledFor time.Time, jobID string, definitionVersion int, audit dal.RunAuditMetadata) (string, int, bool, error) {
	if m.CreateRunErr != nil {
		return "", 0, false, m.CreateRunErr
	}

	m.mu.Lock()
	m.LastCreateJobID = jobID
	m.LastDefinitionVersion = definitionVersion
	m.LastRunAudit = audit
	m.LastScheduleID = scheduleID
	m.LastScheduledFor = scheduledFor
	m.mu.Unlock()
	return m.CreateRunID, m.CreateRunIndex, m.CreateRunCreated, nil
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
	if m.QueuedListErr != nil {
		return nil, m.QueuedListErr
	}
	return append([]dal.QueuedRun(nil), m.QueuedRuns...), nil
}

func (m *MockRunsRepository) GetPendingExecution(ctx context.Context, runID string) (dal.ExecutionDispatchRecord, error) {
	if m.PendingExecutionErr != nil {
		return dal.ExecutionDispatchRecord{}, m.PendingExecutionErr
	}

	rec := m.PendingExecution
	if rec.RunID == "" {
		rec.RunID = runID
	}

	return rec, nil
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

func (m *MockRunsRepository) MarkExecutionAccepted(ctx context.Context, executionID string) error {
	if m.MarkExecutionErr != nil {
		return m.MarkExecutionErr
	}

	m.mu.Lock()
	m.ExecutionTransitions = append(m.ExecutionTransitions, executionID+":"+dal.ExecutionStatusAccepted)
	m.mu.Unlock()
	return nil
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

func (m *MockRunsRepository) MarkExecutionTerminal(ctx context.Context, executionID, status string) error {
	if m.MarkExecutionErr != nil {
		return m.MarkExecutionErr
	}

	m.mu.Lock()
	m.ExecutionTransitions = append(m.ExecutionTransitions, executionID+":"+status)
	m.mu.Unlock()
	return nil
}

func (m *MockRunsRepository) GetRunJobID(ctx context.Context, runID string) (string, error) {
	return m.LastCreateJobID, nil
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
	Ready []dal.CronSchedule

	GetReadyErr              error
	ClaimDueErr              error
	ClaimDueOK               bool
	CompleteClaimErr         error
	CompleteClaimOK          bool
	ReleaseClaimErr          error
	CountCronSchedulesErr    error
	CountCronSchedulesResult int64

	GetReadyCalled     int
	ClaimDueCalls      []ClaimDueCall
	CompleteClaimCalls []CompleteClaimCall
	ReleaseClaimCalls  []ReleaseClaimCall
}

func NewMockSchedulesRepository() *MockSchedulesRepository {
	return &MockSchedulesRepository{ClaimDueOK: true, CompleteClaimOK: true}
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
