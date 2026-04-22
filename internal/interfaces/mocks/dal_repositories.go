package mocks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vectis/internal/dal"
)

type StubEphemeralRunStarter struct{}

func (StubEphemeralRunStarter) CreateDefinitionAndRun(context.Context, string, string, *int) (string, int, error) {
	return "", 0, fmt.Errorf("stub ephemeral run starter: not configured")
}

type MockJobsRepository struct {
	Definitions map[string]string
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
		Definitions: map[string]string{},
		Versions:    map[string]map[int]string{},
	}
}

func (m *MockJobsRepository) Create(ctx context.Context, jobID, definitionJSON string, namespaceID int64) error {
	if m.CreateErr != nil {
		return m.CreateErr
	}

	m.Definitions[jobID] = definitionJSON
	return nil
}

func (m *MockJobsRepository) Delete(ctx context.Context, jobID string) error {
	if m.DeleteErr != nil {
		return m.DeleteErr
	}

	delete(m.Definitions, jobID)
	return nil
}

func (m *MockJobsRepository) List(ctx context.Context) ([]dal.JobRecord, error) {
	if m.ListErr != nil {
		return nil, m.ListErr
	}

	out := make([]dal.JobRecord, 0, len(m.Definitions))
	for id, def := range m.Definitions {
		out = append(out, dal.JobRecord{
			JobID:          id,
			DefinitionJSON: def,
		})
	}

	return out, nil
}

func (m *MockJobsRepository) GetDefinition(ctx context.Context, jobID string) (string, error) {
	if m.GetErr != nil {
		return "", m.GetErr
	}

	def, ok := m.Definitions[jobID]
	if !ok {
		return "", fmt.Errorf("%w: job %s", dal.ErrNotFound, jobID)
	}
	return def, nil
}

func (m *MockJobsRepository) GetDefinitionVersion(ctx context.Context, jobID string, version int) (string, error) {
	if m.GetErr != nil {
		return "", m.GetErr
	}

	byVer, ok := m.Versions[jobID]
	if !ok {
		return "", fmt.Errorf("%w: job %s version %d", dal.ErrNotFound, jobID, version)
	}

	def, ok := byVer[version]
	if !ok {
		return "", fmt.Errorf("%w: job %s version %d", dal.ErrNotFound, jobID, version)
	}

	return def, nil
}

func (m *MockJobsRepository) UpdateDefinition(ctx context.Context, jobID, definitionJSON string) error {
	if m.UpdateErr != nil {
		return m.UpdateErr
	}
	m.Definitions[jobID] = definitionJSON
	return nil
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

	CreateRunID    string
	CreateRunIndex int

	CreateRunErr       error
	TouchDispatchedErr error
	ListByJobErr       error
	QueuedListErr      error
	TryClaimErr        error
	RenewLeaseErr      error
	MarkRunRunningErr  error
	MarkRunSuccessErr  error
	MarkRunFailedErr   error
	MarkRunOrphanedErr error
	RequeueRunErr      error
	MarkOrphanedErr    error
	GetRunStatusErr    error

	TryClaimResult bool
	ClaimToken     string
	RunStatus      string
	RunStatusFound bool
	OrphanedRunIDs []string

	ListByJobResults []dal.RunRecord
	QueuedRuns       []dal.QueuedRun

	TouchedRunIDs []string

	LastCreateJobID       string
	LastDefinitionVersion int
	LastListJobID         string
	LastListSince         *int
}

func NewMockRunsRepository() *MockRunsRepository {
	return &MockRunsRepository{
		CreateRunID:    "mock-run-id",
		CreateRunIndex: 1,
		TryClaimResult: false,
		ClaimToken:     "mock-claim-token",
	}
}

func (m *MockRunsRepository) MarkRunRunning(ctx context.Context, runID string) error {
	return m.MarkRunRunningErr
}

func (m *MockRunsRepository) MarkRunSucceeded(ctx context.Context, runID, claimToken string) error {
	return m.MarkRunSuccessErr
}

func (m *MockRunsRepository) MarkRunFailed(ctx context.Context, runID, claimToken, failureCode, reason string) error {
	return m.MarkRunFailedErr
}

func (m *MockRunsRepository) MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error {
	return m.MarkRunOrphanedErr
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

func (m *MockRunsRepository) TouchDispatched(ctx context.Context, runID string) error {
	if m.TouchDispatchedErr != nil {
		return m.TouchDispatchedErr
	}

	m.mu.Lock()
	m.TouchedRunIDs = append(m.TouchedRunIDs, runID)
	m.mu.Unlock()
	return nil
}

func (m *MockRunsRepository) CreateRun(ctx context.Context, jobID string, runIndex *int, definitionVersion int) (string, int, error) {
	if m.CreateRunErr != nil {
		return "", 0, m.CreateRunErr
	}

	m.mu.Lock()
	m.LastCreateJobID = jobID
	m.LastDefinitionVersion = definitionVersion
	m.mu.Unlock()
	return m.CreateRunID, m.CreateRunIndex, nil
}

func (m *MockRunsRepository) ListByJob(ctx context.Context, jobID string, since *int) ([]dal.RunRecord, error) {
	if m.ListByJobErr != nil {
		return nil, m.ListByJobErr
	}

	m.mu.Lock()
	m.LastListJobID = jobID
	if since != nil {
		v := *since
		m.LastListSince = &v
	} else {
		m.LastListSince = nil
	}
	m.mu.Unlock()

	return append([]dal.RunRecord(nil), m.ListByJobResults...), nil
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

func (m *MockRunsRepository) SnapshotLastListSince() *int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.LastListSince == nil {
		return nil
	}
	v := *m.LastListSince
	return &v
}

func (m *MockRunsRepository) ListQueuedBeforeDispatchCutoff(ctx context.Context, cutoffUnix int64) ([]dal.QueuedRun, error) {
	if m.QueuedListErr != nil {
		return nil, m.QueuedListErr
	}
	return append([]dal.QueuedRun(nil), m.QueuedRuns...), nil
}

func (m *MockRunsRepository) GetRunJobID(ctx context.Context, runID string) (string, error) {
	return m.LastCreateJobID, nil
}

var _ dal.RunsRepository = (*MockRunsRepository)(nil)

type UpdateNextRunCall struct {
	ID   int64
	Next time.Time
}

type MockSchedulesRepository struct {
	Ready []dal.CronSchedule

	GetReadyErr   error
	UpdateNextErr error

	GetReadyCalled int
	UpdateCalls    []UpdateNextRunCall
}

func NewMockSchedulesRepository() *MockSchedulesRepository {
	return &MockSchedulesRepository{}
}

func (m *MockSchedulesRepository) GetReady(ctx context.Context, at time.Time) ([]dal.CronSchedule, error) {
	m.GetReadyCalled++
	if m.GetReadyErr != nil {
		return nil, m.GetReadyErr
	}
	return append([]dal.CronSchedule(nil), m.Ready...), nil
}

func (m *MockSchedulesRepository) UpdateNextRun(ctx context.Context, scheduleID int64, nextRun time.Time) error {
	if m.UpdateNextErr != nil {
		return m.UpdateNextErr
	}
	m.UpdateCalls = append(m.UpdateCalls, UpdateNextRunCall{
		ID:   scheduleID,
		Next: nextRun,
	})
	return nil
}

var _ dal.SchedulesRepository = (*MockSchedulesRepository)(nil)
