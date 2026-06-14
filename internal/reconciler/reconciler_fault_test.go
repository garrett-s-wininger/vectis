package reconciler

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

type faultInjectQueue struct {
	mu         sync.Mutex
	jobs       []*api.JobRequest
	enqueueErr error
	down       bool
}

func newFaultInjectQueue() *faultInjectQueue {
	return &faultInjectQueue{jobs: make([]*api.JobRequest, 0)}
}

func (q *faultInjectQueue) SetDown(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.down = true
	q.enqueueErr = err
}

func (q *faultInjectQueue) SetUp() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.down = false
	q.enqueueErr = nil
}

func (q *faultInjectQueue) Restart() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.jobs = make([]*api.JobRequest, 0)
	q.down = false
	q.enqueueErr = nil
}

func (q *faultInjectQueue) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.down {
		if q.enqueueErr != nil {
			return nil, q.enqueueErr
		}

		return nil, errors.New("queue unavailable")
	}

	q.jobs = append(q.jobs, req)
	return &api.Empty{}, nil
}

func (q *faultInjectQueue) Jobs() []*api.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*api.Job, 0, len(q.jobs))
	for _, req := range q.jobs {
		out = append(out, req.GetJob())
	}
	return out
}

func seedStoredJobAndRun(t *testing.T, db *sql.DB, jobID string) string {
	t.Helper()

	ctx := context.Background()
	jobDef := `{"id":"` + jobID + `","root":{"uses":"builtins/shell","with":{"command":"echo ok"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	return runID
}

func fetchRunState(t *testing.T, ctx context.Context, db *sql.DB, runID string) (string, sql.NullInt64) {
	t.Helper()

	var status string
	var last sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT status, last_dispatched_at FROM job_runs WHERE run_id = ?`, runID,
	).Scan(&status, &last); err != nil {
		t.Fatalf("query run state: %v", err)
	}

	return status, last
}

func TestService_Process_QueueDown_DoesNotTouchDispatched(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	runID := seedStoredJobAndRun(t, db, "job-fault-down")

	q := newFaultInjectQueue()
	q.SetDown(errors.New("queue unavailable"))

	clock := mocks.NewMockClock()
	clock.SetNow(time.Now().UTC())

	svc := NewService(interfaces.NewLogger("test"), db, q, clock)
	svc.SetMinDispatchGap(1 * time.Second)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if got := len(q.Jobs()); got != 0 {
		t.Fatalf("expected 0 successful enqueues, got %d", got)
	}

	status, last := fetchRunState(t, ctx, db, runID)
	if status != "queued" {
		t.Fatalf("expected status queued, got %q", status)
	}

	if last.Valid {
		t.Fatalf("expected last_dispatched_at to remain NULL, got %v", last)
	}
}

func TestService_Process_AfterQueueRecovery_ReenqueuesAndTouchesDispatched(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	runID := seedStoredJobAndRun(t, db, "job-fault-recover")

	q := newFaultInjectQueue()
	q.SetDown(errors.New("queue unavailable"))

	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	svc := NewService(interfaces.NewLogger("test"), db, q, clock)
	svc.SetMinDispatchGap(1 * time.Second)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process during outage: %v", err)
	}

	_, lastBefore := fetchRunState(t, ctx, db, runID)
	if lastBefore.Valid {
		t.Fatalf("expected last_dispatched_at NULL before recovery, got %v", lastBefore)
	}

	q.Restart()
	clock.SetNow(now.Add(2 * time.Second))

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process after recovery: %v", err)
	}

	jobs := q.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 successful enqueue after recovery, got %d", len(jobs))
	}

	if jobs[0].GetRunId() != runID {
		t.Fatalf("expected run_id %q, got %q", runID, jobs[0].GetRunId())
	}

	status, lastAfter := fetchRunState(t, ctx, db, runID)
	if status != "queued" {
		t.Fatalf("expected status queued, got %q", status)
	}

	if !lastAfter.Valid || lastAfter.Int64 == 0 {
		t.Fatalf("expected last_dispatched_at set after recovery, got %v", lastAfter)
	}
}

func TestService_Process_TouchDispatchedFailureRetriesFrozenPayload(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	jobID := "job-touch-dispatched-fault"
	runID := seedStoredJobAndRun(t, db, jobID)

	q := newFaultInjectQueue()
	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	repos := dal.NewSQLRepositories(db)
	touchErr := errors.New("database unavailable after enqueue")
	runs := &failOnceTouchRunsRepository{
		RunsRepository: repos.Runs(),
		err:            touchErr,
	}

	svc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), runs, q, clock)
	svc.SetServiceLeases(nil)
	svc.SetMinDispatchGap(1 * time.Second)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process with injected touch failure: %v", err)
	}

	jobs := q.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("expected first pass to enqueue once before touch failure, got %d", len(jobs))
	}

	if jobs[0].GetRunId() != runID {
		t.Fatalf("first enqueued run_id = %q, want %q", jobs[0].GetRunId(), runID)
	}

	_, lastBefore := fetchRunState(t, ctx, db, runID)
	if lastBefore.Valid {
		t.Fatalf("failed touch should leave last_dispatched_at unset, got %v", lastBefore)
	}

	firstReqs := q.Requests()
	firstEnvelope := firstReqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if firstEnvelope == "" {
		t.Fatal("expected first dispatch envelope")
	}

	clock.SetNow(now.Add(2 * time.Second))
	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process after touch recovery: %v", err)
	}

	secondReqs := q.Requests()
	if len(secondReqs) != 2 {
		t.Fatalf("expected retry to enqueue a duplicate handoff, got %d", len(secondReqs))
	}

	if secondReqs[1].GetJob().GetRunId() != runID {
		t.Fatalf("retry enqueued run_id = %q, want %q", secondReqs[1].GetJob().GetRunId(), runID)
	}

	secondEnvelope := secondReqs[1].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if secondEnvelope != firstEnvelope {
		t.Fatalf("redispatch envelope changed after touch failure:\nfirst:  %s\nsecond: %s", firstEnvelope, secondEnvelope)
	}

	_, lastAfter := fetchRunState(t, ctx, db, runID)
	if !lastAfter.Valid || lastAfter.Int64 == 0 {
		t.Fatalf("expected retry to touch last_dispatched_at, got %v", lastAfter)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claim1, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("first claim after duplicate handoff: %v", err)
	}

	if !claim1.Claimed || claim1.ClaimToken == "" {
		t.Fatalf("expected first duplicate handoff claim to win, claim=%+v", claim1)
	}

	claim2, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("second claim after duplicate handoff: %v", err)
	}

	if claim2.Claimed || claim2.ClaimToken != "" {
		t.Fatalf("expected second duplicate handoff claim to lose, claim=%+v", claim2)
	}
}

func TestService_Process_MinGapPreventsImmediateRedispatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	_ = seedStoredJobAndRun(t, db, "job-fault-gap")

	q := newFaultInjectQueue()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Now().UTC())

	svc := NewService(interfaces.NewLogger("test"), db, q, clock)
	svc.SetMinDispatchGap(1 * time.Hour)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	if got := len(q.Jobs()); got != 1 {
		t.Fatalf("expected 1 enqueue in first pass, got %d", got)
	}

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("second Process: %v", err)
	}

	if got := len(q.Jobs()); got != 1 {
		t.Fatalf("expected no immediate redispatch within min gap; got %d jobs", got)
	}
}

func (q *faultInjectQueue) Requests() []*api.JobRequest {
	q.mu.Lock()
	defer q.mu.Unlock()

	return append([]*api.JobRequest(nil), q.jobs...)
}

type failOnceTouchRunsRepository struct {
	dal.RunsRepository
	err    error
	failed bool
}

func (r *failOnceTouchRunsRepository) TouchDispatched(ctx context.Context, runID string) error {
	if !r.failed {
		r.failed = true
		return r.err
	}

	return r.RunsRepository.TouchDispatched(ctx, runID)
}

func TestService_Process_DuplicateDelivery_AllowsSingleClaimedExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	runID := seedStoredJobAndRun(t, db, "job-fault-dup")

	q := newFaultInjectQueue()
	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	svc := NewService(interfaces.NewLogger("test"), db, q, clock)
	svc.SetMinDispatchGap(2 * time.Second)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	if got := len(q.Jobs()); got != 1 {
		t.Fatalf("expected 1 enqueue in first pass, got %d", got)
	}

	clock.SetNow(now.Add(3 * time.Second))
	if err := svc.Process(ctx); err != nil {
		t.Fatalf("second Process: %v", err)
	}

	if got := len(q.Jobs()); got != 2 {
		t.Fatalf("expected duplicate enqueue after min-gap expiry, got %d jobs", got)
	}

	store := dal.NewSQLRepositories(db).Runs()
	leaseUntil := time.Now().Add(time.Minute)

	dispatch, err := store.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claim1, err := store.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", leaseUntil)
	if err != nil {
		t.Fatalf("TryClaimExecution worker-a: %v", err)
	}

	if !claim1.Claimed {
		t.Fatal("expected worker-a to claim execution")
	}

	claim2, err := store.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", leaseUntil)
	if err != nil {
		t.Fatalf("TryClaimExecution worker-b: %v", err)
	}

	if claim2.Claimed {
		t.Fatal("expected worker-b execution claim to fail after execution already claimed")
	}
}
