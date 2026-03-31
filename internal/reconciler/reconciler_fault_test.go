package reconciler

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

type faultInjectQueue struct {
	mu         sync.Mutex
	jobs       []*api.Job
	enqueueErr error
	down       bool
}

func newFaultInjectQueue() *faultInjectQueue {
	return &faultInjectQueue{jobs: make([]*api.Job, 0)}
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

	q.jobs = make([]*api.Job, 0)
	q.down = false
	q.enqueueErr = nil
}

func (q *faultInjectQueue) Enqueue(ctx context.Context, job *api.Job) (*api.Empty, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.down {
		if q.enqueueErr != nil {
			return nil, q.enqueueErr
		}

		return nil, errors.New("queue unavailable")
	}

	q.jobs = append(q.jobs, job)
	return &api.Empty{}, nil
}

func (q *faultInjectQueue) Jobs() []*api.Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*api.Job, len(q.jobs))
	copy(out, q.jobs)
	return out
}

func seedStoredJobAndRun(t *testing.T, db *sql.DB, jobID string) string {
	t.Helper()

	ctx := context.Background()
	jobDef := `{"id":"` + jobID + `","root":{"uses":"builtins/shell","with":{"command":"echo ok"}}}`
	_, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)`, jobID, jobDef)
	if err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := dal.NewSQLRepositories(db).Runs().CreateRun(ctx, jobID, nil, 1)
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

	ok1, _, err := store.TryClaim(ctx, runID, "worker-a", leaseUntil)
	if err != nil {
		t.Fatalf("TryClaim worker-a: %v", err)
	}

	if !ok1 {
		t.Fatal("expected worker-a to claim run")
	}

	ok2, _, err := store.TryClaim(ctx, runID, "worker-b", leaseUntil)
	if err != nil {
		t.Fatalf("TryClaim worker-b: %v", err)
	}

	if ok2 {
		t.Fatal("expected worker-b claim to fail after run already claimed")
	}
}
