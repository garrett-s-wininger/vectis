package reconciler

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestService_Process_ReenqueuesQueuedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-a", jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runs := repos.Runs()
	runID, _, err := runs.CreateRun(ctx, "job-a", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 enqueued job, got %d", len(jobs))
	}
	if jobs[0].GetId() != "job-a" || jobs[0].GetRunId() != runID {
		t.Errorf("job id/run mismatch: id=%q run=%q", jobs[0].GetId(), jobs[0].GetRunId())
	}

	reqs := q.GetJobRequests()
	envelopeJSON := reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if envelopeJSON == "" {
		t.Fatal("expected execution envelope metadata")
	}

	env, err := cell.DecodeExecutionEnvelope([]byte(envelopeJSON))
	if err != nil {
		t.Fatalf("decode execution envelope: %v", err)
	}

	if env.RunID != runID || env.Job.GetId() != "job-a" {
		t.Fatalf("unexpected envelope identity: run=%q job=%q", env.RunID, env.Job.GetId())
	}

	if env.ExecutionID == "" || env.SegmentID == "" {
		t.Fatalf("expected envelope execution and segment ids, got execution=%q segment=%q", env.ExecutionID, env.SegmentID)
	}

	var last sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&last); err != nil {
		t.Fatalf("scan last_dispatched_at: %v", err)
	}
	if !last.Valid || last.Int64 == 0 {
		t.Errorf("expected last_dispatched_at set, got %v", last)
	}
}

func TestService_Process_ReenqueuesCapturedDefinitionVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	jobID := "job-versioned"
	defV1 := `{"id":"job-versioned","root":{"uses":"builtins/shell","with":{"command":"echo old"}}}`
	defV2 := `{"id":"job-versioned","root":{"uses":"builtins/shell","with":{"command":"echo new"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defV1, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if _, err := repos.Jobs().UpdateDefinition(ctx, jobID, defV2); err != nil {
		t.Fatalf("update job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 enqueued job, got %d", len(jobs))
	}

	if jobs[0].GetRunId() != runID {
		t.Fatalf("run id: want %q, got %q", runID, jobs[0].GetRunId())
	}

	if got := jobs[0].GetRoot().GetWith()["command"]; got != "echo old" {
		t.Fatalf("expected reenqueue to use definition version 1, command=%q", got)
	}
}

func TestService_Process_ReenqueuesEphemeralWithJobDefinition(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "ephemeral-uuid"
	jobDef := `{"id":"ephemeral-uuid","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.CreateDefinitionAndRun(ctx, jobID, jobDef, nil)
	if err != nil {
		t.Fatalf("CreateDefinitionAndRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err = svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 re-enqueued job, got %d", len(jobs))
	}

	if jobs[0].GetId() != jobID || jobs[0].GetRunId() != runID {
		t.Fatalf("unexpected payload: id=%q run=%q", jobs[0].GetId(), jobs[0].GetRunId())
	}
}

func TestService_Process_SkipsRecentDispatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-b","root":{"uses":"builtins/shell"}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-b", jobDef, 1); err != nil {
		t.Fatalf("insert: %v", err)
	}

	runs := repos.Runs()
	runID, _, err := runs.CreateRun(ctx, "job-b", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := runs.TouchDispatched(ctx, runID); err != nil {
		t.Fatalf("TouchDispatched: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(q.GetJobs()) != 0 {
		t.Errorf("expected no re-enqueue within min gap, got %d jobs", len(q.GetJobs()))
	}
}

func TestService_Process_MarksExpiredRunningLeaseAsOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.Runs().CreateRun(ctx, "job-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed running lease: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "orphaned" {
		t.Fatalf("expected status orphaned, got %q", status)
	}

	if got := len(q.GetJobs()); got != 0 {
		t.Fatalf("expected no queued redispatch from orphan sweep, got %d jobs", got)
	}
}

func TestService_Process_DBUnavailable_SkipsUntilRecovered(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-db-down","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-db-down", jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	if _, _, err := repos.Runs().CreateRun(ctx, "job-db-down", nil, 1); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := mocks.NewMockLogger()
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process with db down should not error, got: %v", err)
	}

	if got := len(q.GetJobs()); got != 0 {
		t.Fatalf("expected no enqueue while db down, got %d", got)
	}

	warns := strings.Join(logger.GetWarnCalls(), "\n")
	if !strings.Contains(warns, "database unavailable; skipping processing until recovery") {
		t.Fatalf("expected db-down warning log, got %v", logger.GetWarnCalls())
	}
}
