package reconciler

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/runstore"
	"vectis/internal/testutil/dbtest"
)

func TestService_Process_ReenqueuesQueuedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	_, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)`, "job-a", jobDef)
	if err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := runstore.CreateRun(ctx, db, "job-a", nil)
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

	var last sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&last); err != nil {
		t.Fatalf("scan last_dispatched_at: %v", err)
	}
	if !last.Valid || last.Int64 == 0 {
		t.Errorf("expected last_dispatched_at set, got %v", last)
	}
}

func TestService_Process_SkipsEphemeralWithoutStoredJob(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	// Ephemeral-style run: job_id is not in stored_jobs.
	_, _, err := runstore.CreateRun(ctx, db, "not-in-stored-jobs", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err = svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(q.GetJobs()) != 0 {
		t.Fatalf("ephemeral run should not be re-enqueued, got %d jobs", len(q.GetJobs()))
	}
}

func TestService_Process_SkipsRecentDispatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-b","root":{"uses":"builtins/shell"}}`
	_, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)`, "job-b", jobDef)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	runID, _, err := runstore.CreateRun(ctx, db, "job-b", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := runstore.TouchDispatched(ctx, db, runID); err != nil {
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
