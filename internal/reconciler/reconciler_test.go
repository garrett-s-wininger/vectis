package reconciler

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
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

	runs := dal.NewSQLRepositories(db).Runs()
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

	var last sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&last); err != nil {
		t.Fatalf("scan last_dispatched_at: %v", err)
	}
	if !last.Valid || last.Int64 == 0 {
		t.Errorf("expected last_dispatched_at set, got %v", last)
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
	_, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)`, "job-b", jobDef)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	runs := dal.NewSQLRepositories(db).Runs()
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
