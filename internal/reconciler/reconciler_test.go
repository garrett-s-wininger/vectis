package reconciler

import (
	"context"
	"database/sql"
	"strings"
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
	if _, err := db.ExecContext(ctx, `INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)`, "job-db-down", jobDef); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	repos := dal.NewSQLRepositories(db)
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
