//go:build integration

package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/migrations"
	"vectis/internal/reconciler"
	"vectis/internal/testutil/pgtest"
)

func TestPostgres_MigrationsUpDownRoundTrip(t *testing.T) {
	db := pgtest.NewUnmigratedTestDB(t)
	ctx := context.Background()

	if err := migrations.Run(db, "pgx"); err != nil {
		t.Fatalf("run up migrations: %v", err)
	}

	assertPostgresTableExists(t, ctx, db, "job_runs")
	assertPostgresTableExists(t, ctx, db, "run_dispatch_events")

	if err := migrations.Down(db, "pgx"); err != nil {
		t.Fatalf("run down migrations: %v", err)
	}

	assertPostgresTableMissing(t, ctx, db, "job_runs")
	assertPostgresTableMissing(t, ctx, db, "run_dispatch_events")
}

func TestPostgres_DALDispatchEventsSmoke(t *testing.T) {
	db := pgtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	runID, _, err := repos.Runs().CreateRun(ctx, "pg-dispatch-job", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	msg := "queue unavailable"
	if err := repos.DispatchEvents().Record(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil); err != nil {
		t.Fatalf("record attempt: %v", err)
	}

	if err := repos.DispatchEvents().Record(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg); err != nil {
		t.Fatalf("record failure: %v", err)
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 dispatch events, got %+v", events)
	}

	if events[0].EventType != dal.DispatchEventAttempt {
		t.Fatalf("first event type: got %q", events[0].EventType)
	}

	if events[1].EventType != dal.DispatchEventFailure || events[1].Message == nil || *events[1].Message != msg {
		t.Fatalf("second event mismatch: %+v", events[1])
	}
}

func assertPostgresTableExists(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRowContext(ctx,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`,
		table,
	).Scan(&name)

	if err != nil {
		t.Fatalf("expected table %s to exist: %v", table, err)
	}
}

func assertPostgresTableMissing(t *testing.T, ctx context.Context, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRowContext(ctx,
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`,
		table,
	).Scan(&name)

	if err == nil {
		t.Fatalf("expected table %s to be dropped, found %s", table, name)
	}

	if err != sql.ErrNoRows {
		t.Fatalf("query table %s: %v", table, err)
	}
}

func TestPostgres_CronClaimAndDispatchSmoke(t *testing.T) {
	db := pgtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	logger := mocks.NewMockLogger()
	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	service := cron.NewCronService(logger, db)
	service.SetQueueClient(queue)
	service.SetClock(clock)

	ctx := context.Background()
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	clock.SetNow(now)

	jobID := "pg-cron-job"
	definition := `{"id":"pg-cron-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo pg"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definition); err != nil {
		t.Fatalf("create job: %v", err)
	}

	var triggerID int64
	if err := db.QueryRowContext(ctx,
		`INSERT INTO job_triggers (job_id, trigger_type) VALUES ($1, $2) RETURNING id`,
		jobID,
		"cron",
	).Scan(&triggerID); err != nil {
		t.Fatalf("insert trigger: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES ($1, $2, $3)`,
		triggerID,
		"* * * * *",
		now.Add(-time.Minute).Format(time.RFC3339),
	); err != nil {
		t.Fatalf("insert schedule: %v", err)
	}

	if err := service.ProcessSchedules(ctx); err != nil {
		t.Fatalf("process schedules: %v", err)
	}

	jobs := queue.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(jobs))
	}

	runID := jobs[0].GetRunId()
	if runID == "" {
		t.Fatal("expected run_id on cron-enqueued job")
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 || events[0].Source != dal.DispatchSourceCron || events[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected cron dispatch events: %+v", events)
	}

	ready, err := repos.Schedules().GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules: %v", err)
	}

	if len(ready) != 0 {
		t.Fatalf("expected schedule to be advanced, still ready: %+v", ready)
	}
}

func TestPostgres_ReconcilerRedispatchSmoke(t *testing.T) {
	db := pgtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	queue := mocks.NewMockQueueService()
	logger := mocks.NewMockLogger()
	service := reconciler.NewService(logger, db, queue, interfaces.SystemClock{})
	service.SetMinDispatchGap(time.Second)

	ctx := context.Background()
	jobID := "pg-reconciler-job"
	definition := `{"id":"pg-reconciler-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo pg"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definition); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := service.Process(ctx); err != nil {
		t.Fatalf("process reconciler: %v", err)
	}

	jobs := queue.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 re-enqueued job, got %d", len(jobs))
	}

	got := jobs[0]
	if got.GetId() != jobID || got.GetRunId() != runID {
		t.Fatalf("unexpected re-enqueued job: id=%q run_id=%q", got.GetId(), got.GetRunId())
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 || events[0].Source != dal.DispatchSourceReconciler || events[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected reconciler dispatch events: %+v", events)
	}

	var lastDispatchedAt sql.NullInt64
	if err := db.QueryRowContext(ctx,
		`SELECT last_dispatched_at FROM job_runs WHERE run_id = $1`,
		runID,
	).Scan(&lastDispatchedAt); err != nil {
		t.Fatalf("scan last_dispatched_at: %v", err)
	}

	if !lastDispatchedAt.Valid || lastDispatchedAt.Int64 == 0 {
		t.Fatal("expected reconciler to touch dispatch metadata")
	}
}
