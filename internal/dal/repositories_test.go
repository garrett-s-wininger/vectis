package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestJobsRepository_CRUDAndConflict(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	ctx := context.Background()

	jobID := "job-a"
	def1 := `{"id":"job-a","root":{"uses":"builtins/shell"}}`
	def2 := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`

	if err := jobs.Create(ctx, jobID, def1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := jobs.Create(ctx, jobID, def1); !dal.IsConflict(err) {
		t.Fatalf("expected conflict on duplicate create, got: %v", err)
	}

	gotDef, err := jobs.GetDefinition(ctx, jobID)
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if gotDef != def1 {
		t.Fatalf("definition mismatch: got %q want %q", gotDef, def1)
	}

	if err := jobs.UpdateDefinition(ctx, jobID, def2); err != nil {
		t.Fatalf("update definition: %v", err)
	}

	gotDef, err = jobs.GetDefinition(ctx, jobID)
	if err != nil {
		t.Fatalf("get definition after update: %v", err)
	}

	if gotDef != def2 {
		t.Fatalf("updated definition mismatch: got %q want %q", gotDef, def2)
	}

	list, err := jobs.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("expected 1 job in list, got %d", len(list))
	}

	if err := jobs.Delete(ctx, jobID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, err = jobs.GetDefinition(ctx, jobID)
	if !dal.IsNotFound(err) {
		t.Fatalf("expected not found after delete, got: %v", err)
	}
}

func TestRunsRepository_CreateRunAndListSinceOrdered(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	_, idx1, err := runs.CreateRun(ctx, "job-order", nil, 1)
	if err != nil {
		t.Fatalf("create run 1: %v", err)
	}
	_, idx2, err := runs.CreateRun(ctx, "job-order", nil, 1)
	if err != nil {
		t.Fatalf("create run 2: %v", err)
	}

	if idx1 != 1 || idx2 != 2 {
		t.Fatalf("unexpected run indexes: idx1=%d idx2=%d", idx1, idx2)
	}

	all, err := runs.ListByJob(ctx, "job-order", nil)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}

	if len(all) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(all))
	}

	if all[0].RunIndex != 1 || all[1].RunIndex != 2 {
		t.Fatalf("runs not ordered asc by run_index: %+v", all)
	}

	since := 1
	after, err := runs.ListByJob(ctx, "job-order", &since)
	if err != nil {
		t.Fatalf("list since: %v", err)
	}

	if len(after) != 1 || after[0].RunIndex != 2 {
		t.Fatalf("expected only run_index 2 after since=1, got %+v", after)
	}
}

func TestRunsRepository_ClaimRenewAndDispatchQueries(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-claim", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	queued, err := runs.ListQueuedBeforeDispatchCutoff(ctx, time.Now().Unix()+60)
	if err != nil {
		t.Fatalf("list queued before cutoff: %v", err)
	}

	if len(queued) != 1 || queued[0].RunID != runID {
		t.Fatalf("expected queued run %s, got %+v", runID, queued)
	}

	if queued[0].DefinitionVersion != 1 {
		t.Fatalf("expected definition_version 1, got %d", queued[0].DefinitionVersion)
	}

	claimed, err := runs.TryClaim(ctx, runID, "worker-1", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim first: %v", err)
	}

	if !claimed {
		t.Fatal("expected first claim to succeed")
	}

	claimed, err = runs.TryClaim(ctx, runID, "worker-2", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim second: %v", err)
	}

	if claimed {
		t.Fatal("expected second claim to fail")
	}

	if err := runs.RenewLease(ctx, runID, "worker-1", time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("renew lease for owner: %v", err)
	}

	if err := runs.RenewLease(ctx, runID, "worker-2", time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected renew lease by non-owner to fail")
	}

	if err := runs.TouchDispatched(ctx, runID); err != nil {
		t.Fatalf("touch dispatched: %v", err)
	}

	queued, err = runs.ListQueuedBeforeDispatchCutoff(ctx, time.Now().Unix()+60)
	if err != nil {
		t.Fatalf("list queued after claim/touch: %v", err)
	}

	if len(queued) != 0 {
		t.Fatalf("expected no queued rows after claim, got %+v", queued)
	}
}

func TestRunsRepository_CreateRunWithExplicitRunIndex(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-explicit", nil, 1)
	if err != nil {
		t.Fatalf("create initial run: %v", err)
	}

	idx := 10
	runID2, outIdx, err := runs.CreateRun(ctx, "job-explicit", &idx, 1)
	if err != nil {
		t.Fatalf("create explicit run_index: %v", err)
	}

	if outIdx != idx {
		t.Fatalf("expected run_index %d, got %d", idx, outIdx)
	}

	all, err := runs.ListByJob(ctx, "job-explicit", nil)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	if len(all) != 2 {
		t.Fatalf("expected 2 runs, got %+v", all)
	}

	if all[0].RunID != runID || all[1].RunID != runID2 {
		t.Fatalf("unexpected run ids in list: %+v", all)
	}
}

func TestSQLRepositories_CreateDefinitionAndRun_AndGetDefinitionVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	jobID := "ephemeral-job-id"
	def := `{"id":"ephemeral-job-id","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	idx := 1

	runID, outIdx, err := repos.CreateDefinitionAndRun(ctx, jobID, def, &idx)
	if err != nil {
		t.Fatalf("CreateDefinitionAndRun: %v", err)
	}

	if outIdx != idx {
		t.Fatalf("run index: want %d, got %d", idx, outIdx)
	}

	got, err := repos.Jobs().GetDefinitionVersion(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("GetDefinitionVersion: %v", err)
	}

	if got != def {
		t.Fatalf("definition mismatch: got %q", got)
	}

	var dv int
	if err := db.QueryRowContext(ctx, "SELECT definition_version FROM job_runs WHERE run_id = ?", runID).Scan(&dv); err != nil {
		t.Fatalf("scan definition_version: %v", err)
	}

	if dv != 1 {
		t.Fatalf("job_runs.definition_version: want 1, got %d", dv)
	}
}

func TestJobsRepository_GetDefinitionVersion_NotFound(t *testing.T) {
	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	ctx := context.Background()

	_, err := jobs.GetDefinitionVersion(ctx, "missing", 1)
	if err == nil || !dal.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSchedulesRepository_GetReadyAndUpdateNextRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	schedules := repos.Schedules()
	ctx := context.Background()

	if err := jobs.Create(ctx, "cron-job", `{"id":"cron-job"}`); err != nil {
		t.Fatalf("create stored job: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	past := now.Add(-1 * time.Minute)
	future := now.Add(5 * time.Minute)

	if _, err := db.ExecContext(ctx,
		"INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "* * * * *", past.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert past schedule: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		"INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "0 * * * *", future.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert future schedule: %v", err)
	}

	ready, err := schedules.GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules: %v", err)
	}

	if len(ready) != 1 || ready[0].CronSpec != "* * * * *" {
		t.Fatalf("expected one ready schedule for '* * * * *', got %+v", ready)
	}

	updatedNext := now.Add(10 * time.Minute)
	if err := schedules.UpdateNextRun(ctx, ready[0].ID, updatedNext); err != nil {
		t.Fatalf("update next run: %v", err)
	}

	readyAfter, err := schedules.GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules after update: %v", err)
	}

	if len(readyAfter) != 0 {
		t.Fatalf("expected no ready schedules after update, got %+v", readyAfter)
	}
}
