package dal_test

import (
	"context"
	"database/sql"
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

	claimed, claimToken, err := runs.TryClaim(ctx, runID, "worker-1", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim first: %v", err)
	}

	if !claimed {
		t.Fatal("expected first claim to succeed")
	}

	if claimToken == "" {
		t.Fatal("expected non-empty claim token on successful claim")
	}

	claimed, _, err = runs.TryClaim(ctx, runID, "worker-2", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim second: %v", err)
	}

	if claimed {
		t.Fatal("expected second claim to fail")
	}

	if err := runs.RenewLease(ctx, runID, "worker-1", claimToken, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("renew lease for owner: %v", err)
	}

	if err := runs.RenewLease(ctx, runID, "worker-2", claimToken, time.Now().Add(2*time.Minute)); err == nil {
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

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET status = 'orphaned' WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("force orphaned status: %v", err)
	}

	if err := runs.RenewLease(ctx, runID, "worker-1", claimToken, time.Now().Add(3*time.Minute)); err != nil {
		t.Fatalf("renew lease should recover orphaned run: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status running after orphaned renew, got %q", status)
	}
}

func TestRunsRepository_MarkExpiredRunningAsOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()
	ctx := context.Background()

	runA, _, err := runs.CreateRun(ctx, "job-orphan-a", nil, 1)
	if err != nil {
		t.Fatalf("create run A: %v", err)
	}

	runB, _, err := runs.CreateRun(ctx, "job-orphan-b", nil, 1)
	if err != nil {
		t.Fatalf("create run B: %v", err)
	}

	leaseExpired := time.Now().Add(-1 * time.Minute).Unix()
	leaseFuture := time.Now().Add(10 * time.Minute).Unix()

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, leaseExpired, runA); err != nil {
		t.Fatalf("seed run A running expired lease: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-b', lease_until = ?
		WHERE run_id = ?
	`, leaseFuture, runB); err != nil {
		t.Fatalf("seed run B running active lease: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("MarkExpiredRunningAsOrphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runA {
		t.Fatalf("expected only runA orphaned, got %+v", orphaned)
	}

	var statusA, statusB string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runA).Scan(&statusA); err != nil {
		t.Fatalf("scan run A status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runB).Scan(&statusB); err != nil {
		t.Fatalf("scan run B status: %v", err)
	}

	if statusA != "orphaned" {
		t.Fatalf("expected run A orphaned, got %q", statusA)
	}
	if statusB != "running" {
		t.Fatalf("expected run B running, got %q", statusB)
	}
}

func TestRunsRepository_MarkRunSucceeded_FromOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-orphan-finish", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed orphaned run: %v", err)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, ""); err != nil {
		t.Fatalf("MarkRunSucceeded from orphaned: %v", err)
	}

	var status string
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_until, CAST(finished_at AS TEXT)
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &leaseOwner, &leaseUntil, &finishedAt); err != nil {
		t.Fatalf("query run state: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected status succeeded, got %q", status)
	}

	if leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected lease owner/until cleared, got owner=%v lease_until=%v", leaseOwner, leaseUntil)
	}

	if !finishedAt.Valid || finishedAt.String == "" {
		t.Fatal("expected finished_at set")
	}
}

func TestRunsRepository_FencingTokenRejectsStaleFinalizeAndRenew(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-fencing", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, tokenA, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-a: %v", err)
	}

	if !claimed || tokenA == "" {
		t.Fatalf("expected worker-a claim and token, got claimed=%v token=%q", claimed, tokenA)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'queued', lease_owner = NULL, lease_until = NULL, claim_token = NULL
		WHERE run_id = ?
	`, runID); err != nil {
		t.Fatalf("force requeue: %v", err)
	}

	claimed, tokenB, err := runs.TryClaim(ctx, runID, "worker-b", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-b: %v", err)
	}

	if !claimed || tokenB == "" {
		t.Fatalf("expected worker-b claim and token, got claimed=%v token=%q", claimed, tokenB)
	}

	if tokenA == tokenB {
		t.Fatal("expected distinct claim tokens across attempts")
	}

	var attempt int
	if err := db.QueryRowContext(ctx, `SELECT attempt FROM job_runs WHERE run_id = ?`, runID).Scan(&attempt); err != nil {
		t.Fatalf("scan attempt: %v", err)
	}
	if attempt != 2 {
		t.Fatalf("expected attempt=2 after two successful claims, got %d", attempt)
	}

	if err := runs.RenewLease(ctx, runID, "worker-b", tokenA, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected stale token renew to fail")
	}

	if err := runs.MarkRunSucceeded(ctx, runID, tokenA); err == nil {
		t.Fatal("expected stale token finalize to fail")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after stale finalize: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status to remain running for active token, got %q", status)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, tokenB); err != nil {
		t.Fatalf("expected active token finalize to succeed: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after active finalize: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected status succeeded after active token finalize, got %q", status)
	}
}

func TestRunsRepository_RequeueRunForRetry_ClearsLeaseAndToken(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-requeue-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}
	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunFailed(ctx, runID, token, "test failure"); err != nil {
		t.Fatalf("mark run failed: %v", err)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("RequeueRunForRetry: %v", err)
	}

	var status string
	var failure sql.NullString
	var claimToken sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var lastDispatched sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, claim_token, lease_owner, lease_until, last_dispatched_at
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failure, &claimToken, &leaseOwner, &leaseUntil, &lastDispatched); err != nil {
		t.Fatalf("query requeued run: %v", err)
	}

	if status != "queued" {
		t.Fatalf("expected queued status, got %q", status)
	}

	if failure.Valid || claimToken.Valid || leaseOwner.Valid || leaseUntil.Valid || lastDispatched.Valid {
		t.Fatalf("expected queue retry to clear runtime fields; got failure=%v token=%v owner=%v lease_until=%v dispatched=%v",
			failure, claimToken, leaseOwner, leaseUntil, lastDispatched)
	}
}

func TestRunsRepository_MarkRunOrphaned_WithClaimToken(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-mark-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunOrphaned(ctx, runID, token, dal.OrphanReasonAckUncertain); err != nil {
		t.Fatalf("MarkRunOrphaned: %v", err)
	}

	var status string
	var reason sql.NullString
	var orphanReason sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, orphan_reason, lease_owner, lease_until, claim_token
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &reason, &orphanReason, &leaseOwner, &leaseUntil, &claimToken); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "orphaned" {
		t.Fatalf("expected orphaned status, got %q", status)
	}

	if !reason.Valid || reason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan reason, got %v", reason)
	}
	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan_reason, got %v", orphanReason)
	}

	if leaseOwner.Valid || leaseUntil.Valid || claimToken.Valid {
		t.Fatalf("expected lease/token cleared, got owner=%v lease=%v token=%v", leaseOwner, leaseUntil, claimToken)
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
