package runstore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/testutil/dbtest"
)

func TestCreateRun_StoredJob_ComputesNextIndex(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	runID1, idx1, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if idx1 != 1 {
		t.Errorf("first run_index want 1, got %d", idx1)
	}

	if runID1 == "" {
		t.Error("run_id should be non-empty")
	}

	runID2, idx2, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun second: %v", err)
	}

	if idx2 != 2 {
		t.Errorf("second run_index want 2, got %d", idx2)
	}

	if runID2 == runID1 {
		t.Error("run_ids should differ")
	}

	var status string
	var startedAt sql.NullString
	err = db.QueryRowContext(ctx, "SELECT status, started_at FROM job_runs WHERE run_id = ?", runID1).Scan(&status, &startedAt)
	if err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "queued" {
		t.Errorf("status want queued, got %s", status)
	}

	if startedAt.Valid {
		t.Errorf("started_at should be NULL for queued run, got %s", startedAt.String)
	}
}

func TestCreateRun_Ephemeral_UsesGivenIndex(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	idx := 1
	runID, outIdx, err := CreateRun(ctx, db, "ephemeral-job", &idx)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if outIdx != 1 {
		t.Errorf("run_index want 1, got %d", outIdx)
	}

	if runID == "" {
		t.Error("run_id should be non-empty")
	}

	var jobID string
	err = db.QueryRowContext(ctx, "SELECT job_id FROM job_runs WHERE run_id = ?", runID).Scan(&jobID)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if jobID != "ephemeral-job" {
		t.Errorf("job_id want ephemeral-job, got %s", jobID)
	}
}

func TestStore_MarkRunRunning(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	err = store.MarkRunRunning(ctx, runID)
	if err != nil {
		t.Fatalf("MarkRunRunning: %v", err)
	}

	var status string
	var startedAt sql.NullString
	err = db.QueryRowContext(ctx, "SELECT status, started_at FROM job_runs WHERE run_id = ?", runID).Scan(&status, &startedAt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "running" {
		t.Errorf("status want running, got %s", status)
	}

	if !startedAt.Valid {
		t.Error("started_at should be set")
	}
}

func TestStore_MarkRunSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	_ = store.MarkRunRunning(ctx, runID)
	err = store.MarkRunSucceeded(ctx, runID)
	if err != nil {
		t.Fatalf("MarkRunSucceeded: %v", err)
	}

	var status string
	var finishedAt sql.NullString
	err = db.QueryRowContext(ctx, "SELECT status, finished_at FROM job_runs WHERE run_id = ?", runID).Scan(&status, &finishedAt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "succeeded" {
		t.Errorf("status want succeeded, got %s", status)
	}

	if !finishedAt.Valid {
		t.Error("finished_at should be set")
	}
}

func TestStore_MarkRunFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	_ = store.MarkRunRunning(ctx, runID)
	reason := "step failed: exit code 1"
	err = store.MarkRunFailed(ctx, runID, reason)
	if err != nil {
		t.Fatalf("MarkRunFailed: %v", err)
	}

	var status string
	var failureReason sql.NullString
	var finishedAt sql.NullString
	err = db.QueryRowContext(ctx, "SELECT status, failure_reason, finished_at FROM job_runs WHERE run_id = ?", runID).Scan(&status, &failureReason, &finishedAt)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "failed" {
		t.Errorf("status want failed, got %s", status)
	}

	if !failureReason.Valid || failureReason.String != reason {
		t.Errorf("failure_reason want %q, got valid=%v %q", reason, failureReason.Valid, failureReason.String)
	}

	if !finishedAt.Valid {
		t.Error("finished_at should be set")
	}
}

func TestStore_StatusTransitions_QueuedToRunningToSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if err := store.MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("MarkRunRunning: %v", err)
	}

	if err := store.MarkRunSucceeded(ctx, runID); err != nil {
		t.Fatalf("MarkRunSucceeded: %v", err)
	}

	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM job_runs WHERE run_id = ?", runID).Scan(&status)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "succeeded" {
		t.Errorf("status want succeeded, got %s", status)
	}
}

func TestTryClaim_Exclusive(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	s1 := NewStore(db)
	s2 := NewStore(db)
	until := time.Now().Add(time.Hour)

	ok1, err := s1.TryClaim(ctx, runID, "worker-a", until)
	if err != nil || !ok1 {
		t.Fatalf("first TryClaim: ok=%v err=%v", ok1, err)
	}

	ok2, err := s2.TryClaim(ctx, runID, "worker-b", until)
	if err != nil {
		t.Fatalf("second TryClaim: %v", err)
	}

	if ok2 {
		t.Fatal("second TryClaim should lose")
	}

	var status, owner string
	var lease sql.NullInt64
	err = db.QueryRowContext(ctx,
		"SELECT status, lease_owner, lease_until FROM job_runs WHERE run_id = ?", runID).Scan(&status, &owner, &lease)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if status != "running" || owner != "worker-a" || !lease.Valid {
		t.Errorf("row status=%q owner=%q lease=%v", status, owner, lease)
	}
}

func TestRenewLease_Extends(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	first := time.Now().Add(10 * time.Minute)
	ok, err := store.TryClaim(ctx, runID, "w1", first)
	if err != nil || !ok {
		t.Fatalf("TryClaim: %v %v", ok, err)
	}

	second := time.Now().Add(30 * time.Minute)
	if err := store.RenewLease(ctx, runID, "w1", second); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	var lease int64
	if err := db.QueryRowContext(ctx, "SELECT lease_until FROM job_runs WHERE run_id = ?", runID).Scan(&lease); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if lease != second.Unix() {
		t.Errorf("lease_until unix want %d got %d", second.Unix(), lease)
	}
}

func TestMarkRunSucceeded_ClearsLease(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	store := NewStore(db)

	runID, _, err := CreateRun(ctx, db, "job-1", nil)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	ok, err := store.TryClaim(ctx, runID, "w1", time.Now().Add(time.Hour))
	if err != nil || !ok {
		t.Fatalf("TryClaim: %v", err)
	}

	if err := store.MarkRunSucceeded(ctx, runID); err != nil {
		t.Fatalf("MarkRunSucceeded: %v", err)
	}

	var owner sql.NullString
	var lease sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT lease_owner, lease_until FROM job_runs WHERE run_id = ?", runID).Scan(&owner, &lease)
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if owner.Valid || lease.Valid {
		t.Errorf("expected lease cleared, owner=%v lease=%v", owner, lease)
	}
}
