//go:build integration

package postgres_test

import (
	"errors"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/pgtest"
)

func TestPostgres_OrphanAndAdmissionRetirementContracts(t *testing.T) {
	db := pgtest.NewTestDB(t)
	ctx := t.Context()
	repos := dal.NewSQLRepositories(db)
	for _, terminal := range []string{dal.RunStatusSucceeded, dal.RunStatusFailed, dal.RunStatusCancelled, dal.RunStatusAborted, dal.RunStatusAbandoned} {
		runID, _, err := repos.Runs().CreateRun(ctx, "terminal-"+terminal, nil, 1)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := db.ExecContext(ctx, `UPDATE job_runs SET status = $1 WHERE run_id = $2`, terminal, runID); err != nil {
			t.Fatal(err)
		}

		if err := repos.Runs().MarkRunOrphaned(ctx, runID, dal.OrphanReasonAckUncertain); err != nil {
			t.Fatal(err)
		}

		state, found, err := repos.Runs().GetRunStatus(ctx, runID)
		if err != nil || !found || state != terminal {
			t.Fatalf("late orphan changed %s to %s: %v", terminal, state, err)
		}
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "admission", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	receipts := repos.OrchestratorRetirements()
	if recorded, err := receipts.RecordRejectedAdmission(ctx, runID, root.ExecutionID, "active"); recorded || err != nil {
		t.Fatalf("active admission retired: %v %v", recorded, err)
	}

	other := dal.NewSQLRepositoriesWithCellID(db, "other").OrchestratorRetirements()
	if _, err := other.RecordRejectedAdmission(ctx, runID, root.ExecutionID, "other"); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("wrong-cell admission accepted: %v", err)
	}

	if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if recorded, err := receipts.RecordRejectedAdmission(ctx, runID, root.ExecutionID, "superseded"); !recorded || err != nil {
		t.Fatalf("superseded admission not retired: %v %v", recorded, err)
	}

	if _, err := db.ExecContext(ctx, `DELETE FROM job_runs WHERE run_id = $1`, runID); err != nil {
		t.Fatal(err)
	}

	due, err := receipts.ListDue(ctx, time.Now().Unix(), 256)
	if err != nil || len(due) != 1 || due[0].PersistenceToken != "superseded" {
		t.Fatalf("receipt lost with run history: %+v %v", due, err)
	}
}
