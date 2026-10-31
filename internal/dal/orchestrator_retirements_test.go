package dal_test

import (
	"errors"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestOrchestratorRetirementsRejectActiveAdmissionAndWrongCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := t.Context()
	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.Runs().CreateRun(ctx, "admission", nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}

	if recorded, err := repos.OrchestratorRetirements().RecordRejectedAdmission(ctx, runID, root.ExecutionID, "active"); recorded || err != nil {
		t.Fatalf("active admission recorded: %v %v", recorded, err)
	}

	wrongCell := dal.NewSQLRepositoriesWithCellID(db, "other").OrchestratorRetirements()
	if _, err := wrongCell.RecordRejectedAdmission(ctx, runID, root.ExecutionID, "wrong-cell"); !errors.Is(err, dal.ErrConflict) {
		t.Fatalf("wrong cell accepted: %v", err)
	}

	if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatal(err)
	}

	if recorded, err := repos.OrchestratorRetirements().RecordRejectedAdmission(ctx, runID, root.ExecutionID, "superseded"); !recorded || err != nil {
		t.Fatalf("superseded admission not recorded: %v %v", recorded, err)
	}
}

func TestOrchestratorRetirementsCellScopeAndRetry(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := t.Context()

	for _, cell := range []string{"a", "b"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO orchestrator_retirements (persistence_token, run_id, cell_id) VALUES (?, ?, ?)`, cell, "deleted-run", cell); err != nil {
			t.Fatal(err)
		}
	}

	a := dal.NewSQLRepositoriesWithCellID(db, "a").OrchestratorRetirements()
	b := dal.NewSQLRepositoriesWithCellID(db, "b").OrchestratorRetirements()
	if err := a.Delete(ctx, "b"); err != nil {
		t.Fatal(err)
	}

	if err := a.RetryAfter(ctx, "a", 100); err != nil {
		t.Fatal(err)
	}

	if rows, err := a.ListDue(ctx, 99, 1); err != nil || len(rows) != 0 {
		t.Fatalf("receipt retried too soon: %+v %v", rows, err)
	}

	if rows, err := a.ListDue(ctx, 100, 1); err != nil || len(rows) != 1 || rows[0].PersistenceToken != "a" {
		t.Fatalf("due receipt: %+v %v", rows, err)
	}

	if rows, err := b.ListDue(ctx, 0, 1); err != nil || len(rows) != 1 || rows[0].PersistenceToken != "b" {
		t.Fatalf("other cell receipt changed: %+v %v", rows, err)
	}

	if err := a.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if rows, err := a.ListDue(ctx, 100, 1); err != nil || len(rows) != 0 {
		t.Fatalf("delivered receipt retained: %+v %v", rows, err)
	}
}
