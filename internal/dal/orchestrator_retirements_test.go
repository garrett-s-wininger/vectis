package dal_test

import (
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

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
