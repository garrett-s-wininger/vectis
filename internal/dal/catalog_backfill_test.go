package dal_test

import (
	"context"
	"testing"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestCatalogStatusBackfillRepository_ListMissingRunEvents(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")

	runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, "job-backfill-run", `{"id":"job-backfill-run","root":{"uses":"builtins/shell"}}`, nil, "iad-a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := repos.Runs().MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	missing, err := repos.CatalogStatusBackfill().ListMissingRunStatusCatalogEvents(ctx, "iad-a", 10)
	if err != nil {
		t.Fatalf("ListMissingRunStatusCatalogEvents: %v", err)
	}
	if len(missing) != 1 || missing[0].RunID != runID || missing[0].Status != dal.RunStatusRunning {
		t.Fatalf("missing run events: %+v", missing)
	}

	if _, _, err := repos.CatalogEvents().Record(ctx, "iad-a", cell.CatalogRunStatusEventKey(runID, dal.RunStatusRunning), cell.CatalogEventTypeRunStatus, []byte(`{"run_id":"`+runID+`","status":"running"}`)); err != nil {
		t.Fatalf("record catalog event: %v", err)
	}

	missing, err = repos.CatalogStatusBackfill().ListMissingRunStatusCatalogEvents(ctx, "iad-a", 10)
	if err != nil {
		t.Fatalf("ListMissingRunStatusCatalogEvents after record: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing run events, got %+v", missing)
	}
}

func TestCatalogStatusBackfillRepository_ListMissingExecutionEvents(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "pdx-b")

	runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, "job-backfill-execution", `{"id":"job-backfill-execution","root":{"uses":"builtins/shell"}}`, nil, "pdx-b")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark execution started: %v", err)
	}

	missing, err := repos.CatalogStatusBackfill().ListMissingExecutionStatusCatalogEvents(ctx, "pdx-b", 10)
	if err != nil {
		t.Fatalf("ListMissingExecutionStatusCatalogEvents: %v", err)
	}
	if len(missing) != 1 || missing[0].ExecutionID != dispatch.ExecutionID || missing[0].Status != dal.ExecutionStatusRunning {
		t.Fatalf("missing execution events: %+v", missing)
	}

	if _, _, err := repos.CatalogEvents().Record(ctx, "pdx-b", cell.CatalogExecutionStatusEventKey(dispatch.ExecutionID, dal.ExecutionStatusRunning), cell.CatalogEventTypeExecutionStatus, []byte(`{"execution_id":"`+dispatch.ExecutionID+`","status":"running"}`)); err != nil {
		t.Fatalf("record catalog event: %v", err)
	}

	missing, err = repos.CatalogStatusBackfill().ListMissingExecutionStatusCatalogEvents(ctx, "pdx-b", 10)
	if err != nil {
		t.Fatalf("ListMissingExecutionStatusCatalogEvents after record: %v", err)
	}

	if len(missing) != 0 {
		t.Fatalf("expected no missing execution events, got %+v", missing)
	}
}
