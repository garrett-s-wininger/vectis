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

func TestCatalogStatusBackfillRepository_ListMissingExecutionSecurityEvents(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "pdx-b")

	runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, "job-backfill-security", `{"id":"job-backfill-security","root":{"uses":"builtins/shell"}}`, nil, "pdx-b")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	event := dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        dispatch.TaskID,
		TaskAttemptID: dispatch.TaskAttemptID,
		ExecutionID:   dispatch.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "denied",
		Reason:        "authorization_denied",
		Provider:      "encryptedfs",
		CreatedAt:     123,
	}

	event.EventKey = dal.ExecutionSecurityEventKey(event)
	if err := repos.Runs().RecordExecutionSecurityEvent(ctx, event); err != nil {
		t.Fatalf("record security event: %v", err)
	}

	missing, err := repos.CatalogStatusBackfill().ListMissingExecutionSecurityCatalogEvents(ctx, "pdx-b", 10)
	if err != nil {
		t.Fatalf("ListMissingExecutionSecurityCatalogEvents: %v", err)
	}

	if len(missing) != 1 || missing[0].EventKey != event.EventKey || missing[0].ExecutionID != dispatch.ExecutionID {
		t.Fatalf("missing security events: %+v", missing)
	}

	if _, _, err := repos.CatalogEvents().Record(ctx, "pdx-b", event.EventKey, cell.CatalogEventTypeExecutionSecurity, []byte(`{"event_key":"`+event.EventKey+`","run_id":"`+runID+`","event_type":"secret_resolution","outcome":"denied"}`)); err != nil {
		t.Fatalf("record catalog event: %v", err)
	}

	missing, err = repos.CatalogStatusBackfill().ListMissingExecutionSecurityCatalogEvents(ctx, "pdx-b", 10)
	if err != nil {
		t.Fatalf("ListMissingExecutionSecurityCatalogEvents after record: %v", err)
	}

	if len(missing) != 0 {
		t.Fatalf("expected no missing security events, got %+v", missing)
	}
}
