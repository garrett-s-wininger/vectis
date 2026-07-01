package catalog

import (
	"context"
	"testing"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestBackfillProcessorRecordsMissingRunAndExecutionEvents(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")

	runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, "job-backfill", `{"id":"job-backfill","root":{"uses":"builtins/script"}}`, nil, "iad-a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if err := repos.Runs().MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark execution started: %v", err)
	}

	if err := repos.Runs().RecordExecutionSecurityEvent(ctx, dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        dispatch.TaskID,
		TaskAttemptID: dispatch.TaskAttemptID,
		ExecutionID:   dispatch.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSVIDCheck,
		Outcome:       "failed",
		Reason:        "mismatch",
		CreatedAt:     123,
	}); err != nil {
		t.Fatalf("record execution security event: %v", err)
	}

	processor := NewBackfillProcessor(
		"iad-a",
		repos.CatalogStatusBackfill(),
		cell.NewCatalogEventPublisher("iad-a", repos.CatalogEvents()),
	)

	result, err := processor.RepairMissing(ctx, 10)
	if err != nil {
		t.Fatalf("RepairMissing: %v", err)
	}

	if result.RunEvents != 1 || result.ExecutionEvents != 1 || result.ExecutionSecurityEvents != 1 {
		t.Fatalf("unexpected backfill result: %+v", result)
	}

	pending, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending catalog events: %v", err)
	}

	if len(pending) != 3 {
		t.Fatalf("pending events: got %d, want 3 (%+v)", len(pending), pending)
	}

	wantKeys := map[string]bool{
		cell.CatalogRunStatusEventKey(runID, dal.RunStatusRunning):                            true,
		cell.CatalogExecutionStatusEventKey(dispatch.ExecutionID, dal.ExecutionStatusRunning): true,
		dal.ExecutionSecurityEventKey(dal.RecordExecutionSecurityEventParams{
			RunID:         runID,
			TaskID:        dispatch.TaskID,
			TaskAttemptID: dispatch.TaskAttemptID,
			ExecutionID:   dispatch.ExecutionID,
			EventType:     dal.ExecutionSecurityEventSVIDCheck,
			Outcome:       "failed",
			Reason:        "mismatch",
			CreatedAt:     123,
		}): true,
	}

	for _, event := range pending {
		if !wantKeys[event.EventKey] {
			t.Fatalf("unexpected event key %q in %+v", event.EventKey, pending)
		}
	}
}

func TestBackfillProcessorHonorsLimit(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")

	for _, jobID := range []string{"job-backfill-limit-a", "job-backfill-limit-b"} {
		runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, jobID, `{"id":"`+jobID+`","root":{"uses":"builtins/script"}}`, nil, "iad-a")
		if err != nil {
			t.Fatalf("create run %s: %v", jobID, err)
		}

		if err := repos.Runs().MarkRunRunning(ctx, runID); err != nil {
			t.Fatalf("mark run %s running: %v", runID, err)
		}
	}

	processor := NewBackfillProcessor(
		"iad-a",
		repos.CatalogStatusBackfill(),
		cell.NewCatalogEventPublisher("iad-a", repos.CatalogEvents()),
	)

	result, err := processor.RepairMissing(ctx, 1)
	if err != nil {
		t.Fatalf("RepairMissing: %v", err)
	}

	if result.Total() != 1 {
		t.Fatalf("backfilled events: got %d, want 1 (%+v)", result.Total(), result)
	}
}
