package catalog

import (
	"context"
	"testing"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestFanInProcessorCopiesPendingEventsToTarget(t *testing.T) {
	ctx := context.Background()
	sourceDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceRepos := dal.NewSQLRepositories(sourceDB)
	targetRepos := dal.NewSQLRepositories(targetDB)

	sourceEvent, _, err := sourceRepos.CatalogEvents().Record(
		ctx,
		"pdx-b",
		"execution:execution-1:accepted",
		cell.CatalogEventTypeExecutionStatus,
		[]byte(`{"execution_id":"execution-1","status":"accepted"}`),
	)

	if err != nil {
		t.Fatalf("record source event: %v", err)
	}

	processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
		{CellID: "pdx-b", Events: sourceRepos.CatalogEvents()},
	})

	result, err := processor.IngestPending(ctx, 10)
	if err != nil {
		t.Fatalf("IngestPending: %v", err)
	}

	if result.Sources != 1 || result.Read != 1 || result.Copied != 1 {
		t.Fatalf("unexpected fan-in result: %+v", result)
	}

	sourcePending, err := sourceRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list source pending: %v", err)
	}

	if len(sourcePending) != 0 {
		t.Fatalf("source event was not marked applied: %+v", sourcePending)
	}

	targetPending, err := targetRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list target pending: %v", err)
	}

	if len(targetPending) != 1 {
		t.Fatalf("target pending: got %d, want 1", len(targetPending))
	}

	got := targetPending[0]
	if got.SourceCell != "pdx-b" || got.EventKey != sourceEvent.EventKey || got.EventType != cell.CatalogEventTypeExecutionStatus {
		t.Fatalf("unexpected target event: %+v", got)
	}
}

func TestFanInProcessorHonorsLimitAcrossSources(t *testing.T) {
	ctx := context.Background()
	sourceADB := dbtest.NewTestDB(t)
	sourceBDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceA := dal.NewSQLRepositories(sourceADB)
	sourceB := dal.NewSQLRepositories(sourceBDB)
	target := dal.NewSQLRepositories(targetDB)

	if _, _, err := sourceA.CatalogEvents().Record(ctx, "iad-a", "event-a", cell.CatalogEventTypeRunStatus, []byte(`{"run_id":"run-a","status":"running"}`)); err != nil {
		t.Fatalf("record source A: %v", err)
	}

	if _, _, err := sourceB.CatalogEvents().Record(ctx, "pdx-b", "event-b", cell.CatalogEventTypeRunStatus, []byte(`{"run_id":"run-b","status":"running"}`)); err != nil {
		t.Fatalf("record source B: %v", err)
	}

	processor := NewFanInProcessor(target.CatalogEvents(), []FanInSource{
		{CellID: "iad-a", Events: sourceA.CatalogEvents()},
		{CellID: "pdx-b", Events: sourceB.CatalogEvents()},
	})

	result, err := processor.IngestPending(ctx, 1)
	if err != nil {
		t.Fatalf("IngestPending: %v", err)
	}

	if result.Read != 1 || result.Copied != 1 {
		t.Fatalf("unexpected limited result: %+v", result)
	}

	targetPending, err := target.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list target pending: %v", err)
	}

	if len(targetPending) != 1 || targetPending[0].SourceCell != "iad-a" {
		t.Fatalf("unexpected target pending after limited ingest: %+v", targetPending)
	}
}

func TestFanInProcessorMarksSourceAppliedWhenTargetAlreadyHasEvent(t *testing.T) {
	ctx := context.Background()
	sourceDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceRepos := dal.NewSQLRepositories(sourceDB)
	targetRepos := dal.NewSQLRepositories(targetDB)
	payload := []byte(`{"run_id":"run-a","status":"running"}`)

	if _, _, err := sourceRepos.CatalogEvents().Record(ctx, "iad-a", "run:run-a:running", cell.CatalogEventTypeRunStatus, payload); err != nil {
		t.Fatalf("record source event: %v", err)
	}

	if _, _, err := targetRepos.CatalogEvents().Record(ctx, "iad-a", "run:run-a:running", cell.CatalogEventTypeRunStatus, payload); err != nil {
		t.Fatalf("record target event: %v", err)
	}

	processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
		{CellID: "iad-a", Events: sourceRepos.CatalogEvents()},
	})

	result, err := processor.IngestPending(ctx, 10)
	if err != nil {
		t.Fatalf("IngestPending: %v", err)
	}

	if result.Read != 1 || result.Copied != 0 {
		t.Fatalf("unexpected fan-in result: %+v", result)
	}

	sourcePending, err := sourceRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list source pending: %v", err)
	}

	if len(sourcePending) != 0 {
		t.Fatalf("source duplicate event was not marked applied: %+v", sourcePending)
	}
}
