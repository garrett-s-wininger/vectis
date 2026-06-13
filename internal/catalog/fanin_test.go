package catalog

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

type recordingFanInMetrics struct {
	results []FanInSourceResult
}

func (m *recordingFanInMetrics) RecordFanInSourceResult(ctx context.Context, result FanInSourceResult) {
	m.results = append(m.results, result)
}

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

func TestFanInProcessorRecordsSourceMetrics(t *testing.T) {
	ctx := context.Background()
	sourceDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceRepos := dal.NewSQLRepositoriesWithCellID(sourceDB, "iad-a")
	targetRepos := dal.NewSQLRepositories(targetDB)

	runID, _, err := sourceRepos.CreateDefinitionAndRunInCell(ctx, "job-fanin-metrics", `{"id":"job-fanin-metrics","root":{"uses":"builtins/shell"}}`, nil, "iad-a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := sourceRepos.Runs().MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	events := sourceRepos.CatalogEvents()
	metrics := &recordingFanInMetrics{}
	processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
		{
			CellID:   "iad-a",
			Events:   events,
			Backfill: NewBackfillProcessor("iad-a", sourceRepos.CatalogStatusBackfill(), cell.NewCatalogEventPublisher("iad-a", events)),
		},
	})
	processor.SetMetrics(metrics)

	result, err := processor.IngestPending(ctx, 10)
	if err != nil {
		t.Fatalf("IngestPending: %v", err)
	}

	if result.Backfilled != 1 || result.Read != 1 || result.Copied != 1 {
		t.Fatalf("unexpected fan-in result: %+v", result)
	}

	if len(metrics.results) != 1 {
		t.Fatalf("metrics results: got %d, want 1 (%+v)", len(metrics.results), metrics.results)
	}

	got := metrics.results[0]
	if got.CellID != "iad-a" || got.Backfilled != 1 || got.Read != 1 || got.Copied != 1 {
		t.Fatalf("unexpected metrics result: %+v", got)
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

func TestFanInProcessorChaos_RetryAfterSourceMarkAppliedFailure(t *testing.T) {
	ctx := context.Background()
	sourceDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceRepos := dal.NewSQLRepositories(sourceDB)
	targetRepos := dal.NewSQLRepositories(targetDB)
	payload := []byte(`{"run_id":"run-a","status":"running"}`)

	if _, _, err := sourceRepos.CatalogEvents().Record(ctx, "iad-a", "run:run-a:running", cell.CatalogEventTypeRunStatus, payload); err != nil {
		t.Fatalf("record source event: %v", err)
	}

	markErr := errors.New("source database unavailable")
	sourceEvents := &failOnceMarkAppliedEventsRepository{
		CatalogEventsRepository: sourceRepos.CatalogEvents(),
		err:                     markErr,
	}

	processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
		{CellID: "iad-a", Events: sourceEvents},
	})

	if _, err := processor.IngestPending(ctx, 10); !errors.Is(err, markErr) {
		t.Fatalf("first IngestPending error = %v, want %v", err, markErr)
	}

	sourcePending, err := sourceRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list source pending after injected failure: %v", err)
	}

	if len(sourcePending) != 1 {
		t.Fatalf("source event should remain pending after failed mark-applied, got %+v", sourcePending)
	}

	targetPending, err := targetRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list target pending after injected failure: %v", err)
	}

	if len(targetPending) != 1 {
		t.Fatalf("target event should have been copied before failure, got %+v", targetPending)
	}

	result, err := processor.IngestPending(ctx, 10)
	if err != nil {
		t.Fatalf("retry IngestPending: %v", err)
	}

	if result.Read != 1 || result.Copied != 0 {
		t.Fatalf("retry result = %+v, want duplicate read with no new copy", result)
	}

	sourcePending, err = sourceRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list source pending after retry: %v", err)
	}

	if len(sourcePending) != 0 {
		t.Fatalf("source duplicate event was not marked applied after retry: %+v", sourcePending)
	}

	targetPending, err = targetRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list target pending after retry: %v", err)
	}

	if len(targetPending) != 1 {
		t.Fatalf("target duplicate should not be copied twice, got %+v", targetPending)
	}
}

func TestFanInProcessorBackfillsSourceBeforeCopy(t *testing.T) {
	ctx := context.Background()
	sourceDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceRepos := dal.NewSQLRepositoriesWithCellID(sourceDB, "iad-a")
	targetRepos := dal.NewSQLRepositories(targetDB)

	runID, _, err := sourceRepos.CreateDefinitionAndRunInCell(ctx, "job-fanin-backfill", `{"id":"job-fanin-backfill","root":{"uses":"builtins/shell"}}`, nil, "iad-a")
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := sourceRepos.Runs().MarkRunRunning(ctx, runID); err != nil {
		t.Fatalf("mark run running: %v", err)
	}

	events := sourceRepos.CatalogEvents()
	processor := NewFanInProcessor(targetRepos.CatalogEvents(), []FanInSource{
		{
			CellID:   "iad-a",
			Events:   events,
			Backfill: NewBackfillProcessor("iad-a", sourceRepos.CatalogStatusBackfill(), cell.NewCatalogEventPublisher("iad-a", events)),
		},
	})

	result, err := processor.IngestPending(ctx, 10)
	if err != nil {
		t.Fatalf("IngestPending: %v", err)
	}

	if result.Backfilled != 1 || result.Read != 1 || result.Copied != 1 {
		t.Fatalf("unexpected fan-in result: %+v", result)
	}

	targetPending, err := targetRepos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list target pending: %v", err)
	}

	if len(targetPending) != 1 || targetPending[0].EventKey != cell.CatalogRunStatusEventKey(runID, dal.RunStatusRunning) {
		t.Fatalf("unexpected target event after backfill: %+v", targetPending)
	}
}

type failOnceMarkAppliedEventsRepository struct {
	dal.CatalogEventsRepository
	err    error
	failed bool
}

func (r *failOnceMarkAppliedEventsRepository) MarkApplied(ctx context.Context, id int64) error {
	if !r.failed {
		r.failed = true
		return r.err
	}

	return r.CatalogEventsRepository.MarkApplied(ctx, id)
}
