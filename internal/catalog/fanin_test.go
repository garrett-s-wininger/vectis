package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/quick"

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

func TestFanInProcessorFault_RetryAfterSourceMarkAppliedFailure(t *testing.T) {
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

func TestFanInProcessorProperty_DrainsUniqueSourceEventsWithoutTargetDuplicates(t *testing.T) {
	prop := func(raw []byte) bool {
		if err := checkFanInDrainsUniqueSourceEvents(t, raw); err != nil {
			t.Logf("fan-in property failed for %v: %v", raw, err)
			return false
		}

		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 50}); err != nil {
		t.Fatalf("fan-in property failed: %v", err)
	}
}

func checkFanInDrainsUniqueSourceEvents(t *testing.T, raw []byte) error {
	t.Helper()

	ctx := context.Background()
	sourceADB := dbtest.NewTestDB(t)
	sourceBDB := dbtest.NewTestDB(t)
	targetDB := dbtest.NewTestDB(t)
	sourceA := dal.NewSQLRepositories(sourceADB)
	sourceB := dal.NewSQLRepositories(sourceBDB)
	target := dal.NewSQLRepositories(targetDB)

	events := fanInPropertyEvents(raw)
	unique := make(map[string]struct{})
	for _, event := range events {
		repos := sourceA
		if event.sourceCell == "pdx-b" {
			repos = sourceB
		}

		if _, _, err := repos.CatalogEvents().Record(ctx, event.sourceCell, event.eventKey, event.eventType, event.payload); err != nil {
			return fmt.Errorf("record source event %s/%s: %w", event.sourceCell, event.eventKey, err)
		}

		unique[event.sourceCell+"\x00"+event.eventKey] = struct{}{}
	}

	processor := NewFanInProcessor(target.CatalogEvents(), []FanInSource{
		{CellID: "iad-a", Events: sourceA.CatalogEvents()},
		{CellID: "pdx-b", Events: sourceB.CatalogEvents()},
	})
	limit := fanInPropertyLimit(raw)
	for i := 0; i < len(unique)+len(events)+2; i++ {
		if _, err := processor.IngestPending(ctx, limit); err != nil {
			return fmt.Errorf("ingest pass %d: %w", i, err)
		}
	}

	if err := assertFanInPropertySourceDrained(ctx, sourceA.CatalogEvents(), "iad-a"); err != nil {
		return err
	}

	if err := assertFanInPropertySourceDrained(ctx, sourceB.CatalogEvents(), "pdx-b"); err != nil {
		return err
	}

	targetPending, err := target.CatalogEvents().ListPending(ctx, len(unique)+1)
	if err != nil {
		return fmt.Errorf("list target pending: %w", err)
	}

	if len(targetPending) != len(unique) {
		return fmt.Errorf("target pending count = %d, want %d", len(targetPending), len(unique))
	}

	seen := make(map[string]struct{}, len(targetPending))
	for _, rec := range targetPending {
		key := rec.SourceCell + "\x00" + rec.EventKey
		if _, ok := seen[key]; ok {
			return fmt.Errorf("target duplicate event %s/%s", rec.SourceCell, rec.EventKey)
		}

		if _, ok := unique[key]; !ok {
			return fmt.Errorf("target contains unexpected event %s/%s", rec.SourceCell, rec.EventKey)
		}

		seen[key] = struct{}{}
	}

	result, err := processor.IngestPending(ctx, limit)
	if err != nil {
		return fmt.Errorf("final idempotent ingest: %w", err)
	}

	if result.Read != 0 || result.Copied != 0 {
		return fmt.Errorf("final idempotent ingest read/copy = %d/%d, want 0/0", result.Read, result.Copied)
	}

	return nil
}

type fanInPropertyEvent struct {
	sourceCell string
	eventKey   string
	eventType  string
	payload    []byte
}

func fanInPropertyEvents(raw []byte) []fanInPropertyEvent {
	if len(raw) > 24 {
		raw = raw[:24]
	}

	events := make([]fanInPropertyEvent, 0, len(raw))
	for _, b := range raw {
		sourceCell := "iad-a"
		if b&1 == 1 {
			sourceCell = "pdx-b"
		}

		keyIndex := int((b >> 1) % 8)
		eventKey := fmt.Sprintf("event-%d", keyIndex)
		eventType := cell.CatalogEventTypeRunStatus
		payload := []byte(fmt.Sprintf(`{"run_id":"run-%d","status":"running"}`, keyIndex))
		if b&0x10 != 0 {
			eventType = cell.CatalogEventTypeExecutionStatus
			payload = []byte(fmt.Sprintf(`{"execution_id":"execution-%d","status":"accepted"}`, keyIndex))
		}

		events = append(events, fanInPropertyEvent{
			sourceCell: sourceCell,
			eventKey:   eventKey,
			eventType:  eventType,
			payload:    payload,
		})
	}

	return events
}

func fanInPropertyLimit(raw []byte) int {
	if len(raw) == 0 {
		return 1
	}

	return 1 + int(raw[0]%4)
}

func assertFanInPropertySourceDrained(ctx context.Context, events dal.CatalogEventsRepository, sourceCell string) error {
	pending, err := events.ListPending(ctx, 1)
	if err != nil {
		return fmt.Errorf("list source %s pending: %w", sourceCell, err)
	}

	if len(pending) != 0 {
		return fmt.Errorf("source %s still has pending events: %+v", sourceCell, pending)
	}

	return nil
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
