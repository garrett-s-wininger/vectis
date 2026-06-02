package cell

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

type recordingCatalogUpdater struct {
	failAt int
	calls  []string
}

func (u *recordingCatalogUpdater) ApplyRunStatusUpdate(ctx context.Context, update dal.RunStatusUpdate) error {
	u.calls = append(u.calls, "run:"+update.RunID+":"+update.Status)
	return u.errIfNeeded()
}

func (u *recordingCatalogUpdater) ApplyExecutionStatusUpdate(ctx context.Context, update dal.ExecutionStatusUpdate) error {
	u.calls = append(u.calls, "execution:"+update.ExecutionID+":"+update.Status)
	return u.errIfNeeded()
}

func (u *recordingCatalogUpdater) errIfNeeded() error {
	if u.failAt > 0 && len(u.calls) == u.failAt {
		return fmt.Errorf("forced update failure")
	}

	return nil
}

func TestCatalogEventConsumer_ApplyBatchAppliesEventsInOrder(t *testing.T) {
	updater := &recordingCatalogUpdater{}
	consumer := NewCatalogEventConsumer(updater)

	events := []CatalogEvent{
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-1",
				Status: dal.RunStatusRunning,
			},
		},
		{
			SourceCellID: "iad-a",
			ExecutionStatus: &dal.ExecutionStatusUpdate{
				ExecutionID: "execution-1",
				Status:      dal.ExecutionStatusAccepted,
			},
		},
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:       "run-1",
				Status:      dal.RunStatusFailed,
				FailureCode: dal.FailureCodeExecution,
				Reason:      "failed in cell",
			},
		},
	}

	if err := consumer.ApplyBatch(context.Background(), events); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	want := []string{
		"run:run-1:running",
		"execution:execution-1:accepted",
		"run:run-1:failed",
	}

	if !reflect.DeepEqual(updater.calls, want) {
		t.Fatalf("calls: got %+v, want %+v", updater.calls, want)
	}
}

func TestCatalogEventConsumer_ApplyBatchStopsOnError(t *testing.T) {
	updater := &recordingCatalogUpdater{failAt: 2}
	consumer := NewCatalogEventConsumer(updater)

	events := []CatalogEvent{
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-1",
				Status: dal.RunStatusRunning,
			},
		},
		{
			SourceCellID: "iad-a",
			ExecutionStatus: &dal.ExecutionStatusUpdate{
				ExecutionID: "execution-1",
				Status:      dal.ExecutionStatusAccepted,
			},
		},
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-2",
				Status: dal.RunStatusRunning,
			},
		},
	}

	if err := consumer.ApplyBatch(context.Background(), events); err == nil {
		t.Fatal("expected ApplyBatch to fail")
	}

	want := []string{
		"run:run-1:running",
		"execution:execution-1:accepted",
	}

	if !reflect.DeepEqual(updater.calls, want) {
		t.Fatalf("calls: got %+v, want %+v", updater.calls, want)
	}
}

func TestCatalogEventConsumer_RejectsInvalidEvents(t *testing.T) {
	consumer := NewCatalogEventConsumer(&recordingCatalogUpdater{})

	tests := []struct {
		name  string
		event CatalogEvent
	}{
		{
			name: "missing source cell",
			event: CatalogEvent{
				RunStatus: &dal.RunStatusUpdate{RunID: "run-1", Status: dal.RunStatusRunning},
			},
		},
		{
			name: "missing status update",
			event: CatalogEvent{
				SourceCellID: "iad-a",
			},
		},
		{
			name: "multiple status updates",
			event: CatalogEvent{
				SourceCellID:    "iad-a",
				RunStatus:       &dal.RunStatusUpdate{RunID: "run-1", Status: dal.RunStatusRunning},
				ExecutionStatus: &dal.ExecutionStatusUpdate{ExecutionID: "execution-1", Status: dal.ExecutionStatusAccepted},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := consumer.Apply(context.Background(), tt.event)
			if !errors.Is(err, ErrInvalidCatalogEvent) {
				t.Fatalf("expected ErrInvalidCatalogEvent, got %v", err)
			}
		})
	}
}

func TestCatalogEventPublisher_RecordStatusEvents(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	publisher := NewCatalogEventPublisher("iad-a", repos.CatalogEvents())

	if err := publisher.RecordRunStatus(ctx, dal.RunStatusUpdate{RunID: "run-1", Status: dal.RunStatusRunning}); err != nil {
		t.Fatalf("RecordRunStatus: %v", err)
	}

	if err := publisher.RecordExecutionStatus(ctx, dal.ExecutionStatusUpdate{ExecutionID: "execution-1", Status: dal.ExecutionStatusAccepted}); err != nil {
		t.Fatalf("RecordExecutionStatus: %v", err)
	}

	records, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}

	if len(records) != 2 {
		t.Fatalf("pending records: got %d, want 2", len(records))
	}

	if records[0].SourceCell != "iad-a" || records[0].EventKey != "run:run-1:running" || records[0].EventType != CatalogEventTypeRunStatus {
		t.Fatalf("unexpected run event: %+v", records[0])
	}

	if records[1].SourceCell != "iad-a" || records[1].EventKey != "execution:execution-1:accepted" || records[1].EventType != CatalogEventTypeExecutionStatus {
		t.Fatalf("unexpected execution event: %+v", records[1])
	}
}

func TestCatalogInboxProcessor_ProcessPendingAppliesAndMarksEvents(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-catalog-inbox", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-inbox"
	if err := repos.Jobs().Create(ctx, jobID, `{"id":"job-catalog-inbox","root":{"uses":"builtins/shell"}}`, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if _, _, err := repos.CatalogEvents().Record(
		ctx,
		"iad-a",
		"event-execution-accepted",
		CatalogEventTypeExecutionStatus,
		fmt.Appendf(nil, `{"execution_id":%q,"status":%q}`, dispatch.ExecutionID, dal.ExecutionStatusAccepted),
	); err != nil {
		t.Fatalf("record execution event: %v", err)
	}

	if _, _, err := repos.CatalogEvents().Record(
		ctx,
		"iad-a",
		"event-run-running",
		CatalogEventTypeRunStatus,
		fmt.Appendf(nil, `{"run_id":%q,"status":%q}`, runID, dal.RunStatusRunning),
	); err != nil {
		t.Fatalf("record run event: %v", err)
	}

	processor := NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs())
	result, err := processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}

	if result.Read != 2 || result.Applied != 2 || result.Failed != 0 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	pending, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending after process: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected no pending events, got %+v", pending)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, "SELECT status FROM segment_executions WHERE execution_id = ?", dispatch.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAccepted {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusAccepted)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, "SELECT status FROM job_runs WHERE run_id = ?", runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusRunning {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusRunning)
	}
}

func TestCatalogInboxProcessor_ProcessPendingMarksInvalidPayloadFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	rec, _, err := repos.CatalogEvents().Record(ctx, "iad-a", "event-bad", CatalogEventTypeRunStatus, []byte(`{"run_id":`))
	if err != nil {
		t.Fatalf("record invalid event: %v", err)
	}

	processor := NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs())
	result, err := processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}

	if result.Read != 1 || result.Applied != 0 || result.Failed != 1 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	assertCatalogEventFailed(t, db, rec.ID, "decode run status payload")
}

func TestCatalogInboxProcessor_ProcessPendingMarksUpdaterErrorFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	rec, _, err := repos.CatalogEvents().Record(
		ctx,
		"iad-a",
		"event-updater-fails",
		CatalogEventTypeRunStatus,
		[]byte(`{"run_id":"run-1","status":"running"}`),
	)

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	processor := NewCatalogInboxProcessor(repos.CatalogEvents(), &recordingCatalogUpdater{failAt: 1})
	result, err := processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}

	if result.Read != 1 || result.Applied != 0 || result.Failed != 1 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	assertCatalogEventFailed(t, db, rec.ID, "forced update failure")
}

func assertCatalogEventFailed(t *testing.T, db *sql.DB, eventID int64, wantError string) {
	t.Helper()

	var status string
	var attempts int
	var lastError sql.NullString
	if err := db.QueryRow("SELECT status, attempts, last_error FROM cell_catalog_events WHERE id = ?", eventID).Scan(&status, &attempts, &lastError); err != nil {
		t.Fatalf("query catalog event: %v", err)
	}

	if status != dal.CatalogEventStatusFailed {
		t.Fatalf("status: got %q, want %q", status, dal.CatalogEventStatusFailed)
	}

	if attempts != 1 {
		t.Fatalf("attempts: got %d, want 1", attempts)
	}

	if !lastError.Valid || !strings.Contains(lastError.String, wantError) {
		t.Fatalf("last_error: got %+v, want substring %q", lastError, wantError)
	}
}
