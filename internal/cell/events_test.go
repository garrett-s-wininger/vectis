package cell

import (
	"context"
	"database/sql"
	"encoding/json"
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

	if err := publisher.RecordArtifact(ctx, dal.ArtifactCreate{
		RunID:           "run-1",
		CellID:          "iad-a",
		Name:            "coverage",
		Path:            "coverage.txt",
		ContentType:     "text/plain",
		BlobKey:         "blob-key",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "abc123",
		SizeBytes:       42,
		ArtifactShardID: "artifact-shard",
	}); err != nil {
		t.Fatalf("RecordArtifact: %v", err)
	}

	if err := publisher.RecordExecutionSecurity(ctx, dal.RecordExecutionSecurityEventParams{
		RunID:       "run-1",
		ExecutionID: "execution-1",
		EventType:   dal.ExecutionSecurityEventSVIDCheck,
		Outcome:     "failed",
		Reason:      "mismatch",
		CreatedAt:   123,
	}); err != nil {
		t.Fatalf("RecordExecutionSecurity: %v", err)
	}

	records, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}

	if len(records) != 4 {
		t.Fatalf("pending records: got %d, want 4", len(records))
	}

	if records[0].SourceCell != "iad-a" || records[0].EventKey != "run:run-1:running" || records[0].EventType != CatalogEventTypeRunStatus {
		t.Fatalf("unexpected run event: %+v", records[0])
	}

	if records[1].SourceCell != "iad-a" || records[1].EventKey != "execution:execution-1:accepted" || records[1].EventType != CatalogEventTypeExecutionStatus {
		t.Fatalf("unexpected execution event: %+v", records[1])
	}

	if records[2].SourceCell != "iad-a" || records[2].EventKey != "artifact:run-1:coverage" || records[2].EventType != CatalogEventTypeArtifactRecord {
		t.Fatalf("unexpected artifact event: %+v", records[2])
	}

	if records[3].SourceCell != "iad-a" || !strings.HasPrefix(records[3].EventKey, "security:run-1:") || records[3].EventType != CatalogEventTypeExecutionSecurity {
		t.Fatalf("unexpected security event: %+v", records[3])
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

	securityPayload, err := json.Marshal(dal.RecordExecutionSecurityEventParams{
		EventKey:      "security-event-1",
		RunID:         runID,
		TaskID:        dispatch.TaskID,
		TaskAttemptID: dispatch.TaskAttemptID,
		ExecutionID:   dispatch.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "denied",
		Reason:        "authorization_denied",
		Provider:      "encryptedfs",
		CreatedAt:     123,
	})

	if err != nil {
		t.Fatalf("marshal security payload: %v", err)
	}

	if _, _, err := repos.CatalogEvents().Record(
		ctx,
		"iad-a",
		"security-event-1",
		CatalogEventTypeExecutionSecurity,
		securityPayload,
	); err != nil {
		t.Fatalf("record security event: %v", err)
	}

	processor := NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs())
	result, err := processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}

	if result.Read != 3 || result.Applied != 3 || result.Failed != 0 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	pending, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending after process: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected no pending events, got %+v", pending)
	}

	latest, err := repos.Runs().LatestRunSecurityEvent(ctx, runID, true)
	if err != nil {
		t.Fatalf("LatestRunSecurityEvent: %v", err)
	}

	if latest == nil || latest.EventType != dal.ExecutionSecurityEventSecretResolution || latest.Outcome != "denied" {
		t.Fatalf("latest security event: %+v", latest)
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

func TestCatalogInboxProcessor_ProcessPendingAppliesArtifactRecord(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-catalog-artifact", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-artifact"
	if err := repos.Jobs().Create(ctx, jobID, `{"id":"job-catalog-artifact","root":{"uses":"builtins/shell"}}`, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	create := dal.ArtifactCreate{
		RunID:           runID,
		CellID:          "iad-a",
		Name:            "coverage",
		Path:            "coverage.txt",
		ContentType:     "text/plain",
		BlobKey:         "blob-key",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "abc123",
		SizeBytes:       42,
		ArtifactShardID: "artifact-shard",
	}

	payload, err := json.Marshal(create)
	if err != nil {
		t.Fatalf("marshal artifact event: %v", err)
	}

	if _, _, err := repos.CatalogEvents().Record(ctx, "iad-a", CatalogArtifactEventKey(runID, create.Name), CatalogEventTypeArtifactRecord, payload); err != nil {
		t.Fatalf("record artifact event: %v", err)
	}

	processor := NewCatalogInboxProcessor(repos.CatalogEvents(), repos.Runs(), repos.Artifacts())
	result, err := processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending: %v", err)
	}

	if result.Read != 1 || result.Applied != 1 || result.Failed != 0 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	rec, err := repos.Artifacts().GetByRunAndName(ctx, runID, create.Name)
	if err != nil {
		t.Fatalf("get artifact manifest: %v", err)
	}

	if rec.Path != create.Path || rec.BlobKey != create.BlobKey || rec.ArtifactShardID != create.ArtifactShardID {
		t.Fatalf("artifact manifest mismatch: %+v", rec)
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

func TestCatalogInboxProcessor_ProcessPendingLeavesUpdaterErrorRetryable(t *testing.T) {
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

	if result.Read != 1 || result.Applied != 0 || result.Failed != 0 || result.Retryable != 1 {
		t.Fatalf("unexpected process result: %+v", result)
	}

	assertCatalogEventPending(t, db, rec.ID, 1, "forced update failure")

	result, err = processor.ProcessPending(ctx, 10)
	if err != nil {
		t.Fatalf("ProcessPending retry: %v", err)
	}

	if result.Read != 1 || result.Applied != 1 || result.Failed != 0 || result.Retryable != 0 {
		t.Fatalf("unexpected retry process result: %+v", result)
	}

	assertCatalogEventApplied(t, db, rec.ID, 2)
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

func assertCatalogEventPending(t *testing.T, db *sql.DB, eventID int64, wantAttempts int, wantError string) {
	t.Helper()

	var status string
	var attempts int
	var lastError sql.NullString
	if err := db.QueryRow("SELECT status, attempts, last_error FROM cell_catalog_events WHERE id = ?", eventID).Scan(&status, &attempts, &lastError); err != nil {
		t.Fatalf("query catalog event: %v", err)
	}

	if status != dal.CatalogEventStatusPending {
		t.Fatalf("status: got %q, want %q", status, dal.CatalogEventStatusPending)
	}

	if attempts != wantAttempts {
		t.Fatalf("attempts: got %d, want %d", attempts, wantAttempts)
	}

	if !lastError.Valid || !strings.Contains(lastError.String, wantError) {
		t.Fatalf("last_error: got %+v, want substring %q", lastError, wantError)
	}
}

func assertCatalogEventApplied(t *testing.T, db *sql.DB, eventID int64, wantAttempts int) {
	t.Helper()

	var status string
	var attempts int
	var lastError sql.NullString
	var appliedAt sql.NullInt64
	if err := db.QueryRow("SELECT status, attempts, last_error, applied_at FROM cell_catalog_events WHERE id = ?", eventID).Scan(&status, &attempts, &lastError, &appliedAt); err != nil {
		t.Fatalf("query catalog event: %v", err)
	}

	if status != dal.CatalogEventStatusApplied {
		t.Fatalf("status: got %q, want %q", status, dal.CatalogEventStatusApplied)
	}

	if attempts != wantAttempts {
		t.Fatalf("attempts: got %d, want %d", attempts, wantAttempts)
	}

	if lastError.Valid {
		t.Fatalf("last_error should be cleared after apply, got %+v", lastError)
	}

	if !appliedAt.Valid {
		t.Fatal("applied_at should be set after apply")
	}
}
