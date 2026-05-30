package cellingress

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestExecutionRepairService_ProcessReenqueuesAcceptedExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()
	req := validJobRequestForCell(t, "iad-a")

	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	acceptance, err := executionAcceptance(submission)
	if err != nil {
		t.Fatalf("executionAcceptance: %v", err)
	}

	if _, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance); err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	svc := NewExecutionRepairService(repos.CellExecutionAcceptances(), queue, mocks.NewMockLogger(), clock)
	svc.SetMinAttemptGap(0)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued requests: got %d, want 1", len(reqs))
	}

	if reqs[0].GetJob().GetRunId() != "run-1" {
		t.Fatalf("queued run id: got %q, want run-1", reqs[0].GetJob().GetRunId())
	}

	if reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey] == "" {
		t.Fatal("repair enqueue should preserve execution envelope metadata")
	}

	assertReceiptEnqueued(t, db, acceptance.ExecutionID)
}

func TestExecutionRepairService_ProcessThrottlesFailedHandoffAndRetriesLater(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()
	req := validJobRequestForCell(t, "iad-a")

	submission, err := cell.NewExecutionSubmission(req)
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	acceptance, err := executionAcceptance(submission)
	if err != nil {
		t.Fatalf("executionAcceptance: %v", err)
	}

	if _, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance); err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue closed"))
	clock := mocks.NewMockClock()
	start := clock.Now()
	svc := NewExecutionRepairService(repos.CellExecutionAcceptances(), queue, mocks.NewMockLogger(), clock)
	svc.SetMinAttemptGap(time.Minute)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process with queue failure: %v", err)
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("expected no queued request while queue fails, got %d", got)
	}

	queue.SetEnqueueError(nil)
	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process before retry gap: %v", err)
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("expected failed handoff to be throttled, got %d queued requests", got)
	}

	clock.SetNow(start.Add(time.Minute + time.Second))
	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process after retry gap: %v", err)
	}

	if got := len(queue.GetJobRequests()); got != 1 {
		t.Fatalf("expected repair retry to enqueue once, got %d queued requests", got)
	}

	var attempts int
	var lastErr sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT enqueue_attempts, last_enqueue_error
		FROM cell_execution_acceptances
		WHERE execution_id = ?
	`, acceptance.ExecutionID).Scan(&attempts, &lastErr); err != nil {
		t.Fatalf("query receipt: %v", err)
	}

	if attempts != 2 || lastErr.Valid {
		t.Fatalf("unexpected receipt after retry: attempts=%d last_err=%v", attempts, lastErr)
	}
}

func assertReceiptEnqueued(t *testing.T, db *sql.DB, executionID string) {
	t.Helper()

	var enqueuedAt sql.NullInt64
	if err := db.QueryRow("SELECT enqueued_at FROM cell_execution_acceptances WHERE execution_id = ?", executionID).Scan(&enqueuedAt); err != nil {
		t.Fatalf("query enqueued_at: %v", err)
	}

	if !enqueuedAt.Valid || enqueuedAt.Int64 == 0 {
		t.Fatalf("expected receipt %s to be marked enqueued, got %v", executionID, enqueuedAt)
	}
}
