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

func TestExecutionRepairService_ProcessMarkEnqueuedFailureRetriesHandoff(t *testing.T) {
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

	baseAcceptances := repos.CellExecutionAcceptances()
	if _, err := baseAcceptances.AcceptExecution(ctx, acceptance); err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	acceptances := &failOnceMarkEnqueuedAcceptancesRepository{
		CellExecutionAcceptancesRepository: baseAcceptances,
		err:                                errors.New("database unavailable after queue handoff"),
	}

	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	svc := NewExecutionRepairService(acceptances, queue, mocks.NewMockLogger(), clock)
	svc.SetMinAttemptGap(0)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process with injected mark failure: %v", err)
	}

	firstReqs := queue.GetJobRequests()
	if len(firstReqs) != 1 {
		t.Fatalf("expected first repair pass to enqueue once, got %d", len(firstReqs))
	}

	firstEnvelope := firstReqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if firstEnvelope == "" {
		t.Fatal("first repair handoff should include execution envelope")
	}

	assertReceiptPending(t, db, acceptance.ExecutionID)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process after marker recovery: %v", err)
	}

	secondReqs := queue.GetJobRequests()
	if len(secondReqs) != 2 {
		t.Fatalf("expected marker retry to enqueue duplicate handoff, got %d", len(secondReqs))
	}

	if secondReqs[1].GetJob().GetRunId() != acceptance.RunID {
		t.Fatalf("retry enqueued run_id = %q, want %q", secondReqs[1].GetJob().GetRunId(), acceptance.RunID)
	}

	if got := secondReqs[1].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]; got != firstEnvelope {
		t.Fatalf("repair retry changed execution envelope:\nfirst:  %s\nsecond: %s", firstEnvelope, got)
	}

	assertReceiptEnqueued(t, db, acceptance.ExecutionID)
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

func assertReceiptPending(t *testing.T, db *sql.DB, executionID string) {
	t.Helper()

	var enqueuedAt sql.NullInt64
	if err := db.QueryRow("SELECT enqueued_at FROM cell_execution_acceptances WHERE execution_id = ?", executionID).Scan(&enqueuedAt); err != nil {
		t.Fatalf("query enqueued_at: %v", err)
	}

	if enqueuedAt.Valid {
		t.Fatalf("expected receipt %s to remain pending, got enqueued_at=%v", executionID, enqueuedAt)
	}
}

type failOnceMarkEnqueuedAcceptancesRepository struct {
	dal.CellExecutionAcceptancesRepository
	err    error
	failed bool
}

func (r *failOnceMarkEnqueuedAcceptancesRepository) MarkEnqueued(ctx context.Context, executionID string, enqueuedAtUnixNano int64) error {
	if !r.failed {
		r.failed = true
		return r.err
	}

	return r.CellExecutionAcceptancesRepository.MarkEnqueued(ctx, executionID, enqueuedAtUnixNano)
}
