//go:build integration

package reconciler_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/reconciler"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpcservices"
)

func TestIntegrationReconciler_RedispatchesQueuedRun(t *testing.T) {
	ctx := context.Background()

	// Database setup.
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	// Insert a definition snapshot.
	jobID := "integration-reconciler-job"
	defJSON := `{"id":"integration-reconciler-job","root":{"id":"root","uses":"builtins/script","with":{"script":"echo hello"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defJSON); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Create a run in queued status with no dispatch timestamp.
	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Verify run is queued.
	status, found, err := repos.Runs().GetRunStatus(ctx, runID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}
	if !found || status != "queued" {
		t.Fatalf("expected run to be queued, got status=%q found=%v", status, found)
	}

	_, queueClient, queueService := grpcservices.StartQueueServer(t, mocks.NewMockLogger())

	// Create reconciler with very short gap for testing.
	logger := mocks.NewMockLogger()
	rec := reconciler.NewServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), queueService, interfaces.SystemClock{})
	rec.SetMinDispatchGap(1 * time.Millisecond)

	// Run reconciler.
	if err := rec.Process(ctx); err != nil {
		t.Fatalf("reconciler process: %v", err)
	}

	// Verify the job was enqueued by dequeuing it.
	dequeuedJob, err := queueClient.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue after reconciler: %v", err)
	}

	if dequeuedJob == nil {
		t.Fatal("expected job to be enqueued by reconciler, but queue is empty")
	}

	if dequeuedJob.GetJob().GetId() != jobID {
		t.Fatalf("expected job id %q, got %q", jobID, dequeuedJob.GetJob().GetId())
	}

	if dequeuedJob.GetJob().GetRunId() != runID {
		t.Fatalf("expected run id %q, got %q", runID, dequeuedJob.GetJob().GetRunId())
	}

	// Verify last_dispatched_at was updated.
	queuedRuns, err := repos.Runs().ListQueuedBeforeDispatchCutoff(ctx, time.Now().Add(-1*time.Hour).Unix())
	if err != nil {
		t.Fatalf("list queued before cutoff: %v", err)
	}

	for _, qr := range queuedRuns {
		if qr.RunID == runID {
			t.Fatal("expected run to have been touched (last_dispatched_at updated)")
		}
	}

	t.Logf("Reconciler re-enqueued run %s successfully", runID)
}

func TestIntegrationReconciler_OrphansExpiredLease(t *testing.T) {
	ctx := context.Background()

	// Database setup.
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	// Insert a definition snapshot.
	jobID := "integration-reconciler-orphan-job"
	defJSON := `{"id":"integration-reconciler-orphan-job","root":{"id":"root","uses":"builtins/script","with":{"script":"echo hello"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defJSON); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Create a run and claim its pending execution with an expired lease.
	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	expiredLease := time.Now().Add(-1 * time.Hour)
	claim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "test-worker", expiredLease)
	if err != nil {
		t.Fatalf("try claim execution: %v", err)
	}

	if !claim.Claimed {
		t.Fatal("expected execution claim to succeed")
	}

	_, _, queueService := grpcservices.StartQueueServer(t, mocks.NewMockLogger())

	// Create reconciler.
	logger := mocks.NewMockLogger()
	rec := reconciler.NewServiceWithRepositories(logger, repos.Jobs(), repos.Runs(), queueService, interfaces.SystemClock{})
	rec.SetMinDispatchGap(1 * time.Millisecond)

	// Run reconciler.
	if err := rec.Process(ctx); err != nil {
		t.Fatalf("reconciler process: %v", err)
	}

	// Verify run was orphaned.
	status, found, err := repos.Runs().GetRunStatus(ctx, runID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found {
		t.Fatal("expected run to still exist")
	}

	if status != "orphaned" {
		t.Fatalf("expected run status orphaned, got %q", status)
	}

	t.Logf("Reconciler orphaned expired run %s successfully", runID)
}
