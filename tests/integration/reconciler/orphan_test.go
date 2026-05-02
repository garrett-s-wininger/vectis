//go:build integration

package reconciler_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queue"
	"vectis/internal/reconciler"
	"vectis/internal/testutil/dbtest"
)

func TestIntegrationReconciler_RedispatchesQueuedRun(t *testing.T) {
	ctx := context.Background()

	// Database setup.
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	// Insert a stored job.
	jobID := "integration-reconciler-job"
	defJSON := `{"id":"integration-reconciler-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hello"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defJSON, 1); err != nil {
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

	// Start queue server.
	queueLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen queue: %v", err)
	}

	queueServer := grpc.NewServer()
	queueSvc := queue.NewQueueService(mocks.NewMockLogger())
	api.RegisterQueueServiceServer(queueServer, queueSvc)
	go queueServer.Serve(queueLis)
	defer queueServer.Stop()

	queueConn, err := grpc.NewClient(queueLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial queue: %v", err)
	}
	defer queueConn.Close()

	queueClient := interfaces.NewGRPCQueueClient(queueConn)
	queueService := interfaces.NewQueueService(api.NewQueueServiceClient(queueConn))

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

	// Insert a stored job.
	jobID := "integration-reconciler-orphan-job"
	defJSON := `{"id":"integration-reconciler-orphan-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hello"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defJSON, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Create a run and claim it with an expired lease.
	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	// Manually claim with an expired lease.
	expiredLease := time.Now().Add(-1 * time.Hour)
	claimed, _, err := repos.Runs().TryClaim(ctx, runID, "test-worker", expiredLease)
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}

	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	// Start queue server (reconciler needs it even though we won't check queue).
	queueLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen queue: %v", err)
	}

	queueServer := grpc.NewServer()
	queueSvc := queue.NewQueueService(mocks.NewMockLogger())
	api.RegisterQueueServiceServer(queueServer, queueSvc)
	go queueServer.Serve(queueLis)
	defer queueServer.Stop()

	queueConn, err := grpc.NewClient(queueLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial queue: %v", err)
	}
	defer queueConn.Close()

	queueService := interfaces.NewQueueService(api.NewQueueServiceClient(queueConn))

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
