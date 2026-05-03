//go:build integration

package worker_test

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/observability"
	"vectis/internal/queue"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func TestIntegrationWorker_DequeueClaimExecuteFinalize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Database setup.
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	// Insert a stored job.
	jobID := "integration-worker-job"
	defJSON := `{"id":"integration-worker-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hello-from-worker"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defJSON, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Create a run in queued status.
	runID, runIndex, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	queueServer := grpctest.StartServer(t, func(srv *grpc.Server) {
		queueSvc := queue.NewQueueService(mocks.NewMockLogger())
		api.RegisterQueueServiceServer(srv, queueSvc)
	})
	queueClient := interfaces.NewGRPCQueueClient(queueServer.Conn)

	logStore, _ := logserver.NewLocalRunLogStore(t.TempDir())
	logServer := grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterLogServiceServer(srv, logserver.NewServerWithStore(mocks.NewMockLogger(), logStore))
	})
	logClient := interfaces.NewGRPCLogClient(logServer.Conn)

	// Enqueue the job with run_id.
	rootID := "root"
	uses := "builtins/shell"
	enqueueJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &uses,
			With: map[string]string{"command": "echo hello-from-worker"},
		},
	}

	if err := queueClient.Enqueue(ctx, &api.JobRequest{Job: enqueueJob}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Create and run worker.
	logger := mocks.NewMockLogger()
	workerMetrics, _ := observability.NewWorkerMetrics()
	w := &worker{
		ctx:           ctx,
		runCtx:        context.Background(),
		logger:        logger,
		workerID:      "test-worker-1",
		clock:         interfaces.SystemClock{},
		renewInterval: dal.DefaultRenewInterval,
		queue:         queueClient,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         repos.Runs(),
		metrics:       workerMetrics,
	}

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		j, keepGoing := w.dequeueNext()
		if !keepGoing || j == nil {
			return
		}

		w.handleJob(j)
	}()

	// Wait for run to complete.
	deadline := time.Now().Add(10 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		status, found, err := repos.Runs().GetRunStatus(ctx, runID)
		if err != nil {
			t.Fatalf("get run status: %v", err)
		}

		if found && (status == "succeeded" || status == "failed") {
			finalStatus = status
			break
		}

		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	<-workerDone

	if finalStatus != "succeeded" {
		t.Fatalf("expected run status succeeded, got %q", finalStatus)
	}

	// Verify run record details.
	queryCtx := context.Background()
	recs, _, err := repos.Runs().ListByJob(queryCtx, jobID, nil, 0, 100)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	if len(recs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(recs))
	}

	if recs[0].RunID != runID {
		t.Fatalf("expected run_id %s, got %s", runID, recs[0].RunID)
	}

	if recs[0].RunIndex != runIndex {
		t.Fatalf("expected run_index %d, got %d", runIndex, recs[0].RunIndex)
	}

	t.Logf("Worker completed run %s successfully", runID)
}

// Minimal worker struct mirroring cmd/worker for test control.
type worker struct {
	ctx           context.Context
	runCtx        context.Context
	logger        interfaces.Logger
	workerID      string
	clock         interfaces.Clock
	renewInterval time.Duration
	queue         interfaces.QueueClient
	logClient     interfaces.LogClient
	executor      *job.Executor
	store         dal.RunsRepository
	metrics       *observability.WorkerMetrics
}

func (w *worker) dequeueNext() (*api.Job, bool) {
	pollCtx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	req, err := w.queue.Dequeue(pollCtx)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, false
		}

		w.logger.Error("dequeue error: %v", err)
		return nil, true
	}

	if req == nil || req.GetJob() == nil {
		return nil, true
	}

	return req.GetJob(), true
}

func (w *worker) handleJob(job *api.Job) {
	jobID := job.GetId()
	runID := job.GetRunId()
	deliveryID := job.GetDeliveryId()
	w.logger.Info("Dequeued job: %s (run %s)", jobID, runID)

	if runID == "" {
		w.logger.Error("Job has no run_id")
		_ = w.queue.Ack(w.runCtx, deliveryID)
		return
	}

	leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)
	claimed, claimToken, err := w.store.TryClaim(w.runCtx, runID, w.workerID, leaseUntil)
	if err != nil {
		w.logger.Error("TryClaim %s: %v", runID, err)
		return
	}

	if !claimed {
		w.logger.Debug("Run %s not claimed", runID)
		_ = w.queue.Ack(w.runCtx, deliveryID)
		return
	}

	if err := w.queue.Ack(w.runCtx, deliveryID); err != nil {
		w.logger.Error("Ack failed: %v", err)
		_ = w.store.MarkRunOrphaned(w.runCtx, runID, claimToken, "ack_uncertain")
		return
	}

	execErr := w.executor.ExecuteJob(w.runCtx, job, w.logClient, w.logger)
	if execErr != nil {
		w.logger.Error("Job %s failed: %v", jobID, execErr)
		_ = w.store.MarkRunFailed(w.runCtx, runID, claimToken, "execution_error", execErr.Error())
		return
	}

	if err := w.store.MarkRunSucceeded(w.runCtx, runID, claimToken); err != nil {
		w.logger.Error("MarkRunSucceeded failed: %v", err)
		return
	}

	w.logger.Info("Job completed successfully: %s", jobID)
}
