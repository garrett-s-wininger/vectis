//go:build integration

package worker_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/observability"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpcservices"
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

	_, queueClient, _ := grpcservices.StartQueueServer(t, mocks.NewMockLogger())

	logStore, _ := logserver.NewLocalRunLogStore(t.TempDir())
	_, logClient := grpcservices.StartLogServer(t, mocks.NewMockLogger(), logStore)

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

	req := &api.JobRequest{Job: enqueueJob}
	if _, err := cell.AttachPendingExecutionEnvelope(ctx, repos.Runs(), req, runID, time.Now().UnixNano()); err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	if err := queueClient.Enqueue(ctx, req); err != nil {
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
		req, keepGoing := w.dequeueNext()
		if !keepGoing || req == nil {
			return
		}

		w.handleJob(req)
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
	recs, _, err := repos.Runs().ListByJob(queryCtx, jobID, nil, nil, "", 0, 100)
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

func TestIntegrationWorker_ExecutesCustomActionFromEnvelopeLocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	jobID := "integration-custom-action-job"
	defJSON := `{"id":"integration-custom-action-job","root":{"id":"root","uses":"examples/greet@v1","with":{"name":"Vectis"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defJSON, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	_, queueClient, _ := grpcservices.StartQueueServer(t, mocks.NewMockLogger())

	logStore, _ := logserver.NewLocalRunLogStore(t.TempDir())
	_, logClient := grpcservices.StartLogServer(t, mocks.NewMockLogger(), logStore)

	rootID := "root"
	uses := "examples/greet@v1"
	enqueueJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &uses,
			With: map[string]string{"name": "Vectis"},
		},
	}

	resolver := descriptorResolver{"examples/greet@v1": greetDescriptor()}
	req := &api.JobRequest{Job: enqueueJob}
	env, err := cell.AttachPendingExecutionEnvelopeWithActions(ctx, repos.Runs(), req, runID, time.Now().UnixNano(), resolver)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}
	if len(env.ActionLocks) != 1 || env.ActionLocks[0].Descriptor.Digest != greetDescriptor().Digest {
		t.Fatalf("expected custom action lock, got %+v", env.ActionLocks)
	}

	if err := queueClient.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	logger := mocks.NewMockLogger()
	workerMetrics, _ := observability.NewWorkerMetrics()
	w := &worker{
		ctx:            ctx,
		runCtx:         context.Background(),
		logger:         logger,
		workerID:       "test-worker-custom-action",
		clock:          interfaces.SystemClock{},
		renewInterval:  dal.DefaultRenewInterval,
		queue:          queueClient,
		logClient:      logClient,
		executor:       job.NewExecutor(),
		store:          repos.Runs(),
		metrics:        workerMetrics,
		actionResolver: resolver,
	}

	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		req, keepGoing := w.dequeueNext()
		if !keepGoing || req == nil {
			return
		}

		w.handleJob(req)
	}()

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

	entries, err := logStore.List(runID)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if !logEntriesContain(entries, "Hello, Vectis") {
		t.Fatalf("expected custom action log output, got %+v", entries)
	}
}

// Minimal worker struct mirroring cmd/worker for test control.
type worker struct {
	ctx            context.Context
	runCtx         context.Context
	logger         interfaces.Logger
	workerID       string
	clock          interfaces.Clock
	renewInterval  time.Duration
	queue          interfaces.QueueClient
	logClient      interfaces.LogClient
	executor       *job.Executor
	store          dal.RunsRepository
	metrics        *observability.WorkerMetrics
	actionResolver actionregistry.Resolver
}

func (w *worker) dequeueNext() (*api.JobRequest, bool) {
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

	return req, true
}

func (w *worker) handleJob(req *api.JobRequest) {
	work := req.GetJob()
	jobID := work.GetId()
	runID := work.GetRunId()
	deliveryID := work.GetDeliveryId()
	w.logger.Info("Dequeued job: %s (run %s)", jobID, runID)

	if runID == "" {
		w.logger.Error("Job has no run_id")
		_ = w.queue.Ack(w.runCtx, deliveryID)
		return
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil || !ok {
		w.logger.Error("Invalid execution envelope for run %s: %v", runID, err)
		_ = w.queue.Ack(w.runCtx, deliveryID)
		_ = w.store.MarkRunFailed(w.runCtx, runID, dal.FailureCodeInvalidEnvelope, "missing or invalid execution envelope for persisted run")
		return
	}

	if err := w.queue.Ack(w.runCtx, deliveryID); err != nil {
		w.logger.Error("Ack failed: %v", err)
		return
	}

	executionClaim, err := w.store.TryClaimExecution(w.runCtx, env.ExecutionID, w.workerID, time.Now().Add(dal.DefaultLeaseTTL))
	if err != nil {
		w.logger.Error("TryClaimExecution %s: %v", env.ExecutionID, err)
		_ = w.store.MarkRunOrphaned(w.runCtx, runID, "ack_uncertain")
		return
	}

	if !executionClaim.Claimed {
		w.logger.Error("Execution %s not claimed", env.ExecutionID)
		return
	}

	_ = w.store.MarkExecutionStarted(w.runCtx, env.ExecutionID)

	execErr := w.executor.ExecuteTaskWithOptions(w.runCtx, work, env.TaskKey, w.logClient, w.logger, job.ExecuteOptions{
		ActionResolver: w.actionResolver,
		ActionLocks:    env.ActionLocks,
	})
	if execErr != nil {
		w.logger.Error("Job %s failed: %v", jobID, execErr)
		_, _ = w.store.CompleteExecutionAndFinalizeRunByClaim(w.runCtx, env.ExecutionID, w.workerID, executionClaim.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, execErr.Error())
		return
	}

	finalized, err := w.store.CompleteExecutionAndFinalizeRunByClaim(w.runCtx, env.ExecutionID, w.workerID, executionClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		w.logger.Error("CompleteExecutionAndFinalizeRunByClaim failed: %v", err)
		return
	}

	if finalized.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		w.logger.Error("CompleteExecutionAndFinalizeRunByClaim outcome %q", finalized.Outcome)
		return
	}

	w.logger.Info("Job completed successfully: %s", jobID)
}

type descriptorResolver map[string]actionregistry.Descriptor

func (r descriptorResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r[uses]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func greetDescriptor() actionregistry.Descriptor {
	return actionregistry.Descriptor{
		CanonicalName: "examples/greet",
		DisplayName:   "Greet",
		Version:       "v1",
		Digest:        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		Source:        actionregistry.SourceLocalFilesystem,
		Runtime:       actionregistry.RuntimeProcess,
		RuntimeConfig: map[string]string{
			"command": "echo \"Hello, ${VECTIS_INPUT_NAME}\"",
		},
		InputSchema: actionregistry.InputSchema{
			Fields: []actionregistry.InputField{{
				Name:     "name",
				Type:     action.FieldString,
				Required: true,
			}},
		},
		Capabilities: []actionregistry.Capability{actionregistry.CapabilityProcessLaunch},
	}
}

func logEntriesContain(entries []logserver.LogEntry, needle string) bool {
	for _, entry := range entries {
		if strings.Contains(entry.Data, needle) {
			return true
		}
	}

	return false
}
