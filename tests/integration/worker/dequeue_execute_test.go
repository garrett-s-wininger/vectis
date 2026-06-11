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
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpcservices"
	"vectis/internal/testutil/workertest"
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

	// Run one worker iteration.
	logger := mocks.NewMockLogger()
	w := &workertest.Runner{
		Logger:    logger,
		WorkerID:  "test-worker-1",
		Queue:     queueClient,
		LogClient: logClient,
		Executor:  job.NewExecutor(),
		Store:     repos.Runs(),
	}

	runCtx, cancelRun := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRun()
	if _, err := w.RunOne(runCtx); err != nil {
		t.Fatalf("worker run: %v", err)
	}

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
	w := &workertest.Runner{
		Logger:         logger,
		WorkerID:       "test-worker-custom-action",
		Queue:          queueClient,
		LogClient:      logClient,
		Executor:       job.NewExecutor(),
		Store:          repos.Runs(),
		ActionResolver: resolver,
	}

	runCtx, cancelRun := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRun()
	if _, err := w.RunOne(runCtx); err != nil {
		t.Fatalf("worker run: %v", err)
	}

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
