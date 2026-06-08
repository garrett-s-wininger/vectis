package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/observability"
	"vectis/internal/taskdispatch"
	"vectis/internal/taskfinalize"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/runfixture"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func attachPendingExecutionEnvelopeForTest(t *testing.T, runs dal.RunsRepository, j *api.Job, runID string) *cell.ExecutionEnvelope {
	t.Helper()

	dispatch, err := runs.GetPendingExecution(context.Background(), runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.DefinitionHash == "" {
		dispatch.DefinitionHash = "test-definition-hash"
	}

	env, err := cell.AttachExecutionEnvelope(&api.JobRequest{Job: j}, dispatch, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	return env
}

func spanAttributeString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}

	return ""
}

func assertTaskFinalizeOutcome(t *testing.T, recorder *tracetest.SpanRecorder, want taskfinalize.Outcome) {
	t.Helper()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	for _, event := range spans[0].Events() {
		if event.Name != "task.finalize" {
			continue
		}

		if got := spanAttributeString(event.Attributes, "vectis.task.finalize.outcome"); got != string(want) {
			t.Fatalf("task finalize outcome: got %q, want %q", got, want)
		}
		return
	}

	t.Fatalf("task.finalize event missing: %+v", spans[0].Events())
}

type flakyFinalizeRunsStore struct {
	dal.RunsRepository

	mu                  sync.Mutex
	renewFailuresLeft   int
	succeedFailuresLeft int
	failedFailuresLeft  int
	orphanFailuresLeft  int
}

type recordingExecutionClaimStore struct {
	dal.RunsRepository

	mu                   sync.Mutex
	claimedExecutions    []string
	renewedExecutions    []string
	executionClaimTokens []string
}

func (s *recordingExecutionClaimStore) TryClaimExecution(ctx context.Context, executionID, owner string, leaseUntil time.Time) (bool, string, error) {
	claimed, token, err := s.RunsRepository.TryClaimExecution(ctx, executionID, owner, leaseUntil)

	s.mu.Lock()
	s.claimedExecutions = append(s.claimedExecutions, executionID)
	if token != "" {
		s.executionClaimTokens = append(s.executionClaimTokens, token)
	}
	s.mu.Unlock()

	return claimed, token, err
}

func (s *recordingExecutionClaimStore) RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	err := s.RunsRepository.RenewExecutionLease(ctx, executionID, owner, claimToken, leaseUntil)
	s.mu.Lock()
	s.renewedExecutions = append(s.renewedExecutions, executionID)
	s.mu.Unlock()

	return err
}

func (s *recordingExecutionClaimStore) executionClaimCounts() (claimed, renewed int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.claimedExecutions), len(s.renewedExecutions)
}

func (s *flakyFinalizeRunsStore) RenewLease(ctx context.Context, runID, owner, claimToken string, leaseUntil time.Time) error {
	s.mu.Lock()
	if s.renewFailuresLeft > 0 {
		s.renewFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("renew: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.RenewLease(ctx, runID, owner, claimToken, leaseUntil)
}

func (s *flakyFinalizeRunsStore) MarkRunFailed(ctx context.Context, runID, claimToken, failureCode, reason string) error {
	s.mu.Lock()
	if s.failedFailuresLeft > 0 {
		s.failedFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("finalize failed: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.MarkRunFailed(ctx, runID, claimToken, failureCode, reason)
}

func (s *flakyFinalizeRunsStore) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	s.mu.Lock()
	switch status {
	case dal.ExecutionStatusSucceeded:
		if s.succeedFailuresLeft > 0 {
			s.succeedFailuresLeft--
			s.mu.Unlock()
			return dal.ExecutionFinalizationResult{}, fmt.Errorf("finalize success: %w", sql.ErrConnDone)
		}
	case dal.ExecutionStatusFailed:
		if s.failedFailuresLeft > 0 {
			s.failedFailuresLeft--
			s.mu.Unlock()
			return dal.ExecutionFinalizationResult{}, fmt.Errorf("finalize failed: %w", sql.ErrConnDone)
		}
	}
	s.mu.Unlock()

	return s.RunsRepository.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claimToken, status, failureCode, reason)
}

func (s *flakyFinalizeRunsStore) MarkRunOrphaned(ctx context.Context, runID, claimToken, reason string) error {
	s.mu.Lock()
	if s.orphanFailuresLeft > 0 {
		s.orphanFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("finalize orphan: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.MarkRunOrphaned(ctx, runID, claimToken, reason)
}

type blockingSuccessStore struct {
	dal.RunsRepository

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSuccessStore) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	if status == dal.ExecutionStatusSucceeded {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}

	return s.RunsRepository.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claimToken, status, failureCode, reason)
}

func TestLeaseRenewalLoop_ReclaimsOrphanedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-reclaim", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-1"
	claimToken := "claim-test-1"
	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = ?, claim_token = ?, lease_until = ?
		WHERE run_id = ?
	`, workerID, claimToken, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed orphaned run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		store:         runs,
		renewInterval: 5 * time.Millisecond,
	}

	execCtx := t.Context()

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	go w.leaseRenewalLoop(execCtx, runID, claimToken, nil, "", stopRenew, doneRenew)

	time.Sleep(30 * time.Millisecond)
	close(stopRenew)
	<-doneRenew

	var status string
	var leaseUntil int64
	if err := db.QueryRowContext(ctx, `SELECT status, lease_until FROM job_runs WHERE run_id = ?`, runID).Scan(&status, &leaseUntil); err != nil {
		t.Fatalf("query run state: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected run status running after renew, got %q", status)
	}

	if leaseUntil <= time.Now().Unix() {
		t.Fatalf("expected lease_until to be renewed into the future, got %d", leaseUntil)
	}
}

func TestWorkerDBUnavailableSignals_LogOutageAndRecoveryOnce(t *testing.T) {
	logger := mocks.NewMockLogger()
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}
	w := &worker{
		ctx:     context.Background(),
		logger:  logger,
		metrics: workerMetrics,
	}

	w.noteDBError(errors.New("database is closed"))
	w.noteDBError(errors.New("database is closed"))

	warns := logger.GetWarnCalls()
	if len(warns) != 1 {
		t.Fatalf("expected a single outage warning, got %d (%v)", len(warns), warns)
	}

	if !workerMetrics.DBUnavailable() {
		t.Fatal("expected worker metrics to report database unavailable")
	}

	w.noteDBRecovered()
	if workerMetrics.DBUnavailable() {
		t.Fatal("expected worker metrics to clear database unavailable after recovery")
	}

	infos := logger.GetInfoCalls()
	foundRecovery := false
	for _, msg := range infos {
		if strings.Contains(msg, "Database connectivity recovered; DB-backed run transitions resumed") {
			foundRecovery = true
			break
		}
	}

	if !foundRecovery {
		t.Fatalf("expected recovery info log, got %v", infos)
	}
}

func TestWorkerRunClaimedJob_CompletesWhileOrphaned_MarksSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finish-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-2"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		cellID:        "local",
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	jobID := "job-worker-finish-orphaned"
	deliveryID := "delivery-orphaned-finish"
	commandNodeID := "node-1"
	command := "sleep 0.08"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	done := make(chan struct{})
	go func() {
		w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
			t.Fatalf("query run status: %v", err)
		}

		if status == "running" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run to reach running, last status=%q", status)
		}

		time.Sleep(5 * time.Millisecond)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = ?, lease_until = ?
		WHERE run_id = ?
	`, workerID, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("mark run orphaned during execution: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker runClaimedJob")
	}

	var status string
	var leaseOwner any
	var leaseUntil any
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_until
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &leaseOwner, &leaseUntil); err != nil {
		t.Fatalf("query final run state: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected run to succeed after orphaned mid-flight, got %q", status)
	}

	if leaseOwner != nil || leaseUntil != nil {
		t.Fatalf("expected lease fields cleared on success, got lease_owner=%v lease_until=%v", leaseOwner, leaseUntil)
	}
}

func TestWorkerRunClaimedJob_WithExecutionEnvelope_TransitionsExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-envelope", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-envelope"
	def := `{"id":"job-worker-envelope","root":{"uses":"builtins/shell","with":{"command":"echo envelope"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	workerID := "worker-test-envelope"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		cellID:        "local",
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-envelope"
	commandNodeID := "node-1"
	command := "echo envelope"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}
	w.handleJob(req)

	var executionStatus string
	var segmentStatus string
	var eventSequence int64
	var acceptedAt, startedAt, finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT se.status, rs.status, se.accepted_at, se.started_at, se.finished_at, se.event_sequence
		FROM segment_executions se
		JOIN run_segments rs ON rs.segment_id = se.segment_id
		WHERE se.execution_id = ?
	`, env.ExecutionID).Scan(&executionStatus, &segmentStatus, &acceptedAt, &startedAt, &finishedAt, &eventSequence); err != nil {
		t.Fatalf("query execution state: %v", err)
	}

	if executionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusSucceeded)
	}

	if segmentStatus != dal.SegmentStatusSucceeded {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, dal.SegmentStatusSucceeded)
	}

	if eventSequence != 3 {
		t.Fatalf("event sequence: got %d, want 3", eventSequence)
	}

	if !acceptedAt.Valid || !startedAt.Valid || !finishedAt.Valid {
		t.Fatalf("expected accepted_at, started_at, and finished_at to be set; got accepted=%v started=%v finished=%v", acceptedAt, startedAt, finishedAt)
	}

	events, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list catalog events: %v", err)
	}

	wantEvents := []struct {
		key       string
		eventType string
	}{
		{key: cell.CatalogRunStatusEventKey(runID, dal.RunStatusRunning), eventType: cell.CatalogEventTypeRunStatus},
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusAccepted), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusRunning), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusSucceeded), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogRunStatusEventKey(runID, dal.RunStatusSucceeded), eventType: cell.CatalogEventTypeRunStatus},
	}

	if len(events) != len(wantEvents) {
		t.Fatalf("catalog events: got %d, want %d (%+v)", len(events), len(wantEvents), events)
	}

	for i, want := range wantEvents {
		if events[i].SourceCell != "local" || events[i].EventKey != want.key || events[i].EventType != want.eventType {
			t.Fatalf("catalog event %d: got source=%q key=%q type=%q, want source=local key=%q type=%q",
				i, events[i].SourceCell, events[i].EventKey, events[i].EventType, want.key, want.eventType)
		}
	}
}

func TestWorkerRunClaimedJob_TaskFanoutQueuesContinuation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-task-fanout", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-fanout"
	def := `{"id":"job-worker-task-fanout","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	child, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "local",
	})
	if err != nil {
		t.Fatalf("ensure child task: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	clock := mocks.NewMockClock()
	taskDispatcher := taskdispatch.New(runs, repos.TaskDispatchIntents(), repos.DispatchEvents(), cell.NewQueueExecutionIngress(queueClientServiceAdapter{queue: queue}, interfaces.NewLogger("worker-test")), clock)
	w := &worker{
		ctx:                 context.Background(),
		runCtx:              context.Background(),
		logger:              interfaces.NewLogger("worker-test"),
		workerID:            "worker-task-fanout",
		cellID:              "local",
		clock:               clock,
		renewInterval:       time.Hour,
		queue:               queue,
		logClient:           mocks.NewMockLogClient(),
		executor:            job.NewExecutor(),
		store:               runs,
		catalog:             cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		taskDispatchService: taskdispatch.NewService(interfaces.NewLogger("worker-test"), taskDispatcher),
	}

	deliveryID := "delivery-task-fanout"
	rootID := "root"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"command": "echo root"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-a"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal root payload: %v", err)
	}

	if _, _, err := runs.RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(def)); err != nil {
		t.Fatalf("record execution payload: %v", err)
	}

	w.handleJob(req)

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusQueued {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusQueued)
	}

	var rootExecutionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, rootDispatch.ExecutionID).Scan(&rootExecutionStatus); err != nil {
		t.Fatalf("query root execution status: %v", err)
	}

	if rootExecutionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("root execution status: got %q, want %q", rootExecutionStatus, dal.ExecutionStatusSucceeded)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued continuation requests: got %d, want 1", len(reqs))
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(reqs[0])
	if err != nil {
		t.Fatalf("queued child envelope: %v", err)
	}

	if !ok {
		t.Fatal("queued continuation missing execution envelope")
	}

	if env.ExecutionID != child.ExecutionID || env.TaskID != child.TaskID || env.TaskKey != child.TaskKey {
		t.Fatalf("queued child envelope mismatch: got %+v want child %+v", env, child)
	}

	if env.Metadata["traceparent"] != "trace-a" {
		t.Fatalf("queued child trace metadata: got %q, want trace-a", env.Metadata["traceparent"])
	}

	pending, err := repos.TaskDispatchIntents().ListPending(ctx, "local", clock.Now().UnixNano(), 10)
	if err != nil {
		t.Fatalf("list pending intents after continuation: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("dispatched child intent should not remain pending: %+v", pending)
	}
}

func TestWorkerRunClaimedJob_TaskFanoutWaitingReductionRequeuesRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-task-reduce-waiting", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-reduce-waiting"
	def := `{"id":"job-worker-task-reduce-waiting","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	if _, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "local",
	}); err != nil {
		t.Fatalf("ensure child task: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-reduce-waiting",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-reduce-waiting"
	rootID := "root"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"command": "echo root"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-waiting"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	w.handleJob(req)

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusQueued {
		t.Fatalf("run status after waiting reduction: got %q, want %q", runStatus, dal.RunStatusQueued)
	}

	pending, err := repos.TaskDispatchIntents().ListPending(ctx, "local", w.clock.Now().UnixNano(), 10)
	if err != nil {
		t.Fatalf("list pending intents: %v", err)
	}

	if len(pending) != 1 {
		t.Fatalf("pending child intent: got %d, want 1: %+v", len(pending), pending)
	}
}

func TestWorkerRunClaimedJob_TaskFanoutFailureFinalizesExecutionAndRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-task-failure-order", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-failure-order"
	def := `{"id":"job-worker-task-failure-order","root":{"id":"root","uses":"builtins/shell","with":{"command":"false"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-failure-order",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-failure-order"
	rootID := "root"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"command": "false"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-failure-order"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	w.handleJob(req)

	var runStatus string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus, &failureReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status after failed task: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if !failureReason.Valid || (!strings.Contains(failureReason.String, "command failed") && !strings.Contains(failureReason.String, "exit status")) {
		t.Fatalf("failure reason should describe command failure, got %+v", failureReason)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, rootDispatch.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusFailed {
		t.Fatalf("execution status after failed task: got %q, want %q", executionStatus, dal.ExecutionStatusFailed)
	}

	summary, err := runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("get task completion: %v", err)
	}

	if summary.Total != 1 || summary.TerminalFailed != 1 || summary.Incomplete != 0 {
		t.Fatalf("task completion summary after failed task: %+v", summary)
	}
}

func TestWorkerRunClaimedJob_TaskFanoutCancelFinalizesExecutionAndRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-task-cancel-order", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-cancel-order"
	def := `{"id":"job-worker-task-cancel-order","root":{"id":"root","uses":"builtins/shell","with":{"command":"exec sleep 5"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-cancel-order",
		cellID:        "local",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		cancelCh:      make(chan string, 1),
	}

	deliveryID := "delivery-task-cancel-order"
	rootID := "root"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"command": "exec sleep 5"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-cancel-order"}}
	env, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1)
	if err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	outcomeCh := make(chan string, 1)
	finished := make(chan struct{})
	go func() {
		outcomeCh <- w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
		close(finished)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentRunID, _ := w.getCurrentRunInfo()
		if currentRunID == runID {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable task run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelTicker := time.NewTicker(10 * time.Millisecond)
	defer cancelTicker.Stop()
	go func() {
		for {
			select {
			case <-finished:
				return
			case <-cancelTicker.C:
				select {
				case w.cancelCh <- runID:
				default:
				}
			}
		}
	}()

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for canceled task run to finish")
	}

	if outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	var runStatus string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus, &failureReason); err != nil {
		t.Fatalf("query canceled run: %v", err)
	}

	if runStatus != dal.RunStatusCancelled {
		t.Fatalf("run status after canceled task: got %q, want %q", runStatus, dal.RunStatusCancelled)
	}

	if !failureReason.Valid || failureReason.String != dal.CancelReasonAPI {
		t.Fatalf("expected failure_reason %q, got %+v", dal.CancelReasonAPI, failureReason)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, rootDispatch.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAborted {
		t.Fatalf("execution status after canceled task: got %q, want %q", executionStatus, dal.ExecutionStatusAborted)
	}

	summary, err := runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("get task completion: %v", err)
	}

	if summary.Total != 1 || summary.TerminalFailed != 1 || summary.Incomplete != 0 {
		t.Fatalf("task completion summary after canceled task: %+v", summary)
	}
}

func TestWorkerRunClaimedJob_TaskFanoutExecutesEnvelopeTaskOnly(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	jobID := "job-worker-task-scope"
	runID, runIndex, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	second, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "second",
		Name:         "second",
		SpecHash:     "sha256:second",
		TargetCellID: "local",
	})

	if err != nil {
		t.Fatalf("ensure second task: %v", err)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	clock := mocks.NewMockClock()
	executor := job.NewExecutor()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-scope",
		cellID:        "local",
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      executor,
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-scope"
	rootID := "root-node"
	firstID := "first"
	secondID := "second"
	sequenceAction := "builtins/sequence"
	shellAction := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &sequenceAction,
			Steps: []*api.Node{
				{
					Id:   &firstID,
					Uses: &shellAction,
					With: map[string]string{"command": "echo worker-first-marker"},
				},
				{
					Id:   &secondID,
					Uses: &shellAction,
					With: map[string]string{"command": "echo worker-second-marker"},
				},
			},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-task-scope"}}
	dispatch := dal.ExecutionDispatchRecord{
		RunID:             runID,
		JobID:             jobID,
		RunIndex:          runIndex,
		TaskID:            second.TaskID,
		TaskKey:           second.TaskKey,
		TaskName:          second.Name,
		TaskAttemptID:     second.TaskAttemptID,
		SegmentID:         second.SegmentID,
		SegmentName:       second.SegmentName,
		SegmentStatus:     dal.SegmentStatusPending,
		ExecutionID:       second.ExecutionID,
		ExecutionStatus:   dal.ExecutionStatusPending,
		CellID:            "local",
		Attempt:           second.Attempt,
		DefinitionVersion: 1,
		DefinitionHash:    dal.DefinitionHash(`{"id":"job-worker-task-scope"}`),
		OwningCell:        "local",
	}
	if _, err := cell.AttachExecutionEnvelope(req, dispatch, 1); err != nil {
		t.Fatalf("attach child envelope: %v", err)
	}

	w.handleJob(req)

	select {
	case stream := <-streamCh:
		if err := stream.WaitForDone(5 * time.Second); err != nil {
			t.Fatalf("wait for log stream: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for log stream hook")
	}

	chunks := logClient.GetChunks()
	if len(chunks) == 0 {
		t.Fatal("expected log chunks")
	}

	var sawSecond bool
	for _, chunk := range chunks {
		data := string(chunk.GetData())
		if strings.Contains(data, "worker-first-marker") {
			t.Fatalf("worker replayed sibling task; chunks=%v", chunks)
		}
		if strings.Contains(data, "worker-second-marker") {
			sawSecond = true
		}
	}
	if !sawSecond {
		t.Fatalf("expected selected task marker in chunks=%v", chunks)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusSucceeded {
		t.Fatalf("run status after final task: got %q, want %q", runStatus, dal.RunStatusSucceeded)
	}
}

func TestWorkerFinalizeAbortedTaskRunByExecutionClaim_CancelsRun(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("worker-test").Start(context.Background(), "finalize-aborted-by-claim")
	runs := mocks.NewMockRunsRepository()
	runs.ExecutionFinalization = dal.ExecutionFinalizationResult{
		ExecutionID: "execution-root",
		RunID:       "run-aborted",
		Outcome:     dal.ExecutionFinalizationOutcomeRunCancelled,
		Summary:     dal.RunTaskCompletion{RunID: "run-aborted", Total: 1, TerminalFailed: 1},
	}

	w := &worker{
		runCtx:   context.Background(),
		logger:   interfaces.NewLogger("worker-test"),
		workerID: "worker-a",
		store:    runs,
		catalog:  cell.NewCatalogEventPublisher("local", nil),
	}

	env := &cell.ExecutionEnvelope{ExecutionID: "execution-root"}
	outcome := w.finalizeAbortedTaskRunByExecutionClaim(ctx, "execution-claim-token", dal.CancelReasonAPI, env)
	span.End()
	if outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeAborted)
	}

	if runs.LastFinalizedExecID != "execution-root" || runs.LastExecutionOwner != "worker-a" || runs.LastFinalizedStatus != dal.ExecutionStatusAborted {
		t.Fatalf("finalized execution call mismatch: exec=%q owner=%q status=%q", runs.LastFinalizedExecID, runs.LastExecutionOwner, runs.LastFinalizedStatus)
	}

	assertTaskFinalizeOutcome(t, recorder, taskfinalize.OutcomeExecutionAborted)
}

type scriptedAckQueue struct {
	ackErrors []error
	ackCalls  int
}

func (q *scriptedAckQueue) Enqueue(context.Context, *api.JobRequest) error {
	return errors.New("not implemented")
}

func (q *scriptedAckQueue) Dequeue(context.Context) (*api.JobRequest, error) {
	return nil, errors.New("not implemented")
}

func (q *scriptedAckQueue) TryDequeue(context.Context) (*api.JobRequest, error) {
	return nil, errors.New("not implemented")
}

func (q *scriptedAckQueue) Close() error { return nil }

func (q *scriptedAckQueue) Ack(context.Context, string) error {
	err := error(nil)
	if q.ackCalls < len(q.ackErrors) {
		err = q.ackErrors[q.ackCalls]
	}
	q.ackCalls++
	return err
}

func TestWorkerHandleJob_RunlessDeliveryIsMalformed(t *testing.T) {
	queue := &scriptedAckQueue{}
	logClient := mocks.NewMockLogClient()
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-runless",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		metrics:       workerMetrics,
	}

	jobID := "job-worker-runless"
	deliveryID := "delivery-runless"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/shell"
	req := &api.JobRequest{Job: &api.Job{
		Id:         &jobID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &commandNodeID,
			Uses: &action,
			With: map[string]string{"command": command},
		},
	}}

	w.handleJob(req)

	if queue.ackCalls != 1 {
		t.Fatalf("ack calls: got %d, want 1", queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after runless delivery, got %d log streams", logClient.GetStreamCount())
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected worker to return idle after malformed delivery, got %q", got)
	}
}

func TestWorkerRunClaimedJob_MissingExecutionEnvelopeFailsRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-missing-envelope", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	queue := &scriptedAckQueue{}
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-missing-envelope",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-missing-envelope"
	deliveryID := "delivery-missing-envelope"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	outcome := w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureCode, &failureReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if failureCode != dal.FailureCodeInvalidEnvelope {
		t.Fatalf("failure code: got %q, want %q", failureCode, dal.FailureCodeInvalidEnvelope)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, "execution envelope") {
		t.Fatalf("failure reason should describe missing envelope, got %+v", failureReason)
	}

	if queue.ackCalls != 1 {
		t.Fatalf("ack calls: got %d, want 1", queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after missing envelope, got %d log streams", logClient.GetStreamCount())
	}
}

func TestWorkerRunClaimedJob_ExecutionClaimRequiredBeforeExecute(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	ns, err := repos.Namespaces().Create(ctx, "worker-execution-claim-required", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-execution-claim-required"
	def := `{"id":"job-worker-execution-claim-required","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo should-not-run"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claimed, token, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "other-worker", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("preclaim execution: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected preclaimed execution, claimed=%v token=%q", claimed, token)
	}

	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-claim-required",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-claim-required"
	rootID := "root"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"command": "echo should-not-run"},
		},
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	outcome := w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution not to start without execution claim, got %d log streams", logClient.GetStreamCount())
	}

	var runStatus string
	var failureReason sql.NullString
	var orphanReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, orphan_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureReason, &orphanReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusOrphaned {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusOrphaned)
	}

	if !failureReason.Valid || failureReason.String != dal.OrphanReasonAckUncertain || !orphanReason.Valid || orphanReason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("orphan reasons: failure=%v orphan=%v", failureReason, orphanReason)
	}

	var executionStatus string
	var leaseOwner sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner
		FROM segment_executions
		WHERE execution_id = ?
	`, dispatch.ExecutionID).Scan(&executionStatus, &leaseOwner); err != nil {
		t.Fatalf("query execution state: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAccepted || !leaseOwner.Valid || leaseOwner.String != "other-worker" {
		t.Fatalf("execution state: status=%q owner=%v", executionStatus, leaseOwner)
	}
}

func TestWorkerRunClaimedJob_AckTransientThenSuccess_Completes(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-ack-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-ack-retry"
	clock := mocks.NewMockClock()
	queue := &scriptedAckQueue{
		ackErrors: []error{
			status.Error(codes.Unavailable, "queue temporarily unavailable"),
			status.Error(codes.Unavailable, "queue temporarily unavailable"),
			nil,
		},
	}

	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-ack-retry"
	deliveryID := "delivery-ack-retry"
	commandNodeID := "node-1"
	command := "echo ack-retry"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after ack retries recover, got %q", statusVal)
	}

	if queue.ackCalls != 3 {
		t.Fatalf("expected 3 ack attempts, got %d", queue.ackCalls)
	}

	sleeps := clock.GetSleeps()
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 backoff sleeps for transient ack errors, got %d", len(sleeps))
	}
}

func TestWorkerRunClaimedJob_AckPersistentFailure_OrphansRunWithoutExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-ack-persistent", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-ack-persistent"
	clock := mocks.NewMockClock()
	queue := &scriptedAckQueue{
		ackErrors: []error{
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
		},
	}

	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-ack-persistent"
	deliveryID := "delivery-ack-persistent"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	var reason sql.NullString
	var orphanReason sql.NullString
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason, orphan_reason, claim_token FROM job_runs WHERE run_id = ?`, runID).
		Scan(&statusVal, &reason, &orphanReason, &claimToken); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "orphaned" {
		t.Fatalf("expected orphaned on persistent ack failure, got %q", statusVal)
	}

	if !reason.Valid || reason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected failure_reason %q, got %v", dal.OrphanReasonAckUncertain, reason)
	}

	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan_reason %q, got %v", dal.OrphanReasonAckUncertain, orphanReason)
	}

	if claimToken.Valid {
		t.Fatalf("expected claim_token cleared after orphaning, got %v", claimToken)
	}

	if queue.ackCalls != ackMaxAttempts {
		t.Fatalf("expected %d ack attempts, got %d", ackMaxAttempts, queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after persistent ack failure, got %d log streams", logClient.GetStreamCount())
	}
}

func TestWorkerRunClaimedJob_FinalizeSucceededRetriesOnTransientStoreFailure(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalize-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-finalize-retry"
	clock := mocks.NewMockClock()
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	store := &flakyFinalizeRunsStore{
		RunsRepository:      runs,
		succeedFailuresLeft: 2,
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         store,
	}

	jobID := "job-worker-finalize-retry"
	deliveryID := "delivery-finalize-retry"
	commandNodeID := "node-1"
	command := "echo finalize-retry"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after transient finalize failures, got %q", statusVal)
	}

	sleeps := clock.GetSleeps()
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 finalize-retry sleeps, got %d", len(sleeps))
	}
}

func TestWorkerRunClaimedJob_LifecyclePhaseShowsFinalizing(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalizing-phase", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	store := &blockingSuccessStore{
		RunsRepository: runs,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-finalizing-phase",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         store,
		metrics:       workerMetrics,
	}

	jobID := "job-worker-finalizing-phase"
	deliveryID := "delivery-finalizing-phase"
	commandNodeID := "node-1"
	command := "echo finalizing-phase"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	done := make(chan string, 1)
	go func() {
		done <- w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
	}()

	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for finalization to begin")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseFinalizing {
		t.Fatalf("expected finalizing phase, got %q", got)
	}

	close(store.release)

	select {
	case outcome := <-done:
		if outcome != observability.WorkerOutcomeSuccess {
			t.Fatalf("expected success, got %q", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for finalizing run to finish")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected idle phase after completion, got %q", got)
	}
}

func TestWorkerRunClaimedJob_RenewLeaseTransientStoreFailure_StillSucceeds(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-renew-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-renew-retry"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	store := &flakyFinalizeRunsStore{
		RunsRepository:    runs,
		renewFailuresLeft: 2,
	}
	recordingStore := &recordingExecutionClaimStore{RunsRepository: store}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         interfaces.SystemClock{},
		renewInterval: 10 * time.Millisecond,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         recordingStore,
	}

	jobID := "job-worker-renew-retry"
	deliveryID := "delivery-renew-retry"
	commandNodeID := "node-1"
	command := "echo renew-retry-start; sleep 0.06; echo renew-retry-end"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}
	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after transient renew failures, got %q", statusVal)
	}

	claimedExecutions, renewedExecutions := recordingStore.executionClaimCounts()
	if claimedExecutions == 0 {
		t.Fatal("expected worker to claim the execution")
	}

	if renewedExecutions == 0 {
		t.Fatal("expected worker to renew the execution lease")
	}
}

func TestWorkerRestartMidRun_LeaseExpiryThenRequeue_AllowsRecovery(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-restart-recovery", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, tokenA, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-a: %v", err)
	}

	if !claimed || tokenA == "" {
		t.Fatalf("expected worker-a claim and token, got claimed=%v token=%q", claimed, tokenA)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET lease_until = ?
		WHERE run_id = ?
	`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("force expired lease: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("mark expired running as orphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runID {
		t.Fatalf("expected run %s orphaned, got %+v", runID, orphaned)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("requeue run for retry: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-b",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-restart-recovery"
	deliveryID := "delivery-restart-recovery"
	commandNodeID := "node-1"
	command := "echo restart-recovered"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after restart recovery path, got %q", statusVal)
	}
}

func TestWorkerRunClaimedJob_FinalizeSucceededExhausted_LeavesRunningForOrphanSweep(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalize-exhausted", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-finalize-exhausted"
	clock := mocks.NewMockClock()
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()
	store := &flakyFinalizeRunsStore{
		RunsRepository:      runs,
		succeedFailuresLeft: finalizeMaxAttempts,
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        logger,
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         store,
	}

	jobID := "job-worker-finalize-exhausted"
	deliveryID := "delivery-finalize-exhausted"
	commandNodeID := "node-1"
	command := "echo finalize-exhausted"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)

	sleeps := clock.GetSleeps()
	if len(sleeps) != finalizeMaxAttempts-1 {
		t.Fatalf("expected %d finalize-retry sleeps, got %d", finalizeMaxAttempts-1, len(sleeps))
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if statusVal != "running" {
		t.Fatalf("expected run to remain running after finalize retries exhausted, got %q", statusVal)
	}

	joinedInfo := strings.Join(logger.GetInfoCalls(), "\n")
	if strings.Contains(joinedInfo, "Job completed successfully") {
		t.Fatalf("should not log successful completion when run finalize exhausted; info logs: %v", logger.GetInfoCalls())
	}

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET lease_until = ? WHERE run_id = ?`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("mark expired running as orphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runID {
		t.Fatalf("expected orphan sweep to include run %s, got %+v", runID, orphaned)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query status after orphan sweep: %v", err)
	}

	if statusVal != "orphaned" {
		t.Fatalf("expected orphaned after orphan sweep, got %q", statusVal)
	}
}

func TestWorkerDrain_ShutdownDuringRun_StillFinalizesRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-drain", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	runCtx := context.Background()

	queue := mocks.NewMockQueueClient()
	jobID := "job-worker-drain"
	deliveryID := "delivery-drain"
	commandNodeID := "node-1"
	command := "sleep 0.08"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	req := &api.JobRequest{Job: j}
	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.DefinitionHash == "" {
		dispatch.DefinitionHash = "test-definition-hash"
	}

	if _, err := cell.AttachExecutionEnvelope(req, dispatch, time.Now().UnixNano()); err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	queue.AddJobRequest(req)
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	w := &worker{
		ctx:           shutdownCtx,
		runCtx:        runCtx,
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-drain",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
		metrics:       workerMetrics,
	}

	done := make(chan struct{})
	go func() {
		w.run()
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		var st string
		if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&st); err != nil {
			t.Fatalf("query run status: %v", err)
		}

		if st == "running" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run to reach running, last status=%q", st)
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelShutdown()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker run() after shutdown")
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected run succeeded after drain (shutdown during execution), got %q", statusVal)
	}

	if !workerMetrics.Draining() {
		t.Fatal("expected worker metrics to report draining after shutdown")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected worker lifecycle to return to idle after drain, got %q", got)
	}
}

func TestWorkerRunClaimedJob_RemoteCancel_MarksRunAborted(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-cancel", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-cancel",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		executor:      job.NewExecutor(),
		store:         runs,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		cancelCh:      make(chan string, 1),
	}

	jobID := "job-worker-cancel"
	deliveryID := "delivery-cancel"
	commandNodeID := "node-1"
	command := "exec sleep 5"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	outcomeCh := make(chan string, 1)
	finished := make(chan struct{})
	go func() {
		outcomeCh <- w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
		close(finished)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentRunID, _ := w.getCurrentRunInfo()
		if currentRunID == runID {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelTicker := time.NewTicker(10 * time.Millisecond)
	defer cancelTicker.Stop()
	go func() {
		for {
			select {
			case <-finished:
				return
			case <-cancelTicker.C:
				select {
				case w.cancelCh <- runID:
				default:
				}
			}
		}
	}()

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for canceled run to finish")
	}

	if outcome != "aborted" {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	var statusVal string
	var failureCode string
	var failureReason sql.NullString
	var finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, CAST(finished_at AS TEXT)
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&statusVal, &failureCode, &failureReason, &finishedAt); err != nil {
		t.Fatalf("query canceled run: %v", err)
	}

	if statusVal != dal.RunStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", statusVal)
	}

	if failureCode != "" {
		t.Fatalf("expected empty failure_code, got %q", failureCode)
	}

	if !failureReason.Valid || failureReason.String != dal.CancelReasonAPI {
		t.Fatalf("expected failure_reason %q, got %v", dal.CancelReasonAPI, failureReason)
	}

	if !finishedAt.Valid {
		t.Fatal("expected finished_at to be set for aborted run")
	}

	events, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list catalog events: %v", err)
	}

	wantKey := cell.CatalogRunStatusEventKey(runID, dal.RunStatusCancelled)
	found := false
	for _, event := range events {
		if event.EventKey == wantKey {
			found = true
			break
		}

		if event.EventType == cell.CatalogEventTypeRunStatus && strings.Contains(event.EventKey, dal.RunStatusAborted) {
			t.Fatalf("unexpected aborted run catalog event: %+v", event)
		}
	}

	if !found {
		t.Fatalf("expected cancelled catalog event %q, got %+v", wantKey, events)
	}
}

func TestWorkerRunClaimedJob_DurableCancel_MarksRunAborted(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-durable-cancel", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:                context.Background(),
		runCtx:             context.Background(),
		logger:             interfaces.NewLogger("worker-test"),
		workerID:           "worker-durable-cancel",
		clock:              interfaces.SystemClock{},
		renewInterval:      time.Hour,
		cancelPollInterval: 10 * time.Millisecond,
		queue:              mocks.NewMockQueueClient(),
		logClient:          mocks.NewMockLogClient(),
		executor:           job.NewExecutor(),
		store:              runs,
		cancelCh:           make(chan string, 1),
	}

	jobID := "job-worker-durable-cancel"
	deliveryID := "delivery-durable-cancel"
	commandNodeID := "node-1"
	command := "exec sleep 5"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	outcomeCh := make(chan string, 1)
	go func() {
		outcomeCh <- w.runClaimedJob(context.Background(), j, jobID, runID, deliveryID, env)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var claimToken string
	for {
		currentRunID, currentClaimToken := w.getCurrentRunInfo()
		if currentRunID == runID {
			claimToken = currentClaimToken
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	if _, err := runs.RequestRunCancel(ctx, runID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("request durable cancel: %v", err)
	}

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for durable-canceled run to finish")
	}

	if outcome != "aborted" {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	requested, err := runs.RunCancelRequested(ctx, runID, claimToken)
	if err != nil {
		t.Fatalf("run cancel requested after abort: %v", err)
	}

	if requested {
		t.Fatal("expected terminal transition to clear durable cancel request")
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query durable-canceled run: %v", err)
	}

	if statusVal != dal.RunStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", statusVal)
	}
}

func TestHandleDequeueError_ContextCanceledStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &worker{
		ctx:    ctx,
		logger: interfaces.NewLogger("worker-test"),
		clock:  interfaces.SystemClock{},
	}

	job, keep := w.handleDequeueError(context.Canceled)
	if job != nil || keep {
		t.Fatalf("expected exit, got job=%v keep=%v", job, keep)
	}
}

func TestHandleDequeueError_GRPCCanceledStops(t *testing.T) {
	t.Parallel()
	logger := mocks.NewMockLogger()
	w := &worker{
		ctx:    context.Background(),
		logger: logger,
		clock:  interfaces.SystemClock{},
	}

	job, keep := w.handleDequeueError(status.Error(codes.Canceled, "context canceled"))
	if job != nil || keep {
		t.Fatalf("expected exit, got job=%v keep=%v", job, keep)
	}

	if len(logger.GetWarnCalls()) != 0 {
		t.Fatalf("expected no warn on grpc Canceled shutdown, got %v", logger.GetWarnCalls())
	}
}

func TestHandleDequeueError_NonTransientRetriesWithBackoff(t *testing.T) {
	t.Parallel()
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	w := &worker{
		ctx:    context.Background(),
		logger: logger,
		clock:  clock,
	}

	job, keep := w.handleDequeueError(status.Error(codes.InvalidArgument, "bad request"))
	if job != nil || !keep {
		t.Fatalf("expected keep retrying, got job=%v keep=%v", job, keep)
	}

	if len(clock.GetSleeps()) != 1 {
		t.Fatalf("expected one backoff sleep, got %v", clock.GetSleeps())
	}

	errs := logger.GetErrorCalls()
	if len(errs) != 1 || !strings.Contains(errs[0], "self-healing") {
		t.Fatalf("expected one error log about self-healing backoff, got %v", errs)
	}
}

func TestWorker_Run_ExitsWhenDequeueCanceled(t *testing.T) {
	t.Parallel()
	q := mocks.NewMockQueueClient()
	q.SetDequeueError(context.Canceled)
	w := &worker{
		ctx:    context.Background(),
		logger: interfaces.NewLogger("worker-test"),
		clock:  interfaces.SystemClock{},
		queue:  q,
	}
	w.run()
}

func TestForwarderSocketPath_RespectsXDGRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	got := forwarderSocketPath()
	want := "/run/user/1000/vectis/log-forwarder.sock"
	if got != want {
		t.Fatalf("forwarderSocketPath() = %q, want %q", got, want)
	}
}

func TestForwarderSocketPath_FallsBackToTempDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	got := forwarderSocketPath()
	if !strings.HasSuffix(got, "/log-forwarder.sock") {
		t.Fatalf("forwarderSocketPath() = %q, expected suffix /log-forwarder.sock", got)
	}
	if !strings.Contains(got, fmt.Sprintf("vectis-%d", os.Getuid())) {
		t.Fatalf("forwarderSocketPath() = %q, expected user-isolated directory", got)
	}
}

type fakeControlAddr string

func (a fakeControlAddr) Network() string { return "tcp" }

func (a fakeControlAddr) String() string { return string(a) }

type fakeControlListener struct {
	addr net.Addr
}

func (l *fakeControlListener) Accept() (net.Conn, error) {
	return nil, errors.New("fake control listener does not accept connections")
}

func (l *fakeControlListener) Close() error { return nil }

func (l *fakeControlListener) Addr() net.Addr { return l.addr }

func fakeControlListen(failures map[string]error, calls *[]string) controlListenFunc {
	return func(network, address string) (net.Listener, error) {
		*calls = append(*calls, address)
		if network != "tcp" {
			return nil, fmt.Errorf("unexpected network %q", network)
		}

		if err := failures[address]; err != nil {
			return nil, err
		}

		port := strings.TrimPrefix(address, ":")
		return &fakeControlListener{addr: fakeControlAddr("127.0.0.1:" + port)}, nil
	}
}

func assertControlListenCalls(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("listen calls = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listen calls = %v, want %v", got, want)
		}
	}
}

func TestStartControlListener_StaticUsesConfiguredPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "static")
	viper.Set("control_port", 19084)

	var calls []string
	ln, addr, err := startControlListenerWithListen(fakeControlListen(nil, &calls))
	if err != nil {
		t.Fatalf("startControlListener(static): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":19084")

	if addr != "127.0.0.1:19084" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:19084")
	}
}

func TestStartControlListener_RangeUsesConfiguredPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "range")
	viper.Set("control_port_min", 19085)
	viper.Set("control_port_max", 19086)

	var calls []string
	failures := map[string]error{":19085": errors.New("port unavailable")}
	ln, addr, err := startControlListenerWithListen(fakeControlListen(failures, &calls))
	if err != nil {
		t.Fatalf("startControlListener(range): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":19085", ":19086")

	if addr != "127.0.0.1:19086" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:19086")
	}
}

func TestStartControlListener_EphemeralUsesZeroPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "ephemeral")

	var calls []string
	ln, addr, err := startControlListenerWithListen(fakeControlListen(nil, &calls))
	if err != nil {
		t.Fatalf("startControlListener(ephemeral): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":0")

	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:0")
	}
}

func TestControlPublishAddress_NormalizesUnspecifiedHost(t *testing.T) {
	if got := controlPublishAddress("[::]:19084"); got != "localhost:19084" {
		t.Fatalf("IPv6 unspecified addr = %q, want localhost:19084", got)
	}

	if got := controlPublishAddress("0.0.0.0:19084"); got != "localhost:19084" {
		t.Fatalf("IPv4 unspecified addr = %q, want localhost:19084", got)
	}

	if got := controlPublishAddress("127.0.0.1:19084"); got != "127.0.0.1:19084" {
		t.Fatalf("loopback addr = %q, want 127.0.0.1:19084", got)
	}
}

func TestStartControlListener_RangeExhausted(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "range")
	viper.Set("control_port_min", 19085)
	viper.Set("control_port_max", 19086)

	var calls []string
	failures := map[string]error{
		":19085": errors.New("port unavailable"),
		":19086": errors.New("port unavailable"),
	}

	ln, _, err := startControlListenerWithListen(fakeControlListen(failures, &calls))
	if err == nil {
		_ = ln.Close()
		t.Fatal("startControlListener(range) succeeded, want exhaustion error")
	}

	assertControlListenCalls(t, calls, ":19085", ":19086")

	if !strings.Contains(err.Error(), "no available port in range 19085-19086") {
		t.Fatalf("error = %v, want no available port range", err)
	}
}
