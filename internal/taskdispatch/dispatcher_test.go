package taskdispatch_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/taskdispatch"
	"vectis/internal/testutil/dbtest"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestDispatcherDrainEnqueuesPendingTaskIntent(t *testing.T) {
	ctx := context.Background()
	repos, child := setupDispatchableChild(t, ctx)
	queue := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	clock.SetNow(time.Unix(0, 123456789))

	dispatcher := taskdispatch.New(
		repos.Runs(),
		repos.TaskDispatchIntents(),
		repos.DispatchEvents(),
		cell.NewQueueExecutionIngress(queue, mocks.NewMockLogger()),
		clock,
	)

	result, err := dispatcher.Drain(ctx, taskdispatch.DrainOptions{CellID: "iad-a", Limit: 10})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if result.Listed != 1 || result.Enqueued != 1 || result.Failed != 0 {
		t.Fatalf("drain result: %+v", result)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued requests: got %d, want 1", len(reqs))
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(reqs[0])
	if err != nil {
		t.Fatalf("queued envelope: %v", err)
	}

	if !ok {
		t.Fatal("queued request missing execution envelope")
	}

	if env.ExecutionID != child.ExecutionID || env.TaskID != child.TaskID || env.TaskKey != child.TaskKey || env.TaskAttemptID != child.TaskAttemptID {
		t.Fatalf("queued envelope task mismatch: got %+v want child %+v", env, child)
	}

	if env.CreatedAtUnixNano != 123456789 {
		t.Fatalf("queued envelope created_at: got %d, want 123456789", env.CreatedAtUnixNano)
	}

	if env.Metadata["traceparent"] != "trace-a" {
		t.Fatalf("queued envelope trace metadata: got %q, want trace-a", env.Metadata["traceparent"])
	}

	if _, ok := env.Metadata[cell.ExecutionEnvelopeMetadataKey]; ok {
		t.Fatal("queued envelope recursively included prior envelope metadata")
	}

	pending, err := repos.TaskDispatchIntents().ListPending(ctx, "iad-a", clock.Now().UnixNano(), 10)
	if err != nil {
		t.Fatalf("list pending after drain: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("drained intent should not remain pending: %+v", pending)
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, child.RunID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("dispatch events: got %d, want 2: %+v", len(events), events)
	}

	for i, want := range []string{dal.DispatchEventAttempt, dal.DispatchEventSuccess} {
		event := events[i]
		if event.Source != dal.DispatchSourceTask || event.EventType != want {
			t.Fatalf("dispatch event %d: got source=%q type=%q, want source=%q type=%q", i, event.Source, event.EventType, dal.DispatchSourceTask, want)
		}

		if event.Message == nil || !strings.Contains(*event.Message, child.ExecutionID) || !strings.Contains(*event.Message, child.TaskAttemptID) {
			t.Fatalf("dispatch event %d missing task identity: %+v", i, event)
		}
	}
}

func TestDispatcherDrainMarksFailedIntentForRetry(t *testing.T) {
	ctx := context.Background()
	repos, child := setupDispatchableChild(t, ctx)
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue unavailable"))
	clock := mocks.NewMockClock()
	clock.SetNow(time.Unix(0, 2000))

	dispatcher := taskdispatch.New(
		repos.Runs(),
		repos.TaskDispatchIntents(),
		repos.DispatchEvents(),
		cell.NewQueueExecutionIngress(queue, mocks.NewMockLogger()),
		clock,
	)

	result, err := dispatcher.Drain(ctx, taskdispatch.DrainOptions{CellID: "iad-a", Limit: 10})
	if err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if result.Listed != 1 || result.Enqueued != 0 || result.Failed != 1 {
		t.Fatalf("drain result: %+v", result)
	}

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("failed enqueue should not record queued request, got %d", got)
	}

	pending, err := repos.TaskDispatchIntents().ListPending(ctx, "iad-a", 1999, 10)
	if err != nil {
		t.Fatalf("list pending before retry cutoff: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("failed intent should wait for retry cutoff: %+v", pending)
	}

	pending, err = repos.TaskDispatchIntents().ListPending(ctx, "iad-a", 2000, 10)
	if err != nil {
		t.Fatalf("list pending at retry cutoff: %v", err)
	}

	if len(pending) != 1 || pending[0].ExecutionID != child.ExecutionID || pending[0].EnqueueAttempts != 1 || pending[0].LastEnqueueError == nil {
		t.Fatalf("failed intent retry state mismatch: %+v", pending)
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, child.RunID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("dispatch events: got %d, want 2: %+v", len(events), events)
	}

	if events[0].Source != dal.DispatchSourceTask || events[0].EventType != dal.DispatchEventAttempt {
		t.Fatalf("attempt dispatch event mismatch: %+v", events[0])
	}

	if events[1].Source != dal.DispatchSourceTask || events[1].EventType != dal.DispatchEventFailure || events[1].Message == nil {
		t.Fatalf("failure dispatch event mismatch: %+v", events[1])
	}

	if !strings.Contains(*events[1].Message, child.ExecutionID) || !strings.Contains(*events[1].Message, "queue unavailable") {
		t.Fatalf("failure dispatch event message missing context: %+v", events[1])
	}
}

func setupDispatchableChild(t *testing.T, ctx context.Context) (*dal.SQLRepositories, dal.TaskExecutionRecord) {
	t.Helper()

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")

	ns, err := repos.Namespaces().Create(ctx, "team-task-dispatch-drain", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-dispatch-drain"
	def := `{"id":"job-task-dispatch-drain","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "iad-a",
	})

	if err != nil {
		t.Fatalf("ensure child task: %v", err)
	}

	rootID := "root"
	uses := "builtins/shell"
	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &uses,
			With: map[string]string{"command": "echo root"},
		},
	}

	rootReq := &api.JobRequest{
		Job: job,
		Metadata: map[string]string{
			"traceparent": "trace-a",
		},
	}

	if _, err := cell.AttachExecutionEnvelope(rootReq, rootDispatch, 1000); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	payloadJSON, err := protojson.Marshal(rootReq)
	if err != nil {
		t.Fatalf("marshal root payload: %v", err)
	}

	if _, _, err := repos.Runs().RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(def)); err != nil {
		t.Fatalf("record root execution payload: %v", err)
	}

	if _, activated, err := repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID); err != nil {
		t.Fatalf("root success fan-out: %v", err)
	} else if activated != 1 {
		t.Fatalf("root success fan-out activated: got %d, want 1", activated)
	}

	return repos, child
}
