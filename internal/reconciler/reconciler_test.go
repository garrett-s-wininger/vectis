package reconciler

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestService_Process_ReenqueuesQueuedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-a", jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runs := repos.Runs()
	runID, _, err := runs.CreateRun(ctx, "job-a", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 enqueued job, got %d", len(jobs))
	}
	if jobs[0].GetId() != "job-a" || jobs[0].GetRunId() != runID {
		t.Errorf("job id/run mismatch: id=%q run=%q", jobs[0].GetId(), jobs[0].GetRunId())
	}

	reqs := q.GetJobRequests()
	envelopeJSON := reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if envelopeJSON == "" {
		t.Fatal("expected execution envelope metadata")
	}

	env, err := cell.DecodeExecutionEnvelope([]byte(envelopeJSON))
	if err != nil {
		t.Fatalf("decode execution envelope: %v", err)
	}

	if env.RunID != runID || env.Job.GetId() != "job-a" {
		t.Fatalf("unexpected envelope identity: run=%q job=%q", env.RunID, env.Job.GetId())
	}

	if env.ExecutionID == "" || env.SegmentID == "" {
		t.Fatalf("expected envelope execution and segment ids, got execution=%q segment=%q", env.ExecutionID, env.SegmentID)
	}

	var last sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&last); err != nil {
		t.Fatalf("scan last_dispatched_at: %v", err)
	}
	if !last.Valid || last.Int64 == 0 {
		t.Errorf("expected last_dispatched_at set, got %v", last)
	}
}

func TestService_Process_RepairsTaskDispatchIntent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "job-task-dispatch-repair"
	jobDef := `{"id":"job-task-dispatch-repair","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().Create(ctx, jobID, jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root execution: %v", err)
	}

	rootID := "root"
	uses := "builtins/shell"
	req := &api.JobRequest{
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Id:   &rootID,
				Uses: &uses,
				With: map[string]string{"command": "echo root"},
			},
		},
		Metadata: map[string]string{"traceparent": "trace-root"},
	}

	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1000); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	if _, _, err := repos.Runs().RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(jobDef)); err != nil {
		t.Fatalf("record execution payload: %v", err)
	}

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "child",
		Name:         "child",
		SpecHash:     "sha256:child",
		TargetCellID: "local",
	})
	if err != nil {
		t.Fatalf("ensure child execution: %v", err)
	}

	activated, count, err := repos.SQLRuns().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID)
	if err != nil {
		t.Fatalf("activate child execution: %v", err)
	}

	if count != 1 || len(activated) != 1 || activated[0].ExecutionID != child.ExecutionID {
		t.Fatalf("activated child mismatch: count=%d children=%+v want %+v", count, activated, child)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	reqs := q.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("expected only task continuation enqueue, got %d request(s)", len(reqs))
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(reqs[0])
	if err != nil {
		t.Fatalf("queued envelope: %v", err)
	}
	if !ok {
		t.Fatal("queued request missing execution envelope")
	}

	if env.ExecutionID != child.ExecutionID || env.TaskID != child.TaskID || env.TaskAttemptID != child.TaskAttemptID {
		t.Fatalf("queued wrong execution: got %+v want child %+v", env, child)
	}

	summary, err := repos.TaskDispatchIntents().GetRunSummary(ctx, runID)
	if err != nil {
		t.Fatalf("task dispatch summary: %v", err)
	}
	if summary.Total != 1 || summary.Enqueued != 1 || summary.Pending != 0 || summary.Failed != 0 {
		t.Fatalf("task dispatch summary mismatch: %+v", summary)
	}

	queued, err := repos.Runs().ListQueuedBeforeDispatchCutoff(ctx, time.Now().Unix()-1)
	if err != nil {
		t.Fatalf("list queued after repair: %v", err)
	}
	if len(queued) != 0 {
		t.Fatalf("task dispatch repair should touch dispatched and suppress root repair: %+v", queued)
	}

	events, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}
	if len(events) != 2 || events[0].Source != dal.DispatchSourceTask || events[0].EventType != dal.DispatchEventAttempt || events[1].Source != dal.DispatchSourceTask || events[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("task dispatch events mismatch: %+v", events)
	}
}

func TestService_Process_RepairsOrphanedTaskRunSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "job-task-finalize-repair-success"
	jobDef := `{"id":"job-task-finalize-repair-success","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().Create(ctx, jobID, jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root execution: %v", err)
	}

	claimed, token, err := repos.Runs().TryClaim(ctx, runID, "worker-task-finalize-success", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim run: %v", err)
	}

	if !claimed {
		t.Fatal("expected run claim")
	}

	if _, _, err := repos.SQLRuns().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID); err != nil {
		t.Fatalf("mark root succeeded: %v", err)
	}

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, token, "lease expired"); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var status string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &failureReason); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != dal.RunStatusSucceeded {
		t.Fatalf("status: got %q, want %q", status, dal.RunStatusSucceeded)
	}

	if failureReason.Valid {
		t.Fatalf("succeeded repair should not leave failure reason, got %q", failureReason.String)
	}

	if len(q.GetJobRequests()) != 0 {
		t.Fatalf("terminal repair should not enqueue work: %+v", q.GetJobRequests())
	}
}

func TestService_Process_RepairsOrphanedTaskRunFailedWithIncompleteSibling(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "job-task-finalize-repair-failed"
	jobDef := `{"id":"job-task-finalize-repair-failed","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo root"}}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().Create(ctx, jobID, jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root execution: %v", err)
	}

	failedBranch, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "failed-branch",
		Name:         "failed branch",
		SpecHash:     "sha256:failed-branch",
		TargetCellID: "local",
	})
	if err != nil {
		t.Fatalf("ensure failed branch: %v", err)
	}

	incompleteBranch, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "incomplete-branch",
		Name:         "incomplete branch",
		SpecHash:     "sha256:incomplete-branch",
		TargetCellID: "local",
	})
	if err != nil {
		t.Fatalf("ensure incomplete branch: %v", err)
	}

	claimed, token, err := repos.Runs().TryClaim(ctx, runID, "worker-task-finalize-failed", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim run: %v", err)
	}
	if !claimed {
		t.Fatal("expected run claim")
	}

	activated, count, err := repos.SQLRuns().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID)
	if err != nil {
		t.Fatalf("activate child executions: %v", err)
	}

	if count != 2 || len(activated) != 2 {
		t.Fatalf("activated children mismatch: count=%d children=%+v", count, activated)
	}

	if err := repos.SQLRuns().MarkExecutionTerminal(ctx, failedBranch.ExecutionID, dal.ExecutionStatusFailed); err != nil {
		t.Fatalf("mark failed branch terminal: %v", err)
	}

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, token, "lease expired"); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var status string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failureReason); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != dal.RunStatusFailed {
		t.Fatalf("status: got %q, want %q", status, dal.RunStatusFailed)
	}
	if failureCode != dal.FailureCodeExecution {
		t.Fatalf("failure_code: got %q, want %q", failureCode, dal.FailureCodeExecution)
	}
	if !failureReason.Valid || !strings.Contains(failureReason.String, "1 task execution(s) ended in a terminal failure") {
		t.Fatalf("failure reason: %+v", failureReason)
	}
	if len(q.GetJobRequests()) != 0 {
		t.Fatalf("terminal repair should not enqueue sibling continuation: %+v", q.GetJobRequests())
	}

	pendingSibling, err := repos.Runs().GetExecutionDispatch(ctx, incompleteBranch.ExecutionID)
	if err != nil {
		t.Fatalf("incomplete sibling should remain dispatchable record: %v", err)
	}
	if pendingSibling.ExecutionID != incompleteBranch.ExecutionID {
		t.Fatalf("wrong incomplete sibling: %+v", pendingSibling)
	}
}

func TestService_Process_ReplaysFrozenPayloadOnRedispatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-frozen","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-frozen", jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "job-frozen", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	clock := mocks.NewMockClock()
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	clock.SetNow(now)
	svc := NewService(interfaces.NewLogger("test"), db, q, clock)
	svc.SetMinDispatchGap(1 * time.Second)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	reqs := q.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("want 1 enqueued request, got %d", len(reqs))
	}
	firstEnvelope := reqs[0].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if firstEnvelope == "" {
		t.Fatal("expected first dispatch envelope")
	}

	var firstPayloadHash string
	if err := db.QueryRowContext(ctx, "SELECT execution_payload_hash FROM job_runs WHERE run_id = ?", runID).Scan(&firstPayloadHash); err != nil {
		t.Fatalf("query first payload hash: %v", err)
	}
	if firstPayloadHash == "" {
		t.Fatal("expected frozen execution payload hash")
	}

	if _, err := db.ExecContext(ctx, "UPDATE job_runs SET last_dispatched_at = ? WHERE run_id = ?", now.Add(-time.Hour).Unix(), runID); err != nil {
		t.Fatalf("force redispatch eligibility: %v", err)
	}
	clock.SetNow(now.Add(time.Hour))

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("second Process: %v", err)
	}

	reqs = q.GetJobRequests()
	if len(reqs) != 2 {
		t.Fatalf("want 2 enqueued requests, got %d", len(reqs))
	}
	secondEnvelope := reqs[1].GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if secondEnvelope != firstEnvelope {
		t.Fatalf("redispatch envelope changed:\nfirst:  %s\nsecond: %s", firstEnvelope, secondEnvelope)
	}

	var secondPayloadHash string
	if err := db.QueryRowContext(ctx, "SELECT execution_payload_hash FROM job_runs WHERE run_id = ?", runID).Scan(&secondPayloadHash); err != nil {
		t.Fatalf("query second payload hash: %v", err)
	}
	if secondPayloadHash != firstPayloadHash {
		t.Fatalf("payload hash changed: got %q want %q", secondPayloadHash, firstPayloadHash)
	}
}

func TestService_Process_ReenqueuesCapturedDefinitionVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	jobID := "job-versioned"
	defV1 := `{"id":"job-versioned","root":{"uses":"builtins/shell","with":{"command":"echo old"}}}`
	defV2 := `{"id":"job-versioned","root":{"uses":"builtins/shell","with":{"command":"echo new"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defV1, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if _, err := repos.Jobs().UpdateDefinition(ctx, jobID, defV2); err != nil {
		t.Fatalf("update job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 enqueued job, got %d", len(jobs))
	}

	if jobs[0].GetRunId() != runID {
		t.Fatalf("run id: want %q, got %q", runID, jobs[0].GetRunId())
	}

	if got := jobs[0].GetRoot().GetWith()["command"]; got != "echo old" {
		t.Fatalf("expected reenqueue to use definition version 1, command=%q", got)
	}
}

func TestService_Process_ReenqueuesEphemeralWithJobDefinition(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "ephemeral-uuid"
	jobDef := `{"id":"ephemeral-uuid","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.CreateDefinitionAndRun(ctx, jobID, jobDef, nil)
	if err != nil {
		t.Fatalf("CreateDefinitionAndRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err = svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 re-enqueued job, got %d", len(jobs))
	}

	if jobs[0].GetId() != jobID || jobs[0].GetRunId() != runID {
		t.Fatalf("unexpected payload: id=%q run=%q", jobs[0].GetId(), jobs[0].GetRunId())
	}
}

func TestService_Process_SkipsRecentDispatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-b","root":{"uses":"builtins/shell"}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-b", jobDef, 1); err != nil {
		t.Fatalf("insert: %v", err)
	}

	runs := repos.Runs()
	runID, _, err := runs.CreateRun(ctx, "job-b", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if err := runs.TouchDispatched(ctx, runID); err != nil {
		t.Fatalf("TouchDispatched: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := interfaces.NewLogger("test")
	svc := NewService(logger, db, q, interfaces.SystemClock{})

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(q.GetJobs()) != 0 {
		t.Errorf("expected no re-enqueue within min gap, got %d jobs", len(q.GetJobs()))
	}
}

func TestService_Process_MarksExpiredRunningLeaseAsOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	repos := dal.NewSQLRepositories(db)
	runID, _, err := repos.Runs().CreateRun(ctx, "job-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed running lease: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "orphaned" {
		t.Fatalf("expected status orphaned, got %q", status)
	}

	if got := len(q.GetJobs()); got != 0 {
		t.Fatalf("expected no queued redispatch from orphan sweep, got %d jobs", got)
	}
}

func TestService_Process_DBUnavailable_SkipsUntilRecovered(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-db-down","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().Create(ctx, "job-db-down", jobDef, 1); err != nil {
		t.Fatalf("insert stored job: %v", err)
	}

	if _, _, err := repos.Runs().CreateRun(ctx, "job-db-down", nil, 1); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	logger := mocks.NewMockLogger()
	svc := NewService(logger, db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process with db down should not error, got: %v", err)
	}

	if got := len(q.GetJobs()); got != 0 {
		t.Fatalf("expected no enqueue while db down, got %d", got)
	}

	warns := strings.Join(logger.GetWarnCalls(), "\n")
	if !strings.Contains(warns, "database unavailable; skipping processing until recovery") {
		t.Fatalf("expected db-down warning log, got %v", logger.GetWarnCalls())
	}
}

func TestService_Process_ServiceLeaseAllowsStandbyTakeover(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	clock := mocks.NewMockClock()
	now := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	clock.SetNow(now)

	qActive := mocks.NewMockQueueService()
	active := NewService(interfaces.NewLogger("active"), db, qActive, clock)
	active.SetLeaseOwner("reconciler-a")
	active.SetLeaseTTL(time.Minute)
	active.SetMinDispatchGap(time.Millisecond)

	qStandby := mocks.NewMockQueueService()
	standby := NewService(interfaces.NewLogger("standby"), db, qStandby, clock)
	standby.SetLeaseOwner("reconciler-b")
	standby.SetLeaseTTL(time.Minute)
	standby.SetMinDispatchGap(time.Millisecond)

	if err := active.Process(ctx); err != nil {
		t.Fatalf("active process: %v", err)
	}

	if err := standby.Process(ctx); err != nil {
		t.Fatalf("standby process while lease held: %v", err)
	}

	if got := len(qStandby.GetJobs()); got != 0 {
		t.Fatalf("standby should not process while lease is held, got %d jobs", got)
	}

	repos := dal.NewSQLRepositories(db)
	jobID := "job-lease-takeover"
	jobDef := `{"id":"job-lease-takeover","root":{"uses":"builtins/shell","with":{"command":"echo takeover"}}}`
	if err := repos.Jobs().Create(ctx, jobID, jobDef, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	clock.SetNow(now.Add(2 * time.Minute))
	if err := standby.Process(ctx); err != nil {
		t.Fatalf("standby process after lease expiry: %v", err)
	}

	jobs := qStandby.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected standby takeover to enqueue 1 job, got %d", len(jobs))
	}

	if jobs[0].GetRunId() != runID {
		t.Fatalf("expected run %q, got %q", runID, jobs[0].GetRunId())
	}
}
