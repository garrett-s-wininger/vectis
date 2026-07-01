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
	"vectis/internal/testutil/runfixture"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestService_Process_ReenqueuesQueuedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"job-a","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "job-a", jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
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

func TestService_DispatchTriggeredRun_EnqueuesQueuedRunOnce(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobDef := `{"id":"triggered-job","root":{"uses":"builtins/script","with":{"script":"echo triggered"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "triggered-job", jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, "triggered-job", nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	if err := svc.DispatchTriggeredRun(ctx, dal.CreatedRun{RunID: runID}); err != nil {
		t.Fatalf("DispatchTriggeredRun: %v", err)
	}

	jobs := q.GetJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 enqueued job, got %d", len(jobs))
	}

	if jobs[0].GetId() != "triggered-job" || jobs[0].GetRunId() != runID {
		t.Fatalf("job id/run mismatch: id=%q run=%q", jobs[0].GetId(), jobs[0].GetRunId())
	}

	if err := svc.DispatchTriggeredRun(ctx, dal.CreatedRun{RunID: runID}); err != nil {
		t.Fatalf("second DispatchTriggeredRun: %v", err)
	}

	if got := len(q.GetJobs()); got != 1 {
		t.Fatalf("second direct dispatch should skip touched run, got %d jobs", got)
	}
}

func TestService_Process_ReenqueuesAllPendingTaskContinuations(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "job-task-continuation-repair"
	jobDef := `{"id":"job-task-continuation-repair","root":{"id":"root","uses":"builtins/parallel","steps":[{"id":"child-a","uses":"builtins/script","with":{"script":"echo a"}},{"id":"child-b","uses":"builtins/script","with":{"script":"echo b"}}]}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
	}

	runs := repos.Runs()
	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root execution: %v", err)
	}

	rootID := "root"
	childAID := "child-a"
	childBID := "child-b"
	parallelAction := "builtins/parallel"
	shellAction := "builtins/script"
	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &parallelAction,
			Steps: []*api.Node{
				{Id: &childAID, Uses: &shellAction, With: map[string]string{"script": "echo a"}},
				{Id: &childBID, Uses: &shellAction, With: map[string]string{"script": "echo b"}},
			},
		},
	}

	rootReq := &api.JobRequest{Job: job}
	if _, err := cell.AttachExecutionEnvelope(rootReq, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	rootPayloadJSON, err := protojson.Marshal(rootReq)
	if err != nil {
		t.Fatalf("marshal root payload: %v", err)
	}

	if _, _, err := runs.RecordExecutionPayload(ctx, runID, string(rootPayloadJSON), dal.DefinitionHash(jobDef)); err != nil {
		t.Fatalf("record root payload: %v", err)
	}

	if _, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      childAID,
		Name:         childAID,
		SpecHash:     "sha256:child-a",
		TargetCellID: "local",
	}); err != nil {
		t.Fatalf("ensure child-a: %v", err)
	}

	if _, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      childBID,
		Name:         childBID,
		SpecHash:     "sha256:child-b",
		TargetCellID: "local",
	}); err != nil {
		t.Fatalf("ensure child-b: %v", err)
	}

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)
	if result.Outcome != dal.ExecutionFinalizationOutcomeContinued || len(result.Children) != 2 {
		t.Fatalf("root finalization should activate two children: %+v", result)
	}

	q := mocks.NewMockQueueService()
	svc := NewService(interfaces.NewLogger("test"), db, q, interfaces.SystemClock{})
	svc.SetMinDispatchGap(1 * time.Millisecond)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	reqs := q.GetJobRequests()
	if len(reqs) != 2 {
		t.Fatalf("want 2 enqueued continuation requests, got %d", len(reqs))
	}

	got := map[string]bool{}
	for _, req := range reqs {
		env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
		if err != nil {
			t.Fatalf("decode continuation envelope: %v", err)
		}

		if !ok {
			t.Fatal("continuation request missing execution envelope")
		}

		if env.TaskKey == dal.RootTaskKey {
			t.Fatalf("reconciler replayed root envelope instead of child continuation: %+v", env)
		}

		got[env.TaskKey] = true
	}

	if !got[childAID] || !got[childBID] {
		t.Fatalf("continuation task keys: got %+v, want child-a and child-b", got)
	}
}

func TestService_Process_RepairsOrphanedTaskRunSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "job-task-finalize-repair-success"
	jobDef := `{"id":"job-task-finalize-repair-success","root":{"id":"root","uses":"builtins/script","with":{"script":"echo root"}}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root execution: %v", err)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, "lease expired"); err != nil {
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
	jobDef := `{"id":"job-task-finalize-repair-failed","root":{"id":"root","uses":"builtins/script","with":{"script":"echo root"}}}`
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
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

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)
	if result.Activated != 2 || len(result.Children) != 2 {
		t.Fatalf("activated children mismatch: count=%d children=%+v", result.Activated, result.Children)
	}

	runfixture.FinalizeExecutionByClaimWithFailure(t, ctx, repos, failedBranch.ExecutionID, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "failed branch")

	if err := repos.Runs().MarkRunOrphaned(ctx, runID, "lease expired"); err != nil {
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

	jobDef := `{"id":"job-frozen","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "job-frozen", jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
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
	defV1 := `{"id":"job-versioned","root":{"uses":"builtins/script","with":{"script":"echo old"}}}`
	defV2 := `{"id":"job-versioned","root":{"uses":"builtins/script","with":{"script":"echo new"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defV1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defV2); err != nil {
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

	if got := jobs[0].GetRoot().GetWith()["script"]; got != "echo old" {
		t.Fatalf("expected reenqueue to use definition version 1, command=%q", got)
	}
}

func TestService_Process_ReenqueuesEphemeralWithJobDefinition(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	jobID := "ephemeral-uuid"
	jobDef := `{"id":"ephemeral-uuid","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
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

	jobDef := `{"id":"job-b","root":{"uses":"builtins/script"}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "job-b", jobDef); err != nil {
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

	jobDef := `{"id":"job-db-down","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
	repos := dal.NewSQLRepositories(db)
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, "job-db-down", jobDef); err != nil {
		t.Fatalf("insert definition snapshot: %v", err)
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
	jobDef := `{"id":"job-lease-takeover","root":{"uses":"builtins/script","with":{"script":"echo takeover"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, jobDef); err != nil {
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
