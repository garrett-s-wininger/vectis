package job_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/job"
	"vectis/internal/testutil/dbtest"
)

func claimExecutionAccepted(t testing.TB, ctx context.Context, runs dal.RunsRepository, executionID string) {
	t.Helper()

	claim, err := runs.TryClaimExecution(ctx, executionID, "task-materializer-test", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim execution %s: %v", executionID, err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution %s to be claimable, claim=%+v", executionID, claim)
	}
}

func TestEnsureJobTaskExecutionsMaterializesPlannedTasks(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-materialize", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-materialize"
	def := `{"id":"job-task-materialize","root":{"id":"root","uses":"builtins/sequence","steps":[{"id":"setup","uses":"builtins/shell","with":{"command":"echo setup"}},{"id":"build","uses":"builtins/sequence","steps":[{"id":"compile","uses":"builtins/shell","with":{"command":"echo compile"}},{"id":"test","uses":"builtins/shell","with":{"command":"echo test"}}]}]}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	j := taskPlanJob("echo compile")
	j.Id = &jobID
	j.RunId = &runID

	result, err := job.EnsureJobTaskExecutions(ctx, repos.Runs(), j, "pdx-b")
	if err != nil {
		t.Fatalf("EnsureJobTaskExecutions: %v", err)
	}

	if result.Created != 4 || len(result.Tasks) != 4 {
		t.Fatalf("materialized: created=%d tasks=%+v", result.Created, result.Tasks)
	}

	wantCells := map[string]string{
		"setup":   "pdx-b",
		"build":   "pdx-b",
		"compile": "pdx-b",
		"test":    "pdx-b",
	}

	for _, rec := range result.Tasks {
		if rec.CellID != wantCells[rec.TaskKey] {
			t.Fatalf("task %s cell: got %q, want %q", rec.TaskKey, rec.CellID, wantCells[rec.TaskKey])
		}

		if rec.Attempt != 1 || rec.TaskAttemptID == "" || rec.ExecutionID == "" || rec.SegmentID == "" {
			t.Fatalf("task %s has incomplete execution record: %+v", rec.TaskKey, rec)
		}
	}

	tasks, _, err := repos.Runs().ListRunTasks(ctx, runID, 0, 20)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}

	byKey := map[string]dal.TaskRecord{}
	for _, task := range tasks {
		byKey[task.TaskKey] = task
	}

	if len(byKey) != 5 {
		t.Fatalf("task count: got %d, want root + 4: %+v", len(byKey), tasks)
	}

	assertMaterializedTask(t, byKey, runID, "setup", dal.RootTaskKey)
	assertMaterializedTask(t, byKey, runID, "build", dal.RootTaskKey)
	assertMaterializedTask(t, byKey, runID, "compile", "build")
	assertMaterializedTask(t, byKey, runID, "test", "build")

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	if dispatch.TaskKey != dal.RootTaskKey {
		t.Fatalf("pending execution task key: got %q, want %q", dispatch.TaskKey, dal.RootTaskKey)
	}

	claimExecutionAccepted(t, ctx, repos.Runs(), dispatch.ExecutionID)

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("planned tasks should not dispatch after root accepts, got %v", err)
	}

	again, err := job.EnsureJobTaskExecutions(ctx, repos.Runs(), j, "pdx-b")
	if err != nil {
		t.Fatalf("EnsureJobTaskExecutions again: %v", err)
	}

	if again.Created != 0 || len(again.Tasks) != 4 {
		t.Fatalf("idempotent materialization: created=%d tasks=%+v", again.Created, again.Tasks)
	}
}

func TestEnsureJobTaskExecutionsRejectsMissingRunID(t *testing.T) {
	t.Parallel()

	j := taskPlanJob("echo compile")
	j.RunId = nil

	if _, err := job.EnsureJobTaskExecutions(context.Background(), &noopRunsRepository{}, j, ""); err == nil || !strings.Contains(err.Error(), "job run_id is required") {
		t.Fatalf("expected missing run_id error, got %v", err)
	}
}

func TestEnsurePlannedTaskExecutionsRejectsOutOfOrderPlan(t *testing.T) {
	t.Parallel()

	if _, err := job.EnsurePlannedTaskExecutions(context.Background(), &noopRunsRepository{}, "run-1", []job.TaskPlanEntry{{
		TaskKey:       "child",
		ParentTaskKey: "missing-parent",
	}}, ""); err == nil || !strings.Contains(err.Error(), `parent "missing-parent" has not been materialized`) {
		t.Fatalf("expected missing parent error, got %v", err)
	}
}

func assertMaterializedTask(t *testing.T, byKey map[string]dal.TaskRecord, runID, taskKey, parentKey string) {
	t.Helper()

	task, ok := byKey[taskKey]
	if !ok {
		t.Fatalf("missing task %q in %+v", taskKey, byKey)
	}

	wantTaskID := runID + ":" + taskKey
	wantParentID := runID + ":" + parentKey
	if task.TaskID != wantTaskID || task.RunID != runID || task.Name != taskKey || task.Status != dal.TaskStatusPlanned {
		t.Fatalf("task %s mismatch: %+v", taskKey, task)
	}

	if task.ParentTaskID == nil || *task.ParentTaskID != wantParentID {
		t.Fatalf("task %s parent: got %+v, want %q", taskKey, task.ParentTaskID, wantParentID)
	}

	if !strings.HasPrefix(task.SpecHash, "sha256:") {
		t.Fatalf("task %s spec hash: %q", taskKey, task.SpecHash)
	}

	if len(task.Attempts) != 1 || task.Attempts[0].CellID != "pdx-b" || task.Attempts[0].Status != dal.TaskStatusPlanned {
		t.Fatalf("task %s attempts: %+v", taskKey, task.Attempts)
	}
}

type noopRunsRepository struct {
	dal.RunsRepository
}
