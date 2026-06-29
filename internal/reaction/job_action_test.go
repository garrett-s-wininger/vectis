package reaction_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/reaction"
	"vectis/internal/testutil/dbtest"
)

func TestTriggerJobActionExecuteCreatesAuditedRunsAndReusesOnRetry(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "source-job")
	createStoredJob(t, ctx, repos.Jobs(), "downstream-job")

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "source-job",
		RunID:       "source-run",
		PayloadJSON: []byte(`{"job_id":"source-job","run_id":"source-run","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "downstream-trigger",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"downstream-job","target_cell_ids":["iad-a","pdx-b"]}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := reactions.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/trigger-job"}`),
		TargetConfigJSON:     target.ConfigJSON,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	action := &reaction.TriggerJobAction{Store: newJobTriggerStore(repos)}
	first, err := action.Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if err != nil {
		t.Fatalf("execute trigger job action: %v", err)
	}

	if first.Reused {
		t.Fatal("first execution should create runs")
	}

	if first.TriggerInvocation.InvocationID != invocation.InvocationID {
		t.Fatalf("trigger invocation id: got %q want %q", first.TriggerInvocation.InvocationID, invocation.InvocationID)
	}

	if first.TriggerInvocation.TriggerType != dal.TriggerTypeReaction {
		t.Fatalf("trigger type: got %q want %q", first.TriggerInvocation.TriggerType, dal.TriggerTypeReaction)
	}

	if len(first.Runs) != 2 || first.Runs[0].TargetCellID != "iad-a" || first.Runs[1].TargetCellID != "pdx-b" {
		t.Fatalf("created runs: %+v", first.Runs)
	}

	run, err := repos.Runs().GetRun(ctx, first.Runs[0].RunID)
	if err != nil {
		t.Fatalf("get created run: %v", err)
	}

	if run.TriggerInvocationID == nil || *run.TriggerInvocationID != invocation.InvocationID {
		t.Fatalf("run trigger invocation id: got %+v want %q", run.TriggerInvocationID, invocation.InvocationID)
	}

	if run.TriggerType == nil || *run.TriggerType != dal.TriggerTypeReaction {
		t.Fatalf("run trigger type: got %+v want %q", run.TriggerType, dal.TriggerTypeReaction)
	}

	second, err := action.Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if err != nil {
		t.Fatalf("retry trigger job action: %v", err)
	}

	if !second.Reused {
		t.Fatal("retry should reuse existing runs")
	}

	if len(second.Runs) != len(first.Runs) || second.Runs[0].RunID != first.Runs[0].RunID || second.Runs[1].RunID != first.Runs[1].RunID {
		t.Fatalf("retry runs: got %+v want %+v", second.Runs, first.Runs)
	}

	existing, err := repos.Runs().ListRunsByTriggerInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("list runs by trigger invocation: %v", err)
	}

	if len(existing) != 2 {
		t.Fatalf("retry should not create duplicate runs, got %+v", existing)
	}
}

func TestRunnerRunOnceExecutesTriggerJobActionWhenStoreSupportsIt(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "runner-source-job")
	createStoredJob(t, ctx, repos.Jobs(), "runner-downstream-job")

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "runner-source-job",
		RunID:       "runner-source-run",
		PayloadJSON: []byte(`{"job_id":"runner-source-job","run_id":"runner-source-run","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "runner-downstream-trigger",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"runner-downstream-job"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := reactions.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/trigger-job"}`),
		TargetConfigJSON:     target.ConfigJSON,
		NextAttemptAt:        time.Now().UnixNano(),
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	summary, err := (&reaction.Runner{
		Store: newRunnerJobTriggerStore(repos),
		Owner: "trigger-job-runner",
	}).RunOnce(ctx, 10)

	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Succeeded != 1 || summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	updated, err := reactions.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if updated.Status != dal.ReactionInvocationStatusSucceeded {
		t.Fatalf("updated invocation: %+v", updated)
	}

	runs, err := repos.Runs().ListRunsByTriggerInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	if len(runs) != 1 {
		t.Fatalf("triggered runs: %+v", runs)
	}
}

func TestTriggerJobActionRejectsDirectSelfCycle(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "self-job")

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "self-job",
		PayloadJSON: []byte(`{"job_id":"self-job","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	invocation := createTriggerJobInvocation(t, ctx, reactions, event.EventID, "self-target", "self-job")
	_, err = (&reaction.TriggerJobAction{Store: newJobTriggerStore(repos)}).Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected self-cycle conflict, got %v", err)
	}
}

func TestTriggerJobActionRejectsConfiguredCycle(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "job-a")
	createStoredJob(t, ctx, repos.Jobs(), "job-b")

	targetA, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "target-a",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"job-a"}`),
	})

	if err != nil {
		t.Fatalf("create target a: %v", err)
	}

	if _, err := reactions.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  targetA.TargetID,
		Name:      "b-completion-triggers-a",
		EventType: dal.ReactionEventTypeRunCompleted,
		JobID:     "job-b",
	}); err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "job-a",
		PayloadJSON: []byte(`{"job_id":"job-a","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	invocation := createTriggerJobInvocation(t, ctx, reactions, event.EventID, "target-b", "job-b")
	_, err = (&reaction.TriggerJobAction{Store: newJobTriggerStore(repos)}).Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected configured cycle conflict, got %v", err)
	}
}

func TestTriggerJobActionRejectsWildcardCompletionSelfTrigger(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "job-a")
	createStoredJob(t, ctx, repos.Jobs(), "job-b")

	targetB, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "wildcard-target-b",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"job-b"}`),
	})

	if err != nil {
		t.Fatalf("create target b: %v", err)
	}

	if _, err := reactions.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  targetB.TargetID,
		Name:      "any-completion-triggers-b",
		EventType: dal.ReactionEventTypeRunCompleted,
	}); err != nil {
		t.Fatalf("create wildcard subscription: %v", err)
	}

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "job-a",
		PayloadJSON: []byte(`{"job_id":"job-a","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	invocation := createTriggerJobInvocation(t, ctx, reactions, event.EventID, "direct-target-b", "job-b")
	_, err = (&reaction.TriggerJobAction{Store: newJobTriggerStore(repos)}).Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected wildcard self-trigger conflict, got %v", err)
	}
}

func TestTriggerJobActionAllowsWildcardCompletionForDifferentTriggerType(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	reactions := repos.Reactions()
	ctx := context.Background()

	createStoredJob(t, ctx, repos.Jobs(), "manual-source")
	createStoredJob(t, ctx, repos.Jobs(), "manual-filtered-target")

	target, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "manual-filtered-target",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"manual-filtered-target"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	if _, err := reactions.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:    target.TargetID,
		Name:        "manual-completion-triggers-target",
		EventType:   dal.ReactionEventTypeRunCompleted,
		TriggerType: dal.TriggerTypeManual,
	}); err != nil {
		t.Fatalf("create manual-filtered subscription: %v", err)
	}

	event, err := reactions.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "manual-source",
		PayloadJSON: []byte(`{"job_id":"manual-source","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	invocation := createTriggerJobInvocation(t, ctx, reactions, event.EventID, "direct-manual-filtered-target", "manual-filtered-target")
	result, err := (&reaction.TriggerJobAction{Store: newJobTriggerStore(repos)}).Execute(ctx, reaction.TriggerJobActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if err != nil {
		t.Fatalf("manual-filtered wildcard should not block reaction trigger: %v", err)
	}

	if len(result.Runs) != 1 {
		t.Fatalf("created runs: %+v", result.Runs)
	}
}

func createStoredJob(t *testing.T, ctx context.Context, jobs dal.JobsRepository, jobID string) {
	t.Helper()
	if err := jobs.Create(ctx, jobID, `{"id":"`+jobID+`"}`, 1); err != nil {
		t.Fatalf("create stored job %s: %v", jobID, err)
	}
}

func createTriggerJobInvocation(t *testing.T, ctx context.Context, reactions dal.ReactionsRepository, eventID, targetName, targetJobID string) dal.ReactionInvocationRecord {
	t.Helper()
	target, err := reactions.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       targetName,
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"` + targetJobID + `"}`),
	})

	if err != nil {
		t.Fatalf("create target %s: %v", targetName, err)
	}

	invocation, err := reactions.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              eventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/trigger-job"}`),
		TargetConfigJSON:     target.ConfigJSON,
	})

	if err != nil {
		t.Fatalf("create invocation %s: %v", targetName, err)
	}

	return invocation
}

func newJobTriggerStore(repos *dal.SQLRepositories) *reaction.DALJobTriggerStore {
	return &reaction.DALJobTriggerStore{
		Jobs:      repos.Jobs(),
		Runs:      repos.Runs(),
		Triggers:  repos.TriggerInvocations(),
		Reactions: repos.Reactions(),
	}
}

type runnerJobTriggerStore struct {
	dal.ReactionsRepository
	*reaction.DALJobTriggerStore
}

func (s *runnerJobTriggerStore) ListJobTriggerEdges(ctx context.Context) ([]dal.ReactionJobTriggerEdge, error) {
	return s.DALJobTriggerStore.ListJobTriggerEdges(ctx)
}

func newRunnerJobTriggerStore(repos *dal.SQLRepositories) *runnerJobTriggerStore {
	return &runnerJobTriggerStore{
		ReactionsRepository: repos.Reactions(),
		DALJobTriggerStore:  newJobTriggerStore(repos),
	}
}
