package reaction_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/reaction"
	"vectis/internal/testutil/dbtest"
)

func TestRunnerRunOnceExecutesLocalNotification(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		Source:      dal.ReactionEventSourceManual,
		EventType:   dal.ReactionEventTypeManualNotice,
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"runner path"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "runner-local-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"runner"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
		NextAttemptAt:        time.Now().UnixNano(),
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	runner := &reaction.Runner{Store: store, Owner: "runner-test"}
	summary, err := runner.RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Scanned != 1 || summary.Claimed != 1 || summary.Succeeded != 1 || summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	updated, err := store.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if updated.Status != dal.ReactionInvocationStatusSucceeded || updated.Attempts != 1 || updated.CompletedAt == nil {
		t.Fatalf("updated invocation: %+v", updated)
	}

	messages, _, err := store.ListLocalMessages(ctx, "runner", 0, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	if len(messages) != 1 || messages[0].InvocationID != invocation.InvocationID {
		t.Fatalf("messages: %+v", messages)
	}

	if string(messages[0].PayloadJSON) != string(event.PayloadJSON) {
		t.Fatalf("payload: got %s want %s", messages[0].PayloadJSON, event.PayloadJSON)
	}
}

func TestRunnerRunOnceRetriesLocalNotificationWithoutDuplicateMessage(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		Source:      dal.ReactionEventSourceManual,
		EventType:   dal.ReactionEventTypeManualNotice,
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"retry local path"}`),
	})
	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "runner-local-retry-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"runner-retry"}`),
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          2,
		NextAttemptAt:        time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	action := &crashAfterLocalNotifyAction{store: store, failNext: true}
	runner := &reaction.Runner{
		Store:      store,
		Owner:      "runner-test",
		RetryDelay: time.Nanosecond,
		Actions:    []reaction.Action{action},
	}

	first, err := runner.RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("first run once: %v", err)
	}
	if first.Succeeded != 0 || first.Failed != 1 {
		t.Fatalf("first summary: %+v", first)
	}

	time.Sleep(time.Millisecond)
	second, err := runner.RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("second run once: %v", err)
	}
	if second.Succeeded != 1 || second.Failed != 0 {
		t.Fatalf("second summary: %+v", second)
	}

	updated, err := store.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}
	if updated.Status != dal.ReactionInvocationStatusSucceeded || updated.Attempts != 2 {
		t.Fatalf("updated invocation: %+v", updated)
	}

	messages, _, err := store.ListLocalMessages(ctx, "runner-retry", 0, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].InvocationID != invocation.InvocationID {
		t.Fatalf("messages: %+v", messages)
	}
}

func TestRunnerRunOnceFinalizesExpiredMaxAttemptsWithoutExecuting(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"expired max runner"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "expired-runner-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"expired-runner"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          1,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	claimed, err := store.MarkInvocationRunning(ctx, invocation.InvocationID, "stale-runner", time.Now().Add(-time.Second).UnixNano())
	if err != nil {
		t.Fatalf("mark running: %v", err)
	}

	if !claimed {
		t.Fatal("expected stale runner claim")
	}

	action := &countingAction{actionType: dal.ReactionActionNotifyLocal}
	summary, err := (&reaction.Runner{
		Store:   store,
		Owner:   "runner-test",
		Actions: []reaction.Action{action},
	}).RunOnce(ctx, 10)

	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Expired != 1 || summary.Scanned != 0 || summary.Claimed != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	if action.calls != 0 {
		t.Fatalf("action should not execute, calls=%d", action.calls)
	}

	updated, err := store.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if updated.Status != dal.ReactionInvocationStatusFailed {
		t.Fatalf("updated invocation: %+v", updated)
	}
}

func TestRunnerRunOnceMarksUnsupportedActionFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		PayloadJSON: []byte(`{"run_id":"run-1","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "trigger-job-target",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"downstream"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"` + dal.ReactionActionTriggerJob + `"}`),
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          1,
		NextAttemptAt:        time.Now().UnixNano(),
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	runner := &reaction.Runner{Store: store, Owner: "runner-test", RetryDelay: time.Nanosecond}
	summary, err := runner.RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Scanned != 1 || summary.Claimed != 1 || summary.Succeeded != 0 || summary.Failed != 1 {
		t.Fatalf("summary: %+v", summary)
	}

	updated, err := store.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if updated.Status != dal.ReactionInvocationStatusFailed {
		t.Fatalf("status: got %q want %q", updated.Status, dal.ReactionInvocationStatusFailed)
	}

	if updated.LastError == nil || !strings.Contains(*updated.LastError, dal.ReactionActionTriggerJob) {
		t.Fatalf("last error: %v", updated.LastError)
	}
}

type countingAction struct {
	actionType string
	calls      int
}

func (a *countingAction) Type() string {
	return a.actionType
}

func (a *countingAction) ExecuteInvocation(context.Context, reaction.ActionRequest) error {
	a.calls++
	return nil
}

type crashAfterLocalNotifyAction struct {
	store    dal.ReactionsRepository
	failNext bool
}

func (a *crashAfterLocalNotifyAction) Type() string {
	return dal.ReactionActionNotifyLocal
}

func (a *crashAfterLocalNotifyAction) ExecuteInvocation(ctx context.Context, req reaction.ActionRequest) error {
	if err := (&reaction.LocalNotifyAction{Store: a.store}).ExecuteInvocation(ctx, req); err != nil {
		return err
	}

	if a.failNext {
		a.failNext = false
		return fmt.Errorf("simulated crash after local notification write")
	}

	return nil
}
