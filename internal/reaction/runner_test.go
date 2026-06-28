package reaction_test

import (
	"context"
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
		EventType:   "manual.notice",
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

func TestRunnerRunOnceMarksUnsupportedActionFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   "run.completed",
		PayloadJSON: []byte(`{"run_id":"run-1","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	const triggerJobAction = "builtins/trigger-job"
	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "trigger-job-target",
		Kind:       "job",
		Uses:       triggerJobAction,
		ConfigJSON: []byte(`{"job_id":"downstream"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := store.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/trigger-job"}`),
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

	if updated.LastError == nil || !strings.Contains(*updated.LastError, triggerJobAction) {
		t.Fatalf("last error: %v", updated.LastError)
	}
}
