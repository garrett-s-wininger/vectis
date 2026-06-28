package dal_test

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestReactionsRepository_DurableLocalInvocationFlow(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositoriesWithCellID(db, "iad-a").Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		Source:      dal.ReactionEventSourceManual,
		EventType:   "manual.notice",
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"build queue degraded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	if event.EventID == "" || event.SourceCell != "iad-a" || event.Source != dal.ReactionEventSourceManual {
		t.Fatalf("unexpected event: %+v", event)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "local-test",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"tests"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	if target.TargetID == "" || target.Uses != dal.ReactionActionNotifyLocal {
		t.Fatalf("unexpected target: %+v", target)
	}

	now := time.Now().UnixNano()
	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local","digest":"sha256:local"}`),
		ActionDigest:         "sha256:local",
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          3,
		NextAttemptAt:        now,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	if invocation.InvocationID == "" || invocation.Status != dal.ReactionInvocationStatusPending {
		t.Fatalf("unexpected invocation: %+v", invocation)
	}

	ready, err := repo.ListReadyInvocations(ctx, now, 10)
	if err != nil {
		t.Fatalf("list ready: %v", err)
	}

	if len(ready) != 1 || ready[0].InvocationID != invocation.InvocationID {
		t.Fatalf("ready invocations: %+v", ready)
	}

	claimed, err := repo.MarkInvocationRunning(ctx, invocation.InvocationID, "notifier-1", now+int64(time.Minute))
	if err != nil {
		t.Fatalf("mark running: %v", err)
	}

	if !claimed {
		t.Fatal("expected invocation claim")
	}

	message, err := repo.RecordLocalMessage(ctx, dal.ReactionLocalMessageCreate{
		EventID:      event.EventID,
		InvocationID: invocation.InvocationID,
		Mailbox:      "tests",
		PayloadJSON:  event.PayloadJSON,
	})

	if err != nil {
		t.Fatalf("record local message: %v", err)
	}

	if message.MessageID == "" || message.Mailbox != "tests" {
		t.Fatalf("unexpected local message: %+v", message)
	}

	if err := repo.MarkInvocationSucceeded(ctx, invocation.InvocationID, now+2); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	messages, next, err := repo.ListLocalMessages(ctx, "tests", 0, 10)
	if err != nil {
		t.Fatalf("list local messages: %v", err)
	}

	if len(messages) != 1 || next != messages[0].ID {
		t.Fatalf("messages=%+v next=%d", messages, next)
	}

	if string(messages[0].PayloadJSON) != string(event.PayloadJSON) {
		t.Fatalf("message payload: got %s want %s", messages[0].PayloadJSON, event.PayloadJSON)
	}
}

func TestReactionsRepository_ListReadyInvocationsIncludesExpiredClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   "run.completed",
		PayloadJSON: []byte(`{"run_id":"run-1","status":"succeeded"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "expired-claim-test",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"expired-claims"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	now := time.Now().UnixNano()
	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          3,
		NextAttemptAt:        now,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	claimed, err := repo.MarkInvocationRunning(ctx, invocation.InvocationID, "stale-runner", time.Now().Add(-time.Second).UnixNano())
	if err != nil {
		t.Fatalf("mark stale running: %v", err)
	}

	if !claimed {
		t.Fatal("expected stale runner claim")
	}

	ready, err := repo.ListReadyInvocations(ctx, time.Now().UnixNano(), 10)
	if err != nil {
		t.Fatalf("list ready: %v", err)
	}

	if len(ready) != 1 || ready[0].InvocationID != invocation.InvocationID || ready[0].Status != dal.ReactionInvocationStatusRunning {
		t.Fatalf("ready expired invocations: %+v", ready)
	}

	claimed, err = repo.MarkInvocationRunning(ctx, invocation.InvocationID, "fresh-runner", time.Now().Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("reclaim running: %v", err)
	}

	if !claimed {
		t.Fatal("expected expired invocation reclaim")
	}

	reclaimed, err := repo.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if reclaimed.ClaimedBy != "fresh-runner" || reclaimed.Attempts != 2 {
		t.Fatalf("reclaimed invocation: %+v", reclaimed)
	}
}

func TestReactionsRepository_RejectsInvalidJSON(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()

	_, err := repo.RecordEvent(context.Background(), dal.ReactionEventCreate{
		EventType:   "manual.notice",
		PayloadJSON: []byte(`not-json`),
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid payload JSON, got %v", err)
	}
}
