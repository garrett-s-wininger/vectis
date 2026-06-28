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

	pending, err := repo.ListPendingInvocations(ctx, now, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}

	if len(pending) != 1 || pending[0].InvocationID != invocation.InvocationID {
		t.Fatalf("pending invocations: %+v", pending)
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
