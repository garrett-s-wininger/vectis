package reaction_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/reaction"
	"vectis/internal/testutil/dbtest"
)

func TestLocalNotifyActionExecuteRecordsDurableMessage(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := store.RecordEvent(ctx, dal.ReactionEventCreate{
		Source:      dal.ReactionEventSourceManual,
		EventType:   "manual.notice",
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"manual heads up"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "local-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"manual"}`),
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
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	action := &reaction.LocalNotifyAction{Store: store}
	if got := action.Type(); got != dal.ReactionActionNotifyLocal {
		t.Fatalf("action type: got %q want %q", got, dal.ReactionActionNotifyLocal)
	}

	message, err := action.Execute(ctx, reaction.LocalNotifyActionRequest{
		Event:      event,
		Invocation: invocation,
	})

	if err != nil {
		t.Fatalf("execute local action: %v", err)
	}

	if message.Mailbox != "manual" {
		t.Fatalf("mailbox: got %q want manual", message.Mailbox)
	}

	messages, _, err := store.ListLocalMessages(ctx, "manual", 0, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	if len(messages) != 1 || messages[0].MessageID != message.MessageID {
		t.Fatalf("messages: %+v, recorded=%+v", messages, message)
	}

	if string(messages[0].PayloadJSON) != string(event.PayloadJSON) {
		t.Fatalf("payload: got %s want %s", messages[0].PayloadJSON, event.PayloadJSON)
	}
}
