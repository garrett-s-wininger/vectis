package reaction_test

import (
	"context"
	"encoding/json"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/reaction"
	"vectis/internal/testutil/dbtest"
)

func TestPublisherPublishCreatesInvocationForMatchingSubscription(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "local-published-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"published"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	subscription, err := store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "manual-notices",
		EventType: dal.ReactionEventTypeManualNotice,
	})

	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	if _, err := store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "manual-notices-copy",
		EventType: dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create duplicate-target subscription: %v", err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.Publish(ctx, dal.ReactionEventCreate{
		Source:      dal.ReactionEventSourceManual,
		EventType:   dal.ReactionEventTypeManualNotice,
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"published path"}`),
	})

	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if publication.Event.EventID == "" || publication.Event.Source != dal.ReactionEventSourceManual {
		t.Fatalf("event: %+v", publication.Event)
	}

	if len(publication.Matches) != 2 || publication.Matches[0].Subscription.SubscriptionID != subscription.SubscriptionID {
		t.Fatalf("matches: %+v", publication.Matches)
	}

	if len(publication.Invocations) != 1 {
		t.Fatalf("invocations: %+v", publication.Invocations)
	}

	invocation := publication.Invocations[0]
	if invocation.ActionUses != dal.ReactionActionNotifyLocal || string(invocation.TargetConfigJSON) != string(target.ConfigJSON) {
		t.Fatalf("invocation: %+v", invocation)
	}

	var descriptor struct {
		CanonicalName  string `json:"canonical_name"`
		SubscriptionID string `json:"subscription_id"`
	}

	if err := json.Unmarshal(invocation.ActionDescriptorJSON, &descriptor); err != nil {
		t.Fatalf("decode action descriptor: %v", err)
	}

	if descriptor.CanonicalName != dal.ReactionActionNotifyLocal || descriptor.SubscriptionID != subscription.SubscriptionID {
		t.Fatalf("descriptor: %+v", descriptor)
	}

	runner := &reaction.Runner{Store: store, Owner: "publisher-test"}
	summary, err := runner.RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Succeeded != 1 || summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}

	messages, _, err := store.ListLocalMessages(ctx, "published", 0, 10)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	if len(messages) != 1 || messages[0].InvocationID != invocation.InvocationID {
		t.Fatalf("messages: %+v", messages)
	}
}
