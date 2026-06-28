package reaction_test

import (
	"context"
	"encoding/json"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/reaction"
	"vectis/internal/testutil/dbtest"
)

func TestPublisherPublishManualNoticeUsesCanonicalEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	if err := createLocalSubscription(ctx, store, "manual-target", "manual-notices", dal.ReactionEventTypeManualNotice, "manual"); err != nil {
		t.Fatal(err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		Actor:   "operator",
		Message: "queue is degraded",
		Reason:  "draining cell",
	})

	if err != nil {
		t.Fatalf("publish manual notice: %v", err)
	}

	if publication.Event.Source != dal.ReactionEventSourceManual || publication.Event.EventType != dal.ReactionEventTypeManualNotice {
		t.Fatalf("event: %+v", publication.Event)
	}

	if publication.Event.Actor != "operator" {
		t.Fatalf("actor: got %q want operator", publication.Event.Actor)
	}

	var payload struct {
		Message  string `json:"message"`
		Reason   string `json:"reason"`
		Severity string `json:"severity"`
	}

	if err := json.Unmarshal(publication.Event.PayloadJSON, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}

	if payload.Message != "queue is degraded" || payload.Reason != "draining cell" || payload.Severity != "info" {
		t.Fatalf("payload: %+v", payload)
	}

	runLocalReactions(t, ctx, store)
	assertLocalMessageCount(t, ctx, store, "manual", 1)
}

func TestPublisherPublishManualNoticeCanUseExplicitTarget(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "direct-manual-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"direct"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		TargetIDs: []string{target.TargetID},
		Actor:     "operator",
		Message:   "send to direct target",
	})

	if err != nil {
		t.Fatalf("publish manual notice: %v", err)
	}

	if len(publication.Matches) != 0 {
		t.Fatalf("matches: %+v", publication.Matches)
	}

	if len(publication.DirectTargets) != 1 || publication.DirectTargets[0].TargetID != target.TargetID {
		t.Fatalf("direct targets: %+v", publication.DirectTargets)
	}

	if len(publication.Invocations) != 1 || publication.Invocations[0].TargetID != target.TargetID {
		t.Fatalf("invocations: %+v", publication.Invocations)
	}

	runLocalReactions(t, ctx, store)
	assertLocalMessageCount(t, ctx, store, "direct", 1)
}

func TestPublisherPublishRunCompletedMatchesSubscriptionMetadata(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositoriesWithCellID(db, "iad-a").Reactions()
	ctx := context.Background()

	completedTarget, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "completed-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"completed"}`),
	})

	if err != nil {
		t.Fatalf("create completed target: %v", err)
	}

	if _, err := store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:    completedTarget.TargetID,
		Name:        "completed-runs",
		EventType:   dal.ReactionEventTypeRunCompleted,
		RunStatus:   dal.RunStatusSucceeded,
		TriggerType: dal.TriggerTypeManual,
		OwningCell:  "iad-a",
	}); err != nil {
		t.Fatalf("create completed subscription: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "failed-run-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"failed"}`),
	})

	if err != nil {
		t.Fatalf("create failed target: %v", err)
	}

	if _, err := store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "failed-runs",
		EventType: dal.ReactionEventTypeRunCompleted,
		RunStatus: dal.RunStatusFailed,
	}); err != nil {
		t.Fatalf("create failed subscription: %v", err)
	}

	if _, err := store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:    target.TargetID,
		Name:        "wrong-cell",
		EventType:   dal.ReactionEventTypeRunCompleted,
		RunStatus:   dal.RunStatusSucceeded,
		TriggerType: dal.TriggerTypeManual,
		OwningCell:  "pdx-b",
	}); err != nil {
		t.Fatalf("create wrong cell subscription: %v", err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishRunCompleted(ctx, reaction.RunCompleted{
		JobID:       "job-a",
		RunID:       "run-1",
		Status:      dal.RunStatusSucceeded,
		TriggerType: dal.TriggerTypeManual,
		OwningCell:  "iad-a",
	})

	if err != nil {
		t.Fatalf("publish run completed: %v", err)
	}

	if publication.Event.Source != dal.ReactionEventSourceLifecycle || publication.Event.EventType != dal.ReactionEventTypeRunCompleted {
		t.Fatalf("event: %+v", publication.Event)
	}

	if publication.Event.JobID != "job-a" || publication.Event.RunID != "run-1" || publication.Event.SourceCell != "iad-a" {
		t.Fatalf("event metadata: %+v", publication.Event)
	}

	if len(publication.Invocations) != 1 {
		t.Fatalf("invocations: %+v", publication.Invocations)
	}

	runLocalReactions(t, ctx, store)
	assertLocalMessageCount(t, ctx, store, "completed", 1)
	assertLocalMessageCount(t, ctx, store, "failed", 0)
}

func TestPublisherPublishDefinitionValidationFailedHasNoRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	if err := createLocalSubscription(ctx, store, "validation-target", "validation-failures", dal.ReactionEventTypeDefinitionValidationFailed, "validation"); err != nil {
		t.Fatal(err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishDefinitionValidationFailed(ctx, reaction.DefinitionValidationFailed{
		JobID:          "job-b",
		Actor:          "developer",
		Message:        "definition is invalid",
		Reason:         "unknown action",
		DefinitionHash: "sha256:bad",
	})

	if err != nil {
		t.Fatalf("publish validation failure: %v", err)
	}

	if publication.Event.EventType != dal.ReactionEventTypeDefinitionValidationFailed || publication.Event.RunID != "" {
		t.Fatalf("event: %+v", publication.Event)
	}

	var payload struct {
		Message        string `json:"message"`
		Reason         string `json:"reason"`
		DefinitionHash string `json:"definition_hash"`
	}

	if err := json.Unmarshal(publication.Event.PayloadJSON, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}

	if payload.Message != "definition is invalid" || payload.Reason != "unknown action" || payload.DefinitionHash != "sha256:bad" {
		t.Fatalf("payload: %+v", payload)
	}

	runLocalReactions(t, ctx, store)
	assertLocalMessageCount(t, ctx, store, "validation", 1)
}

func createLocalSubscription(ctx context.Context, store dal.ReactionsRepository, targetName, subscriptionName, eventType, mailbox string) error {
	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       targetName,
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"` + mailbox + `"}`),
	})

	if err != nil {
		return err
	}

	_, err = store.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      subscriptionName,
		EventType: eventType,
	})

	return err
}

func runLocalReactions(t *testing.T, ctx context.Context, store dal.ReactionsRepository) {
	t.Helper()

	summary, err := (&reaction.Runner{Store: store, Owner: "events-test"}).RunOnce(ctx, 10)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}

	if summary.Failed != 0 {
		t.Fatalf("summary: %+v", summary)
	}
}

func assertLocalMessageCount(t *testing.T, ctx context.Context, store dal.ReactionsRepository, mailbox string, want int) {
	t.Helper()

	messages, _, err := store.ListLocalMessages(ctx, mailbox, 0, 10)
	if err != nil {
		t.Fatalf("list %s messages: %v", mailbox, err)
	}

	if len(messages) != want {
		t.Fatalf("%s messages: got %d want %d: %+v", mailbox, len(messages), want, messages)
	}
}
