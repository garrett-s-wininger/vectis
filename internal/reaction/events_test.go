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

func TestPublisherPublishManualNoticeCanUseSameNamespaceExplicitTarget(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	store := repos.Reactions()
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "manual-direct-team", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: ns.ID,
		Name:        "same-namespace-direct-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"same-namespace-direct"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	publication, err := (&reaction.Publisher{Store: store}).PublishManualNotice(ctx, reaction.ManualNotice{
		NamespaceID: ns.ID,
		TargetIDs:   []string{target.TargetID},
		Actor:       "operator",
		Message:     "same namespace target",
	})

	if err != nil {
		t.Fatalf("publish manual notice: %v", err)
	}

	if len(publication.DirectTargets) != 1 || len(publication.Invocations) != 1 {
		t.Fatalf("publication: %+v", publication)
	}
}

func TestPublisherPublishManualNoticeDedupesExplicitTargetIDs(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "duplicate-direct-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"duplicate-direct"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		TargetIDs: []string{target.TargetID, " " + target.TargetID + " "},
		Actor:     "operator",
		Message:   "dedupe direct target",
	})

	if err != nil {
		t.Fatalf("publish manual notice: %v", err)
	}

	if len(publication.DirectTargets) != 1 || len(publication.Invocations) != 1 {
		t.Fatalf("publication: %+v", publication)
	}
}

func TestPublisherPublishManualNoticeRejectsForeignNamespaceExplicitTargetBeforeEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	store := repos.Reactions()
	ctx := context.Background()

	nsA, err := repos.Namespaces().Create(ctx, "manual-team-a", nil)
	if err != nil {
		t.Fatalf("create namespace a: %v", err)
	}

	nsB, err := repos.Namespaces().Create(ctx, "manual-team-b", nil)
	if err != nil {
		t.Fatalf("create namespace b: %v", err)
	}

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsB.ID,
		Name:        "foreign-direct-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"foreign-direct"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	_, err = (&reaction.Publisher{Store: store}).PublishManualNotice(ctx, reaction.ManualNotice{
		EventID:     "manual-foreign-target",
		NamespaceID: nsA.ID,
		TargetIDs:   []string{target.TargetID},
		Actor:       "operator",
		Message:     "foreign namespace target should not create event",
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected foreign target to return conflict, got %v", err)
	}

	if _, err := store.GetEvent(ctx, "manual-foreign-target"); !dal.IsNotFound(err) {
		t.Fatalf("expected foreign-target publish to avoid event insert, got %v", err)
	}
}

func TestPublisherPublishManualNoticeRejectsMissingExplicitTargetBeforeEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	publisher := &reaction.Publisher{Store: store}
	_, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		EventID:   "manual-missing-target",
		TargetIDs: []string{"missing-target"},
		Actor:     "operator",
		Message:   "missing target should not create event",
	})

	if !dal.IsNotFound(err) {
		t.Fatalf("expected missing target to return not found, got %v", err)
	}

	if _, err := store.GetEvent(ctx, "manual-missing-target"); !dal.IsNotFound(err) {
		t.Fatalf("expected missing-target publish to avoid event insert, got %v", err)
	}
}

func TestPublisherPublishManualNoticeRejectsBlankExplicitTargetBeforeEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	publisher := &reaction.Publisher{Store: store}
	_, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		EventID:   "manual-blank-target",
		TargetIDs: []string{" "},
		Actor:     "operator",
		Message:   "blank target should not create event",
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected blank target to return conflict, got %v", err)
	}

	if _, err := store.GetEvent(ctx, "manual-blank-target"); !dal.IsNotFound(err) {
		t.Fatalf("expected blank-target publish to avoid event insert, got %v", err)
	}
}

func TestPublisherPublishManualNoticeIsIdempotentForEventID(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	if err := createLocalSubscription(ctx, store, "manual-idempotent-target", "manual-idempotent", dal.ReactionEventTypeManualNotice, "manual-idempotent"); err != nil {
		t.Fatal(err)
	}

	publisher := &reaction.Publisher{Store: store}
	first, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		EventID: "manual-event-idempotent",
		Actor:   "operator",
		Message: "same notice",
	})

	if err != nil {
		t.Fatalf("publish first notice: %v", err)
	}

	duplicate, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		EventID: "manual-event-idempotent",
		Actor:   "operator",
		Message: "same notice",
	})

	if err != nil {
		t.Fatalf("publish duplicate notice: %v", err)
	}

	if duplicate.Event.ID != first.Event.ID || len(duplicate.Invocations) != 1 || duplicate.Invocations[0].InvocationID != first.Invocations[0].InvocationID {
		t.Fatalf("duplicate publication: got %+v want %+v", duplicate, first)
	}

	if _, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		EventID: "manual-event-idempotent",
		Actor:   "operator",
		Message: "different notice",
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate notice, got %v", err)
	}
}

func TestPublisherPublishManualNoticeSkipsDisabledExplicitTarget(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	target, err := store.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "disabled-direct-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"disabled-direct"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	if _, err := store.SetTargetEnabled(ctx, target.TargetID, false); err != nil {
		t.Fatalf("disable target: %v", err)
	}

	publisher := &reaction.Publisher{Store: store}
	publication, err := publisher.PublishManualNotice(ctx, reaction.ManualNotice{
		TargetIDs: []string{target.TargetID},
		Actor:     "operator",
		Message:   "disabled target should not receive this",
	})

	if err != nil {
		t.Fatalf("publish manual notice: %v", err)
	}

	if len(publication.DirectTargets) != 0 || len(publication.Invocations) != 0 {
		t.Fatalf("publication: %+v", publication)
	}
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

func TestPublisherPublishRunCompletedRejectsNonTerminalStatusBeforeEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	publisher := &reaction.Publisher{Store: store}
	_, err := publisher.PublishRunCompleted(ctx, reaction.RunCompleted{
		EventID: "run-completed-non-terminal",
		JobID:   "job-a",
		RunID:   "run-queued",
		Status:  dal.RunStatusQueued,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected non-terminal status to return conflict, got %v", err)
	}

	if _, err := store.GetEvent(ctx, "run-completed-non-terminal"); !dal.IsNotFound(err) {
		t.Fatalf("expected non-terminal publish to avoid event insert, got %v", err)
	}
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

func TestPublisherPublishDefinitionValidationFailedRequiresJobIDBeforeEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	store := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	publisher := &reaction.Publisher{Store: store}
	_, err := publisher.PublishDefinitionValidationFailed(ctx, reaction.DefinitionValidationFailed{
		EventID: "validation-missing-job",
		Message: "definition is invalid",
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected missing job_id to return conflict, got %v", err)
	}

	if _, err := store.GetEvent(ctx, "validation-missing-job"); !dal.IsNotFound(err) {
		t.Fatalf("expected missing-job publish to avoid event insert, got %v", err)
	}
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
