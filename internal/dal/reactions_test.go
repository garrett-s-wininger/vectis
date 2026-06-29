package dal_test

import (
	"context"
	"strings"
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
		EventType:   dal.ReactionEventTypeManualNotice,
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

	if err := repo.MarkInvocationSucceeded(ctx, invocation.InvocationID, "notifier-2", now+1); !dal.IsNotFound(err) {
		t.Fatalf("expected wrong owner to miss running invocation, got %v", err)
	}

	if err := repo.MarkInvocationSucceeded(ctx, invocation.InvocationID, "notifier-1", now+2); err != nil {
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

func TestReactionsRepository_ValidatesTargetActionPairs(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	if _, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "job-trigger-target",
		Kind:       dal.ReactionTargetKindJob,
		Uses:       dal.ReactionActionTriggerJob,
		ConfigJSON: []byte(`{"job_id":"downstream"}`),
	}); err != nil {
		t.Fatalf("create job target: %v", err)
	}

	tests := []struct {
		name string
		kind string
		uses string
	}{
		{
			name: "unsupported-kind",
			kind: "webhook",
			uses: dal.ReactionActionNotifyLocal,
		},
		{
			name: "local-trigger-job",
			kind: dal.ReactionTargetKindLocal,
			uses: dal.ReactionActionTriggerJob,
		},
		{
			name: "job-notify-local",
			kind: dal.ReactionTargetKindJob,
			uses: dal.ReactionActionNotifyLocal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
				Name:       tt.name,
				Kind:       tt.kind,
				Uses:       tt.uses,
				ConfigJSON: []byte(`{}`),
			})

			if !dal.IsConflict(err) {
				t.Fatalf("expected conflict, got %v", err)
			}
		})
	}
}

func TestReactionsRepository_CreateInvocationRejectsTargetActionMismatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"mismatched invocation"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "mismatch-local-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"mismatch"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	_, err = repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           dal.ReactionActionTriggerJob,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/trigger-job"}`),
		TargetConfigJSON:     target.ConfigJSON,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestReactionsRepository_CreateInvocationRequiresMatchingTargetConfig(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"target config mismatch"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "config-mismatch-local-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"expected"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	_, err = repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     []byte(`{"mailbox":"different"}`),
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestReactionsRepository_RejectsInvalidEventAndSubscriptionMetadata(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	if _, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		Source:      "manualish",
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"invalid source"}`),
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid event source, got %v", err)
	}

	if _, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   "run.finished",
		PayloadJSON: []byte(`{"message":"invalid event type"}`),
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid event type, got %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "metadata-validation-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"metadata-validation"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "invalid-event-type-filter",
		EventType: "run.finished",
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid subscription event type, got %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "invalid-run-status-filter",
		EventType: dal.ReactionEventTypeRunCompleted,
		RunStatus: "done",
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid run status filter, got %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:    target.TargetID,
		Name:        "invalid-trigger-type-filter",
		EventType:   dal.ReactionEventTypeRunCompleted,
		TriggerType: "timer",
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid trigger type filter, got %v", err)
	}
}

func TestReactionsRepository_TargetAndSubscriptionNamesAreScoped(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repo := repos.Reactions()
	ctx := context.Background()

	nsA, err := repos.Namespaces().Create(ctx, "reaction-name-team-a", nil)
	if err != nil {
		t.Fatalf("create namespace a: %v", err)
	}

	nsB, err := repos.Namespaces().Create(ctx, "reaction-name-team-b", nil)
	if err != nil {
		t.Fatalf("create namespace b: %v", err)
	}

	globalTarget, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "shared-name-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"global"}`),
	})

	if err != nil {
		t.Fatalf("create global target: %v", err)
	}

	if _, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "shared-name-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"duplicate-global"}`),
	}); !dal.IsConflict(err) {
		t.Fatalf("expected duplicate global target conflict, got %v", err)
	}

	targetA, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsA.ID,
		Name:        "shared-name-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-a"}`),
	})

	if err != nil {
		t.Fatalf("create namespace target a: %v", err)
	}

	if _, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsB.ID,
		Name:        "shared-name-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-b"}`),
	}); err != nil {
		t.Fatalf("same target name in different namespace should be allowed: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  globalTarget.TargetID,
		Name:      "shared-name-subscription",
		EventType: dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create global subscription: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  globalTarget.TargetID,
		Name:      "shared-name-subscription",
		EventType: dal.ReactionEventTypeManualNotice,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected duplicate global subscription conflict, got %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		NamespaceID: nsA.ID,
		TargetID:    targetA.TargetID,
		Name:        "shared-name-subscription",
		EventType:   dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("same subscription name in namespace should be allowed: %v", err)
	}
}

func TestReactionsRepository_RecordEventIsIdempotentForSameEventID(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	create := dal.ReactionEventCreate{
		EventID:     "event-idempotent",
		Source:      dal.ReactionEventSourceManual,
		EventType:   dal.ReactionEventTypeManualNotice,
		Actor:       "operator",
		PayloadJSON: []byte(`{"message":"same event","severity":"info"}`),
	}

	first, err := repo.RecordEvent(ctx, create)
	if err != nil {
		t.Fatalf("record first event: %v", err)
	}

	duplicateCreate := create
	duplicateCreate.PayloadJSON = []byte(`{
		"severity": "info",
		"message": "same event"
	}`)

	duplicate, err := repo.RecordEvent(ctx, duplicateCreate)
	if err != nil {
		t.Fatalf("record duplicate event: %v", err)
	}

	if duplicate.ID != first.ID || duplicate.EventID != first.EventID {
		t.Fatalf("duplicate event: got %+v want %+v", duplicate, first)
	}

	if string(first.PayloadJSON) != `{"message":"same event","severity":"info"}` {
		t.Fatalf("payload should be canonical JSON, got %s", first.PayloadJSON)
	}

	create.PayloadJSON = []byte(`{"message":"different event"}`)
	if _, err := repo.RecordEvent(ctx, create); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate event, got %v", err)
	}
}

func TestReactionsRepository_CreateInvocationIsIdempotentForSameEventTarget(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"same invocation"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "idempotent-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"idempotent","priority":"normal"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	create := dal.ReactionInvocationCreate{
		InvocationID:         "invocation-idempotent",
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local","labels":{"team":"ci"}}`),
		TargetConfigJSON:     []byte(`{"mailbox":"idempotent","priority":"normal"}`),
		MaxAttempts:          3,
	}

	first, err := repo.CreateInvocation(ctx, create)
	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	create.InvocationID = "invocation-idempotent-retry"
	create.ActionDescriptorJSON = []byte(`{
		"labels": {"team": "ci"},
		"canonical_name": "builtins/notify-local"
	}`)

	create.TargetConfigJSON = []byte(`{
		"priority": "normal",
		"mailbox": "idempotent"
	}`)

	duplicate, err := repo.CreateInvocation(ctx, create)
	if err != nil {
		t.Fatalf("create duplicate invocation: %v", err)
	}

	if duplicate.InvocationID != first.InvocationID || duplicate.ID != first.ID {
		t.Fatalf("duplicate invocation: got %+v want %+v", duplicate, first)
	}

	if string(first.ActionDescriptorJSON) != `{"canonical_name":"builtins/notify-local","labels":{"team":"ci"}}` {
		t.Fatalf("descriptor should be canonical JSON, got %s", first.ActionDescriptorJSON)
	}
	if string(first.TargetConfigJSON) != `{"mailbox":"idempotent","priority":"normal"}` {
		t.Fatalf("target config should be canonical JSON, got %s", first.TargetConfigJSON)
	}

	create.ActionDescriptorJSON = []byte(`{"canonical_name":"builtins/notify-local","variant":"different"}`)
	if _, err := repo.CreateInvocation(ctx, create); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate invocation, got %v", err)
	}
}

func TestReactionsRepository_RecordLocalMessageIsIdempotentForInvocation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"same local message"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "local-idempotent-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"local-idempotent"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	create := dal.ReactionLocalMessageCreate{
		MessageID:    "local-message-idempotent",
		EventID:      event.EventID,
		InvocationID: invocation.InvocationID,
		Mailbox:      "local-idempotent",
		PayloadJSON:  []byte(`{"message":"same local message","severity":"info"}`),
	}

	first, err := repo.RecordLocalMessage(ctx, create)
	if err != nil {
		t.Fatalf("record local message: %v", err)
	}

	create.MessageID = "local-message-idempotent-retry"
	create.PayloadJSON = []byte(`{
		"severity": "info",
		"message": "same local message"
	}`)

	duplicate, err := repo.RecordLocalMessage(ctx, create)
	if err != nil {
		t.Fatalf("record duplicate local message: %v", err)
	}

	if duplicate.MessageID != first.MessageID || duplicate.ID != first.ID {
		t.Fatalf("duplicate local message: got %+v want %+v", duplicate, first)
	}

	if string(first.PayloadJSON) != `{"message":"same local message","severity":"info"}` {
		t.Fatalf("local message payload should be canonical JSON, got %s", first.PayloadJSON)
	}

	create.PayloadJSON = []byte(`{"message":"different local message"}`)
	if _, err := repo.RecordLocalMessage(ctx, create); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate local message, got %v", err)
	}
}

func TestReactionsRepository_RecordLocalMessageRejectsInvocationEventMismatch(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	eventA, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"event a"}`),
	})

	if err != nil {
		t.Fatalf("record event a: %v", err)
	}

	eventB, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"event b"}`),
	})

	if err != nil {
		t.Fatalf("record event b: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "local-message-mismatch-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"mismatch"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              eventA.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	_, err = repo.RecordLocalMessage(ctx, dal.ReactionLocalMessageCreate{
		EventID:      eventB.EventID,
		InvocationID: invocation.InvocationID,
		Mailbox:      "mismatch",
		PayloadJSON:  eventB.PayloadJSON,
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestReactionsRepository_ListMatchingSubscriptionsFiltersEventMetadata(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositoriesWithCellID(db, "iad-a").Reactions()
	ctx := context.Background()

	matchingTarget, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "matching-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"matches"}`),
	})

	if err != nil {
		t.Fatalf("create matching target: %v", err)
	}

	ignoredTarget, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "ignored-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"ignored"}`),
	})

	if err != nil {
		t.Fatalf("create ignored target: %v", err)
	}

	matchingSubscription, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:    matchingTarget.TargetID,
		Name:        "succeeded-manual-run",
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "job-a",
		RunStatus:   dal.RunStatusSucceeded,
		TriggerType: dal.TriggerTypeManual,
		OwningCell:  "iad-a",
	})

	if err != nil {
		t.Fatalf("create matching subscription: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  ignoredTarget.TargetID,
		Name:      "failed-run",
		EventType: dal.ReactionEventTypeRunCompleted,
		JobID:     "job-a",
		RunStatus: dal.RunStatusFailed,
	}); err != nil {
		t.Fatalf("create ignored subscription: %v", err)
	}

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
		JobID:       "job-a",
		PayloadJSON: []byte(`{"status":"succeeded","trigger_type":"manual"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	matches, err := repo.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		t.Fatalf("list matching subscriptions: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("matches: %+v", matches)
	}

	if matches[0].Subscription.SubscriptionID != matchingSubscription.SubscriptionID {
		t.Fatalf("subscription: got %+v want %+v", matches[0].Subscription, matchingSubscription)
	}

	if matches[0].Target.TargetID != matchingTarget.TargetID || string(matches[0].Target.ConfigJSON) != string(matchingTarget.ConfigJSON) {
		t.Fatalf("target: got %+v want %+v", matches[0].Target, matchingTarget)
	}
}

func TestReactionsRepository_ListMatchingSubscriptionsSkipsForeignNamespaceTargets(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repo := repos.Reactions()
	ctx := context.Background()

	nsA, err := repos.Namespaces().Create(ctx, "reaction-team-a", nil)
	if err != nil {
		t.Fatalf("create namespace a: %v", err)
	}

	nsB, err := repos.Namespaces().Create(ctx, "reaction-team-b", nil)
	if err != nil {
		t.Fatalf("create namespace b: %v", err)
	}

	targetA, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsA.ID,
		Name:        "team-a-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-a"}`),
	})

	if err != nil {
		t.Fatalf("create target a: %v", err)
	}

	targetB, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsB.ID,
		Name:        "team-b-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-b"}`),
	})

	if err != nil {
		t.Fatalf("create target b: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  targetA.TargetID,
		Name:      "team-a-subscription",
		EventType: dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create subscription a: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  targetB.TargetID,
		Name:      "team-b-subscription",
		EventType: dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create subscription b: %v", err)
	}

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		NamespaceID: nsA.ID,
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"namespace scoped"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	matches, err := repo.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		t.Fatalf("list matches: %v", err)
	}

	if len(matches) != 1 || matches[0].Target.TargetID != targetA.TargetID {
		t.Fatalf("matches: %+v", matches)
	}
}

func TestReactionsRepository_CreateSubscriptionRejectsForeignNamespaceTarget(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repo := repos.Reactions()
	ctx := context.Background()

	nsA, err := repos.Namespaces().Create(ctx, "reaction-sub-team-a", nil)
	if err != nil {
		t.Fatalf("create namespace a: %v", err)
	}

	nsB, err := repos.Namespaces().Create(ctx, "reaction-sub-team-b", nil)
	if err != nil {
		t.Fatalf("create namespace b: %v", err)
	}

	globalTarget, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "subscription-global-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"global"}`),
	})

	if err != nil {
		t.Fatalf("create global target: %v", err)
	}

	targetA, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsA.ID,
		Name:        "subscription-team-a-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-a"}`),
	})

	if err != nil {
		t.Fatalf("create target a: %v", err)
	}

	targetB, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		NamespaceID: nsB.ID,
		Name:        "subscription-team-b-target",
		Kind:        dal.ReactionTargetKindLocal,
		Uses:        dal.ReactionActionNotifyLocal,
		ConfigJSON:  []byte(`{"mailbox":"team-b"}`),
	})

	if err != nil {
		t.Fatalf("create target b: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		NamespaceID: nsA.ID,
		TargetID:    globalTarget.TargetID,
		Name:        "team-a-global-target",
		EventType:   dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create namespace subscription to global target: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		NamespaceID: nsA.ID,
		TargetID:    targetA.TargetID,
		Name:        "team-a-target",
		EventType:   dal.ReactionEventTypeManualNotice,
	}); err != nil {
		t.Fatalf("create namespace subscription to same target: %v", err)
	}

	if _, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		NamespaceID: nsA.ID,
		TargetID:    targetB.TargetID,
		Name:        "team-a-foreign-target",
		EventType:   dal.ReactionEventTypeManualNotice,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected foreign target conflict, got %v", err)
	}
}

func TestReactionsRepository_ListMatchingSubscriptionsSkipsDisabledRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "disable-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"disabled"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	subscription, err := repo.CreateSubscription(ctx, dal.ReactionSubscriptionCreate{
		TargetID:  target.TargetID,
		Name:      "disable-subscription",
		EventType: dal.ReactionEventTypeManualNotice,
	})

	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"disable check"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	matches, err := repo.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		t.Fatalf("list matches: %v", err)
	}

	if len(matches) != 1 {
		t.Fatalf("enabled matches: %+v", matches)
	}

	if _, err := repo.SetSubscriptionEnabled(ctx, subscription.SubscriptionID, false); err != nil {
		t.Fatalf("disable subscription: %v", err)
	}

	matches, err = repo.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		t.Fatalf("list matches after subscription disable: %v", err)
	}

	if len(matches) != 0 {
		t.Fatalf("disabled subscription matches: %+v", matches)
	}

	if _, err := repo.SetSubscriptionEnabled(ctx, subscription.SubscriptionID, true); err != nil {
		t.Fatalf("enable subscription: %v", err)
	}

	if _, err := repo.SetTargetEnabled(ctx, target.TargetID, false); err != nil {
		t.Fatalf("disable target: %v", err)
	}

	matches, err = repo.ListMatchingSubscriptions(ctx, event)
	if err != nil {
		t.Fatalf("list matches after target disable: %v", err)
	}

	if len(matches) != 0 {
		t.Fatalf("disabled target matches: %+v", matches)
	}
}

func TestReactionsRepository_ListReadyInvocationsIncludesExpiredClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeRunCompleted,
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

func TestReactionsRepository_MarkExpiredInvocationsFailedFinalizesMaxedClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"expired max attempts"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "expired-max-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"expired-max"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
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

	if len(ready) != 0 {
		t.Fatalf("maxed expired claim should not be ready: %+v", ready)
	}

	expired, err := repo.MarkExpiredInvocationsFailed(ctx, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("mark expired failed: %v", err)
	}

	if expired != 1 {
		t.Fatalf("expired count: got %d want 1", expired)
	}

	final, err := repo.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get invocation: %v", err)
	}

	if final.Status != dal.ReactionInvocationStatusFailed || final.Attempts != 1 || final.ClaimedBy != "" || final.ClaimUntil != nil {
		t.Fatalf("final invocation: %+v", final)
	}

	if final.LastError == nil || !strings.Contains(*final.LastError, "claim expired") {
		t.Fatalf("last error: %v", final.LastError)
	}
}

func TestReactionsRepository_MarkInvocationFailedRetriesThenFinalizesAndCapsError(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()
	ctx := context.Background()

	event, err := repo.RecordEvent(ctx, dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`{"message":"retry check"}`),
	})

	if err != nil {
		t.Fatalf("record event: %v", err)
	}

	target, err := repo.CreateTarget(ctx, dal.ReactionTargetCreate{
		Name:       "retry-target",
		Kind:       dal.ReactionTargetKindLocal,
		Uses:       dal.ReactionActionNotifyLocal,
		ConfigJSON: []byte(`{"mailbox":"retry"}`),
	})

	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	invocation, err := repo.CreateInvocation(ctx, dal.ReactionInvocationCreate{
		EventID:              event.EventID,
		TargetID:             target.TargetID,
		ActionUses:           target.Uses,
		ActionDescriptorJSON: []byte(`{"canonical_name":"builtins/notify-local"}`),
		TargetConfigJSON:     target.ConfigJSON,
		MaxAttempts:          2,
	})

	if err != nil {
		t.Fatalf("create invocation: %v", err)
	}

	claimed, err := repo.MarkInvocationRunning(ctx, invocation.InvocationID, "retry-1", time.Now().Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("claim first: %v", err)
	}

	if !claimed {
		t.Fatal("expected first claim")
	}

	longError := strings.Repeat("x", dal.ReactionInvocationLastErrorMaxLength+512)
	if err := repo.MarkInvocationFailed(ctx, invocation.InvocationID, "retry-1", longError, time.Now().Add(-time.Second).UnixNano()); err != nil {
		t.Fatalf("mark first failed: %v", err)
	}

	retryable, err := repo.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get retryable invocation: %v", err)
	}

	if retryable.Status != dal.ReactionInvocationStatusPending || retryable.Attempts != 1 {
		t.Fatalf("retryable invocation: %+v", retryable)
	}

	if retryable.LastError == nil || len([]rune(*retryable.LastError)) > dal.ReactionInvocationLastErrorMaxLength || !strings.HasSuffix(*retryable.LastError, "...(truncated)") {
		t.Fatalf("last error was not capped: %v", retryable.LastError)
	}

	claimed, err = repo.MarkInvocationRunning(ctx, invocation.InvocationID, "retry-2", time.Now().Add(time.Minute).UnixNano())
	if err != nil {
		t.Fatalf("claim second: %v", err)
	}

	if !claimed {
		t.Fatal("expected second claim")
	}

	if err := repo.MarkInvocationFailed(ctx, invocation.InvocationID, "retry-2", "still failing", time.Now().Add(-time.Second).UnixNano()); err != nil {
		t.Fatalf("mark second failed: %v", err)
	}

	final, err := repo.GetInvocation(ctx, invocation.InvocationID)
	if err != nil {
		t.Fatalf("get final invocation: %v", err)
	}

	if final.Status != dal.ReactionInvocationStatusFailed || final.Attempts != 2 {
		t.Fatalf("final invocation: %+v", final)
	}
}

func TestReactionsRepository_RejectsInvalidJSON(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repo := dal.NewSQLRepositories(db).Reactions()

	_, err := repo.RecordEvent(context.Background(), dal.ReactionEventCreate{
		EventType:   dal.ReactionEventTypeManualNotice,
		PayloadJSON: []byte(`not-json`),
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict for invalid payload JSON, got %v", err)
	}
}
