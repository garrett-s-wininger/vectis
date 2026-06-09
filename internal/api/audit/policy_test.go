package audit

import "testing"

func TestPolicy_DurabilityFor_Defaults(t *testing.T) {
	p := DefaultPolicy()

	tests := map[string]Durability{
		EventTokenCreated:            DurabilityFailClosed,
		EventJobCreated:              DurabilityDurableBestEffort,
		EventAuthSuccess:             DurabilityBestEffort,
		"future.event":               DurabilityBestEffort,
		EventSetupCompleted:          DurabilityFailClosed,
		EventRunForceRequeued:        DurabilityDurableBestEffort,
		EventSourceRepositoryCreated: DurabilityDurableBestEffort,
		EventSourceRepositoryUpdated: DurabilityDurableBestEffort,
	}

	for eventType, want := range tests {
		if got := p.DurabilityFor(eventType); got != want {
			t.Fatalf("%s durability: got %s want %s", eventType, got, want)
		}
	}
}

func TestPolicy_DurabilityFor_Disabled(t *testing.T) {
	p := Policy{Enabled: false}
	if got := p.DurabilityFor(EventTokenCreated); got != DurabilityDisabled {
		t.Fatalf("disabled policy: got %s", got)
	}
}

func TestParseDurabilityOverrides(t *testing.T) {
	got, err := ParseDurabilityOverrides("token.created=best_effort,auth.success=disabled")
	if err != nil {
		t.Fatal(err)
	}

	if got[EventTokenCreated] != DurabilityBestEffort {
		t.Fatalf("token.created override: got %s", got[EventTokenCreated])
	}

	if got[EventAuthSuccess] != DurabilityDisabled {
		t.Fatalf("auth.success override: got %s", got[EventAuthSuccess])
	}
}

func TestParseDurabilityOverrides_RejectsUnknownEvent(t *testing.T) {
	if _, err := ParseDurabilityOverrides("nope=best_effort"); err == nil {
		t.Fatal("expected unknown event error")
	}
}
