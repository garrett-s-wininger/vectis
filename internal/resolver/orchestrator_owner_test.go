package resolver

import (
	"context"
	"strconv"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/registry"
)

type fakeOrchestratorRegistry struct {
	entries []*api.RegistryEntry
}

func (f fakeOrchestratorRegistry) InstanceAddress(context.Context, api.Component, string) (string, error) {
	return "", nil
}

func (f fakeOrchestratorRegistry) ListRegistrations(_ context.Context, _ api.Component, metadata map[string]string) ([]*api.RegistryEntry, error) {
	if len(metadata) == 0 {
		return f.entries, nil
	}

	out := make([]*api.RegistryEntry, 0, len(f.entries))
	for _, entry := range f.entries {
		matches := true
		for key, value := range metadata {
			if entry.GetMetadata()[key] != value {
				matches = false
				break
			}
		}

		if matches {
			out = append(out, entry)
		}
	}

	return out, nil
}

func (f fakeOrchestratorRegistry) Close() error {
	return nil
}

func TestParseOrchestratorOwnerID(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		kind  OrchestratorOwnerKind
		value string
		ok    bool
	}{
		{
			name:  "pinned",
			raw:   OrchestratorPinnedOwnerID("127.0.0.1:8085"),
			kind:  OrchestratorOwnerPinned,
			value: "127.0.0.1:8085",
			ok:    true,
		},
		{
			name:  "registry",
			raw:   OrchestratorRegistryOwnerID("iad-a"),
			kind:  OrchestratorOwnerRegistry,
			value: "iad-a",
			ok:    true,
		},
		{
			name:  "instance",
			raw:   OrchestratorInstanceOwnerID("orch-1"),
			kind:  OrchestratorOwnerInstance,
			value: "orch-1",
			ok:    true,
		},
		{
			name:  "address",
			raw:   OrchestratorAddressOwnerID("orch-1:8085"),
			kind:  OrchestratorOwnerAddress,
			value: "orch-1:8085",
			ok:    true,
		},
		{name: "empty value", raw: "orchestrator:instance:", ok: false},
		{name: "unknown", raw: "worker:instance:worker-1", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseOrchestratorOwnerID(tt.raw)
			if ok != tt.ok {
				t.Fatalf("ok: got %v, want %v", ok, tt.ok)
			}

			if !tt.ok {
				return
			}

			if got.Kind != tt.kind || got.Value != tt.value {
				t.Fatalf("owner: got %+v, want kind=%q value=%q", got, tt.kind, tt.value)
			}
		})
	}
}

func TestSelectOrchestratorRegistryEntryUsesCellMetadataAndStableOrder(t *testing.T) {
	cellID := "iad-a"
	component := api.Component_COMPONENT_ORCHESTRATOR
	now := time.Now().Add(time.Minute).UnixNano()

	entry := func(instanceID, address, cell string) *api.RegistryEntry {
		return &api.RegistryEntry{
			Component:                &component,
			InstanceId:               &instanceID,
			Address:                  &address,
			LeaseExpiresUnixNano:     &now,
			TombstoneExpiresUnixNano: &now,
			Metadata: map[string]string{
				registry.MetadataCellID: cell,
			},
		}
	}

	reg := fakeOrchestratorRegistry{entries: []*api.RegistryEntry{
		entry("orch-b", "orch-b:8085", cellID),
		entry("orch-a", "orch-a:8085", cellID),
		entry("orch-c", "orch-c:8085", "sjc-c"),
	}}

	got, err := selectOrchestratorRegistryEntry(context.Background(), reg, cellID, "")
	if err != nil {
		t.Fatalf("select orchestrator registration: %v", err)
	}

	if got.GetInstanceId() != "orch-a" || got.GetAddress() != "orch-a:8085" {
		t.Fatalf("selected registration: got instance=%q address=%q", got.GetInstanceId(), got.GetAddress())
	}
}

func TestSelectLiveOrchestratorRegistrationUsesSelectionKey(t *testing.T) {
	component := api.Component_COMPONENT_ORCHESTRATOR
	entry := func(instanceID, address string) *api.RegistryEntry {
		return &api.RegistryEntry{
			Component:  &component,
			InstanceId: &instanceID,
			Address:    &address,
		}
	}

	entries := []*api.RegistryEntry{
		entry("orch-a", "orch-a:8085"),
		entry("orch-b", "orch-b:8085"),
	}

	selected := map[string]bool{}
	for i := 0; i < 100; i++ {
		got := selectLiveOrchestratorRegistration(entries, "worker-"+strconv.Itoa(i))
		if got == nil {
			t.Fatal("selection returned nil")
		}

		selected[got.GetInstanceId()] = true
	}

	if len(selected) != len(entries) {
		t.Fatalf("selection key did not distribute across registrations: got %v", selected)
	}
}
