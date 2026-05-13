package registry

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestRegMergeRejectsOlderVersion(t *testing.T) {
	now := time.Unix(100, 0)
	r := newReg("node-a", time.Minute, 5*time.Minute)

	first := r.register(api.Component_COMPONENT_QUEUE, "", "queue-old:1", nil, now)
	r.register(api.Component_COMPONENT_QUEUE, "", "queue-new:1", nil, now.Add(time.Second))

	stale := first
	stale.address = "queue-stale:1"
	r.mergeProtoEntries([]*api.RegistryEntry{registryEntryToProto(stale)}, now.Add(2*time.Second))

	got, ok := r.get(api.Component_COMPONENT_QUEUE, "", now.Add(2*time.Second))
	if !ok {
		t.Fatal("expected queue registration")
	}

	if got.address != "queue-new:1" {
		t.Fatalf("expected stale update to be ignored, got %q", got.address)
	}
}

func TestRegTombstonePreventsOldLiveRevival(t *testing.T) {
	now := time.Unix(100, 0)
	r := newReg("node-a", time.Second, 5*time.Minute)

	live := r.register(api.Component_COMPONENT_LOG, "", "log:1", nil, now)
	expiredAt := now.Add(2 * time.Second)
	snapshot := r.snapshotProtoEntries(expiredAt)

	if len(snapshot) != 1 || !snapshot[0].GetTombstone() {
		t.Fatalf("expected expired registration to produce tombstone, got %+v", snapshot)
	}

	live.leaseExpiresAt = expiredAt.Add(time.Minute)
	r.mergeProtoEntries([]*api.RegistryEntry{registryEntryToProto(live)}, expiredAt)
	if _, ok := r.get(api.Component_COMPONENT_LOG, "", expiredAt); ok {
		t.Fatal("expected old live entry to be blocked by tombstone")
	}
}

func TestRegTombstoneAllowsNewSponsorLiveEntry(t *testing.T) {
	now := time.Unix(100, 0)
	r := newReg("node-a", time.Second, 5*time.Minute)

	r.register(api.Component_COMPONENT_WORKER, "worker-1", "worker-old:1", nil, now)
	expiredAt := now.Add(2 * time.Second)
	r.snapshotProtoEntries(expiredAt)

	newSponsor := registrationEntry{
		component:      api.Component_COMPONENT_WORKER,
		instanceID:     "worker-1",
		address:        "worker-new:1",
		version:        registryVersion{originNodeID: "node-b", counter: 1},
		leaseExpiresAt: expiredAt.Add(time.Minute),
	}
	r.mergeProtoEntries([]*api.RegistryEntry{registryEntryToProto(newSponsor)}, expiredAt)

	got, ok := r.get(api.Component_COMPONENT_WORKER, "worker-1", expiredAt)
	if !ok {
		t.Fatal("expected new sponsor registration")
	}

	if got.address != "worker-new:1" {
		t.Fatalf("expected new sponsor address, got %q", got.address)
	}
}

func TestRegEntriesNewerThanDigestsSendsOnlyMissingOrNewer(t *testing.T) {
	now := time.Unix(100, 0)
	r := newReg("node-a", time.Minute, 5*time.Minute)
	entry := r.register(api.Component_COMPONENT_QUEUE, "", "queue:1", nil, now)

	if got := r.entriesNewerThanDigests(nil, now); len(got) != 1 {
		t.Fatalf("expected one missing entry, got %d", len(got))
	}

	currentDigest := registryEntryToDigest(registryEntryToProto(entry))
	if got := r.entriesNewerThanDigests([]*api.RegistryDigest{currentDigest}, now); len(got) != 0 {
		t.Fatalf("expected no entries for current digest, got %d", len(got))
	}

	oldCounter := uint64(0)
	oldDigest := &api.RegistryDigest{
		Component:    currentDigest.Component,
		InstanceId:   currentDigest.InstanceId,
		OriginNodeId: currentDigest.OriginNodeId,
		Counter:      &oldCounter,
		Tombstone:    currentDigest.Tombstone,
	}

	if got := r.entriesNewerThanDigests([]*api.RegistryDigest{oldDigest}, now); len(got) != 1 {
		t.Fatalf("expected one entry for old digest, got %d", len(got))
	}
}

func TestRegListEntriesFiltersByMetadata(t *testing.T) {
	now := time.Unix(100, 0)
	r := newReg("node-a", time.Minute, 5*time.Minute)

	r.register(api.Component_COMPONENT_QUEUE, "ingress", "queue-ingress:1", map[string]string{
		MetadataCellID:    DefaultCellID,
		MetadataQueueRole: QueueRoleIngress,
	}, now)

	r.register(api.Component_COMPONENT_QUEUE, "pool-linux", "queue-linux:1", map[string]string{
		MetadataCellID:    DefaultCellID,
		MetadataQueueRole: QueueRolePool,
		"pool":            "linux",
		"trait.os":        "linux",
	}, now)

	r.register(api.Component_COMPONENT_LOG, "log", "log:1", map[string]string{
		MetadataCellID: DefaultCellID,
	}, now)

	got := r.listEntries(api.Component_COMPONENT_QUEUE, map[string]string{MetadataQueueRole: QueueRolePool, "trait.os": "linux"}, now)
	if len(got) != 1 {
		t.Fatalf("expected one matching pool queue, got %d", len(got))
	}

	if got[0].address != "queue-linux:1" {
		t.Fatalf("expected pool queue address, got %q", got[0].address)
	}

	got[0].metadata["trait.os"] = "darwin"
	again := r.listEntries(api.Component_COMPONENT_QUEUE, map[string]string{MetadataQueueRole: QueueRolePool, "trait.os": "linux"}, now)
	if len(again) != 1 || again[0].metadata["trait.os"] != "linux" {
		t.Fatalf("listEntries returned mutable metadata: %+v", again)
	}

	if got := r.listEntries(api.Component_COMPONENT_QUEUE, map[string]string{"trait.missing": ""}, now); len(got) != 0 {
		t.Fatalf("expected missing metadata key not to match empty value, got %+v", got)
	}
}

func TestSponsorOrderedRegistryAddressIsStable(t *testing.T) {
	addresses := "reg-a:8082,reg-b:8082,reg-c:8082"
	first := sponsorOrderedRegistryAddress(addresses, api.Component_COMPONENT_QUEUE, "", "queue:8081")
	second := sponsorOrderedRegistryAddress(addresses, api.Component_COMPONENT_QUEUE, "", "queue:8081")

	if first != second {
		t.Fatalf("expected stable sponsor order, got %q then %q", first, second)
	}

	if len(splitRegistryAddresses(first)) != 3 {
		t.Fatalf("expected all registry addresses to remain in failover set, got %q", first)
	}
}

func TestRegistryServerGossipReplicatesDelta(t *testing.T) {
	now := time.Now()
	sponsor := NewRegistryServiceWithOptions(mocks.NopLogger{}, ServiceOptions{
		NodeID:       "node-a",
		LeaseTTL:     time.Minute,
		TombstoneTTL: 5 * time.Minute,
	})

	peer := NewRegistryServiceWithOptions(mocks.NopLogger{}, ServiceOptions{
		NodeID:       "node-b",
		LeaseTTL:     time.Minute,
		TombstoneTTL: 5 * time.Minute,
	})

	component := api.Component_COMPONENT_QUEUE
	address := "queue:8081"
	metadata := map[string]string{
		MetadataCellID:    DefaultCellID,
		MetadataQueueRole: QueueRoleIngress,
	}

	if _, err := sponsor.Register(contextForTest(t), &api.Registration{Component: &component, Address: &address, Metadata: metadata}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	entries := sponsor.reg.drainDirtyProtoEntries(now)
	if len(entries) != 1 {
		t.Fatalf("expected one dirty delta, got %d", len(entries))
	}

	if _, err := peer.Gossip(contextForTest(t), &api.GossipRequest{Entries: entries}); err != nil {
		t.Fatalf("Gossip: %v", err)
	}

	got, ok := peer.reg.get(api.Component_COMPONENT_QUEUE, "", now)
	if !ok {
		t.Fatal("expected replicated queue entry")
	}

	if got.address != address {
		t.Fatalf("expected %q, got %q", address, got.address)
	}

	if got.metadata[MetadataQueueRole] != QueueRoleIngress {
		t.Fatalf("expected replicated queue metadata, got %+v", got.metadata)
	}
}

func TestRegistryServerSnapshotRepairsMissedGossip(t *testing.T) {
	now := time.Now()
	sponsor := NewRegistryServiceWithOptions(mocks.NopLogger{}, ServiceOptions{
		NodeID:       "node-a",
		LeaseTTL:     time.Minute,
		TombstoneTTL: 5 * time.Minute,
	})

	peer := NewRegistryServiceWithOptions(mocks.NopLogger{}, ServiceOptions{
		NodeID:       "node-b",
		LeaseTTL:     time.Minute,
		TombstoneTTL: 5 * time.Minute,
	})

	component := api.Component_COMPONENT_LOG
	address := "log:8083"
	if _, err := sponsor.Register(contextForTest(t), &api.Registration{Component: &component, Address: &address}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	resp, err := sponsor.GetSnapshot(contextForTest(t), &api.RegistrySnapshotRequest{
		Digests: peer.reg.digestProtoEntries(now),
	})

	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}

	if len(resp.GetEntries()) != 1 {
		t.Fatalf("expected one snapshot entry, got %d", len(resp.GetEntries()))
	}

	peer.reg.mergeProtoEntries(resp.GetEntries(), now)
	got, ok := peer.reg.get(api.Component_COMPONENT_LOG, "", now)
	if !ok {
		t.Fatal("expected anti-entropy repair to install log entry")
	}

	if got.address != address {
		t.Fatalf("expected %q, got %q", address, got.address)
	}
}

func contextForTest(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}
