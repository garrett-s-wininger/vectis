package registry

import (
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
)

type registryVersion struct {
	originNodeID string
	counter      uint64
}

type registrationEntry struct {
	component          api.Component
	instanceID         string
	address            string
	metadata           map[string]string
	version            registryVersion
	leaseExpiresAt     time.Time
	tombstone          bool
	tombstoneExpiresAt time.Time
}

type registrationChange int

const (
	registrationChangeNew registrationChange = iota
	registrationChangeUpdated
	registrationChangeRenewed
)

type reg struct {
	mu            sync.RWMutex
	nodeID        string
	leaseTTL      time.Duration
	tombstoneTTL  time.Duration
	clock         uint64
	registrations map[string]registrationEntry
	tombstones    map[string]registrationEntry
	dirty         map[string]registrationEntry
}

func newReg(nodeID string, leaseTTL, tombstoneTTL time.Duration) *reg {
	return &reg{
		nodeID:        nodeID,
		leaseTTL:      leaseTTL,
		tombstoneTTL:  tombstoneTTL,
		registrations: make(map[string]registrationEntry),
		tombstones:    make(map[string]registrationEntry),
		dirty:         make(map[string]registrationEntry),
	}
}

func makeRegKey(component api.Component, instanceID string) string {
	return component.String() + ":" + instanceID
}

func makeEntryKey(entry registrationEntry) string {
	return makeRegKey(entry.component, entry.instanceID)
}

func makeVersionKey(component api.Component, instanceID, originNodeID string) string {
	return makeRegKey(component, instanceID) + "\x00" + originNodeID
}

func makeEntryVersionKey(entry registrationEntry) string {
	return makeVersionKey(entry.component, entry.instanceID, entry.version.originNodeID)
}

func cleanPeerAddresses(peers []string, self string) []string {
	cleaned := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, peer := range peers {
		for part := range strings.SplitSeq(peer, ",") {
			part = strings.TrimSpace(part)
			if part == "" || part == self {
				continue
			}

			if _, ok := seen[part]; ok {
				continue
			}

			seen[part] = struct{}{}
			cleaned = append(cleaned, part)
		}
	}

	sort.Strings(cleaned)
	return cleaned
}

func (r *reg) register(component api.Component, instanceID, address string, metadata map[string]string, now time.Time) registrationEntry {
	entry, _ := r.registerWithChange(component, instanceID, address, metadata, now)
	return entry
}

func (r *reg) registerWithChange(component api.Component, instanceID, address string, metadata map[string]string, now time.Time) (registrationEntry, registrationChange) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireLeasesLocked(now)
	change := registrationChangeNew
	if current, ok := r.registrations[makeRegKey(component, instanceID)]; ok && current.isLiveAt(now) {
		change = registrationChangeUpdated
		if current.address == address && maps.Equal(current.metadata, metadata) {
			change = registrationChangeRenewed
		}
	}

	r.clock++
	entry := registrationEntry{
		component:      component,
		instanceID:     instanceID,
		address:        address,
		metadata:       cloneMetadata(metadata),
		version:        registryVersion{originNodeID: r.nodeID, counter: r.clock},
		leaseExpiresAt: now.Add(r.leaseTTL),
	}

	r.applyLiveLocked(entry, now)
	return entry, change
}

func (r *reg) get(component api.Component, instanceID string, now time.Time) (registrationEntry, bool) {
	return r.getByKey(makeRegKey(component, instanceID), now)
}

func (r *reg) getByKey(key string, now time.Time) (registrationEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.registrations[key]
	if !ok || !entry.isLiveAt(now) {
		return registrationEntry{}, false
	}

	return entry, true
}

func (r *reg) listByComponent(component api.Component, now time.Time) []string {
	entries := r.listEntries(component, nil, now)
	matches := make([]string, 0, len(entries))
	for _, entry := range entries {
		matches = append(matches, makeEntryKey(entry))
	}

	return matches
}

func (r *reg) listEntries(component api.Component, metadata map[string]string, now time.Time) []registrationEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	matches := make([]registrationEntry, 0)
	for _, entry := range r.registrations {
		if component != api.Component_COMPONENT_UNKNOWN && entry.component != component {
			continue
		}

		if !entry.isLiveAt(now) {
			continue
		}

		if !metadataMatches(entry.metadata, metadata) {
			continue
		}

		matches = append(matches, entry.clone())
	}

	sort.Slice(matches, func(i, j int) bool {
		return makeEntryKey(matches[i]) < makeEntryKey(matches[j])
	})

	return matches
}

func (r *reg) mergeProtoEntries(entries []*api.RegistryEntry, now time.Time) []*api.RegistryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireLeasesLocked(now)
	changed := make([]*api.RegistryEntry, 0)
	for _, protoEntry := range entries {
		entry, ok := registrationEntryFromProto(protoEntry)
		if !ok {
			continue
		}

		if !entry.tombstone {
			if err := ValidateComponentMetadata(entry.component, entry.metadata); err != nil {
				continue
			}
		}

		var applied bool
		if entry.tombstone {
			applied = r.applyTombstoneLocked(entry, now)
		} else if entry.isLiveAt(now) {
			applied = r.applyLiveLocked(entry, now)
		}

		if applied {
			changed = append(changed, registryEntryToProto(entry))
		}
	}

	r.purgeExpiredTombstonesLocked(now)
	return changed
}

func (r *reg) drainDirtyProtoEntries(now time.Time) []*api.RegistryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireLeasesLocked(now)
	r.purgeExpiredTombstonesLocked(now)
	if len(r.dirty) == 0 {
		return nil
	}

	keys := make([]string, 0, len(r.dirty))
	for key := range r.dirty {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	entries := make([]*api.RegistryEntry, 0, len(keys))

	for _, key := range keys {
		entry := r.dirty[key]
		if entry.tombstone && !entry.tombstoneLiveAt(now) {
			continue
		}

		if !entry.tombstone && !entry.isLiveAt(now) {
			continue
		}

		entries = append(entries, registryEntryToProto(entry))
	}

	r.dirty = make(map[string]registrationEntry)
	return entries
}

func (r *reg) snapshotProtoEntries(now time.Time) []*api.RegistryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireLeasesLocked(now)
	r.purgeExpiredTombstonesLocked(now)
	return r.snapshotProtoEntriesLocked(now)
}

func (r *reg) snapshotProtoEntriesLocked(now time.Time) []*api.RegistryEntry {
	entries := make([]registrationEntry, 0, len(r.registrations)+len(r.tombstones))
	for _, entry := range r.registrations {
		if entry.isLiveAt(now) {
			entries = append(entries, entry)
		}
	}

	for _, tombstone := range r.tombstones {
		if tombstone.tombstoneLiveAt(now) {
			entries = append(entries, tombstone)
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return makeEntryVersionKey(entries[i]) < makeEntryVersionKey(entries[j])
	})

	out := make([]*api.RegistryEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, registryEntryToProto(entry))
	}

	return out
}

func (r *reg) digestProtoEntries(now time.Time) []*api.RegistryDigest {
	entries := r.snapshotProtoEntries(now)
	digests := make([]*api.RegistryDigest, 0, len(entries))
	for _, entry := range entries {
		digests = append(digests, registryEntryToDigest(entry))
	}

	return digests
}

func (r *reg) entriesNewerThanDigests(digests []*api.RegistryDigest, now time.Time) []*api.RegistryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.expireLeasesLocked(now)
	r.purgeExpiredTombstonesLocked(now)

	known := make(map[string]*api.RegistryDigest, len(digests))
	for _, digest := range digests {
		if digest == nil {
			continue
		}

		key := makeVersionKey(digest.GetComponent(), digest.GetInstanceId(), digest.GetOriginNodeId())
		known[key] = digest
	}

	entries := r.snapshotProtoEntriesLocked(now)
	out := make([]*api.RegistryEntry, 0)
	for _, entry := range entries {
		v := entry.GetVersion()
		key := makeVersionKey(entry.GetComponent(), entry.GetInstanceId(), v.GetOriginNodeId())
		digest, ok := known[key]

		if !ok || v.GetCounter() > digest.GetCounter() || (v.GetCounter() == digest.GetCounter() && entry.GetTombstone() && !digest.GetTombstone()) {
			out = append(out, entry)
		}
	}

	return out
}

func (r *reg) applyLiveLocked(entry registrationEntry, now time.Time) bool {
	if entry.version.originNodeID == "" || entry.version.counter == 0 {
		return false
	}

	if !entry.isLiveAt(now) {
		return false
	}

	if tombstone, ok := r.tombstones[makeEntryVersionKey(entry)]; ok && tombstone.version.counter >= entry.version.counter {
		return false
	}

	r.observeClockLocked(entry.version)
	key := makeEntryKey(entry)
	current, ok := r.registrations[key]
	if ok && !liveEntryWins(entry, current, now) {
		return false
	}

	if ok && current.equal(entry) {
		return false
	}

	r.registrations[key] = entry
	r.dirty[makeEntryVersionKey(entry)] = entry
	return true
}

func (r *reg) applyTombstoneLocked(entry registrationEntry, now time.Time) bool {
	if entry.version.originNodeID == "" || entry.version.counter == 0 {
		return false
	}

	if entry.tombstoneExpiresAt.IsZero() {
		entry.tombstoneExpiresAt = now.Add(r.tombstoneTTL)
	}

	if !entry.tombstoneLiveAt(now) {
		return false
	}

	r.observeClockLocked(entry.version)
	entry.tombstone = true
	entry.address = ""
	entry.metadata = nil
	key := makeEntryVersionKey(entry)
	currentTombstone, hadTombstone := r.tombstones[key]
	if hadTombstone && currentTombstone.version.counter >= entry.version.counter && !entry.tombstoneExpiresAt.After(currentTombstone.tombstoneExpiresAt) {
		return false
	}

	r.tombstones[key] = entry
	current, ok := r.registrations[makeEntryKey(entry)]
	if ok && current.version.originNodeID == entry.version.originNodeID && current.version.counter <= entry.version.counter {
		delete(r.registrations, makeEntryKey(entry))
	}

	r.dirty[key] = entry
	return true
}

func (r *reg) observeClockLocked(version registryVersion) {
	if version.originNodeID == r.nodeID && version.counter > r.clock {
		r.clock = version.counter
	}
}

func (r *reg) expireLeasesLocked(now time.Time) {
	for key, entry := range r.registrations {
		if entry.isLiveAt(now) {
			continue
		}

		delete(r.registrations, key)
		tombstone := entry
		tombstone.address = ""
		tombstone.metadata = nil
		tombstone.tombstone = true
		tombstone.tombstoneExpiresAt = now.Add(r.tombstoneTTL)
		r.applyTombstoneLocked(tombstone, now)
	}
}

func (r *reg) purgeExpiredTombstonesLocked(now time.Time) {
	for key, entry := range r.tombstones {
		if !entry.tombstoneLiveAt(now) {
			delete(r.tombstones, key)
		}
	}
}

func liveEntryWins(incoming, current registrationEntry, now time.Time) bool {
	if !current.isLiveAt(now) {
		return true
	}

	if incoming.version.originNodeID == current.version.originNodeID {
		return incoming.version.counter >= current.version.counter
	}

	if !incoming.leaseExpiresAt.Equal(current.leaseExpiresAt) {
		return incoming.leaseExpiresAt.After(current.leaseExpiresAt)
	}

	if incoming.version.counter != current.version.counter {
		return incoming.version.counter > current.version.counter
	}

	return incoming.version.originNodeID > current.version.originNodeID
}

func (e registrationEntry) isLiveAt(now time.Time) bool {
	return !e.tombstone && (e.leaseExpiresAt.IsZero() || e.leaseExpiresAt.After(now))
}

func (e registrationEntry) tombstoneLiveAt(now time.Time) bool {
	return e.tombstone && (e.tombstoneExpiresAt.IsZero() || e.tombstoneExpiresAt.After(now))
}

func (e registrationEntry) equal(other registrationEntry) bool {
	return e.component == other.component &&
		e.instanceID == other.instanceID &&
		e.address == other.address &&
		maps.Equal(e.metadata, other.metadata) &&
		e.version == other.version &&
		e.leaseExpiresAt.Equal(other.leaseExpiresAt) &&
		e.tombstone == other.tombstone &&
		e.tombstoneExpiresAt.Equal(other.tombstoneExpiresAt)
}

func (e registrationEntry) clone() registrationEntry {
	e.metadata = cloneMetadata(e.metadata)
	return e
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}

	return maps.Clone(metadata)
}

func metadataMatches(entryMetadata, filter map[string]string) bool {
	for key, value := range filter {
		got, ok := entryMetadata[key]
		if !ok || got != value {
			return false
		}
	}

	return true
}

func registrationEntryFromProto(entry *api.RegistryEntry) (registrationEntry, bool) {
	if entry == nil || entry.GetVersion() == nil {
		return registrationEntry{}, false
	}

	version := entry.GetVersion()
	out := registrationEntry{
		component:  entry.GetComponent(),
		instanceID: entry.GetInstanceId(),
		address:    entry.GetAddress(),
		metadata:   cloneMetadata(entry.GetMetadata()),
		version:    registryVersion{originNodeID: version.GetOriginNodeId(), counter: version.GetCounter()},
		tombstone:  entry.GetTombstone(),
	}

	if leaseExpires := entry.GetLeaseExpiresUnixNano(); leaseExpires > 0 {
		out.leaseExpiresAt = time.Unix(0, leaseExpires)
	}

	if tombstoneExpires := entry.GetTombstoneExpiresUnixNano(); tombstoneExpires > 0 {
		out.tombstoneExpiresAt = time.Unix(0, tombstoneExpires)
	}

	return out, true
}

func registryEntryToProto(entry registrationEntry) *api.RegistryEntry {
	component := entry.component
	instanceID := entry.instanceID
	address := entry.address
	metadata := cloneMetadata(entry.metadata)
	origin := entry.version.originNodeID
	counter := entry.version.counter
	tombstone := entry.tombstone
	leaseExpires := unixNanoOrZero(entry.leaseExpiresAt)
	tombstoneExpires := unixNanoOrZero(entry.tombstoneExpiresAt)

	return &api.RegistryEntry{
		Component:                &component,
		InstanceId:               &instanceID,
		Address:                  &address,
		Metadata:                 metadata,
		Version:                  &api.RegistryVersion{OriginNodeId: &origin, Counter: &counter},
		LeaseExpiresUnixNano:     &leaseExpires,
		Tombstone:                &tombstone,
		TombstoneExpiresUnixNano: &tombstoneExpires,
	}
}

func unixNanoOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.UnixNano()
}

func registryEntryToDigest(entry *api.RegistryEntry) *api.RegistryDigest {
	component := entry.GetComponent()
	instanceID := entry.GetInstanceId()
	origin := entry.GetVersion().GetOriginNodeId()
	counter := entry.GetVersion().GetCounter()
	tombstone := entry.GetTombstone()

	return &api.RegistryDigest{
		Component:    &component,
		InstanceId:   &instanceID,
		OriginNodeId: &origin,
		Counter:      &counter,
		Tombstone:    &tombstone,
	}
}
