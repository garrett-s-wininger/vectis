# ADR 0005: Gossip-based HA service registry

## Status

Accepted (implemented)

## Context

The `vectis-registry` was initially a singleton service providing service discovery via `Register` and `GetAddress` gRPC. A single registry node is a single point of failure: if it goes down, new queue/log/worker instances cannot register or be discovered, and existing clients lose the ability to resolve fresh addresses.

Options for HA included:

- **Shared SQL** — would simplify convergence but make the database a registry dependency.
- **Raft/consensus log (etcd, etc.)** — stronger consistency but heavier than needed for lease-based service discovery.
- **Gossip protocol** — matches the registry's lightweight role; failures stay local.

## Decision

Implement HA registry as an in-memory, eventually consistent registry cluster using gossip:

- **State:** versioned replicated map keyed by component + instance ID, with Lamport-dotted version vectors (per-origin monotonic counter + sponsor node ID).
- **Convergence:** delta gossip pushes recent updates to peers; periodic anti-entropy exchanges full snapshots or digests to repair missed gossip.
- **Leases:** heartbeats refresh leases; expired entries stop being returned by discovery reads; tombstones retained past gossip propagation delay to prevent stale revival.
- **Sponsor selection:** clients pick a stable sponsor via rendezvous hashing from the registry address set, re-picking on sponsor failure.
- **Membership:** static cluster membership for v1 — each node configured with node ID, advertise address, and peer list.

## Consequences

- **No external dependencies** — registry HA is self-contained.
- **Bounded staleness** — during partitions, discovery reads may briefly return stale addresses (within lease TTL) or miss new registrations until gossip/anti-entropy converges.
- **Operational simplicity** — no consensus log to manage; static membership keeps deployment straightforward.
- **Migration path** — existing single-node deployments continue working; multi-node cells are additive.

## References

- `internal/registry/store.go` — Lamport-dotted versioned map, lease expiry, tombstone retention
- `internal/registry/gossip.go` — delta push and anti-entropy snapshots
- `internal/registry/registration.go` — rendezvous hash sponsor selection
- `internal/resolver/` — discovery with last-good fallback
- `internal/config/defaults.toml` — cluster configuration keys
