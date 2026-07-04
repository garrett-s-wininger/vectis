# ADR 0005: Gossip-based HA service registry

## Status

Accepted

## Context

The `vectis-registry` was initially a singleton service providing service discovery via `Register` and `GetAddress` gRPC. A single registry node is a single point of failure: if it goes down, new queue/log/worker instances cannot register or be discovered, and existing clients lose the ability to resolve fresh addresses.

Options for HA included:

- **Shared SQL** — would simplify convergence but make the database a registry dependency.
- **Raft/consensus log (etcd, etc.)** — stronger consistency but heavier than needed for lease-based service discovery.
- **Gossip protocol** — matches the registry's lightweight role; failures stay local.

## Decision

Implement HA registry as an in-memory, eventually consistent registry cluster using gossip:

- State: versioned replicated map keyed by component and instance ID, with Lamport-dotted version vectors.
- Convergence: delta gossip pushes recent updates to peers; periodic anti-entropy exchanges full snapshots or digests to repair missed gossip.
- Leases: heartbeats refresh leases; expired entries stop being returned by discovery reads; tombstones are retained past gossip propagation delay to prevent stale revival.
- Sponsor selection: registration clients order registry targets with rendezvous hashing, publish initial registration and heartbeat updates to the active sponsor, and fail over to another configured target on errors.
- Membership: static cluster membership for v1. Each node is configured with node ID, advertise address, and peer list.

This decision records the HA machinery. A single registry remains the simplest operator default; multi-node registry deployment is an advanced posture that must be configured deliberately.

## Consequences

- No external dependencies: registry HA is self-contained.
- Bounded staleness: during partitions, discovery reads may briefly return stale addresses within lease TTL or miss registrations that were accepted by another registry target until gossip and anti-entropy converge.
- Operational simplicity: there is no consensus log to manage. Static membership keeps deployment straightforward, but it still requires deliberate configuration.
- Migration path: existing single-node deployments continue working. Multi-node registry cells are additive.
- Documentation requirement: operator docs should distinguish the safe default from the advanced HA registry posture.

## References

- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- [Configuration](../../operating/configuration.md#service-discovery-vs-fixed-addresses)
- `internal/registry/store.go` — Lamport-dotted versioned map, lease expiry, tombstone retention
- `internal/registry/gossip.go` — delta push and anti-entropy snapshots
- `internal/registry/registration.go` — sponsor-ordered registration and heartbeats
- `internal/resolver/` — discovery with last-good fallback
- `internal/config/defaults.toml` — cluster configuration keys
