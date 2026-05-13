# HA Service Registry Plan

## Summary

Build HA registry as an in-memory, eventually consistent registry cluster. When multiple registry nodes are configured, services heartbeat to one sponsor registry node, that sponsor replicates updates through gossip, and all registry nodes can answer discovery reads from their converged local state.

This first milestone is registry HA only. It keeps the current queue/log singleton discovery behavior and does not introduce active/active queue or log service semantics.

## Approach

Use in-memory gossip rather than shared SQL or a consensus log.

- Shared SQL would simplify convergence, but it would make the database a registry dependency.
- Raft or an etcd-style consensus log would provide stronger consistency, but it is heavier than needed for lease-based service discovery.
- Gossip matches the registry's current lightweight role and keeps registry failures local.

Use static registry membership for v1. Each registry node is configured with a stable node ID, its own advertise address, and the peer addresses it should gossip with.

Keep the existing `Register` and `GetAddress` behavior compatible. Add internal peer RPCs for delta gossip and periodic anti-entropy repair.

Bound staleness with leases:

- Heartbeats refresh the lease for a registration.
- Expired entries stop being returned by discovery reads.
- Tombstones are retained long enough to prevent stale gossip from reviving old records.

## Key Changes

- Registry state becomes a versioned replicated map keyed by component plus instance ID.
- Each update gets a per-origin Lamport dot: sponsor node ID plus monotonically increasing counter.
- Merge rules reject old dots, preserve tombstone watermarks, and allow a new sponsor to publish a fresh live record after sponsor failover.
- Client registration learns multiple registry addresses, picks a stable sponsor by rendezvous hashing, and re-picks on sponsor failure.
- Gossip pushes recent deltas to peers; anti-entropy periodically exchanges full snapshots or digests to repair missed gossip.
- Add config for registry clustering and discovery:
  - `registry.cluster.node_id`
  - `registry.cluster.advertise_address`
  - `registry.cluster.peer_addresses`
  - `registry.cluster.gossip_interval`
  - `registry.cluster.anti_entropy_interval`
  - `registry.cluster.lease_ttl`
  - `registry.cluster.tombstone_ttl`
  - `discovery.registry.addresses`
- Update docs that currently describe `vectis-registry` as singleton-only once implementation begins.

## Test Plan

- Unit test merge ordering, stale update rejection, tombstone retention, lease expiry, and sponsor failover.
- Registry integration tests with three in-process registry servers:
  - registration on one node becomes readable from all nodes;
  - missed delta gossip is repaired by anti-entropy;
  - old data cannot overwrite newer data;
  - sponsor failure causes client re-sponsorship without losing discoverability beyond lease bounds.
- Regression test existing single-node registration, worker instance lookup, queue/log lookup, and resolver last-good fallback.
- Run `make test-quick`; add targeted package tests for `internal/registry`, `internal/resolver`, and config parsing.

## Assumptions

- First milestone is registry HA only, not active/active queue/log.
- Static cluster membership is enough for v1.
- Bounded TTL staleness is acceptable during partitions.
- No explicit deregistration RPC in v1; graceful shutdown can rely on lease expiry, with deregistration left as a later improvement.
