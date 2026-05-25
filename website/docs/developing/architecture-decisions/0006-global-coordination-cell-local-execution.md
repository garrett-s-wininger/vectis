# ADR 0006: Global coordination and cell-local execution

## Status

Accepted

## Context

Vectis needs multi-datacenter execution to be a core capability, not an implementation detail hidden behind generic worker labels. A single job should be able to fan out work across named cells such as datacenters, regions, availability zones, or other operator-defined execution locations.

The current single-cell architecture records runs in one SQL database, hands work to one queue, and has workers claim and finalize runs through that same database. That model is simple and remains valuable for local development, but it would force remote cells to pay cross-datacenter latency for hot-path execution operations such as queue handoff, worker claims, lease renewal, local retries, and log movement.

We considered treating cells as full independent Vectis deployments behind a global gateway. That would preserve local autonomy, but it would duplicate user-facing control-plane concerns such as job storage, auth, schedules, namespaces, and RBAC inside each cell. It would also make global job and run views rely on broad scatter/gather reads.

## Decision

Model Vectis as a two-tier system:

- The global control plane owns user-facing API behavior, auth, namespaces, job definitions, schedules, routing policy, cell catalog, workflow coordination, and global run summaries.
- Global run summaries are a catalog read model. Status changes should enter that catalog through a narrow updater boundary so a future cell event consumer can update summaries without depending on the full run repository.
- Each cell owns private execution ingress, local queueing, local execution state, worker leases, local retries, local log ingest/storage, and an event outbox back to the global control plane.
- Global workflow coordination happens at explicit segment boundaries. A segment is a schedulable slice of a job graph that can run in one cell or fan out across multiple cells.
- Cell-local choreography happens inside a segment. The global coordinator should not manage every local action, lease renewal, or queue delivery.
- Cross-cell dependencies are represented as durable segment transitions with explicit inputs, outputs, artifacts, and success policy. Cells do not directly dispatch work into other cells.
- Execution cells must not depend on the global database for their worker hot path. Remote cells should be able to keep running accepted work while the global control plane is slow or temporarily unreachable.
- Single-cell Vectis remains the degenerate deployment shape where the global control plane and local cell runtime run together with `cell_id = "local"`.

The first implementation should make the existing local path cell-shaped before adding remote cells. The near-term milestones are:

1. Introduce configured cell identity while preserving the default `local` cell.
2. Replace hardcoded local cell values with injected cell identity.
3. Make registry metadata and run records cell-aware.
4. Add segment and execution records while creating only one local execution at first.
5. Route enqueue and reconciliation through a cell-aware interface that initially has only a local implementation.
6. Add private cell ingress, async cell events, and remote fanout only after the local shape is stable.

## Consequences

- Multi-datacenter fanout becomes a first-class Vectis feature instead of an emergent property of runner labels.
- The latency-sensitive execution loop stays local to each cell.
- Global status is an observed summary, not always the freshest execution truth. APIs and UIs must expose observed timestamps, sequence numbers, and partial-failure states where needed.
- Cross-cell DAGs require global choreography at segment boundaries. Fine-grained cross-cell step ping-pong is intentionally not the optimized path.
- Cells need their own operational surfaces for queue state, execution state, logs, outbox replay, metrics, and repair.
- Local development should remain simple by collapsing global and cell responsibilities into one deployment.
- Future features such as matrix fanout, quorum success, fail-fast, placement labels, global concurrency gates, and artifact manifests can layer on segment boundaries.
- Features that require shared mutable workspaces, distributed transactions, mid-process migration, or low-latency cross-cell coordination remain non-goals unless a later ADR accepts that complexity.

## References

- [Planning](../roadmap/planning.md)
- [Architecture](../../concepts/architecture.md)
- [Failure Domains](../../concepts/failure-domains.md)
- [ADR 0001: Async enqueue after HTTP 202](./0001-async-enqueue-after-http-202.md)
- [ADR 0002: Standalone reconciler process](./0002-standalone-reconciler-process.md)
- [ADR 0003: Database claims and queue deliveries](./0003-database-claims-and-queue-deliveries.md)
- [ADR 0005: Gossip-based HA service registry](./0005-gossip-based-ha-registry.md)
- `internal/dal/` - current SQL-backed job and run persistence
- `internal/queue/` and `internal/queueclient/` - current queue server and client handoff path
- `internal/registry/metadata.go` - existing cell metadata key used by service registration
