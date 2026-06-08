# Architecture Decision Records (ADRs)

ADRs are short, append-only notes that capture why Vectis made a non-obvious design choice. They complement [Architecture](../../concepts/architecture.md), which explains what exists now, and [Planning](../roadmap/planning.md), which tracks goals and future work.

## When To Add One

Write an ADR when a choice changes Vectis' architecture, operator contract, developer workflow, or long-term compatibility story. Usually skip an ADR for small refactors, straightforward bug fixes, or implementation details already explained well in code.

New decisions add the next numbered file. Keep older decisions in place; when a decision changes, add a new ADR and mark the old one as superseded instead of rewriting history.

## Current Decisions

| ADR | Status | Decision |
| --- | --- | --- |
| [0001](./0001-async-enqueue-after-http-202.md) | Accepted | Async enqueue after HTTP 202 |
| [0002](./0002-standalone-reconciler-process.md) | Accepted | Standalone reconciler process |
| [0003](./0003-database-claims-and-queue-deliveries.md) | Accepted | Database claims and queue deliveries |
| [0004](./0004-migration-compatibility-and-rollback.md) | Accepted | Migration compatibility and rollback |
| [0005](./0005-gossip-based-ha-registry.md) | Accepted | Gossip-based HA service registry |
| [0006](./0006-global-coordination-cell-local-execution.md) | Accepted | Global coordination and cell-local execution |
| [0007](./0007-queue-pool-shards.md) | Accepted | Queue pool shards |
| [0008](./0008-api-edge-state-backend-contract.md) | Accepted | API edge-state backend contract |
| [0009](./0009-worker-execution-containment-providers.md) | Accepted | Worker execution containment providers |

## Writing Style

- Keep the reader at maintainer level: enough context to understand the tradeoff, not a full implementation walkthrough.
- Use `Context`, `Decision`, `Consequences`, and `References` unless the decision needs a different shape.
- Link to user or operator docs when the decision affects their mental model.
- Link to code references for the source of truth, but avoid turning the ADR into a code tour.
