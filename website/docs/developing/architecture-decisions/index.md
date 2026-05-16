# Architecture Decision Records (ADRs)

Short, **append-only** notes that capture **why** we made a non-obvious design choice. They complement [ARCHITECTURE.md](../../concepts/architecture.md) (what exists) and [PLANNING.md](../roadmap/planning.md) (§1 goals, §4+ roadmap).

New decisions add the next numbered file; prefer **superseded** status over deleting history.

| ADR | Title |
| --- | --- |
| [0001](./0001-async-enqueue-after-http-202.md) | Async enqueue after HTTP 202 |
| [0002](./0002-standalone-reconciler-process.md) | Standalone reconciler process |
| [0003](./0003-database-claims-and-queue-deliveries.md) | Database claims and queue deliveries |
| [0004](./0004-migration-compatibility-and-rollback.md) | Migration compatibility and rollback |
| [0005](./0005-gossip-based-ha-registry.md) | Gossip-based HA service registry |
