# ADR 0007: Queue pool shards

## Status

Accepted

## Context

The queue buffers work between producers and workers, but the database is the authority for persisted run execution. ADR 0003 defines the key safety property: duplicate queue messages for the same `run_id` are acceptable because workers must claim the run in the database before executing.

That means queue availability and throughput can improve by adding independent queue shards before building active/passive failover or shared queue storage.

## Decision

Support multiple `vectis-queue` instances as a pool within one execution cell:

1. Each queue instance registers with a stable instance ID and owns its own local WAL/snapshot persistence directory.
2. Producers discover queue registrations and choose a shard for each enqueue.
3. Workers discover the same queue pool, try to dequeue across shards, and ack the shard encoded in the delivery ID.
4. Queue shards do not share persistence files and do not form a single global FIFO. Ordering is per shard.
5. If no explicit instance ID is configured, a queue derives a stable default ID from `hostname-port`.
6. If no explicit persistence directory is configured, a queue derives `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>`.
7. If a queue shard loses local state, queued database runs remain repairable through the reconciler.

## Consequences

- Queue scale-out is operationally simple: add queue shards with distinct stable IDs and persistence directories. Defaults are suitable for local multi-shard testing when each shard uses a different port.
- The common crash/restart path remains covered by per-shard WAL plus platform volume persistence.
- Starting two active queues on the same persistence directory fails fast through the queue persistence lock.
- Starting two active queues with the same instance ID is a misconfiguration: the registry treats that ID as one logical shard, so the later registration can steal ack routing from the earlier process.
- Total queue ordering is no longer global when more than one shard is active.
- Active/passive failover for one shard is deferred. It is mainly useful for deployments where local persistent storage cannot restart with the queue process.
- Jobs without a `run_id` still depend solely on queue delivery state. New production trigger paths should continue to create durable runs first.

## References

- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- [Failure Domains](../../concepts/failure-domains.md)
- [Configuration](../../operating/configuration.md)
- [ADR 0003](./0003-database-claims-and-queue-deliveries.md)
- `internal/queueclient/pool.go`
- `internal/queue/server.go`
