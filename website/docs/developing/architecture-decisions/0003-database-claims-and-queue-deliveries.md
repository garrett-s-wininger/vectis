# ADR 0003: Database claims and queue deliveries

## Status

Accepted

## Context

Work reaches workers through a queue message with a delivery ID. The database also tracks task execution lifecycle and leases so only one worker owns a persisted execution at a time, while the run record remains the aggregate lifecycle.

We could try to make the queue the sole source of truth for “who runs this,” or the database alone, or keep both with defined responsibilities.

## Decision

Use two coordinated layers:

1. Queue: buffers `Job` payloads, assigns delivery identifiers, and supports ack after a worker accepts responsibility for the message. In a queue pool, the delivery identifier includes the queue shard so workers ack the right instance. Undelivered or unacked deliveries can expire and be requeued. The default delivery TTL is on the order of minutes in `internal/queue`.
2. Database: for deliveries that include a `run_id` and execution envelope, the worker acks the queue delivery and then calls `TryClaimExecution` before executing. Only one worker claims the execution lease; that claim also promotes the aggregate run to running and records the worker/cancel token on the run. If the execution claim fails because another worker won or the run/execution is not eligible, the worker stops before executing.

The queue provides buffering and fan-out. The database provides authoritative concurrency control and durable run state. Re-enqueue from the API async path, orchestrator fan-out, or reconciler can produce duplicate messages; `TryClaimExecution` prevents double execution of the same task execution.

Workers require queue deliveries for persisted work to carry a `run_id` and execution envelope. A delivery without `run_id` is malformed at the worker boundary: the worker acks it to avoid poison-message loops and does not execute it.

## Consequences

- Debugging requires both queue health and database health. Queue backlog, persistence, and delivery TTL explain delivery behavior; database claims and leases explain run ownership.
- Queue delivery TTL and database execution lease TTL are separate timers. Misconfiguration or crashes can still produce stranded runs/executions that need reconciler or operator intervention.
- Duplicate queue messages for the same `run_id` are expected to be harmless at the execution layer if execution claims work as designed.

## References

- [Failure Domains](../../concepts/failure-domains.md)
- [Dispatch Visibility](../../operating/reliability/dispatch-visibility.md)
- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- [ADR 0007](./0007-queue-pool-shards.md)
- `cmd/worker/main.go` (`runTaskExecution`, `TryClaimExecution`, `ackDelivery`)
- `internal/dal` (`DefaultLeaseTTL`, `DefaultRenewInterval`, execution claim/update run status)
- `internal/queue/server.go` (delivery TTL, inflight/requeue behavior)
