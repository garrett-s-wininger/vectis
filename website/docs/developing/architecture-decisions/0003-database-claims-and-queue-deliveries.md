# ADR 0003: Database claims and queue deliveries

## Status

Accepted

## Context

Work reaches workers through a queue message with a delivery ID. The database also tracks run lifecycle and leases so only one worker owns a persisted run at a time.

We could try to make the queue the sole source of truth for “who runs this,” or the database alone, or keep both with defined responsibilities.

## Decision

Use two coordinated layers:

1. Queue: buffers `Job` payloads, assigns delivery identifiers, and supports ack after a worker accepts responsibility for the message. Undelivered or unacked deliveries can expire and be requeued. The default delivery TTL is on the order of minutes in `internal/queue`.
2. Database: for runs that include `run_id`, the worker calls `TryClaim` before executing. Only one worker transitions the run from queued to running with a lease and periodic renewal. After a successful claim, the worker acks the queue delivery. If claim fails because another worker won or the run is not eligible, the worker acks to drop the message without executing.

The queue provides buffering and fan-out. The database provides authoritative concurrency control and durable run state. Re-enqueue from the API async path or reconciler can produce duplicate messages; `TryClaim` prevents double execution of the same run.

A legacy path still handles jobs without `run_id` by acking and executing without a database claim. New trigger flows set `run_id`.

## Consequences

- Debugging requires both queue health and database health. Queue backlog, persistence, and delivery TTL explain delivery behavior; database claims and leases explain run ownership.
- Queue delivery TTL and database lease TTL are separate timers. Misconfiguration or crashes can still produce stranded runs that need reconciler or operator intervention.
- Duplicate queue messages for the same `run_id` are expected to be harmless at the execution layer if claims work as designed.

## References

- [Failure Domains](../../concepts/failure-domains.md)
- [Dispatch Visibility](../../operating/reliability/dispatch-visibility.md)
- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- `cmd/worker/main.go` (`runClaimedJob`, `TryClaim`, `ackDelivery`)
- `internal/dal` (`DefaultLeaseTTL`, `DefaultRenewInterval`, claim/update run status)
- `internal/queue/server.go` (delivery TTL, inflight/requeue behavior)
