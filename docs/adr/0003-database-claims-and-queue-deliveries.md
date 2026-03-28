# ADR 0003: Database claims and queue deliveries

## Status

Accepted

## Context

Work reaches workers through a **queue message** (with a **delivery id**). The **database** also tracks **run** lifecycle (**queued**, **running**, success/failure) and **leases** so only one worker owns a run at a time.

We could try to make the queue the sole source of truth for “who runs this,” or the database alone, or keep both with defined responsibilities.

## Decision

Use **two coordinated layers**:

1. **Queue** — Buffers **Job** payloads, assigns **delivery** identifiers, supports **ack** after a worker accepts responsibility for the message. Undelivered or unacked deliveries can **expire** and be **requeued** (default delivery TTL on the order of minutes in `internal/queue`).
2. **Database** — For runs that include **`run_id`**, the worker calls **`TryClaim`** before executing: only one worker transitions the run from **queued** to **running** with a **lease** and periodic **renewal**. After a successful **claim**, the worker **acks** the queue delivery. If claim fails (another worker won, or run not eligible), the worker **acks** to drop the message without executing.

**Rationale:** The queue provides **cheap buffering and fan-out**; the database provides **authoritative concurrency control** and **durable run state**. Re-enqueue (from API async path or reconciler) can produce **duplicate** messages; **TryClaim** prevents double execution of the same **run**.

A legacy path still handles jobs **without** `run_id` (ack then execute without DB claim); new trigger flows set `run_id`.

## Consequences

- **Mental model:** Operators must reason about **both** queue health (backlog, persistence, TTL) and **database** health (claims, leases) when debugging stuck or duplicate-looking behavior ([FAILURE_DOMAINS.md](../FAILURE_DOMAINS.md)).
- **Lease vs delivery TTL:** Different timers apply (queue delivery TTL vs DAL **lease TTL** / renewal interval); misconfiguration or crashes can still produce **stranded** runs needing reconciler or operator intervention.
- **Correctness:** Duplicate queue messages for the same `run_id` are expected to be **harmless** at the execution layer if claims work as designed.

## References

- `cmd/worker/main.go` (`runClaimedJob`, `TryClaim`, `ackDelivery`)
- `internal/dal` (`DefaultLeaseTTL`, `DefaultRenewInterval`, claim/update run status)
- `internal/queue/server.go` (delivery TTL, inflight/requeue behavior)
