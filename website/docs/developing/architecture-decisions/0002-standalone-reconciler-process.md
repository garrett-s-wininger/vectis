# ADR 0002: Standalone reconciler process

## Status

Accepted

## Context

Runs can be queued in the database while not present on the queue, or not yet successfully consumed. Causes include:

- Async enqueue after `202 Accepted` failing after retries; see [ADR 0001](./0001-async-enqueue-after-http-202.md).
- Cron or another producer hitting the same class of enqueue failure.
- Queue restart with persistence disabled, leaving database rows that still expect dispatch.

The API could own all recovery through long-lived retries or background loops inside the API process. A separate process could instead scan the database and enqueue eligible runs on a schedule.

## Decision

Ship a dedicated binary, `vectis-reconciler`, that:

1. On a configurable interval, queries a bounded batch of runs in queued status whose last dispatch timestamp is older than a minimum gap (`MinDispatchGap`, default 30s) to avoid fighting normal enqueue latency.
2. Loads the job definition version pinned on the run from `job_definitions`.
3. Enqueues with the same retry helper used elsewhere, then records dispatch metadata on success.
4. Acquires a database-backed service lease before each scan. Multiple reconciler processes may be started in one cell, but only the lease holder performs repair work during a lease window.

The reconciler is not embedded in the API. Recovery should survive API restarts and apply uniformly to every enqueue source, not only HTTP triggers.

## Consequences

- Operator requirement: deployments that use async enqueue should run the reconciler or accept manual re-enqueue as their repair path.
- Repair latency: stuck runs are healed on the order of the reconciler interval plus `MinDispatchGap`, and large backlogs may drain over multiple passes according to the redispatch limit.
- Standby latency: with multiple reconcilers, failover is bounded by the service lease TTL plus the next standby poll.
- Simpler API behavior: the API remains a relatively thin HTTP and enqueue initiator; recovery policy lives in one place.

## References

- [Dispatch Visibility](../../operating/reliability/dispatch-visibility.md)
- [Repair Runbooks](../../operating/reliability/repair-runbooks.md)
- `internal/reconciler/reconciler.go`
- `cmd/reconciler/main.go`
- `internal/dal` (`ListQueuedBeforeDispatchCutoff`, `TouchDispatched`)
