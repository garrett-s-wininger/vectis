# ADR 0002: Standalone reconciler process

## Status

Accepted

## Context

Runs can be **queued** in the database while **not** present on the **queue** (or not yet successfully consumed). Causes include:

- Async enqueue after **HTTP 202** failing after retries ([0001](0001-async-enqueue-after-http-202.md)).
- **Cron** or other producers hitting the same class of failure.
- Queue restart with **persistence disabled**, leaving DB rows that still expect dispatch.

The API could own all recovery (long-lived retries, background loops inside the API process), or a **separate process** could periodically scan the database and enqueue eligible runs.

## Decision

Ship a dedicated binary **`vectis-reconciler`** that:

1. On a **configurable interval**, queries runs in **queued** status whose last dispatch timestamp is older than a **minimum gap** (`MinDispatchGap`, default 30s) to avoid fighting normal enqueue latency.
2. Loads the job definition (stored job or `job_definitions` fallback for ephemeral-style rows).
3. **Enqueues** with the same retry helper used elsewhere, then **touches** dispatch metadata on success.

The reconciler is **not** embedded in the API so recovery survives API restarts and applies uniformly to **all** enqueue sources, not only HTTP triggers.

## Consequences

- **Operational requirement:** Production-style deployments should run the reconciler (or accept manual re-enqueue) whenever async enqueue is in use.
- **Latency:** Stuck runs are healed on the order of the reconciler **interval** plus `MinDispatchGap`, not instantaneously.
- **Simplicity:** The API remains a relatively thin HTTP + enqueue initiator; recovery policy lives in one place.

## References

- `internal/reconciler/reconciler.go`
- `cmd/reconciler/main.go`
- `internal/dal` (`ListQueuedBeforeDispatchCutoff`, `TouchDispatched`)
