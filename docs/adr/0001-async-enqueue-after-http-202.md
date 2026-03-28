# ADR 0001: Async enqueue after HTTP 202

## Status

Accepted

## Context

Triggering a run requires two steps: persist a **run** row in the database (source of truth for status and history) and **enqueue** a **job** message so a **worker** can execute it. The queue is a separate service; enqueue can fail or be slow (network, registry resolution, queue overload).

We could block the HTTP handler until enqueue succeeds, or return to the client as soon as the run exists and enqueue afterward.

## Decision

For **`POST /api/v1/jobs/trigger/{id}`** and the analogous **ephemeral run** path (`RunJob`):

1. Create the run in the database.
2. Respond with **HTTP 202 Accepted** and the `run_id` (and related fields) **before** enqueue completes.
3. Perform enqueue in a **background goroutine** with **bounded retries** (`internal/queueclient` retry helper).

The in-code rationale: clients should not wait on queue latency; **duplicate enqueue** is mitigated by **worker claim** semantics (see [0003](0003-database-claims-and-queue-deliveries.md)).

## Consequences

- **Client semantics:** Success of the HTTP call means the run is **recorded**, not guaranteed to be **on the queue** yet. Operators and integrators should run the **reconciler** and monitor for stuck **queued** runs ([0002](0002-standalone-reconciler-process.md), [FAILURE_DOMAINS.md](../FAILURE_DOMAINS.md)).
- **Failure visibility:** If enqueue fails after all retries, the error is **logged**; the API does not retroactively change the HTTP response.
- **Idempotency:** Producers may enqueue the same logical run more than once; execution correctness relies on the database **claim** path for runs that carry a `run_id`.

## References

- `internal/api/server.go` (`TriggerJob`, `finishTriggerEnqueue`, `finishRunJobEnqueue`)
- `internal/queueclient/retry.go`
