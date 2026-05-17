# ADR 0001: Async enqueue after HTTP 202

## Status

Accepted

## Context

Triggering a run requires two steps: persist a run row in the database, then enqueue a job message so a worker can execute it. The database is the source of truth for run status and history. The queue is a separate service, so enqueue can fail or be slow because of network issues, registry resolution, or queue overload.

We could block the HTTP handler until enqueue succeeds, or return to the client as soon as the run exists and enqueue afterward.

## Decision

For `POST /api/v1/jobs/trigger/{id}` and the analogous ephemeral run path (`RunJob`):

1. Create the run in the database.
2. Respond with `202 Accepted` and the `run_id` before enqueue completes.
3. Perform enqueue in a background goroutine with bounded retries through the `internal/queueclient` retry helper.

Clients should not wait on queue latency after the run has been durably recorded. Duplicate enqueue is mitigated by worker claim semantics; see [ADR 0003](./0003-database-claims-and-queue-deliveries.md).

## Consequences

- Client semantics: success of the HTTP call means the run is recorded, not guaranteed to be on the queue yet.
- Failure visibility: if enqueue fails after all retries, the error is logged; the API does not retroactively change the HTTP response.
- Repair path: operators should run the reconciler and watch for queued runs that are not being dispatched.
- Idempotency: producers may enqueue the same logical run more than once; execution correctness relies on the database claim path for runs that carry a `run_id`.

## References

- [ADR 0002: Standalone reconciler process](./0002-standalone-reconciler-process.md)
- [Failure Domains](../../concepts/failure-domains.md)
- [Dispatch Visibility](../../operating/reliability/dispatch-visibility.md)
- [Repair Runbooks](../../operating/reliability/repair-runbooks.md)
- `internal/api/server.go` (`TriggerJob`, `finishTriggerEnqueue`, `finishRunJobEnqueue`)
- `internal/queueclient/retry.go`
