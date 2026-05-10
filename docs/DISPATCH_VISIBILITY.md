# Dispatch Handoff Visibility

Dispatch events explain the handoff from durable run state in the database to work delivery through the queue.

The API records run rows first. Queue submission may happen from the API, from `vectis-cron`, or later from `vectis-reconciler` when a run remained queued but was not successfully handed to the queue. For the broader repair flow, see [REPAIR_RUNBOOKS.md#queued-runs-or-backlog](REPAIR_RUNBOOKS.md#queued-runs-or-backlog).

## Where To Look

- `GET /api/v1/runs/{id}` includes `dispatch_events`.
- `vectis-cli run get <run-id>` prints dispatch events.
- Reconciler metrics and logs explain whether stuck queued runs are being scanned and redispatched.

Normal triage should not require SQL.

## Reading Dispatch Events

Use the event source, timestamp, and message together:

| Pattern | Meaning | Operator action |
| --- | --- | --- |
| Queued run with no dispatch event | Run was recorded but queue handoff did not complete or was not attempted yet | Check API/cron logs, queue reachability, and reconciler health. |
| Dispatch attempt followed by success | Queue accepted the handoff | If the run is still queued, inspect worker availability and queue backlog. |
| Dispatch attempt followed by failure | Producer could not enqueue the run | Check queue health, registry/pinned address config, gRPC TLS, and retry exhaustion. |
| Reconciler dispatch event | Reconciler repaired a queued run that missed initial handoff | Confirm this is occasional; repeated events point to producer/queue instability. |
| Multiple successful handoff events | A retry or reconciler submitted the run more than once | Worker database claims should prevent duplicate execution for the same run ID; inspect queue duplicate pressure. |

## Runbook: Queued With No Dispatch

1. Run `vectis-cli run get <run-id>` and inspect `dispatch_events`.
2. Check `GET /health/ready` on the API.
3. Check queue gRPC health and queue backlog metrics.
4. Confirm `vectis-reconciler` is running and scanning queued runs.
5. Wait at least one reconciler interval before manual intervention unless this is an urgent run.
6. Use the force-requeue/manual retry path only after confirming automatic repair is not progressing.

## Runbook: Dispatch Failure

1. Read the dispatch failure message from the run detail.
2. Check whether the failure came from API, cron, or reconciler.
3. Verify queue address resolution: pinned address, registry availability, and advertised queue address.
4. Verify gRPC TLS settings, especially CA file and server name/SAN matching.
5. Check `vectis_retries_exhausted_total` when retry metrics are available for the path.
6. After repair, confirm a later dispatch success appears.

## Runbook: Duplicate Dispatch

Duplicate handoff can happen when a producer retries after an uncertain failure or when the reconciler repairs a run whose queue state was lost. The database run claim is the execution guard.

1. Confirm only one worker claims the run.
2. Inspect queue delivery and DLQ metrics.
3. If duplicate delivery is persistent, check producer retry logs and reconciler interval/min-age settings.

## Alert Examples

Use emitted metrics first:

```promql
increase(vectis_reconciler_reenqueue_total{outcome!="success"}[10m]) > 0
```

Warn when reconciler repair attempts are failing.

```promql
sum(rate(vectis_queue_jobs_pending[10m])) > 0 and vectis_queue_jobs_pending > 0
```

Warn when queue backlog is not draining. Tune the threshold to your workload.

```promql
increase(vectis_retries_exhausted_total[10m]) > 0
```

Page when retry loops exhaust; use alongside run dispatch events to find the impacted handoff.

Future metrics should expose dispatch failure counts and queued-run age directly so this runbook can alert without inference.
