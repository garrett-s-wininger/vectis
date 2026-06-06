# Dispatch Handoff Visibility

Use dispatch events when a run is queued but not starting, when queue handoff alerts fire, or when you need to know whether a run actually reached the queue.

Vectis records the run row before handing work to the queue. That keeps the run durable even if the producer, queue, network, or reconciler has trouble during handoff. Dispatch events are the audit trail for that handoff.

For the broader repair flow, see [Queued Runs Or Backlog](./repair-runbooks.md#queued-runs-or-backlog).

## What Dispatch Events Tell You

Dispatch events answer four questions:

| Question | Where to look |
| --- | --- |
| Who tried to hand off the run? | `source` |
| What happened? | `event_type` |
| When did it happen? | `created_at` |
| Why did it fail, if it failed? | `message` |

The producer may be:

| Source | Meaning |
| --- | --- |
| `api` | A user or API client created or retried a run. |
| `cron` | `vectis-cron` created a run from a schedule. |
| `reconciler` | `vectis-reconciler` found a queued run that still needed queue handoff. |
| `task_dispatch` | Task dispatch handed off newly dispatchable task work inside the same run, normally from worker completion and later from reconciler repair when an intent remained pending. |

In multi-cell deployments, dispatch events still live on the global run. A failure message can describe a missing cell route, an unavailable private cell ingress endpoint, or a local queue handoff failure inside the target cell. See [Multi-Cell Operation](../multi-cell.md) for the routing and repair shape.

The event type may be:

| Event type | Meaning |
| --- | --- |
| `accepted` | The API recorded the run and is about to return `202`; queue handoff may still be pending. |
| `attempt` | A producer is about to submit the run to the queue. |
| `success` | The queue accepted the handoff. |
| `failure` | The producer could not complete the handoff or could not mark it complete. Read `message` next. |

## Where To Look

- `vectis-cli runs show <run-id>` prints `next_action`, dispatch events, task completion, and task dispatch summary with the rest of the run detail.
- `vectis-cli runs tasks <run-id>` lists task graph nodes and task attempts for the run, including execution status and worker lease ownership when present.
- `GET /api/v1/runs/{id}` includes `next_action`, `dispatch_events`, and, when present, `task_dispatch` for API-based tooling.
- `GET /api/v1/runs/{id}/tasks` returns task graph nodes and attempts for API-based tooling, including execution status and worker lease ownership when present.
- Reconciler metrics and logs explain whether stuck queued runs are being scanned and redispatched.

Normal triage should not require SQL.

## Reading Common Patterns

Use the event source, timestamp, and message together:

| Pattern | Meaning | Operator action |
| --- | --- | --- |
| Queued run with only `accepted` | The API returned or was about to return `202`, but its detached queue handoff did not start or did not record an attempt. | Check API logs, queue reachability, and reconciler health. |
| Queued run with no dispatch event | The run was recorded by a non-API producer, but queue handoff did not complete or has not been attempted yet. | Check cron logs, queue reachability, and reconciler health. |
| `attempt` without a later `success` | A producer started handoff, then failed or stopped before success was recorded. | Read nearby logs for that source and wait for reconciler repair if the run is not urgent. |
| `attempt` followed by `success` | The queue accepted the handoff. | If the run is still queued, focus on worker availability and queue backlog. |
| `failure` | The producer could not enqueue the run or could not record the dispatch completion. | Read `message`, then check queue health, registry or pinned address config, gRPC TLS, and retry exhaustion. |
| `reconciler` event | The reconciler tried to repair a queued run that missed or lost its original handoff. | Occasional repair is expected. Repeated repair points to producer, queue, or network instability. |
| `task_dispatch` event | A task completion produced continuation work for the same run. | Repeated failures point to queue handoff, worker capacity, or task fan-out pressure. |
| Multiple successful handoff events | A retry or reconciler submitted the same run more than once. | Worker database claims should prevent duplicate execution for the same run ID; inspect queue duplicate pressure. |

Task fan-in is reduction based: any terminal task failure reduces the run to failed, even when sibling branches are still incomplete; all tasks must succeed before the run reduces to succeeded; otherwise the run is queued for continuation. Workers own normal reduce/finalize. If a worker expires after task completion but before finalizing the run, the reconciler can repair an orphaned task run whose stored task summary already reduces to succeeded or failed. Worker spans emit `task.reduce` and `task.finalize` events, and worker metrics emit `vectis_task_reduce_decisions_total` and `vectis_task_finalize_decisions_total`.

## Runbook: Queued With No Dispatch {#runbook-queued-with-no-dispatch}

1. Run `vectis-cli runs show <run-id>` and inspect `dispatch_events`.
2. Check the producer logs: API for user-created runs, `vectis-cron` for scheduled runs.
3. Check `GET /health/ready` on the API.
4. Check queue gRPC health and queue backlog metrics.
5. Confirm `vectis-reconciler` is running and scanning queued runs.
6. Wait at least one reconciler interval before manual intervention unless this is an urgent run.
7. Use the force-requeue or manual retry path only after confirming automatic repair is not progressing.

## Runbook: Dispatch Failure {#runbook-dispatch-failure}

1. Read the dispatch failure message from the run detail.
2. Check whether the failure came from API, cron, task dispatch, or reconciler.
3. Verify queue address resolution: pinned address, registry availability, and advertised queue address.
4. Verify gRPC TLS settings, especially CA file and server name/SAN matching.
5. Check `vectis_retries_exhausted_total` when retry metrics are available for the path.
6. After repair, confirm a later `success` event appears.

## Runbook: Duplicate Dispatch {#runbook-duplicate-dispatch}

Duplicate handoff can happen when a producer retries after an uncertain failure or when the reconciler repairs a run whose queue state was lost. The database run claim is the execution guard, so duplicate handoff should not become duplicate execution for the same run ID.

1. Confirm only one worker claims the run.
2. Inspect queue delivery and DLQ metrics.
3. If duplicate delivery is persistent, check producer retry logs and reconciler interval/min-age settings.

## Alert Signals

Use emitted metrics first. The example rules live in [`prometheus-examples.yml`](../../alerts/prometheus-examples.yml).

```promql
increase(vectis_run_dispatch_events_total{event_type="failure"}[10m]) > 0
```

Warn when API, cron, task dispatch, or reconciler handoff is failing. In multi-cell deployments, group by `target_cell` to find the affected cell.

```promql
increase(vectis_task_dispatch_intents_total{outcome="failed"}[10m]) > 0
```

Warn when worker-owned task continuation handoff is failing. Pair this with `vectis_task_dispatch_drains_total` to see whether the failure is isolated or part of sustained fan-out pressure.

```promql
sum(vectis_task_dispatch_pending_intents) > 0
```

Warn when retry-eligible task continuation work remains pending. The API emits this SQL-backed gauge with `cell_id` labels, so group by `cell_id` to find the affected target cell.

```promql
increase(vectis_task_reduce_decisions_total{outcome="error"}[10m]) > 0
```

Warn when a worker cannot reduce task completion into a run-level decision. Pair this with `vectis_task_finalize_decisions_total` to see whether runs are continuing, reducing to terminal states, or remaining incomplete.

```promql
increase(vectis_reconciler_reenqueue_total{outcome!="success"}[10m]) > 0
```

Warn when reconciler queued-run redispatch attempts are failing.

```promql
increase(vectis_reconciler_task_finalization_repairs_total{outcome="error"}[10m]) > 0
```

Warn when reconciler repair of orphaned task finalization is failing. Group by `reduce_outcome` to separate reduce-to-succeeded and reduce-to-failed repairs.

```promql
sum(rate(vectis_queue_jobs_pending[10m])) > 0 and vectis_queue_jobs_pending > 0
```

Warn when queue backlog is not draining. Tune the threshold to your workload.

```promql
increase(vectis_retries_exhausted_total[10m]) > 0
```

Page when retry loops exhaust; use alongside run dispatch events to find the impacted handoff.

For API-created runs, `vectis_api_run_enqueue_total{outcome=...}` counts `accepted`, `attempt`, `success`, `failed_enqueue`, and `failed_touch_dispatched` outcomes by run kind. There is not yet a direct queued-run-age metric, so combine these signals with the run's dispatch events before deciding whether the problem is producer handoff, queue backlog, or worker capacity.

## Related Docs

| Need | Doc |
| --- | --- |
| Repair steps for stuck runs | [Repair Runbooks](./repair-runbooks.md) |
| First-response triage | [Runbooks And Alerts](./runbooks.md) |
| Queue and reconciler failure behavior | [Failure Domains](../../concepts/failure-domains.md) |
| Queue address and TLS settings | [Configuration](../configuration.md) |
| API shape for run detail | [API Reference](../../using/api-reference.md) |
