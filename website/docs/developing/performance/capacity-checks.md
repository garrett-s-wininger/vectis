# Capacity And Performance Checks

Use these checks when development work might change Vectis throughput, latency, queue behavior, log behavior, or the published operating envelope.

This is a developer and release-validation page. It is not a production operator drill framework yet. Operators should use [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) for the current operating contract and [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md) for safe replica-count behavior.

## Before You Start

Run these checks before:

- changing queue, worker, log streaming, cron, reconciler, or API hot paths;
- changing database schema or query patterns that affect run creation, claiming, finalization, or log lookup;
- changing retry, idempotency, dispatch, or repair behavior;
- publishing a release that claims a new capacity envelope.

Use a local, CI, staging, or dedicated test environment. Do not point ad hoc performance experiments at production.

Before generating load:

1. Pick the question the check should answer, such as "can this worker count drain 200 queued runs?" or "does log streaming stay responsive under 20 readers?"
2. Confirm the environment is disposable, isolated, or backed up.
3. Confirm the workload is safe to run repeatedly.
4. Record the current deployment shape and resource limits.
5. Run `vectis-cli health check --strict` and either start from a clean baseline or record known warnings.
6. Decide the stop conditions before the check begins.

## Check Record

Capture this for every meaningful performance check:

| Field | Record |
| --- | --- |
| Date and owner | Who ran the check and when. |
| Code and build | Git commit, release version, build flags, and container image tags. |
| Deployment shape | API, queue, log, cron, reconciler, and worker counts. |
| Database | Driver, DSN class, pool settings, host size, and storage class. |
| Queue and logs | Queue persistence path, log storage medium, spool location, and free space. |
| Workload | Exact command, script, job definition, client count, trigger rate, and duration. |
| Observability | Benchmark output, Prometheus snapshots, dashboard screenshots, and notable service logs. |
| Result | Pass/fail decision, observed limit, and follow-up issues. |

Keep the raw output. Summaries are helpful, but raw output is what lets the next check compare honestly.

## Local Benchmark Check

Use this check when queue behavior, run handoff, or release confidence is the main question.

1. Set benchmark duration and repetition count:

```sh
VECTIS_CAPACITY_BENCHTIME=5s VECTIS_CAPACITY_COUNT=3 make capacity-benchmark
```

2. Save the summary and raw Go benchmark output.
3. Record Go version, OS, CPU model, and whether the run used a laptop, CI worker, or staging host.
4. Compare queue ops/sec and p95/p99 latency with the previous baseline for the same machine class.
5. Investigate changes larger than normal local variance before changing the published envelope.

Use shorter runs for quick local checks and repeated longer runs for baseline changes.

## Deployed Stack Check

Use this check when the question involves a real API, database, queue, worker fleet, log service, or dashboard.

1. Start a reference or staging stack with Postgres and durable log storage.
2. Run `vectis-cli health check --strict`.
3. Create one small stored shell job that is safe to run many times.
4. Trigger a small warm-up batch and confirm each run reaches a terminal status.
5. Trigger a measured burst of runs with idempotency keys.
6. Increase workers in steps and record queued-to-running and running-to-terminal latency at each step.
7. Stream logs from multiple clients for at least one active run.
8. Inspect metrics for queue depth, worker outcomes, API request status, DB pool waits, log drops, replay truncation, and reconciler re-enqueues.
9. Stop at the first sustained error, saturation, or operator-visible degradation and record the limit.
10. Let the system drain, then rerun `vectis-cli health check --strict`.

Do not treat a larger worker count as a win unless queue depth drains, DB pool pressure stays acceptable, and logs remain usable.

## What To Watch

| Area | Signals |
| --- | --- |
| API | Request status, latency, `429`, `503`, and dependency readiness. |
| Queue | Pending depth, in-flight deliveries, DLQ entries, dequeue rate, and expired requeues. |
| Workers | Jobs received, terminal outcomes, job duration, claim conflicts, and host CPU/memory/disk. |
| Database | Pool waits, open/in-use connections, query latency, storage growth, and host pressure. |
| Logs | Append failures, stream disconnects, replay truncation, spool growth, and storage free space. |
| Reconciler | Re-enqueue attempts, failures, and repeated repair for the same runs. |
| Users | CLI/API responsiveness and whether operators can still inspect runs and logs during load. |

Use `vectis-cli health check --json` when you want to capture health evidence alongside Prometheus or dashboard snapshots.

## Stop Conditions

Stop the check and record the limit when any of these are sustained:

- API readiness fails or trigger requests return dependency errors.
- Queue depth grows without draining after load stops.
- DLQ receives unexpected entries.
- Worker terminal outcomes do not match the workload.
- DB pool waits continue climbing.
- Log streams become unusable or log storage/spool free space approaches the warning threshold.
- Reconciler repair attempts repeatedly fail.
- Operators can no longer inspect runs, dispatch events, or logs.

## Exit Criteria

A check passes when:

- No run is duplicated.
- No completed run is left nonterminal.
- Queue depth returns to baseline after load stops.
- DLQ remains empty unless the workload intentionally causes failures.
- API and log streams recover cleanly after clients disconnect.
- The observed limits fit within [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md), or the envelope is updated.

## After The Check

1. Save raw output and dashboard snapshots with the check record.
2. Record the observed safe range and the first limiting component.
3. Open follow-up issues for code, config, dashboards, alerts, or docs.
4. Update [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) when the check changes the known-safe range.
5. If retention, backups, or restore assumptions changed, update the reliability docs before relying on the new operating point.

## Future Representative Workloads

The current checks are intentionally simple. A fuller framework should add representative workloads before Vectis makes stronger production throughput claims:

| Workload shape | What it should cover |
| --- | --- |
| Small fast jobs | Queue handoff, run claiming, and terminal-state churn. |
| Log-heavy jobs | Log append, replay, storage growth, and reader fan-out. |
| Cron bursts | Schedule-to-run latency and duplicate/missed enqueue detection. |
| Mixed API/admin traffic | Job listing, run lookup, auth/token paths, and rate-limit behavior under build traffic. |
| Failure-and-repair load | Reconciler behavior, retry exhaustion, DLQ pressure, and dispatch visibility. |

When those workloads exist, keep the operator docs focused on how to size and monitor a deployment, and keep the raw performance methodology here.

## Related Docs

| Need | Doc |
| --- | --- |
| Current operator envelope | [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) |
| Scaling and restart behavior | [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md) |
| First-response signals | [Runbooks And Alerts](../../operating/reliability/runbooks.md) |
| Health check IDs and JSON output | [Health Check Catalog](../../operating/reference/health-check-catalog.md) |
| Backup and restore assumptions | [Backup And Restore](../../operating/reliability/backup-restore.md) |
