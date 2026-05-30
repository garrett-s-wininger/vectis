# Repair Runbooks

These recipes are for operators responding to failed health checks, alerts, restore drills, or stuck runs.

Start with the broad health check when the API is reachable:

```sh
vectis-cli health check --strict
```

For a run-specific issue, start with:

```sh
vectis-cli runs show <run-id>
```

That command shows run status and dispatch events without direct database access.

## Quick Map

| Symptom or check | Recipe |
| --- | --- |
| `queue.backlog.ratio`, queue backlog alert, queued run not starting | [Queued Runs Or Backlog](#queued-runs-or-backlog) |
| `reconciler.stuck.runs`, reconciler alert | [Reconciler Repair](#reconciler-repair) |
| `log.reachable`, log append/drop alert, log streaming failure | [Log Service Repair](#log-service-repair) |
| `audit.drops.recent`, `audit.flush.failures`, audit alert | [Audit Durability Repair](#audit-durability-repair) |
| `db.connection.pool`, DB pool alert | [Database Pool Pressure](#database-pool-pressure) |
| `db.schema.current`, API readiness schema error | [Schema Or Migration Repair](#schema-or-migration-repair) |
| Old retained records or SQL storage pressure | [Retention Cleanup](#retention-cleanup) |
| One run needs operator action | [Manual Run Intervention](#manual-run-intervention) |

## Before Manual Changes

Manual run repair can create confusing history if automatic repair is still working. Before retrying, requeueing, or marking a run terminal:

1. Read the run with `vectis-cli runs show <run-id>`.
2. Check whether a worker currently owns the run.
3. Check dispatch events for recent API, cron, or reconciler handoff attempts.
4. Fix underlying queue, database, log, registry, or TLS issues first.
5. Wait at least one reconciler interval when the run is queued and not urgent.
6. Prefer the narrowest action that matches the situation.

Do not requeue a succeeded run. Do not requeue a running run unless you have confirmed the worker is gone and duplicate execution is not possible.

## Queued Runs Or Backlog

Use this when `health check` warns on `queue.backlog.ratio`, queued runs are not draining, or `VectisQueueBacklogGrowing` fires.

1. Run `vectis-cli health check --strict` and note failures for `api.ready`, `queue.backlog.ratio`, and `reconciler.stuck.runs`.
2. For a specific run, run `vectis-cli runs show <run-id>` and inspect `status` plus `dispatch_events`.
3. If there is no dispatch event, follow [Queued With No Dispatch](./dispatch-visibility.md#runbook-queued-with-no-dispatch).
4. If dispatch failed, follow [Dispatch Failure](./dispatch-visibility.md#runbook-dispatch-failure).
5. Confirm workers are running and receiving jobs with worker process health and `vectis_worker_jobs_received_total`.
6. Check queue gRPC health and queue metrics: pending jobs, in-flight deliveries, and DLQ size.
7. If queue persistence was disabled or lost after restart, keep `vectis-reconciler` running and wait at least one reconciler interval before manual retry.
8. Use `vectis-cli runs retry <run-id>` only after automatic redispatch is not progressing and the run is not already running or succeeded.

## Reconciler Repair

Use this when `health check` warns on `reconciler.stuck.runs`, cannot read reconciler recovery visibility, or `VectisReconcilerReenqueueFailures` fires.

1. Confirm API readiness with `vectis-cli health check --strict`.
2. Confirm exactly one active reconciler for the current scale posture; see [Scaling And Restarts](../deployment/scaling-and-restarts.md).
3. Check reconciler process logs for database, queue, registry, or gRPC TLS errors.
4. Verify the reconciler is using the same global database as the API and cron: shared `VECTIS_DATABASE_DSN`, or `VECTIS_GLOBAL_DATABASE_DSN` when global/cell databases are split.
5. Verify queue resolution through the pinned queue address or registry path configured for the reconciler.
6. Inspect `vectis_reconciler_reenqueue_total` by outcome and `vectis_retries_exhausted_total`.
7. For impacted runs, use `vectis-cli runs show <run-id>` to confirm whether later reconciler dispatch events appear.
8. If the reconciler is healthy but one urgent run remains queued, use `vectis-cli runs retry <run-id>` after confirming no worker currently owns it.

## Log Service Repair

Use this when `health check` warns on `log.reachable`, log append failures increase, or log streaming fails.

1. Run `vectis-cli health check --json` and inspect the `log.reachable` evidence.
2. Confirm `vectis-log` gRPC health is serving.
3. Confirm the log storage directory is mounted, writable, and has free space.
4. Verify API and worker log client resolution: pinned log address or registry registration.
5. Verify gRPC TLS settings: CA file, server name, and certificate SANs. See [Configuration](../configuration.md#internal-grpc-tls).
6. Check `vectis_log_storage_append_failures_total`, memory buffer drops, subscriber drops, and log service disk telemetry.
7. On worker hosts, check log-forwarder spool age and size if a forwarder is deployed.
8. After repair, trigger a small known-safe job and confirm `vectis-cli logs run <run-id>` returns fresh output.

## Audit Durability Repair

Use this when `health check` warns on `audit.drops.recent` or `audit.flush.failures`, or audit alerts fire.

1. Confirm API and database readiness with `vectis-cli health check --strict`.
2. Check API logs for audit buffer full, flush failure, or database write errors.
3. Inspect `vectis_audit_events_dropped_total` and `vectis_audit_flush_failures_total`.
4. Check database pool pressure with the `db.connection.pool` health check and DB metrics.
5. Reduce audit event pressure or increase API/database capacity before clearing the incident.
6. Record dropped-event windows in the incident record; dropped audit events cannot be reconstructed from Vectis state.

## Database Pool Pressure

Use this when `health check` warns on `db.connection.pool` or the DB pool alert fires.

1. Run `vectis-cli health check --json` and record `db.connection.pool` evidence.
2. Check whether API, cron, reconciler, workers, or other DB-using processes recently scaled up.
3. Check database availability, slow queries, locks, and server-side connection limits.
4. Review Vectis pool settings; see [Configuration](../configuration.md#postgresql-connection-pool-pgx-only).
5. If the database is overloaded, reduce replica count or workload pressure before raising pool limits.
6. Confirm `db.connection.pool` returns to `pass` and API 5xx rates recover.

## Schema Or Migration Repair

Use this when `health check` fails `db.schema.current`, API readiness reports database/schema issues, or a restore drill reaches migration checks.

1. Stop workers, cron, reconciler, and catalog if the schema state is uncertain.
2. Confirm `VECTIS_DATABASE_DRIVER` and `VECTIS_DATABASE_DSN` point at the intended database. For split global/cell deployments, run this once for the global DSN and once for the cell DSN.
3. Run the migration from the same network/config context used by the deployment:

```sh
vectis-cli database migrate
```

4. Restart API first.
5. Run `vectis-cli health check --strict`.
6. Restart workers, cron, reconciler, and catalog after `api.ready` and `db.schema.current` pass.
7. For restore-specific order and partial-restore outcomes, use [Backup And Restore](./backup-restore.md).

## Retention Cleanup

Use this when SQL storage pressure grows, old retained records alert, or a maintenance window calls for cleanup.

1. Read [Retention](./retention.md) and confirm the retention window is acceptable for the environment.
2. Preview cleanup:

```sh
vectis-cli retention cleanup --dry-run
```

3. Include `--log-storage-dir` only when pruning local durable run log files for the same deployment.
4. Review delete counts, cutoffs, and backup status before applying.
5. Apply during a maintenance window:

```sh
vectis-cli retention cleanup --yes
```

6. Run `vectis-cli health check --strict`.
7. Check storage pressure metrics after cleanup.

## Manual Run Intervention

Use this when an operator must change one run after automatic recovery has had time to act.

1. Read the run:

```sh
vectis-cli runs show <run-id>
```

2. Inspect status, failure reason, orphan reason, lease/worker ownership if visible, and dispatch events.
3. Prefer automatic repair first: fix queue, log, database, registry, TLS, and reconciler issues, then wait at least one reconciler interval for queued runs.
4. To retry a failed, orphaned, or safely queued run:

```sh
vectis-cli runs retry <run-id>
```

5. To request cancellation of a currently executing run:

```sh
vectis-cli runs cancel <run-id>
```

6. To mark a run failed after external investigation:

```sh
vectis-cli runs fail <run-id> --reason "operator reason"
```

7. Re-read the run and confirm the terminal or queued state is the one you intended.

## After Repair

1. Run `vectis-cli health check --strict`.
2. Trigger a small known-safe job.
3. Confirm the run reaches a terminal state.
4. Confirm logs are readable.
5. Check the relevant metrics for at least one reconciler interval.
6. Record the root cause, impacted run IDs, commands used, and any data loss or audit gaps.

## Related Documentation

| Topic | Document |
| --- | --- |
| Reliability landing page | [Runbooks And Alerts](./runbooks.md) |
| Dispatch event triage | [Dispatch Visibility](./dispatch-visibility.md) |
| Retention policy | [Retention](./retention.md) |
| Backup and restore | [Backup And Restore](./backup-restore.md) |
| Health check catalog | [Health Check Catalog](../reference/health-check-catalog.md) |
| Scaling and restart posture | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
