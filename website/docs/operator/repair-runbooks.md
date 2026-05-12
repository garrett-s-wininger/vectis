# Repair Runbooks

These recipes are for operators responding to failed `doctor` checks, alerts, restore drills, or stuck runs. Start with `vectis-cli doctor` when the API is reachable, then use the recipe that matches the failing check or alert.

Use `vectis-cli run get <run-id>` as the first run-specific command. It includes run status and dispatch events without requiring direct SQL access.

## Quick Map

| Symptom or check | Recipe |
| --- | --- |
| `queue.backlog.ratio`, queue backlog alert, queued run not starting | [Queued Runs Or Backlog](#queued-runs-or-backlog) |
| `reconciler.active`, `reconciler.stuck.runs`, reconciler alert | [Reconciler Repair](#reconciler-repair) |
| `log.reachable`, log append/drop alert | [Log Service Repair](#log-service-repair) |
| `audit.drops.recent`, `audit.flush.failures`, audit alert | [Audit Durability Repair](#audit-durability-repair) |
| `db.connection.pool`, DB pool alert | [Database Pool Pressure](#database-pool-pressure) |
| `db.schema.current` | [Schema Or Migration Repair](#schema-or-migration-repair) |
| Old retained records or SQL storage pressure | [Retention Cleanup](#retention-cleanup) |
| A run needs operator intervention | [Manual Run Intervention](#manual-run-intervention) |

## Queued Runs Or Backlog

Use this when queued runs are not draining, `doctor` warns on `queue.backlog.ratio`, or `VectisQueueBacklogGrowing` fires.

1. Run `vectis-cli doctor --strict` and note failures for `api.ready`, `queue.backlog.ratio`, `reconciler.active`, and `reconciler.stuck.runs`.
2. For a specific run, run `vectis-cli run get <run-id>` and inspect `status` plus `dispatch_events`.
3. If there is no dispatch event, follow [DISPATCH_VISIBILITY.md#runbook-queued-with-no-dispatch](dispatch-visibility.md#runbook-queued-with-no-dispatch).
4. If dispatch failed, follow [DISPATCH_VISIBILITY.md#runbook-dispatch-failure](dispatch-visibility.md#runbook-dispatch-failure).
5. Confirm workers are running and receiving jobs by checking worker process health and `vectis_worker_jobs_received_total`.
6. Check queue gRPC health and queue metrics: pending jobs, in-flight deliveries, and DLQ size.
7. If the queue lost non-persistent state after restart, keep `vectis-reconciler` running and wait at least one reconciler interval before manual intervention.
8. Use `vectis-cli force-requeue <run-id>` only after automatic redispatch is not progressing and the run is not already running or succeeded.

## Reconciler Repair

Use this when `doctor` warns on `reconciler.active` or `reconciler.stuck.runs`, or `VectisReconcilerReenqueueFailures` fires.

1. Confirm API readiness: `vectis-cli doctor --strict`.
2. Confirm exactly one active reconciler for the current scale posture; see [SCALING_AND_RESTARTS.md](scaling-and-restarts.md).
3. Check the reconciler process logs for database, queue, registry, or gRPC TLS errors.
4. Verify the reconciler is using the same database settings as the API, cron, and other SQL writers.
5. Verify queue resolution through the pinned queue address or registry path configured for the reconciler.
6. Inspect `vectis_reconciler_reenqueue_total` by outcome and `vectis_retries_exhausted_total`.
7. For impacted runs, use `vectis-cli run get <run-id>` to confirm whether later reconciler dispatch events appear.
8. If the reconciler is healthy but one urgent run remains queued, use `vectis-cli force-requeue <run-id>` after checking that no worker currently owns the run.

## Log Service Repair

Use this when `doctor` warns on `log.reachable`, log append failures increase, or log streaming fails.

1. Check `vectis-cli doctor --json` and inspect the `log.reachable` evidence/state.
2. Confirm `vectis-log` gRPC health is serving and its storage directory is writable.
3. Verify API and worker log client resolution: pinned log address or registry registration.
4. Verify gRPC TLS settings: CA file, server name, and certificate SANs. See [CONFIGURATION.md](configuration.md#internal-grpc-tls).
5. Check `vectis_log_storage_append_failures_total`, memory buffer drops, subscriber drops, and log service disk space.
6. On worker hosts, check log-forwarder spool age and size if a forwarder is deployed.
7. After repair, trigger a small known-safe job and confirm `vectis-cli logs run <run-id>` returns fresh output.

## Audit Durability Repair

Use this when `doctor` warns on `audit.drops.recent` or `audit.flush.failures`, or audit alerts fire.

1. Confirm API and database readiness with `vectis-cli doctor --strict`.
2. Check API logs for audit buffer full, flush failure, or database write errors.
3. Inspect `vectis_audit_events_dropped_total` and `vectis_audit_flush_failures_total`.
4. Check database pool pressure using the `db.connection.pool` doctor check and DB metrics.
5. Reduce audit event pressure or increase API/database capacity before clearing the incident.
6. Record dropped-event windows in the incident record; dropped audit events cannot be reconstructed from Vectis state.

## Database Pool Pressure

Use this when `doctor` warns on `db.connection.pool` or the DB pool alert fires.

1. Run `vectis-cli doctor --json` and record `db.connection.pool` evidence.
2. Check whether API, cron, reconciler, and any other DB-using processes recently scaled up.
3. Check database availability, slow queries, and server-side connection limits.
4. Tune Vectis pool settings for the deployment; see [CONFIGURATION.md](configuration.md#common-operator-settings).
5. If the database is overloaded, reduce replica count or workload pressure before raising pool limits.
6. Confirm `db.connection.pool` returns to `pass` and API 5xx rates recover.

## Schema Or Migration Repair

Use this when `doctor` fails `db.schema.current`, API readiness reports database/schema issues, or a restore drill reaches migration checks.

1. Stop workers, cron, and reconciler if the schema state is uncertain.
2. Confirm `VECTIS_DATABASE_DRIVER` and `VECTIS_DATABASE_DSN` point at the intended database.
3. Run `vectis-cli migrate` from the same network/config context used by the deployment.
4. Restart API first and run `vectis-cli doctor --strict`.
5. Restart workers, cron, and reconciler after `api.ready` and `db.schema.current` pass.
6. For restore-specific order and partial-restore outcomes, use [BACKUP_RESTORE.md](backup-restore.md).

## Retention Cleanup

Use this when SQL storage pressure grows, old retained records alert, or a maintenance window calls for cleanup.

1. Read [RETENTION.md](retention.md) and confirm the retention window is acceptable for the environment.
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

6. Run `vectis-cli doctor --strict` and check storage pressure metrics after cleanup.

## Manual Run Intervention

Use this when an operator must change an individual run state after automatic recovery has had time to act.

1. Get the run:

```sh
vectis-cli run get <run-id>
```

2. Inspect status, failure reason, orphan reason, and dispatch events.
3. Prefer automatic repair first: fix queue/log/database/reconciler issues and wait at least one reconciler interval for queued runs.
4. To retry a failed, orphaned, or safely queued run, use:

```sh
vectis-cli force-requeue <run-id>
```

5. Do not requeue a succeeded run. Do not requeue a running run unless you have confirmed the worker is gone and lease/reconciler behavior will not produce duplicate execution.
6. To mark a run failed after external investigation:

```sh
vectis-cli force-fail <run-id> --reason "operator reason"
```

7. To request cancellation of a cancellable executing run:

```sh
vectis-cli run cancel <run-id>
```

8. Re-read the run and confirm the terminal or queued state is the one you intended.

## After Repair

1. Run `vectis-cli doctor --strict`.
2. Trigger a small known-safe job.
3. Confirm the run reaches a terminal state and logs are readable.
4. Check the relevant metrics for at least one reconciler interval.
5. Record the root cause, impacted run IDs, commands used, and any data loss.
