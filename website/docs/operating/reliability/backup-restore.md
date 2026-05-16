# Backup, Restore, And Disaster Recovery

This page defines what Vectis state operators should preserve, how to restore it, and what partial restores mean.

## Backup Inventory

| State | Why it matters | Typical location |
| --- | --- | --- |
| SQL database | Source of truth for jobs, job definitions, runs, schedules, auth, RBAC, audit, idempotency, dispatch events, and setup state | SQLite file from `VECTIS_DATABASE_DSN`, or the configured Postgres database |
| Queue persistence | Pending, in-flight, and DLQ delivery state when queue persistence is enabled | `VECTIS_QUEUE_PERSISTENCE_DIR` or `vectis-queue --persistence-dir` |
| Log storage | Durable run logs served by `vectis-log` | `VECTIS_LOG_STORAGE_DIR` or `vectis-log --storage-dir` |
| Log-forwarder spool | Worker-side batches not yet delivered to log service | Configured log-forwarder spool directory |
| Deployment secrets | Postgres password, API bootstrap token, rendered local deploy secrets | `VECTIS_DEPLOY_CONFIG_DIR/podman` or the OS user config Vectis deploy directory |
| TLS material | gRPC CA, server certs, client certs, Postgres TLS CA/certs, local dev TLS | Paths configured under `VECTIS_GRPC_TLS_*`, Podman TLS volumes, and `$XDG_DATA_HOME/vectis/local-tls` for `vectis-local` |
| Config and manifests | The exact deployment shape needed to interpret restored paths and credentials | Environment, rendered kube YAML, Podman/systemd/unit manifests, ConfigMaps, dashboards |
| Observability customizations | Dashboards, alert rules, and log shipping config used during incident review | Grafana/OpenSearch/Prometheus configuration outside the Vectis DB |

Back up the SQL database first and most carefully. Queue and log state can be repaired or partially missing, but losing the database loses authoritative run and security state.

## Restore Order

1. Stop API, cron, reconciler, workers, queue, log, and log-forwarder processes so no restored state is modified while files are being replaced.
2. Restore deployment config, secrets, and TLS material to the same paths or update environment variables before starting services.
3. Restore the SQL database.
4. Run `vectis-cli database migrate` with the same `VECTIS_DATABASE_DRIVER` and `VECTIS_DATABASE_DSN` that services will use.
5. Restore queue persistence and log storage when available.
6. Restore log-forwarder spools on worker hosts if they are part of the backup set.
7. Start registry, queue, and log first; then API; then workers, cron, and reconciler.
8. Run the restore smoke test below.

Do not start cron or workers before the database has been restored and migrations have been checked. They can enqueue or execute work against an incomplete view of the world.

## Partial Restore Outcomes

| Restored state | Expected inconsistency | Repair path |
| --- | --- | --- |
| Database restored, queue persistence missing | Runs may be `queued` in the database but absent from the queue | Run `vectis-reconciler`; use manual run retry only after checking reconciler failures |
| Queue restored, database older than queue | Queue entries may reference missing or stale runs | Prefer restoring database and queue from the same backup point; otherwise drain cautiously and inspect failed dequeues |
| Database restored, log storage missing | Completed runs still exist but logs may be unavailable | Mark as data loss in the incident record; rerun jobs only when safe |
| Log storage restored, database missing run rows | Logs may be orphaned and not discoverable through API run history | Keep logs for forensic review, but restore the database from a better backup if possible |
| Secrets/TLS missing | Services may fail auth, database TLS, or gRPC TLS | Restore secrets from backup; rotating secrets may require coordinated config changes and client re-login |
| Dashboard/config missing | Core Vectis may run, but operators lose visibility | Restore dashboards/alerts before declaring DR complete |

The reconciler is the primary repair loop for database rows that should have reached the queue but did not. It cannot reconstruct lost job definitions, auth records, audit rows, or logs.

## SQLite / Local DR Runbook

1. Stop `vectis-local` or every standalone `vectis-*` process.
2. Copy the SQLite database file from backup to the configured `VECTIS_DATABASE_DSN` path.
3. Restore `$XDG_DATA_HOME/vectis/queue`, `$XDG_DATA_HOME/vectis/jobs`, and `$XDG_DATA_HOME/vectis/local-tls` when they are part of the backup.
4. Restore CLI token and local deploy secrets only when you intentionally want the same local identity state.
5. Run `vectis-cli database migrate` with the restored database settings.
6. Start `vectis-local` or the standalone services.
7. Run the restore smoke test.

For a local-only restore where queue persistence was not backed up, start the reconciler and wait for old queued runs to be redispatched before judging the queue as empty.

## Postgres / Reference Deploy DR Runbook

1. Stop API, workers, cron, reconciler, queue, log, and log-forwarder containers/processes.
2. Restore Postgres from the database backup using the database platform's restore process.
3. Restore or recreate the Podman deploy secrets and TLS volumes. If secrets are recreated instead of restored, update all generated DSNs and client credentials consistently.
4. Restore queue persistence, log storage, and log-forwarder spools from matching backups when available.
5. Run `vectis-cli database migrate` against the restored Postgres DSN from the same host/network path used for deployment migrations.
6. Start registry, queue, log, API, workers, cron, and reconciler in dependency order.
7. Run the restore smoke test and confirm dashboards/alerts are receiving fresh data.

Prefer database-native online backups for Postgres. File-level backups of a live Postgres data directory are not enough unless your database platform documents that procedure as crash-consistent.

## Restore Smoke Test

Run this after every restore drill and after real disaster recovery:

1. Check API liveness and readiness: `GET /health/live` and `GET /health/ready`.
2. If auth is enabled, verify setup state and log in with an expected operator account or token.
3. List jobs with `vectis-cli jobs list`.
4. List recent runs for one restored job with `vectis-cli runs list <job-id>`.
5. Trigger a small known-safe job.
6. Confirm the run reaches a terminal status.
7. Stream or fetch logs for the new run.
8. Inspect queue/reconciler/worker metrics for retry exhaustion or stuck queued runs.
9. Inspect dispatch events for the restored and newly triggered run.
10. Confirm Prometheus, logs, and dashboards show fresh samples from the restored services.

`vectis-cli health check` automates the API-oriented part of this smoke test: API liveness, API readiness, auth-aware setup status, auth-aware local CLI token visibility, schema status, queue backlog, reconciler recovery visibility, stuck queued runs, log reachability, audit drops/flush failures, and DB pool pressure. Keep the active run trigger/log verification and dashboard freshness checks in the manual drill.

Record the backup timestamp, restored schema version, release version, and any partial-restore data loss in the incident or drill notes.

See [DOCTOR_CHECK_CATALOG.md](../reference/doctor-check-catalog.md) for the current check catalog and output contract.
