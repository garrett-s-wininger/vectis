# Backup, Restore, And Disaster Recovery

Use this page when you are designing backups, running a restore drill, or recovering a Vectis deployment after data loss.

The safest recovery point is a matching set of database, queue, log, artifact, secret, TLS, and deployment config backups from the same time window. If you can only restore part of that set, Vectis can repair some queue handoff gaps, but it cannot recreate lost job definitions, auth records, audit rows, secrets, logs, or artifact blobs.

For production-oriented deployments, this page is the required drill behind
[Production Topology v1](../deployment/production-topology-v1.md) and
[Production Config And Secrets Contract](../deployment/production-config-contract.md).

## Backup Inventory

Back up the SQL database first and most carefully. It is the source of truth for durable Vectis state. Queue, log, artifact, and job secret state are still important; queue handoff gaps are easier to repair than missing logs, artifacts, or secret material.

| State | Why it matters | Typical location |
| --- | --- | --- |
| SQL database | Source of truth for jobs, job definitions, runs, schedules, auth, RBAC, audit, idempotency, dispatch events, and setup state | SQLite file from `VECTIS_DATABASE_DSN`, role files from `VECTIS_GLOBAL_DATABASE_DSN` / `VECTIS_CELL_DATABASE_DSN`, or the configured Postgres database |
| Queue persistence | Pending, in-flight, and DLQ delivery state when queue persistence is enabled | `VECTIS_QUEUE_PERSISTENCE_DIR`, `vectis-queue --persistence-dir`, or the default `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>` |
| Log storage | Durable run logs served by `vectis-log` | `VECTIS_LOG_STORAGE_DIR`, `vectis-log --storage-dir`, or the default `$XDG_DATA_HOME/vectis/log/<instance-id>` |
| Artifact storage | Durable content-addressed blobs served by `vectis-artifact` | `VECTIS_ARTIFACT_STORAGE_DIR`, `vectis-artifact --storage-dir`, or the default `$XDG_DATA_HOME/vectis/artifact/<instance-id>` |
| Log-forwarder spool | Worker-side batches not yet delivered to log service | Configured log-forwarder spool directory |
| Job secret store | Encrypted secret envelopes, Knox auth tokens, and provider key material used by `vectis-secrets` | `VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_ROOT`, `VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_KEY_FILE`, `VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN_FILE`, or matching deploy volumes |
| Deployment secrets | Postgres password, API bootstrap token, rendered local deploy secrets | `VECTIS_DEPLOY_CONFIG_DIR/podman` or the OS user config Vectis deploy directory |
| TLS material | gRPC CA, server certs, client certs, Postgres TLS CA/certs, local dev TLS | Paths configured under `VECTIS_GRPC_TLS_*`, Podman TLS volumes, and `$XDG_DATA_HOME/vectis/local-tls` for `vectis-local` |
| Config and manifests | The exact deployment shape needed to interpret restored paths and credentials | Environment, rendered kube YAML, Podman/systemd/unit manifests, ConfigMaps, dashboards |
| Observability customizations | Dashboards, alert rules, and log shipping config used during incident review | Grafana/OpenSearch/Prometheus configuration outside the Vectis DB |

Treat this inventory as sensitive data. Database backups, queue persistence, log storage, artifact storage, job secret stores, and rendered config can contain job definitions, token hashes, operational metadata, and secrets.

Use `vectis-cli backup inventory --format json` on each host that owns local
Vectis state before a planned backup or restore drill. The command emits Vectis
version, schema visibility, redacted database DSNs, queue/log/artifact/spool
paths, configured secret and TLS paths, service instance IDs, and path
readability evidence. It does not perform a backup; store its JSON output as
scope evidence next to the backup set.

After collecting host inventories, build and verify a backup manifest:

```sh
vectis-cli backup manifest --format json host-a.inventory.json host-b.inventory.json > backup-manifest.json
vectis-cli backup verify backup-manifest.json
```

The manifest records the database roles, service instance IDs, and required
paths from every inventory input. Verification fails when core database,
queue, log, or artifact evidence is missing, when a required path was missing
or unreadable during inventory capture, or when a database schema was marked
dirty. Missing secret store, TLS, or config paths are warnings because some
deployments intentionally delegate those surfaces to external systems.

When you know the intended deployment shape, make absent hosts or shards hard
failures with an expected topology file:

```sh
vectis-cli backup verify --expect expected-topology.json backup-manifest.json
```

For the Podman reference deployment, generate that file from the deployment
profile:

```sh
vectis-cli backup expect podman --profile ha --format json > expected-topology.json
vectis-cli backup verify --expect expected-topology.json backup-manifest.json
```

For the Linux service artifacts, generate a baseline from the services manifest:

```sh
vectis-cli backup expect linux --manifest deploy/linux/services.toml --format json > expected-topology.json
vectis-cli backup verify --expect expected-topology.json backup-manifest.json
```

Linux expectation output uses the manifest's example environment. Config
management still owns real host placement, extra shards, and overrides; edit or
augment the generated file for production-specific topology. When collecting
inventory for Linux service hosts, run with the service environment loaded so
`XDG_CONFIG_HOME=/etc` records `/etc/vectis` as config evidence.

## Storage Integrity Verification

Use `vectis-cli storage verify` against the local paths named by `backup
inventory` before accepting a backup set or after restoring files into an
isolated environment.

```sh
vectis-cli storage verify queue --dir "$VECTIS_QUEUE_PERSISTENCE_DIR"
vectis-cli storage verify logs --dir "$VECTIS_LOG_STORAGE_DIR"
vectis-cli storage verify artifact --dir "$VECTIS_ARTIFACT_STORAGE_DIR"
vectis-cli storage verify log-forwarder-spool --dir "$VECTIS_LOG_FORWARDER_SPOOL_DIR"
vectis-cli storage verify worker-log-spool --dir /var/lib/vectis/worker-log-spool/pending
```

The verifier is read-only. It checks queue snapshot/WAL JSON and protobuf
payloads, durable run-log record framing, artifact blob SHA-256 path digests,
log-forwarder spool CRCs, and worker pending log-spool frames. Text output is
`key=value`; `--format json` emits the same report model for release evidence.
Any corrupt file, digest mismatch, malformed record, or quarantined spool exits
non-zero.

For machine-readable backup evidence, save JSON reports and attach them to
`backup verify`:

```sh
vectis-cli storage verify queue --dir "$VECTIS_QUEUE_PERSISTENCE_DIR" --format json > queue.storage-report.json
vectis-cli storage verify logs --dir "$VECTIS_LOG_STORAGE_DIR" --format json > logs.storage-report.json
vectis-cli storage verify artifact --dir "$VECTIS_ARTIFACT_STORAGE_DIR" --format json > artifact.storage-report.json

vectis-cli backup verify backup-manifest.json \
  --storage-report queue.storage-report.json \
  --storage-report logs.storage-report.json \
  --storage-report artifact.storage-report.json \
  --storage-max-age 24h
```

When any `--storage-report` is supplied, `backup verify` requires every
storage-backed `local_state` path in the manifest to have a matching `ok`
report for the expected surface and path. Use additional reports for
`log-forwarder-spool` and `worker-log-spool` when those paths are present in
the manifest.

After a restored deployment passes a known-safe smoke run, save one retained
restore validation artifact that binds the backup manifest, storage reports, and
smoke run status:

```sh
vectis-cli backup restore-validation backup-manifest.json \
  --expect expected-topology.json \
  --storage-report queue.storage-report.json \
  --storage-report logs.storage-report.json \
  --storage-report artifact.storage-report.json \
  --storage-max-age 24h \
  --smoke-run "$RESTORE_SMOKE_RUN_ID" \
  --deployment production \
  --profile v1 \
  --format json > restore-validation.json
```

`backup restore-validation` exits non-zero if manifest verification fails, a
required storage report is missing or stale, or the referenced smoke run did not
finish with status `succeeded`. Retain the JSON output with release, audit,
disaster recovery, or compliance evidence.

Retention cleanup can use the same manifest as a destructive-operation gate:

```sh
vectis-cli retention cleanup --yes \
  --require-backup-manifest \
  --backup-manifest backup-manifest.json \
  --backup-expect expected-topology.json \
  --backup-max-age 24h \
  --backup-storage-report queue.storage-report.json \
  --backup-storage-report logs.storage-report.json \
  --backup-storage-report artifact.storage-report.json \
  --backup-storage-max-age 24h
```

The retention command verifies the manifest before deleting SQL rows or local
files, and rejects stale evidence when `--backup-max-age` or
`--backup-storage-max-age` is set.

Expected topology files are JSON. `inventory_sources` and each
`inventory_source` matcher must use the same source strings passed to
`backup manifest`.

```json
{
  "schema_version": 1,
  "inventory_sources": ["host-a.inventory.json", "host-b.inventory.json"],
  "instances": [
    {"service": "queue", "instance_id": "queue-1"},
    {"service": "log", "instance_id": "log-1"},
    {"service": "artifact", "instance_id": "artifact-1"}
  ],
  "paths": [
    {"inventory_source": "host-a.inventory.json", "category": "local_state", "id": "queue.persistence"},
    {"inventory_source": "host-b.inventory.json", "category": "local_state", "id": "artifact.storage"}
  ],
  "require_categories": ["database", "local_state", "secret_stores", "tls_files", "config_paths"]
}
```

## Backup Timing

Prefer backups that capture these pieces close together:

1. SQL database.
2. Queue persistence directory.
3. Log storage, artifact storage directories or object-store buckets, job secret store, and log-forwarder spools.
4. Deployment secrets, TLS material, config, and manifests.
5. Observability rules and dashboards.

For Postgres, use your database platform's online backup or snapshot mechanism. File-level backups of a live Postgres data directory are not enough unless the database platform documents that procedure as crash-consistent.

For SQLite, stop Vectis or use a SQLite-safe backup process before copying the database file.

## Production v1 Drill

Run this drill before declaring a production v1 deployment ready, before major
schema upgrades, and on a regular operations cadence.

For the shorter rehearsal checklist that ties restore validation to upgrade and
rollback drill evidence, see [Production Drills](./production-drills.md).

| Phase | Evidence to collect |
| --- | --- |
| Scope | Deployment name, Vectis version, topology, service instance IDs, queue/log/artifact shard IDs, database DSNs without secrets, and durable storage paths. |
| Backup | Backup timestamp, database backup identifier, queue/log/artifact/secrets/TLS/config backup identifiers, storage verifier reports, and whether the backup set is same-window or intentionally partial. |
| Restore target | Isolated restore environment or maintenance window, restored DNS/endpoint plan, and operator who approved traffic isolation. |
| Migration | `vectis-cli database migrate` output for every restored database and resulting schema status. |
| Service recovery | Service start order, health check output, and any services intentionally skipped. |
| Data validation | Restored jobs/runs visible, restored logs readable, restored artifact blobs downloadable when expected, and secret-resolution smoke result when enabled. |
| New work | A known-safe run triggered after restore reaches terminal status and streams logs. |
| Repair | Reconciler outcomes for queued runs, DLQ state, and any manual repair actions. |
| Observability | Fresh metrics, service logs, dashboards, and alert routing after restore. |
| Exceptions | Missing backup pieces, data loss, rotated credentials, follow-up tickets, and whether production readiness is blocked. |

Recommended drill flow:

1. Run `vectis-cli backup inventory --format json` from each relevant host and record the expected restore point.
2. Build a backup manifest from those inventories and run `vectis-cli backup verify`, preferably with `--expect` for the intended topology.
3. Run `vectis-cli storage verify --format json` for each restored or backed-up queue, log, artifact, log-forwarder spool, and worker pending log-spool path that belongs to the drill scope, then rerun `backup verify` with each report passed as `--storage-report`.
4. Restore into an isolated environment when possible. If the drill uses the production environment, schedule a maintenance window and stop producers first.
5. Restore config, secrets, TLS material, and manifests before starting services.
6. Restore PostgreSQL through the database platform's documented process.
7. Restore queue persistence, log storage, artifact storage directories or object-store buckets, secret envelopes, SPIFFE CA material, and log-forwarder spools that belong to the same backup window.
8. Run `vectis-cli storage verify --format json` against the restored file stores before starting services, then rerun `backup verify` with the restored storage reports attached.
9. Run migrations against every restored database.
10. Start services in dependency order.
11. Run `vectis-cli health check --strict`.
12. Save `vectis-cli health check --json` output as machine-readable evidence.
13. Run the restore smoke test below.
14. Build `vectis-cli backup restore-validation --format json` with the restored manifest, storage reports, and post-restore smoke run ID.
15. Record evidence and update the backup, config, or runbook gaps found during the drill.

Do not run retention cleanup during a restore drill until the restored deployment
passes the smoke test and the operator has accepted the restore point.

## Restore Order

Restore in dependency order, then start services in dependency order.

1. Stop API, cell ingress, cron, SCM poller, reconciler, catalog, workers, worker-core, orchestrator, queue, log, artifact, secrets, spiffe, and log-forwarder processes so no restored state is modified while files are being replaced.
2. Restore deployment config, secrets, and TLS material to the same paths or update environment variables before starting services.
3. Restore the SQL database.
4. Run `vectis-cli database migrate` for each restored SQL database using the same `VECTIS_DATABASE_DRIVER` and DSN settings that services will use.
5. Restore queue persistence, log storage, artifact storage directories or object-store buckets, and job secret store when available.
6. Restore log-forwarder spools on worker hosts if they are part of the backup set.
7. Start registry, queue, orchestrator, log, artifact, spiffe, and secrets first; then cell ingress, API, worker-core, workers, cron, SCM poller, reconciler, catalog, and log-forwarder.
8. Run the restore smoke test below.

Do not start cron, SCM poller, or workers before the database has been restored and migrations have been checked. They can enqueue, record trigger events, or execute work against an incomplete view of the world.

## Partial Restore Outcomes

Use this table when a restore cannot use one clean backup point.

| Restored state | Expected inconsistency | Repair path |
| --- | --- | --- |
| Database restored, queue persistence missing | Runs may be `queued` in the database but absent from the queue | Run `vectis-reconciler`; use manual run retry only after checking reconciler failures |
| Queue restored, database older than queue | Queue entries may reference missing or stale runs | Prefer restoring database and queue from the same backup point; otherwise drain cautiously and inspect failed dequeues |
| Database restored, log storage missing | Completed runs still exist but logs may be unavailable | Mark as data loss in the incident record; rerun jobs only when safe |
| Database restored, artifact byte storage missing | Runs and manifests may reference blobs that are no longer available | Mark as data loss in the incident record; rerun producing jobs only when safe |
| Log storage restored, database missing run rows | Logs may be orphaned and not discoverable through API run history | Keep logs for forensic review, but restore the database from a better backup if possible |
| Secrets/TLS missing | Services may fail auth, database TLS, or gRPC TLS | Restore secrets from backup; rotating secrets may require coordinated config changes and client re-login |
| Dashboard/config missing | Core Vectis may run, but operators lose visibility | Restore dashboards/alerts before declaring DR complete |

The reconciler is the primary repair loop for database rows that should have reached the queue but did not. It cannot reconstruct lost job definitions, auth records, audit rows, or logs.

## Before You Start Services

Check these before bringing the restored deployment back online:

| Check | Why it matters |
| --- | --- |
| Database DSN points at the restored database | Prevents services from writing to the wrong database. |
| Schema migration status is current | Prevents old schema errors during API, cron, worker, or reconciler startup. |
| Queue, log, artifact, and job secret store paths match restored volumes | Prevents services from starting with empty replacement directories by accident. |
| TLS files and server names match config | Prevents internal gRPC and database TLS failures. |
| Bootstrap token expectations are clear | A restored database that already completed setup does not need a standing bootstrap token. |
| Retention cleanup is paused until verification | Prevents cleanup from deleting evidence before the restore is validated. |

## SQLite / Local DR Runbook

Use this for `vectis-local`, local development, single-node SQLite deployments, and test environments that rely on local files.

1. Stop `vectis-local` or every standalone `vectis-*` process.
2. Copy the SQLite database file or files from backup to the configured `VECTIS_DATABASE_DSN` path, or to the role-specific `VECTIS_GLOBAL_DATABASE_DSN` and `VECTIS_CELL_DATABASE_DSN` paths when split.
3. Restore `$XDG_DATA_HOME/vectis/queue`, `$XDG_DATA_HOME/vectis/log`, `$XDG_DATA_HOME/vectis/artifact`, `$XDG_DATA_HOME/vectis/jobs`, `$XDG_DATA_HOME/vectis/local-tls`, and cell-local secrets directories/keys under `$XDG_DATA_HOME/vectis/cells/<cell>/` when they are part of the backup. Queue shard directories are nested below `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>` by default; log shard directories are nested below `$XDG_DATA_HOME/vectis/log/<instance-id>` by default; artifact shard directories are nested below `$XDG_DATA_HOME/vectis/artifact/<instance-id>` by default.
4. Restore CLI token and local deploy secrets only when you intentionally want the same local identity state.
5. Run `vectis-cli database migrate` with the restored database settings, once per restored SQL database.
6. Start `vectis-local` or the standalone services.
7. Run the restore smoke test.

For a local-only restore where queue persistence was not backed up, start the reconciler and wait for old queued runs to be redispatched before judging the queue as empty.

## Postgres / Production DR Runbook

Use this for the reference Podman deployment and any production-like deployment backed by Postgres.

For the Podman reference deployment, treat the generated deploy config
directory and these named volumes as the restore unit:

- `VECTIS_DEPLOY_CONFIG_DIR/podman`
- `vectis-postgres-data`
- `vectis-queue-data`
- `vectis-log-data`
- `vectis-artifact-data`
- `vectis-secrets-data`
- `vectis-spiffe-data`

The reference e2e restore drill exercises a stopped-stack media restore by
running a secret/log/artifact smoke job, stopping `deploy podman`, exporting the
volumes with `podman volume export`, deleting the pod, volumes, and pod secret,
importing the archives with `podman volume import`, restarting `deploy podman`,
confirming the pre-restore run/log/artifact, and running a second smoke job.
Keep `vectis-cli backup expect podman --profile <profile> --format json`
output next to the volume archives so the captured media is tied to the intended
reference topology.

1. Stop external trigger sources or block API traffic at the edge.
2. Stop API, cell ingress, cron, SCM poller, reconciler, catalog, workers, worker-core, orchestrator, queue, log, artifact, secrets, spiffe, and log-forwarder containers/processes.
3. Restore or recreate deployment config, secrets, and TLS volumes. If secrets are recreated instead of restored, update all DSNs, trust bundles, client credentials, and service identity allowlists consistently.
4. Restore Postgres from the database backup using the database platform's restore process.
5. Restore queue persistence, log storage, artifact storage directories or object-store buckets, job secret store, SPIFFE CA material, and log-forwarder spools from matching backups when available.
6. Verify file ownership and permissions for restored volumes before services start.
7. Run `vectis-cli database migrate` against each restored Postgres DSN from the same host/network path used for deployment migrations.
8. Start registry, queue, orchestrator, log, artifact, spiffe, and secrets first.
9. Start cell ingress and API after their dependencies are healthy.
10. Start worker-core and workers.
11. Start cron only when you are ready for scheduled work to resume.
12. Start reconciler and catalog.
13. Start log-forwarder where worker-host spooling is used.
14. Run `vectis-cli health check --strict`.
15. Save `vectis-cli health check --json` output as machine-readable evidence.
16. Run the restore smoke test and confirm dashboards/alerts are receiving fresh data.

If queue persistence was not restored, expect the reconciler to redispatch
queued runs that still exist in the database. If log storage or artifact byte
storage was not restored, mark that as data loss; Vectis cannot reconstruct
those bytes from SQL metadata.

## Restore Smoke Test

Run this after every restore drill and after real disaster recovery:

1. Check API liveness and readiness: `GET /health/live` and `GET /health/ready`.
2. If auth is enabled, verify setup state and log in with an expected operator account or token.
3. List jobs with `vectis-cli jobs list --repository <repo>`.
4. List recent runs for one restored job with `vectis-cli runs list <job-id> --repository <repo>`.
5. Fetch logs for one restored run when log storage was part of the backup set.
6. Download or stat one restored artifact when artifact storage was part of the backup set.
7. Trigger a small known-safe job.
8. Confirm the run reaches a terminal status.
9. Stream or fetch logs for the new run.
10. List artifacts for the new run when the job produces artifacts.
11. If secret resolution is enabled, trigger or replay a known secret-using smoke job and verify the run succeeds without exposing secret plaintext in logs.
12. Inspect queue/reconciler/worker metrics for retry exhaustion or stuck queued runs.
13. Inspect dispatch events for the restored and newly triggered run.
14. Confirm Prometheus, logs, and dashboards show fresh samples from the restored services.

`vectis-cli health check` automates the API-oriented part of this smoke test: API liveness, API readiness, auth-aware setup status, auth-aware local CLI token visibility, schema status, queue backlog, cron schedule backlog, reconciler recovery visibility, stuck queued runs, log reachability, audit drops/flush failures, and DB pool pressure. Keep the active run trigger/log verification and dashboard freshness checks in the manual drill.

## What To Record

Record these after a drill or real recovery:

- Backup timestamp and restore timestamp.
- Vectis release version.
- Production topology shape and whether the restore target matched it.
- Restored schema version and migration result.
- Which backup pieces were restored and which were missing.
- Service instance IDs and durable storage paths used after restore.
- `vectis-cli health check --strict` result.
- `vectis-cli health check --json` artifact.
- New smoke run ID and terminal status.
- Any known data loss, especially missing logs, orphaned queue entries, or credentials that had to be rotated.
- Smoke test result and any follow-up repair work.

## Related Docs

| Need | Doc |
| --- | --- |
| Current health check output | [Health Check Catalog](../reference/health-check-catalog.md) |
| Queued run repair after partial restore | [Dispatch Visibility](./dispatch-visibility.md) |
| Manual repair recipes | [Repair Runbooks](./repair-runbooks.md) |
| Data retention and cleanup | [Retention And Storage Pressure](./retention.md) |
| Secret handling during deploy and recovery | [Secrets And Redaction](../deployment/secrets-and-redaction.md) |
| Production config and secrets | [Production Config And Secrets Contract](../deployment/production-config-contract.md) |
| Production Linux deployment | [Production Linux Deployment](../deployment/production-linux.md) |
| Production drills | [Production Drills](./production-drills.md) |
