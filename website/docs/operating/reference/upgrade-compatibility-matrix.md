# Upgrade Compatibility Matrix

This matrix turns the compatibility policy into an operator checklist. Use it before changing Vectis versions, especially when release notes mention SQL, config, API, queue, worker, log, artifact, secret, or deployment behavior.

Default rule: run one Vectis release version across all long-running services unless the release notes explicitly allow short version skew.

For the higher-level compatibility contract, see [Compatibility](../../concepts/compatibility.md). For migration authoring rules, see [Database Migrations](../../developing/migrations.md). For release note requirements, see [Releases And Upgrades](../../developing/releases.md).

## Pre-Upgrade Questions

| Question | Needed answer before rollout |
| --- | --- |
| Artifact set | Are all binaries, containers, generated deploy artifacts, and docs built from the same version and commit? |
| SQL migration | Does this release change `schema_migrations`, SQL tables, indexes, constraints, or DAL behavior? |
| Version skew | Can old binaries run against the new schema? Can new binaries start before migration? |
| Service coupling | Which services must stop or roll together? |
| Config | Are there new, renamed, removed, or default-changed config keys, env vars, flags, or service prefixes? |
| API/CLI | Are REST routes, OpenAPI shape, error codes, idempotency behavior, or CLI JSON fields changed? |
| gRPC/protobuf | Are protobuf fields, enum values, RPCs, or service semantics changed? |
| Runtime payloads | Did queue payloads, execution envelopes, action locks, task spec hashes, or worker-core contracts change? |
| Durable stores | Do SQL, queue persistence, log storage, artifact CAS, secret envelopes, SPIFFE CA material, or deploy secrets need backup or migration? |
| Rollback | Is rollback previous artifacts only, database restore, roll-forward repair, or safe down migration? |

If release notes do not answer these for a compatibility-sensitive release, treat the upgrade as coordinated downtime until proven otherwise.

## Surface Matrix

| Surface | Compatible additive changes | Requires release-note sequencing |
| --- | --- | --- |
| SQL schema | New nullable columns, tables, indexes, metadata, and expand/contract changes. | Dropping/renaming fields, tightening constraints, data rewrites, dirty migration recovery, or any migration old binaries cannot tolerate. |
| REST API v1 | New routes, new response fields, more specific documented error codes. | Removed/renamed routes, changed field types, changed auth actions, changed success status meaning, or changed stable error `code` meanings. |
| OpenAPI | New schemas, routes, optional fields, or descriptions. | Generated clients need regeneration because existing paths or field types changed. |
| CLI | New commands, flags, human-readable output improvements, new `--json` fields. | Removed commands/flags, changed exit-code semantics, removed or type-changed JSON fields. |
| gRPC/protobuf | New fields with new tags, new services/RPCs, compatible enum additions. | Reused tags, field type changes, removed fields without reserved tags/names, changed RPC semantics. |
| Queue and dispatch | Additional metadata tolerated by old consumers. | Delivery envelope shape changes, ack/claim behavior changes, queue persistence format changes, or dispatch idempotency changes. |
| Worker and worker-core | New optional capabilities, isolation backends, or descriptor metadata. | Action execution semantics, worker-core RPC compatibility, action lock meaning, task spec hash inputs, or cancellation semantics changed. |
| Logs and SSE | New event fields or control metadata clients can ignore. | Stream event names, ordering, replay controls, or log storage format changed incompatibly. |
| Artifacts | New manifest metadata or filters. | Blob key semantics, CAS layout, upload limits, shard routing, or download verification changed. |
| Secrets and SPIFFE | New provider metadata or stricter docs. | Secret delivery, broker auth, execution identity template, Workload API, key format, or policy semantics changed. |
| Config and deploy | New optional settings and examples. | Defaults that alter production behavior, renamed env vars, removed aliases, port changes, service unit changes, or generated secret layout changes. |
| Metrics and health | New metrics, labels, checks, or dashboards. | Removed/renamed metric names, label sets, health check IDs, or alert semantics. |
| Retention and backups | New cleanup surfaces with disabled or conservative defaults. | New durable stores, changed retention defaults, destructive cleanup changes, or backup inventory changes. |

## Rollout Order

Use release notes when they are more specific. Otherwise:

1. Read the release notes and identify the highest risk class.
2. Back up SQL and every durable non-SQL store that matters: queue persistence, log storage, artifact storage, job secret envelopes and keys, SPIFFE authority material, TLS material, deploy secrets, and config.
3. Stop `vectis-cron` and workers first if mixed execution is not explicitly allowed.
4. Run `vectis-cli database migrate` with the target `VECTIS_DATABASE_DRIVER` and `VECTIS_DATABASE_DSN`.
5. Roll internal services in dependency-aware order: registry, queue, orchestrator, log, artifact, SPIFFE, secrets, cell ingress, API, worker-core, workers, cron, reconciler, catalog, log-forwarder, docs.
6. Run `vectis-cli health check --strict`.
7. Run the upgrade smoke test: check versions, API health, job list, run list, trigger a safe job, wait for terminal status, and stream logs.
8. Watch queue backlog, due cron schedules, stuck runs, DB pool waits, worker failures, log/artifact failures, and security gate failures for at least one reconciler interval.

## Schema Readiness

Runtime services wait for the expected schema; they do not apply migrations themselves. Use:

```sh
vectis-cli database migrate
curl /api/v1/schema/status
vectis-cli health check --strict
```

`GET /api/v1/schema/status` returns:

| Field | Meaning |
| --- | --- |
| `has_schema` | Whether `schema_migrations` exists and contains a migration version. |
| `current_version` | Highest applied migration version when a schema exists. |

A dirty migration version blocks readiness. Repair it before rolling services.

## Rollback Choices

| Choice | Use when | Notes |
| --- | --- | --- |
| Previous artifacts only | Release notes say old binaries tolerate the current schema and config. | Fastest rollback, but unsafe when schema/data changed incompatibly. |
| Database restore plus previous artifacts | Migration or data rewrite is not downgrade-safe. | Requires a fresh backup taken before migration. Restore matching non-SQL stores when needed. |
| Roll-forward repair | New schema is sound but data or config needs correction. | Prefer when reverting would lose data or create more skew. |
| Safe down migration | Release notes explicitly say the down migration is production-safe for the operator's data shape. | Down migrations are required for development confidence, not automatic production rollback permission. |

## Durable Store Matrix

| Store | Upgrade concern | Backup/rollback note |
| --- | --- | --- |
| SQL database | Schema, jobs, runs, auth, RBAC, audit, idempotency, cron schedules, execution state. | Always back up before schema changes. |
| Queue persistence | Queued and in-flight deliveries. | Prefer database and queue backups from the same point; otherwise drain cautiously. |
| Log storage | Run logs outside SQL. | SQL restore cannot recreate missing log files. |
| Artifact CAS | Blob bytes outside SQL manifests. | SQL manifests reference blobs but do not contain blob bytes. |
| Secret envelopes and keys | encryptedfs job secret material. | Keys and envelopes must remain matched; rotate deliberately after exposure. |
| SPIFFE authority material | CA and registration state. | Restore or rotate consistently with worker and secrets identity config. |
| Deploy config/secrets | Bootstrap tokens, DB DSNs, TLS keys, generated deployment secrets. | Treat as production secrets and keep rollback copies. |

## Post-Upgrade Signals

| Signal | Healthy result |
| --- | --- |
| `vectis-cli --version` and daemon `--version` | Expected release version, commit, and build date. |
| `GET /health/live` | API process is alive. |
| `GET /health/ready` | API dependencies are ready. |
| `GET /api/v1/schema/status` | `has_schema=true` and expected `current_version`. |
| `vectis-cli health check --strict` | No critical failures; warnings are understood. |
| Safe job trigger | Run reaches terminal status and logs stream. |
| Reconciler/queue metrics | No repeated queue handoff failures or growing stuck-run counts. |
| Cron status | No unexpected `due_count` or long-lived `claimed_count`. |
| Catalog status | No persistent failed catalog events in multi-cell deployments. |

## Related Documentation

| Need | Document |
| --- | --- |
| Compatibility policy | [Compatibility](../../concepts/compatibility.md) |
| Release note template | [Releases And Upgrades](../../developing/releases.md) |
| Migration rules | [Database Migrations](../../developing/migrations.md) |
| Schema repair | [Repair Runbooks](../reliability/repair-runbooks.md#schema-or-migration-repair) |
| Backup planning | [Backup And Restore](../reliability/backup-restore.md) |
| Deployment order | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
