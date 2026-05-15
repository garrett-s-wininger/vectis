# Doctor Check Catalog

`vectis-cli health check` runs a stable catalog of operational checks against the configured API and locally visible deployment paths. Text output is tab-separated:

```text
status<TAB>check_id<TAB>summary
```

Use `--json` for the full machine-readable check model, including severity, evidence, suggested action, and documentation link. Failed checks always exit non-zero. With `--strict`, warnings also exit non-zero.

## Status And Severity

| Field | Values | Meaning |
| --- | --- | --- |
| `status` | `pass`, `warn`, `fail` | Result of the check. |
| `severity` | `critical`, `warning` | Whether failure blocks basic operation or indicates an operational risk to investigate. |

## Active Checks

| Check ID | Severity | Source | Pass condition | Suggested first action |
| --- | --- | --- | --- | --- |
| `api.live` | critical | `GET /health/live` | API liveness endpoint returns `200`. | Check the API process and listener. |
| `api.ready` | critical | `GET /health/ready` | API readiness endpoint returns `200`. | Check database and queue connectivity; see [Schema Or Migration Repair](repair-runbooks.md#schema-or-migration-repair) if schema/database state is the issue. |
| `setup.status` | warning | `GET /api/v1/setup/status` | Setup status is readable; incomplete setup warns only when API auth is enabled. | Complete setup when API auth is enabled. |
| `cli.token` | warning | Local CLI config / `VECTIS_API_TOKEN` | A CLI API token is configured when API auth is enabled; auth-disabled deployments do not require one. | Run `vectis-cli login` or set `VECTIS_API_TOKEN` when auth is enabled. |
| `db.schema.current` | critical | `GET /api/v1/schema/status` | Schema exists and reports a migration version. | [Schema Or Migration Repair](repair-runbooks.md#schema-or-migration-repair). |
| `reconciler.active` | warning | `GET /api/v1/reconciler/heartbeat` | The recovery activity endpoint is readable; no activity is healthy when no runs need recovery. | [Reconciler Repair](repair-runbooks.md#reconciler-repair). |
| `audit.drops.recent` | warning | `GET /api/v1/audit/drops` | Audit dropped-event counter is zero. | [Audit Durability Repair](repair-runbooks.md#audit-durability-repair). |
| `db.connection.pool` | warning | `GET /api/v1/db/pool-stats` | Pool is not fully in use with recorded waits. | [Database Pool Pressure](repair-runbooks.md#database-pool-pressure). |
| `queue.backlog.ratio` | warning | `GET /api/v1/queue/backlog` | Queued run count is within the built-in threshold. | [Queued Runs Or Backlog](repair-runbooks.md#queued-runs-or-backlog). |
| `reconciler.stuck.runs` | warning | `GET /api/v1/reconciler/stuck-runs` | No queued runs are older than the reconciler dispatch gap. | [Reconciler Repair](repair-runbooks.md#reconciler-repair). |
| `log.reachable` | warning | `GET /api/v1/log/reachable` | API's log gRPC connection is `READY` or `IDLE`. | [Log Service Repair](repair-runbooks.md#log-service-repair). |
| `audit.flush.failures` | warning | `GET /api/v1/audit/flush-failures` | Audit flush failure counter is zero. | [Audit Durability Repair](repair-runbooks.md#audit-durability-repair). |
| `tls.files` | warning | Local `VECTIS_GRPC_TLS_*` and `VECTIS_METRICS_TLS_*` paths | TLS is disabled, or configured cert/key/CA files are readable, parseable, not near expiry, and certificate/key pairs match. | Check TLS env vars, mounted files, certificate expiry, and key pairing. |
| `queue.persistence.filesystem` | warning | Local `VECTIS_QUEUE_PERSISTENCE_DIR` or default data path | Queue persistence directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move queue persistence to a larger writable volume. |
| `log.storage.filesystem` | warning | Local `VECTIS_LOG_STORAGE_DIR` or default data path | Durable log storage directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move log storage to a larger writable volume. |
| `log.forwarder.spool.filesystem` | warning | Local `VECTIS_LOG_FORWARDER_SPOOL_DIR` or default data path | Log-forwarder spool directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move the spool to a larger writable volume. |

## Compatibility Notes

Check IDs are stable within a release line. New checks may be added in later releases, so JSON consumers should tolerate unknown IDs and fields. Treat `status` as the primary result and `severity` as triage guidance.
