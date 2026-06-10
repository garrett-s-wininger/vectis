# Health Check Catalog

`vectis-cli health check` runs a stable catalog of operational checks against the configured API and locally visible deployment paths.

Use the default text output for humans during triage. It prints an overall result, groups checks by subsystem, and shows each check as `OK`, `WARN`, or `FAIL`.

Use `--json` for automation. JSON includes the stable check ID, title, status, severity, summary, evidence when available, suggested action when available, and documentation link when available.

Failed checks always exit non-zero. With `--strict`, warnings also exit non-zero.

## Status And Severity

| Field | Values | Meaning |
| --- | --- | --- |
| `status` | `pass`, `warn`, `fail` | Result of the check. |
| `severity` | `critical`, `warning` | Whether failure blocks basic operation or indicates an operational risk to investigate. |

## Output Shape

Human output is grouped by subsystem:

```text
Vectis health check

Overall: WARN  15 passed, 2 warnings, 0 failed

Core
  OK    API liveness                   API liveness probe passed
  OK    API readiness                  API readiness probe passed
```

JSON output is an array of check objects:

```json
[
  {
    "id": "api.live",
    "title": "API liveness",
    "status": "pass",
    "severity": "critical",
    "summary": "API liveness probe passed",
    "doc": "website/docs/operating/reliability/runbooks.md"
  }
]
```

Treat `id`, `status`, and `severity` as the fields most suitable for automation. Treat `summary`, `evidence`, `action`, and `doc` as operator-facing context.

## Active Checks

| Check ID | Severity | Source | Pass condition | Suggested first action |
| --- | --- | --- | --- | --- |
| `api.live` | critical | `GET /health/live` | API liveness endpoint returns `200`. | Check the API process and listener. |
| `api.ready` | critical | `GET /health/ready` | API readiness endpoint returns `200`. It returns `503` during API shutdown drain, database unavailability, or queue unavailability. | Check database and queue connectivity; see [Schema Or Migration Repair](../reliability/repair-runbooks.md#schema-or-migration-repair) if schema/database state is the issue. |
| `setup.status` | warning | `GET /api/v1/setup/status` | Setup status is readable; incomplete setup warns only when API auth is enabled. | Complete setup when API auth is enabled. |
| `cli.token` | warning | Local CLI config / `VECTIS_API_TOKEN` | A CLI API token is configured when API auth is enabled; auth-disabled deployments do not require one. | Run `vectis-cli auth login` or set `VECTIS_API_TOKEN` when auth is enabled. |
| `db.schema.current` | critical | `GET /api/v1/schema/status` | Schema exists and reports a migration version. | [Schema Or Migration Repair](../reliability/repair-runbooks.md#schema-or-migration-repair). |
| `reconciler.active` | warning | `GET /api/v1/reconciler/heartbeat` | The recovery activity endpoint is readable; no activity is healthy when no runs need recovery. | [Reconciler Repair](../reliability/repair-runbooks.md#reconciler-repair). |
| `audit.drops.recent` | warning | `GET /api/v1/audit/drops` | Audit dropped-event counter is zero. | [Audit Durability Repair](../reliability/repair-runbooks.md#audit-durability-repair). |
| `db.connection.pool` | warning | `GET /api/v1/db/pool-stats` | The pool is not fully in use while waits have been recorded. | [Database Pool Pressure](../reliability/repair-runbooks.md#database-pool-pressure). |
| `queue.backlog.ratio` | warning | `GET /api/v1/queue/backlog` | Queued run count is at or below the built-in threshold of 100; task continuation pending counts are included as explanatory evidence. | [Queued Runs Or Backlog](../reliability/repair-runbooks.md#queued-runs-or-backlog). |
| `reconciler.stuck.runs` | warning | `GET /api/v1/reconciler/stuck-runs` | No root-dispatch queued runs are older than the reconciler dispatch gap, and no task continuation dispatch intents are pending for enqueue. | [Reconciler Repair](../reliability/repair-runbooks.md#reconciler-repair). |
| `cells.ingress` | warning | `GET /api/v1/cells/status` | Required cell ingress routes answer readiness checks. | Check cell ingress processes, route map, and network path. |
| `catalog.inbox` | warning | `GET /api/v1/catalog/status` | No catalog events are failed, and pending cell catalog events are at or below the built-in threshold of 100. | Check `vectis-catalog` process health, logs, and database write latency. |
| `log.reachable` | warning | `GET /api/v1/log/reachable` | API's log gRPC connection is `READY` or `IDLE`. | [Log Service Repair](../reliability/repair-runbooks.md#log-service-repair). |
| `audit.flush.failures` | warning | `GET /api/v1/audit/flush-failures` | Audit flush failure counter is zero. | [Audit Durability Repair](../reliability/repair-runbooks.md#audit-durability-repair). |
| `tls.files` | warning | Local `VECTIS_GRPC_TLS_*` and `VECTIS_METRICS_TLS_*` paths | TLS is disabled, or configured cert/key/CA files are readable, parseable, not expired or within 14 days of expiry, and certificate/key pairs match. | Check TLS env vars, mounted files, certificate expiry, and key pairing. |
| `queue.persistence.filesystem` | warning | Local `VECTIS_QUEUE_PERSISTENCE_DIR` or default per-shard data path | Queue persistence directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move queue persistence to a larger writable volume. |
| `log.storage.filesystem` | warning | Local `VECTIS_LOG_STORAGE_DIR` or default data path | Durable log storage directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move log storage to a larger writable volume. |
| `log.forwarder.spool.filesystem` | warning | Local `VECTIS_LOG_FORWARDER_SPOOL_DIR` or default data path | Log-forwarder spool directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move the spool to a larger writable volume. |
| `artifact.storage.filesystem` | warning | Local `VECTIS_ARTIFACT_STORAGE_DIR` or default data path | Durable artifact storage directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move artifact storage to a larger writable volume. |

When `queue.backlog.ratio` warns in a multi-cell deployment, `evidence` includes a per-cell breakdown from the global run catalog, for example `queued=101 cells=iad-a:75,pdx-b:26`. If queued work includes pending task continuations, evidence also includes `task_dispatch_pending` and `task_cells`, for example `queued=101 cells=iad-a:75,pdx-b:26 task_dispatch_pending=4 task_cells=iad-a:4`.

When `reconciler.stuck.runs` warns in a multi-cell deployment, `evidence` includes a per-cell breakdown from the global run catalog, for example `stuck=3 cells=iad-a:2,pdx-b:1`. The `stuck` bucket is scoped to root-dispatch redispatch candidates; task continuation dispatch has its own `task_dispatch_pending` and `task_cells` evidence, for example `stuck=0 task_dispatch_pending=2 task_cells=iad-a:2`. Orphaned task runs whose stored task summary can already reduce to a terminal state use `task_finalization_pending` and `task_finalization_cells`, for example `stuck=0 task_finalization_pending=1 task_finalization_cells=pdx-b:1`.

When `cells.ingress` warns, `evidence` includes each observed cell and readiness state, for example `iad-a:ready,pdx-b:missing_route`. The endpoint reports cell IDs, route health, queued run counts, root-dispatch stuck counts, pending task continuation counts, and catalog inbox counts; it does not return private ingress URLs.

When `catalog.inbox` warns in a multi-cell deployment, `evidence` includes source-cell inbox pressure for cells with pending or failed events, for example `sources=iad-a:p=2/f=1,pdx-b:p=101/f=0`.

## How To Respond

| Result | Meaning | Next step |
| --- | --- | --- |
| Critical `fail` | A core dependency or API-facing contract is broken. | Treat as blocking and start with [Runbooks And Alerts](../reliability/runbooks.md). |
| Warning `fail` | The check itself could not complete, usually because an API call or local inspection failed. | Check API reachability, credentials, or local file access. |
| Warning `warn` | Vectis is reachable, but the check found operational risk. | Follow the suggested action or linked repair runbook. |
| `pass` | The check passed from the CLI's current vantage point. | Continue with workload-specific verification when needed. |

The health check is a triage aid, not a full monitoring replacement. It does not replace host disk telemetry, database-native monitoring, queue/log/artifact capacity dashboards, or workload-specific alerts.

## Compatibility Notes

Check IDs are stable within a release line. New checks may be added in later releases, so JSON consumers should tolerate unknown IDs and fields.

## Related Docs

| Need | Doc |
| --- | --- |
| Compact CLI command map | [CLI Operational Coverage](./cli-operational-coverage.md) |
| First-response triage | [Runbooks And Alerts](../reliability/runbooks.md) |
| Step-by-step repairs | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Restore smoke tests | [Backup And Restore](../reliability/backup-restore.md#restore-smoke-test) |
