# Health Check Catalog

`vectis-cli health check` runs a stable catalog of operational checks against the configured API and locally visible deployment paths.

Use the default text output for humans during triage. It prints an overall result, groups checks by subsystem, and shows each check as `OK`, `WARN`, or `FAIL`.

Use `--format json` for automation. JSON includes summary counts and the full list of checks. Each check includes the stable check ID, title, status, severity, summary, evidence when available, suggested action when available, and documentation link when available. The legacy `--json` flag emits the same shape for compatibility.

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

Overall: WARN  18 passed, 2 warnings, 0 failed

Core
  OK    API liveness                   API liveness probe passed
  OK    API readiness                  API readiness probe passed
```

JSON output is a report object with summary counts and a `checks` array:

```json
{
  "status": "pass",
  "passed": 23,
  "warnings": 0,
  "failed": 0,
  "checks": [
    {
      "id": "api.live",
      "title": "API liveness",
      "status": "pass",
      "severity": "critical",
      "summary": "API liveness probe passed",
      "doc": "website/docs/operating/reliability/runbooks.md"
    }
  ]
}
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
| `queue.backlog.ratio` | warning | `GET /api/v1/queue/backlog` | Queued run count is at or below the built-in threshold of 100. | [Queued Runs Or Backlog](../reliability/repair-runbooks.md#queued-runs-or-backlog). |
| `cron.schedules` | warning | `GET /api/v1/cron/status` | No enabled cron schedules are due for dispatch or held by active claims. | Check `vectis-cron` process health, database access, and queue or cell-ingress handoff. |
| `reconciler.stuck.runs` | warning | `GET /api/v1/reconciler/stuck-runs` | No queued runs are older than the reconciler dispatch gap, no pending task continuations are waiting for redispatch, and no orphaned task finalization repairs are pending. | [Reconciler Repair](../reliability/repair-runbooks.md#reconciler-repair). |
| `cells.ingress` | warning | `GET /api/v1/cells/status` | Required cell ingress routes answer readiness checks. | Check cell ingress processes, route map, and network path. |
| `catalog.inbox` | warning | `GET /api/v1/catalog/status` | No catalog events are failed, and pending cell catalog events are at or below the built-in threshold of 100. Cell catalog events include run status, execution status, artifact manifests, and redacted worker-controlled SVID/secret-resolution security events. | Check `vectis-catalog` process health, logs, and database write latency. |
| `source.mode` | warning | `GET /api/v1/source/status` | Source-backed reusable jobs have source repository persistence and at least one enabled source repository. | Declare or enable a source repository. |
| `source.repositories.sync` | warning | `GET /api/v1/source/status`, then namespace/repository inventory only when failed or running repositories need IDs or timeout checks | Enabled source repositories have no failed syncs, no stale running sync reservations, and no unknown sync status. | Run `vectis-cli sources status <repository-id>` or retry `vectis-cli sources sync <repository-id>`. |
| `source.repositories.declared` | warning | `GET /api/v1/source/status`, then namespace/repository inventory only when stale enabled repositories need IDs | No enabled source repository is missing from current source repository configuration. | Disable stale source repositories or restore their source repository declarations. |
| `source.schedules.declared` | warning | `GET /api/v1/source/status`, then `GET /api/v1/source-repositories/{id}/schedules` only when stale enabled schedules need IDs | No enabled source-backed cron schedule is missing from current source schedule configuration. | Disable stale source schedules or restore their source schedule declarations. |
| `source.schedules.overrides` | warning | `GET /api/v1/source/status`, then `GET /api/v1/source-repositories/{id}/schedules` only when active overrides need IDs | No source-backed cron schedule has an active hotfix override. | Clear source schedule overrides after hotfixes land back in source. |
| `log.reachable` | warning | `GET /api/v1/log/reachable` | API's log gRPC connection is `READY` or `IDLE`. | [Log Service Repair](../reliability/repair-runbooks.md#log-service-repair). |
| `audit.flush.failures` | warning | `GET /api/v1/audit/flush-failures` | Audit flush failure counter is zero. | [Audit Durability Repair](../reliability/repair-runbooks.md#audit-durability-repair). |
| `secrets.encryptedfs.files` | warning | Local `VECTIS_SECRETS_ENCRYPTEDFS_ROOT` and `VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE` paths | Encryptedfs is disabled, or both paths are configured, the root exists and is writable, the key file is regular and private, and the key parses as a valid encryptedfs key. | Check encryptedfs secret root/key mounts, ownership, permissions, and backup placement. |
| `tls.files` | warning | Local `VECTIS_GRPC_TLS_*` and `VECTIS_METRICS_TLS_*` paths | TLS is disabled, or configured cert/key/CA files are readable, parseable, not expired or within 14 days of expiry, and certificate/key pairs match. | Check TLS env vars, mounted files, certificate expiry, and key pairing. |
| `queue.persistence.filesystem` | warning | Local `VECTIS_QUEUE_PERSISTENCE_DIR` or default per-shard data path | Queue persistence directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move queue persistence to a larger writable volume. |
| `log.storage.filesystem` | warning | Local `VECTIS_LOG_STORAGE_DIR` or default data path | Durable log storage directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move log storage to a larger writable volume. |
| `log.forwarder.spool.filesystem` | warning | Local `VECTIS_LOG_FORWARDER_SPOOL_DIR` or default data path | Log-forwarder spool directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move the spool to a larger writable volume. |
| `artifact.storage.filesystem` | warning | Local `VECTIS_ARTIFACT_STORAGE_DIR` or default data path | Durable artifact storage directory, or nearest existing parent, is inspectable and has at least 1 GiB free. | Free disk space or move artifact storage to a larger writable volume. |

When `queue.backlog.ratio` warns in a multi-cell deployment, `evidence` includes a per-cell breakdown from the global run catalog, for example `queued=101 cells=iad-a:75,pdx-b:26`.

When `cron.schedules` warns, `evidence` includes enabled schedule count, due count, active claim count, and the oldest due timestamp when available, for example `schedules=3 due=2 claimed=1 oldest_due=2026-06-13T12:30:00Z`.

When `reconciler.stuck.runs` warns in a multi-cell deployment, `evidence` includes a per-cell breakdown from the global run catalog, for example `stuck=3 cells=iad-a:2,pdx-b:1`. Pending child task continuations use `task_continuation_pending` and `task_continuation_cells`, for example `stuck=1 task_continuation_pending=2 task_continuation_cells=pdx-b:2`. Orphaned task runs whose stored task summary can already reduce to a terminal state use `task_finalization_pending` and `task_finalization_cells`, for example `stuck=0 task_finalization_pending=1 task_finalization_cells=pdx-b:1`.

When `cells.ingress` warns, `evidence` includes each observed cell and ingress state, for example `iad-a:ready,pdx-b:missing_route`. The endpoint reports cell IDs, a per-cell `ready` summary, ingress/dispatch/catalog checks, queued run counts, stuck queued-run counts, pending task continuation/finalization repair counts, and catalog inbox counts; it does not return private ingress URLs.

When `catalog.inbox` warns in a multi-cell deployment, `evidence` includes source-cell inbox pressure for cells with pending or failed events, for example `sources=iad-a:p=2/f=1,pdx-b:p=101/f=0`. Failed catalog security events can delay the global view of a worker-controlled SVID or secret-resolution denial, so use `vectis-cli runs show <run-id>` and `vectis-cli runs tasks <run-id>` against the owning cell when the global catalog looks stale.

When `source.repositories.sync` warns, `evidence` includes repository counts and affected repository IDs after the detailed repository fetch, for example `repositories=3 enabled=3 failed=1 running=1 failed_repositories=vectis stale_running_repositories=infra`. Credential-resolution failures are reported as repository IDs in `credential_failed_repositories`; raw secret refs and provider errors are not included in health evidence.

When `source.mode` warns, `evidence` includes source-mode booleans and aggregate counts from source status, for example `repositories_configured=true declared_repositories=1 repositories=1 enabled_repositories=0`.

When `source.repositories.declared` warns, `evidence` includes stale enabled and disabled repository IDs after the detailed repository fetch, for example `stale_enabled_ids=vectis stale_disabled_ids=old-mirror`.

When `source.schedules.declared` warns, `evidence` includes stale enabled and disabled schedule IDs after the detailed schedule fetch, for example `stale_enabled_ids=nightly-build stale_disabled_ids=old-hourly`.

When `source.schedules.overrides` warns, `evidence` includes the schedule IDs with active overrides after the detailed schedule fetch, for example `override_ids=nightly-build,release-smoke`.

When `secrets.encryptedfs.files` warns, `evidence` includes only local paths,
for example `root=/var/lib/vectis/secrets/encryptedfs key_file=/etc/vectis/secrets/encryptedfs.key`.
It does not list secret refs, envelope filenames, plaintext, or key material.

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
