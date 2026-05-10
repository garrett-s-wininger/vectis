# Doctor Check Catalog

`vectis-cli doctor` runs a stable catalog of operational checks against the configured API. Text output is tab-separated:

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
| `api.ready` | critical | `GET /health/ready` | API readiness endpoint returns `200`. | Check database and queue connectivity. |
| `setup.status` | warning | `GET /api/v1/setup/status` | Setup status is readable; incomplete setup warns. | Complete setup or confirm the deployment is intentionally pre-setup. |
| `cli.token` | warning | Local CLI config / `VECTIS_API_TOKEN` | A CLI API token is configured. | Run `vectis-cli login` or set `VECTIS_API_TOKEN` when auth is enabled. |
| `db.schema.current` | critical | `GET /api/v1/schema/status` | Schema exists and reports a migration version. | Run `vectis-cli migrate` with the deployment database settings. |
| `reconciler.active` | warning | `GET /api/v1/reconciler/heartbeat` | A reconciler dispatch event has been observed. | Start or restart `vectis-reconciler`; check DB and queue access. |
| `audit.drops.recent` | warning | `GET /api/v1/audit/drops` | Audit dropped-event counter is zero. | Check audit buffer pressure and database write capacity. |
| `db.connection.pool` | warning | `GET /api/v1/db/pool-stats` | Pool is not fully in use with recorded waits. | Check slow queries, pool sizing, and database reachability. |
| `queue.backlog.ratio` | warning | `GET /api/v1/queue/backlog` | Queued run count is within the built-in threshold. | Check queue health, workers, and dispatch failures. |
| `reconciler.stuck.runs` | warning | `GET /api/v1/reconciler/stuck-runs` | No queued runs are older than the reconciler dispatch gap. | Check reconciler logs and queue handoff. |
| `log.reachable` | warning | `GET /api/v1/log/reachable` | API's log gRPC connection is `READY` or `IDLE`. | Check log service, registry/pinned address config, and gRPC TLS. |
| `audit.flush.failures` | warning | `GET /api/v1/audit/flush-failures` | Audit flush failure counter is zero. | Check audit persistence and database write capacity. |

## Compatibility Notes

Check IDs are stable within a release line. New checks may be added in later releases, so JSON consumers should tolerate unknown IDs and fields. Treat `status` as the primary result and `severity` as triage guidance.
