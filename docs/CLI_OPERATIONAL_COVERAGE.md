# CLI Operational Coverage

`vectis-cli` now covers the shipped auth/admin API surfaces operators need for routine management:

| Area | Commands |
| --- | --- |
| Namespaces | `vectis-cli namespace list`, `get`, `create`, `delete` |
| Users | `vectis-cli user list`, `get`, `create`, `update`, `delete`, `change-password` |
| Role bindings | `vectis-cli role-binding list`, `create`, `delete` |
| Tokens | `vectis-cli token list`, `create`, `delete` |
| Runs | `vectis-cli run get`, `list`, `cancel`; top-level `force-fail`, `force-requeue` |
| Retention | `vectis-cli retention cleanup --dry-run`, `--yes` |
| Health checks | `vectis-cli doctor [--json] [--strict]` |

## Output Contract

These admin commands use stable, line-oriented text:

- List commands print one record per line.
- Get commands print `key=value` lines.
- Create/delete/update commands print a short success line.
- `doctor` prints `status<TAB>check_id<TAB>summary`, using stable check IDs (see [doctor check catalog](DOCTOR_CHECK_CATALOG.md)).
- `doctor --json` emits the full check model as a JSON array.
- `doctor --strict` exits non-zero on warnings (for CI).
- `retention cleanup` prints `key=value` summary lines for cutoffs and delete counts.
- Errors are written to stderr by command runners and return a non-zero process exit.

## Doctor Checks

`vectis-cli doctor` runs a versioned catalog of operational checks defined in [DOCTOR_CHECK_CATALOG.md](DOCTOR_CHECK_CATALOG.md). Check IDs are frozen between releases.

The 16 active checks are:

| Check ID | Severity | Source |
| --- | --- | --- |
| `api.live` | critical | `GET /health/live` returns `200`. |
| `api.ready` | critical | `GET /health/ready` returns `200`; covers DB, queue, and other readiness deps. |
| `setup.status` | warning | `GET /api/v1/setup/status` — incomplete setup is a warning. |
| `cli.token` | warning | CLI token configured or missing (auth may be disabled). |
| `db.schema.current` | critical | `GET /api/v1/schema/status` — schema must be present and current. |
| `reconciler.active` | warning | `GET /api/v1/reconciler/heartbeat` — checks recent reconciler activity. |
| `audit.drops.recent` | warning | `GET /api/v1/audit/drops` — no dropped audit events. |
| `db.connection.pool` | warning | `GET /api/v1/db/pool-stats` — checks connection pool health. |
| `queue.backlog.ratio` | warning | `GET /api/v1/queue/backlog` — warns if queue depth exceeds threshold. |
| `reconciler.stuck.runs` | warning | `GET /api/v1/reconciler/stuck-runs` — detects runs stuck in queued state. |
| `log.reachable` | warning | `GET /api/v1/log/reachable` — verifies log service gRPC connectivity. |
| `audit.flush.failures` | warning | `GET /api/v1/audit/flush-failures` — no audit flush failures. |
| `tls.files` | warning | Local TLS env/path validation for gRPC and metrics TLS. |
| `queue.persistence.filesystem` | warning | Local queue persistence path free-space and writability check. |
| `log.storage.filesystem` | warning | Local durable log storage path free-space and writability check. |
| `log.forwarder.spool.filesystem` | warning | Local log-forwarder spool path free-space and writability check. |

Failed checks always exit non-zero. Under `--strict`, warnings also cause a non-zero exit.
