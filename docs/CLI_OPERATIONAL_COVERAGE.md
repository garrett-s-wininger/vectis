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
| Health checks | `vectis-cli doctor` |

## Output Contract

These admin commands use stable, line-oriented text:

- List commands print one record per line.
- Get commands print `key=value` lines.
- Create/delete/update commands print a short success line.
- `doctor` prints `status<TAB>check_id<TAB>message`, using stable check IDs.
- `retention cleanup` prints `key=value` summary lines for cutoffs and delete counts.
- Errors are written to stderr by command runners and return a non-zero process exit.

The CLI still needs a broader `--json` contract across all commands before scripts should depend on JSON output. Until that lands, prefer the line-oriented output above for simple automation.

## Remaining Operator Gap

Queue DLQ list/requeue still needs a supported API proxy endpoint or explicitly configured queue-admin mode before it can be exposed safely in `vectis-cli`.

## Doctor Checks

`vectis-cli doctor` is a first-pass smoke test for the configured API. The initial stable check IDs are:

| Check ID | Meaning |
| --- | --- |
| `api.live` | `GET /health/live` returns `200`. |
| `api.ready` | `GET /health/ready` returns `200`; this covers the API's readiness dependencies. |
| `setup.status` | `GET /api/v1/setup/status` returns valid setup state. Incomplete setup is a warning. |
| `cli.token` | A CLI API token is configured through `VECTIS_API_TOKEN` or the persisted login token. Missing token is a warning because auth may be disabled or setup may still be pending. |

Failed checks make the command exit non-zero. Warnings are informational.
