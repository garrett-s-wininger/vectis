# CLI Operational Coverage

`vectis-cli` now covers the shipped auth/admin API surfaces operators need for routine management:

| Area | Commands |
| --- | --- |
| Namespaces | `vectis-cli namespace list`, `get`, `create`, `delete` |
| Users | `vectis-cli user list`, `get`, `create`, `update`, `delete`, `change-password` |
| Role bindings | `vectis-cli role-binding list`, `create`, `delete` |
| Tokens | `vectis-cli token list`, `create`, `delete` |
| Runs | `vectis-cli run get`, `list`, `cancel`; top-level `force-fail`, `force-requeue` |

## Output Contract

These admin commands use stable, line-oriented text:

- List commands print one record per line.
- Get commands print `key=value` lines.
- Create/delete/update commands print a short success line.
- Errors are written to stderr by command runners and return a non-zero process exit.

The CLI still needs a broader `--json` contract across all commands before scripts should depend on JSON output. Until that lands, prefer the line-oriented output above for simple automation.

## Remaining Operator Gap

Queue DLQ list/requeue still needs a supported API proxy endpoint or explicitly configured queue-admin mode before it can be exposed safely in `vectis-cli`.
