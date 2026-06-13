# CLI Operational Coverage

Use this page as a compact map of what operators can do from `vectis-cli` today.

For task walkthroughs, use the [CLI Guide](../../using/cli-guide.md). For repair procedures, use [Repair Runbooks](../reliability/repair-runbooks.md).

## Command Areas

| Area | Operator use | Commands |
| --- | --- | --- |
| Actions | Inspect configured action descriptors and resolve friendly names to digests for pinning. | `vectis-cli actions list`, `actions resolve`, `--ignore-policy` |
| Jobs | Manage stored jobs and run one-off job files. | `vectis-cli jobs list`, `show`, `create`, `edit`, `delete`, `trigger`, `trigger --cell`, `run`, `run --cell` |
| Runs | Inspect, cancel, retry, download artifacts, identify failed worker-controlled SVID/secret gates, or manually repair run state. | `vectis-cli runs list`, `runs list --cell`, `show`, `tasks`, `artifacts list`, `artifacts download`, `cancel`, `retry`, `fail`, `repair mark-succeeded`, `mark-failed`, `mark-cancelled`, `mark-abandoned`, `mark-queued` |
| Cells | Inspect execution cell readiness, routing, queued pressure, orchestrator-driven task progress, and catalog fan-in counts. | `vectis-cli cells status` |
| Logs | Stream logs for one run or follow future runs for a job. | `vectis-cli logs run`, `logs job` |
| Auth sessions | Log in and out for API-backed CLI use. | `vectis-cli auth login`, `logout` |
| API tokens | Manage personal/API tokens through the auth API. | `vectis-cli auth tokens list`, `create`, `delete` |
| Namespaces | Manage namespace hierarchy. | `vectis-cli namespaces list`, `show`, `create`, `delete` |
| Users | Manage user accounts. | `vectis-cli users list`, `show`, `create`, `enable`, `disable`, `delete`, `change-password` |
| Role bindings | Grant or revoke namespace roles. | `vectis-cli role-bindings list`, `grant`, `revoke` |
| Health checks | Run operator checks against API and local deployment paths. | `vectis-cli health check`, `--json`, `--strict` |
| Database migrations | Apply embedded SQL migrations during deploy, upgrade, or restore. | `vectis-cli database migrate` |
| Retention | Preview or apply cleanup for old durable records. | `vectis-cli retention cleanup --dry-run`, `--yes` |
| Reference deploy | Render, start, stop, and inspect the Podman reference deployment, including `--profile simple` and `--profile ha`. | `vectis-cli deploy podman init`, `render`, `up`, `status`, `down` |
| Local reset | Preview or reset local Vectis development state. | `vectis-cli local reset --dry-run`, `--yes` |

## Routine Operator Commands

| Need | Start with |
| --- | --- |
| Check whether the system is healthy | `vectis-cli health check --strict` |
| Get machine-readable health evidence | `vectis-cli health check --json` |
| Discover an action digest to pin | `vectis-cli actions resolve <uses>` |
| Inspect multi-cell readiness, routing, task progress, and fan-in state | `vectis-cli cells status` |
| Inspect a stuck or failed run | `vectis-cli runs show <run-id>` |
| Inspect task, attempt, and redacted security-gate state for a run | `vectis-cli runs tasks <run-id>` |
| Download a run artifact | `vectis-cli runs artifacts download <run-id> <artifact-name> --output <path>` |
| Cancel a running run | `vectis-cli runs cancel <run-id>` |
| Retry a failed or repaired run | `vectis-cli runs retry <run-id>` |
| Stream logs for a run | `vectis-cli logs run <run-id>` |
| Preview retention cleanup | `vectis-cli retention cleanup --dry-run` |
| Apply database migrations | `vectis-cli database migrate` |

## Output Contract

Most operational commands use stable, line-oriented text:

- List commands print one record per line.
- Get commands print `key=value` lines.
- `runs show` prints `next_action=security_gate_failed`, a redacted `latest_failed_security_event`, and retry guidance when a failed run is explained by the newest worker-controlled SVID or secret-resolution gate.
- Create/delete/update commands print a short success line.
- `health check` prints a grouped human report using stable check IDs from the [Health Check Catalog](./health-check-catalog.md).
- `health check --json` emits the full check model as a JSON array.
- `health check --strict` exits non-zero on warnings (for CI).
- `retention cleanup` prints `key=value` summary lines for cutoffs and delete counts.
- Errors are written to stderr by command runners and return a non-zero process exit.

Commands that stream logs or SSE-backed run activity are intentionally interactive; use them for live inspection rather than stable parsers.

## Health Checks

`vectis-cli health check` runs the versioned catalog defined in [Health Check Catalog](./health-check-catalog.md). Keep that page as the source of truth for check IDs, severity, pass conditions, and suggested first actions.

Use plain text for humans:

```sh
vectis-cli health check
```

Use strict mode for CI or release gates:

```sh
vectis-cli health check --strict
```

Use JSON when automation needs evidence, severity, suggested action, and documentation links:

```sh
vectis-cli health check --json
```

Failed checks always exit non-zero. Under `--strict`, warnings also cause a non-zero exit.

## Related Docs

| Need | Doc |
| --- | --- |
| Task-based CLI walkthrough | [CLI Guide](../../using/cli-guide.md) |
| Health check IDs and evidence | [Health Check Catalog](./health-check-catalog.md) |
| First-response triage | [Runbooks And Alerts](../reliability/runbooks.md) |
| Repair procedures | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Retention behavior | [Retention And Storage Pressure](../reliability/retention.md) |
