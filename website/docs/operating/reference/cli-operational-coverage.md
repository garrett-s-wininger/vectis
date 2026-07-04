# CLI Operational Coverage

Use this page as a compact map of what operators can do from `vectis-cli` today.

For task walkthroughs, use the [CLI Guide](../../using/cli-guide.md). For repair procedures, use [Repair Runbooks](../reliability/repair-runbooks.md).

## Command Areas

| Area | Operator use | Commands |
| --- | --- | --- |
| Actions | Inspect configured action descriptors and resolve friendly names to digests for pinning. | `vectis-cli actions list`, `actions resolve`, `--ignore-policy` |
| Jobs | Manage source-backed reusable jobs, and run one-off job files. | `vectis-cli jobs list --repository`, `show --repository`, `create --repository`, `edit --repository`, `delete --repository`, `trigger --repository`, `trigger --cell`, `run`, `run --cell` |
| Runs | Inspect, cancel, retry, download artifacts, identify failed worker-controlled SVID/secret gates, filter by repository or cell, or manually repair run state. | `vectis-cli runs list`, `runs list --repository`, `runs list --cell`, `show`, `tasks`, `artifacts list`, `artifacts download`, `cancel`, `retry`, `fail`, `repair mark-succeeded`, `mark-failed`, `mark-cancelled`, `mark-abandoned`, `mark-queued` |
| Cells | Inspect execution cell readiness, routing, queued pressure, orchestrator-driven task progress, and catalog fan-in counts. | `vectis-cli cells status` |
| Logs | Stream logs for one run or follow future runs for a job, including source-backed jobs. | `vectis-cli logs run`, `logs job`, `logs job --repository` |
| Auth sessions | Log in and out for API-backed CLI use. | `vectis-cli auth login`, `logout` |
| API tokens | Manage personal/API tokens through the auth API. | `vectis-cli auth tokens list`, `create`, `delete` |
| Namespaces | Manage namespace hierarchy. | `vectis-cli namespaces list`, `show`, `create`, `delete` |
| Users | Manage user accounts and external identity links. | `vectis-cli users list`, `show`, `create`, `enable`, `disable`, `delete`, `change-password`, `external-identities` |
| Role bindings | Grant or revoke namespace roles. | `vectis-cli role-bindings list`, `grant`, `revoke` |
| Source control | Register, sync, inspect, author, and clean up source repositories and schedules. | `vectis-cli sources overview`, `list`, `register`, `sync`, `status`, `schedules`, `override`, `clear-override`, `delete-schedule`, `jobs`, `show`, `write`, `trigger`, `runs`, `logs` |
| Health checks | Run operator checks against API and local deployment paths. | `vectis-cli health check`, `--format json`, `--strict` |
| Audit events | Review or export API audit events by type, actor, target, correlation ID, or time range. | `vectis-cli audit list`, `audit list --cursor`, `audit export --output`, `--event-type`, `--actor-id`, `--target-id`, `--correlation-id`, `--since`, `--until`, `--format json` |
| Backup evidence | Capture local backup scope evidence, aggregate host inventories, generate reference expectations, verify manifest completeness, and emit retained restore validation evidence with attached storage reports and a post-restore smoke run. | `vectis-cli backup inventory --format json`, `backup manifest --format json`, `backup expect podman --format json`, `backup expect linux --format json`, `backup verify`, `backup verify --expect`, `backup verify --storage-report`, `backup restore-validation --smoke-run`, `--storage-max-age` |
| Storage integrity | Verify local file-backed durable state before accepting a backup set or restored files. | `vectis-cli storage verify queue`, `logs`, `artifact`, `log-forwarder-spool`, `worker-log-spool`, `--dir`, `--format json` |
| Database migrations | Apply embedded SQL migrations during deploy, upgrade, or restore. | `vectis-cli database migrate` |
| Retention | Preview or apply cleanup for old durable records, optionally gated by backup manifest freshness, storage integrity reports, expected topology, audit export evidence, active hold review evidence, cleanup evidence manifests, and required policy gates with retained waivers; generate, verify, and promote retained cleanup evidence manifests; export retained evidence as Prometheus textfile metrics; run the scheduled evidence-and-cleanup workflow; create, list, release, and review run-scoped or audit-range compliance holds. | `vectis-cli retention cleanup --dry-run`, `--yes`, `--evidence-manifest`, `--backup-manifest`, `--backup-expect`, `--backup-max-age`, `--backup-storage-report`, `--backup-storage-max-age`, `--audit-export`, `--audit-export-max-age`, `--hold-review`, `--hold-review-max-age`, `--require-backup-manifest`, `--require-audit-export`, `--require-hold-review`, `--waiver`, `retention evidence manifest --output`, `--promote`, `--verify`, `retention evidence metrics --scheduled-cleanup`, `--restore-validation`, `--storage-report`, `--output`, `retention scheduled-cleanup`, `--audit-export-output`, `--hold-review-output`, `--evidence-manifest-promote`, `retention holds create --run`, `retention holds create --audit-since --audit-until`, `holds list`, `holds release`, `holds review` |
| Reference deploy | Render, start, stop, and inspect the Podman reference deployment, including `--profile simple` and `--profile ha`; render the Kubernetes reference deployment. | `vectis-cli deploy podman init`, `render`, `up`, `status`, `down`; `vectis-cli deploy kubernetes render` |
| Local reset | Preview or reset local Vectis development state. | `vectis-cli local reset --dry-run`, `--yes` |

## Routine Operator Commands

| Need | Start with |
| --- | --- |
| Check whether the system is healthy | `vectis-cli health check --strict` |
| Get machine-readable health evidence | `vectis-cli health check --format json` |
| Discover an action digest to pin | `vectis-cli actions resolve <uses>` |
| Inspect multi-cell readiness, routing, task progress, and fan-in state | `vectis-cli cells status` |
| Inspect a stuck or failed run | `vectis-cli runs show <run-id>` |
| Inspect task, attempt, and redacted security-gate state for a run | `vectis-cli runs tasks <run-id>` |
| Download a run artifact | `vectis-cli runs artifacts download <run-id> <artifact-name> --output <path>` |
| Cancel a running run | `vectis-cli runs cancel <run-id>` |
| Retry a failed or repaired run | `vectis-cli runs retry <run-id>` |
| Stream logs for a run | `vectis-cli logs run <run-id>` |
| Inspect source-control readiness | `vectis-cli sources overview` |
| Inspect source repository health | `vectis-cli sources status <repository-id>` |
| Check config-as-code readiness | `vectis-cli health check --strict` |
| Export audit events for a range | `vectis-cli audit export --until <RFC3339> --output audit-export.json` |
| Capture backup scope evidence | `vectis-cli backup inventory --format json` |
| Generate Podman expected topology | `vectis-cli backup expect podman --profile simple --format json` or `--profile ha` |
| Generate Linux expected topology | `vectis-cli backup expect linux --manifest deploy/linux/services.toml --format json` |
| Build and verify backup manifest evidence | `vectis-cli backup manifest --format json <inventory.json...>`, then `vectis-cli backup verify [--expect expected-topology.json] [--storage-report report.json] <manifest.json>` |
| Build retained restore validation | `vectis-cli backup restore-validation --format json --smoke-run <run-id> [--expect expected-topology.json] [--storage-report report.json] <manifest.json>` |
| Verify restored file stores | `vectis-cli storage verify queue --dir <queue-dir>`, then repeat for `logs`, `artifact`, `log-forwarder-spool`, and `worker-log-spool` paths in scope |
| Preserve a run for compliance or incident review | `vectis-cli retention holds create --run <run-id> --owner <owner> --reason <reason> [--external-ref <ticket>]` |
| Preserve audit rows for a compliance review window | `vectis-cli retention holds create --audit-since <RFC3339> --audit-until <RFC3339> --owner <owner> --reason <reason> [--external-ref <ticket>]` |
| List active retention holds | `vectis-cli retention holds list` |
| Run scheduled retention evidence and cleanup workflow | `vectis-cli retention scheduled-cleanup --yes --backup-manifest backup-manifest.json --backup-expect expected-topology.json --audit-export-output audit-export.json --hold-review-output hold-review.json --reason "weekly retention cleanup review" --require-backup-manifest --require-audit-export --require-hold-review --evidence-manifest-promote retention-cleanup-evidence.json` |
| Publish retained evidence metrics | `vectis-cli retention evidence metrics --scheduled-cleanup retention-scheduled-cleanup.json --backup-manifest backup-manifest.json --restore-validation restore-validation.json --storage-report queue.storage-report.json --audit-export audit-export.json --hold-review hold-review.json --output retention-evidence.prom` |
| Verify, generate, and promote cleanup evidence manifest | `vectis-cli retention evidence manifest --verify --backup-manifest backup-manifest.json --backup-expect expected-topology.json --backup-storage-report queue.storage-report.json --audit-export audit-export.json --audit-age 8760h --hold-review hold-review.json --require-backup-manifest --require-audit-export --require-hold-review --output retention-cleanup-evidence-20260702.json --promote retention-cleanup-evidence.json` |
| Apply retention cleanup after backup, audit, and hold-review validation | `vectis-cli retention cleanup --yes --require-backup-manifest --backup-manifest backup-manifest.json --backup-expect expected-topology.json --backup-max-age 24h --backup-storage-report queue.storage-report.json --require-audit-export --audit-export audit-export.json --audit-export-max-age 24h --require-hold-review --hold-review hold-review.json --hold-review-max-age 24h` |
| Apply scheduled retention cleanup from an evidence manifest | `vectis-cli retention cleanup --yes --evidence-manifest /var/lib/vectis/ops/retention-cleanup-evidence.json` |
| List stale source repositories | `vectis-cli sources list --stale` |
| List stale source schedules | `vectis-cli sources schedules --stale` |
| Trigger a source-defined job | `vectis-cli jobs trigger <job-id> --repository <repository-id>` |
| Commit a source-defined job change | `vectis-cli jobs create <file> --repository <repository-id>` or `vectis-cli jobs edit <job-id> --repository <repository-id>` |
| Preview retention cleanup | `vectis-cli retention cleanup --dry-run` |
| Apply database migrations | `vectis-cli database migrate` |

## Output Contract

Most operational commands use stable, line-oriented text:

- List commands print one record per line.
- Get commands print `key=value` lines.
- `runs show` prints `next_action=security_gate_failed`, a redacted `latest_failed_security_event`, and retry guidance when a failed run is explained by the newest worker-controlled SVID or secret-resolution gate.
- Create/delete/update commands print a short success line.
- `health check` prints a grouped human report using stable check IDs from the [Health Check Catalog](./health-check-catalog.md).
- `health check --format json` emits a summary object with the full check model in `checks`.
- `health check --strict` exits non-zero on warnings (for CI).
- `audit list` prints one event per line in newest-first order and emits a continuation cursor when more results exist; `--format json` emits an `events` array, effective page `limit`, and optional `next_cursor`. `audit export` follows all pages and writes a versioned JSON evidence envelope with filters, page count, row count, truncation signal, event time bounds, exported rows, and `events_sha256`.
- `backup inventory --format json` emits local backup scope evidence with redacted database DSNs and path readability.
- `backup expect podman --format json` emits expected topology JSON for the Podman reference deployment's simple or HA profile.
- `backup expect linux --format json` emits expected topology JSON from the Linux services manifest's example environment.
- `backup manifest --format json` aggregates inventory files into backup-set evidence; `backup verify` exits non-zero when core database, queue, log, or artifact evidence is incomplete, and `--expect` also fails when expected host inventory sources, service instances, database roles, paths, or path categories are absent. When `--storage-report` is supplied, every storage-backed `local_state` path in the manifest must have a matching `ok` report; `--storage-max-age` rejects stale reports. `backup restore-validation --format json` emits retained validation evidence and exits non-zero unless backup verification passes and the referenced smoke run has status `succeeded`.
- `storage verify` prints `key=value` report summaries or JSON reports for queue snapshot/WAL files, durable run logs, artifact blobs, log-forwarder CRC spools, and worker pending log spools. It exits non-zero on corrupt files, digest mismatches, malformed records, or quarantined spools.
- `retention cleanup` prints `key=value` summary lines for cutoffs, delete counts, and `held.*` counts. When `--evidence-manifest` is provided, cleanup loads a retained `vectis.retention_cleanup_evidence.v1` file, resolves relative evidence paths next to that file, and reports `evidence_manifest_*` fields or a JSON `evidence_manifest` object. `retention evidence manifest` writes the same versioned manifest contract, prints the full JSON to stdout when no file output is requested, and prints a receipt when `--output` or `--promote` writes retained files. With `--verify`, it checks backup, audit export, hold review, waiver, and required-gate evidence before writing or promoting; JSON receipts include a `verification` object. `retention evidence metrics` reads retained backup, restore-validation, storage, audit, hold-review, waiver, and scheduled-cleanup JSON evidence and emits Prometheus textfile gauges without raw path labels. `retention scheduled-cleanup` exports retained audit evidence, writes active hold review evidence, verifies and promotes the cleanup evidence manifest, then runs cleanup through that promoted manifest; JSON output wraps the generated evidence paths, verification object, and cleanup result in one workflow envelope. When `--backup-manifest` is provided, text output includes `backup_manifest_*` and optional `backup_storage_*` validation lines, and JSON output includes a `backup` object. When `--audit-export` is provided, cleanup verifies a fully exhausted audit export before deleting audit rows and reports `audit_export_*` fields or a JSON `audit_export` object. When `--hold-review` is provided, cleanup verifies retained active-hold review evidence against current active holds and reports `hold_review_*` fields or a JSON `hold_review` object. Verification or freshness failure exits non-zero before cleanup. `--require-backup-manifest`, `--require-audit-export`, and `--require-hold-review` make those gates mandatory unless a verified `--waiver` file covers the missing evidence; supplied waivers are reported as `retention_waiver_*` fields or a JSON `waiver` object.
- `retention holds create` and `retention holds release` print the hold record as `key=value` lines; `retention holds list` prints one hold per line, or a JSON `holds` array with `--format json`.
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
vectis-cli health check --format json
```

Failed checks always exit non-zero. Under `--strict`, warnings also cause a non-zero exit.

For release gates and production drills, keep both outputs: `--strict` proves
the pass/fail decision for the current gate, while `--json` preserves the
machine-readable evidence needed for later audit or comparison.

## Related Docs

| Need | Doc |
| --- | --- |
| Task-based CLI walkthrough | [CLI Guide](../../using/cli-guide.md) |
| Health check IDs and evidence | [Health Check Catalog](./health-check-catalog.md) |
| First-response triage | [Runbooks And Alerts](../reliability/runbooks.md) |
| Repair procedures | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Retention behavior | [Retention And Storage Pressure](../reliability/retention.md) |
