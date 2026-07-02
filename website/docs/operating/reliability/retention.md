# Retention And Storage Pressure

Use this page when Vectis SQL storage is growing, when you are setting a cleanup policy, or when a health check or alert points at old retained records.

Vectis keeps durable SQL state for runs, artifact manifests, dispatch visibility, unreferenced job definition snapshots, idempotency keys, and audit events. Operators should prune old data deliberately, after checking backups and previewing exactly what cleanup will delete.

## What Cleanup Does

`vectis-cli retention cleanup` deletes retention-eligible records from the configured Vectis database. It can also delete matching local durable run log files when you pass `--log-storage-dir`, and unreferenced local artifact CAS blobs when you pass `--artifact-storage-dir`. Active retention holds protect matching terminal runs, related run state, and held audit-log time ranges from cleanup.

Cleanup is intentionally explicit today:

- `--dry-run` previews cutoffs and counts without changing the database.
- `--yes` applies the same policy.
- `--evidence-manifest` loads a retained cleanup evidence manifest that names the backup, audit export, hold review, waiver, requirement, and freshness inputs for scheduled cleanup.
- `--backup-manifest` verifies backup manifest evidence before preview or apply.
- `--backup-expect` makes the manifest match an expected topology file.
- `--backup-max-age` rejects stale manifest evidence, based on `generated_at`.
- `--audit-export` verifies retained audit export evidence before deleting old audit rows.
- `--audit-export-max-age` rejects stale audit export evidence, based on `generated_at`.
- `--hold-review` verifies retained active hold review evidence before cleanup.
- `--hold-review-max-age` rejects stale hold review evidence, based on `generated_at`.
- `--require-backup-manifest` makes backup-manifest evidence mandatory unless an approved waiver is supplied.
- `--require-audit-export` makes audit-export evidence mandatory when audit rows are eligible for deletion unless an approved waiver is supplied.
- `--require-hold-review` makes active hold review evidence mandatory unless an approved waiver is supplied.
- `--waiver` verifies retained waiver JSON for policy-required gates.
- Running without `--dry-run` or `--yes` fails.

`vectis-cli retention holds` creates, lists, and releases compliance holds.
Use run holds for incident response, legal discovery, customer evidence
preservation, or other cases where a specific run must survive automated
cleanup. Use audit-range holds when a security review, audit request, legal
matter, or policy exception requires `audit_log` rows for a time range to stay
queryable while the normal policy continues elsewhere.

The command prints `key=value` output so it can be captured in runbooks, incident notes, or automation logs.

Production deployments should either schedule this command through the
operator's scheduler or assign it to a recurring operations runbook. Vectis does
not currently ship a built-in retention controller.

## Defaults

By default, cleanup uses these windows:

| Surface | Default | Cleanup behavior |
| --- | ---: | --- |
| Terminal runs | 30 days | Deletes terminal runs older than the cutoff: `succeeded`, `failed`, `aborted`, `cancelled`, and `abandoned`. |
| Run dispatch events | follows terminal runs | Deletes dispatch events for runs being deleted. |
| Artifact manifests | follows terminal runs | Deletes `run_artifacts` rows for runs being deleted. |
| Task graph rows | follows terminal runs | Deletes task nodes, task attempts, run segments, and segment executions for runs being deleted. |
| Unreferenced job definition snapshots | 30 days | Deletes `job_definitions` rows older than the cutoff when no run or source provenance still references them. |
| Idempotency keys | 24 hours | Deletes old idempotency records; retry deduplication is no longer guaranteed after the window. |
| Audit log | 365 days | Deletes old audit rows outside active audit-range holds and inserts a fresh `retention.cleanup` audit event when cleanup is applied. |
| Durable run log files | disabled by default | Pass `--log-storage-dir` to prune local run log files for the terminal runs being deleted. |
| Durable artifact blobs | 30 days when enabled | Pass `--artifact-storage-dir` to prune local CAS blobs with no remaining SQL references and a file mtime older than the cutoff. |

Durations can be overridden per run. Use `0` to disable a surface. Artifact manifest cleanup is SQL cleanup and follows `--terminal-run-age`; artifact blob cleanup is local filesystem cleanup and only runs when `--artifact-storage-dir` is provided.

Deployment defaults can be set with `VECTIS_RETENTION_CLEANUP_*` environment
variables or the matching `retention.cleanup.*` config keys. These defaults
cover cleanup windows, evidence freshness limits, and whether backup-manifest
audit-export, or hold-review evidence is mandatory. They can also name a
default cleanup evidence manifest for scheduled jobs. Command-line flags still
override the configured defaults for one cleanup invocation.

## Choose A Policy

Before applying cleanup in production, decide:

| Decision | Why it matters |
| --- | --- |
| How long terminal run history must remain queryable | Deleted runs no longer appear in normal run history. |
| How long idempotency keys must survive client retries | Retrying the same request after the idempotency window may create new work. |
| How long audit rows must be retained | Audit deletion may be subject to security or compliance requirements. |
| Whether local run log files should be pruned | SQL cleanup and log file cleanup are separate; logs may contain sensitive output. |
| Whether local artifact blobs should be pruned | Artifact metadata cleanup and CAS garbage collection are separate; shared blobs must stay while any manifest references them. |
| Whether backups have already captured the data | Cleanup should normally happen after a successful backup or accepted retention window. |
| Whether any evidence is under compliance hold | Active run holds preserve selected run rows, task graph rows, dispatch events, artifact manifests, and local run logs. Active audit-range holds preserve matching `audit_log` rows. |

Keep the production idempotency window longer than the longest realistic client retry window. Keep audit retention aligned with your security policy, not just disk capacity.

## Operator Flow

Create a compliance hold before cleanup when a run must be preserved for an
incident, legal matter, customer case, or audit review:

```sh
vectis-cli retention holds create \
  --run run-123 \
  --owner security \
  --reason "INC-1234 evidence preservation" \
  --external-ref INC-1234
```

Create an audit-range hold when audit rows for a specific review window must
stay queryable even after the normal audit retention cutoff:

```sh
vectis-cli retention holds create \
  --audit-since 2026-06-01T00:00:00Z \
  --audit-until 2026-07-01T00:00:00Z \
  --owner compliance \
  --reason "Q2 access review evidence" \
  --external-ref AUD-2026-Q2
```

List active holds before applying cleanup:

```sh
vectis-cli retention holds list
```

Generate retained hold-review evidence after the active hold list has been
reviewed. The review file records the active hold inventory, reviewer, reason,
optional external reference, and a SHA-256 hash over the canonical hold list.
Cleanup verifies that the retained file still matches the current active holds,
including the zero-active-holds case:

```sh
vectis-cli retention holds review \
  --reviewed-by compliance-oncall \
  --reason "weekly retention cleanup review" \
  --external-ref GRC-123 \
  --output hold-review.json
```

Release a hold only after the preservation requirement has ended:

```sh
vectis-cli retention holds release hold-abc123 \
  --reason "INC-1234 closed"
```

Always preview first:

```sh
vectis-cli retention cleanup --dry-run
```

Apply the same policy after review:

```sh
vectis-cli retention cleanup --yes
```

For production apply jobs, pass the backup manifest that was captured with the
backup set. Add `--require-backup-manifest` so missing backup evidence also
stops cleanup before the database or local files are opened for deletion:

```sh
vectis-cli retention cleanup --yes \
  --require-backup-manifest \
  --backup-manifest backup-manifest.json \
  --backup-expect expected-topology.json \
  --backup-max-age 24h
```

When audit rows are being pruned, capture an unfiltered audit export for the
range that covers the cleanup cutoff and pass it to cleanup. The export gate
rejects filtered, stale, hash-mismatched, or potentially truncated evidence:

```sh
vectis-cli audit export \
  --until 2026-07-01T00:00:00Z \
  --output audit-export.json

vectis-cli retention cleanup --yes \
  --require-audit-export \
  --audit-age 8760h \
  --audit-export audit-export.json \
  --audit-export-max-age 24h
```

For environments where retention holds are part of the compliance process, make
hold review a cleanup gate:

```sh
vectis-cli retention cleanup --yes \
  --require-hold-review \
  --hold-review hold-review.json \
  --hold-review-max-age 24h
```

Scheduled cleanup jobs can keep the latest accepted evidence paths in one
retained cleanup evidence manifest. Paths inside the manifest can be absolute or
relative to the manifest file, and explicit CLI flags override individual
manifest fields when an operator needs a one-off apply:

```sh
vectis-cli retention evidence manifest \
  --backup-manifest /var/lib/vectis/ops/backup-manifest.json \
  --backup-expect /etc/vectis/expected-topology.json \
  --backup-storage-report /var/lib/vectis/ops/queue.storage.json \
  --backup-storage-report /var/lib/vectis/ops/logs.storage.json \
  --backup-max-age 24h \
  --backup-storage-max-age 24h \
  --audit-export /var/lib/vectis/ops/audit-export.json \
  --audit-export-max-age 24h \
  --audit-age 8760h \
  --hold-review /var/lib/vectis/ops/hold-review.json \
  --hold-review-max-age 24h \
  --require-backup-manifest \
  --require-audit-export \
  --require-hold-review \
  --verify \
  --generated-by retention-scheduler \
  --external-ref CHG-123 \
  --output /var/lib/vectis/ops/retention-cleanup-evidence-20260702.json \
  --promote /var/lib/vectis/ops/retention-cleanup-evidence.json
```

`--verify` runs the same retained-evidence checks that cleanup uses before the
manifest is written or promoted. When audit export evidence is present or
required, `--audit-age` must match the audit cleanup window the scheduled cleanup
job will use so export coverage is checked against the same cutoff.

For recurring jobs, `retention scheduled-cleanup` runs the evidence sequence and
cleanup preview/apply as one command. It exports audit evidence, records active
hold review evidence, verifies backup/audit/hold/waiver gates, promotes the
cleanup evidence manifest, and then runs cleanup through that promoted manifest:

```sh
vectis-cli retention scheduled-cleanup --yes \
  --terminal-run-age 720h \
  --idempotency-age 48h \
  --audit-age 8760h \
  --backup-manifest /var/lib/vectis/ops/backup-manifest.json \
  --backup-expect /etc/vectis/expected-topology.json \
  --backup-max-age 24h \
  --audit-export-output /var/lib/vectis/ops/audit-export.json \
  --audit-export-max-age 24h \
  --hold-review-output /var/lib/vectis/ops/hold-review.json \
  --hold-review-max-age 24h \
  --reason "weekly retention cleanup review" \
  --require-backup-manifest \
  --require-audit-export \
  --require-hold-review \
  --evidence-manifest-promote /var/lib/vectis/ops/retention-cleanup-evidence.json
```

```json
{
  "schema_version": "vectis.retention_cleanup_evidence.v1",
  "generated_at": "2026-07-02T02:45:00Z",
  "generated_by": "retention-scheduler",
  "external_ref": "CHG-123",
  "backup_manifest": "backup-manifest.json",
  "backup_expect": "/etc/vectis/expected-topology.json",
  "backup_storage_reports": ["queue.storage.json", "logs.storage.json"],
  "backup_max_age": "24h",
  "backup_storage_max_age": "24h",
  "audit_export": "audit-export.json",
  "audit_export_max_age": "24h",
  "hold_review": "hold-review.json",
  "hold_review_max_age": "24h",
  "require_backup_manifest": true,
  "require_audit_export": true,
  "require_hold_review": true
}
```

```sh
vectis-cli retention cleanup --yes \
  --evidence-manifest /var/lib/vectis/ops/retention-cleanup-evidence.json
```

Use a waiver only for an approved exception where the gate is intentionally
being bypassed. Waivers are retained JSON files with an expiry, an approver,
and the gate names being waived:

```json
{
  "schema_version": "vectis.retention_waiver.v1",
  "waives": ["audit_export"],
  "reason": "Emergency storage pressure while the audit export job is unavailable",
  "approved_by": "security-oncall",
  "external_ref": "INC-1234",
  "expires_at": "2026-07-03T00:00:00Z"
}
```

```sh
vectis-cli retention cleanup --yes \
  --require-audit-export \
  --waiver retention-waiver.json
```

Override windows when the defaults are not right for the environment:

```sh
vectis-cli retention cleanup --dry-run \
  --terminal-run-age 720h \
  --idempotency-age 48h \
  --audit-age 8760h
```

Use `0` to skip a surface for that cleanup:

```sh
vectis-cli retention cleanup --dry-run --audit-age 0
```

For a single-node/local log service, include the log storage directory if you want matching durable run log files removed:

```sh
vectis-cli retention cleanup --dry-run --log-storage-dir "$XDG_DATA_HOME/vectis/log/<instance-id>"
vectis-cli retention cleanup --yes --log-storage-dir "$XDG_DATA_HOME/vectis/log/<instance-id>"
```

For a local artifact service, include the artifact storage directory if you want unreferenced CAS blobs removed:

```sh
vectis-cli retention cleanup --dry-run --artifact-storage-dir "$XDG_DATA_HOME/vectis/artifact/<instance-id>"
vectis-cli retention cleanup --yes --artifact-storage-dir "$XDG_DATA_HOME/vectis/artifact/<instance-id>"
```

Use `--artifact-blob-age` to change the blob grace period. Apply-time blob pruning takes the artifact storage lock and fails if a `vectis-artifact` process is actively using the same directory, so run it during a maintenance window or after stopping that shard.

## Production Scheduling

Use two phases in production:

1. A frequent dry run that records expected delete counts.
2. A less frequent apply run after backup freshness and delete counts are acceptable.

The scheduler must provide the same database environment as deployment
migrations:

```sh
VECTIS_DATABASE_DRIVER=pgx
VECTIS_DATABASE_DSN=postgres://vectis:<secret>@postgres.internal:5432/vectis?sslmode=require
```

For single-shard deployments, add local file pruning only when the scheduler
runs on the host that owns those paths:

```sh
vectis-cli retention cleanup --dry-run \
  --terminal-run-age 720h \
  --idempotency-age 48h \
  --audit-age 8760h \
  --log-storage-dir /var/lib/vectis/log/log-1
```

For artifact blob pruning, schedule a maintenance window or stop the artifact
shard first because cleanup must acquire the artifact storage lock:

```sh
vectis-cli retention cleanup --yes \
  --terminal-run-age 720h \
  --idempotency-age 48h \
  --audit-age 8760h \
  --artifact-storage-dir /var/lib/vectis/artifact/artifact-1
```

For multi-shard log or artifact deployments, either run shard-local cleanup on
each shard owner or keep SQL cleanup separate from filesystem cleanup. Do not
point one cleanup job at a storage directory owned by another active shard.

### Example systemd Timer

The Linux service artifacts and `vectis-common` package include
`vectis-retention-scheduled-cleanup.service` and
`vectis-retention-scheduled-cleanup.timer`. The reference timer applies cleanup
weekly, writes the JSON workflow receipt to
`/var/lib/vectis/ops/retention-scheduled-cleanup.json`, and expects retained
backup evidence at `/var/lib/vectis/ops/backup-manifest.json`.

This example adds a site-owned daily dry-run beside the packaged apply timer.
Adjust windows, paths, and backup checks for the environment.

`/etc/systemd/system/vectis-retention-dry-run.service`:

```ini
[Unit]
Description=Preview Vectis retention cleanup

[Service]
Type=oneshot
EnvironmentFile=/etc/vectis/vectis.env
ExecStart=/usr/bin/vectis-cli retention cleanup --dry-run --terminal-run-age 720h --idempotency-age 48h --audit-age 8760h
```

`/etc/systemd/system/vectis-retention-dry-run.timer`:

```ini
[Unit]
Description=Daily Vectis retention cleanup preview

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

Generated `/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.service`:

```ini
[Unit]
Description=Run Vectis scheduled retention cleanup

[Service]
Type=oneshot
EnvironmentFile=-/etc/vectis/vectis.env
EnvironmentFile=-/etc/vectis/vectis-retention-scheduled-cleanup.env
ExecStart=/bin/sh -c 'umask 077; /usr/bin/vectis-cli --format json retention scheduled-cleanup --yes --terminal-run-age 720h --idempotency-age 48h --audit-age 8760h --backup-manifest /var/lib/vectis/ops/backup-manifest.json --backup-expect /etc/vectis/expected-topology.json --backup-max-age 24h --audit-export-output /var/lib/vectis/ops/audit-export.json --audit-export-max-age 24h --hold-review-output /var/lib/vectis/ops/hold-review.json --hold-review-max-age 24h --generated-by systemd:vectis-retention-scheduled-cleanup.timer --reviewed-by retention-scheduler --reason scheduled-retention-cleanup-review --external-ref systemd:vectis-retention-scheduled-cleanup.timer --require-backup-manifest --require-audit-export --require-hold-review --evidence-manifest-promote /var/lib/vectis/ops/retention-cleanup-evidence.json > /var/lib/vectis/ops/retention-scheduled-cleanup.json'
```

Generated `/usr/lib/systemd/system/vectis-retention-scheduled-cleanup.timer`:

```ini
[Unit]
Description=Run Vectis scheduled retention cleanup

[Timer]
OnCalendar=Sun *-*-* 03:00:00
Persistent=true
Unit=vectis-retention-scheduled-cleanup.service

[Install]
WantedBy=timers.target
```

The CLI verifies the manifest contents, expected topology, and `generated_at`
freshness before cleanup. In production, wire
`/var/lib/vectis/ops/backup-manifest.json` to your backup platform's latest
successful backup evidence or require an operator approval step before moving a
manifest into place.

Enable timers with:

```sh
systemctl daemon-reload
systemctl enable --now vectis-retention-dry-run.timer
systemctl enable --now vectis-retention-scheduled-cleanup.timer
```

### Example Kubernetes CronJobs

For Kubernetes-style deployments, use two CronJobs: one dry-run job that always
runs and one apply job gated by your backup platform or manual approval.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: vectis-retention-dry-run
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: retention
              image: registry.example/vectis-cli:VERSION
              envFrom:
                - secretRef:
                    name: vectis-database
              command:
                - /usr/bin/vectis-cli
                - retention
                - cleanup
                - --dry-run
                - --terminal-run-age
                - 720h
                - --idempotency-age
                - 48h
                - --audit-age
                - 8760h
```

Only mount log or artifact storage into an apply CronJob when that job is the
owner for the shard being pruned and the artifact service lock behavior is
accounted for. Mount the backup manifest and expected-topology file into the
apply CronJob, then pass `--require-backup-manifest`, `--backup-manifest`,
`--backup-expect`, and `--backup-max-age` so a missing, stale, or incomplete
backup set stops cleanup. If the job prunes audit rows, also mount the retained
audit export and pass `--require-audit-export`, `--audit-export`, and
`--audit-export-max-age`. If compliance requires active-hold signoff, mount the
retained hold review and pass `--require-hold-review`, `--hold-review`, and
`--hold-review-max-age`. For recurring jobs, mount one retained cleanup evidence
manifest and pass `--evidence-manifest` instead of repeating every evidence path
as command-line flags.

## Safety Guarantees

Retention cleanup does not delete runs in `queued`, `running`, or `orphaned` state. These remain visible for reconciliation and operator repair.

Cleanup also protects:

| Protection | Behavior |
| --- | --- |
| Active and repairable runs | Only terminal runs with `finished_at` older than the cutoff are eligible. |
| Source-backed definition snapshots | Definitions with recorded source provenance are preserved. |
| Job definitions referenced by runs | Referenced definitions are preserved. |
| Shared artifact blobs | A CAS blob is deleted only when no remaining SQL artifact manifest references its blob key. |
| Active artifact storage | Apply-time blob pruning takes `artifact.lock` and refuses to delete while the artifact service owns the directory. |
| Recently orphaned blobs | Unreferenced blobs are skipped until their file mtime is older than the artifact blob cutoff. |
| Disabled surfaces | A duration of `0` disables cleanup for that surface. |
| Cleanup evidence manifest | `--evidence-manifest` accepts only a retained file path using schema `vectis.retention_cleanup_evidence.v1`. It can name backup, audit export, hold review, waiver, freshness, and required-gate inputs for scheduled cleanup. Relative evidence paths resolve next to the manifest file. Explicit CLI flags override individual manifest fields. |
| Backup evidence gate | When `--backup-manifest` is provided, cleanup verifies the manifest and optional expected topology before deletion. `--backup-max-age` also rejects stale manifest evidence. `--require-backup-manifest` fails cleanup if that evidence is missing unless a verified waiver covers `backup_manifest`. |
| Audit export evidence gate | When `--audit-export` is provided, cleanup verifies a retained `vectis-cli audit export` envelope before deleting audit rows. The export must be unfiltered, fresh when `--audit-export-max-age` is set, hash-valid, fully exhausted across cursor pages, and broad enough to cover the audit cleanup cutoff and eligible row count. `--require-audit-export` fails cleanup when audit rows are eligible and export evidence is missing unless a verified waiver covers `audit_export`. |
| Hold review evidence gate | When `--hold-review` is provided, cleanup verifies a retained `vectis-cli retention holds review` envelope against the current active hold inventory before deletion. The review must be fresh when `--hold-review-max-age` is set and hash-valid. `--require-hold-review` fails cleanup if this evidence is missing unless a verified waiver covers `hold_review`. |
| Waiver evidence | `--waiver` accepts only a retained file path. The waiver must use schema `vectis.retention_waiver.v1`, list known gates, include `reason`, `approved_by`, and a future RFC3339 `expires_at`, and is reported in cleanup output. |
| Active retention holds | Active run-scoped holds skip matching terminal runs, related SQL child rows, local run log deletion, and artifact reference removal. Active audit-range holds skip matching `audit_log` rows. |
| Audit trail of cleanup | Applied SQL cleanup inserts a `retention.cleanup` audit event. |
| Audit trail of holds | Creating or releasing a hold inserts `retention.hold.created` or `retention.hold.released` in `audit_log`. |

The SQL cleanup happens in one transaction. Local run log and artifact blob deletion is filesystem work and cannot share that transaction; use dry-run output to confirm the file counts before applying it.

## Reading The Output

Preview output uses `would_delete.*` keys:

```text
dry_run=true
evidence_manifest_verified=true
evidence_manifest_path=/var/lib/vectis/ops/retention-cleanup-evidence.json
evidence_manifest_checked_at=2026-06-28T16:00:00Z
evidence_manifest_generated_at=2026-06-28T15:45:00Z
evidence_manifest_generated_by=retention-scheduler
evidence_manifest_external_ref=CHG-123
evidence_manifest_backup_manifest=/var/lib/vectis/ops/backup-manifest.json
evidence_manifest_backup_expect=/etc/vectis/expected-topology.json
evidence_manifest_backup_storage_reports=2
evidence_manifest_require_backup_manifest=true
evidence_manifest_require_audit_export=true
evidence_manifest_require_hold_review=true
backup_manifest_verified=true
backup_manifest_path=backup-manifest.json
backup_manifest_checked_at=2026-06-28T16:00:00Z
backup_manifest_generated_at=2026-06-28T15:30:00Z
backup_manifest_expectation_source=expected-topology.json
backup_manifest_max_age=24h0m0s
backup_manifest_age=30m0s
backup_manifest_warnings=0
hold_review_verified=true
hold_review_path=hold-review.json
hold_review_checked_at=2026-06-28T16:00:00Z
hold_review_generated_at=2026-06-28T15:45:00Z
hold_review_reviewed_by=compliance-oncall
hold_review_reason=weekly retention cleanup review
hold_review_external_ref=GRC-123
hold_review_active_holds=3
hold_review_holds_sha256=8ce4...
hold_review_max_age=24h0m0s
hold_review_age=15m0s
retention_waiver_verified=true
retention_waiver_path=retention-waiver.json
retention_waiver_checked_at=2026-06-28T16:00:00Z
retention_waiver_waives=audit_export
retention_waiver_reason=Emergency storage pressure while the audit export job is unavailable
retention_waiver_approved_by=security-oncall
retention_waiver_external_ref=INC-1234
retention_waiver_expires_at=2026-07-03T00:00:00Z
cutoff.terminal_runs=2026-04-16T12:00:00Z
would_delete.terminal_runs=42
would_delete.run_dispatch_events=84
would_delete.run_artifacts=42
would_delete.run_tasks=84
would_delete.task_attempts=84
would_delete.run_segments=42
would_delete.segment_executions=84
would_delete.run_log_files=42
would_delete.run_log_bytes=1048576
would_delete.artifact_blob_files=12
would_delete.artifact_blob_bytes=8388608
held.terminal_runs=3
held.run_dispatch_events=6
held.run_artifacts=3
held.run_tasks=6
held.task_attempts=6
held.run_segments=3
held.segment_executions=6
Cleanup not applied.
```

Apply output uses `deleted.*` keys:

```text
dry_run=false
deleted.terminal_runs=42
deleted.run_dispatch_events=84
deleted.run_artifacts=42
deleted.run_tasks=84
deleted.task_attempts=84
deleted.run_segments=42
deleted.segment_executions=84
deleted.run_log_files=42
deleted.run_log_bytes=1048576
deleted.artifact_blob_files=12
deleted.artifact_blob_bytes=8388608
held.terminal_runs=3
held.run_dispatch_events=6
held.run_artifacts=3
held.run_tasks=6
held.task_attempts=6
held.run_segments=3
held.segment_executions=6
audit_event_inserted=true
Cleanup applied.
```

If a cutoff prints `disabled`, that surface was skipped.

## Metrics

The API registers SQL storage pressure gauges on `/metrics`:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `vectis_storage_records` | `surface` | Current row counts for active runs, terminal runs, dispatch events, artifact manifests, task graph tables, job definitions, idempotency keys, and audit log. |
| `vectis_storage_oldest_record_age_seconds` | `surface` | Age of the oldest record for retention-managed SQL surfaces. |

Use these with disk/database capacity signals to decide whether cleanup cadence or retention windows need adjustment.

## When To Run Cleanup

Run cleanup when:

- SQL growth is exceeding the capacity plan.
- A health check or alert points at old retained records.
- A restore drill or release process includes an approved retention step.
- You have a regular operations window and a recent backup.

Avoid cleanup when:

- You are still investigating an incident and may need old runs, logs, or audit rows.
- A restore is in progress and the restored state has not passed the smoke test.
- The dry-run counts are surprising.
- The backup or retention policy is unclear.
- Backup freshness cannot be proved.
- Artifact blob cleanup would contend with an active artifact shard.

For the step-by-step operator recipe, see [Retention Cleanup](./repair-runbooks.md#retention-cleanup).

## Related Docs

| Need | Doc |
| --- | --- |
| Retention cleanup runbook | [Repair Runbooks](./repair-runbooks.md#retention-cleanup) |
| Backup expectations before deletion | [Backup And Restore](./backup-restore.md) |
| Production config contract | [Production Config And Secrets Contract](../deployment/production-config-contract.md) |
| Production monitoring | [Production Monitoring Contract](./production-monitoring.md) |
| CLI command coverage | [CLI Operational Coverage](../reference/cli-operational-coverage.md) |
| Idempotency behavior | [Idempotency And Retries](../../using/idempotency-and-retries.md) |
