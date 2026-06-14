# Retention And Storage Pressure

Use this page when Vectis SQL storage is growing, when you are setting a cleanup policy, or when a health check or alert points at old retained records.

Vectis keeps durable SQL state for runs, artifact manifests, dispatch visibility, ephemeral job definitions, idempotency keys, and audit events. Operators should prune old data deliberately, after checking backups and previewing exactly what cleanup will delete.

## What Cleanup Does

`vectis-cli retention cleanup` deletes retention-eligible records from the configured Vectis database. It can also delete matching local durable run log files when you pass `--log-storage-dir`, and unreferenced local artifact CAS blobs when you pass `--artifact-storage-dir`.

Cleanup is intentionally explicit today:

- `--dry-run` previews cutoffs and counts without changing the database.
- `--yes` applies the same policy.
- Running without either flag fails.

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
| Ephemeral job definitions | 30 days | Deletes unreferenced `job_definitions` rows older than the cutoff. Stored-job definitions are preserved. |
| Idempotency keys | 24 hours | Deletes old idempotency records; retry deduplication is no longer guaranteed after the window. |
| Audit log | 365 days | Deletes old audit rows and inserts a fresh `retention.cleanup` audit event when cleanup is applied. |
| Durable run log files | disabled by default | Pass `--log-storage-dir` to prune local run log files for the terminal runs being deleted. |
| Durable artifact blobs | 30 days when enabled | Pass `--artifact-storage-dir` to prune local CAS blobs with no remaining SQL references and a file mtime older than the cutoff. |

Durations can be overridden per run. Use `0` to disable a surface. Artifact manifest cleanup is SQL cleanup and follows `--terminal-run-age`; artifact blob cleanup is local filesystem cleanup and only runs when `--artifact-storage-dir` is provided.

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

Keep the production idempotency window longer than the longest realistic client retry window. Keep audit retention aligned with your security policy, not just disk capacity.

## Operator Flow

Always preview first:

```sh
vectis-cli retention cleanup --dry-run
```

Apply the same policy after review:

```sh
vectis-cli retention cleanup --yes
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

This example records a daily dry-run and applies cleanup weekly. Adjust windows,
paths, and backup checks for the environment.

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

`/etc/systemd/system/vectis-retention-apply.service`:

```ini
[Unit]
Description=Apply Vectis retention cleanup
ConditionPathExists=/var/lib/vectis/ops/backup-fresh

[Service]
Type=oneshot
EnvironmentFile=/etc/vectis/vectis.env
ExecStart=/usr/bin/vectis-cli retention cleanup --yes --terminal-run-age 720h --idempotency-age 48h --audit-age 8760h
```

`/etc/systemd/system/vectis-retention-apply.timer`:

```ini
[Unit]
Description=Weekly Vectis retention cleanup

[Timer]
OnCalendar=Sun *-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

The `ConditionPathExists` guard is only an example. In production, wire the
apply job to your backup platform's freshness signal or require an operator
approval step before creating that marker.

Enable timers with:

```sh
systemctl daemon-reload
systemctl enable --now vectis-retention-dry-run.timer
systemctl enable --now vectis-retention-apply.timer
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
accounted for.

## Safety Guarantees

Retention cleanup does not delete runs in `queued`, `running`, or `orphaned` state. These remain visible for reconciliation and operator repair.

Cleanup also protects:

| Protection | Behavior |
| --- | --- |
| Active and repairable runs | Only terminal runs with `finished_at` older than the cutoff are eligible. |
| Stored job definitions | Definitions still referenced by stored jobs are preserved. |
| Job definitions referenced by runs | Referenced definitions are preserved. |
| Shared artifact blobs | A CAS blob is deleted only when no remaining SQL artifact manifest references its blob key. |
| Active artifact storage | Apply-time blob pruning takes `artifact.lock` and refuses to delete while the artifact service owns the directory. |
| Recently orphaned blobs | Unreferenced blobs are skipped until their file mtime is older than the artifact blob cutoff. |
| Disabled surfaces | A duration of `0` disables cleanup for that surface. |
| Audit trail of cleanup | Applied SQL cleanup inserts a `retention.cleanup` audit event. |

The SQL cleanup happens in one transaction. Local run log and artifact blob deletion is filesystem work and cannot share that transaction; use dry-run output to confirm the file counts before applying it.

## Reading The Output

Preview output uses `would_delete.*` keys:

```text
dry_run=true
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
