# Retention And Storage Pressure

Use this page when Vectis SQL storage is growing, when you are setting a cleanup policy, or when a health check or alert points at old retained records.

Vectis keeps durable SQL state for runs, dispatch visibility, ephemeral job definitions, idempotency keys, and audit events. Operators should prune old data deliberately, after checking backups and previewing exactly what cleanup will delete.

## What Cleanup Does

`vectis-cli retention cleanup` deletes retention-eligible records from the configured Vectis database. It can also delete matching local durable run log files when you pass `--log-storage-dir`.

Cleanup is intentionally manual today:

- `--dry-run` previews cutoffs and counts without changing the database.
- `--yes` applies the same policy.
- Running without either flag fails.

The command prints `key=value` output so it can be captured in runbooks, incident notes, or automation logs.

## Defaults

By default, cleanup uses these windows:

| Surface | Default | Cleanup behavior |
| --- | ---: | --- |
| Terminal runs | 30 days | Deletes terminal runs older than the cutoff: `succeeded`, `failed`, `aborted`, `cancelled`, and `abandoned`. |
| Run dispatch events | follows terminal runs | Deletes dispatch events for runs being deleted. |
| Task graph rows | follows terminal runs | Deletes task nodes, task attempts, run segments, segment executions, and task dispatch intents for runs being deleted. |
| Ephemeral job definitions | 30 days | Deletes unreferenced `job_definitions` rows older than the cutoff. Stored-job definitions are preserved. |
| Idempotency keys | 24 hours | Deletes old idempotency records; retry deduplication is no longer guaranteed after the window. |
| Audit log | 365 days | Deletes old audit rows and inserts a fresh `retention.cleanup` audit event when cleanup is applied. |
| Durable run log files | disabled by default | Pass `--log-storage-dir` to prune local run log files for the terminal runs being deleted. |

Durations can be overridden per run. Use `0` to disable a surface.

## Choose A Policy

Before applying cleanup in production, decide:

| Decision | Why it matters |
| --- | --- |
| How long terminal run history must remain queryable | Deleted runs no longer appear in normal run history. |
| How long idempotency keys must survive client retries | Retrying the same request after the idempotency window may create new work. |
| How long audit rows must be retained | Audit deletion may be subject to security or compliance requirements. |
| Whether local run log files should be pruned | SQL cleanup and log file cleanup are separate; logs may contain sensitive output. |
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

## Safety Guarantees

Retention cleanup does not delete runs in `queued`, `running`, or `orphaned` state. These remain visible for reconciliation and operator repair.

Cleanup also protects:

| Protection | Behavior |
| --- | --- |
| Active and repairable runs | Only terminal runs with `finished_at` older than the cutoff are eligible. |
| Stored job definitions | Definitions still referenced by stored jobs are preserved. |
| Job definitions referenced by runs | Referenced definitions are preserved. |
| Disabled surfaces | A duration of `0` disables cleanup for that surface. |
| Audit trail of cleanup | Applied SQL cleanup inserts a `retention.cleanup` audit event. |

The SQL cleanup happens in one transaction. Local run log file deletion is filesystem work and cannot share that transaction; use dry-run output to confirm the file count before applying it.

## Reading The Output

Preview output uses `would_delete.*` keys:

```text
dry_run=true
cutoff.terminal_runs=2026-04-16T12:00:00Z
would_delete.terminal_runs=42
would_delete.run_dispatch_events=84
would_delete.run_tasks=84
would_delete.task_attempts=84
would_delete.run_segments=42
would_delete.segment_executions=84
would_delete.task_dispatch_intents=40
would_delete.run_log_files=42
Cleanup not applied.
```

Apply output uses `deleted.*` keys:

```text
dry_run=false
deleted.terminal_runs=42
deleted.run_dispatch_events=84
deleted.run_tasks=84
deleted.task_attempts=84
deleted.run_segments=42
deleted.segment_executions=84
deleted.task_dispatch_intents=40
audit_event_inserted=true
Cleanup applied.
```

If a cutoff prints `disabled`, that surface was skipped.

## Metrics

The API registers SQL storage pressure gauges on `/metrics`:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `vectis_storage_records` | `surface` | Current row counts for active runs, terminal runs, dispatch events, job definitions, idempotency keys, and audit log. |
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

For the step-by-step operator recipe, see [Retention Cleanup](./repair-runbooks.md#retention-cleanup).

## Related Docs

| Need | Doc |
| --- | --- |
| Retention cleanup runbook | [Repair Runbooks](./repair-runbooks.md#retention-cleanup) |
| Backup expectations before deletion | [Backup And Restore](./backup-restore.md) |
| CLI command coverage | [CLI Operational Coverage](../reference/cli-operational-coverage.md) |
| Idempotency behavior | [Idempotency And Retries](../../using/idempotency-and-retries.md) |
