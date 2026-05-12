# Retention And Storage Pressure

Vectis keeps durable SQL state for runs, dispatch visibility, ephemeral job definitions, idempotency keys, and audit events. Operators should prune old data deliberately rather than letting these tables grow without bounds.

## Defaults

`vectis-cli retention cleanup` uses these first-pass defaults:

| Surface | Default | Cleanup behavior |
| --- | ---: | --- |
| Terminal runs | 30 days | Deletes only `succeeded` and `failed` runs older than the cutoff. |
| Run dispatch events | follows terminal runs | Deletes dispatch events for runs being deleted. |
| Ephemeral job definitions | 30 days | Deletes unreferenced `job_definitions` rows older than the cutoff. Stored-job definitions are preserved. |
| Idempotency keys | 24 hours | Deletes old idempotency records; retry deduplication is no longer guaranteed after the window. |
| Audit log | 365 days | Deletes old audit rows and inserts a fresh `retention.cleanup` audit event when cleanup is applied. |
| Durable run log files | disabled by default | Pass `--log-storage-dir` to prune local run log files for the terminal runs being deleted. |

Durations can be overridden per run. Use `0` to disable a surface.

## Operator Flow

Always preview first:

```sh
vectis-cli retention cleanup --dry-run
```

Apply the same policy after review:

```sh
vectis-cli retention cleanup --yes
```

For a single-node/local log service, include the log storage directory if you want matching durable run log files removed:

```sh
vectis-cli retention cleanup --dry-run --log-storage-dir "$XDG_DATA_HOME/vectis/jobs"
vectis-cli retention cleanup --yes --log-storage-dir "$XDG_DATA_HOME/vectis/jobs"
```

## Safety Guarantees

Retention cleanup does not delete runs in `queued`, `running`, or `orphaned` state. These remain visible for reconciliation and operator repair.

The SQL cleanup happens in one transaction. Local run log file deletion is filesystem work and cannot share that transaction; use dry-run output to confirm the file count before applying it.

## Metrics

The API registers SQL storage pressure gauges on `/metrics`:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `vectis_storage_records` | `surface` | Current row counts for active runs, terminal runs, dispatch events, job definitions, idempotency keys, and audit log. |
| `vectis_storage_oldest_record_age_seconds` | `surface` | Age of the oldest record for retention-managed SQL surfaces. |

Use these with disk/database capacity signals to decide whether cleanup cadence or retention windows need adjustment. For the operator recipe, see [REPAIR_RUNBOOKS.md#retention-cleanup](repair-runbooks.md#retention-cleanup).
