# Runbooks, SLOs, And Alerts

This page is the operator index for Vectis observability. The initial goals are intentionally modest: name the user-facing signals, provide alert examples for emitted metrics, and link each alert to a repair path. For step-by-step repair procedures, see [REPAIR_RUNBOOKS.md](REPAIR_RUNBOOKS.md).

## Initial SLIs And SLOs

| Area | Signal | Starter objective |
| --- | --- | --- |
| Trigger acceptance | API request success and low 5xx rate on trigger/run routes | Keep API dependency failures visible; tune once traffic exists. |
| Queue handoff | Queue pending and in-flight gauges; reconciler reenqueue outcomes | Queued work should drain within one reconciler interval under normal load. |
| Worker execution | `vectis_worker_jobs_received_total` and `vectis_worker_job_duration_seconds` | Workers should continue receiving jobs and terminal outcomes should match workload expectations. |
| Log availability | `vectis_log_storage_append_failures_total`, drops, and gRPC chunk rate | Log append failures should be zero. |
| Audit durability | `vectis_audit_events_dropped_total` and `vectis_audit_flush_failures_total` | Audit drops should be zero. |
| Retry health | `vectis_retries_exhausted_total` and retry delay histogram | Retry exhaustion should be rare and investigated. |
| Database pressure | `database/sql` pool gauges when DB pool metrics are registered | Connections should not sit at configured limits. |
| Storage pressure | `vectis_storage_records` and `vectis_storage_oldest_record_age_seconds` | Durable SQL state should stay within the operator's retention and capacity plan. |

Use these as starter operating signals, not contractual product SLOs. Production targets should be set after load testing and real traffic baselines.

## Alert Examples

Prometheus examples live in [docs/alerts/prometheus-examples.yml](alerts/prometheus-examples.yml). They cover:

- Queue backlog and DLQ growth.
- Reconciler reenqueue failures.
- Worker job failure ratio.
- Log append failures and subscriber drops.
- Audit drops and flush failures.
- Retry exhaustion.
- Database pool saturation.
- SQL storage pressure and old retained records.

Tune thresholds by environment. The Podman reference deploy is useful for demos and smoke tests, but production alert routing should live in the operator's telemetry system.

## Triage Index

Start with `vectis-cli doctor` when the API should be reachable. It checks API liveness/readiness, setup, local CLI token visibility, schema status, queue backlog, reconciler activity, stuck queued runs, log reachability, audit durability, and database pool pressure. For check meanings, see [DOCTOR_CHECK_CATALOG.md](DOCTOR_CHECK_CATALOG.md).

| Alert / symptom | First checks | Repair recipe |
| --- | --- | --- |
| Queue backlog growing | Queue health, worker count, worker job failures, database availability. | [Queued Runs Or Backlog](REPAIR_RUNBOOKS.md#queued-runs-or-backlog) |
| DLQ non-empty | Queue logs, failed delivery reasons, run status, worker availability. | [Queued Runs Or Backlog](REPAIR_RUNBOOKS.md#queued-runs-or-backlog) |
| Reconciler failures | Database health, queue health, job definition availability, dispatch events. | [Reconciler Repair](REPAIR_RUNBOOKS.md#reconciler-repair) |
| Log append failures | Log storage directory permissions, disk space, log service health. | [Log Service Repair](REPAIR_RUNBOOKS.md#log-service-repair) |
| Audit drops | API/database health, async audit buffer pressure, security event volume. | [Audit Durability Repair](REPAIR_RUNBOOKS.md#audit-durability-repair) |
| Retry exhaustion | Component label, dependency health, TLS/config mismatch, network policy. | [Repair Runbooks](REPAIR_RUNBOOKS.md#quick-map) |
| DB pool saturation | Postgres availability, pool sizing, number of service replicas, slow queries. | [Database Pool Pressure](REPAIR_RUNBOOKS.md#database-pool-pressure) |
| Old retained records / table growth | Run `vectis-cli retention cleanup --dry-run`, review [RETENTION.md](RETENTION.md), then apply with `--yes` during a maintenance window. | [Retention Cleanup](REPAIR_RUNBOOKS.md#retention-cleanup) |

## Trace And Log Lookup

1. Start from the API response `X-Request-ID` or the run ID.
2. Search structured API access logs for `correlation_id` when JSON access logs are enabled.
3. Use run ID to inspect worker logs, log service entries, and run dispatch events.
4. In the Podman reference deploy, use Jaeger for traces and OpenSearch/Grafana for service logs when those components are enabled.

Run IDs are the most reliable cross-service handle today. Request IDs are strongest for API-originated workflows.

## Known Gaps

- No direct queued-run-age metric yet.
- No direct dispatch failure counter yet.
- No rate-limit accepted/rejected metric yet.
- File-backed run log and queue persistence pressure still depend on filesystem/disk telemetry outside the SQL gauges.
- Dashboard panels are not yet annotated with runbook links.
