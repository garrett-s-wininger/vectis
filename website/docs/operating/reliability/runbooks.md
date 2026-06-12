# Runbooks And Alerts

This is the operator entry point for Vectis health checks, alerts, and first triage. Start here when the system is reachable but something looks wrong.

For the production monitoring contract, see [Production Monitoring Contract](./production-monitoring.md). For rehearsed upgrade, rollback, and restore procedures, see [Production Drills](./production-drills.md). For step-by-step repair procedures, see [Repair Runbooks](./repair-runbooks.md). For check IDs and JSON output shape, see [Health Check Catalog](../reference/health-check-catalog.md).

## First Response

When the API should be reachable, start with:

```sh
vectis-cli health check --strict
```

Use JSON when you need evidence, documentation links, or automation-friendly output:

```sh
vectis-cli health check --json
```

For a specific run, start with:

```sh
vectis-cli runs show <run-id>
```

That command shows status, dispatch events, and the latest failed worker-controlled SVID or secret-resolution gate when one explains a failed run.

## Triage Map

| Symptom or alert | First checks | Repair path |
| --- | --- | --- |
| API not ready | Database reachability, schema status, queue connectivity. | [Schema Or Migration Repair](./repair-runbooks.md#schema-or-migration-repair), then [Queued Runs Or Backlog](./repair-runbooks.md#queued-runs-or-backlog) if queue is involved. |
| Queue backlog growing | Queue health, worker count, worker failures, database availability. | [Queued Runs Or Backlog](./repair-runbooks.md#queued-runs-or-backlog) |
| DLQ non-empty | Queue logs, failed delivery reasons, run status, worker availability. | [Queued Runs Or Backlog](./repair-runbooks.md#queued-runs-or-backlog) |
| Queued run not starting | Run dispatch events, reconciler health, queue state, worker availability. | [Dispatch Visibility](./dispatch-visibility.md), then [Reconciler Repair](./repair-runbooks.md#reconciler-repair) |
| Reconciler failures | Database health, queue health, job definition availability, dispatch events. | [Reconciler Repair](./repair-runbooks.md#reconciler-repair) |
| Logs unavailable | Log gRPC health, log storage path, registry/pinned log address, TLS config. | [Log Service Repair](./repair-runbooks.md#log-service-repair) |
| Audit drops or flush failures | API health, database health, audit buffer pressure, event volume. | [Audit Durability Repair](./repair-runbooks.md#audit-durability-repair) |
| Retry exhaustion | Component label, dependency health, TLS mismatch, network policy. | [Repair Runbooks](./repair-runbooks.md#quick-map) |
| DB pool saturation | Postgres availability, pool sizing, replica count, slow queries. | [Database Pool Pressure](./repair-runbooks.md#database-pool-pressure) |
| API security rejection spike | `reason`, `route`, `status`, edge proxy logs, recent client or ingress changes. | [API Security Rejections](./repair-runbooks.md#api-security-rejections) |
| Secret resolution failures | `runs show` latest failed gate, secret broker logs, `outcome`, `reason`, `provider`, SPIFFE SVID checks, policy rules, encryptedfs root/key. | [Secret Resolution](./repair-runbooks.md#secret-resolution) |
| Old retained records or SQL growth | Retention policy, dry-run cleanup counts, backup status. | [Retention Cleanup](./repair-runbooks.md#retention-cleanup) |
| A run needs manual action | Run status, dispatch events, worker ownership, automatic repair state. | [Manual Run Intervention](./repair-runbooks.md#manual-run-intervention) |

## Health Check Coverage

`vectis-cli health check` checks the API-facing parts of the deployment:

| Area | Examples |
| --- | --- |
| API state | Liveness, readiness, versioned operational endpoints. |
| Auth setup | Setup status and local CLI token visibility when API auth is enabled. |
| Schema | Database schema status through the API. |
| Queue, cron, and reconciler | Queue backlog, due cron schedules, stuck queued runs, reconciler recovery visibility. |
| Source control | Source repository sync status, stale enabled source repositories and schedules, and active schedule overrides. |
| Logs | API-to-log-service gRPC reachability. |
| Audit | Dropped audit events and flush failures. |
| Database pool | API-visible `database/sql` pool pressure. |
| Local files | TLS file readability and queue/log/artifact/spool filesystem checks where paths are locally visible. |

The health check is not a complete production monitoring system. It does not replace host disk telemetry, database monitoring, queue/log/artifact capacity dashboards, or workload-specific alerts. Use [Production Monitoring Contract](./production-monitoring.md) for the production-v1 handoff checklist.

## Starter Signals

Use these as starter operating signals, not contractual product SLOs. Production targets should be set after load testing and real traffic baselines.

| Area | Signal | Starter objective |
| --- | --- | --- |
| Trigger acceptance | API request success and low 5xx rate on trigger/run routes. | Dependency failures should be visible quickly. |
| Queue handoff | Queue pending/in-flight gauges, cron due schedule count, and reconciler reenqueue outcomes. | Queued work should drain within one reconciler interval under normal load. |
| Worker execution | `vectis_worker_jobs_received_total`, `vectis_worker_job_duration_seconds`, and `vectis_worker_spiffe_svid_checks_total` when SPIFFE execution SVID acquisition is enabled. | Workers should keep receiving jobs; terminal outcomes should match workload expectations, and SPIFFE SVID acquisition failures should be investigated. |
| Secret resolution | `vectis_secrets_resolve_requests_total` and `vectis_secrets_resolve_duration_seconds` by `outcome`, `reason`, and `provider`. | Failed resolves should match known policy/config changes and be investigated when unexpected. |
| Log availability | `vectis_log_storage_append_failures_total`, `vectis_log_shard_route_failures_total`, log drops, and gRPC chunk rate. | Log append and shard routing failures should be zero. |
| Log-forwarder backlog | `vectis_log_forwarder_spool_files`, `vectis_log_forwarder_spool_oldest_age_seconds`, and `vectis_log_forwarder_batches_total`. | Spool backlog should drain quickly after log service recovery. |
| Audit durability | `vectis_audit_events_dropped_total` and `vectis_audit_flush_failures_total`. | Audit drops should be zero. |
| Retry health | `vectis_retries_exhausted_total` and retry delay histogram. | Retry exhaustion should be rare and investigated. |
| Database pressure | `database/sql` pool gauges where DB pool metrics are registered. | Connections should not sit at configured limits. |
| API security posture | `vectis_api_security_rejections_total` by `reason`, `route`, and `status`. | Rejections should match expected noise; sustained changes should be investigated. |
| Storage pressure | `vectis_storage_records` and `vectis_storage_oldest_record_age_seconds`. | Durable SQL state should stay within the retention and capacity plan. |

## Alert Examples

Prometheus examples live in [prometheus-examples.yml](../../alerts/prometheus-examples.yml). They cover:

- queue backlog and DLQ growth;
- reconciler reenqueue failures;
- worker job failure ratio;
- secret resolution failures;
- log append failures, shard routing failures, subscriber drops, and log-forwarder spool backlog;
- audit drops and flush failures;
- retry exhaustion;
- database pool saturation;
- API security rejection spikes and sustained rejection rates.

Tune thresholds by environment. The Podman reference deployment is useful for demos and smoke tests, but production alert routing should live in the operator's telemetry system.

## Trace And Log Lookup

Use the most specific handle you have:

| Handle | Where to use it |
| --- | --- |
| Run ID | Run status, run logs, worker logs, log service entries, and dispatch events. |
| API `X-Request-ID` | API access logs and request-specific traces. |
| `correlation_id` | Structured API access logs when JSON access logs are enabled. |
| Component label | Retry metrics and service logs. |

In the Podman reference deployment, Jaeger can help with traces and OpenSearch/Grafana can help with service logs when those components are enabled.

Run IDs are the most reliable cross-service handle today. Request IDs are strongest for API-originated workflows.

## Known Monitoring Gaps

These are current monitoring limits operators should cover with external telemetry or manual triage:

| Gap | Practical workaround |
| --- | --- |
| No direct queued-run-age metric yet. | Use `vectis-cli health check`, stuck-run checks, dispatch events, and queue backlog alerts. |
| Non-API dispatch failure counters are partial. | Use API enqueue outcomes, reconciler outcomes, and per-run dispatch events together. |
| No rate-limit accepted metric yet. | Use `vectis_api_security_rejections_total{reason="rate_limit_exceeded"}` for rejects, plus API access logs and HTTP status monitoring for accepted traffic. |
| File-backed run log and queue persistence pressure need filesystem telemetry. | Monitor the storage paths directly. |
| Dashboard panels are not yet annotated with runbook links. | Keep alert annotations linked to repair runbooks. |

## Related Documentation

| Topic | Document |
| --- | --- |
| Production monitoring | [Production Monitoring Contract](./production-monitoring.md) |
| Production drills | [Production Drills](./production-drills.md) |
| Repair steps | [Repair Runbooks](./repair-runbooks.md) |
| Health check catalog | [Health Check Catalog](../reference/health-check-catalog.md) |
| Queue handoff triage | [Dispatch Visibility](./dispatch-visibility.md) |
| Retention cleanup | [Retention](./retention.md) |
| Scaling and restarts | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
