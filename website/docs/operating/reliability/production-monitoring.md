# Production Monitoring Contract

This page defines the minimum monitoring contract for a
[Production Topology v1](../deployment/production-topology-v1.md) deployment.
It complements the first-response [Runbooks And Alerts](./runbooks.md) page by
separating Vectis-native signals from telemetry the operator's platform must
provide.

Use this page before production handoff, after topology changes, and when
installing alert rules in the production telemetry system.

## Monitoring Ownership

| Owner | Must cover |
| --- | --- |
| Vectis | Health checks, service metrics, run/dispatch visibility, audit durability signals, queue/log/artifact/reconciler/worker metrics, retained-evidence textfile metric generation, and CLI triage commands. |
| Platform telemetry | Host CPU, memory, disk, inodes, filesystem latency, process supervision, service restarts, network reachability, TLS expiry, and log shipping. |
| PostgreSQL platform | Database availability, replication/failover if used, backups, restore status, slow queries, connection pressure, locks, and storage growth. |
| Secret and identity platform | Secret rotation, secret-store availability, SPIFFE authority health, certificate expiry, and unauthorized access attempts. |
| Operators | Threshold tuning, alert routing, runbook links, escalation policy, smoke checks, retained-evidence textfile collection, and periodic backup/restore drill evidence. |

`vectis-cli health check --strict` is the first operator command, not a complete
monitoring system. Production monitoring must combine Vectis metrics with host,
database, filesystem, network, and secret-manager telemetry.

## Required Vectis Signals

Scrape the API plus every enabled dedicated metrics listener from a trusted
monitoring network. Metrics endpoints are private operational surfaces, not
public APIs.

For exact metric names, labels, and interpretation, see the
[Metrics Catalog](../reference/metrics-catalog.md).

| Area | Required signals |
| --- | --- |
| API readiness | API liveness/readiness, HTTP 5xx/429 rate from edge or access logs, API security rejection metrics, audit drop/flush metrics, and DB pool gauges. |
| Queue | Pending depth, in-flight deliveries, DLQ size, expired dispatch drops, and shard process health. |
| Reconciler | Reenqueue attempts/failures, task continuation/finalization repair outcomes, active lease holder visibility, and repeated repair failures. |
| Workers | Jobs received, job duration/outcomes, worker lifecycle/drain state, DB unavailable state, orchestrator recovery count, and SPIFFE SVID check outcomes when enabled. |
| Orchestrator | gRPC health, claim/renew/complete pressure through worker/retry signals, and process availability. |
| Logs | Append failures, shard route failures, memory/subscriber drops, chunk ingest rate, storage read-only state, and log-forwarder spool age/size where used. |
| Artifacts | Upload failures through worker/run outcomes, local CAS blob/byte/free-space gauges, writable state, and artifact service process health. |
| Cron | Due schedule backlog, schedule-to-run latency where available, cron claim failures through service logs, and repeated dispatch attempts. |
| Catalog | Per-cell fan-in read/copy/backfill counts, pending/failed inbox counts, and global catalog apply progress. |
| Secrets | Resolve request totals/duration by outcome/reason/provider, encryptedfs storage health, Knox reachability when enabled, and worker SVID gate failures. |
| Retention | SQL storage record gauges, oldest retained record age, scheduled cleanup evidence freshness, backup manifest freshness, restore-validation freshness, storage verification evidence status, retained waiver expiry, dry-run cleanup evidence, and database/storage growth. |

## Required Platform Signals

Vectis does not replace these telemetry sources:

| Area | Required platform signal |
| --- | --- |
| Hosts | CPU, memory, load, disk bytes, inodes, filesystem latency, process restarts, OOM kills, and clock skew. |
| Durable volumes | Free bytes/inodes and latency for queue persistence, log storage, artifact CAS, encryptedfs envelopes, SPIFFE CA material, and log-forwarder spools. |
| PostgreSQL | Availability, backup freshness, restore job status, connection count, slow queries, locks, replication/failover state when used, storage growth, and transaction log pressure. |
| Edge/load balancer | HTTPS availability, TLS expiry, request rate, 4xx/5xx by route, response latency, SSE disconnects, forwarded-header sanitation failures, and direct-listener bypass attempts. |
| Network | Reachability between API, queue, orchestrator, log, artifact, workers, secrets, cell ingress, registry, and PostgreSQL. |
| Secret/identity systems | Secret access failures, rotation status, certificate expiry, SPIFFE bundle/authority health, and Workload API / Entry API socket availability. |
| Log shipping | Service log ingestion freshness, dropped log records, query availability, and retention. |

## Starter Alert Set

Install the example rules in
[prometheus-examples.yml](../../alerts/prometheus-examples.yml) as a starting
point, then tune thresholds to the environment.

Production v1 should have alerts for:

| Alert area | Why it pages or warns |
| --- | --- |
| API readiness or high 5xx | Users cannot submit or inspect work reliably. |
| Queue backlog and DLQ growth | Work is not draining or deliveries are failing repeatedly. |
| Expired dispatch drops | Runs reached their dispatch start deadline before execution. |
| Reconciler failures | Automatic repair cannot redeliver queued or orphaned work. |
| Worker high failure ratio | Workload execution is failing beyond expected job-level failures. |
| Worker SPIFFE SVID failures | Security gate or identity configuration is blocking execution. |
| Secret resolution failures | Jobs needing secrets cannot run or policy/config changed unexpectedly. |
| Log append, route, or drop failures | Operators may lose run observability. |
| Log-forwarder spool backlog | Worker-side logs are not reaching the log service. |
| Artifact storage pressure | Artifact uploads may fail or shards may become read-only. |
| Audit drops or flush failures | Security/audit trail durability is degraded. |
| Retry exhaustion | A dependency or repair loop is failing past its retry policy. |
| Database pool saturation | DB-backed API, workers, cron, catalog, audit, or retention may slow or fail. |
| API security rejection spikes | Edge/proxy/client behavior changed or hostile traffic increased. |
| Retention evidence freshness | Backup, restore-validation, hold-review, audit export, or scheduled cleanup evidence is missing, stale, or unverified. |
| Retention/storage growth | SQL state or local storage is exceeding the retention plan. |

Every production alert should carry a runbook annotation that points to
[Repair Runbooks](./repair-runbooks.md), [Dispatch Visibility](./dispatch-visibility.md),
[Retention](./retention.md), or this page.

## Dashboards

At handoff, dashboards should show:

- API readiness, request rate, latency, 4xx/5xx, security rejections, and audit durability;
- queue pending/in-flight/DLQ by shard;
- worker throughput, duration, failure ratio, lifecycle/drain state, and SVID gate outcomes;
- reconciler reenqueue and task repair outcomes;
- log append failures, drops, route failures, and forwarder spool backlog;
- artifact storage bytes, free bytes/inodes, blob count, and writable state;
- PostgreSQL pool pressure from Vectis plus database-platform health;
- retention surfaces, oldest retained record age, retained evidence age, storage verification status, and waiver expiry;
- host disk and inode pressure for all configured durable paths.

Bundled dashboards in the reference deployment are useful for demos and smoke
tests. Production dashboards should live in the operator's telemetry system,
with thresholds, ownership, and runbook links tuned for the environment.

## Known Gaps And Workarounds

| Gap | Workaround |
| --- | --- |
| No direct queued-run-age metric yet. | Use `vectis-cli health check`, stuck-run checks, dispatch events, queue backlog alerts, and reconciler outcomes together. |
| Non-API dispatch failure counters are partial. | Correlate API enqueue outcomes, reconciler metrics, per-run dispatch events, and service logs. |
| No rate-limit accepted metric yet. | Use rejection metrics for denied traffic, plus edge/API access logs and HTTP status monitoring for accepted traffic. |
| File-backed queue/log/artifact/spool pressure needs host telemetry. | Monitor the configured paths directly for free bytes, inodes, and latency. |
| Retained evidence freshness is textfile-collected. | Run `vectis-cli retention evidence metrics` after scheduled cleanup, backup verification, restore-validation, audit export, hold review, or waiver changes, then scrape the generated `.prom` file through platform telemetry. |
| Dashboard panels are not yet annotated with runbook links by default. | Put runbook URLs in production alert annotations and dashboard panel descriptions. |
| Orchestrator hot-state pressure is mostly indirect. | Watch worker claim/renew/complete failures, retry exhaustion, worker orchestrator recoveries, queue backlog after task completions, and orchestrator process health. |

## Handoff Checklist

Before declaring monitoring production-ready:

1. All Vectis services expose metrics only to trusted scrapers.
2. API `/health/ready` is wired into traffic gates.
3. gRPC health is monitored for queue, registry, orchestrator, log, and artifact services.
4. Alert rules are installed with environment-specific thresholds.
5. Alert annotations point to runbooks.
6. Host disk and inode telemetry covers every durable Vectis path.
7. PostgreSQL backup freshness and restore status are monitored.
8. Retained backup, restore-validation, scheduled cleanup, audit export, hold-review, storage-report, and waiver evidence metrics are generated and scraped.
9. TLS and SPIFFE certificate expiry are monitored.
10. A known-safe smoke run is exercised after deployment and restore drills.
11. Known monitoring gaps are accepted or tracked as production-readiness follow-ups.

## Related Documentation

| Topic | Document |
| --- | --- |
| First response and alerts | [Runbooks And Alerts](./runbooks.md) |
| Alert examples | [prometheus-examples.yml](../../alerts/prometheus-examples.yml) |
| Metric names and labels | [Metrics Catalog](../reference/metrics-catalog.md) |
| Production config contract | [Production Config And Secrets Contract](../deployment/production-config-contract.md) |
| Production topology | [Production Topology v1](../deployment/production-topology-v1.md) |
| Health check catalog | [Health Check Catalog](../reference/health-check-catalog.md) |
| Capacity envelope | [Capacity And Load Envelope](../capacity/capacity-load-envelope.md) |
| Backup and restore | [Backup And Restore](./backup-restore.md) |
