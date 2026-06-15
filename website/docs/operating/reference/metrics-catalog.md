# Metrics Catalog

This page catalogs Vectis-owned Prometheus metrics emitted through the OpenTelemetry Prometheus exporter. It is for dashboard authors, alert maintainers, and operators debugging production incidents.

Use this alongside the [Production Monitoring Contract](../reliability/production-monitoring.md) and the starter [Prometheus alert examples](../../alerts/prometheus-examples.yml). This catalog covers Vectis application metrics; it does not list Go runtime, process, HTTP server, gRPC, host, database-native, or platform metrics.

## Scrape Surfaces

Scrape only from trusted monitoring networks. Metrics expose operational shape and failure state.

| Binary | Metrics surface | Vectis-specific families |
| --- | --- | --- |
| `vectis-api` | API listener `GET /metrics` when metrics are enabled | API enqueue, dispatch events, API security rejections, audit durability, SQL pool, retention storage, retry, log routing |
| `vectis-queue` | Dedicated metrics listener | Queue counters and gauges |
| `vectis-worker` | Dedicated metrics listener | Worker execution, lifecycle, DB state, SPIFFE SVID checks, SQL pool, retry, log routing, task reduce/finalize |
| `vectis-log` | Dedicated metrics listener | Log ingest, durable append failures, in-memory drops, subscriber drops |
| `vectis-log-forwarder` | Dedicated metrics listener | Forwarder chunk/batch outcomes, durable spool gauges, retry, log routing |
| `vectis-artifact` | Dedicated metrics listener | Local artifact CAS storage gauges |
| `vectis-reconciler` | Dedicated metrics listener | Reenqueue and task-finalization repair outcomes, dispatch events, retry |
| `vectis-catalog` | Dedicated metrics listener | Catalog inbox and fan-in counters |
| `vectis-secrets` | Dedicated metrics listener | Secret resolution counters and latency |
| `vectis-cron`, `vectis-cell-ingress` | Dedicated metrics listener when configured | Retry metrics |
| `vectis-orchestrator`, `vectis-registry` | Dedicated metrics listener when configured | No Vectis-specific families yet; use gRPC health, process/runtime metrics, and dependent worker/queue signals |

## Reading The Tables

| Type | Prometheus behavior |
| --- | --- |
| Counter | Monotonic value. Use `rate()` or `increase()` for alerts and dashboards. |
| Gauge | Current sampled value. Alert on sustained bad state rather than a single scrape when possible. |
| Histogram | Base family also exports Prometheus `_bucket`, `_sum`, and `_count` series. Use `rate(<metric>_count[...])` for throughput and `histogram_quantile()` for latency. |

Label values are intentionally low-cardinality unless noted. Do not add raw run IDs, job IDs, tokens, request headers, secret refs, usernames, or filesystem paths to custom metric labels.

## API, Audit, SQL, And Retention

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_api_run_enqueue_total` | Counter | `run_kind`, `outcome` | API run-creation enqueue transitions. `run_kind` is `stored` or `ephemeral`; `outcome` is `accepted`, `attempt`, `success`, `failed_enqueue`, or `failed_touch_dispatched`. | Alert when accepted runs are not followed by success and reconciler is also failing. |
| `vectis_run_dispatch_events_total` | Counter | `source`, `event_type`, `target_cell` | Run dispatch events by producer. `source` is `api`, `cron`, or `reconciler`; `event_type` is `accepted`, `attempt`, `success`, or `failure`. | Correlate queue handoff failures with API, cron, or reconciler producers. |
| `vectis_api_security_rejections_total` | Counter | `reason`, `route`, `status` | Requests rejected by Host, CORS, Fetch Metadata, CSRF, request-target, query, header, method, media-type, body-policy, Accept, or rate-limit controls. | Sudden increases indicate edge/proxy/client changes or hostile traffic. Keep `route` values to route patterns or `unknown_route`. |
| `vectis_audit_events_dropped_total` | Counter | `event_type` | Audit events dropped before persistence, usually because the async buffer is full or blocked. | Should be zero in production. Treat increases as audit durability degradation. |
| `vectis_audit_flush_failures_total` | Counter | `event_count` | Failed audit flush attempts. `event_count` is diagnostic; aggregate without it for broad alerts. | Should be zero. Check DB health and audit repository errors. |
| `db_client_connections_open` | Gauge | none | Open `database/sql` connections in a process pool. | Watch alongside database-native connection limits. |
| `db_client_connections_in_use` | Gauge | none | Connections currently executing a query. | Sustained high values plus pool waits/readiness failures indicate DB pressure. |
| `vectis_storage_records` | Gauge | `surface` | Durable SQL row counts by retention surface. Surfaces include `active_runs`, `terminal_runs`, `run_dispatch_events`, `run_artifacts`, `run_tasks`, `task_attempts`, `run_segments`, `segment_executions`, `job_definitions`, `idempotency_keys`, and `audit_log`. | Use for retention sizing and task graph growth. |
| `vectis_storage_oldest_record_age_seconds` | Gauge | `surface` | Age of the oldest retained record for surfaces with age tracking: `terminal_runs`, `job_definitions`, `idempotency_keys`, and `audit_log`. | Alert when retained data exceeds policy or backup/cleanup expectations. |

## Queue

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_queue_jobs_pending` | Gauge | none | Jobs waiting in the queue and not yet delivered to a worker. | Backlog signal. Pair with worker receive rate and API dispatch events. |
| `vectis_queue_deliveries_inflight` | Gauge | none | Deliveries handed to workers but not acked. | High values can mean slow workers, stuck workers, or lease/ack trouble. |
| `vectis_queue_dlq_size` | Gauge | none | Items currently in the dead-letter queue. | Should normally be zero. Use [Queued Runs Or Backlog](../reliability/repair-runbooks.md#queued-runs-or-backlog). |
| `vectis_queue_enqueued_total` | Counter | none | Jobs accepted into the queue. | Throughput baseline. |
| `vectis_queue_dequeued_total` | Counter | none | Jobs delivered to workers. | Compare with enqueue rate and worker receive rate. |
| `vectis_queue_expired_requeued_total` | Counter | none | Expired in-flight deliveries requeued to pending. | Indicates workers did not ack before lease expiry. |
| `vectis_queue_expired_dropped_total` | Counter | none | Pending jobs or deliveries dropped after dispatch start deadline expiry. | Work missed its start deadline. Investigate backlog and worker capacity. |
| `vectis_queue_dlq_moved_total` | Counter | none | Deliveries moved to DLQ after max requeue attempts. | Investigate queue/worker failures. |
| `vectis_queue_dlq_requeued_total` | Counter | none | DLQ items requeued to the main queue. | Repair activity signal. |

## Worker And Task Execution

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_worker_jobs_received_total` | Counter | none | Jobs dequeued and passed to the worker handler. | Confirms workers are receiving queue deliveries. |
| `vectis_worker_job_duration_seconds` | Histogram | `outcome` | Wall time from worker handler entry to terminal outcome. Outcomes include `success`, `failed`, `aborted`, `skipped_unclaimed`, and `malformed`. | Use `_count` for throughput and outcome ratios; use buckets for duration. |
| `vectis_worker_lifecycle_state` | Gauge | `state` | One labelled worker phase is `1`; others are `0`. States are `idle`, `dequeuing`, `claiming`, `acking`, `executing`, and `finalizing`. | Debug shutdowns, stalls, and where workers spend time. |
| `vectis_worker_draining` | Gauge | none | `1` when the worker has received shutdown and stopped accepting new dequeue work. | Drain/restart visibility. |
| `vectis_worker_db_unavailable` | Gauge | none | `1` after the worker observes DB unavailability on DB-backed transitions. | Database outage visibility from the worker side. |
| `vectis_worker_orchestrator_recoveries_total` | Counter | `stage` | Worker recovered task claims after missing orchestrator hot state. | Growth after orchestrator restart can be normal; sustained growth means unstable or inconsistent orchestrator state. |
| `vectis_worker_spiffe_svid_checks_total` | Counter | `outcome`, `reason` | Worker execution X.509-SVID gate results. Outcomes are `success` or `failed`; reasons include `matched`, `missing_identity`, `missing_source`, `invalid_expected_id`, `mismatch`, `source_error`, `source_timeout`, `canceled`, and `unknown`. | Alert on failures other than expected cancellations when SPIFFE is enabled. |
| `vectis_task_reduce_decisions_total` | Counter | `outcome` | Task reducer decisions. Outcomes are `waiting`, `succeeded`, `failed`, or `error`. | Errors indicate reducer/DB trouble; failed outcomes explain run finalization. |
| `vectis_task_finalize_decisions_total` | Counter | `outcome`, `reduce_outcome` | Task finalizer decisions. Outcomes include `continue`, `reduce_succeeded`, `reduce_failed`, `incomplete`, `execution_failed`, and `execution_aborted`; `reduce_outcome` is reducer output or `none`. | Debug task graph continuation and terminal run decisions. |

## Log Routing, Log Service, And Forwarder

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_log_shard_assignments_total` | Counter | `outcome` | Log shard assignment decisions. Outcomes include `new`, `existing`, or `unknown`. | Assignment churn or failures can explain missing logs. |
| `vectis_log_shard_route_failures_total` | Counter | `operation`, `reason` | Log shard routing failures. Operations include `read`, `write`, and `assigned_write`; reasons include `get_assignment`, `assign`, `no_endpoint`, `no_writable_endpoint`, `assigned_unavailable`, and `shard_mismatch`. | Should be zero or rare. Use [Log Service Repair](../reliability/repair-runbooks.md#log-service-repair). |
| `vectis_log_grpc_chunks_received_total` | Counter | none | Log line chunks received on the log service gRPC stream. | Ingest throughput baseline. |
| `vectis_log_storage_append_failures_total` | Counter | none | Durable log append failures. | Should be zero. Check log storage and shard writability. |
| `vectis_log_memory_buffer_drops_total` | Counter | none | Log lines dropped because a run memory buffer was full. | Indicates live log fanout pressure or slow subscribers. |
| `vectis_log_subscriber_channel_drops_total` | Counter | none | Lines dropped because an SSE/subscriber channel was full. | Indicates slow readers or too-small buffers. |
| `vectis_log_forwarder_chunks_received_total` | Counter | `route` | Chunks received by the sidecar forwarder. `route` is `hinted`, `unhinted`, or `unknown`. | Shows local producer activity and whether chunks carry routing hints. |
| `vectis_log_forwarder_batches_total` | Counter | `outcome` | Forwarder batch outcomes: `sent`, `spooled`, `lost`, or `unknown`. | Spooling indicates log service outage or network failure; lost batches are urgent. |
| `vectis_log_forwarder_spool_files` | Gauge | none | Durable spool files waiting to drain. | Should return to zero after recovery. |
| `vectis_log_forwarder_spool_oldest_age_seconds` | Gauge | none | Age of oldest forwarder spool file. | Alert when backlog age exceeds operator tolerance. |

## Artifact Storage

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_artifact_storage_blobs` | Gauge | none | Content-addressed artifact blob files stored by this shard. | Capacity and retention trend. |
| `vectis_artifact_storage_bytes` | Gauge | none | Artifact blob bytes stored by this shard. | Capacity and retention trend. |
| `vectis_artifact_storage_free_bytes` | Gauge | none | Filesystem bytes available to artifact storage. | Alert before uploads fail or shard becomes read-only. |
| `vectis_artifact_storage_free_inodes` | Gauge | none | Filesystem inodes available to artifact storage. | Alert before many small artifacts exhaust inodes. |
| `vectis_artifact_storage_new_blob_writable` | Gauge | none | `1` when the shard accepts new blobs; `0` when read-only. | `0` should page or warn depending on redundancy. |

## Reconciler And Catalog

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_reconciler_runs_scanned_total` | Counter | none | Queued runs considered for reconcile dispatch. | Confirms reconciler is active when queued work exists. |
| `vectis_reconciler_reenqueue_total` | Counter | `outcome` | Reconcile redispatch outcomes. Outcomes include `success`, `skipped_policy`, `skipped_missing_job_definition`, `failed_load_job_definition`, `failed_parse_job_definition`, `failed_enqueue`, and `failed_touch_dispatched`. | Alert on non-success outcomes except known policy skips. |
| `vectis_reconciler_task_finalization_repairs_total` | Counter | `outcome`, `reduce_outcome` | Task finalization repair outcomes. Outcomes are `success`, `skipped_conflict`, or `error`; `reduce_outcome` is reducer output or `unknown`. | Alert on `outcome="error"`. |
| `vectis_catalog_events_read_total` | Counter | none | Pending cell catalog events read from the inbox. | Catalog processor activity. |
| `vectis_catalog_events_applied_total` | Counter | none | Cell catalog events applied to the global catalog. | Should track reads over time. |
| `vectis_catalog_events_failed_total` | Counter | none | Cell catalog events marked permanently failed. | Investigate stale global run/artifact/security state. |
| `vectis_catalog_events_retryable_total` | Counter | none | Events left pending after retryable apply failure. | Check target DB health or temporary conflicts. |
| `vectis_catalog_process_errors_total` | Counter | none | Catalog inbox process attempts that failed before event-level status updates completed. | Indicates processor-level failure. |
| `vectis_catalog_fanin_events_read_total` | Counter | `source_cell` | Pending events read from cell source databases during fan-in. | Per-cell fan-in activity. `source_cell` should be bounded to configured cell IDs. |
| `vectis_catalog_fanin_events_copied_total` | Counter | `source_cell` | Events copied into the global inbox. | Compare with read count. |
| `vectis_catalog_fanin_events_backfilled_total` | Counter | `source_cell` | Missing catalog events synthesized before fan-in copy. | Backfill activity; sustained growth means source catalog gaps. |

## Secrets

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_secrets_resolve_requests_total` | Counter | `outcome`, `reason`, `provider` | Secret resolve RPCs. Outcomes are `success`, `denied`, `not_found`, or `failed`; reasons include `ok`, `missing_provider`, `missing_authorizer`, `authorization_denied`, `provider_denied`, `provider_not_found`, `provider_error`, `invalid_bundle`, and `unknown`. | Alert when non-success outcomes increase unexpectedly. |
| `vectis_secrets_resolve_duration_seconds` | Histogram | `outcome`, `reason`, `provider` | Secret resolve RPC wall time with the same labels as request count. | Track provider latency and failure latency. |

Secrets metrics deliberately exclude secret refs, plaintext, run IDs, execution IDs, and SPIFFE IDs.

## Retry

Retry metrics are shared across services that use the common retry helper.

| Metric | Type | Labels | Meaning | Operator use |
| --- | --- | --- | --- | --- |
| `vectis_retries_total` | Counter | `component`, `service`, `operation` | Retry attempts by component string. `service` and `operation` are parsed from `component`. | Dependency instability or transient transport failure. |
| `vectis_retries_exhausted_total` | Counter | `component`, `service`, `operation` | Retry loops that exhausted all attempts. | Alert on increases; pair with the component label to find the failing dependency. |
| `vectis_retry_delay_seconds` | Histogram | `component`, `service`, `operation` | Observed backoff delay between retry attempts. | Understand how much latency retries add. |

Keep `component` values from code-controlled constants. Do not include dynamic target addresses or user input.

## High-Signal Starting Queries

| Goal | Query |
| --- | --- |
| Queue backlog | `vectis_queue_jobs_pending > 0` |
| Worker failure ratio | `sum(rate(vectis_worker_job_duration_seconds_count{outcome!="success"}[15m])) / clamp_min(sum(rate(vectis_worker_job_duration_seconds_count[15m])), 1)` |
| Secret failures | `increase(vectis_secrets_resolve_requests_total{outcome!="success"}[10m]) > 0` |
| Audit drops | `increase(vectis_audit_events_dropped_total[10m]) > 0` |
| API security rejection spike | `sum by (reason, route, status) (increase(vectis_api_security_rejections_total[5m]))` |
| Dispatch drops | `increase(vectis_queue_expired_dropped_total[10m]) > 0` |
| Log append failures | `increase(vectis_log_storage_append_failures_total[10m]) > 0` |
| Artifact read-only shard | `vectis_artifact_storage_new_blob_writable == 0` |
| Reconciler failures | `increase(vectis_reconciler_reenqueue_total{outcome!="success"}[10m]) > 0` |
| Retry exhaustion | `increase(vectis_retries_exhausted_total[10m]) > 0` |

Tune all thresholds to your traffic, worker count, retention plan, and recovery objectives before paging on them.

## Known Gaps

| Gap | Workaround |
| --- | --- |
| No direct queued-run-age metric yet. | Use `vectis-cli health check`, stuck-run checks, dispatch events, queue backlog, and reconciler outcomes. |
| Orchestrator hot-state pressure is mostly indirect. | Watch worker orchestrator recoveries, worker claim/finalize failures, retry exhaustion, queue backlog after task completions, and process health. |
| API accepted request rate is not a Vectis-specific metric yet. | Use edge/API access logs and HTTP server instrumentation, plus `vectis_api_security_rejections_total` for denied requests. |
| Queue/log/artifact/spool filesystem latency is platform-owned. | Monitor durable paths directly with host or storage telemetry. |

## Related Docs

| Need | Doc |
| --- | --- |
| Monitoring ownership and dashboards | [Production Monitoring Contract](../reliability/production-monitoring.md) |
| Starter alert rules | [prometheus-examples.yml](../../alerts/prometheus-examples.yml) |
| Audit event names and durability defaults | [Audit Event Catalog](./audit-event-catalog.md) |
| Health check IDs | [Health Check Catalog](./health-check-catalog.md) |
| First-response runbooks | [Runbooks And Alerts](../reliability/runbooks.md) |
| Repair procedures | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Storage surfaces | [Database Schema Reference](./database-schema.md) |
