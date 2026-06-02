# Capacity And Load Envelope

Use this page to understand the current operating envelope for Vectis: what is safe to rely on today, what needs deployment-specific validation, and which signals show pressure before users feel it.

This page is not a throughput promise for every deployment. Database size, worker host size, log volume, job duration, and client behavior all affect the real limit. Treat the known-safe range as the default posture, then validate larger shapes before depending on them.

For component replica-count and rolling-restart semantics, see [Scaling And Restarts](../deployment/scaling-and-restarts.md). For developer and release validation, see [Capacity And Performance Checks](../../developing/performance/capacity-checks.md).

This page answers "can this deployment shape handle my workload?" Scaling And Restarts answers "is this component topology supported, and what happens when it changes?"

## Current Envelope

| Area | Known-safe posture | Validate before relying on |
| --- | --- | --- |
| API replicas | One API process | Horizontal API replicas, load-balancer behavior, shared SQL rate-limit pressure, and SSE reconnect behavior. |
| Queue | One or more independent queue shards | Shared multi-writer queue storage or active/passive failover for a single shard. |
| Workers | Multiple workers, one run per process | Large worker fleets and DB pool sizing under high claim/renew/finalize rates. |
| Task fan-out | Worker-choreographed fan-out through database-backed task dispatch intents | Very wide DAGs, deep continuation chains, and database/event growth under high task cardinality. |
| Cron | One or more cron processes in one shared database cell | Large schedule sets, cross-cell partitioning, and clock-skew tolerance. |
| Reconciler | One active reconciler, with optional active/passive standbys | Sharded reconcilers under heavy queued-run repair load. |
| Logs | Durable local JSONL storage plus bounded in-memory terminal buffers | Very large persisted logs and many replaying clients. |
| SQLite | Local/dev and small single-node deployments | High-concurrency API, worker, cron, reconciler, and catalog load. |
| Postgres | Recommended for multi-service deployments | Exact pool sizes and trigger/worker scale must be benchmarked per deployment. |

The envelope above is a throughput and pressure statement. The scale-out contract in [Scaling And Restarts](../deployment/scaling-and-restarts.md) remains the source of truth for whether a component is safe to run as multiple active replicas.

## Pressure Signals

Watch these when you increase workload, worker count, client concurrency, or log volume:

| Area | Pressure signal | What it usually means |
| --- | --- | --- |
| API | Rising request latency, `429`, `503`, or readiness failures | API replicas, rate limits, DB, queue, or log dependencies are saturated or unavailable. |
| Queue | Pending depth grows and does not drain after load stops | Producers are outpacing workers, a queue shard is unhealthy, or workers cannot claim work. |
| Workers | Queued-to-running latency rises | Worker count, worker host resources, database claims, or queue delivery are limiting throughput. |
| Task dispatch | Frequent dispatch failures with source `task_dispatch`, rising `vectis_task_dispatch_intents_total{outcome="failed"}`, nonzero `vectis_task_dispatch_pending_intents`, repeated continuation handoffs for the same run, or rising task graph surfaces in `vectis_storage_records` | Task fan-out is creating more queue handoff or database work than the deployment can absorb. |
| Database | Pool waits, maxed in-use connections, slow queries, or storage growth in `vectis_storage_records` | Pool size, query load, retention, or database host capacity needs attention. |
| Logs | Append failures, shard route failures, replay truncation, stream disconnects, forwarder spool backlog, or low log storage space | Log service, forwarding, storage, or client replay demand is limiting observability. |
| Reconciler | Re-enqueue failures or repeated repair for the same runs | Dispatch handoff, queue reachability, registry, TLS, or database state needs repair. |
| Cron | Schedule-to-run latency, repeated handoff attempts for the same run, or missed schedule behavior | Cron load, queue reachability, or schedule ownership needs validation. |

`vectis-cli health check --strict` gives a quick operator view of API readiness, queue backlog, stuck queued runs, log reachability, DB pool pressure, audit durability, TLS files, and local filesystem pressure. Pair it with Prometheus and host/database telemetry for capacity decisions.

## Common Scaling Decisions

| Decision | Guidance |
| --- | --- |
| Add workers | Safe first lever for more parallel job and task delivery execution, but each worker adds database, queue, log, CPU, memory, disk, and network pressure. |
| Increase task fan-out | Validate queue handoff rate, `vectis_task_dispatch_intents_total`, `vectis_task_dispatch_pending_intents`, task reduce/finalize decisions, dispatch event volume, task graph row counts in `vectis_storage_records`, database write load, and retention before relying on very wide or deep DAGs. |
| Add API replicas | Validate load-balancer behavior, shared rate-limit DB pressure, SSE reconnect behavior, and async enqueue repair. |
| Increase DB pool size | Do this only with database host capacity in mind; raising pool limits can move pressure into the database. |
| Increase trigger rate | Watch queue depth, dispatch events, DB pool waits, and idempotency behavior. |
| Increase log readers | Watch log replay, active streams, storage pressure, and client disconnect behavior. |
| Use SQLite under more load | Keep this to local/small single-node deployments unless you validate the exact workload. |
| Use bundled observability in reference deploy | Treat it as demo/staging-friendly until retention, storage, backup, and alerting are sized for your environment. |

## Overload Behavior

| Symptom | Expected behavior |
| --- | --- |
| Trigger succeeds but the run does not start immediately | The run row is durable; enqueue may still be asynchronous and repaired by the reconciler. Inspect dispatch events. |
| Queue overload | Queue depth grows, queued runs age, and DLQ may receive entries if delivery failures persist. |
| Worker overload | Queued-to-running latency grows before terminal status latency improves. |
| Log replay overload | Replay is bounded by the HTTP replay controls in [Log Streaming](../../using/log-streaming.md). |
| API overload | API can return `429` when rate limiting is enabled or `503` when dependencies are unavailable. |
| Database pressure | API readiness, schema checks, run state transitions, audit flushing, and retention cleanup may slow or fail. |

## When To Revalidate

Revalidate the envelope before relying on a new operating point when you:

- add a larger workload class;
- increase task fan-out width, continuation depth, or task cardinality;
- significantly increase worker count or trigger concurrency;
- change database driver, host size, pool settings, or storage class;
- change queue persistence, log storage, or log-forwarder spool storage;
- introduce large logs or many concurrent log readers;
- change queue, worker, cron, reconciler, catalog, API, retry, or log-streaming behavior.

Use [Capacity And Performance Checks](../../developing/performance/capacity-checks.md) for the developer/release process that produces new baseline evidence.

## Related Docs

| Need | Doc |
| --- | --- |
| Developer/release validation | [Capacity And Performance Checks](../../developing/performance/capacity-checks.md) |
| Replica and restart behavior | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
| First-response signals | [Runbooks And Alerts](../reliability/runbooks.md) |
| Health check coverage | [Health Check Catalog](../reference/health-check-catalog.md) |
| Retention and storage pressure | [Retention And Storage Pressure](../reliability/retention.md) |
