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
| Workers | Multiple workers, one task delivery per process | Large worker fleets, orchestrator claim/renew/finalize pressure, and queue/log/database side effects. |
| Task fan-out | Orchestrator-choreographed fan-out with direct worker enqueue of activated child executions | Very wide DAGs, deep continuation chains, orchestrator shard sizing, queue handoff rate, and catalog/event growth under high task cardinality. |
| Cron | One or more cron processes in one shared database cell | Large schedule sets, cross-cell partitioning, and clock-skew tolerance. |
| Reconciler | One active reconciler, with optional active/passive standbys | Sharded reconcilers under heavy queued-run repair load. |
| Logs | Durable local JSONL storage plus bounded in-memory terminal buffers | Very large persisted logs and many replaying clients. |
| SQLite | Local/dev and small single-node deployments | High-concurrency API, worker, cron, reconciler, and catalog load. |
| Postgres | Recommended for multi-service deployments | Exact pool sizes and trigger/worker scale must be benchmarked per deployment. |

The envelope above is a throughput and pressure statement. The scale-out contract in [Scaling And Restarts](../deployment/scaling-and-restarts.md) remains the source of truth for whether a component is safe to run as multiple active replicas.

## Initial Control-Plane Rate Budget

For a first deployment, start below the measured local control-plane envelope and grow only after a deployed-stack check. These budgets are for light control-plane work; real CI duration, checkout cost, artifact volume, log volume, and host resources can lower effective throughput before the Vectis control plane is saturated.

| Workload slice | Initial budget | Revalidate before relying on |
| --- | ---: | --- |
| Run admission to queued state | Up to 100 sustained accepted runs/s | Higher sustained trigger rates, API replicas, heavy idempotency replay, large stored job payloads, or mixed API reads during trigger bursts. |
| Light terminal completions with Postgres and orchestrator | Up to 100 sustained terminal runs/s | More than 4 active workers, higher terminal rates, long retry/finalization paths, large fan-out, or many concurrent log readers. |
| Direct child fan-out per activation | Up to 100 direct children from one completed parent task | 250, 1,000, or larger direct fan-outs; wide DAGs mixed with high trigger rate; or any fan-out that must drain within a strict latency SLO. |
| Worker fleet size | Start with 4 workers, then step upward | 8, 16, or larger workers without DB pool, queue depth, orchestrator latency, and log health evidence. |
| Queue shard throughput | Size from macro drain behavior, not queue-only rows | Treating in-memory queue benchmark throughput as a production persistence or service-boundary claim. |
| Log volume and readers | Light logs and a small number of readers | Large stdout/stderr volume, many replay clients, or log shard storage near pressure thresholds. |

This is deliberately conservative. The local measurements below show higher rates, but they omit real load-balancer behavior, TLS, registry lookup, service process placement, host contention, large logs, artifact traffic, API read traffic, SSE clients, repair/list scans, and production database storage behavior.

## Calibration Snapshot

The following non-normative snapshot was captured on an Apple M3 development host with Go 1.26.2 using the checked-in performance harness, disposable Podman Postgres (`pgx_podman`, safe durability), 4 benchmark worker loops, and 1 second benchmark samples. Treat these rows as local calibration evidence and keep raw outputs from your own environment before changing the budget above.

| Slice | Local observation | Interpretation |
| --- | --- | --- |
| Orchestrator service only | Single-leaf runs: ~0.27M-0.81M terminal runs/s. Root+child runs: ~0.39M-0.46M terminal runs/s. | The in-memory orchestrator is not the first limiter for an initial single-cell deployment. |
| Run admission, Postgres | SQL path: ~402-446 queued runs/s. In-process orchestrator load: ~430-467 queued runs/s. gRPC orchestrator load: ~447-463 queued runs/s. | `CreateRun` and durable run/envelope writes dominate admission; orchestrator load adds roughly 0.01 ms in-process or 0.05 ms over the local gRPC benchmark. |
| Script `true` completion, Postgres | SQL-backed claim/finalize: ~182-201 terminal runs/s. In-process orchestrator: ~372-414 terminal runs/s. gRPC orchestrator: ~204-365 terminal runs/s. | Moving claim/finalize hot state out of SQL roughly doubles the best local script completion row, but the remaining limit is worker/process/log/database visibility work. |
| Result-action completion, Postgres | SQL-backed claim/finalize: ~125-158 terminal runs/s. In-process orchestrator: ~309-381 terminal runs/s. gRPC orchestrator: ~278-746 terminal runs/s. | This removes external process spawn and is useful for isolating control-plane cost, but short local samples show enough variance that it should not be published as a deployment promise. |
| Orchestrator fan-out activation | Loading, claiming, and completing a parent with 100 direct children: ~13k-16k parent completions/s. 1,000 children: ~1.4k-1.5k parent completions/s. 5,000 children: ~266-323 parent completions/s. | In-memory activation is not the primary limit, but cost is linear in child count and allocates about 1.2 MB per 1,000-child activation and about 5.7 MB per 5,000-child activation in this local benchmark. |
| Queue-only local memory path | Balanced ring latency rows reached ~10k-18k dequeue ops/s; enqueue/dequeue round trip was roughly 105-126 us/op. | The queue memory path is comfortably above the Postgres macro rows, but queue persistence and real service boundaries still need deployment-specific checks. |

The practical conclusion is that the orchestrator split removed SQL claim/finalize as the primary hot-state bottleneck. The next capacity questions are database write latency, worker count, log behavior, fan-out queue handoff bursts, and service-boundary overhead. Do not interpret "we could not take the orchestrator down" as evidence that the whole solution is beyond one box; the Postgres-backed macro rows are measurable in the hundreds of light terminal events per second on this local setup.

## Pressure Signals

Watch these when you increase workload, worker count, client concurrency, or log volume:

| Area | Pressure signal | What it usually means |
| --- | --- | --- |
| API | Rising request latency, `429`, `503`, or readiness failures | API replicas, rate limits, DB, queue, or log dependencies are saturated or unavailable. |
| Queue | Pending depth grows and does not drain after load stops | Producers are outpacing workers, a queue shard is unhealthy, or workers cannot claim work. |
| Workers | Queued-to-running latency rises | Worker count, worker host resources, orchestrator claim/finalization, database visibility writes, or queue delivery are limiting throughput. |
| Task fan-out | Repeated continuation handoff failures, growing queue backlog after task completions, orchestrator claim/completion latency, or rising task graph surfaces in `vectis_storage_records` | Task fan-out is creating more queue handoff, orchestrator, or catalog work than the deployment can absorb. |
| Database | Pool waits, maxed in-use connections, slow queries, or storage growth in `vectis_storage_records` | Pool size, query load, retention, or database host capacity needs attention. |
| Logs | Append failures, shard route failures, replay truncation, stream disconnects, forwarder spool backlog, or low log storage space | Log service, forwarding, storage, or client replay demand is limiting observability. |
| Reconciler | Re-enqueue failures or repeated repair for the same runs | Dispatch handoff, queue reachability, registry, TLS, or database state needs repair. |
| Cron | Schedule-to-run latency, repeated handoff attempts for the same run, or missed schedule behavior | Cron load, queue reachability, or schedule ownership needs validation. |

`vectis-cli health check --strict` gives a quick operator view of API readiness, queue backlog, cron schedule backlog, stuck queued runs, log reachability, DB pool pressure, audit durability, TLS files, and local filesystem pressure. Pair it with Prometheus and host/database telemetry for capacity decisions.

## Common Scaling Decisions

| Decision | Guidance |
| --- | --- |
| Add workers | Safe first lever for more parallel task delivery execution, but each worker adds orchestrator, database, queue, log, CPU, memory, disk, and network pressure. |
| Increase task fan-out | Validate queue handoff rate, orchestrator claim/completion latency, task reduce/finalize decisions, task graph row counts in `vectis_storage_records`, catalog event load, database write load, and retention before relying on very wide or deep DAGs. |
| Add API replicas | Validate load-balancer behavior, in-process rate limits, SSE reconnect behavior, and async enqueue repair. |
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
| Orchestrator pressure | Workers can dequeue but fail to claim, renew, or complete task executions quickly. |
| Database pressure | API readiness, schema checks, catalog visibility, audit flushing, and retention cleanup may slow or fail. |

## When To Revalidate

Revalidate the envelope before relying on a new operating point when you:

- add a larger workload class;
- increase task fan-out width, continuation depth, or task cardinality;
- significantly increase worker count or trigger concurrency;
- change orchestrator shard count, placement, or discovery mode;
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
