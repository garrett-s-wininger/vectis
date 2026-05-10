# Scaling And Restarts

This document is the first-pass scale-out contract for Vectis. It describes which components can run as multiple replicas today, which components should remain singleton, and what operators should expect during rolling restarts.

It is intentionally conservative. Future scale-out work should update this document as code gains stronger HA, sharding, shared-state, or leader-election behavior.

## Current Posture

Vectis is safest as a single-site deployment with one shared SQL database, one active queue, one active log service, and as many workers as the database and queue can comfortably support.

Horizontal scale-out is currently strongest at the **worker** layer. Most control-plane services are better treated as singleton or externally coordinated until the repository grows explicit clustering semantics.

## Replica Matrix

| Binary | Current recommended replicas | Multiple replicas today? | Semantics and caveats |
| --- | ---: | --- | --- |
| `vectis-api` | 1 | Conditional | Multiple API replicas can serve stateless HTTP traffic against the same database and queue, but rate limits are in-process per replica, SSE clients must reconnect through the load balancer, setup/admin writes still rely on database constraints, and async enqueue after `202` is repaired by `vectis-reconciler` rather than joined during API shutdown. |
| `vectis-queue` | 1 | No active/active | The queue owns in-memory delivery state plus optional local WAL/snapshot persistence. There is no shared distributed queue or multi-writer queue protocol. Run one active queue endpoint; use persistence and supervisor restart for recovery. |
| `vectis-registry` | 1 | Not a shared registry | Registry state is in memory. Multiple registries would be independent unless an operator pins clients to one or provides an external discovery layer. Prefer pinned service addresses if registry availability is a concern. |
| `vectis-log` | 1 | Not active/active | The log service owns local durable log storage and active stream buffers. Multiple instances do not share run log files or SSE state unless an external storage/routing design is added. |
| `vectis-worker` | N | Yes | Workers are the primary safe scale-out unit. Each process executes one run at a time. Database claims and leases prevent duplicate execution for persisted runs even when queue messages are duplicated. Size database pools and queue delivery timeouts for the fleet. |
| `vectis-log-forwarder` | One per producer host/process group | Yes, by ownership | Multiple forwarders are safe when each owns its own socket/spool path. Sharing a spool directory between independent forwarders is not a clustering mechanism. |
| `vectis-cron` | 1 | Not without partitioning | Schedule claiming protects a row during a firing attempt, but Vectis does not provide HA leadership or schedule sharding as a deployment contract yet. Treat one active cron as the safe default; use external partitioning or external schedulers for HA. |
| `vectis-reconciler` | 1 | Limited | Multiple reconcilers may produce duplicate queue handoffs under some races; database run claims still guard execution. The safe default is one active reconciler until repair-load and duplicate-handoff behavior are tested as a scale target. |
| `vectis-local` | 1 | No | Development supervisor for a local stack. Do not use as a replicated production control plane. |
| `vectis-cli` | N/A | Yes | One-shot client. Concurrent commands rely on API and database semantics. |

## Rolling Restart Table

| Binary | Restart behavior | Operator expectation |
| --- | --- | --- |
| `vectis-api` | Stops accepting new HTTP requests through graceful shutdown. In-flight requests and SSE streams get a bounded drain window. Detached enqueue goroutines are not joined. | Put the replica behind readiness checks. During rolling restarts, clients may need to reconnect SSE streams. Keep `vectis-reconciler` healthy so accepted-but-not-enqueued runs are repaired. |
| `vectis-queue` | With persistence enabled, pending and in-flight delivery metadata reload from disk. Without persistence, in-memory queue state is lost. | Use one active queue and durable storage for planned restarts. After restart, watch queue depth, delivery age, DLQ movement, and reconciler repair attempts. |
| `vectis-registry` | In-memory registrations are lost on restart and must be republished by services or avoided with pinned addresses. | Restart registry before dependents when possible, or use pinned queue/log addresses for critical services. Expect discovery clients to reconnect. |
| `vectis-log` | Ingest and SSE streams are interrupted. Durable local log files remain if storage is preserved; active stream buffers are process-local. | Restart during a quiet window when possible. Workers require log service availability before executing runs. Clients should reconnect log streams. |
| `vectis-worker` | On `SIGINT`/`SIGTERM`, stops dequeuing and lets the current job continue toward finalization. Abrupt death relies on leases, queue delivery timeout, and later repair. | Roll workers gradually. Prefer graceful termination windows long enough for typical jobs, or accept that long jobs may need lease/reconciler handling after hard stops. |
| `vectis-log-forwarder` | Local spool preserves unsent batches when configured and writable. | Keep spool storage across restarts. Watch spool age/size because there is no common fleet-level counter yet. |
| `vectis-cron` | Missed schedule evaluations are not replayed as a separate HA log; each cycle reads due schedules from the database. | Keep one active cron. Avoid overlapping old and new cron processes unless the deployment intentionally partitions schedules. |
| `vectis-reconciler` | Repair scans pause while the process is down. Queued runs remain in the database. | One active reconciler is enough for the current posture. During restarts, queued-run repair latency can increase by at least one interval. |
| `vectis-local` | Stops or restarts the supervised local stack. | Development-only behavior; not a production rolling-restart primitive. |

## Probe And Traffic Gates

Use readiness to decide whether a replica should receive new work, not whether the process exists.

| Component class | Liveness | Readiness / traffic gate |
| --- | --- | --- |
| API | `GET /health/live` | `GET /health/ready`, which checks database ping and managed queue connectivity. |
| Queue, registry, log gRPC | Standard gRPC health service | Standard gRPC health service returning `SERVING`. |
| Worker, cron, reconciler, log-forwarder | Supervisor process health | External wrapper or deployment checks for required dependencies; do not route work to them until database/queue/log dependencies are reachable as applicable. |
| Metrics listeners | Process/exporter visibility | Do not use `/metrics` as workflow readiness by itself. |

## Known Scale-Out Boundaries

- **Database:** every writer must use the same logical database and schema. Vectis does not provide database failover, read-replica routing, or cross-site replication.
- **API rate limits:** buckets are per API process. Adding replicas raises the effective limit unless an external limiter or sticky routing is added.
- **Queue:** active/active queue replicas are not supported. Queue persistence is local to one queue instance.
- **Logs:** active/active log replicas are not supported without shared storage and routing.
- **Cron:** no built-in leader election or schedule partitioning contract.
- **Reconciler:** duplicate handoff is tolerable for persisted runs because worker database claims guard execution, but duplicate repair traffic is still operational noise.
- **Workers:** scale-out is limited by database pool sizing, queue throughput, log service capacity, and workload resource isolation.

## Future Scale-Out Tranche

Use this document as the checklist for future work:

- Decide whether API rate limits should move to a shared backend.
- Add explicit queue HA strategy or declare queue singleton as a long-term contract.
- Add log storage/routing strategy before multiple log replicas.
- Add cron leader election or schedule partitioning.
- Load-test multiple reconcilers and document duplicate-handoff bounds.
- Add worker pools, labels, and pool-aware scheduling.
- Add rolling-restart tests for API, queue persistence, worker drain, cron, and reconciler repair latency.
