# Scaling And Restarts

This page explains how far you can scale each Vectis component today, which services should stay singleton, and what to expect when restarting them.

The current posture is intentionally conservative: Vectis is safest as a single-site deployment with one shared SQL database, one active queue, one active log service, one active cron, one active reconciler, and as many workers as the database, queue, and log service can comfortably support.

For dependency behavior during outages, see [Failure Domains](../../concepts/failure-domains.md). For reference deployment boundaries, see [Reference Deployment Posture](./reference-deployment-posture.md). For database pool sizing, see [Configuration](../configuration.md#postgresql-connection-pool-pgx-only).

This page answers "is this component topology supported, and what happens when it changes?" For workload pressure, saturation signals, and when to revalidate a larger operating point, see [Capacity And Load Envelope](../capacity/capacity-load-envelope.md).

## Quick Guidance

| Component | Scale guidance |
| --- | --- |
| API | Can be replicated for stateless HTTP traffic with caveats. Keep the reconciler healthy. |
| Workers | Primary safe scale-out unit. Add workers to increase job throughput. |
| Queue | Run one active queue. Active/active queue replicas are not supported. |
| Registry | Run one registry by default, configure gossip clustering deliberately, or avoid registry dependency with pinned addresses. |
| Log service | Run one active log service unless you add external storage/routing. |
| Cron | Run one active cron unless you intentionally partition schedules or use an external scheduler. |
| Reconciler | Run one active reconciler for the current posture. |
| Log-forwarder | One per producer host or process group, each with its own socket/spool path. |
| `vectis-local` | Development only. Do not treat it as a production control plane. |

## Replica Matrix

| Binary | Safe default | Can run multiple? | What to watch |
| --- | ---: | --- | --- |
| `vectis-api` | 1 | Conditional | Multiple API replicas can serve HTTP against the same database and queue. Rate limits are in-process per replica, SSE clients must reconnect through load balancers, and accepted-but-not-enqueued runs rely on the reconciler if an API exits after `202`. |
| `vectis-worker` | N | Yes | Each worker executes one run at a time. Database claims and leases guard persisted runs against duplicate execution even if queue handoff is duplicated. Size DB pools, queue delivery timeouts, and log capacity for the fleet. |
| `vectis-queue` | 1 | No active/active | Queue delivery state is owned by one queue process, with optional local persistence. Use one active endpoint and durable storage. |
| `vectis-registry` | 1 | Conditional | Single registry is the safe default. Gossip-based HA registry is available when every registry node is configured with static cluster membership; otherwise, multiple registries are independent. Pin addresses if registry availability is a concern. |
| `vectis-log` | 1 | Not active/active | Durable log files and active stream buffers are local to the service. Multiple instances do not share run logs without external storage/routing. |
| `vectis-cron` | 1 | Not without coordination | Schedule claiming helps during a firing attempt, but Vectis does not provide a cron leader-election or sharding contract. Avoid uncoordinated duplicates. |
| `vectis-reconciler` | 1 | Limited, not recommended as default | Duplicate handoff is usually guarded by worker run claims, but duplicate repair traffic is still operational noise. Use one active reconciler unless you have tested the behavior. |
| `vectis-log-forwarder` | One per owner | Yes, by ownership | Safe when each forwarder owns its own socket and spool path. Do not share one spool directory as if it were a cluster. |
| `vectis-cli` | N/A | Yes | One-shot client. Concurrent commands rely on API and database semantics. |

## Worker Scale-Out

Workers are where horizontal scale is most natural.

When adding workers, check:

| Area | Why it matters |
| --- | --- |
| Database pool | Every worker uses database connections for claim, lease renewal, and final status. Pool limits are per process. |
| Queue throughput | More workers increase dequeue, ack, and redelivery pressure. |
| Log service capacity | Every running job streams logs before and during execution. |
| Workload isolation | Shell and checkout actions consume host/container CPU, memory, disk, and network. |
| Worker-control reachability | Remote cancel depends on resolving the assigned worker's control address. |

Each worker runs one job at a time today. To increase parallel job throughput, add workers rather than expecting one worker to run multiple jobs concurrently.

## API Replicas

API replicas are mostly stateless, but not perfectly interchangeable.

| Behavior | Operator impact |
| --- | --- |
| Rate limits are in-memory per API process. | Adding replicas raises the effective limit unless an external limiter or sticky routing is used. |
| SSE streams are long-lived connections. | Clients should reconnect when a replica restarts or a load balancer moves them. |
| Setup and admin writes use the shared database. | Database constraints remain the source of truth. |
| Trigger requests can return `202` before queue handoff is complete. | Keep `vectis-reconciler` healthy so durable queued runs are redispatched if an API exits after accepting them. |

Put each API replica behind `/health/ready`, not just process liveness.

## Singleton Services

These services should usually remain singleton in the current architecture:

| Service | Why |
| --- | --- |
| Queue | There is no shared distributed queue or multi-writer queue protocol. Queue persistence belongs to one active queue instance. |
| Log service | Run log storage and active stream buffers are process-local. |
| Cron | Vectis does not yet provide a leader-election or schedule-sharding deployment contract. |
| Reconciler | Duplicate repair handoff is tolerable for persisted runs but can add noise; one active reconciler is the safe default. |

Registry is also commonly singleton. Gossip-based registry HA is an advanced configured posture, not something you get by starting extra registry processes with independent state. You can also reduce registry importance by pinning queue and log addresses. See [Configuration](../configuration.md#service-discovery-vs-fixed-addresses).

## Restart Behavior

| Binary | What happens on restart | Operator expectation |
| --- | --- | --- |
| `vectis-api` | Stops accepting new HTTP requests and gives in-flight requests/SSE streams a bounded drain window. Detached enqueue work is not joined. | Use readiness checks and client retries. Keep reconciler running for accepted runs that missed queue handoff. |
| `vectis-queue` | Reloads pending and in-flight delivery metadata when persistence is enabled. Without persistence, in-memory queue state is lost. | Use one active queue and persistent storage for planned restarts. Watch queue depth, delivery age, and reconciler repair. |
| `vectis-registry` | A single registry loses in-memory registrations and must receive fresh registrations. In a configured gossip cluster, peers can retain converged entries within lease and tombstone behavior. | Restart before dependents when possible, keep HA registry membership stable during planned restarts, or pin addresses for critical paths. |
| `vectis-log` | Ingest and log streams are interrupted. Durable log files remain if storage is preserved; stream buffers are process-local. | Restart during a quiet window when possible. Workers need log service availability before executing runs. |
| `vectis-worker` | On `SIGINT` or `SIGTERM`, stops dequeuing and lets the current job continue toward finalization. Abrupt death relies on leases, queue delivery timeout, and repair. | Roll workers gradually. Use graceful termination windows long enough for normal jobs, or expect long jobs to rely on lease/reconciler behavior after hard stops. |
| `vectis-log-forwarder` | Local spool preserves unsent batches when configured and writable. | Preserve spool storage and watch age/size. |
| `vectis-cron` | Schedule scans pause while cron is down. Missed evaluations are not replayed from a separate HA log. | Keep one active cron. Avoid overlap unless you intentionally partition schedules. |
| `vectis-reconciler` | Repair scans pause while down. Queued runs remain in the database. | Repair latency can increase by at least one reconciler interval during restart. |
| `vectis-local` | Stops or restarts the supervised local stack. | Development-only behavior. |

## Probes And Traffic Gates

Readiness should answer "should this receive new work?" Liveness should answer "should the supervisor restart this process?"

| Component | Liveness | Readiness / traffic gate |
| --- | --- | --- |
| API | `GET /health/live` | `GET /health/ready`, which checks database ping and managed queue connectivity. |
| Queue, registry, log gRPC | Standard gRPC health service | Standard gRPC health service returning `SERVING`. |
| Worker | Supervisor process state | Gate externally on database, queue, and log availability; there is no worker HTTP readiness endpoint. |
| Cron | Supervisor process state | Gate on database and queue reachability before relying on schedules. |
| Reconciler | Supervisor process state | Gate on database and queue reachability before relying on repair. |
| Log-forwarder | Supervisor process state | Gate on log service reachability and local spool health. |
| Metrics listeners | `/metrics` shows exporter/process visibility | Do not use `/metrics` as workflow readiness by itself. |

## Planning A Restart

For planned maintenance:

1. Confirm `vectis-cli health check --strict` is clean or understand existing warnings.
2. Confirm queue persistence and log storage are on durable writable paths.
3. Stop or roll cron first if you need to prevent new scheduled work.
4. Roll API replicas behind readiness checks.
5. Restart queue, registry, or log during a quiet window when possible.
6. Roll workers gradually and allow graceful drains.
7. Keep the reconciler running, or restart it last if queue handoff repair matters during the change.
8. Afterward, check queue backlog, queued-run age, worker outcomes, log reachability, and audit/DB pool warnings.

For repair steps, see [Repair Runbooks](../reliability/repair-runbooks.md).

## Current Limits

| Area | Current limit |
| --- | --- |
| Database | One configured logical database and schema. No in-app database failover, read-replica routing, or cross-site replication. |
| API rate limits | In-memory per API process. |
| Queue | One active queue endpoint. Persistence is local to that queue. |
| Logs | One active log service unless you add external storage/routing. |
| Cron | No built-in leader election or schedule partitioning contract. |
| Reconciler | One active reconciler is the documented safe default. |
| Workers | Scale is bounded by DB pool sizing, queue throughput, log capacity, and workload resource isolation. |

## Related Documentation

| Topic | Document |
| --- | --- |
| Failure behavior | [Failure Domains](../../concepts/failure-domains.md) |
| Configuration and pool sizing | [Configuration](../configuration.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Runbooks and alerts | [Runbooks](../reliability/runbooks.md) |
| Repair procedures | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
