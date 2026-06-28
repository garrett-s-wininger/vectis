# Scaling And Restarts

This page explains how far you can scale each Vectis component today, which services should stay singleton, and what to expect when restarting them.

The current posture is intentionally conservative: Vectis is safest as a single-site deployment with one shared SQL database, one orchestrator, one or more queue shards with separate persistence, one or more run-sharded log services with separate storage, one or more artifact shards with separate storage, DB-coordinated cron replicas, one active reconciler lease holder, and as many workers as the orchestrator, database, queue, log service, and artifact service can comfortably support.

For the supported production-oriented starting shape, see [Production Topology v1](./production-topology-v1.md). For dependency behavior during outages, see [Failure Domains](../../concepts/failure-domains.md). For per-daemon startup, readiness, drain, and hard-stop behavior, see [Service Lifecycle Contracts](../reference/lifecycle-contracts.md). For reference deployment boundaries, see [Reference Deployment Posture](./reference-deployment-posture.md). For database pool sizing, see [Configuration](../configuration.md#postgresql-connection-pool-pgx-only).

This page answers "is this component topology supported, and what happens when it changes?" For workload pressure, saturation signals, and when to revalidate a larger operating point, see [Capacity And Load Envelope](../capacity/capacity-load-envelope.md).

## Quick Guidance

| Component | Scale guidance |
| --- | --- |
| API | Can be replicated for stateless HTTP traffic with caveats. Keep the reconciler healthy. |
| Workers | Primary safe scale-out unit. Add workers to increase job throughput. |
| Orchestrator | Keep one active orchestrator for now; it owns in-memory run graphs and task claim fencing. Workers can recover missing hot state after restart from durable task rows, but active/active orchestrators are not supported. |
| Worker cores | Pair cores with workers by default. Sharing one core across multiple active workers is only for deliberately tested local/provider topologies. |
| Queue | Run one or more independent queue shards through registry discovery. Each shard owns one stable instance ID and one persistence directory. |
| Registry | Run one registry by default, configure gossip clustering deliberately, and list multiple registry addresses on clients when using HA registry nodes. |
| Log service | Run one or more log shards through registry discovery. DB-aware clients persist the run's shard assignment; each shard owns one stable instance ID and one storage directory. |
| Artifact service | Run one or more artifact shards through registry discovery. Each shard owns one stable instance ID and one storage directory. |
| Cron | Multiple cron instances may run against one shared database cell; schedule claims and scheduled-fire idempotency coordinate them. |
| Reconciler | Multiple instances may run as active/passive standbys through the database service lease. |
| Docs | Static docs service. Run zero, one, or more depending on how you expose documentation. |
| Log-forwarder | One per producer host or process group, each with its own socket/spool path. |
| `vectis-local` | Development only. Do not treat it as a production control plane. |

## Local HA Exercise Profiles

Use these profiles when you want to test the single-cell HA contract without hand-starting every process:

| Tool | Command | Shape |
| --- | --- | --- |
| `vectis-local` | `vectis-local --profile ha` | Starts a local multi-process cell with three registries, one orchestrator, two queue shards, two log shards, two artifact shards, two API replicas, two workers, two cron instances, and two reconcilers. |
| Podman reference deployment | `vectis-cli deploy podman --profile ha up` | Renders and starts the same HA exercise shape inside the reference Podman pod, backed by the bundled Postgres and persistent queue/log/artifact volumes. |

The default profile remains `simple` for both tools. HA profiles are intended for local validation of registry failover, queue/log/artifact sharding, cron claims, reconciler lease takeover, worker drain behavior, and API replica behavior.

## Replica Matrix

| Binary | Safe default | Can run multiple? | What to watch |
| --- | ---: | --- | --- |
| `vectis-api` | 1 | Conditional | Multiple API replicas can serve HTTP against the same database and queue. The default rate-limit backend is in-process per replica, SSE clients must reconnect through load balancers, and accepted-but-not-enqueued runs rely on the reconciler if an API exits after `202`. |
| `vectis-worker` | N | Yes | Each worker executes one task delivery at a time. Orchestrator claims and leases guard task executions against duplicate execution even if queue handoff is duplicated. Size orchestrator capacity, DB pools, queue delivery timeouts, and log capacity for the fleet. |
| `vectis-orchestrator` | 1 | Not yet | The orchestrator owns in-memory run graphs, claim tokens, and task continuation state. Workers can hydrate and reclaim missing state after a singleton restart, but run one active instance until per-run sticky routing or replicated state exists. |
| `vectis-worker-core` | Paired with workers | Yes | The core speaks to workers over a Unix-domain socket. Run it as a sidecar, sibling process, or supervisor child with a shared runtime directory. Do not point multiple active workers at one mutable core socket unless the core is deliberately designed and tested for that multiplexing model. |
| `vectis-queue` | 1+ | Yes, as independent shards | Producers discover queue registrations and pick a shard. Workers dequeue across the pool and ack the shard encoded in the delivery ID. Do not duplicate active instance IDs or share one persistence directory across active queue processes. |
| `vectis-registry` | 1 | Conditional | Single registry is the safe default. Clients can list multiple registry addresses and service registrations fail over between them, but gossip-based HA registry still requires every registry node to be configured with static cluster membership. Pin queue/orchestrator/log/artifact addresses if registry availability is a concern. |
| `vectis-log` | 1+ | Yes, as independent run shards | Clients discover log registrations and route a given `run_id` to one shard. DB-aware clients persist `job_runs.log_shard_id`; unassigned runs fall back to deterministic hashing. Each shard owns local durable log files and active stream buffers. Keep shard instance IDs stable; a second active process on the same storage directory refuses to start. |
| `vectis-artifact` | 1+ | Yes, as independent blob shards | Clients discover artifact registrations and can upload/read content-addressed blobs. Each shard owns local durable blob storage. Keep shard instance IDs stable; a second active process on the same storage directory refuses to start. |
| `vectis-cron` | 1+ | Yes, within one DB cell | Schedule claims select one firing attempt for a due row. Each claim records an instance ID and expires after `--claim-ttl` / `VECTIS_CRON_CLAIM_TTL` so another replica can retry after a crash. If a retry sees the same schedule tick, it reuses the existing run and may repeat queue handoff for that run. Watch DB pressure, queue reachability, clock skew, and schedule-to-run latency. |
| `vectis-reconciler` | 1 active | Yes, active/passive | Instances coordinate through the `service_leases` table. Only the lease holder scans and redispatches; standbys take over after the lease TTL. |
| `vectis-docs` | 1 | Yes | Static HTTP docs. It has no database or queue state; scale or disable it according to your exposure model. |
| `vectis-log-forwarder` | One per owner | Yes, by ownership | Safe when each forwarder owns its own socket and spool path. DB-aware workers stamp log shard hints into the socket protocol; the forwarder stays DB-free and preserves those hints through its spool. Do not share one spool directory as if it were a cluster. |
| `vectis-cli` | N/A | Yes | One-shot client. Concurrent commands rely on API and database semantics. |

## Worker Scale-Out

Workers are where horizontal scale is most natural.

When adding workers, check:

| Area | Why it matters |
| --- | --- |
| Orchestrator | Every worker uses it for task claim, lease renewal, completion, and fan-out decisions. |
| Database pool | Every worker still uses database connections for planned task visibility, catalog events, durable cancel polling, and repair writes. Pool limits are per process. |
| Queue throughput | More workers increase dequeue, ack, and redelivery pressure. |
| Log service capacity | Every running job streams logs before and during execution. Add log shards when a single shard's ingest, replay, or disk pressure becomes the limit. |
| Workload isolation | Shell and checkout actions consume host/container CPU, memory, disk, and network. |
| Worker-control reachability | Remote cancel uses worker-control as a fast path; durable cancel intent is still stored in the database and polled by the assigned worker. |
| Drain visibility | `vectis_worker_draining`, `vectis_worker_lifecycle_state`, `vectis_worker_db_unavailable`, and `vectis_worker_orchestrator_recoveries_total` show whether a worker is safely idle, still executing, finalizing DB state, blocked on database recovery, or recovering missing orchestrator hot state. |

Each worker runs one job at a time today. To increase parallel job throughput, add workers rather than expecting one worker to run multiple jobs concurrently.

During rolling restarts, wait for draining workers to report lifecycle state `idle` before forcing termination. A worker in `executing` or `finalizing` is still protecting an active task execution from unnecessary lease expiry and repair.

### Worker Core Topology

`vectis-worker` and `vectis-worker-core` should normally be scheduled as one failure domain:

| Shape | Fit |
| --- | --- |
| Same pod sidecar | Preferred for Kubernetes-style deployments. Share an `emptyDir` or equivalent runtime directory for `worker-core.sock` and `worker-core-shell.sock`; terminate the core with the worker. |
| Same host supervisor group | Good for systemd, Nomad, or process-supervisor deployments. Put both sockets in a private runtime directory owned by the worker service account. |
| Remote network service | Not the default contract. The current worker/core transport is a local Unix-domain socket boundary and assumes worker-owned shell callbacks are reachable only by the paired core. |

The worker validates the core's `Describe` response at startup. A core that does not advertise the current protocol version plus execute, cancel, log callback, and artifact callback capabilities is rejected before the worker connects to the queue.

Restart the pair carefully. If the core exits while a worker is executing, the worker records an execution result according to the returned or observed failure. Unknown external outcomes are marked orphaned with `worker_core_unknown` so operators can repair the run after checking the provider. If the worker exits first, normal run leases and queue redelivery decide repair.

## API Replicas

API replicas are mostly stateless, but not perfectly interchangeable.

| Behavior | Operator impact |
| --- | --- |
| Sessions and rate limits use the configured API cache backend; the default backend stores them in the shared SQL database. | Adding replicas preserves login sessions and one rate-limit budget unless `api.cache.backend` is changed to `memory`. |
| SSE streams are long-lived connections. | Clients should reconnect when a replica restarts or a load balancer moves them. |
| Setup and admin writes use the shared database. | Database constraints remain the source of truth. |
| Trigger requests can return `202` before queue handoff is complete. | The API records an `accepted` dispatch event before returning `202`; keep `vectis-reconciler` healthy so durable queued runs are redispatched if an API exits after accepting them. |

Put each API replica behind `/health/ready`, not just process liveness. Readiness returns `503` as soon as shutdown drain begins, before the HTTP listener finishes draining in-flight requests.

## Singleton Services

Registry is commonly singleton. Gossip-based registry HA is an advanced configured posture, not something you get by starting extra registry processes with independent state. Clients can fail over across multiple configured registry addresses, and services write registration heartbeats to one active sponsor at a time, but static registry cluster membership is still what makes registry nodes converge. You can also reduce registry importance by pinning queue, orchestrator, log, and artifact addresses. See [Configuration](../configuration.md#service-discovery-vs-fixed-addresses).

The orchestrator is currently a singleton hot-state service. Workers can reload a missing run graph from durable task rows and reclaim their execution when a singleton orchestrator restarts, but that recovery path does not make multiple active orchestrators safe. Starting multiple active orchestrators behind the same non-sticky resolver can split `LoadRun`, claim, renew, and complete calls for the same run across different in-memory states. Keep one active orchestrator until sticky routing or replicated orchestrator state is available.

Each queue shard remains single-writer for its own WAL and snapshot files. Queue scale-out comes from adding shards, not from starting multiple active processes on the same queue persistence path. `vectis-queue` takes an advisory lock on the persistence directory and refuses to start when another process already owns it.

Each log shard remains single-writer for its own local run log files. Log scale-out comes from adding shards, not from starting multiple active processes on the same log storage path. `vectis-log` takes an advisory lock on the storage directory and refuses to start when another process already owns it. If a log shard falls below `--storage-read-only-min-free-bytes`, it advertises read-only for new runs and rejects the first log append for new unassigned runs while continuing to serve stored logs and appends for runs that already have files on that shard.

Each artifact shard remains single-writer for its own local blob storage. Artifact scale-out comes from adding shards, not from starting multiple active processes on the same artifact storage path. `vectis-artifact` takes an advisory lock on the storage directory and refuses to start when another process already owns it. If a shard falls below `--storage-read-only-min-free-bytes`, it advertises read-only for new blobs and rejects uploads for blobs it does not already store while continuing to serve existing blobs.

Cron scale-out is active/active within one shared database cell. Replicas race on `cron_trigger_specs` claims; only the winner creates or reuses the scheduled run for that due tick. Set `--instance-id` / `VECTIS_CRON_INSTANCE_ID` to make claim ownership readable in the database and logs. Duplicate cron instance IDs do not weaken the database exclusion, but they make ownership harder to diagnose.

## Restart Behavior

| Binary | What happens on restart | Operator expectation |
| --- | --- | --- |
| `vectis-api` | Marks readiness unhealthy, stops accepting new HTTP requests, and gives in-flight requests/SSE streams a bounded drain window. Detached enqueue work is not joined. | Use readiness checks and client retries. Watch `accepted` dispatch events and `vectis_api_run_enqueue_total` outcomes. Keep reconciler running for accepted runs that missed queue handoff. |
| `vectis-queue` | Reloads pending and in-flight delivery metadata for that shard when persistence is enabled. Without persistence, in-memory queue state for that shard is lost. | Keep each shard on a stable `--instance-id` and durable, separate `--persistence-dir`; when omitted, defaults are derived from `--pool` and `hostname-port`. Watch per-shard depth, delivery age, and reconciler repair. |
| `vectis-orchestrator` | Loses in-memory loaded run graphs, active claim tokens, and continuation state. Workers reload missing run graphs from durable task rows, rehydrate task status snapshots, and reclaim active executions when claim, lease renewal, or completion observes missing hot state. | Restart during quiet windows when possible. Watch `vectis_worker_orchestrator_recoveries_total`, worker outcomes, and queue delivery behavior after restart. |
| `vectis-registry` | A single registry loses in-memory registrations and must receive fresh registrations. In a configured gossip cluster, peers can retain converged entries within lease and tombstone behavior. Clients can fail over to other configured registry addresses, and service heartbeats republish to surviving or recovered targets. | Restart before dependents when possible, keep HA registry membership stable during planned restarts, or pin addresses for critical paths. |
| `vectis-log` | Ingest and log streams for runs routed to that shard are interrupted. Durable log files remain if storage is preserved; stream buffers are process-local. | Keep each shard on a stable `--instance-id` and durable, separate `--storage-dir`; when omitted, defaults are derived from `hostname-port`. Workers need the owning log shard available before executing runs routed there. |
| `vectis-artifact` | Uploads and reads routed to that shard are interrupted. Durable blobs remain if storage is preserved. | Keep each shard on a stable `--instance-id` and durable, separate `--storage-dir`; when omitted, defaults are derived from `hostname-port`. |
| `vectis-worker` | On `SIGINT` or `SIGTERM`, stops dequeuing and lets the current task delivery continue toward finalization. Abrupt death relies on leases, queue delivery timeout, and repair. | Roll workers gradually. Use graceful termination windows long enough for normal task deliveries, or expect long deliveries to rely on lease/reconciler behavior after hard stops. |
| `vectis-worker-core` | Stops accepting worker-core RPCs. Active execution behavior depends on the core and execution backend. | Restart with its paired worker when possible. If the core is replaced independently, expect the worker to fail, cancel, or orphan active work based on the last provable core outcome. |
| `vectis-log-forwarder` | Local spool preserves unsent batches when configured and writable. | Preserve spool storage and watch age/size. |
| `vectis-cron` | Schedule scans pause if all cron instances are down. A crash after recording a scheduled run but before advancing the schedule can cause another instance to retry queue handoff for the same run. Missed evaluations are not replayed from a separate HA log. | Run one or more cron instances against the same database and queue. Gate them on database and queue reachability. |
| `vectis-reconciler` | The active lease holder stops scanning. Queued runs remain in the database. | With standby instances, repair resumes after lease expiry plus the next poll; without standbys, repair pauses until the process returns. |
| `vectis-docs` | Documentation is unavailable while the process is down. | No effect on run execution. Restart whenever needed. |
| `vectis-local` | Stops or restarts the supervised local stack. | Development-only behavior. |

## Probes And Traffic Gates

Readiness should answer "should this receive new work?" Liveness should answer "should the supervisor restart this process?"

| Component | Liveness | Readiness / traffic gate |
| --- | --- | --- |
| API | `GET /health/live` | `GET /health/ready`, which checks database ping and managed queue connectivity. |
| Queue, registry, orchestrator, log, artifact gRPC | Standard gRPC health service | Standard gRPC health service returning `SERVING`. |
| Worker | Supervisor process state | Gate externally on database, queue, orchestrator, and log availability; there is no worker HTTP readiness endpoint. |
| Worker core | Supervisor process state plus worker startup validation | The worker refuses to start queue consumption unless the configured core socket is reachable and advertises the required protocol/capabilities. |
| Cron | Supervisor process state | Gate on database and queue reachability before relying on schedules. |
| Reconciler | Supervisor process state | Gate on database and queue reachability before relying on repair; with replicas, ensure only one instance holds the service lease. |
| Log-forwarder | Supervisor process state | Gate on log service reachability and local spool health. |
| Metrics listeners | `/metrics` shows exporter/process visibility | Do not use `/metrics` as workflow readiness by itself. |

## Planning A Restart

For planned maintenance:

1. Confirm `vectis-cli health check --strict` is clean or understand existing warnings.
2. Confirm queue persistence and log storage are on durable writable paths.
3. Pause or roll all cron instances first if you need to prevent new scheduled work.
4. Roll API replicas behind readiness checks.
5. Restart queue, orchestrator, registry, log, or artifact during a quiet window when possible.
6. Roll workers gradually and allow graceful drains.
7. Keep the reconciler running, or restart it last if queue handoff repair matters during the change.
8. Afterward, check queue backlog, queued-run age, worker outcomes, `vectis_worker_orchestrator_recoveries_total`, log reachability, and audit/DB pool warnings.

For repair steps, see [Repair Runbooks](../reliability/repair-runbooks.md).

## Current Limits

| Area | Current limit |
| --- | --- |
| Database | One configured logical database and schema. No in-app database failover, read-replica routing, or cross-site replication. |
| API rate limits | Shared through the configured SQL database by default; `memory` remains available for per-process buckets. |
| Queue | Queue pool within one cell through registry discovery. Each shard has local persistence; no shared multi-writer queue storage. |
| Logs | Run-sharded local storage within one cell with DB-backed shard assignments for DB-aware clients. There is no shared multi-writer log storage or S3-backed archive yet. The standalone log-forwarder remains DB-free: it uses worker-provided shard hints when present and deterministic routing from discovered shards otherwise. Disk-pressure read-only mode rejects new run log files on a pressured shard. |
| Artifacts | Run-scoped artifact list, metadata, and download APIs backed by worker-originated uploads to content-addressed local storage shards. There is no replication, external object-storage backend, or public API upload path. Disk-pressure read-only mode rejects uploads for new blobs on a pressured shard. |
| Cron | DB-coordinated within one shared database cell; no built-in schedule partitioning across cells. |
| Reconciler | Active/passive within one database cell through `service_leases`; not a sharded repair pool yet. |
| Workers | Scale is bounded by DB pool sizing, queue throughput, log capacity, and workload resource isolation. |
| Worker cores | One core serves one worker today. Shared runtime-provider pools, resource sub-allocation, and remote provider autoscaling are future work. |

## Related Documentation

| Topic | Document |
| --- | --- |
| Failure behavior | [Failure Domains](../../concepts/failure-domains.md) |
| Production topology | [Production Topology v1](./production-topology-v1.md) |
| Service lifecycle contracts | [Service Lifecycle Contracts](../reference/lifecycle-contracts.md) |
| Configuration and pool sizing | [Configuration](../configuration.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Runbooks and alerts | [Runbooks](../reliability/runbooks.md) |
| Repair procedures | [Repair Runbooks](../reliability/repair-runbooks.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
