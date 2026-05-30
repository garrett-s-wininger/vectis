# Failure Domains

This page helps operators understand what happens when a Vectis dependency or service is unavailable. Use it for deployment planning, readiness checks, and incident triage.

For the system layout, see [Architecture](./architecture.md). For security and trust boundaries, see [Security](./security.md). For backup and restore order, see [Backup and Restore](../operating/reliability/backup-restore.md). For queue handoff repair, see [Dispatch Visibility](../operating/reliability/dispatch-visibility.md).

## The Short Version

The database and queue are the two services to protect first.

| Service | Why it matters |
| --- | --- |
| Database | Durable source of truth for jobs, runs, schedules, leases, users, tokens, namespaces, audit records, and idempotency keys. |
| Queue | Handoff point between producers such as API, cron, and reconciler, and consumers such as workers. |
| Log service | Required before a worker starts normal job execution. It owns log ingest and log streaming, not authoritative run state. |
| Registry | Service discovery. It is avoidable for some paths when queue and log addresses are pinned in config. |
| API | User and automation entry point. Running work does not depend on the API once workers have claimed it. |
| Worker | Executes jobs. Capacity and failure handling are mostly worker-count and lease behavior questions. |
| Cron | Turns schedules into queued work. Manual and API triggers can still work without it. |
| Reconciler | Repairs the gap where a run is durable in the database but was not successfully handed to the queue. |

Most Vectis outages reduce to one of these questions:

1. Can the process start and find its hard dependencies?
2. Can the API, cron, or reconciler record state in the database?
3. Can work reach the queue?
4. Can workers claim work, open a log stream, and finalize run state?
5. Can users read status and logs while the system recovers?

## Dependency Map

| Component | Runtime dependencies |
| --- | --- |
| `vectis-api` | Database; queue by pinned address or registry; log service for log routes. |
| `vectis-queue` | Optional registry when it registers its address; persistence directory when queue persistence is enabled. |
| `vectis-registry` | Listen socket and optional TLS files. |
| `vectis-log` | Storage directory; gRPC ingest listener; HTTP/SSE log listener; optional registry. |
| `vectis-worker` | Database; queue; log service; registry unless queue and log addresses are pinned. |
| `vectis-log-forwarder` | Log service and local spool directory. |
| `vectis-cron` | Database and queue. |
| `vectis-reconciler` | Database and queue. |
| `vectis-local` | Child binaries, local ports, and local TLS bootstrap unless disabled. |
| `vectis-cli` | API for normal commands; database DSN for `migrate`. |

If queue and log addresses are pinned, callers can avoid registry lookups. Services that register themselves still need the registry during startup unless you choose a different deployment pattern.

## Dependency Classes

| Class | Meaning |
| --- | --- |
| Startup-hard | The process normally exits or refuses to serve until the dependency is reachable or configured. |
| Runtime-hard | The process may start, but the main workflow cannot complete while the dependency is down. |
| Runtime-soft | The process can continue, retry, or degrade for a period. |
| Optional | Used only when the matching feature or deployment pattern is enabled. |

## Startup and Readiness {#startup-and-recovery-matrix}

| Binary | Must be healthy before startup or serving | Main runtime dependency | Readiness guidance |
| --- | --- | --- | --- |
| `vectis-api` | Database, expected schema, queue dial, required TLS files. | Database for REST state; queue for dispatch; log service for log routes. | `GET /health/live` means the process is serving. `GET /health/ready` checks database ping and managed queue connectivity. |
| `vectis-queue` | gRPC listener, persistence directory when enabled, required TLS files, registry when registration is enabled. | Queue storage and delivery scanner. | Use gRPC health service `queue`; scrape metrics for depth and delivery health. |
| `vectis-registry` | gRPC listener and required TLS files. | In-memory discovery state. | Use gRPC health service `registry`. |
| `vectis-log` | gRPC listener, HTTP log listener, storage directory, required TLS files, registry when registration is enabled. | Durable log storage and active stream buffers. | Use gRPC health service `log` for ingest; check log HTTP separately for clients reading streams. |
| `vectis-worker` | Database, queue, log service, required TLS files. | Database leases/finalization; queue dequeue/ack; log stream before execution. | Use supervisor state plus dependency gates. There is no worker HTTP readiness endpoint. |
| `vectis-log-forwarder` | Log service, required TLS files, local spool directory. | Log service for draining batches. | Use process supervision plus spool size and age. |
| `vectis-cron` | Database, queue, required TLS files. | Database schedules and queue enqueue. | Gate scheduled traffic on database and queue reachability. |
| `vectis-reconciler` | Database, queue, required TLS files. | Database queued-run scan and queue enqueue. | Gate reliance on redispatch on database and queue reachability. |
| `vectis-local` | Child binary paths, local ports, and local TLS bootstrap unless disabled. | Supervised dev stack children. | Intended for local development; it starts services in dependency order. |
| `vectis-cli` | API URL for API commands, database DSN for `migrate`. | Only the dependency needed for the invoked command. | One-shot command exit status is the signal. |

Readiness should answer "should this process receive new work right now?" Liveness should only answer "should the supervisor restart this process?"

## Database Down {#database}

The database is the durable source of truth. If it is unavailable, Vectis can lose the ability to know what should run, who owns a run, and how to report final state.

| Component | Behavior |
| --- | --- |
| API | Startup can fail. After startup, many routes return `503 Service Unavailable` for classified transient database errors, while preserving `404` behavior for true missing resources. Creating jobs, updating jobs, triggering runs, listing runs, auth setup, and audit persistence are affected. |
| Worker | Cannot claim work, renew leases, or reliably mark runs succeeded, failed, or orphaned. |
| Cron | Cannot read schedules or record activity. Startup normally fails if the database is unreachable. |
| Reconciler | Cannot find queued runs that need redispatch. Startup normally fails if the database is unreachable. |

Vectis ships embedded migrations and supports SQLite and PostgreSQL. It does not implement database failover, replica routing, or automatic backup and restore. PostgreSQL connection pool limits are configurable, but they are not a high-availability system by themselves.

## Queue Down

The queue buffers work between producers and workers.

| Component | Behavior |
| --- | --- |
| API | Trigger and ephemeral-run requests can return `202` after recording the run in the database. Queue handoff happens asynchronously with bounded retries. If handoff keeps failing, the run may remain queued in the database until the reconciler submits it again. |
| Worker | Dequeue fails. The worker backs off and retries rather than exiting immediately. |
| Cron | A schedule tick can fail to submit during the outage. Later ticks continue according to schedule behavior. |
| Reconciler | Redispatch attempts fail for that cycle and can retry later. |

Queue persistence changes restart behavior:

| Queue persistence | Restart effect |
| --- | --- |
| Enabled | Pending work and in-flight delivery metadata can be reloaded from disk. The storage path must be treated as durable state. |
| Disabled | In-memory queue state is lost. Runs may still be queued in the database, so the reconciler is the recovery path. |

Run the reconciler and alert on persistent queued-run age if queue handoff matters for your deployment.

## Log Service Down

The log service collects worker log chunks and serves log streams to clients. It does not own authoritative run status.

| Component | Behavior |
| --- | --- |
| Worker | Must open a log stream before normal job execution. If the log service is down or does not respond in time, the run fails before meaningful job steps execute. |
| API clients | Cannot read live or stored logs through the normal log paths until the service is back. |
| API status routes | Can still report run state from the database when the API and database are healthy. |

Today, there is no "run without central logging" mode for normal worker execution.

## Registry Down

The registry matters when services use discovery instead of fixed addresses.

| Component | Behavior |
| --- | --- |
| API, cron, reconciler | Usually cannot start if they need the registry to resolve the queue. |
| Worker | Usually cannot start if it needs the registry to resolve queue or log addresses. |
| Queue and log | Startup fails when they are configured to register and the registry is unavailable. |

Pin queue and log addresses when you want fewer startup dependencies. Keep the registry private when discovery is enabled.

## API Down {#vectis-api}

The API is the HTTP entry point for users, automation, run history, and log access.

| Impact | Behavior |
| --- | --- |
| New user requests | Clients cannot create jobs, trigger runs, query status, or read logs through the API. |
| Already running jobs | Workers do not need the API once work is claimed. Queue, log, cron, and reconciler continue according to their own dependencies. |
| Shutdown | On `SIGINT` or `SIGTERM`, the API stops accepting new HTTP connections and uses `http.Server.Shutdown` with a bounded timeout. |

Use `/health/live` as a restart probe and `/health/ready` as the traffic gate. Do not use `/metrics` as API readiness; metrics can still be served when database or queue dependencies are unhealthy.

Background enqueue after an HTTP `202` uses the reconciler as a backstop if the process exits before queue handoff completes. See [ADR 0001](../developing/architecture-decisions/0001-async-enqueue-after-http-202.md).

## Workers Down or Interrupted

Workers execute jobs and coordinate ownership through database leases.

| Event | Behavior |
| --- | --- |
| All workers offline | Work can remain queued. Throughput returns when workers return. |
| Worker overloaded | Queue depth and queued-run age grow. Add workers or reduce incoming work. |
| `SIGINT` or `SIGTERM` | Worker stops dequeuing new work and tries to let the current job finish and finalize state. |
| `SIGKILL` or crash | No graceful drain. Leases, queue delivery timeouts, and reconciler behavior determine whether work is retried or stuck. |
| Database loss mid-run | Lease renewal and final status updates can fail. A long outage can strand or fail runs until recovery or operator action. |
| Remote cancel | `POST /api/v1/runs/{id}/cancel` and `vectis-cli runs cancel <run-id>` can request cancellation of a currently running run. The API resolves the lease owner to the worker-control endpoint, sends the run's cancel token over gRPC, and the worker cancels the execution context. |

Scale workers by running more worker processes. Remote cancel is available for executing runs when worker resolution is configured and the assigned worker is reachable. It is best-effort at the action boundary: actions that honor context cancellation should stop promptly, while external child processes or blocking operations may need their own cleanup behavior.

## Cron Down

Cron turns schedules into queued work. It is independent of the HTTP API.

| Situation | Behavior |
| --- | --- |
| Cron offline | Schedules do not fire. Manual and API triggers can still work if API and queue are healthy. |
| Multiple uncoordinated cron instances | The same schedule can double-fire. |
| Large schedule set | Partition schedules across cron groups or use an external scheduler to trigger the API. |

Vectis does not currently ship cron sharding or leader election. Each schedule should have one firing path at a time.

## Reconciler Down

The reconciler repairs a specific reliability gap: a run is durable in the database as queued, but it was not successfully handed to the queue.

If all reconciler instances are down, that automatic repair stops. The API and queue do not fully replace it. Dispatch repair attempts are visible in run dispatch events; see [Dispatch Visibility](../operating/reliability/dispatch-visibility.md).

The reconciler runs on a configurable interval and waits a configurable minimum age before redispatching, so it does not fight normal dispatch latency.

Multiple reconcilers can run in one execution cell. They coordinate through a database-backed service lease, so one instance performs scans while other instances stand by. If the lease holder exits or loses database access, another instance can take over after the lease expires.

## Probe Reference

| Surface | Liveness | Readiness |
| --- | --- | --- |
| API HTTP | `GET /health/live` | `GET /health/ready` checks database ping and managed queue connectivity. |
| Registry gRPC | Standard gRPC health `registry` | Same check; discovery clients should wait for `SERVING`. |
| Queue gRPC | Standard gRPC health `queue` | Same check; producers and workers should wait for `SERVING`. |
| Log gRPC | Standard gRPC health `log` | Same check for log ingest. Check log HTTP separately for users reading streams. |
| Metrics-only listeners | `/metrics` confirms exporter/process visibility. | Do not use metrics as workflow readiness by itself. |
| Cron, reconciler, worker, log-forwarder | Supervisor process state. | Gate externally on hard dependencies or deployment-specific wrapper probes. |

For reference deploys, add probes in dependency order: registry, queue, and log gRPC health first; API HTTP live/ready second; worker, cron, reconciler, and log-forwarder dependency gates last.

## Current Limits

| Area | Current behavior | Stronger production expectation |
| --- | --- | --- |
| Database | One configured SQL backend, embedded migrations, no in-app failover. | Managed PostgreSQL, tested backups, restore drills, and deployment-level HA. |
| Queue | One active queue service with optional disk persistence. | Durable storage, queue-depth alerts, and capacity planning. |
| Registry | Commonly a single discovery point unless addresses are pinned. | Redundant discovery or fixed service addresses. |
| Log service | Required before normal job execution. | Replication or local buffering if log availability must not block work. |
| API | Health probes, graceful HTTP shutdown, auth when enabled, async enqueue backstopped by reconciler. | Edge TLS, idempotent clients, multiple replicas, and alerts on enqueue or reconciler failures. |
| Worker | Graceful drain on `SIGINT` and `SIGTERM`; no drain on crash or `SIGKILL`. | Worker isolation, bounded drain policy, and clear operator run-stop procedures. |
| Cron | No built-in sharding or leader election. | One firing path per schedule, either by leader election, partitioning, or an external scheduler. |
| Auth | HTTP API auth is configurable and off by default. | Enable auth or protect the API at the edge before shared use. |
