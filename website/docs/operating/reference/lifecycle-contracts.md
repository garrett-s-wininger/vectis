# Service Lifecycle Contracts

This page defines the current startup, readiness, shutdown, and state-preservation
contracts for Vectis daemons. It is the operator-facing baseline for restarts,
rollouts, and supervisor configuration.

`vectis-cli` is a one-shot client and is intentionally excluded from the daemon
matrix.

## Shared Rules

- Daemon roots use `internal/cli.ExecuteWithShutdownSignals`, which converts
  `SIGINT` and `SIGTERM` into command context cancellation.
- Startup preflight failures exit before the daemon accepts workflow work.
- DB-backed daemons use `OpenReadyDBForRole`, so they wait for schema migrations
  to be applied instead of applying migrations at runtime.
- HTTP daemons use bounded shutdown. API readiness flips unhealthy before the
  HTTP listener finishes draining.
- gRPC daemons using `internal/cli.ServeGRPC` mark the supplied health service
  `NOT_SERVING`, attempt `GracefulStop`, and force `Stop` after the helper
  timeout.
- Metrics listeners prove exporter reachability only. Do not use `/metrics` as a
  workflow readiness gate.
- `vectis-local` is a process supervisor for development. It has extra child
  process signal handling and is not a production daemon contract.

## Network Services

| Binary | Startup preflight | Readiness or serving gate | Drain behavior | State closed or preserved on exit | Hard-stop fallback |
| --- | --- | --- | --- | --- | --- |
| `vectis-api` | Validates client gRPC TLS config, waits for the global DB schema, initializes auth/cache/audit, and connects to orchestrator, queue, and log dependencies. | `GET /health/live`; `GET /health/ready` checks drain state, DB ping, and managed queue connectivity. | Marks readiness unhealthy, stops accepting new HTTP requests, and drains in-flight HTTP/SSE for the API shutdown window. Detached enqueue work is not joined. | Closes DB, metrics, tracer, cache, audit buffer, and gRPC clients. SQL state persists; in-process sessions or rate limits persist only when backed by the configured shared cache. | HTTP shutdown timeout in the API server; supervisor kill after its own grace window. |
| `vectis-cell-ingress` | Validates client gRPC TLS config, waits for the cell DB schema, opens queue routing, and starts local execution repair. | Private HTTP readiness through `/health/ready`; API cell status also probes configured cell ingress endpoints. | Bounded HTTP shutdown with the shared helper; repair loop stops on context cancellation. | Closes DB, metrics, tracer, queue clients, and HTTP listener. Accepted execution records remain in the cell DB. | Shared HTTP shutdown timeout configured by the command call site. |
| `vectis-queue` | Validates queue service gRPC TLS config, opens the listener, initializes metrics, opens optional WAL/snapshot persistence, and registers with registry when enabled. | Standard gRPC health service `queue` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting new gRPC calls, waits for active RPCs, then forces stop after the gRPC helper timeout. | Closes queue persistence. Pending, in-flight, and dead-letter state survives only when persistence is enabled and the shard keeps its instance ID and persistence directory. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-registry` | Validates registry gRPC TLS config, starts TLS reload, opens the listener, and starts optional gossip registry state. | Standard gRPC health service `registry` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting new gRPC calls and drains with the shared gRPC helper. | Single-node registrations are process-local and must be republished by heartbeats after restart. Gossip state follows registry cluster behavior. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-log` | Opens and locks durable log storage, applies disk-pressure read-only policy, initializes metrics, validates log service gRPC TLS, and registers with registry when enabled. | Standard gRPC health service `log` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting new gRPC calls and drains active streams with the shared gRPC helper. | Closes local run log store and metrics. Durable log files remain when storage is preserved; stream buffers are process-local. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-artifact` | Opens and locks durable artifact storage, applies disk-pressure read-only policy, initializes metrics, validates artifact service gRPC TLS, and registers with registry when enabled. | Standard gRPC health service `artifact` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting new gRPC calls and drains active uploads/reads with the shared gRPC helper. | Closes local artifact store and metrics. Content-addressed blobs remain when storage is preserved. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-orchestrator` | Validates orchestrator gRPC TLS config, initializes tracing/metrics, opens the listener, and starts the hot-state choreography service. | Standard gRPC health service `orchestrator` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting new gRPC calls and drains active worker choreography calls with the shared gRPC helper. | Closes the orchestration service. Hot run graphs and claim tokens are process-local; workers can reload missing hot state from durable task rows after restart. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-secrets` | Validates secrets gRPC TLS config, waits for the cell DB schema, configures encryptedfs/provider policy, and opens the listener. | Standard gRPC health service `secrets` reports `SERVING`; during shared shutdown it flips `NOT_SERVING`. | Stops accepting secret resolution RPCs and drains with the shared gRPC helper. | Closes DB, metrics, tracer, and provider resources. Secret material persistence depends on the configured provider, such as encryptedfs storage. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-worker-core` | Resolves the Unix socket path, validates execution backend config, creates the core service, and opens the UDS listener. | Supervisor process state plus worker startup validation through `Describe`; no standalone HTTP readiness endpoint. | Stops accepting worker-core gRPC calls with the shared gRPC helper and removes the socket path on exit. | Socket file is removed. Active execution outcome depends on the core/backend and what the paired worker can prove. | Shared gRPC helper timeout, then forced `Stop`. |
| `vectis-docs` | Builds the docs HTTP handler from embedded files or an override directory, validates host/TLS options, and opens the listener. | `GET /health/live`; docs has no workflow readiness dependency. | Bounded HTTP shutdown with the shared helper. | No workflow state. Embedded or configured static files remain outside the process. | Docs HTTP shutdown timeout, then supervisor kill. |

## Background And Sidecar Daemons

| Binary | Startup preflight | Readiness or serving gate | Drain behavior | State closed or preserved on exit | Hard-stop fallback |
| --- | --- | --- | --- | --- | --- |
| `vectis-worker` | Validates worker gRPC TLS, waits for the cell DB schema, validates worker-core capabilities, connects queue/orchestrator/log/artifact/secrets dependencies, and opens worker-control when configured. | Supervisor process state plus dependency gates; no HTTP readiness endpoint. Metrics expose worker lifecycle state. | Stops dequeuing on command context cancellation and lets the active delivery continue toward lease renewal, finalization, and queue ack. | Closes DB, metrics, tracer, worker-control listener, log forwarder, and clients. Active run ownership is protected by DB leases, queue delivery TTL, and reconciler repair after abrupt death. | Supervisor grace period is the hard boundary; long tasks may rely on lease expiry and repair after a hard stop. |
| `vectis-log-forwarder` | Validates client gRPC TLS, acquires a local lock, opens the producer UDS, initializes metrics, resolves the log client, and opens or creates spool storage. | Supervisor process state plus log reachability and local spool health; no HTTP readiness endpoint. | Stops the socket server, asks the forwarder to shut down, and waits for goroutines up to `--shutdown-timeout`. | Releases the lock and closes metrics/socket resources. Unsent chunks survive in the spool when the spool path is durable and writable. | `--shutdown-timeout`, then process exit with pending work left for the spool. |
| `vectis-spiffe` | Initializes or loads CA/bundle material, prepares runtime sockets, and starts the local Workload API and Entry API authority. `--init-only` exits after material initialization. | Supervisor process state and socket availability; no HTTP readiness endpoint. | Stops on command context cancellation and closes authority resources. | CA and bundle material remain under the configured data directory; runtime sockets are process-owned. | Supervisor grace period. |
| `vectis-cron` | Validates client gRPC TLS and cell-ingress HTTP mTLS, waits for the global DB schema, resolves action descriptors, and connects to queue. | Supervisor process state plus DB and queue reachability; no HTTP readiness endpoint. | Stops the polling loop on context cancellation. | Closes DB and queue dial resources. Schedule claims and created scheduled runs remain in SQL; duplicate handoff is guarded by DB idempotency. | Supervisor grace period; stale claims expire after the cron claim TTL. |
| `vectis-catalog` | Validates metrics TLS, waits for the global DB schema, opens configured cell fan-in DBs after migration readiness, and initializes metrics/tracing. | Supervisor process state plus DB/fan-in reachability; no HTTP readiness endpoint. | Stops catalog processing on context cancellation. | Closes global and fan-in DBs, metrics, and tracer. Pending catalog events remain in SQL for later retry. | Supervisor grace period; pending events retry after restart. |
| `vectis-reconciler` | Validates client gRPC TLS and cell-ingress HTTP mTLS, waits for the global DB schema, initializes metrics/tracing, and connects to queue. | Supervisor process state plus DB and queue reachability; no HTTP readiness endpoint. Active repair ownership is visible through the service lease. | Stops after the current reconcile pass observes context cancellation. | Closes DB, queue pool, metrics, and tracer. Queued runs and repair candidates remain in SQL. | Supervisor grace period; active/passive takeover waits for service lease expiry plus the next poll. |
| `vectis-local` | Builds the requested local profile, prepares local TLS/SPIFFE material when enabled, starts child processes in dependency stages, and waits for configured child gRPC health checks. | Development supervisor health is the health of its child stack. It waits for key child gRPC health checks during startup. | On signal, sends `SIGTERM` to started child process groups, waits, then escalates to `SIGKILL` for remaining children. | Child service state follows each child daemon's own contract. Local generated TLS/SPIFFE/config material remains on disk. | Supervisor child shutdown timeout, then `SIGKILL`. |

## Probe Guidance

| Component | Liveness | Readiness / work gate |
| --- | --- | --- |
| API | `GET /health/live` | `GET /health/ready` |
| Cell ingress | Process and listener state | Private `/health/ready` |
| Queue, registry, orchestrator, log, artifact, secrets | Standard gRPC health | Standard gRPC health returning `SERVING` for the service name in the matrix |
| Worker core | Process and UDS presence | Worker `Describe` startup validation |
| Worker, cron, catalog, reconciler, log-forwarder, spiffe | Supervisor process state | Gate externally on their dependencies and storage paths |
| Docs | `GET /health/live` | Same as liveness; no workflow dependency |
| Metrics listeners | Process/exporter visibility | Not a workflow readiness gate |

## Related Documentation

| Topic | Document |
| --- | --- |
| Restart and scale behavior | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
| Health check IDs | [Health Check Catalog](./health-check-catalog.md) |
| Production monitoring | [Production Monitoring Contract](../reliability/production-monitoring.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Failure domains | [Failure Domains](../../concepts/failure-domains.md) |
