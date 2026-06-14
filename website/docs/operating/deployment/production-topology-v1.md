# Production Topology v1

This page defines the first production-oriented topology Vectis is willing to
document as an operating target. It is intentionally conservative: one site, one
logical PostgreSQL database, one active orchestrator, repair through the
reconciler, and scale-out through workers plus independent queue, log, and
artifact shards.

Use this page as the decision filter for production deployment work. If a shape
is outside this contract, treat it as experimental, staging-only, or
deployment-specific until it has its own documented operating contract.

## Supported Starting Shape

```text
HTTPS edge / load balancer
    |
vectis-api replica(s)
    |
PostgreSQL primary database
    |
single active vectis-orchestrator
    |
queue shard(s) -> vectis-worker + vectis-worker-core pairs
    |
log shard(s), artifact shard(s)
```

The default production v1 topology is a single execution cell. Multi-cell
deployments can be used for controlled production-like environments when whole
runs are routed to named cells, but multi-cell is not the default starting
shape.

## Core Requirements

| Area | Requirement |
| --- | --- |
| Site model | Run one single-site deployment. Do not treat Vectis production v1 as a multi-region active/active system. |
| Database | Use PostgreSQL for the production control plane and run migrations deliberately before service rollout. |
| API edge | Put `vectis-api` behind HTTPS, enable API auth, configure allowed Hosts, configure trusted proxy CIDRs, and block direct untrusted access to the API listener. |
| Internal network | Keep gRPC, metrics, cell ingress, worker-control, log, artifact, registry, database, and secrets ports private. Use TLS or mTLS when traffic crosses host or network boundaries. |
| Orchestrator | Run one active `vectis-orchestrator`. Do not place multiple active orchestrators behind a non-sticky resolver. |
| Queue | Run one or more independent queue shards. Each shard needs a stable instance ID and its own durable persistence directory. |
| Logs | Run one or more independent log shards. Each shard needs a stable instance ID and its own durable storage directory. |
| Artifacts | Run one or more independent artifact shards. Each shard needs a stable instance ID and its own durable storage directory. |
| Workers | Scale job execution by adding `vectis-worker` processes. Pair each worker with its own `vectis-worker-core` unless a different provider topology has been deliberately tested. |
| Reconciler | Run `vectis-reconciler` as required infrastructure. Multiple instances are allowed as active/passive standbys through the database service lease. |
| Cron | Run `vectis-cron` only if schedules are used. Multiple cron instances may coordinate through the shared database. |
| Registry | Run one registry by default, or configure a deliberate registry HA cluster. Pin queue, orchestrator, log, or artifact addresses when registry availability should not be on the critical path. |
| Docs | Treat `vectis-docs` as optional and expose it only to operators unless a separate public-docs posture is chosen. |
| Secrets | Store API tokens, bootstrap token, PostgreSQL credentials, TLS keys, SPIFFE CA material, and encryptedfs keys in an operator-controlled secret manager. |
| Storage | Put database data, queue persistence, logs, artifacts, secret envelopes, SPIFFE CA material, and observability data on durable storage with backup and retention policies. |
| Observability | Use Vectis metrics plus host, database, and filesystem telemetry. The CLI health check is not a complete production monitoring system. |

## Optional Components

Use these components when the deployment needs their feature surface:

| Component | When to include it | Production v1 boundary |
| --- | --- | --- |
| `vectis-secrets` | Jobs need Vectis-mediated secret resolution. | Keep the broker private, require workload identity, back encryptedfs material with durable secret storage, and monitor resolve failures. |
| `vectis-spiffe` | Workers need Vectis-managed per-execution SVIDs. | Store CA material durably, keep Workload API and Entry API sockets private, and do not mount broad SPIFFE sockets directly into untrusted job processes. |
| `vectis-log-forwarder` | Worker hosts need local log spooling before delivery to `vectis-log`. | One owner per socket/spool path. Preserve spool storage and monitor backlog age and size. |
| `vectis-catalog` | Multi-cell fan-in is enabled. | Required when global and cell databases are split. The catalog must read every cell-local database. |
| `vectis-cell-ingress` | Multi-cell routing is enabled. | Private execution submission surface only. Require mTLS for non-loopback ingress and restrict producer identities when possible. |

## Scale-Out Contract

Production v1 scales by adding capacity at the supported boundaries:

| Need | First lever |
| --- | --- |
| More parallel job execution | Add workers, each with a paired worker core. |
| More queue capacity | Add independent queue shards with separate persistence. |
| More log ingest or replay capacity | Add independent log shards with separate storage. |
| More artifact capacity | Add independent artifact shards with separate storage. |
| More API request capacity | Add API replicas after validating load balancer behavior, shared cache/rate-limit posture, and SSE reconnect behavior. |
| More schedule resilience | Add cron replicas against the same database. |
| More repair resilience | Add reconciler standbys; only the DB lease holder repairs at a time. |

Capacity still needs deployment-specific validation. Before depending on a larger
operating point, validate database pool sizing, worker fleet pressure,
orchestrator claim and completion latency, queue backlog behavior, log volume,
artifact volume, and API/SSE traffic patterns.

## Multi-Cell Boundary

The default production v1 topology is single-cell.

Multi-cell can be treated as an advanced production-like topology when:

- whole runs are routed to named execution cells;
- every global producer shares the same cell ingress map;
- each cell has private ingress, queue, workers, and a cell-local database;
- `vectis-catalog` fans cell events back into the global database;
- cell ingress is private, mTLS-protected, and reachable only by trusted global producers.

Production v1 multi-cell does not include cross-cell DAG choreography inside one
run, artifact replication between cells, per-cell action repository
distribution, or cell-to-cell dispatch.

## Not In Production v1

These shapes are outside the production v1 contract:

| Shape | Current status |
| --- | --- |
| Multi-region active/active Vectis | Not supported. |
| Multiple active orchestrators for the same run space | Not supported. |
| Vectis-managed database failover | Not provided; use the operator's PostgreSQL platform and restore/failover procedures. |
| Shared multi-writer queue, log, or artifact storage | Not supported. Add independent shards instead. |
| External object-store artifact backend | Not available yet. |
| Artifact replication between cells | Not available yet. |
| Cross-cell DAG execution | Not available yet. Model cross-cell workflows as multiple runs. |
| Public cell ingress | Not supported. Cell ingress is internal infrastructure. |
| Podman reference deployment as production architecture | Reference only. Use it as a map, not as the final operating model. |
| `vectis-local` as production control plane | Development only. |
| Untrusted workloads without an isolation policy | Not recommended. Use VM isolation and document the worker/provider boundary before accepting untrusted jobs. |

## Production Readiness Checklist

Before calling a production v1 deployment ready, confirm:

1. PostgreSQL backups and restore drills are tested.
2. API auth, allowed Hosts, trusted proxy CIDRs, and HTTPS edge behavior are configured.
3. Internal service ports are private, and TLS or mTLS is configured wherever traffic crosses host or network boundaries.
4. `vectis-reconciler` is running and visible in health checks and metrics.
5. Queue, log, and artifact shards use stable IDs and durable, separate storage paths.
6. Workers drain cleanly during restart and are paired with reachable worker cores.
7. Secrets, TLS keys, SPIFFE CA material, and encryptedfs keys live in the operator's secret manager.
8. Retention cleanup is scheduled or explicitly assigned to an operator runbook.
9. Monitoring covers Vectis metrics, host disk, database health, filesystem pressure, and service logs.
10. A smoke run can be triggered, reaches a terminal state, streams logs, and records expected artifacts and audit events.

## Related Documentation

| Topic | Document |
| --- | --- |
| Linux deployment runbook | [Production Linux Deployment](./production-linux.md) |
| Config and secrets contract | [Production Config And Secrets Contract](./production-config-contract.md) |
| Reference deployment boundaries | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Replica counts and restart behavior | [Scaling And Restarts](./scaling-and-restarts.md) |
| Configuration and service prefixes | [Configuration](../configuration.md) |
| Internal service trust | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Security baseline | [Security](../../concepts/security.md) |
| Multi-cell operation | [Multi-Cell Operation](../multi-cell.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Runbooks and alerts | [Runbooks And Alerts](../reliability/runbooks.md) |
| Capacity envelope | [Capacity And Load Envelope](../capacity/capacity-load-envelope.md) |
| Release production gate | [Releases And Upgrades](../../developing/releases.md#production-readiness-gate) |
