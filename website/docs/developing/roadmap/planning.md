# Planning

This page is the single planning home for Vectis. It is for maintainers deciding what to build, defer, or harden next.

For shipped behavior, prefer the durable docs:

| Topic | Document |
| --- | --- |
| Current architecture | [Architecture](../../concepts/architecture.md) |
| Failure behavior | [Failure Domains](../../concepts/failure-domains.md) |
| Security model | [Security](../../concepts/security.md) |
| Configuration | [Configuration](../../operating/configuration.md) |
| Release process | [Releases](../releases.md) |
| Architecture decisions | [Architecture Decision Records](../architecture-decisions/index.md) |

## Product Direction

Vectis is a self-hosted orchestrator for generic job graphs and CI/CD-style workloads. The core direction is:

- Keep the control plane self-hosted and inspectable.
- Make service contracts explicit through REST, gRPC, protobuf, and documented config.
- Keep the default deployment understandable before adding distributed complexity.
- Prefer operator-visible repair paths over hidden automatic mutation.
- Treat user job execution as a trust boundary, not an implementation detail.
- Grow toward larger deployments through local cells and clear gateway behavior rather than one global shared database.

## Current Roadmap

| Area | Direction |
| --- | --- |
| Pipeline-as-code | Add a `.vectis.yml` or YAML-based authoring path that compiles to the existing job graph shape. |
| Triggers | Add webhook and optional VCS polling support. Decide whether future trigger types belong in one trigger service or separate binaries. |
| Operational coverage | Extend `health check` into deploy-specific checks that cannot be inferred through the API, including TLS files and writable storage paths. |
| Observability | Improve run correlation across API, queue, worker, log service, and database. Keep Prometheus metrics; add richer traces only where they help operators debug real failures. |
| Worker safety | Continue the [worker execution containment provider](../architecture-decisions/0009-worker-execution-containment-providers.md) path: action-level `host`/`vm` selection and Lima command backend first, then profile-aware placement, container profiles, and disposable VM profiles for stronger isolation. |
| Secrets | Design a local secrets service with provider-neutral contracts, worker-side resolution, authorization, audit, and redaction hooks. |
| Federation | Defer until single-cell behavior is boring. Future federation should use a gateway over distributed Vectis cells, not one shared global database. |

## Open Foundation Decisions

These areas are intentionally not active implementation plans yet. They should be revisited before major feature expansion.

| Area | Status | What needs a decision |
| --- | --- | --- |
| Multi-replica semantics | Deferred | Long-term API rate-limit behavior, queue/log HA posture, cron leader election or partitioning, reconciler duplicate-handoff bounds, pool-aware worker scale-out, and rolling-restart tests. |
| Retention and storage pressure | Deferred | Production defaults, cleanup cadence, queue persistence, log-forwarder spools, backup/restore interaction, and deploy-specific disk pressure checks. |
| Worker execution containment | Accepted design; implementation pending | Runner boundary, execution profiles, container provider, VM provider, resource limits, action policy, environment filtering, workspace controls, placement, and cleanup behavior. |
| Local secrets service | Deferred | Provider-neutral service contract, encrypted local backend, runtime identity and authorization, worker-side resolution, audit events, and redaction hooks. |

## Federation Direction

Federation is not implemented. The current direction is a global gateway in front of independent Vectis cells.

Each cell owns its site-local infrastructure:

- Its own Vectis API, queue, workers, log service, registry, cron, and reconciler.
- Its own database and migrations.
- Its own logs, queue persistence, spools, and local operational limits.
- Its own failure domain and repair workflow.

The gateway is responsible for scatter/gather behavior across cells:

| Gateway responsibility | Notes |
| --- | --- |
| Route write requests | Send new work to the selected cell based on project, policy, user choice, or locality. |
| Aggregate reads | Query multiple cells and merge results for global views. |
| Preserve locality | Keep run execution, logs, database state, and repair inside the owning cell. |
| Expose cell health | Show whether a cell is reachable and whether its local control plane is healthy. |
| Avoid global database coupling | Do not require site-local cells to share one database or use database replication as the federation mechanism. |

This keeps federation mostly a routing and aggregation problem. A cell can be down, slow, or isolated without turning the whole deployment into a cross-site database problem.

### Deferred Federation Questions

- How does the gateway authenticate to each cell?
- Which objects are global, and which are cell-local?
- How are project-to-cell routing policies stored?
- How are global list responses paginated across cells?
- How does the gateway stream logs or run events from the owning cell?
- How are partial failures represented when one cell is unavailable?
- What operator action moves a project or workload from one cell to another?

## Feature Readiness Checklist

Before a major user-facing feature, answer yes to:

- Is the public API route, CLI behavior, config, and error contract documented?
- Is there validation at the API boundary, including action-specific validation where needed?
- Does it run on Postgres and SQLite where supported?
- Are migrations forward-safe, and are rollback expectations documented?
- Is observability good enough for a runbook: metrics, alerts, owner, trace path, and log path?
- Does it behave sensibly under retries, duplicate requests, process restarts, and partial restores?
- Does it respect namespace, RBAC, token scope, and internal service trust boundaries?
- Does it avoid storing or logging secrets accidentally?
- Is there an operator repair path for partial failure?
- Does it fit within the documented capacity envelope, or update that envelope?
- Is worker execution isolated appropriately for the trust level of the workload?

## Parking Lot

These are not required before the next useful feature, but they remain part of the broader direction:

- OIDC and LDAP integration.
- Kubernetes-native manifests beyond the Podman reference.
- Autoscaling workers.
- Frontend SPA.
- Plugin or hook catalog.
