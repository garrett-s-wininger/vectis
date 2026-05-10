# Foundation Roadmap Before Feature Expansion

Work that should land before Vectis spends serious time on new user-facing features: deployment readiness, operational clarity, correctness, safety, and maintainability.

**Status:** `todo` | `doing` | `defer` - intentionally **no "done" section**; shipped or documented items live in code, durable docs, ADRs, and git history, not here.

**Last reviewed:** 2026-05-10 - Foundations 1, 2, 4, 8, 10, 12, and 19 cleared into code and durable docs; retention now has first-pass cleanup, metrics, and docs, with remaining production policy work still tracked here.

## Goal

Adding features should be mostly product design and implementation, not archaeology, deployment guesswork, hidden reliability debt, or unclear trust boundaries.

## Outstanding Foundations

| Area | Status | Planning doc | What is still open |
| --- | --- | --- | --- |
| Multi-replica semantics | defer | [05](FOUNDATION_05_MULTI_REPLICA_SEMANTICS.md), [worker pools](RUNTIME_01_WORKER_POOLS.md) | Human review needed for replica-count commitments, singleton/scale-out guidance, rate-limit behavior under API replicas, cron HA stance, pool-aware worker scale-out, and rolling-restart expectations. |
| Retention and storage pressure | defer | [06](FOUNDATION_06_RETENTION_STORAGE_PRESSURE.md), [RETENTION](RETENTION.md) | First-pass SQL cleanup, dry-run/apply CLI, active-run protections, local run-log pruning, audit event, and SQL pressure metrics are shipped. Human review still needed for production defaults, cleanup cadence, queue persistence, spools, and backup/restore expectations. |
| Audit durability and loss policy | defer | [13](FOUNDATION_13_AUDIT_DURABILITY_LOSS_POLICY.md) | Human product decision needed on fail-closed vs best-effort audit events, explicit loss semantics, and tests for buffer/DB/shutdown behavior. |
| Worker execution containment | defer | [21](FOUNDATION_21_WORKER_EXECUTION_CONTAINMENT.md), [pools](RUNTIME_01_WORKER_POOLS.md), [executors](RUNTIME_02_REMOTE_EXECUTORS.md), [actions](RUNTIME_03_ACTION_CATALOG.md) | Human architecture review needed for worker isolation, resource limits, action policy, environment filtering, workspace controls, executor boundary, and sandbox path before untrusted workloads. |
| Local secrets service | defer | [22](FOUNDATION_22_LOCAL_SECRETS_SERVICE.md) | Human architecture review needed for provider-neutral gRPC contract, encrypted local backend, runtime authorization, worker resolution, audit, and redaction integration. |

## Foundation Items

### 5. Multi-Replica Semantics

**Status:** defer

Plan: [FOUNDATION_05_MULTI_REPLICA_SEMANTICS.md](FOUNDATION_05_MULTI_REPLICA_SEMANTICS.md)

Related plan: [RUNTIME_01_WORKER_POOLS.md](RUNTIME_01_WORKER_POOLS.md)

**Open:** Human review for replica counts per component, singleton expectations, readiness vs liveness, rolling-restart runbooks/tests, rate-limit semantics under horizontal API replicas, cron/reconciler duplicate behavior, and pool-aware worker scale-out.

### 6. Retention And Storage Pressure

**Status:** defer

Plan: [FOUNDATION_06_RETENTION_STORAGE_PRESSURE.md](FOUNDATION_06_RETENTION_STORAGE_PRESSURE.md)

First pass: [RETENTION.md](RETENTION.md) documents `vectis-cli retention cleanup`, default windows, active-run protections, local run-log pruning, audit events, and SQL pressure metrics. **Open:** Human review for production retention windows, scheduled cleanup authority, backup/restore interaction, queue persistence, log-forwarder spools, and deploy-specific disk pressure behavior.

### 13. Audit Durability And Loss Policy

**Status:** defer

Plan: [FOUNDATION_13_AUDIT_DURABILITY_LOSS_POLICY.md](FOUNDATION_13_AUDIT_DURABILITY_LOSS_POLICY.md)

Async auditor still drops when the buffer is full; metrics exist for drops and flush failures. **Open:** human product decision on explicit loss semantics, durable queue vs best-effort buffering, and fail-closed behavior for sensitive event types.

### 21. Worker Execution Containment

**Status:** defer

Plan: [FOUNDATION_21_WORKER_EXECUTION_CONTAINMENT.md](FOUNDATION_21_WORKER_EXECUTION_CONTAINMENT.md)

Related plans: [RUNTIME_01_WORKER_POOLS.md](RUNTIME_01_WORKER_POOLS.md), [RUNTIME_02_REMOTE_EXECUTORS.md](RUNTIME_02_REMOTE_EXECUTORS.md), [RUNTIME_03_ACTION_CATALOG.md](RUNTIME_03_ACTION_CATALOG.md)

**Open:** Human architecture review for isolation, limits, action policy, environment filtering, workspace controls, executor boundary, and sandbox path before untrusted workloads.

### 22. Local Secrets Service

**Status:** defer

Plan: [FOUNDATION_22_LOCAL_SECRETS_SERVICE.md](FOUNDATION_22_LOCAL_SECRETS_SERVICE.md)

**Open:** Human architecture review for provider-neutral service contract, encrypted local backend, runtime identity/authorization, worker-side resolution, audit events, and redaction hooks.

## Recommended First Tranche

1. **Human policy decisions:** audit loss semantics, retention defaults, cleanup authority, and fail-open/fail-closed boundaries.
2. **Scale architecture:** multi-replica semantics, worker pools, cron/reconciler HA, and rolling-restart expectations.
3. **Safety and trust:** worker containment, action catalog, executor boundary, and local secrets service.

## Easy Foundation Hardening Still Left

The remaining low-friction foundation work is:

- Add a first-pass replica-count matrix and rolling-restart table.
- Turn repair workflows into linked runbook recipes.
- Assign stable `doctor` check IDs.
- Document audit-loss semantics and the fail-open/fail-closed decision.

## Phased Sequencing

**Phase 0 - Legible:** audit-loss policy, retention policy, repair workflow docs, and `doctor` check IDs.

**Phase 1 - Repairable:** retention cleanup implementation, CLI repair surfaces, and multi-replica runbooks.

**Phase 2 - Repeatable:** replica matrix, worker pools, cron/reconciler HA, and rolling-restart tests.

**Phase 3 - Safe execution:** worker containment, executor boundary, action catalog, and local secrets service.

## Feature Readiness Checklist

Before a major user-facing feature, answer yes to:

- Is the public API route, CLI, config, and error contract documented?
- Is there validation at the API boundary, including action-specific needs?
- Does it run on Postgres and SQLite where supported?
- Are migrations forward-safe and rollback expectations documented?
- Is observability runbook-quality: metrics, alerts, owner, and trace/log path?
- Does it behave sensibly under retries, duplicate requests, process restarts, and partial restores?
- Does it respect namespace/RBAC, token scope, and internal service trust boundaries?
- Does it avoid storing or logging secrets accidentally?
- Is there an operator repair path for partial failure?
- Does it fit within the documented capacity envelope or update that envelope?
- Is worker execution isolated appropriately for the trust level of the workload?

## Adjacent Planning References

Recent planning docs that shape the remaining foundation work:

- [OPERATIONS_01_DOCTOR_COMMAND.md](OPERATIONS_01_DOCTOR_COMMAND.md)
- [OPERATIONS_02_REPAIR_WORKFLOWS.md](OPERATIONS_02_REPAIR_WORKFLOWS.md)
- [RUNTIME_01_WORKER_POOLS.md](RUNTIME_01_WORKER_POOLS.md)
- [RUNTIME_02_REMOTE_EXECUTORS.md](RUNTIME_02_REMOTE_EXECUTORS.md)
- [RUNTIME_03_ACTION_CATALOG.md](RUNTIME_03_ACTION_CATALOG.md)
- [UI_01_RUN_OVERVIEW.md](UI_01_RUN_OVERVIEW.md)
- [UI_02_PROJECT_DASHBOARD.md](UI_02_PROJECT_DASHBOARD.md)
- [UI_03_ADMIN_CONSOLE.md](UI_03_ADMIN_CONSOLE.md)

## Parking Lot

Not required before feature work, but tracked elsewhere:

- OIDC/LDAP integration.
- Kubernetes-native manifests beyond the Podman reference.
- Autoscaling workers; multi-site/federation; frontend SPA; plugin/hook catalog.
