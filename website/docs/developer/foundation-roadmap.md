# Foundation Roadmap Before Feature Expansion

Work that should land before Vectis spends serious time on new user-facing features: deployment readiness, operational clarity, correctness, safety, and maintainability.

**Status:** `todo` | `doing` | `defer` - intentionally **no "done" section**; shipped or documented items live in code, durable docs, ADRs, and git history, not here.

**Last reviewed:** 2026-05-10 - Foundations 1, 2, 4, 8, 10, 12, 13, and 19 cleared into code and durable docs; retention now has first-pass cleanup, metrics, and docs; multi-replica semantics now have a first-pass replica matrix and rolling-restart table.

## Goal

Adding features should be mostly product design and implementation, not archaeology, deployment guesswork, hidden reliability debt, or unclear trust boundaries.

## Outstanding Foundations

| Area | Status | Planning doc | What is still open |
| --- | --- | --- | --- |
| Multi-replica semantics | defer | [Scaling and restarts](../operator/scaling-and-restarts.md) | First-pass replica matrix and rolling-restart table are documented. Open: human review for long-term API rate-limit behavior, queue/log HA stance, cron leader election or partitioning, reconciler duplicate-handoff bounds, pool-aware worker scale-out, and rolling-restart tests. |
| Retention and storage pressure | defer | [RETENTION](../operator/retention.md) | First-pass SQL cleanup, dry-run/apply CLI, active-run protections, local run-log pruning, audit event, and SQL pressure metrics are shipped. Human review still needed for production defaults, cleanup cadence, queue persistence, spools, and backup/restore expectations. |
| Worker execution containment | defer | — | Human architecture review needed for worker isolation, resource limits, action policy, environment filtering, workspace controls, executor boundary, and sandbox path before untrusted workloads. |
| Local secrets service | defer | — | Human architecture review needed for provider-neutral gRPC contract, encrypted local backend, runtime authorization, worker resolution, audit, and redaction integration. |

## Foundation Items

### 5. Multi-Replica Semantics

**Status:** defer

Plan: [SCALING_AND_RESTARTS.md](../operator/scaling-and-restarts.md)

First pass: [SCALING_AND_RESTARTS.md](../operator/scaling-and-restarts.md) documents recommended replica counts, current multi-replica safety, rolling-restart expectations, probe guidance, and scale-out boundaries. **Open:** human review for long-term API rate-limit behavior, queue/log HA stance, cron leader election or partitioning, reconciler duplicate-handoff bounds, pool-aware worker scale-out, and rolling-restart tests.

### 6. Retention And Storage Pressure

**Status:** defer

First pass: [RETENTION.md](../operator/retention.md) documents `vectis-cli retention cleanup`, default windows, active-run protections, local run-log pruning, audit events, and SQL pressure metrics. **Open:** Human review for production retention windows, scheduled cleanup authority, backup/restore interaction, queue persistence, log-forwarder spools, and deploy-specific disk pressure behavior.

### 21. Worker Execution Containment

**Status:** defer

**Open:** Human architecture review for isolation, limits, action policy, environment filtering, workspace controls, executor boundary, and sandbox path before untrusted workloads.

### 22. Local Secrets Service

**Status:** defer

**Open:** Human architecture review for provider-neutral service contract, encrypted local backend, runtime identity/authorization, worker-side resolution, audit events, and redaction hooks.

## Recommended First Tranche

1. **Human policy decisions:** retention defaults, cleanup authority, queue persistence, spool policy, and backup/restore boundaries.
2. **Scale architecture:** worker pools, cron/reconciler HA, queue/log HA posture, and rolling-restart tests.
3. **Safety and trust:** worker containment, action catalog, executor boundary, and local secrets service.

## Easy Foundation Hardening Still Left

The remaining low-friction foundation work is:

- Extend `doctor` with deploy-specific checks that cannot be inferred through the API yet, such as TLS file validation and filesystem pressure for queue persistence, log storage, and log-forwarder spools.

## Phased Sequencing

**Phase 0 - Legible:** retention policy, repair workflow docs, and deploy-specific `doctor` check coverage.

**Phase 1 - Repairable:** retention cleanup implementation, CLI repair surfaces, and multi-replica runbooks.

**Phase 2 - Repeatable:** worker pools, cron/reconciler HA, queue/log HA posture, and rolling-restart tests.

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

- [SCALING_AND_RESTARTS.md](../operator/scaling-and-restarts.md)
- [RETENTION.md](../operator/retention.md)
- [CLI_OPERATIONAL_COVERAGE.md](../operator/cli-operational-coverage.md)
- [BACKUP_RESTORE.md](../operator/backup-restore.md)
- [FAILURE_DOMAINS.md](../general/failure-domains.md)

## Parking Lot

Not required before feature work, but tracked elsewhere:

- OIDC/LDAP integration.
- Kubernetes-native manifests beyond the Podman reference.
- Autoscaling workers; multi-site/federation; frontend SPA; plugin/hook catalog.
