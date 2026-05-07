# Foundation Roadmap Before Feature Expansion

Work that should land before Vectis spends serious time on new user-facing features: deployment readiness, operational clarity, correctness, and maintainability.

**Status:** `todo` | `doing` | `defer` — intentionally **no “done” section**; shipped items live in code and git history, not here.

**Last reviewed:** 2026-05

## Goal

Adding features should be mostly product design and implementation, not archaeology, deployment guesswork, or hidden reliability debt.

## Outstanding foundations

| Area | Status | What is still open |
| --- | --- | --- |
| As-built docs & API reference | doing | Route tables and prose for auth, runs, namespaces, logs; **public** error-contract doc. `internal/logserver` HTTP still mixes in plain text errors — align or document exception. |
| Compatibility & deprecation | todo | Written guarantees for REST v1, protos, CLI output, config/env, schema. |
| CI breadth vs dogfood | doing | `.vectis/ci.sh` is strong (pre-commit, proto drift on `api/gen/go`, `test-quick`, container build). **GitHub** `ci.yml` only runs `make ci-quick`; optional: wire `test-postgres-integration` / `test-integration`, formal verification when feasible. |
| Postgres confidence | doing | `tests/integration/postgres/` exists; keep it green and consider promoting to default CI. |
| Reference deploy | doing | Bootstrap-token lifecycle, demo vs staging vs prod checklist, loud non-prod warnings. |
| Run lifecycle (operator) | doing | CLI run **list**, DLQ-oriented flows if API keeps them; dispatch trail on CLI; docs for repair paths. |
| Worker execution containment | todo | Isolation for shell/checkout before untrusted workloads. |

## Track — remaining blockers

### 1. API response and error contract

**Status:** doing

**Open:** Log service HTTP: same JSON error envelope as `internal/api` **or** explicit documented exception. API reference documenting `code` / `message` / `details`. Optional: exhaustive status-code matrix tests.

### 2. Job definition semantic validation

**Status:** doing

Structural validation and known-action resolution exist (`internal/job/validation`). **Open:** validate action-specific `with` (e.g. shell, checkout); richer graph rules as pipeline-as-code arrives.

### 3. Migration and rollback discipline

**Status:** doing

Up/down tests exist for SQLite and Postgres. **Open:** expand/contract rules, SQLite vs Postgres checklist, binary↔schema compatibility stance, whether `Down` is prod rollback or dev-only — capture in ADR + short operator notes.

### 4. Operational CLI coverage

**Status:** doing

**Open:** `runs list`; consistent `--json` (or equivalent) and exit-code contract; namespaces, users, role bindings; queue DLQ list/requeue if operator-facing; optional: surface dispatch trail on run commands.

### 5. Multi-replica semantics

**Status:** todo

Replica counts per component, singleton expectations, readiness vs liveness, rolling-restart runbooks/tests. **Rate limiting:** in-process limiter today — document limit at scale or add shared limiter.

### 6. Retention and storage pressure

**Status:** todo

Policies and knobs for run history, persisted logs, audit, job versions, queue persistence, local spools; cleanup jobs or documented operator commands; metrics for pressure and cleanup outcomes.

### 7. Observability: SLOs, alerts, runbooks

**Status:** doing

**Open:** SLO targets, Prometheus alert examples (stuck runs, DLQ, queue persistence, log append, auth spikes, etc.), dashboard alignment, `docs/RUNBOOKS.md` (or equivalent) with alert links.

### 8. API idempotency and client retry semantics

**Status:** doing

Trigger and ephemeral `POST /api/v1/jobs/run` support `Idempotency-Key` with DB backing. **Open:** extend to other mutators if needed; **document** which endpoints accept keys and replay behavior.

### 9. Service startup and dependency recovery

**Status:** doing

**Open:** per-binary matrix (hard vs soft deps); deeper recovery tests; clarify “recover without restart” where appropriate (beyond current API not-ready paths).

### 10. Request context and deadline propagation

**Status:** todo

Audit API → registry / worker / queue and worker → log / queue; fix **cancel / run-control** paths that still use detached `context.Background()` where the request should bound work; document intentional detachment for async enqueue.

### 11. Dispatch handoff (operator visibility)

**Status:** doing

Dispatch events are persisted and exposed on run GET. **Open:** CLI and operator docs; any missing tests (e.g. API restart during async enqueue) if product wants that guarantee explicit.

### 12. Log streaming: replay and scale

**Status:** doing

In-memory terminal-buffer eviction and `max_run_buffers` exist. **Open:** documented replay pagination / tail limits; persisted log retention (ties to §6); load or boundedness tests for many SSE clients.

### 13. Audit durability and loss policy

**Status:** doing

Async auditor still drops when the buffer is full; metrics exist for drops and flush failures when wired. **Open:** operator doc on loss semantics; product decision on durable queue or fail-closed for sensitive event types.

### 14. Retry and backoff policy surface

**Status:** doing

**Open:** retry attempt/failure **metrics**; operator doc of defaults and overrides per service (API, cron, reconciler, worker clients).

### 15. Reference deployment: bootstrap and posture

**Status:** doing

CLI-generated Podman secrets improved the demo story. **Open:** bootstrap token supply/rotate/remove workflow in docs; deployment checklist across demo / staging / production.

---

## Recommended first tranche

1. **Docs + policy:** API reference and error contract, compatibility doc, deploy/bootstrap checklist, replica matrix.
2. **CI:** Optionally add Postgres (and broader integration) to published CI, not only dogfood.
3. **Operator surfaces:** CLI gaps (§4), runbooks + alerts (§7), dispatch visibility (§11).
4. **Safety + data:** Worker containment, `with` validation, retention (§6).
5. **Hardening:** Request context on cancel paths (§10), retry metrics (§14).

## Phased sequencing (forward-looking)

**Phase 0 — Legible:** finish public API + error docs; compatibility policy; deploy credential and bootstrap story.

**Phase 1 — Repeatable:** broaden CI if desired; keep migration discipline documented as schema evolves; startup/recovery matrix and tests.

**Phase 2 — Repairable:** CLI and runbooks; retention; SLOs/alerts; multi-replica documentation and rate-limit stance; dispatch/audit visibility in operator tooling.

**Phase 3 — Safe execution:** worker containment; semantic validation.

## Feature readiness checklist

Before a major user-facing feature, answer yes to:

- Is the public API route / CLI / config contract documented?
- Is there validation at the API boundary (including action-specific needs)?
- Does it run on Postgres and SQLite where supported?
- Are migrations forward-safe and rollback expectations documented?
- Is observability runbook-quality (metrics, alerts, owner)?
- Does it behave sensibly under retries, duplicate requests, and process restarts?
- Does it respect namespace/RBAC and token scope?
- Does it avoid storing or logging secrets accidentally?
- Is there an operator repair path for partial failure?

## Parking lot

Not required before feature work, but tracked elsewhere:

- OIDC/LDAP integration.
- Kubernetes-native manifests beyond the Podman reference.
- Autoscaling workers; multi-site/federation; frontend SPA; plugin/hook catalog.
