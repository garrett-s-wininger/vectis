# Foundation Roadmap Before Feature Expansion

This document tracks work that should land before Vectis spends serious time on new user-facing features. It intentionally focuses on deployment readiness, operational clarity, correctness, and maintainability.

Status key: `todo`, `doing`, `done`, `defer`.

## Goal

Reach a point where adding features is mostly product design and implementation, not archaeology, deployment guesswork, or hidden reliability debt.

## Already Identified Baseline

These are already known high-value foundations and remain prerequisites:

| Area | Status | Notes |
| --- | --- | --- |
| As-built docs and API contract alignment | todo | Architecture/failure docs lag code for auth, run control, users, tokens, namespaces, and logs. |
| Production deployment hardening | todo | Reference Podman stack is useful but demo-oriented. |
| Postgres-backed integration lane | todo | Many tests use SQLite-only helpers despite Postgres being the production path. |
| Worker execution containment | todo | Shell/checkout execution needs stronger isolation controls before untrusted workloads. |
| Run lifecycle and repairability | todo | Operators need clearer stuck-run, DLQ, cancel, requeue, and orphan workflows. |
| CI quality gate | todo | Add visible CI for tests, generated artifacts, containers, and integration slices. |

## Additional Blockers

### 1. API Response And Error Contract

Status: todo

Why it blocks feature work:
Clients and future UI code need stable response shapes. Today auth errors use JSON helpers in several paths, while many API handlers still use `http.Error` plain text. Adding features on top of mixed error semantics will spread client special cases.

Evidence:
- Auth JSON helpers exist in `internal/api/auth_response.go`.
- Many route handlers in `internal/api/` still return plain text with `http.Error`.
- `docs/PLANNING.md` calls structured REST errors a target shape.

Acceptance criteria:
- Define one error envelope for all REST routes, including `code`, `message`, and optional `details`.
- Preserve stable auth/setup error codes.
- Add tests for representative 400/401/403/404/409/429/500/503 responses.
- Document the contract in the API reference.

Suggested slice:
Add a small `writeAPIError` helper, migrate high-traffic routes first, then finish the rest in mechanical batches.

### 2. Job Definition Semantic Validation

Status: todo

Why it blocks feature work:
Structural job validation now exists at the API boundary, but new features will add action-specific inputs and richer graph semantics. Without a clear next validation layer, invalid feature-specific definitions may still be discovered late by workers.

Acceptance criteria:
- Add structured validation errors to the REST error envelope once the API error contract lands.
- Validate action-specific `with` values.
- Cover semantic graph constraints that arrive with pipeline-as-code.

Suggested slice:
Structural validation and action lookup are in place. Next slice is action-specific input validation once the structured API error contract exists.

### 3. Migration And Rollback Discipline

Status: todo

Why it blocks feature work:
Future features will require schema changes. The current repo has a single initial migration per backend, which is fine early, but feature velocity will suffer unless migration review, rollback, and compatibility expectations are explicit.

Acceptance criteria:
- Document expand/contract migration rules.
- Add a migration checklist for SQLite and Postgres divergence.
- Add tests that apply all up migrations and all down migrations for both backends where available.
- Add a “new binary against old schema” and “old binary against new schema” compatibility stance.
- Decide whether down migrations are production rollback tools or development-only.

Suggested slice:
Create migration test helpers and a short ADR before adding the next migration.

### 5. Operational CLI Coverage

Status: todo

Why it blocks feature work:
The API has several operational capabilities, but feature deployment is safer when an operator can inspect and repair state without hand-writing HTTP calls or SQL.

Acceptance criteria:
- CLI commands for run list and machine-readable JSON output options.
- CLI commands for setup/login/logout/token management.
- CLI commands for users, namespaces, and bindings if auth is enabled.
- CLI commands for queue DLQ list/requeue if that surface remains operator-facing.
- Clear nonzero exit codes and machine-readable JSON output options.

Suggested slice:
Finish run listing/JSON output, then namespace/user/admin commands.

### 6. Multi-Replica Semantics

Status: todo

Why it blocks feature work:
Feature deployment usually implies scaling API and workers. Some state is intentionally in DB/queue, but other state remains process-local or singleton-sensitive.

Acceptance criteria:
- Document supported replica counts per component.
- Decide which components are singleton-only: queue, cron, registry, log, reconciler.
- Add readiness behavior for lazy/unavailable dependencies where startup fataling is too brittle.
- Make rate limiting semantics explicit: per-process is acceptable for dev, shared limiter required for scaled API, or documented as a limit.
- Add tests or runbooks for API rolling restart, worker rolling restart, and queue restart.

Suggested slice:
Write the replica support matrix, then harden the highest-risk mismatch.

### 7. Cron Single-Firer Guarantee

Status: todo

Why it blocks feature work:
New trigger features will build on the same “exactly one firing path” assumption. Current docs say duplicate cron processes can double-fire schedules.

Acceptance criteria:
- Choose one: singleton-only deployment, DB advisory lock, lease row, or external leader election.
- Enforce or loudly detect duplicate cron ownership.
- Add integration tests for two cron instances attempting the same schedule.
- Add metrics/alerts for skipped or duplicate schedule ownership attempts.

Suggested slice:
Use a DB-backed lease or advisory lock on the Postgres path, with a clear SQLite story for local/dev.

### 8. Retention And Storage Pressure Controls

Status: todo

Why it blocks feature work:
More features usually mean more logs, audits, job versions, and run history. Without retention knobs, a healthy system can slowly become an unavailable system.

Acceptance criteria:
- Define retention policy for run logs, audit log, run history, job definition versions, queue WAL/snapshots, and local log spools.
- Add configurable cleanup jobs or documented operator commands.
- Add metrics for storage pressure and cleanup outcomes.
- Make “never delete automatically” an explicit setting where needed.

Suggested slice:
Start with log retention and audit retention because they can grow fastest and contain sensitive data.

### 9. Secrets And Sensitive Data Hygiene

Status: todo

Why it blocks feature work:
New features like checkout credentials, webhooks, artifacts, and pipeline-as-code tend to introduce secrets. Handling them ad hoc will be expensive to unwind.

Acceptance criteria:
- Define what data may be stored in job definitions.
- Add log redaction hooks or an explicit “no redaction yet” warning surface.
- Avoid logging full DSNs, tokens, bootstrap secrets, and repository credentials.
- Add tests for token/secret redaction in common log/error paths.
- Decide whether secret injection is out-of-process only for now.

Suggested slice:
Add a small redaction package and use it in config/error logging before adding secret-bearing features.

### 10. Artifact And Cache Boundaries

Status: todo

Why it blocks feature work:
Many CI features eventually need artifacts or dependency cache. Even if not implemented now, the core data model should avoid painting this into a corner.

Acceptance criteria:
- Decide whether artifacts are part of Vectis core, an external integration, or explicitly deferred.
- Define run/job identity keys that artifact/cache storage would use.
- Decide retention, access control, and namespace scoping before API routes are added.
- Document non-goals so feature requests do not leak partial artifact behavior into unrelated code.

Suggested slice:
Write an ADR for artifacts/cache before implementing pipeline-as-code or richer job UI.

### 11. Observability SLOs And Alerts

Status: todo

Why it blocks feature work:
Metrics exist, but feature deployment needs crisp answers to “is it healthy?” and “what wakes an operator?”

Acceptance criteria:
- Define initial SLOs for trigger acceptance, dispatch latency, run completion, queue depth, log ingest, and DB availability.
- Add Prometheus alert examples for stuck queued runs, lease expiry spikes, DLQ growth, queue persistence errors, log append failures, and auth failure spikes.
- Ensure dashboard panels map to those alerts.
- Add runbook links for each alert.

Suggested slice:
Add a `docs/RUNBOOKS.md` plus alert rules for the existing Podman/Prometheus reference.

### 12. Formal/Model Coverage Gap

Status: todo

Why it blocks feature work:
The current formal model covers only a narrow reconciliation partition. The real risky behavior now spans async enqueue, queue delivery TTL, DB claims, worker ack uncertainty, leases, and manual repair.

Acceptance criteria:
- Decide which invariants are important enough to model.
- Extend the model or add a second model for queue delivery plus DB claim semantics.
- Include cancellation/requeue/orphan behavior if those paths are considered foundational.
- Run formal checks in CI when tooling is available; otherwise keep a documented manual command.

Suggested slice:
Model duplicate queue messages plus exactly-one successful claim for a run.

### 13. API Idempotency And Client Retry Semantics

Status: todo

Why it blocks feature work:
Clients and future UI flows will retry requests. Trigger endpoints return 202 before enqueue completes, but there is no explicit client idempotency key contract.

Acceptance criteria:
- Decide which mutating endpoints accept an idempotency key.
- Ensure retried triggers cannot unintentionally create multiple runs unless requested.
- Document which operations are safe to retry.
- Add tests for duplicate request behavior around job creation, trigger, run control, setup, token creation, and user creation.

Suggested slice:
Start with trigger and ephemeral run paths.

### 14. Service Startup And Dependency Recovery

Status: todo

Why it blocks feature work:
Many daemons fatal if dependencies are unavailable at startup. That can be fine for simple deploys, but rolling deploys and temporary outages get rough as features multiply.

Acceptance criteria:
- Decide per component which dependencies are startup-hard vs runtime-retry.
- Add readiness states that distinguish “process alive” from “ready to serve useful work.”
- Avoid permanent process death for recoverable dependencies where appropriate.
- Add startup/recovery tests for queue, registry, log, DB, and tracing backend availability.

Suggested slice:
Start with API queue/registry dependency behavior and worker queue/log recovery.

### 15. Compatibility And Deprecation Policy

Status: todo

Why it blocks feature work:
Once users depend on JSON jobs, REST routes, CLI output, and gRPC protos, feature changes need a compatibility posture.

Acceptance criteria:
- Define compatibility guarantees for REST v1, protobuf fields, CLI output, config/env vars, and database schema.
- Add deprecation language and minimum support window.
- Add generated-code drift checks for protobuf output.
- Add examples of acceptable additive changes.

Suggested slice:
Write a short compatibility policy before adding new public routes.

## Follow-Up Scan Findings

These are additional deployment blockers found during a second source scan. They overlap some items above, but are called out because the code paths are concrete enough to plan directly.

### 16. Cron Claim And Schedule Advancement Semantics

Status: todo

Why it blocks feature work:
Cron currently reads ready schedules, creates/enqueues a run, and only then advances `next_run_at`. If enqueue succeeds but schedule advancement fails, or if more than one cron process sees the same ready row, the same schedule can fire more than once. This is more specific than a generic "single cron" concern: the schedule row needs an ownership or compare-and-swap transition around firing.

Evidence:
- `internal/dal/repositories.go` selects ready rows with `WHERE next_run_at <= ?` and does not claim them.
- `internal/cron/cron.go` calls `TriggerJob` before `UpdateNextRun`.

Acceptance criteria:
- Add an atomic schedule claim, lease, or compare-and-swap update based on the previously observed `next_run_at`.
- Make multi-cron behavior explicit in docs and deployment manifests.
- Add a test where two cron services attempt to process the same ready schedule.
- Ensure enqueue success plus advancement failure is observable and repairable.

Suggested slice:
Add `ClaimReady` or `AdvanceIfDue` to the schedules repository, then update cron to fire only after a successful claim.

### 17. Durable Run Dispatch Handoff

Status: todo

Why it blocks feature work:
API trigger paths return `202 Accepted` before enqueue completes. If enqueue later fails, or if enqueue succeeds but `TouchDispatched` fails, the system has an ambiguous handoff state that operators can only infer indirectly. Reconciler behavior may make this eventually correct, but feature work should not build on an implicit partial-failure contract.

Evidence:
- `internal/api/server.go` broadcasts and responds before async enqueue finishes.
- `finishTriggerEnqueue` logs enqueue and `TouchDispatched` failures but does not persist a handoff event.
- `internal/cron/cron.go` has the same enqueue-then-touch pattern.

Acceptance criteria:
- Persist enqueue attempt, success, and failure state or events per run.
- Expose dispatch state through API/CLI inspection.
- Make reconciler decisions use durable handoff state where possible.
- Add tests for enqueue failure, enqueue success plus dispatch metadata failure, and API restart during async enqueue.

Suggested slice:
Introduce a small run event or dispatch state table before expanding trigger-related features.

### 18. Request Context And Deadline Propagation Audit

Status: todo

Why it blocks feature work:
Several control paths involve remote calls after an HTTP request enters the API. Feature work will add more cross-service calls, so request-scoped cancellation and deadlines should be consistent before those patterns spread.

Evidence:
- `cmd/api/main.go` resolves worker addresses for cancellation with `context.Background()` rather than the request context.
- Log server metrics paths also use background contexts for some in-process metric recording, which is lower risk but worth standardizing.

Acceptance criteria:
- Audit API-to-registry, API-to-worker, API-to-queue, worker-to-log, and worker-to-queue calls for caller context propagation.
- Use explicit bounded contexts where detached work is intentional.
- Add tests that canceled requests do not hang on registry or worker resolution.
- Document when background contexts are allowed.

Suggested slice:
Start with cancel/run-control paths because they are operator-facing repair tools.

### 19. Log Streaming Boundedness And Replay Contract

Status: todo

Why it blocks feature work:
Log streaming is central to a CI system. In-memory run buffers are keyed by run id and have no visible eviction path, while replay loads stored log entries before streaming. Feature work that increases run volume or log volume will amplify memory and latency risk.

Evidence:
- `internal/logserver/server.go` stores `buffers map[string]*JobBuffer` and creates buffers on demand.
- Each `JobBuffer` caps entries per run, but the number of buffers is not capped or evicted.
- SSE replay lists persisted logs before falling back to memory.

Acceptance criteria:
- Add buffer eviction for terminal/old runs.
- Define replay pagination, tailing, and max initial replay behavior.
- Add retention settings for persisted logs and memory buffers.
- Add load tests or boundedness tests for many runs and many SSE clients.

Suggested slice:
Add terminal-run buffer eviction and a documented replay limit before investing in richer log UI/features.

### 20. Audit Durability And Loss Policy

Status: todo

Why it blocks feature work:
Auth, tokens, namespaces, and admin actions are already security-sensitive. The async auditor intentionally drops events when the memory buffer is full and only logs flush failures, so the product needs an explicit stance before adding more admin/security features.

Evidence:
- `internal/api/audit/async.go` uses a bounded in-memory channel.
- `Log` drops audit events on a full buffer and returns nil.
- Flush failures are logged but not retried durably.

Acceptance criteria:
- Decide whether audit is best-effort or security-critical.
- Add metrics for dropped audit events and flush failures.
- Document loss behavior in operator docs.
- If security-critical, add durable queueing or fail-closed behavior for selected event types.

Suggested slice:
Keep best-effort audit if that is the intended current posture, but make dropped-event metrics and documentation mandatory before more admin features.

### 21. Retry And Backoff Policy Surface

Status: todo

Why it blocks feature work:
Queue enqueue retries now have default options and jitter, but trigger volume still needs metrics, documentation, and a clear override surface per service.

Evidence:
- `internal/queueclient/retry.go` defines retry options and jitter.
- API, cron, reconciler, and worker retry behavior is still not fully documented as an operator-facing policy.

Acceptance criteria:
- Define retry defaults and which services may override them.
- Emit retry attempt/failure metrics.
- Document retry behavior for API, cron, reconciler, and worker clients.

Suggested slice:
Add retry attempt/failure metrics and document defaults before new trigger sources are added.

### 22. Reference Deployment Secret And Bootstrap Posture

Status: todo

Why it blocks feature work:
The shipped Podman reference is useful for demos, but it currently embeds default database credentials and uses inline DSNs. Before treating it as a staging or production baseline, operators need a clear secret injection path, bootstrap-token story, and "demo only" guardrails.

Evidence:
- `deploy/podman/kube-spec.yaml` sets `POSTGRES_PASSWORD` to `vectis`.
- Database DSNs in the same spec embed `postgres://vectis:vectis@...`.
- Auth bootstrap configuration is documented, but the reference deploy does not provide an operator-safe secret workflow.

Acceptance criteria:
- Move reference credentials to generated secrets, external secret injection, or an explicit local-only setup script.
- Document how `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` is supplied, rotated, and removed after setup.
- Add a deployment checklist that distinguishes demo, staging, and production posture.
- Ensure docs warn loudly when a manifest is intentionally not production-safe.

Suggested slice:
Keep the current Podman stack as a fast demo path, but add a production-overlay or documented secret generation step before calling the deployment story ready.

## Recommended First Tranche

If this roadmap needs to turn into work tickets, start with these. They reduce ambiguity for almost every later feature:

1. API contract alignment: structured errors, route docs, compatibility policy.
2. Deployment repeatability: CI gate, Postgres integration lane, migration discipline.
3. Dispatch correctness: durable run handoff state, idempotency policy, cron claim semantics.
4. Operator repairability: CLI run/admin coverage, runbooks, SLOs/alerts, retention controls.
5. Execution safety: action-specific job validation, worker containment, secrets/redaction policy, audit/log durability decisions.

Everything else in this document can hang from those threads without forcing feature work to stop forever.

## Suggested Sequencing

### Phase 0: Make The Current System Legible

Status: todo

- Align as-built docs and route table with code.
- Add a public API/error contract.
- Add compatibility policy.
- Document deployment credential and bootstrap-token posture.

Exit criteria:
An operator and a future UI/client implementer can trust docs and responses without reading Go code.

### Phase 1: Make Deployment Repeatable

Status: todo

- Add CI quality gates.
- Add Postgres integration tests.
- Harden reference deployment.
- Move reference deployment secrets out of inline demo values or clearly mark the demo boundary.
- Add migration/rollback discipline.
- Add startup/dependency recovery decisions.

Exit criteria:
A release can be built, tested, deployed, inspected, and rolled back with documented expectations.

### Phase 2: Make Operations Repairable

Status: todo

- Expand CLI operational coverage.
- Add run lifecycle repair workflows.
- Add durable dispatch handoff state.
- Add retention controls.
- Add SLOs, alerts, and runbooks.
- Clarify multi-replica semantics and cron single-firer guarantee.
- Add cron schedule claim semantics.

Exit criteria:
Common failure modes have an observable signal and a documented repair path.

### Phase 3: Make Workload Execution Safe Enough

Status: todo

- Add action-specific job validation.
- Harden worker execution containment.
- Define secret handling/redaction.
- Define artifact/cache boundaries.
- Define audit durability and log streaming boundedness.
- Extend formal/model coverage around queue/claim/run invariants.

Exit criteria:
New job features can be added without expanding arbitrary execution risk or correctness ambiguity.

## Feature Readiness Checklist

Before starting a major new user-facing feature, answer yes to:

- Is the public API route/CLI/config contract documented?
- Does the feature have validation at the API boundary?
- Does it work against Postgres and SQLite where supported?
- Are migrations forward-safe and rollback expectations documented?
- Does it have runbook-quality observability?
- Does it interact safely with retries, duplicate requests, and process restarts?
- Does it respect namespace/RBAC and token-scope behavior?
- Does it avoid storing or logging secrets accidentally?
- Does it have an operator repair path if partial failure occurs?

## Parking Lot

Use this section for candidate work that is important but not required before feature work.

- OIDC/LDAP integration.
- Kubernetes-native manifests beyond the Podman reference.
- Autoscaling workers.
- Multi-site/federation.
- Frontend SPA.
- Plugin/hook catalog.
