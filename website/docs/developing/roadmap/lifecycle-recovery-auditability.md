# Lifecycle, Recovery, And Auditability Tranche

Snapshot date: 2026-06-28.

This page tracks the initial tranche for graceful startup/shutdown, backup and
recovery, and auditability. It is a gap map, not a shipped-behavior reference.
When one of these items graduates into a contract, move the durable promise into
the operating docs, reference docs, or an ADR.

## Goals

| Goal | Definition of done |
| --- | --- |
| Graceful lifecycle | Every daemon has an explicit startup preflight, readiness/serving contract, drain behavior, bounded shutdown path, and test coverage for cancellation. |
| Backup and recovery | Operators can identify, capture, verify, restore, and smoke-test every durable state surface with machine-readable evidence. |
| Auditability | Security-relevant actions have explicit event coverage, clear durability semantics, query/export paths, retention policy, and health signals. |

## Current Strengths

| Area | What exists today |
| --- | --- |
| Shared signal context | `internal/cli.ExecuteWithShutdownSignals` gives Cobra daemon roots a SIGINT/SIGTERM-cancelled context. Most daemons use it. |
| HTTP drain | `vectis-api`, `vectis-cell-ingress`, and `vectis-docs` use bounded HTTP shutdown. API readiness flips to `503` while draining. |
| Worker drain | `vectis-worker` stops dequeuing on shutdown while allowing the active execution to finish finalization work. Tests cover shutdown during a run. |
| DB startup gate | DB-backed services call `OpenReadyDBForRole`, which waits for migrations instead of applying them at runtime. |
| Queue recovery | Queue persistence uses a per-shard lock, fsynced WAL records, snapshots, replay tests, and restore-skew reconciler tests. |
| Dispatch repair | The reconciler redispatches queued runs and pending continuations from durable SQL payloads, and repairs some orphaned task finalization cases. |
| Log durability | `vectis-log` has durable local run files; worker durable log streams spool during log-service outages; log-forwarder spools have CRC protection and quarantine paths. |
| Artifact durability | `vectis-artifact` stores content-addressed blobs, verifies SHA-256, and locks each local storage directory to one active writer. |
| Restore docs | Backup/restore, production drills, repair runbooks, health check catalog, and scaling/restart docs already describe much of the operator posture. |
| Audit foundation | API audit events have a central registry, docs catalog tests, SQL persistence, drop/flush metrics, and health checks for audit loss signals. |

## Gap Map

### Graceful Startup And Shutdown

| Gap | Why it matters | Likely fix |
| --- | --- | --- |
| gRPC shutdown fallback coverage is incomplete | The shared gRPC helper now marks health not-serving when a health server is supplied, calls `GracefulStop`, and forces `Stop` after a timeout. Remaining work is to make the timeout configurable where needed, add any missing health services, and avoid command-local gRPC stop paths outside the helper. | Finish the migration audit across worker-control and private callback servers, then promote the bounded helper behavior into the operating lifecycle contract. |
| Lifecycle behavior is unevenly encoded | `vectis-log-forwarder` now relies on the Cobra shutdown context, and worker-core, artifact, and log gRPC serving paths use the shared helper. Other background loops still use local stop/join conventions. | Consolidate daemon lifecycle helpers and add focused cancellation-return tests for long-running loops. |
| Readiness/drain is mostly API-specific | API readiness turns unhealthy during drain and checks DB/queue. gRPC health services generally report serving once started and do not model dependency loss or drain. Background-loop daemons have no readiness endpoint. | Define per-binary readiness: serving, draining, dependency-unready, and startup-preflight-failed. Add gRPC health transitions where applicable. |
| Startup preflight contract needs deeper enforcement | The operating reference now has a per-daemon lifecycle matrix, and a static test requires every daemon command to appear. Remaining work is to test more of the matrix fields against command wiring and to promote dependency-readiness checks into machine-readable health where appropriate. | Expand contract tests beyond row coverage: startup preflight, health surface, drain helper, and durable state expectations. |
| Worker termination grace is operational, not enforced | Workers intentionally let active work finish, but supervisor grace periods are not derived from worker config or surfaced as a contract. | Document and expose recommended grace windows; add metrics/log messages for time spent in drain/finalize. |
| Background loops lack a common stop/join pattern | Catalog, reconciler, cell-ingress repair, TLS reload loops, source sync, log spool forwarding, and registry gossip each use local patterns. | Add small lifecycle tests or helper contracts for "context cancellation returns within N." |

### Backup And Recovery

| Gap | Why it matters | Likely fix |
| --- | --- | --- |
| Backup inventory needs deployment breadth | `vectis-cli backup inventory --format json` now emits local backup scope evidence, and `vectis-cli backup manifest` aggregates one or more host inventories into backup-set evidence. `vectis-cli backup verify --expect` can make missing expected inventory sources, service instances, database roles, paths, or categories fail. `vectis-cli backup expect podman` generates expectations for Podman simple/HA, and `vectis-cli backup expect linux` generates a Linux services-manifest baseline. Remaining work is deploy-specific dashboard inventory and arbitrary config-manager topology generation. | Add config-manager expectation generation for required hosts, cells, shards, dashboards, and externally managed secret/TLS stores. |
| Backup set manifest verifier needs custom topology inputs | `vectis-cli backup verify` now validates manifest shape, core database/queue/log/artifact evidence, dirty schema markers, missing or unreadable required paths, optional expected-topology JSON, generated Podman profile expectations, and generated Linux services-manifest baseline expectations. It does not yet generate expectations from arbitrary config-manager topology. | Add import/generation paths for Ansible/Terraform/Kubernetes inventory or a Vectis-native topology file. |
| No first-party backup/restore helpers | `vectis-cli database migrate`, health checks, and retention exist; backup/restore execution is delegated to the platform. | Start with inventory and verification. Consider SQLite-safe backup helpers later; keep Postgres backup delegated to the DB platform. |
| Restore drill is mostly manual | Unit tests cover queue restore and reconciler restore skew, but there is no full-stack drill that restores DB plus queue/log/artifact/secrets and runs the smoke sequence. | Add an e2e restore-drill lane for the reference deployment and a smaller local SQLite drill. |
| File-store integrity checks are fragmented | Artifact blobs verify by digest, log and queue stores have format tests, and spools have CRCs, but operators cannot run one integrity command. | Add `vectis-cli storage verify` subcommands or health checks for queue persistence, log storage, artifact blobs, log-forwarder spools, and worker pending log spools. |
| Worker pending log spool is not operationalized | Worker durable log streams replay pending spools, but the default path is temp-backed and not represented in backup inventory or health checks. | Make the worker log spool path configurable, add metrics/health for pending/quarantined files, and include it in backup docs. |
| Retention compliance controls are only partially linked | `vectis-cli retention cleanup` can now require backup manifest freshness, and run-scoped `vectis-cli retention holds` can protect selected terminal runs plus related run state from cleanup. Remaining work is explicit operator waivers, audit-range/export holds, and policy checks that decide when backup-manifest or hold review is mandatory. | Add waiver evidence fields, audit export/range holds, and policy checks for mandatory backup-manifest and hold-review enforcement. |
| Partial restore contracts are not executable | The docs describe partial restore outcomes, but there are few automated assertions for each expected inconsistency. | Add tests for database-only, queue-only, missing log store, missing artifact store, and missing secret/TLS restore postures. |

### Auditability

| Gap | Why it matters | Likely fix |
| --- | --- | --- |
| `fail_closed` is not route-enforced everywhere | The audit policy has `fail_closed`, but many route call sites ignore or only log `auditLog` errors. The catalog documents this caveat, so the term is currently persistence intent more than API behavior. | Decide the contract. Either enforce route failure for `fail_closed` events, or rename/document the durability levels so they do not imply caller failure. |
| No audit query/export API | Operators must query `audit_log` directly or rely on external SIEM/database tooling. | Add admin-only `GET /api/v1/audit/events` plus `vectis-cli audit list/export` with filters for time, type, actor, target, correlation ID, and JSON/CSV output. |
| Audit coverage is not mapped to routes | The event catalog lists emitted events, but there is no route/action coverage matrix that fails when a privileged mutation lacks an audit event. | Add tests similar to route inventory: mutation route plus auth action plus expected audit event(s). |
| Some operational/security evidence is outside `audit_log` | Dispatch events, execution security events, access logs, API security rejections, and audit events are separate surfaces. That is reasonable, but the joining contract is not centralized. | Document the evidence model: which table/log/metric answers which question, and how correlation IDs, run IDs, actor IDs, and dispatch events join. |
| Process lifecycle is not audited | Service startup, shutdown, readiness drain, migration wait, and restore/retention operations are not consistently recorded as auditable events. | Add structured service lifecycle logs first; consider DB-backed operator events only where a human-triggered command changes durable state. |
| Audit retention can erase evidence without export workflow | Retention deletes old `audit_log` rows by policy. Run-scoped holds now audit their own create/release events and protect run evidence, but there is no first-party audit export or audit-range hold mechanism before deleting old audit rows. | Add audit export, audit-range holds, retention dry-run evidence, and optional hold/manifest checks before deleting audit rows. |

## First Work Packages

| Package | Scope | Suggested tests |
| --- | --- | --- |
| Lifecycle contract matrix | Initial operating reference table landed for each daemon's startup checks, readiness, drain behavior, state closed on exit, and hard-stop fallback. | Static test requires every `cmd/*` daemon except `vectis-cli` to appear in the matrix. |
| Bounded gRPC shutdown | Shared helper landed for queue, registry, orchestrator, log, artifact, secrets, worker-control, and worker-core serving paths. | Unit test stuck-stream fallback; add command tests where the command has meaningful shutdown-owned state. |
| Log-forwarder lifecycle cleanup | Custom signal handling removed from `runLogForwarder`; shutdown now follows `cmd.Context()`. | Command-level test with cancelled context; existing forwarder shutdown tests. |
| Backup inventory MVP | `vectis-cli backup inventory --format json` landed for schema visibility, redacted DB role DSNs, queue/log/artifact/spool paths, secret/TLS path visibility, and service instance IDs. | CLI tests cover JSON evidence and DSN redaction; add split global/cell DB config coverage next. |
| Backup manifest MVP | `vectis-cli backup manifest --format json` builds manifest evidence from host inventories, and `vectis-cli backup verify` fails on missing core state, unreadable paths, and dirty schema markers. | CLI tests cover manifest aggregation and missing required path failure. |
| Backup expected topology MVP | `vectis-cli backup verify --expect expected-topology.json` landed for required inventory sources, service instances, database roles, paths, and categories. | CLI tests cover expected topology success and missing expected source/instance/path failures. |
| Podman backup expectation MVP | `vectis-cli backup expect podman --profile simple|ha --format json` landed for reference deployment queue/log/artifact shards, Postgres roles, secret store evidence, and config-path category checks. | CLI test covers HA generated expectations and verifier compatibility. |
| Linux backup expectation MVP | `vectis-cli backup expect linux --manifest deploy/linux/services.toml --format json` landed for service-manifest queue/log/artifact/log-forwarder paths, Postgres roles, encryptedfs secret store paths, cron/queue/log/artifact instance IDs, and `/etc/vectis` config evidence. | CLI test covers generated Linux expectations and verifier compatibility. |
| Retention backup gate MVP | `vectis-cli retention cleanup` can verify a backup manifest, optional expected topology, and optional max manifest age before dry-run or apply; output includes backup proof fields when supplied. | CLI tests cover missing manifest flag combinations, failed manifest verification, stale manifest rejection, and fresh expected-topology success. |
| Run-scoped retention hold MVP | `vectis-cli retention holds create/list/release` landed for compliance/legal/incident holds. Active holds skip terminal run cleanup for the run row, dispatch events, artifact manifests, task graph rows, local run logs, and artifact reference removal; create/release writes audit rows. | Retention tests cover hold create/list/release audit events, held cleanup counts, skipped terminal deletion, protected child rows, and protected artifact references. |
| Restore drill MVP | Add a local restore drill that backs up SQLite plus queue/log/artifact state, restores into a new data root, migrates, runs health, and triggers a smoke job. | E2E or integration test gated behind an existing long-running test target. |
| Audit durability enforcement decision | Pick route-enforced `fail_closed` or rename the durability levels. | Route tests with an auditor that returns errors for token/user/setup mutations. |
| Audit export MVP | Admin API and CLI list/export for `audit_log` with pagination and filtering. | Authz tests, pagination tests, metadata redaction tests, docs catalog update. |

## Contract Promotion Checklist

Before marking a gap closed:

- The behavior is implemented in code and not only described in docs.
- The behavior has at least one focused test and, for operator flows, a health/drill evidence path.
- The user-facing or operator-facing contract is documented in the durable docs.
- The failure mode says what data may be lost, what is repairable, and what cannot be reconstructed.
- Metrics, logs, audit rows, or CLI output let an operator prove the behavior happened.
- The release evidence template or drill checklist names the new proof when it matters for production readiness.

## Related Docs

| Need | Doc |
| --- | --- |
| Current planning home | [Planning](./planning.md) |
| Restart behavior | [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md) |
| Backup and restore | [Backup, Restore, And Disaster Recovery](../../operating/reliability/backup-restore.md) |
| Production drills | [Production Drills](../../operating/reliability/production-drills.md) |
| Health checks | [Health Check Catalog](../../operating/reference/health-check-catalog.md) |
| Audit events | [Audit Event Catalog](../../operating/reference/audit-event-catalog.md) |
| CLI coverage | [CLI Operational Coverage](../../operating/reference/cli-operational-coverage.md) |
