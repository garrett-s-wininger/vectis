# Vectis - Modern Build System Planning Document

## 1. Project Overview

**Vectis** is a self-hosted **orchestrator** for **generic job graphs** and **CI/CD-style** workloads. It is **component-based**: each service has a **clear contract** and can be **replaced or reimplemented** in **any language** that honors those protocols (gRPC, REST, protos), not only the shipped Go implementations.

These goals are **ambitious** and **coherent**: some items (uniform distributed traces, a full audit story, a hook catalog, operator-gated retries everywhere, a **one-reference deploy** with **BYO** database and telemetry at scale) are **design intent** or **roadmap**—§2 onward and [ARCHITECTURE.md](ARCHITECTURE.md) separate **shipped** behavior from **target** detail.

**Design goals**

- **Control:** You run the stack; there is no required hosted control plane. Data and execution stay on infrastructure you choose. Trust boundaries for the open HTTP surface are in [SECURITY.md](SECURITY.md).
- **Operability:** Predictable failure behavior and calm upgrade paths; see [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md). Prefer explicit steps (migrations, Podman/Kube-style deploy) over magic.
- **Swappable, isolated components:** Any `vectis-*` service may be swapped for another implementation that speaks the **wire contracts**; each component should stay **understandable on its own** with minimal hidden coupling ([ARCHITECTURE.md](ARCHITECTURE.md)).
- **Auditability (intent):** A durable, reviewable trail of **user** and **automated** platform actions—direction of travel; not every dimension is fully realized in schema and APIs yet.
- **Distributed observability (intent):** Follow one logical run across **API → queue → worker → log / database**; cross-service correlation is **not uniform** everywhere today.
- **Scale and recovery:** Components **scale out** where state and contracts allow. **Job-level** automation that **re-executes or mutates external effects** (retries, remediation, “self-heal” of work) is **operator opt-in** and only where work is **explicitly marked safe**. **Process-level** restart or replacement (e.g. a crashed worker) is normal operations and does **not** assume job idempotence.
- **Explicit safety:** **Generic jobs** and **CI** patterns are first-class, but **no operation is assumed safe by default**—**design intent** where code still defaults broadly today.
- **Bounded extensibility:** Extend through **documented hooks** and structured surfaces (today: **job actions** and JSON/proto-shaped jobs in [ARCHITECTURE.md](ARCHITECTURE.md)), **not** unconstrained scripting across the control plane. A richer **hook catalog** is **roadmap**; the aim is capability **without** Jenkins-style arbitrary plugin sprawl.
- **Deploy posture (intent):** Aim for **one primary, fully capable deploy path**: **storage**, **all Vectis components**, and **clear integration points for observability** (metrics, traces, logs toward **OpenTelemetry-compatible** sinks)—**without** mandating a single vendor observability distro. The repo ships **testing- and CI-suitable** defaults (`vectis-local`, Podman Kube spec with Postgres—§14); **production** is expected to use **bring-your-own PostgreSQL** and **bring-your-own telemetry backends** where operators already run them. **Prometheus `/metrics`** on **`vectis-api`**, **`vectis-queue`**, **`vectis-worker`**, and **`vectis-log`** (OTEL SDK → Prometheus exporter) is **shipped**, with **ports and env** in [CONFIGURATION.md](CONFIGURATION.md). The **Podman kube spec** also bundles **Prometheus + Grafana** and a reference dashboard (`deploy/grafana/dashboards/`, `make grafana-kube-configmaps`). **Distributed traces**, **centralized service log aggregation**, and **production-grade** alerting/backends remain **roadmap** (see §10, §16, §6.2).

**Technical baseline (today)**

- **Protocols:** gRPC between internal services; JSON REST at the API edge—[ARCHITECTURE.md](ARCHITECTURE.md).
- **Persistence:** **SQLite** by default; **PostgreSQL** via `pgx` with the same embedded migrations (§2.5, [CONFIGURATION.md](CONFIGURATION.md)). Supporting **every** SQL vendor or dialect is **not** a goal. Multi-instance and HA are **operational** concerns on the Postgres path.
- **Dependencies and environments:** No required external services beyond storage for the current stack. Local full stack: **`vectis-local`**; containerized deploy: Podman and **`make deploy-podman`** ([README.md](../README.md), §14). **BYO Postgres and BYO observability sinks** are the expected production shape once you outgrow bundled defaults; **service metrics** are **scrapable Prometheus** on the four components above, while **full OTEL pipelines** (traces, log export, vendor distros) are still **operator-chosen**. Initial scale target: **hundreds of builds per day** until benchmarks justify more. **Dogfooding** (Vectis builds Vectis in CI) remains an aspiration.

**Naming conventions (implemented vs target):**

- **Pipeline file (target):** `.vectis.yml` — not yet loaded by the runtime; jobs today are JSON matching the protobuf `Job` shape (`api/proto/common.proto`).
- **Binaries (implemented):** `vectis-local`, `vectis-api`, `vectis-queue`, `vectis-worker`, `vectis-log`, `vectis-registry`, `vectis-cron`, `vectis-reconciler`, `vectis-cli` — built via `Makefile` into `bin/vectis-*`.
- **Container images (Podman / OCI; target registry naming):** `vectis/*` — built from [`build/Containerfile`](../build/Containerfile) via `make build-container` / `make images-components` (see [README.md](../README.md)).
- **Config paths (target prod layout):** `/etc/vectis/`, `/var/lib/vectis/`
- **Environment variable prefix:** `VECTIS_*` (`internal/config`)
- **API contracts:** Buf protos in `api/proto/`; generated Go in `api/gen/go/`
- **Implementation stack:** Go for API, queue, worker, log, registry, cron, reconciler, CLI. A **TypeScript/React SPA** is **target** only.

---

## 2. Architecture and current implementation

**Canonical as-built reference:** [ARCHITECTURE.md](ARCHITECTURE.md) — component diagram, processes, default ports, protocols (including worker **`Dequeue`** / **`TryDequeue`** and **SSE** for runs and logs), primary **data flows**, **persistence** overview, **job** shape and built-in actions, and the **REST** route table.

**Also:** [CONFIGURATION.md](CONFIGURATION.md) (env and flags), [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md) (outages), [SECURITY.md](SECURITY.md) (trust model for the open HTTP API), [GLOSSARY.md](GLOSSARY.md), [adr/README.md](adr/README.md).

**Shipped vs target:** Forward-looking design (**queue Fetch/Claim**, unified triggers, federation, etc.) lives in **§4 onward**. Today, run concurrency uses the **database** (`TryClaim`) with **`Dequeue`** on the queue — [adr/0003-database-claims-and-queue-deliveries.md](adr/0003-database-claims-and-queue-deliveries.md).

### 2.5 Persistence and schema notes

Details also summarized in [ARCHITECTURE.md](ARCHITECTURE.md) §Persistence; **migration file layout and operational discipline** in **§5** below.

- **Drivers:** SQLite default (`sqlite3`, XDG data home path in embedded defaults); **PostgreSQL** via `pgx` and `VECTIS_DATABASE_*` — [CONFIGURATION.md](CONFIGURATION.md). **SQL scope:** only these two backends are in scope; abstracting **all** SQL vendors is **not** a design goal.
- **Migrations:** Embedded under `internal/migrations/sqlite/` and `internal/migrations/postgres/`; runtime binaries **wait** for schema, **`vectis-cli migrate`** applies changes for admin/deploy flows (see §5).
- **DAL:** Stored job definitions, **runs** (status, dispatch, leases), cron schedules — `internal/dal/`.
- **Ephemeral runs:** `POST /api/v1/jobs/run` writes `job_definitions` `(job_id, version=1, definition_json)` and `job_runs.definition_version=1` (no `stored_jobs` row). The reconciler loads `stored_jobs` first, then falls back to `job_definitions` for recovery.
- **Future work:** Append-only versions for stored jobs, delete/version retention.

### 2.6 Failure domains and outages

See [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md). **Security:** [SECURITY.md](SECURITY.md).

---

## 3. Roadmap

Milestones build on the current stack; order is indicative.

### Milestone A — Documentation and planning alignment

- **Largely met:** standalone docs exist ([ARCHITECTURE.md](ARCHITECTURE.md), [CONFIGURATION.md](CONFIGURATION.md), [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md), [GLOSSARY.md](GLOSSARY.md), [SECURITY.md](SECURITY.md), [FEDERATION.md](FEDERATION.md) as deferred target, [adr/README.md](adr/README.md)). **Ongoing:** keep **§2–§3** aligned with the repo on each release; **§4+** remains target spec.
- **JSON/proto job definitions** are current; **`.vectis.yml`** remains target (Milestone E).

### Milestone B — Hardening

- **API:** authentication/authorization shipped — local users, API tokens, login endpoint, hierarchical RBAC, rate limits, and audit logging. See [SECURITY.md](SECURITY.md).
- **Cancellation:** API → worker control path (no `WorkerControl` gRPC today).
- **List jobs:** cursor pagination (`internal/api/server.go` TODOs).
- **Durability / observability:** **`vectis-reconciler`** covers DB–queue gaps after async enqueue; tighten **reconciler- and handoff-specific** visibility (alerts, client-visible status for failed handoffs) and any remaining edge cases (see `RunJob` commentary in `internal/api/server.go`). Baseline **service metrics** for API, queue, worker, and log are **shipped** (§10).

### Milestone C — Queue evolution (optional)

- Adopt **FetchJob / ClaimJob** semantics and capabilities from the target spec, **or** standardize on **Dequeue** and document scale limits explicitly.

### Milestone D — Triggers

- **Webhook** (and optional VCS polling); optionally **merge** `vectis-cron` with future webhook/poll into one trigger binary.

### Milestone E — Pipeline-as-code

- Parse **`.vectis.yml`** (or YAML) into the `Job` graph; validate at submit or checkout.

### Milestone F — Operations at scale

- **PostgreSQL** in production (supported today—**HA**, backup, and pool tuning are ops concerns); **queue persistence** is shipped—focus on **backup of queue data**, replication story if needed; **distributed tracing**, **Postgres exporter**-class signals, and **benchmarks** vs §17 goals (initial **Prometheus metrics** pass for core services is **done**—§10).

---

## Testing Strategy

### Philosophy: Vectis Tests Vectis

Vectis uses itself for CI/CD. Development workflow:

- Local: `make test` runs all packages (`go test ./...`)
- Fast feedback: `make test-race` for race detection; use `go test ./internal/foo/...` for scoped runs (there is no `make test-quick` in the `Makefile` today)
- PR review: Vectis instance runs full test suite on branch (aspiration)
- Release: Vectis builds and tests release artifacts (aspiration)

### Test Levels


| Level       | Scope                  | Runtime   | Purpose                                 |
| ----------- | ---------------------- | --------- | --------------------------------------- |
| Unit        | Individual packages    | <30s      | Fast feedback during development        |
| Integration | Component interactions | 2-5 min   | Validate API contracts, DB interactions |
| E2E         | Full build pipelines   | 10-30 min | Validate real-world scenarios           |
| Self-Test   | Vectis building Vectis | Full CI   | Production validation                   |

E2E and self-test rows are **aspirational** until dogfooding CI exists; day-to-day use **`make test`** and **`make test-integration`**.

### Local Development Testing

```bash
# From repository root (see Makefile)
make test              # All unit tests: go test ./...
make test-integration  # Integration tests: go test -tags=integration ./...
make test-race         # Race detector: go test -race ./...
```

### Test Infrastructure

- **Test doubles:** mocks under `internal/interfaces/mocks/`; in-memory queue where applicable
- **Integration:** `-tags=integration` (see `Makefile`); database tests use SQLite unless configured otherwise
- **Golden files:** expected outputs in `testdata/` where used

---

## Target specification (not yet implemented)

The sections below (**§4–§17**) describe **target** architecture and APIs. They are retained for design alignment. For **shipped** behavior, see **§2–§3** and the codebase.

**Note:** **Phase 1 / Phase 2+** wording in this part of the document is **legacy** — it means **earlier vs later target** behavior, not a fixed release schedule.

---

## 4. Component boundaries (target outline)

**Shipped** — repo is source of truth:

- REST: [ARCHITECTURE.md](ARCHITECTURE.md) (REST surface), `internal/api/server.go`
- gRPC: `api/proto/queue.proto`, `log.proto`, `registry.proto`; job graph: `common.proto`
- **Queue persistence:** on-disk **WAL / snapshot** for `vectis-queue` (optional; default data directory in `cmd/queue`; see [ARCHITECTURE.md](ARCHITECTURE.md) §Persistence).

**Target / not implemented** (roadmap only):

- Richer REST: projects, artifacts, HTTP cancel, cursor-paginated job lists
- Optional **v2 queue**: `FetchJob` / `ClaimJob` / status to queue; multi-queue; worker capabilities (today: `Dequeue` + DB **TryClaim** for concurrency; see [adr/0003-database-claims-and-queue-deliveries.md](adr/0003-database-claims-and-queue-deliveries.md))
- **Worker control** + **heartbeat** for cancel and orphan handling
- **Unified triggers** (webhook, poll, cron, manual); shipped: **vectis-cron** + API
- **`.vectis.yml`** in repo, optional `vectis/overrides` branch — not parsed today (jobs are JSON/proto)

---

## Error handling

- **Transient vs permanent:** backoff/retry for network and rate limits; fail fast for bad input and test failures (behavior lives in worker, queue client, and API handlers).
- **Step/job failure:** executor reports run state via DAL; target design may add queue `ReportJobStatus` — see **§4**.
- **Partitions / dependencies down:** shipped stack uses a **SQL** database (SQLite default) and a **queue** with optional persistence (default-on WAL); **reconciler** helps when enqueue lags the DB. Target **heartbeat** and richer orphan policy remain **§3**.
- **REST errors (target shape):** JSON with `error.code`, `message`, optional `details` — not fully standardized today.

---

## 5. Data Model

### Implemented schema

Authoritative DDL is in **`internal/migrations/sqlite/`** and **`internal/migrations/postgres/`** (embedded via `//go:embed`). Runtime services call **`database.WaitForMigrations`** only; `vectis-local` initializes the local SQLite schema for development, and Postgres deployments should run **`vectis-cli migrate`** as an explicit admin/deploy step.

Current migrations include, among others: **stored jobs** (id + JSON definition), **job runs** (indices, lease/dispatch, failure reason), and **cron schedules** for `vectis-cron`.

Data access: **`internal/dal`** repositories over **`database/sql`** — hand-written SQL, no ORM.

### Target entity model (not fully implemented)

The long-term design may add first-class **projects**, **queues**, granular **job status**, **steps** / **step results**, **artifacts**, **users** / **tokens**, **notifications**, and **audit logs**. None of that should be assumed without checking `internal/migrations/`. **§4** is a target outline; **§2** and [ARCHITECTURE.md](ARCHITECTURE.md) describe what runs today.

### Database abstraction

- **Shipped:** SQLite by default; Postgres via `VECTIS_DATABASE_DRIVER=pgx` and DSN; repository interfaces in `internal/dal`

### Database migration strategy

- **golang-migrate** with SQL up/down files under `internal/migrations/sqlite/` and `internal/migrations/postgres/`
- Embedded in **`vectis-cli migrate`** (and `database.Migrate` for tests/tools); runtime services do not auto-migrate

**Migration layout (baseline):**

```
internal/migrations/sqlite/001_initial.up.sql
internal/migrations/sqlite/001_initial.down.sql
internal/migrations/postgres/001_initial.up.sql
internal/migrations/postgres/001_initial.down.sql
```

**Safe vs unsafe changes:** Adding nullable columns/tables/indexes is safer; drops/renames/types need coordination and down-migration discipline.

**Development:** Tear-down and recreate is acceptable. **Production:** Run migrations before deploying new binaries; test rollback paths.

---

## 6. Storage architecture

### 6.1 Logs (shipped + target)

Workers stream **gRPC** `StreamLogs` to the log service (default port **8083**). Clients use **SSE**: `/sse/logs/{id}` on the log service (**8084**) and/or `GET /api/v1/sse/jobs/{id}/runs` on the API (**8080**) — see [ARCHITECTURE.md](ARCHITECTURE.md) §Protocols and §REST surface.

**Target:** filesystem vs object-store backends; optional Loki/OTEL forwarding; per-project retention and max size — not fully productized in config yet.

**SSE:** long-lived `GET`, `text/event-stream`; reconnect with `?since=` / `Last-Event-ID` where supported; ordering via chunk sequence in protos.

### 6.2 Artifacts, cache, secrets, system logs (target)

**Artifacts:** API upload/download, chunked upload, worker → object store — **not shipped** as in older specs; design when needed.

**Dependency cache:** shared filesystem/S3/Redis + pipeline `cache:` keys — **target**.

**Secrets:** Vault or encrypted local store; worker fetch at runtime — **target** (checkout may use env today).

**System / component logs:** processes still log primarily to **stderr** (structured JSON optional on the API—[CONFIGURATION.md](CONFIGURATION.md)). **Central aggregation** (Loki, cloud log sinks, OTEL log export) is a **target** ops concern. **Prometheus metrics** for core services are **shipped** (§10).

---

## 7. Workspace and VCS

**Shipped:** ephemeral temp dir per run (`internal/job/executor.go`); **`checkout`** builtin for Git when used in the job graph.

**Target:** optional workspace reuse, shared cache roots, resource requests/limits and cgroups, TOML `workspace_root` / `reuse_workspace`, pluggable VCS with credentials from a secret backend.

---

## 8. Job recovery, cancellation, cleanup

**Shipped:** **reconciler** redispatches runs stuck in queued state; runs persisted in the **configured SQL database**; executor cleans temp workspace after a run.

**Target:** per-project auto-retry counts; HTTP cancel → worker RPC (needs worker address / heartbeat); optional preserved failed workspaces; artifact/log retention jobs; generic orphan sweeps — distinct from reconciler.

---

## 9. Rate limiting and quotas

**Target:** per-project and per-token limits (e.g. Redis-backed token bucket), optional per-worker resource quotas, pipeline-level `concurrency` in `.vectis.yml`. **Not enforced** on the open API today.

---

## 10. Metrics and observability

**Shipped (initial metrics pass):** **`vectis-api`**, **`vectis-queue`**, **`vectis-worker`**, **`vectis-log`**, and **`vectis-reconciler`** expose **Prometheus** **`/metrics`** (OpenTelemetry Go metrics → Prometheus exporter). Instrumentation includes, among other things, **HTTP request metrics** (API), **queue depth / in-flight deliveries**, **worker job outcomes and duration**, **log gRPC/SSE pressure** (chunks, drops, active SSE), reconciler scan/repair metrics, and **`database/sql` pool gauges** where the process uses a DB. **Listen ports** default to **8080** (API, metrics on same HTTP server), **9081** (queue), **9082** (worker), **9083** (log), **9085** (reconciler)—overridable; see [CONFIGURATION.md](CONFIGURATION.md). The **Podman kube** reference ([`deploy/podman/kube-spec.yaml`](../deploy/podman/kube-spec.yaml)) runs **Prometheus** scraping those targets and **Grafana** with a bundled overview dashboard (`deploy/grafana/dashboards/`; regenerate `deploy/podman/grafana-configmaps.gen.yaml` via **`make grafana-kube-configmaps`** when dashboards change).

**Target / next passes:** OTEL **traces** end-to-end, **centralized service logs** (Loki/ELK/etc.), **postgres_exporter** or managed DB metrics, richer SLOs and alerting, and optional **Alloy**-style fan-out—without mandating one vendor stack.

Operator-facing **log/run streaming** (job output) is **§6.1** and [ARCHITECTURE.md](ARCHITECTURE.md) (protocols / REST)—distinct from **component** `/metrics`.

---

## 11. Service discovery

**Shipped:** **vectis-registry** gRPC — components resolve queue and log addresses ([ARCHITECTURE.md](ARCHITECTURE.md) §Service discovery).

**Target:** Kubernetes DNS/services; Consul/etcd or static inventory for large bare-metal. **Shipped:** **`vectis-api`** exposes **`GET /health/live`** and **`GET /health/ready`** (see [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md#vectis-api)); other components rely primarily on **gRPC health** or process-level checks—uniform HTTP health across every binary is not a goal yet.

---

## 12. Security and authentication

**Shipped:** HTTP API authentication with local users, bcrypt passwords, API tokens, scoped permissions, login endpoint, and hierarchical RBAC (viewer/trigger/operator/admin roles with namespace inheritance). Rate limits and async audit logging are active. gRPC peers remain **unauthenticated** beyond optional TLS — see [SECURITY.md](SECURITY.md).

**Target:** OIDC/session tokens; worker/trigger static tokens and optional **mTLS** on internal gRPC; webhook HMAC/replay controls when triggers exist.

---

## 13. Federation and multi-site deployment

**Status:** Not implemented. The repository targets a **single-site** stack (see [ARCHITECTURE.md](ARCHITECTURE.md) and §2 above).

Multi-site design (central config, per-site execution, frontend aggregation across sites) is archived for future reference only:

- **[FEDERATION.md](FEDERATION.md)** — full target spec (diagrams, routing, secrets, trigger site selection).

Treat federation as **out of scope** until single-site production hardening and roadmap milestones are satisfied.

---

## 14. Deployment

### Development

**Implemented:** build binaries with `make build` (outputs `bin/vectis-*`). Run the full local stack with `vectis-local` (starts registry, queue, log, worker, cron, reconciler, api — see `cmd/local/main.go`). Individual services are separate binaries (e.g. `bin/vectis-api`, `bin/vectis-worker`).

**Target / not implemented:** optional future `./vectis run …` single-binary UX (today `vectis-local` supervises separate `vectis-*` processes and already runs the full shipped stack); dedicated `heartbeat-service`; unified `trigger` binary; frontend dev server on port 3000.

```bash
make build
./bin/vectis-local   # or add bin/ to PATH
```

### Production

**Intent:** One coherent deploy (storage + all services + **observability integration points**). Core services **emit Prometheus metrics** (`/metrics`); the bundled Kube spec also runs **Prometheus + Grafana** as a **reference** stack. Operators typically **bring their own Postgres** and may **replace or extend** telemetry (traces, log aggregation, long-term metrics storage) with **OTEL-compatible** or equivalent backends at scale—aligned with §1 **Deploy posture** and §16.

**Podman**

**Shipped reference:** [`deploy/podman/kube-spec.yaml`](../deploy/podman/kube-spec.yaml) and **`make deploy-podman`** (`podman play kube`) — see [README.md](../README.md). The same spec includes **Prometheus** and **Grafana** alongside Vectis services for a **demo monitoring bundle** (see §10).

**Compose-style example** (single-site, aligns with `defaults.toml` ports): registry **8082**, queue **8081**, log gRPC **8083**, log SSE **8084**, API **8080**; **Prometheus scrape ports** for metrics: queue **9081**, worker **9082**, log **9083** (API metrics on **8080**). Workers and other services resolve addresses via **registry** in the shipped stack — wire `VECTIS_*` / config to match your images. Runnable with **`podman compose`** if you prefer that layout over the Kube spec.

```yaml
services:
  vectis-registry:
    image: vectis/registry:latest
    ports: ["8082:8082"]
  vectis-queue:
    image: vectis/queue:latest
    ports: ["8081:8081", "9081:9081"] # gRPC + /metrics
  vectis-log:
    image: vectis/log:latest
    ports: ["8083:8083", "8084:8084", "9083:9083"] # gRPC, SSE, /metrics
  vectis-api:
    image: vectis/api:latest
    ports: ["8080:8080"] # REST + /metrics
    environment:
      - VECTIS_DATABASE_DRIVER=sqlite3   # or pgx for Postgres
      - VECTIS_DATABASE_DSN=/var/lib/vectis/db.sqlite3   # or postgres://… on all DB consumers
  vectis-worker:
    image: vectis/worker:latest
    ports: ["9082:9082"] # /metrics (optional; publish if Prometheus runs outside the pod network)
    # Same VECTIS_DATABASE_* as other writers; registry + optional pins per CONFIGURATION.md
  vectis-cron:
    image: vectis/cron:latest
  vectis-reconciler:
    image: vectis/reconciler:latest
```

**Not in this sketch:** frontend SPA, heartbeat service, unified webhook trigger — add when implemented.

**Kubernetes**

- Deployments for registry, queue, log (expose gRPC + SSE), API, worker, cron, reconciler; scale workers horizontally.
- ConfigMap / Secrets for DB and service config. Optional future: frontend, heartbeat, webhook trigger.

### Configuration

**Shipped defaults** are embedded in `internal/config/defaults.toml` (API, registry, queue, log gRPC/SSE ports, discovery timings, database driver/DSN template). Runtime configuration is primarily **environment variables** per service — see [CONFIGURATION.md](CONFIGURATION.md). The `config.toml` fragment below is **illustrative** only (Viper/TOML file loading is not the primary path today). Keys for heartbeat, unified triggers, multi-queue remain **target-only**.

```toml
# Illustrative — not exhaustive
[database]
    driver = "sqlite3"   # or pgx + postgres DSN; see CONFIGURATION.md / defaults.toml
    dsn = "/var/lib/vectis/db.sqlite3"

[api]
    host = "0.0.0.0"
    port = 8080

[registry]
    port = 8082

[queue]
    port = 8081
    metrics_port = 9081

[worker]
    metrics_port = 9082

[log]
    metrics_port = 9083
[log.grpc]
    port = 8083
[log.sse]
    port = 8084
# Target: [log] storage_backend, S3 path, etc.
# Target: heartbeat, webhook trigger, worker subscribed_queues — see §4
```

### Upgrade & Rollback Strategy

**Early development:** stop/start; run **`vectis-cli migrate`** (or `make deploy-podman`) when the schema changes. **Production:** deploy one consistent version across binaries; backup DB; apply migrations before or alongside rollout; redeploy; rollback with previous artifacts + DB restore if migrations cannot be reversed.

---

## 15. Architectural strengths

Mix of **shipped** behavior and **target** intent (see [ARCHITECTURE.md](ARCHITECTURE.md) / §2 vs §4). The running system is intentionally smaller than the full target spec.

- **Pull-based workers / simple queue:** Workers call `Dequeue`; queue is a focused FIFO service with **default on-disk persistence** (see [ARCHITECTURE.md](ARCHITECTURE.md), §4).
- **SSE for logs and runs:** Single HTTP-friendly streaming model for browsers and tools (§6.1, [ARCHITECTURE.md](ARCHITECTURE.md) §Protocols).
- **Registry for internal discovery:** Queue and log addresses resolved without hard-coding every client ([ARCHITECTURE.md](ARCHITECTURE.md)).
- **Pluggable storage (target):** Logs and artifacts — filesystem vs object store (§6).
- **Pipeline-as-code (target):** `.vectis.yml` and overrides; today jobs are JSON/proto-shaped (§1, §3).
- **Multi-site (deferred):** See [FEDERATION.md](FEDERATION.md), not the current codebase.

---

## 16. Open questions — resolved (summary)

**Status:** Shipped / Partial / Planned / N/A — see **§2** and [ARCHITECTURE.md](ARCHITECTURE.md) for shipped ground truth. Older detail lived in prior revisions of §4+; **§4** is now a short outline.

| Topic | Decision (intent) | Status |
| --- | --- | --- |
| Language & protocols | Go; REST at API edge, gRPC internally | Shipped |
| Log / run streaming | Worker → log service (gRPC) → clients (**SSE**) | Shipped — [ARCHITECTURE.md](ARCHITECTURE.md), §6.1; retention/Loki-style target |
| Queue | `Dequeue` + optional disk persistence (WAL); DB **TryClaim** for run concurrency; Fetch/Claim queue API target | Partial — [ARCHITECTURE.md](ARCHITECTURE.md), [adr/0003](adr/0003-database-claims-and-queue-deliveries.md) |
| Persistence | SQLite + Postgres migrations embedded; queue WAL shipped | Partial — HA / ops hardening roadmap |
| Registry | Internal service discovery; optional **pinned** addresses | Shipped |
| Job model | Stored jobs + runs + ephemeral path; JSON/proto graph | Shipped — richer entities (projects, steps table) target |
| Pipeline-as-code | `.vectis.yml`, overrides branch | Planned — JSON jobs today |
| Triggers | Cron service + API; webhook / unified trigger | Partial |
| API security | Auth, RBAC, rate limits, audit logging | **Shipped** — see [SECURITY.md](SECURITY.md) |
| Worker/trigger auth | Tokens, optional mTLS | Planned |
| Cancellation | API → worker control RPC | Planned |
| Heartbeat / orphans | Dedicated service + admin paths | Planned — reconciler differs today |
| Artifacts & secrets | Pluggable storage; Vault-style secrets | Planned |
| Multi-site | Federation model | Planned — **deferred**; [FEDERATION.md](FEDERATION.md) |
| Observability | Prometheus **metrics** on API, queue, worker, log; **Grafana + Prometheus** in Podman kube demo; traces / centralized logs / full OTEL stack | **Partial** — [CONFIGURATION.md](CONFIGURATION.md), §10, `deploy/grafana/dashboards/` |
| Dogfooding CI | Vectis builds Vectis | Aspiration |

---

## 17. Performance and scaling

**Shipped path** ([ARCHITECTURE.md](ARCHITECTURE.md), §2): FIFO queue (`Dequeue`, optional persistence), **TryClaim** in DB, SQLite default or PostgreSQL — suitable for **low to moderate** throughput until measured.

**Target / unvalidated:** sub-second dispatch with claim-based queue + tuned PostgreSQL; horizontal queue shards; SSE fan-out for viewers. **Do not** treat old numeric tables (10k TPS, 10k workers, etc.) as commitments — benchmark after the queue and DB story match that architecture.

---
