# Vectis — Agent Instructions

Self-hosted build/CI orchestrator in Go: services talk gRPC; the API exposes REST and SSE.

## Tooling & build environment

| Requirement | Version / tool |
|---|---|
| Go | `go 1.25.11` (see [`go.mod`](go.mod)) |
| CGO | Required locally (SQLite driver) — `CGO_ENABLED=1` (default). Disabled for container builds (`CGO_ENABLED=0` + `-tags=nosqlite`). |
| Protobuf compiler | `protoc` with local `protoc-gen-go` and `protoc-gen-go-grpc` plugins; override `PROTOC*` env variables if needed |
| Protobuf codegen | `mage proto` — invokes local `protoc`, output to `api/gen/go/` (read-only) |
| TLA+ (formal) | Java + `/opt/tla+/tla2tools.jar` (optional, for `mage formalVerification`) |
| Container | Podman (targets: `mage imageFull`, `mage imagesComponents`, `mage image api`, etc.) |

## Where to change

- **HTTP API, auth, RBAC** → `internal/api/`
- **SQL schema / queries** → `internal/dal/`, `internal/migrations/`
- **gRPC contracts** → `api/proto/` then `mage proto` (generated code in `api/gen/go/` is read-only)
- **Queue / orchestration / worker / logs / registry servers** → `internal/queue/`, `internal/orchestrator/`, `internal/job/` (execute), `cmd/worker/`, `internal/logserver/`, `internal/registry/`
- **Deployables / docs site** → `deploy/`, `website/docs/`
- **Reconciler invariants** → `internal/reconciler/`; formal model → `formal/tla/`

## Binaries (nineteen; `cmd/`)

| Binary | Role | Long-running? | DB? |
|--------|------|---------------|-----|
| `vectis-api` | REST (jobs, runs, SSE), metrics | yes | yes |
| `vectis-artifact` | Artifact blob storage (gRPC), metrics | yes | no |
| `vectis-cell-ingress` | Private cell-local execution ingress | yes | yes |
| `vectis-queue` | FIFO queue + metrics | yes | no |
| `vectis-registry` | Service discovery | yes | no |
| `vectis-log` | Log ingest (gRPC), SSE, metrics | yes | no |
| `vectis-orchestrator` | Hot run state + task choreography | yes | no |
| `vectis-worker` | Action tree + logs; worker-control gRPC | yes | yes |
| `vectis-worker-core` | Worker execution core over UDS | yes | no |
| `vectis-log-forwarder` | Sidecar: worker → log service | yes | no |
| `vectis-secrets` | Cell-local secret resolution broker | yes | yes |
| `vectis-spiffe` | Reference SPIFFE Workload + Entry API authority | yes | no |
| `vectis-cron` | Schedules → queue | yes | yes |
| `vectis-scm-poller` | SCM polling triggers → deduped trigger events → run dispatch | yes | yes |
| `vectis-catalog` | Cell catalog events → global catalog | yes | yes |
| `vectis-ui` | Static browser UI + API proxy | yes | no |
| `vectis-docs` | Static docs site | yes | no |
| `vectis-reconciler` | Stuck runs → queue | yes | yes |
| `vectis-local` | Dev stack + TLS + UI/docs | yes (supervisor) | yes |
| `vectis-cli` | HTTP client to API | no (one-shot) | yes |

**Ports, metrics ports, TLS defaults:** [`internal/config/defaults.toml`](internal/config/defaults.toml) and each `cmd/*/main.go`. Layout and env prefixes: [`cmd/AGENTS.md`](cmd/AGENTS.md).

## Rationale for key decisions

- **`database/sql` + hand-written SQL (no ORM):** explicit query control for the SQLite/Postgres duality; no ORM impedance mismatch for the job-tree model.
- **Hand-written mocks (no mockgen):** avoids generated code churn, makes mock behaviour explicit and reviewable, and keeps the mock package dependency-free.
- **`//go:embed` for defaults:** single source of truth (`defaults.toml`); no config file required at runtime, but env vars and flags override.
- **SQLite + Postgres:** SQLite for dev/test (zero deps) and single-node deploys; Postgres for production multi-service deployments.
- **No testify/ginkgo:** standard `testing` only — avoids external test framework dependency, keeps test output uniform, and simplifies the toolchain.

## Stack (pointers)

| Concern | Source of truth |
|---------|-----------------|
| Go module / deps | [`go.mod`](go.mod) |
| Protobuf codegen | [`api/proto/`](api/proto/) + `mage proto` |
| Default ports, DSN, feature flags | [`internal/config/defaults.toml`](internal/config/defaults.toml) |
| Env ↔ config binding | [`internal/config/`](internal/config/) (`BindEnv`, helpers), plus [`cmd/AGENTS.md`](cmd/AGENTS.md) |
| Containers | [`build/Containerfile`](build/Containerfile) |
| Docs site, architecture, security | [`website/docs/`](website/docs/) |

## Layout

| Path | Purpose |
|------|---------|
| `api/proto/`, `api/gen/go/` | Protobuf; generated Go (do not edit) |
| `cmd/` | Binaries — [`cmd/AGENTS.md`](cmd/AGENTS.md) |
| `internal/dal/`, `internal/migrations/` | SQL access + migrations |
| `internal/api/` | REST, auth, authz, rate limits |
| `internal/config/`, `internal/database/`, `internal/dbdrivers/` | Defaults, open DB, `_` driver import |
| `internal/queue/`, `internal/queueclient/`, `internal/orchestrator/`, `internal/registry/`, `internal/resolver/`, `internal/tlsconfig/` | Queue, hot run orchestration, discovery, dial, TLS reload |
| `internal/logserver/`, `internal/logforwarder/`, `internal/job/`, `internal/action/` | Execution + logging |
| `internal/cron/`, `internal/scmpoller/`, `internal/scmstream/`, `internal/scmtrigger/`, `internal/catalog/`, `internal/cellingress/`, `internal/reconciler/` | Schedules, SCM polling/stream routing, shared SCM trigger event dispatch, catalog application, cell ingress, recovery |
| `internal/interfaces/`, `internal/observability/`, `internal/cli/`, `internal/testutil/` | Logger, metrics/tracing, signals, tests |
| `tests/integration/` | Build tag `integration` — [`tests/AGENTS.md`](tests/AGENTS.md) |
| `deploy/`, `website/docs/`, `formal/tla/` | Kube/Grafana, docs site, TLA+ reconciliation |

## Common workflows

### Add a new HTTP API endpoint
1. Add the route, handler, auth action, and tests in `internal/api/`.
2. Update the route inventory tests if the endpoint changes the public surface.
3. Update [`website/static/openapi/v1.json`](website/static/openapi/v1.json); `mage lint` validates route metadata and local OpenAPI references.
4. Document user-facing request/response behavior in [`website/docs/using/api-reference.md`](website/docs/using/api-reference.md).
5. If the endpoint changes compatibility, auth, repair, or operator behavior, update the matching docs under `website/docs/`.

### Add a new gRPC RPC
1. Define protobuf messages and RPC in `api/proto/*.proto`.
2. `mage proto` regenerates `api/gen/go/`.
3. Register or update the server implementation in the appropriate `internal/` package.
4. If consumers need a domain-facing type, add an interface in `internal/interfaces/` and implement it.

### Add a new binary
1. Create `cmd/<name>/main.go` following the pattern in [`cmd/AGENTS.md`](cmd/AGENTS.md#pattern-services-with-viper)
2. Add the binary name to `appNames` in [`mage_build.go`](mage_build.go), and to image/package lists when applicable.
3. If it opens SQL, add the `_ "vectis/internal/dbdrivers"` import
4. Register its env prefix in the table in [`cmd/AGENTS.md`](cmd/AGENTS.md)

### Add a new database table
1. Add migration files in `internal/migrations/` (both SQLite and Postgres variants if the SQL diverges)
2. Add repository methods in `internal/dal/` using the existing pattern (`hand-written SQL + database/sql`)
3. Wire through `dal.NewSQLRepositories` if adding a new repository struct
4. Use `BeginTx` + deferred `Rollback` for transactional operations

### Add a new builtin action
1. Create `internal/action/builtins/<name>.go` implementing the action interface
2. Register the `uses` name so the worker can dispatch to it (see existing builtins for the pattern)

## Commands

Targets and recipes live in split Mage files (`mage_*.go`). Common dev loop:

```sh
mage proto                 # regenerate protobuf stubs
mage testQuick             # fast feedback (internal + cmd + api + sdk/examples/tools, 60s timeout)
mage lint                  # route security lint + golangci-lint
mage testIntegration       # full integration suite
```

## Configuration

- **Embedded defaults:** [`internal/config/defaults.toml`](internal/config/defaults.toml) (`//go:embed` in `internal/config/`).
- **Cobra/Viper per binary:** [`cmd/AGENTS.md`](cmd/AGENTS.md); **prefix strings** are also in each `cmd/*/main.go` (`rg SetEnvPrefix cmd/`).
- **Ad hoc env (no service prefix):** e.g. CLI token — see [`internal/config/api_auth.go`](internal/config/api_auth.go) and `rg 'VECTIS_' internal/config/`.

## Troubleshooting

- **`mage proto` fails:** ensure `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` are installed; override `PROTOC`, `PROTOC_GEN_GO`, or `PROTOC_GEN_GO_GRPC` if they are outside the defaults.
- **SQLite tests fail:** ensure `CGO_ENABLED=1` (CGO is required for `mattn/go-sqlite3`).
- **Integration tests fail:** check `VECTIS_DATABASE_DSN` and that the Postgres test instance is reachable. See [`tests/AGENTS.md`](tests/AGENTS.md).
- **Env prefix mismatch:** `rg SetEnvPrefix cmd/` is the source of truth; update [`cmd/AGENTS.md`](cmd/AGENTS.md) if the table disagrees.

## More detail

- [`internal/AGENTS.md`](internal/AGENTS.md) — DAL, mocks, config, observability, builtins
- [`api/AGENTS.md`](api/AGENTS.md) — protos, local codegen, gRPC registration map
- [`tests/AGENTS.md`](tests/AGENTS.md) — integration tag, grpctest, fuzz
- [`cmd/AGENTS.md`](cmd/AGENTS.md) — entrypoints, env prefix table
