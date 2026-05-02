# Vectis — Agent Instructions

Self-hosted build/CI orchestrator in Go: services talk gRPC; the API exposes REST and SSE.

## Tooling & build environment

| Requirement | Version / tool |
|---|---|
| Go | `go 1.25.7` (see [`go.mod`](go.mod)) |
| CGO | Required locally (SQLite driver) — `CGO_ENABLED=1` (default). Disabled for container builds (`CGO_ENABLED=0` + `-tags=nosqlite`). |
| Buf | `npx @bufbuild/buf` (pinned in [`Makefile`](Makefile) line 2) |
| Protobuf codegen | `make proto` — invokes Buf, output to `api/gen/go/` (read-only) |
| TLA+ (formal) | Java + `/opt/tla+/tla2tools.jar` (optional, for `make formal-verification`) |
| Container | Podman (targets: `make image-full`, `make image-api`, etc.) |

## Where to change

- **HTTP API, auth, RBAC** → `internal/api/`
- **SQL schema / queries** → `internal/dal/`, `internal/migrations/`
- **gRPC contracts** → `api/proto/` then `make proto` (generated code in `api/gen/go/` is read-only)
- **Queue / worker / logs / registry servers** → `internal/queue/`, `internal/job/` (execute), `cmd/worker/`, `internal/logserver/`, `internal/registry/` (see [`api/AGENTS.md`](api/AGENTS.md))
- **Deployables / deep docs** → `deploy/`, `docs/`
- **Reconciler invariants** → `internal/reconciler/`; formal model → `formal/tla/`

## Binaries (ten; `cmd/`)

| Binary | Role | Long-running? | DB? |
|--------|------|---------------|-----|
| `vectis-api` | REST (jobs, runs, SSE), metrics | yes | yes |
| `vectis-queue` | FIFO queue + metrics | yes | yes |
| `vectis-registry` | Service discovery | yes | no |
| `vectis-log` | Log ingest (gRPC), SSE streams, metrics | yes | yes |
| `vectis-worker` | Action tree + logs; worker-control gRPC | yes | no |
| `vectis-log-forwarder` | Sidecar: worker → log service | yes | no |
| `vectis-cron` | Schedules → queue | yes | yes |
| `vectis-reconciler` | Stuck runs → queue | yes | yes |
| `vectis-local` | Dev stack + TLS | yes (supervisor) | no |
| `vectis-cli` | HTTP client to API | no (one-shot) | no |

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
| Protobuf lint + codegen | [`buf.yaml`](buf.yaml), [`buf.gen.yaml`](buf.gen.yaml) (repo root; module path `./api/proto`) |
| Default ports, DSN, feature flags | [`internal/config/defaults.toml`](internal/config/defaults.toml) |
| Env ↔ config binding | [`internal/config/`](internal/config/) (`BindEnv`, helpers), plus [`cmd/AGENTS.md`](cmd/AGENTS.md) |
| Containers | [`build/Containerfile`](build/Containerfile) |
| Architecture / security prose | [`docs/`](docs/) |

## Layout

| Path | Purpose |
|------|---------|
| `api/proto/`, `api/gen/go/` | Protobuf; generated Go (do not edit) |
| `cmd/` | Binaries — [`cmd/AGENTS.md`](cmd/AGENTS.md) |
| `internal/dal/`, `internal/migrations/` | SQL access + migrations |
| `internal/api/` | REST, auth, authz, rate limits |
| `internal/config/`, `internal/database/`, `internal/dbdrivers/` | Defaults, open DB, `_` driver import |
| `internal/queue/`, `internal/queueclient/`, `internal/registry/`, `internal/resolver/`, `internal/tlsconfig/` | Queue, discovery, dial, TLS reload |
| `internal/logserver/`, `internal/logforwarder/`, `internal/job/`, `internal/action/` | Execution + logging |
| `internal/cron/`, `internal/reconciler/` | Schedules, recovery |
| `internal/interfaces/`, `internal/observability/`, `internal/cli/`, `internal/testutil/` | Logger, metrics/tracing, signals, tests |
| `tests/integration/` | Build tag `integration` — [`tests/AGENTS.md`](tests/AGENTS.md) |
| `deploy/`, `docs/`, `formal/tla/` | Kube/Grafana, prose, TLA+ reconciliation |

## Common workflows

### Add a new API endpoint
1. Define protobuf message + RPC in `api/proto/*.proto`
2. `make proto` → regenerates `api/gen/go/`
3. Register gRPC server in the appropriate `internal/` package
4. If consumers need a domain-facing type, add an interface in `internal/interfaces/` and implement it

### Add a new binary
1. Create `cmd/<name>/main.go` following the pattern in [`cmd/AGENTS.md`](#pattern-services-with-viper)
2. Add `$(OUT_DIR)/vectis-<name>` to `$(BINARIES)` in [`Makefile`](Makefile)
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

Targets and recipes live in the root [`Makefile`](Makefile): `build`, `test`, `test-integration`, `proto`, `format`, `deploy-podman`, `formal-verification`. Common dev loop:

```sh
make proto                 # regenerate protobuf stubs
make test-quick            # fast feedback (internal + cmd + api, 60s timeout)
make test-integration      # full integration suite
```

## Configuration

- **Embedded defaults:** [`internal/config/defaults.toml`](internal/config/defaults.toml) (`//go:embed` in `internal/config/`).
- **Cobra/Viper per binary:** [`cmd/AGENTS.md`](cmd/AGENTS.md); **prefix strings** are also in each `cmd/*/main.go` (`rg SetEnvPrefix cmd/`).
- **Ad hoc env (no service prefix):** e.g. CLI token — see [`internal/config/api_auth.go`](internal/config/api_auth.go) and `rg 'VECTIS_' internal/config/`.

## Troubleshooting

- **`make proto` fails:** run `npx @bufbuild/buf mod update` in `api/proto/` to sync dependencies, then retry.
- **SQLite tests fail:** ensure `CGO_ENABLED=1` (CGO is required for `mattn/go-sqlite3`).
- **Integration tests fail:** check `VECTIS_DATABASE_DSN` and that the Postgres test instance is reachable. See [`tests/AGENTS.md`](tests/AGENTS.md).
- **Env prefix mismatch:** `rg SetEnvPrefix cmd/` is the source of truth; update [`cmd/AGENTS.md`](cmd/AGENTS.md) if the table disagrees.

## More detail

- [`internal/AGENTS.md`](internal/AGENTS.md) — DAL, mocks, config, observability, builtins
- [`api/AGENTS.md`](api/AGENTS.md) — Buf, protos, gRPC registration map
- [`tests/AGENTS.md`](tests/AGENTS.md) — integration tag, grpctest, fuzz
- [`cmd/AGENTS.md`](cmd/AGENTS.md) — entrypoints, env prefix table
