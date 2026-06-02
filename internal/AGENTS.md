# Internal — Backend conventions

Implementation under `internal/`. Repo map: [`../AGENTS.md`](../AGENTS.md). Entrypoints and Viper prefixes: [`../cmd/AGENTS.md`](../cmd/AGENTS.md).

## Interfaces and mocks

**Interfaces** live next to **consumers**, not next to implementations. Shared cross-package interfaces live in `internal/interfaces/`; for example, `internal/interfaces/queue.go` is consumed by `internal/cron/` and `internal/api/`, while the implementation is in `internal/queueclient/`.

**Mocks** are hand-written in `internal/interfaces/mocks/` (no mockgen). Each mock:
- Exposes `XxxErr` fields for error injection (e.g. `enqueueErr`, `dequeueErr`).
- Includes a `var _ Foo = (*MockFoo)(nil)` compile-time check to enforce interface satisfaction.
- Uses `sync.Mutex` for concurrent-safe recording/verification.

Usage in tests: import `"vectis/internal/interfaces/mocks"`, inject via `SetXxxError`, call through the mock, then assert recorded state.

## `internal/dal/` — Data access

Hand-written SQL + `database/sql` (no ORM). This is a deliberate choice: the job-tree model (nested `Node` → `Run` hierarchy) maps poorly to ORM abstractions, and explicit query control is needed for the SQLite/Postgres duality.

- **`rebindQueryForPgx()`** maps `?` → `$n` placeholders when `VECTIS_DATABASE_DRIVER=pgx`. Write SQL with `?` placeholders as if targeting SQLite; the rebind transparently handles Postgres.
- **`normalizeSQLError()`** maps driver-specific errors to domain errors in `internal/interfaces/` (e.g. `ErrNotFound`, `ErrConflict`).
- **`dal.NewSQLRepositories`** defines the repository surface (Jobs, Runs, Schedules, Auth, Namespaces, RoleBindings).
- **Transactions:** `BeginTx` + deferred `Rollback`. If `Rollback` returns an error, log it (do not shadow the original error). Commit explicitly.
- **SQL style:** prefer SQLite-friendly SQL unless a query genuinely diverges — gate diverging queries with a driver check at the call site, not with build tags.

### Adding a new repository method

1. Add the SQL string in the repository file (e.g. `jobs.go`).
2. Add the method signature to the interface in `internal/interfaces/`.
3. Implement using `database/sql` patterns (see existing methods for row scanning idioms).
4. Add a mock method in `internal/interfaces/mocks/`.

## `internal/api/` — Route security

HTTP routes are protected by default: a zero-value `routeAuthPolicy{}` normalizes to `admin:*`.

Public route opt-outs must use `routeAuthPolicy{mode: routeAuthPublic}` and include a nearby source comment starting with `public route:` that explains why the route is unauthenticated. The source-level lint binary lives in `tools/vectis-lint`; run it directly with `make lint-api-routes`. It also runs as part of `make lint`.

## Lint expectations

`make lint` runs the first-party route security lint before golangci-lint. Staticcheck is part of that suite; in tests, make nil handling explicit enough for staticcheck to prove safety. For example, return after `t.Fatal` before dereferencing a possibly nil pointer, or copy pointer-backed values (such as `*http.Cookie`) into value variables after a presence check.

## `internal/config/`

- **Defaults:** [`defaults.toml`](config/defaults.toml) (`//go:embed` in `config/load.go`).
- **Validators:** `Validate*` functions sit alongside the options struct they guard (e.g. `api_auth.go` has `ValidateAPIAuthConfig`, `grpc_tls.go` has `ValidateTLSOptions`). Validators return a descriptive error string — callers log them at startup and exit.
- **Binaries wire flags** in `cmd/*/main.go` and read via helpers in `config/*.go` (e.g. `config.APIPort()`, `config.GRPCTLSEnabled()`).
- **Env override:** use `BindEnv` helpers or `viper.SetEnvPrefix` + `AutomaticEnv()` per binary. Ad-hoc env vars (no prefix, e.g. `VECTIS_API_TOKEN`) are read via `os.Getenv` in the relevant package — see `api_auth.go`.

## Observability

**Package:** `internal/observability/`

Each service chooses its init entrypoints from `cmd/*/main.go` (search `observability.` to find wired examples).

| Aspect | Entry / pattern |
|--------|-----------------|
| Tracing | `observability.SetupTracing(ctx, serviceName, opts...)` — OTLP exporter via env-configured endpoint |
| Metrics | Prometheus via OTel exporter; `observability.RegisterPoolMetrics(db)` for SQL pool stats |
| Correlation | `observability/correlation.go` — gRPC metadata propagation (trace ID → log fields) |
| Per-service metrics | `apimetrics.go`, `queuemetrics.go`, `workermetrics.go`, `logmetrics.go` — init in the binary's `runXxx` |

**To add a new metric:** create a `<name>metrics.go` in `observability/` following the existing pattern (e.g. `queuemetrics.go`), wire init in the binary's `runXxx`, and expose domain-specific counters/histograms. If operators should alert or troubleshoot with it, update the relevant docs under `website/docs/operating/`.

### Logger

Interface in `interfaces/logger.go`. Usage:

```go
log := interfaces.NewLogger("component")
log.WithField("key", value).Info("message")
```

JSON output when `VECTIS_LOG_FORMAT=json` (see `defaults.toml`). The `internal/cli/` package provides `SetLogLevel` for flag-driven level configuration.

## `internal/cli/` — Signal handling and shared helpers

- **`ExecuteWithShutdownSignals`:** wraps `cobra.Command.ExecuteContext` with a context cancelled on SIGINT/SIGTERM. All daemon `main()` functions must use this.
- **`SetLogLevel`:** reads `log_level` from viper and applies it to the logger.

## `internal/action/` — Built-in actions

Builtins live in `internal/action/builtins/`. Currently registered action types are `builtins/shell`, `builtins/checkout`, and `builtins/sequence`; `registry.go` is the builtin resolver, not a user-facing action. Each action implements the interface from `internal/action/action.go` and registers its `uses` name so the worker can dispatch to it.

**To add a new builtin:**
1. Create `internal/action/builtins/<name>.go` implementing the action interface.
2. Register the `uses` name (see existing builtins for the registration call).
3. Add tests in `internal/action/builtins/<name>_test.go`.
4. Update [`../website/docs/developing/actions.md`](../website/docs/developing/actions.md) and user-facing validation docs when the action changes job authoring.

## Major packages

| Area | Packages |
|------|----------|
| Queue | `internal/queue/` (server), `internal/queueclient/` |
| Registry / dial | `internal/registry/`, `internal/resolver/` |
| Logs / run tree | `internal/logserver/`, `internal/logforwarder/`, `internal/job/` |
| TLS | `internal/tlsconfig/` (reload) |
| Platform operations | `internal/platform/` (GOOS-specific host operations such as system trust-store changes; keep privileged OS actions here behind small APIs) |
| DB bootstrap | `internal/database/` (`OpenDB`, `WaitForMigrations`), `internal/migrations/` (embedded SQL, SQLite + Postgres), `internal/dbdrivers/` (`_` import) |
| Schedules / recovery | `internal/cron/`, `internal/reconciler/` · formal model [`../formal/tla/`](../formal/tla/) |
