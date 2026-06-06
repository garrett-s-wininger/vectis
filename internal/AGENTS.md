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

Browser-facing API/docs security headers live in `internal/httpsecurity`; use that shared middleware instead of setting ad hoc header strings in individual handlers. Keep response isolation and legacy browser hardening headers such as `Cross-Origin-Opener-Policy`, `Cross-Origin-Resource-Policy`, `Cross-Origin-Embedder-Policy`, `Origin-Agent-Cluster`, `X-Permitted-Cross-Domain-Policies`, and `X-Download-Options` centralized there so API, docs, cell ingress, and metrics stay aligned.

Docs CSP intentionally excludes `unsafe-inline`; docs pages, placeholder HTML, and generated docs builds should use same-origin script/style assets instead of inline blocks, inline style attributes, or inline event handlers.

API HSTS is configured through `api.hsts.*` / `VECTIS_API_HSTS_*` and validated at API startup. Keep preload guarded behind `include_subdomains=true` and at least a one-year `max_age_seconds`; do not hard-code per-route HSTS values.

HTTP server parser limits use `httpsecurity.DefaultMaxHeaderBytes`; keep API, docs, cell ingress, and metrics servers on that shared cap unless a route-specific threat model justifies a different one.

Metrics HTTP servers must go through `internal/cli.StartMetricsHTTPServer`; it applies Host validation, the shared `/metrics` route guard, security headers, and `no-store` cache policy for every service.

HTTP method allowlists should use `internal/httpsecurity` helpers for shared `Allow` header and `HEAD`-for-`GET` behavior. Docs stay read-only (`GET`/`HEAD`); cell ingress should keep its route surface explicitly guarded.

Cell ingress should keep both direct `ServeHTTP` and `Handler()` callers behind the shared `httpsecurity` header middleware and cell-ingress Host validation. JSON responses should stay `no-store` by default; route guard errors, health checks, and execution submission responses all flow through the shared JSON writer.

Docs static file serving must go through the hardened docs file server wrapper, not raw `http.FileServer`, so directory listings, dotfile paths, and local docs symlink escapes stay blocked.

API Host header validation is enabled by default through `api.host_validation.allowed_hosts` / `VECTIS_API_ALLOWED_HOSTS`. Defaults derive from the API listen host plus loopback; external DNS names must be explicit.

API forwarded headers are trusted only when the TCP peer is inside `api.client_ip.trusted_proxy_cidrs`. Keep client IP and original-scheme inference (`X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, `Forwarded`) on that shared boundary; never trust those headers from direct clients. Trusted proxy scheme inference does not replace `api.session.cookie_secure=true` for auth-enabled HTTPS edge deployments.

API CORS is closed by default. Same-origin `Origin` headers, matching the browser-facing scheme, host, and port, pass without CORS response headers; disallowed cross-origin actual requests and preflights must be rejected before route handling. Configure only exact `https://` origins through `api.cors.allowed_origins` / `VECTIS_API_CORS_ALLOWED_ORIGINS`; `http://` origins are accepted only for loopback/localhost development, and wildcard credentialed CORS is intentionally rejected.

Fetch Metadata is enforced before route handling for browser-marked cross-site requests that omit `Origin`; cross-site requests with an `Origin` must then pass CORS. Cookie-authenticated requests still reject `Sec-Fetch-Site: cross-site` even when other metadata is present.

Protected API routes default to `Cache-Control: no-store` through `routeCachePolicy`. Setup and login routes must remain explicit `no-store` routes so public/setup-time validation and rate-limit failures are covered before handlers run. Only handler-managed streaming responses should opt out, and they must set their own cache headers explicitly.

API routes reject request bodies by default through `routeBodyPolicy`. JSON body routes must opt in with an explicit size cap in `routeSpec.Body`; the body middleware enforces JSON media types before handlers run. Use `readRequestBody` rather than ad hoc `io.LimitReader` calls so oversized streamed bodies return `413`.

API Host, CORS, Fetch Metadata, CSRF, request-target, method, media-type, body-policy, and rate-limit rejections must flow through `recordSecurityRejection` / the route body recorder so `vectis_api_security_rejections_total` and sanitized warning logs stay complete. Keep metric labels low-cardinality; do not include raw headers, tokens, cookies, or request bodies in security rejection logs.

The API cache backend is shared security state for sessions and rate limits. Database mode is the replica-safe default. Memory mode is process-local, cleans expired entries opportunistically, and should stay limited to tests, local development, or deliberate single-process deployments.

Secure browser sessions use only `__Host-` session/CSRF cookie names with `Secure`, `Path=/`, and no `Domain`; do not add unprefixed aliases or migration fallbacks. Browser cookie auth requires HTTPS or local TLS. Unsafe cookie-authenticated requests must include a valid CSRF token and an `Origin` or `Referer` that matches the browser-facing scheme, host, and port; do not allow missing origin metadata. Logout must delete the server-side session, clear the canonical Vectis session/CSRF cookies, and send `Clear-Site-Data: "cache", "storage"`. Do not add the `cookies` directive unless the deployment model intentionally accepts clearing cookies for the same domain and subdomains.

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
| Per-service metrics | `apimetrics.go`, `apisecuritymetrics.go`, `queuemetrics.go`, `workermetrics.go`, `logmetrics.go` — init in the binary's `runXxx` |

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
