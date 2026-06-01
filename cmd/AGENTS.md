# Cmd — Binary Entrypoints

**Authoritative list of commands:** directories under [`cmd/`](.) each with `main.go` (Cobra root + `runXxx` + Viper in `init()` for daemons).

**Go version:** `1.25.11` (see repo root [`go.mod`](../go.mod)).

## Layout

| Directory | Binary | Nature |
|-----------|--------|--------|
| `api/` | `vectis-api` | daemon (REST + metrics) |
| `artifact/` | `vectis-artifact` | daemon (artifact blob storage) |
| `cell-ingress/` | `vectis-cell-ingress` | daemon (private cell execution ingress) |
| `queue/` | `vectis-queue` | daemon (FIFO) |
| `registry/` | `vectis-registry` | daemon (service discovery) |
| `log/` | `vectis-log` | daemon (gRPC) |
| `log-forwarder/` | `vectis-log-forwarder` | daemon (sidecar) |
| `orchestrator/` | `vectis-orchestrator` | daemon (hot run state and task choreography) |
| `secrets/` | `vectis-secrets` | daemon (secret resolution broker) |
| `spiffe/` | `vectis-spiffe` | daemon (reference SPIFFE authority) |
| `worker/` | `vectis-worker` | daemon (action exec) |
| `worker-core/` | `vectis-worker-core` | daemon (worker execution core over UDS) |
| `cron/` | `vectis-cron` | daemon (scheduler) |
| `catalog/` | `vectis-catalog` | daemon (cell catalog applier) |
| `docs/` | `vectis-docs` | daemon (static docs HTTP) |
| `ui/` | `vectis-ui` | daemon (static UI HTTP + API proxy) |
| `reconciler/` | `vectis-reconciler` | daemon (recovery) |
| `local/` | `vectis-local` | daemon (dev supervisor) |
| `cli/` | `vectis-cli` | one-shot (HTTP client) |

Daemons generally follow the pattern below. One-shot binaries (`cli/`) skip the server setup and shutdown logic, and specialized daemons such as `log-forwarder` may replace network server setup with their own loop while still using shared logging, config, and signal handling patterns.

## Pattern (services with Viper)

```go
func runXxx(cmd *cobra.Command, args []string) { /* logger, TLS, DB, OTel, metrics, gRPC, serve */ }

var rootCmd = &cobra.Command{ Use: "vectis-...", Run: runXxx }

func init() {
    rootCmd.PersistentFlags().Int("port", config.APIPort(), "...")
    _ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
    viper.SetEnvPrefix("VECTIS_...") // see table below
    viper.AutomaticEnv()
    // DB binaries: import _ "vectis/internal/dbdrivers"
}

func main() {
    if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil { os.Exit(1) }
}
```

- **`init()`:** `PersistentFlags` → `viper.BindPFlag` → `SetEnvPrefix` → `AutomaticEnv()`.
- **`main()`:** always `cli.ExecuteWithShutdownSignals` (SIGINT/SIGTERM → context); do not roll custom signal handling.
- **`runXxx`:** logger + `cli.SetLogLevel`, TLS validation/reload when relevant, DB + `WaitForMigrations` for DB-backed services, tracer + metrics where wired, registry/static dial where needed, servers or worker loops, graceful shutdown.
- **DB binaries** must `import _ "vectis/internal/dbdrivers"` to side-effect register the SQL driver.

## Which binaries need the database import

Check the `DB?` column in the root [`AGENTS.md`](../AGENTS.md#binaries-eighteen-cmd): `api`, `cell-ingress`, `worker`, `secrets`, `cron`, `reconciler`, `catalog`, `local`, and `cli` need the `dbdrivers` import. `artifact`, `queue`, `registry`, `log`, `orchestrator`, `log-forwarder`, `spiffe`, `worker-core`, and `docs` do not.

## Env prefix mapping

**If this table disagrees with code, code wins:** `rg SetEnvPrefix cmd/`.

Dedicated metrics listeners accept the service bind host plus loopback Host headers by default. Use `VECTIS_METRICS_ALLOWED_HOSTS` or the service-prefixed `VECTIS_<SERVICE>_METRICS_ALLOWED_HOSTS` when publishing metrics outside localhost.

| Binary | `viper.SetEnvPrefix` | Primary TOML / notes |
|--------|----------------------|----------------------|
| `vectis-api` | `VECTIS_API_SERVER` | `[api]` in [`../internal/config/defaults.toml`](../internal/config/defaults.toml); `VECTIS_API_SERVER_HOST` / `--host` controls HTTP bind host; `--tls-cert-file` / `--tls-key-file` enable browser-facing HTTPS; `--cell-ingress-endpoint cell=url` configures remote cell execution ingress routes; ad hoc `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS` for trusted proxy headers in [`trusted-proxy-client-ip.md`](../website/docs/operating/deployment/trusted-proxy-client-ip.md) |
| `vectis-artifact` | `VECTIS_ARTIFACT` | `[artifact]`; default instance ID is `hostname-port`, default storage is `artifact/<instance-id>`, `--grpc-port` changes the internal upload/read listener, `--metrics-host` controls the localhost-default metrics bind host, `--storage-read-only-min-free-bytes` protects new blobs under disk pressure, and the gRPC listener uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleArtifact)` |
| `vectis-cell-ingress` | `VECTIS_CELL_INGRESS` | `[cell_ingress]`; private HTTP `POST /cell/v1/executions` uses internal `VECTIS_GRPC_TLS_*` mTLS when exposed off-loopback, can require producer SPIFFE URI SANs via `VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES`, `--allowed-host` / `VECTIS_CELL_INGRESS_ALLOWED_HOSTS` configure accepted Host headers, local execution repair, metrics host/port, plus queue discovery/pinned queue settings |
| `vectis-queue` | `VECTIS_QUEUE` | `[queue]`; default instance ID is `hostname-port`, default persistence is `queue/<pool>/<instance-id>`; metrics host defaults to localhost; keep active shards unique; gRPC listener uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleQueue)` |
| `vectis-registry` | `VECTIS_REGISTRY` | `[registry]`; HA gossip membership uses `VECTIS_REGISTRY_CLUSTER_*`; gRPC listener uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleRegistry)` |
| `vectis-log` | `VECTIS_LOG` | `[log]`; default instance ID is `hostname-port`, default storage is `log/<instance-id>`, `--grpc-port` changes the ingest/read listener, `--metrics-host` controls the localhost-default metrics bind host, `--storage-read-only-min-free-bytes` protects new run files under disk pressure, and the gRPC listener uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleLog)` |
| `vectis-secrets` | `VECTIS_SECRETS` | `[secrets]`; cell-local gRPC service for resolving job secrets, validates active execution claims against the cell DB, `--encryptedfs-root` plus `--encryptedfs-key-file` enable the encryptedfs provider, `--allow-secret` / `VECTIS_SECRETS_POLICY_ALLOW` configure default-deny access policy, metrics host defaults to localhost, and the gRPC listener uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleSecrets)` |
| `vectis-worker` | `VECTIS_WORKER` | `[worker]`; `--metrics-host` defaults to localhost; `--artifact-max-bytes`, `--artifact-max-run-bytes`, and `--artifact-max-count` cap worker artifact uploads; `--core-socket` dials required `vectis-worker-core`; `--core-connect-timeout` bounds startup dial/describe; `--core-shell-socket` exposes shell callbacks; `--secrets-address` points at the cell-local secrets service; `worker.execution_identity.*` derives expected per-execution SPIFFE IDs for Vectis-owned action state; `worker.spiffe.*` requires an exact SPIFFE Workload API X.509-SVID before action code runs when enabled, and `worker.spiffe.registration.*` can create/renew/release SPIFFE Entry API registrations through a protected local Unix socket; worker-control gRPC uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleWorkerControl)` |
| `vectis-worker-core` | `VECTIS_WORKER_CORE` | socket-local execution core; `--socket` serves the WorkerCore gRPC API over UDS; `--execution-backend host|lima`; `host` is the default, while `lima` registers a VM provider and makes unspecified action isolation inherit `vm`; use `--workspace-root` for VM-visible host workspaces or `--lima-guest-workspace-root` for guest-owned Lima workspaces |
| `vectis-spiffe` | `VECTIS_SPIFFE` | development/reference SPIFFE authority; serves Workload API and Entry API over Unix sockets, persists a CA and bundle, defaults to trust domain `vectis.internal`, and supports `--init-only` for deployment init containers that need bundle material before daemons start |
| `vectis-cron` | `VECTIS_CRON` | `[cron]`; `--instance-id` labels schedule claims, `--claim-ttl` bounds claim failover |
| `vectis-catalog` | `VECTIS_CATALOG` | `[catalog]`; `--cell-database-dsn cell=dsn` / `VECTIS_CATALOG_CELL_DATABASE_DSNS` configures catalog fan-in from cell-local DBs; metrics host defaults to localhost |
| `vectis-docs` | `VECTIS_DOCS` | static docs server; default host `localhost`, default port `8088`, serves embedded docs unless `VECTIS_DOCS_DIR` overrides; `--allowed-host` / `VECTIS_DOCS_ALLOWED_HOSTS` configure accepted Host headers; `--tls-cert-file` / `--tls-key-file` enable HTTPS |
| `vectis-ui` | `VECTIS_UI` | static UI server + browser BFF; default host `localhost`, default port `8089`, serves embedded UI unless `VECTIS_UI_DIR` overrides; can proxy Vite dev assets with `VECTIS_UI_DEV_ASSETS_URL`; manages UI session cookies and proxies `/api/` to `VECTIS_UI_API_URL` |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `[reconciler]`; `--redispatch-limit` bounds queued-run repair work per pass; metrics host defaults to localhost |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | `[log_forwarder]` for metrics host/port plus flat viper keys — see flags in [`log-forwarder/main.go`](log-forwarder/main.go) |
| `vectis-orchestrator` | `VECTIS_ORCHESTRATOR` | `[orchestrator]`; owns in-memory run state shards, exposes the task choreography gRPC service used by workers, `--metrics-host` controls the localhost-default metrics bind host, and uses `config.GRPCServerOptionsForRole(config.ServiceIdentityRoleOrchestrator)` |
| `vectis-local` | `VECTIS_LOCAL` | orchestrates stack; `VECTIS_LOCAL_PROFILE=ha` starts a local multi-instance HA exercise cell, `VECTIS_LOCAL_HOST` controls local API, UI, and docs bind host, `--http-tls` controls local API/docs HTTPS, `init` creates local TLS material, `install-cert` only installs the generated CA, `--cell` / `VECTIS_LOCAL_CELLS` adds extra local execution cells in the simple profile, starts the embedded development-only local `vectis-spiffe` authority when local gRPC TLS is enabled, local API auth defaults on unless `VECTIS_LOCAL_AUTH_ENABLED=false` or `--auth=false`, `VECTIS_LOCAL_UI_DEV_ASSETS_ENABLED=true` starts Vite UI dev assets for `vectis-ui`, exposes `--config-as-code` / `--source-repository` knobs for local config-as-code, and exposes `--ui-*`, `--spiffe-*`, `VECTIS_LOCAL_UI_*`, and `VECTIS_LOCAL_SPIFFE_*` knobs for local UI and secret resolution smoke tests |
| `vectis-cli` | *(none)* | [`internal/config`](../internal/config/) + `os.Getenv` — see [`../internal/config/api_auth.go`](../internal/config/api_auth.go); operator helpers include `secrets encryptedfs put` for writing encrypted job-secret envelopes |

Shared TOML sections: [`../internal/config/defaults.toml`](../internal/config/defaults.toml) (`[database]`, `[discovery]`, `[grpc_tls]`, `[service_identity]`, `[metrics_tls]`, …).

## Common pitfalls

- **Forgetting the dbdrivers import:** the binary compiles but panics at startup with `sql: unknown driver "sqlite3"`.
- **Env prefix mismatch:** the prefix in `SetEnvPrefix` must match `defaults.toml` section names. `rg SetEnvPrefix cmd/` is the reference.
- **Missing flag binding:** `PersistentFlags` → `viper.BindPFlag` is required for CLI flags to override defaults and env vars.
- **Container builds:** `CGO_ENABLED=0` + `-tags=nosqlite` — the `nosqlite` build tag replaces the SQLite driver with a stub.
- **User-facing command changes:** update [`../website/docs/using/cli-guide.md`](../website/docs/using/cli-guide.md) or the relevant operating reference doc when command behavior changes.
