# Cmd — Binary Entrypoints

**Authoritative list of commands:** directories under [`cmd/`](.) each with `main.go` (Cobra root + `runXxx` + Viper in `init()` for daemons).

**Go version:** `1.25.10` (see repo root [`go.mod`](../go.mod)).

## Layout

| Directory | Binary | Nature |
|-----------|--------|--------|
| `api/` | `vectis-api` | daemon (REST + metrics) |
| `cell-ingress/` | `vectis-cell-ingress` | daemon (private cell execution ingress) |
| `queue/` | `vectis-queue` | daemon (FIFO) |
| `registry/` | `vectis-registry` | daemon (service discovery) |
| `log/` | `vectis-log` | daemon (gRPC) |
| `log-forwarder/` | `vectis-log-forwarder` | daemon (sidecar) |
| `worker/` | `vectis-worker` | daemon (action exec) |
| `cron/` | `vectis-cron` | daemon (scheduler) |
| `catalog/` | `vectis-catalog` | daemon (cell catalog applier) |
| `docs/` | `vectis-docs` | daemon (static docs HTTP) |
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

Check the `DB?` column in the root [`AGENTS.md`](../AGENTS.md#binaries-thirteen-cmd): `api`, `cell-ingress`, `worker`, `cron`, `reconciler`, `catalog`, `local`, and `cli` need the `dbdrivers` import. `queue`, `registry`, `log`, `log-forwarder`, and `docs` do not.

## Env prefix mapping

**If this table disagrees with code, code wins:** `rg SetEnvPrefix cmd/`.

| Binary | `viper.SetEnvPrefix` | Primary TOML / notes |
|--------|----------------------|----------------------|
| `vectis-api` | `VECTIS_API_SERVER` | `[api]` in [`../internal/config/defaults.toml`](../internal/config/defaults.toml); `VECTIS_API_SERVER_HOST` / `--host` controls HTTP bind host; `--tls-cert-file` / `--tls-key-file` enable browser-facing HTTPS; `--cell-ingress-endpoint cell=url` configures remote cell execution ingress routes; ad hoc `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS` for trusted proxy headers in [`trusted-proxy-client-ip.md`](../website/docs/operating/deployment/trusted-proxy-client-ip.md) |
| `vectis-cell-ingress` | `VECTIS_CELL_INGRESS` | `[cell_ingress]`; private HTTP `POST /cell/v1/executions`, local execution repair, plus queue discovery/pinned queue settings |
| `vectis-queue` | `VECTIS_QUEUE` | `[queue]`; default instance ID is `hostname-port`, default persistence is `queue/<pool>/<instance-id>`; keep active shards unique |
| `vectis-registry` | `VECTIS_REGISTRY` | `[registry]`; HA gossip membership uses `VECTIS_REGISTRY_CLUSTER_*` |
| `vectis-log` | `VECTIS_LOG` | `[log]`; default instance ID is `hostname-port`, default storage is `log/<instance-id>`, `--grpc-port` changes the ingest/read listener, and `--storage-read-only-min-free-bytes` protects new run files under disk pressure |
| `vectis-worker` | `VECTIS_WORKER` | `[worker]` |
| `vectis-cron` | `VECTIS_CRON` | `[cron]`; `--instance-id` labels schedule claims, `--claim-ttl` bounds claim failover |
| `vectis-catalog` | `VECTIS_CATALOG` | `[catalog]`; `--cell-database-dsn cell=dsn` / `VECTIS_CATALOG_CELL_DATABASE_DSNS` configures catalog fan-in from cell-local DBs |
| `vectis-docs` | `VECTIS_DOCS` | static docs server; default host `localhost`, default port `8088`, serves embedded docs unless `VECTIS_DOCS_DIR` overrides; `--tls-cert-file` / `--tls-key-file` enable HTTPS |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `[reconciler]` |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | `[log_forwarder]` for metrics port plus flat viper keys — see flags in [`log-forwarder/main.go`](log-forwarder/main.go) |
| `vectis-local` | `VECTIS_LOCAL` | orchestrates stack; `VECTIS_LOCAL_PROFILE=ha` starts a local multi-instance HA exercise cell, `VECTIS_LOCAL_HOST` controls local API and docs bind host, `--http-tls` controls local API/docs HTTPS, `init` creates local TLS material, `install-cert` only installs the generated CA, and `--cell` / `VECTIS_LOCAL_CELLS` adds extra local execution cells in the simple profile |
| `vectis-cli` | *(none)* | [`internal/config`](../internal/config/) + `os.Getenv` — see [`../internal/config/api_auth.go`](../internal/config/api_auth.go) |

Shared TOML sections: [`../internal/config/defaults.toml`](../internal/config/defaults.toml) (`[database]`, `[discovery]`, `[grpc_tls]`, `[metrics_tls]`, …).

## Common pitfalls

- **Forgetting the dbdrivers import:** the binary compiles but panics at startup with `sql: unknown driver "sqlite3"`.
- **Env prefix mismatch:** the prefix in `SetEnvPrefix` must match `defaults.toml` section names. `rg SetEnvPrefix cmd/` is the reference.
- **Missing flag binding:** `PersistentFlags` → `viper.BindPFlag` is required for CLI flags to override defaults and env vars.
- **Container builds:** `CGO_ENABLED=0` + `-tags=nosqlite` — the `nosqlite` build tag replaces the SQLite driver with a stub.
- **User-facing command changes:** update [`../website/docs/using/cli-guide.md`](../website/docs/using/cli-guide.md) or the relevant operating reference doc when command behavior changes.
