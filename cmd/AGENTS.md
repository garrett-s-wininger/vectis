# Cmd — Binary Entrypoints

**Authoritative list of commands:** directories under [`cmd/`](.) each with `main.go` (Cobra root + `runXxx` + Viper in `init()` for daemons).

**Go version:** `1.25.7` (see repo root [`go.mod`](../go.mod)).

## Layout

| Directory | Binary | Nature |
|-----------|--------|--------|
| `api/` | `vectis-api` | daemon (REST + metrics) |
| `queue/` | `vectis-queue` | daemon (FIFO) |
| `registry/` | `vectis-registry` | daemon (service discovery) |
| `log/` | `vectis-log` | daemon (gRPC + SSE) |
| `log-forwarder/` | `vectis-log-forwarder` | daemon (sidecar) |
| `worker/` | `vectis-worker` | daemon (action exec) |
| `cron/` | `vectis-cron` | daemon (scheduler) |
| `reconciler/` | `vectis-reconciler` | daemon (recovery) |
| `local/` | `vectis-local` | daemon (dev supervisor) |
| `cli/` | `vectis-cli` | one-shot (HTTP client) |

Daemons all follow the pattern below. One-shot binaries (`cli/`) skip the server setup and shutdown logic.

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
- **`runXxx`:** logger + `cli.SetLogLevel`, TLS validation/reload, DB + `WaitForMigrations`, tracer + metrics + pool metrics, registry/static dial, servers, graceful shutdown (often `exitCode` + deferred cleanup).
- **DB binaries** must `import _ "vectis/internal/dbdrivers"` to side-effect register the SQL driver.

## Which binaries need the database import

Check the `DB?` column in the root [`AGENTS.md`](../AGENTS.md#binaries-ten-cmd): `api`, `queue`, `log`, `cron`, `reconciler` — these need the `dbdrivers` import. `registry`, `worker`, `log-forwarder`, `local`, `cli` do not.

## Env prefix mapping

**If this table disagrees with code, code wins:** `rg SetEnvPrefix cmd/`.

| Binary | `viper.SetEnvPrefix` | Primary TOML / notes |
|--------|----------------------|----------------------|
| `vectis-api` | `VECTIS_API_SERVER` | `[api]` in [`../internal/config/defaults.toml`](../internal/config/defaults.toml) |
| `vectis-queue` | `VECTIS_QUEUE` | `[queue]` |
| `vectis-registry` | `VECTIS_REGISTRY` | `[registry]` |
| `vectis-log` | `VECTIS_LOG` | `[log]` |
| `vectis-worker` | `VECTIS_WORKER` | `[worker]` |
| `vectis-cron` | `VECTIS_CRON` | `[cron]` |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `[reconciler]` |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | flat viper keys — see flags in [`log-forwarder/main.go`](log-forwarder/main.go) |
| `vectis-local` | `VECTIS_LOCAL` | orchestrates stack; extra binds e.g. in [`local/main.go`](local/main.go) |
| `vectis-cli` | *(none)* | [`internal/config`](../internal/config/) + `os.Getenv` — see [`../internal/config/api_auth.go`](../internal/config/api_auth.go) |

Shared TOML sections: [`../internal/config/defaults.toml`](../internal/config/defaults.toml) (`[database]`, `[discovery]`, `[grpc_tls]`, `[metrics_tls]`, …).

## Common pitfalls

- **Forgetting the dbdrivers import:** the binary compiles but panics at startup with `sql: unknown driver "sqlite3"`.
- **Env prefix mismatch:** the prefix in `SetEnvPrefix` must match `defaults.toml` section names. `rg SetEnvPrefix cmd/` is the reference.
- **Missing flag binding:** `PersistentFlags` → `viper.BindPFlag` is required for CLI flags to override defaults and env vars.
- **Container builds:** `CGO_ENABLED=0` + `-tags=nosqlite` — the `nosqlite` build tag replaces the SQLite driver with a stub.
