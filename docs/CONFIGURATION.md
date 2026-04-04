# Configuration reference

This page is for **people running or integrating Vectis** (operators, platform engineers, and developers wiring staging or prod). It focuses on **what to set**—environment variables, ports, and discovery—not on Go package layout. For service roles and data flow, see [ARCHITECTURE.md](ARCHITECTURE.md). For **job**, **run**, **enqueue**, and related terms, see [GLOSSARY.md](GLOSSARY.md). For **secrets and trust boundaries**, see [SECURITY.md](SECURITY.md).

Executables are built as **`bin/vectis-<name>`** (e.g. `bin/vectis-api` after `make build`). The **environment prefix** for each service (e.g. `VECTIS_API_SERVER`) is what you use when building variable names; it may differ from the binary name or from `--help` titles.

## Defaults, environment, and flags

1. **Shipped defaults** — Each binary includes baseline host/port and discovery timings (same values as in the repo file [`internal/config/defaults.toml`](../internal/config/defaults.toml)).
2. **Environment variables** — Override defaults. Each long-running service has its own **`VECTIS_<SERVICE_PREFIX>_…`** namespace (see the table below).
3. **Command-line flags** — On some binaries, flags override the same settings as env (e.g. API `--port`).

**Naming pattern:** Take the service prefix, append the setting path with dots turned into underscores, and uppercase. Example: registry URL for the API process is conceptually `api.registry.address` → **`VECTIS_API_SERVER_API_REGISTRY_ADDRESS`**.

**Durations** (where applicable) use strings like **`30s`**, **`1m`**, **`10s`**.

**Log verbosity** — Set **`VECTIS_<PREFIX>_LOG_LEVEL`** to `debug`, `info`, `warn`, or `error` (prefix from the table). For **`vectis-local`**, also **`VECTIS_LOCAL_LOG_LEVEL`**; if a child service ignores the level you expect, set that child’s `…_LOG_LEVEL` explicitly (e.g. **`VECTIS_API_SERVER_LOG_LEVEL`** for `vectis-api`).

**API correlation and access logs** (`vectis-api` only) — Shipped default is **`api.log_format` = `text`** in [`internal/config/defaults.toml`](../internal/config/defaults.toml). Override with **`VECTIS_API_SERVER_LOG_FORMAT`**:

| Value | Behavior |
| --- | --- |
| **`text`** (default) | Every response still gets a stable **`X-Request-ID`** header (see below), but there is **no** per-request JSON access line on stderr. |
| **`json`** | One JSON object per HTTP request on stderr (fields include `correlation_id`, `method`, `http_route`, `status`, `duration`). **`/health/*`** and **`/metrics`** are excluded. Access lines use **INFO** so they still appear when **`LOG_LEVEL`** is `warn` or `error`. |

**Incoming request IDs:** If the client sends a valid **`X-Request-ID`** or **`X-Correlation-ID`** (printable ASCII, length ≤ 128), that value is reused and echoed as **`X-Request-ID`**. Otherwise the API generates a new UUID. Invalid or oversized values are ignored.

## Database (every service that uses the DB)

These two variables are **global** (no per-service prefix). Every component that talks to the database must use the **same** values.

| Variable | Purpose |
| --- | --- |
| **`VECTIS_DATABASE_DRIVER`** | `sqlite3` (typical default) or **`pgx`** for PostgreSQL. |
| **`VECTIS_DATABASE_DSN`** | PostgreSQL URL, or SQLite **file path**. If unset, SQLite defaults to a file under the XDG data directory (see [README.md](../README.md)). |

Applies to: `vectis-api`, `vectis-worker`, `vectis-cron`, `vectis-reconciler`, `vectis-log`, and **`vectis-cli migrate`**.

### PostgreSQL connection pool (`pgx` only)

When **`VECTIS_DATABASE_DRIVER=pgx`**, `database.OpenDB` applies `*sql.DB` pool settings after `sql.Open`. **SQLite** and other drivers ignore this block. Defaults match [`internal/config/defaults.toml`](../internal/config/defaults.toml) (`database.pgx_pool`); override with **global** env (no per-service prefix):

| Variable | Purpose |
| --- | --- |
| **`VECTIS_DATABASE_PGX_MAX_OPEN_CONNS`** | Maximum open connections to the server (default **25**). |
| **`VECTIS_DATABASE_PGX_MAX_IDLE_CONNS`** | Maximum idle connections (default **10**; clamped to ≤ max open). |
| **`VECTIS_DATABASE_PGX_CONN_MAX_LIFETIME`** | Max lifetime of a connection (default **`1h`**); Go `time.ParseDuration` syntax. |
| **`VECTIS_DATABASE_PGX_CONN_MAX_IDLE_TIME`** | Max idle time before a connection is closed (default **`15m`**). |

Viper keys `database.pgx_pool.*` apply where a binary loads full config into viper (same semantics as other `database.*` keys). See [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md#database) for why this matters under outages and many processes.

## Service discovery vs fixed addresses

In most setups either:

- **Registry-based discovery** — Queue and log register; API, worker, cron, and reconciler learn addresses from the registry (default-oriented), or
- **Fixed addresses** — You set explicit queue/log/registry URLs so components do not depend on discovery.

Settings **specific to one role** (e.g. only the worker) override **shared discovery defaults** when both are set. Shared defaults correspond to a **`DISCOVERY_…`** segment in the env name (conceptually `discovery.*` in config).

| What you’re configuring | Shared default (all roles can inherit) | Role-specific examples (env *segments*, before full name) |
| --- | --- | --- |
| Registry address | `…_DISCOVERY_REGISTRY_ADDRESS` | `…_API_REGISTRY_ADDRESS`, `…_WORKER_REGISTRY_ADDRESS`, `…_CRON_REGISTRY_ADDRESS`, `…_RECONCILER_REGISTRY_ADDRESS`, `…_QUEUE_REGISTRY_ADDRESS`, `…_LOG_REGISTRY_ADDRESS` |
| Queue address (pin or resolver) | `…_DISCOVERY_QUEUE_ADDRESS`, `…_DISCOVERY_QUEUE_RESOLVER_ADDRESS` | `…_API_QUEUE_ADDRESS`, `…_WORKER_QUEUE_ADDRESS`, `…_CRON_QUEUE_ADDRESS`, `…_RECONCILER_QUEUE_ADDRESS` |
| Log gRPC address | `…_DISCOVERY_LOG_ADDRESS`, `…_DISCOVERY_LOG_GRPC_RESOLVER_ADDRESS` | `…_WORKER_LOG_ADDRESS` |
| Address queue/log advertise when registering | `…_DISCOVERY_QUEUE_ADVERTISE_ADDRESS`, `…_DISCOVERY_LOG_GRPC_ADVERTISE_ADDRESS` | `…_QUEUE_ADVERTISE_ADDRESS`, `…_LOG_GRPC_ADVERTISE_ADDRESS` |

Replace `…` with the correct prefix from the next section (e.g. `VECTIS_WORKER` + `_DISCOVERY_REGISTRY_ADDRESS` → `VECTIS_WORKER_DISCOVERY_REGISTRY_ADDRESS`).

**Registration toggles** (queue/log publish themselves to the registry): `VECTIS_QUEUE_QUEUE_REGISTER_WITH_REGISTRY`, `VECTIS_LOG_LOG_GRPC_REGISTER_WITH_REGISTRY` (names follow the same pattern; defaults favor registration on).

**Discovery timing defaults** (override with each process’s prefix + `DISCOVERY_…`): resolver refresh **10s**, poll timeout **5s**, error refresh **2s**, registration heartbeat **45s**.

## Services: env prefix and CLI options

| Program | Env prefix | CLI options (override env when present) |
| --- | --- | --- |
| `vectis-api` | `VECTIS_API_SERVER` | `--port` |
| `vectis-queue` | `VECTIS_QUEUE` | `--port`, `--persistence-dir`, `--persistence-snapshot-every` |
| `vectis-registry` | `VECTIS_REGISTRY` | `--port` |
| `vectis-worker` | `VECTIS_WORKER` | — |
| `vectis-cron` | `VECTIS_CRON` | — |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `--interval` (same as **`VECTIS_RECONCILER_INTERVAL`**) |
| `vectis-log` | `VECTIS_LOG` | — |
| `vectis-local` | `VECTIS_LOCAL` | `--log-level` |
| `vectis-cli` | *(none for API)* | **`VECTIS_DATABASE_*`** for `migrate` only. **API and log stream URLs** for other commands follow **shipped defaults** (`api.host`, `api.port`, `log.host`, log stream port)—not the server env prefixes. |

## Common operator settings

| Goal | What to set |
| --- | --- |
| Change API HTTP port | `VECTIS_API_SERVER_PORT` or `--port` on `vectis-api` |
| Structured API access logs (JSON) | `VECTIS_API_SERVER_LOG_FORMAT=json` |
| Correlate API requests (header) | Responses include **`X-Request-ID`**; send **`X-Request-ID`** or **`X-Correlation-ID`** to propagate your own ID |
| PostgreSQL | `VECTIS_DATABASE_DRIVER=pgx` and `VECTIS_DATABASE_DSN=postgres://…` on **all** DB consumers; tune pool with **`VECTIS_DATABASE_PGX_*`** ([above](#postgresql-connection-pool-pgx-only)) |
| Pin queue to `host:8081` for workers | `VECTIS_WORKER_WORKER_QUEUE_ADDRESS` (or shared `VECTIS_WORKER_DISCOVERY_QUEUE_ADDRESS`) |
| Queue backlog on disk | `VECTIS_QUEUE_PERSISTENCE_DIR` (empty disables persistence—see `vectis-queue --help`) |
| Log files on disk | `VECTIS_LOG_STORAGE_DIR` |
| How often reconciler scans | `VECTIS_RECONCILER_INTERVAL` |

**Log service listen ports** (gRPC **8083**, log HTTP **8084** by default) come from shipped defaults only—there is no port flag on `vectis-log` today.

## Default ports

| Service | Port |
| --- | --- |
| API HTTP | 8080 |
| Queue gRPC | 8081 |
| Registry gRPC | 8082 |
| Log gRPC | 8083 |
| Log HTTP (streams) | 8084 |

## Data paths (typical, SQLite / local)

| Data | Default |
| --- | --- |
| SQLite database file | `$XDG_DATA_HOME/vectis/db.sqlite3` (see README if `XDG_DATA_HOME` is unset) |
| Queue persistence | `$XDG_DATA_HOME/vectis/queue` |
| Run log files (`vectis-log`) | `$XDG_DATA_HOME/vectis/jobs` |

## Related documentation

| Topic | Document |
| --- | --- |
| Components and flows | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Failure behavior | [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md) |
| Roadmap / persistence | [PLANNING.md](PLANNING.md) §2.5 |
| Quick start, Postgres / Podman | [README.md](../README.md) |
| Glossary | [GLOSSARY.md](GLOSSARY.md) |
| Security posture | [SECURITY.md](SECURITY.md) |

---

## For developers

Resolution order for overlapping settings (per-role vs `discovery.*`), flag binding, and exact Viper keys are implemented in [`internal/config/config.go`](../internal/config/config.go). Binaries use [Viper](https://github.com/spf13/viper) (`AutomaticEnv`) and, where noted, Cobra flags bound to the same keys. Default TOML is embedded at build time from `internal/config/defaults.toml`.
