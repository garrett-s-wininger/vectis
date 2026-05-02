# Configuration reference

This page is for **people running or integrating Vectis** (operators, platform engineers, and developers wiring staging or prod). It focuses on **what to set**—environment variables, ports, and discovery—not on Go package layout. For service roles and data flow, see [ARCHITECTURE.md](ARCHITECTURE.md). For **Prometheus `/metrics`** ports and env on each binary, use the **Default ports** and **Common operator settings** sections below; roadmap context is in [PLANNING.md](PLANNING.md) §10. For **job**, **run**, **enqueue**, and related terms, see [GLOSSARY.md](GLOSSARY.md). For **secrets and trust boundaries**, see [SECURITY.md](SECURITY.md).

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

### HTTP API authentication (`vectis-api`)

Shipped default is **`api.auth.enabled` = `false`** in [`internal/config/defaults.toml`](../internal/config/defaults.toml). When **`true`**, the API enforces Bearer tokens on protected routes after initial setup; setup completion is stored in the database.

**Authentication vs authorization:** **`api.auth.*`** controls whether clients must authenticate (Bearer). **`api.authz.*`** selects the authorization engine applied after authentication.

| Variable / key | Purpose |
| --- | --- |
| **`VECTIS_API_AUTH_ENABLED`** / **`api.auth.enabled`** | If `true` (or `1`, `yes`, `on`), enable HTTP API authentication. |
| **`VECTIS_API_AUTH_BOOTSTRAP_TOKEN`** / **`api.auth.bootstrap_token`** | Shared secret for **`POST /api/v1/setup/complete`** on a **new** database. Must be **at least 16 characters** when auth is enabled and setup is not yet complete; optional after the DB records setup completion. |
| **`VECTIS_API_AUTHZ_ENGINE`** / **`api.authz.engine`** | `hierarchical_rbac` (default) or `authenticated_full`. See [SECURITY.md](SECURITY.md). |

**CLI authentication:** `vectis-cli login` calls **`POST /api/v1/login`** and persists the returned token to the OS user config directory (`os.UserConfigDir()/vectis/token`, e.g. `~/.config/vectis/token` on Linux or `~/Library/Application Support/vectis/token` on macOS). Subsequent CLI commands read this file automatically (override with **`VECTIS_API_TOKEN`**).

## Database (every service that uses the DB)

These two variables are **global** (no per-service prefix). Every component that talks to the database must use the **same** values.

| Variable | Purpose |
| --- | --- |
| **`VECTIS_DATABASE_DRIVER`** | `sqlite3` (typical default) or **`pgx`** for PostgreSQL. |
| **`VECTIS_DATABASE_DSN`** | PostgreSQL URL, or SQLite **file path**. If unset, SQLite defaults to a file under the XDG data directory (see [README.md](../README.md)). |

Applies to: `vectis-api`, `vectis-worker`, `vectis-cron`, `vectis-reconciler`, `vectis-log`, and **`vectis-cli migrate`**.

## Internal gRPC TLS (global `VECTIS_GRPC_TLS_*`)

These variables are **shared across all `vectis-*` binaries** (bound in code, not via each process’s `VECTIS_<PREFIX>_…` pattern). Shipped defaults keep **plaintext gRPC** for processes you start individually (`grpc_tls.insecure = true` in [`internal/config/defaults.toml`](../internal/config/defaults.toml)).

**`vectis-local`** (by default) **bootstraps** a dev CA and server certificate under **`$XDG_DATA_HOME/vectis/local-tls`** (or `~/.local/share/vectis/local-tls` when `XDG_DATA_HOME` is unset), sets **`VECTIS_GRPC_TLS_*`** on every child process, and configures its own gRPC health checks to match. The leaf certificate is issued with **47 days** validity (aligned with short public TLS lifetimes); it is **re-issued** when expired or when fewer than **14 days** remain (the CA is rotated separately when it is within **90 days** of expiry). Use **`--grpc-insecure`** or **`VECTIS_LOCAL_GRPC_INSECURE=true`** on `vectis-local` to skip bootstrap and run **plaintext** gRPC for all children (same as the global default for standalone binaries).

**`make deploy-podman`** ([`deploy/podman/kube-spec.yaml`](../deploy/podman/kube-spec.yaml)) runs init container **`vectis-pod-tls-init`**, which writes **gRPC** CA + server cert into **`vectis-grpc-tls`** (mounted at **`/run/vectis/grpc-tls`**) and **Postgres** CA + server cert into **`vectis-postgres-tls`**. ConfigMap **`vectis-grpc-tls-env`** sets **`VECTIS_GRPC_TLS_*`** as in the table below. Database-backed Vectis containers mount **`ca.pem`** from **`vectis-postgres-tls`** at **`/run/vectis/postgres-tls/ca.pem`** and use a DSN with **`sslmode=verify-full`** and **`sslrootcert=/run/vectis/postgres-tls/ca.pem`**. The **`postgres`** container mounts the same volume at **`/run/postgres-tls`** with **`ssl=on`**. Host-side **`vectis-cli migrate`** (see [README.md](../README.md)) typically uses **`sslmode=require`** without **`sslrootcert`**. The init image runs **`apk add openssl`** (needs **network** on a cold cache).

| Variable | Purpose |
| --- | --- |
| **`VECTIS_GRPC_TLS_INSECURE`** | If `true`, use plaintext gRPC (default for **standalone** binaries). If `false`, PEM paths below must satisfy the role of each binary (see [SECURITY.md](SECURITY.md)). **`vectis-local` normally sets this to `false` via bootstrap** unless `--grpc-insecure` / **`VECTIS_LOCAL_GRPC_INSECURE`**. |
| **`VECTIS_GRPC_TLS_CA_FILE`** | PEM file with CA certificate(s) used to **verify peer servers** when this process dials gRPC. Required for client-only daemons when TLS is enabled; required for queue/log when they dial the registry. |
| **`VECTIS_GRPC_TLS_CERT_FILE`** / **`VECTIS_GRPC_TLS_KEY_FILE`** | Server certificate and key for **vectis-registry**, **vectis-queue**, and **vectis-log** gRPC listeners when TLS is enabled. |
| **`VECTIS_GRPC_TLS_CLIENT_CA_FILE`** | If set, servers require and verify **mTLS** client certificates signed by this CA. |
| **`VECTIS_GRPC_TLS_CLIENT_CERT_FILE`** / **`VECTIS_GRPC_TLS_CLIENT_KEY_FILE`** | Optional client identity when dialing gRPC (mTLS). |
| **`VECTIS_GRPC_TLS_SERVER_NAME`** | Optional TLS ServerName / SNI override for **outbound** connections; if unset, a hostname is derived from the dial target when possible (e.g. `localhost:8081` → `localhost`). **Registry discovery often yields `127.0.0.1:*`;** for dev certs whose DNS SAN is `localhost`, you must set this to **`localhost`** ( **`vectis-local` bootstrap does this automatically**). |
| **`VECTIS_GRPC_TLS_RELOAD_INTERVAL`** | If set to a positive duration (e.g. **`30s`**), PEM files are polled for changes and reloaded without process restart. **`0`** disables polling. |

**Roles when TLS is enabled (`VECTIS_GRPC_TLS_INSECURE=false`):**

| Role | Binaries | Required PEM material |
| --- | --- | --- |
| **gRPC server** | `vectis-registry`, `vectis-queue`, `vectis-log` | **`CERT_FILE`** and **`KEY_FILE`** on each host that listens for gRPC. Queue and log also need **`CA_FILE`** so they can verify the registry when registering. |
| **gRPC client only** | `vectis-api`, `vectis-worker`, **`vectis-cron`**, **`vectis-reconciler`** | **`CA_FILE`** to verify registry/queue/log peers. Optional **`CLIENT_CERT_FILE`** / **`CLIENT_KEY_FILE`** if servers enforce mTLS (`CLIENT_CA_FILE` on servers). |

Cron and reconciler only **dial** the queue (via registry or pinned addresses); they do not expose a gRPC listener.

Per-binary keys such as **`VECTIS_QUEUE_GRPC_TLS_CA_FILE`** also map to the same `grpc_tls.*` settings when you prefer a prefix.

## Metrics HTTPS (`vectis-queue`, `vectis-worker`, `vectis-log` only)

Global **`VECTIS_METRICS_TLS_*`** settings wrap the **dedicated** Prometheus **`/metrics`** HTTP listeners with **TLS**. They do **not** affect **`vectis-api`** (REST and API `/metrics` stay on the main HTTP listener until a separate API TLS story exists).

| Variable | Purpose |
| --- | --- |
| **`VECTIS_METRICS_TLS_INSECURE`** | If **`true`** (default in [`internal/config/defaults.toml`](../internal/config/defaults.toml)), metrics HTTP is **plaintext**. If **`false`**, **`CERT_FILE`** and **`KEY_FILE`** are required. |
| **`VECTIS_METRICS_TLS_CERT_FILE`** / **`VECTIS_METRICS_TLS_KEY_FILE`** | Server certificate and key PEM paths for the metrics-only **`http.Server`**. |
| **`VECTIS_METRICS_TLS_RELOAD_INTERVAL`** | Optional PEM reload interval (same semantics as gRPC TLS). **`0`** disables. |

**`make deploy-podman`:** ConfigMap **`vectis-grpc-tls-env`** sets **`VECTIS_METRICS_TLS_INSECURE=false`** and reuses the same pod-local leaf as gRPC under **`/run/vectis/grpc-tls/`**. Bundled **Prometheus** scrapes **`scheme: https`** for queue, worker, and log metrics jobs and mounts **`ca_file`** for verification.

## Tracing export (OpenTelemetry traces)

`InitTracer` always configures local trace context propagation. To export spans, set:

| Variable | Purpose |
| --- | --- |
| **`OTEL_TRACES_EXPORTER`** | Set to **`otlp`** to enable OTLP trace export; leave unset (or `none`) to keep local-only tracing. |
| **`OTEL_EXPORTER_OTLP_ENDPOINT`** | OTLP base endpoint (for example **`http://127.0.0.1:4318`** for HTTP/protobuf). |
| **`OTEL_EXPORTER_OTLP_PROTOCOL`** | OTLP transport/protocol; Podman reference uses **`http/protobuf`**. |

**`make deploy-podman`:** ConfigMap **`vectis-tracing-env`** sets OTLP export to the in-pod Jaeger collector (`http://127.0.0.1:4318`). The reference Podman deployment runs Jaeger **2.17.0** as separate collector/query processes backed by an in-pod OpenSearch instance; Jaeger UI is published on **`http://localhost:16686`**.

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
| `vectis-queue` | `VECTIS_QUEUE` | `--port`, `--metrics-port`, `--persistence-dir`, `--persistence-snapshot-every` |
| `vectis-registry` | `VECTIS_REGISTRY` | `--port` |
| `vectis-worker` | `VECTIS_WORKER` | `--metrics-port` |
| `vectis-cron` | `VECTIS_CRON` | — |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `--interval` (same as **`VECTIS_RECONCILER_INTERVAL`**) |
| `vectis-log` | `VECTIS_LOG` | `--storage-dir`, `--metrics-port` |
| `vectis-local` | `VECTIS_LOCAL` | `--log-level`, `--grpc-insecure` (or **`VECTIS_LOCAL_GRPC_INSECURE=true`** for plaintext gRPC) |
| `vectis-cli` | *(none for API)* | **`VECTIS_DATABASE_*`** for `migrate` only. **API and log stream URLs** for other commands follow **shipped defaults** (`api.host`, `api.port`, `log.host`, log stream port)—not the server env prefixes. |

## Common operator settings

| Goal | What to set |
| --- | --- |
| Change API HTTP port | `VECTIS_API_SERVER_PORT` or `--port` on `vectis-api` |
| Enable HTTP API auth (Bearer after setup) | `VECTIS_API_AUTH_ENABLED=true` and bootstrap token for new DBs — see [HTTP API authentication](#http-api-authentication-vectis-api) and [SECURITY.md](SECURITY.md) |
| Structured API access logs (JSON) | `VECTIS_API_SERVER_LOG_FORMAT=json` |
| Correlate API requests (header) | Responses include **`X-Request-ID`**; send **`X-Request-ID`** or **`X-Correlation-ID`** to propagate your own ID |
| PostgreSQL | `VECTIS_DATABASE_DRIVER=pgx` and `VECTIS_DATABASE_DSN=postgres://…` on **all** DB consumers; tune pool with **`VECTIS_DATABASE_PGX_*`** ([above](#postgresql-connection-pool-pgx-only)) |
| Pin queue to `host:8081` for workers | `VECTIS_WORKER_WORKER_QUEUE_ADDRESS` (or shared `VECTIS_WORKER_DISCOVERY_QUEUE_ADDRESS`) |
| Queue backlog on disk | `VECTIS_QUEUE_PERSISTENCE_DIR` (empty disables persistence—see `vectis-queue --help`) |
| Queue metrics HTTP port | `VECTIS_QUEUE_METRICS_PORT` or `--metrics-port` (default **9081**; must differ from queue gRPC port); use **`VECTIS_METRICS_TLS_*`** for HTTPS on that listener |
| Worker metrics HTTP port | `VECTIS_WORKER_METRICS_PORT` or `--metrics-port` (default **9082**; must differ from queue metrics port); TLS via **`VECTIS_METRICS_TLS_*`** |
| Log metrics HTTP port | `VECTIS_LOG_METRICS_PORT` or `--metrics-port` (default **9083**; must differ from log gRPC/SSE and other metrics ports); TLS via **`VECTIS_METRICS_TLS_*`** |
| Log files on disk | `VECTIS_LOG_STORAGE_DIR` or `--storage-dir` |
| How often reconciler scans | `VECTIS_RECONCILER_INTERVAL` |
| `vectis-local` plaintext gRPC (no bootstrap) | `--grpc-insecure` or `VECTIS_LOCAL_GRPC_INSECURE=true` |

**Log service listen ports** (gRPC **8083**, log HTTP **8084** by default) come from shipped defaults; **`/metrics`** uses **`--metrics-port`** / **`VECTIS_LOG_METRICS_PORT`** (default **9083**).

## Default ports

| Service | Port |
| --- | --- |
| API HTTP | 8080 |
| Queue gRPC | 8081 |
| Queue Prometheus scrape (`/metrics`) | 9081 |
| Worker Prometheus scrape (`/metrics`) | 9082 |
| Registry gRPC | 8082 |
| Log gRPC | 8083 |
| Log HTTP (streams) | 8084 |
| Log Prometheus scrape (`/metrics`) | 9083 |

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
