# Configuration

This page is for people running Vectis: local developers, platform engineers, and operators wiring staging or production. It explains the settings you are most likely to touch, where defaults come from, and which knobs affect service discovery, storage, TLS, metrics, and authentication.

For service roles and data flow, see [Architecture](../concepts/architecture.md). For security posture, see [Security](../concepts/security.md). For startup and outage behavior, see [Failure Domains](../concepts/failure-domains.md). For terms such as job, run, queue, and dispatch, see [Glossary](../concepts/glossary.md).

## How Configuration Resolves

Vectis binaries start with embedded defaults from `internal/config/defaults.toml`, then layer environment variables, then command-line flags where a binary exposes a flag.

| Layer | What to know |
| --- | --- |
| Embedded defaults | Baseline host, port, discovery, database, TLS, metrics, and auth settings. |
| Environment variables | Main operator interface for services. Long-running services use a `VECTIS_<SERVICE>_...` prefix. |
| Command-line flags | Available on selected binaries and override the same setting for that process. |

Durations use Go-style strings such as `30s`, `1m`, or `1h`.

For service-scoped variables, take the service prefix, append the setting path with dots changed to underscores, and uppercase it. For example, a worker discovery registry address becomes:

```sh
VECTIS_WORKER_DISCOVERY_REGISTRY_ADDRESS=localhost:8082
```

Some settings are global and intentionally do not use a service prefix, such as `VECTIS_CELL_ID`, `VECTIS_DATABASE_*`, `VECTIS_GRPC_TLS_*`, `VECTIS_METRICS_TLS_*`, and `VECTIS_API_AUTH_*`.

## Common Settings {#common-operator-settings}

| Goal | Set |
| --- | --- |
| Change API HTTP port | `VECTIS_API_SERVER_PORT` or `vectis-api --port` |
| Bind API HTTP to another interface | `VECTIS_API_SERVER_HOST=0.0.0.0` or `vectis-api --host 0.0.0.0` |
| Expose local API and docs from a dev host | `vectis-local --host 0.0.0.0` |
| Set the execution cell identity | `VECTIS_CELL_ID=local` |
| Bind private cell ingress to another interface | `VECTIS_CELL_INGRESS_HOST=0.0.0.0` or `vectis-cell-ingress --host 0.0.0.0` |
| Route API dispatch to a remote cell | `vectis-api --cell-ingress-endpoint iad-a=http://iad.example:8085` |
| Enable API authentication | `VECTIS_API_AUTH_ENABLED=true` and, for a new database, `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` |
| Select authorization engine | `VECTIS_API_AUTHZ_ENGINE=hierarchical_rbac` or `authenticated_full` |
| Set PostgreSQL | `VECTIS_DATABASE_DRIVER=pgx` and `VECTIS_DATABASE_DSN=postgres://...` on every DB-using service |
| Tune PostgreSQL pool | `VECTIS_DATABASE_PGX_*` |
| Use structured service logs | `VECTIS_LOG_FORMAT=json` |
| Mirror service logs to files | `VECTIS_LOG_DIR=/path/to/dir` |
| Enable API access logs | `VECTIS_API_SERVER_LOG_FORMAT=json` |
| Pin worker to a queue address | `VECTIS_WORKER_WORKER_QUEUE_ADDRESS=host:8081` |
| Persist queue backlog to disk | `VECTIS_QUEUE_PERSISTENCE_DIR=/path/to/queue` |
| Change reconciler interval | `VECTIS_RECONCILER_INTERVAL=30s` |
| Change catalog event drain interval | `VECTIS_CATALOG_INTERVAL=1s` |
| Run `vectis-local` with plaintext internal gRPC | `vectis-local --grpc-insecure` or `VECTIS_LOCAL_GRPC_INSECURE=true` |

## Service Prefixes

Use these prefixes when building service-specific environment variable names.

| Program | Env prefix | Useful flags |
| --- | --- | --- |
| `vectis-api` | `VECTIS_API_SERVER` | `--host`, `--port`, `--cell-ingress-endpoint` |
| `vectis-cell-ingress` | `VECTIS_CELL_INGRESS` | `--host`, `--port`, `--metrics-port`, `--queue-address`, `--registry-address` |
| `vectis-queue` | `VECTIS_QUEUE` | `--port`, `--metrics-port`, `--persistence-dir`, `--persistence-snapshot-every` |
| `vectis-registry` | `VECTIS_REGISTRY` | `--port` |
| `vectis-log` | `VECTIS_LOG` | `--storage-dir`, `--metrics-port`, `--max-run-buffers` |
| `vectis-worker` | `VECTIS_WORKER` | `--metrics-port` |
| `vectis-cron` | `VECTIS_CRON` | none today |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `--interval`, `--metrics-port` |
| `vectis-catalog` | `VECTIS_CATALOG` | `--interval`, `--batch-size`, `--metrics-port` |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | see `vectis-log-forwarder --help` |
| `vectis-docs` | `VECTIS_DOCS` | `--host`, `--port`, `--dir` |
| `vectis-local` | `VECTIS_LOCAL` | `--host`, `--docs-port`, `--docs-dir`, `--log-level`, `--grpc-insecure` |
| `vectis-cli` | none for normal API commands | `VECTIS_API_TOKEN` for auth; `VECTIS_DATABASE_*` for `database migrate` |

The API client IP trust setting is an intentionally separate API-wide variable: `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS`.

## HTTP API Authentication {#http-api-authentication-vectis-api}

API authentication is off by default for local development. Enable it before exposing Vectis to shared or untrusted networks.

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_API_AUTH_ENABLED` / `api.auth.enabled` | Enables Bearer-token authentication on protected API routes after setup. |
| `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` / `api.auth.bootstrap_token` | Shared secret for `POST /api/v1/setup/complete` on a new database. Must be at least 16 characters until setup is recorded in the database. |
| `VECTIS_API_AUTHZ_ENGINE` / `api.authz.engine` | Selects authorization policy: `hierarchical_rbac` by default, or `authenticated_full` for simpler trusted setups. |

`vectis-cli login` calls `POST /api/v1/login` and saves the returned token in the OS user config directory. You can override the saved token for one shell session with:

```sh
export VECTIS_API_TOKEN=<token>
```

For the auth model and operational posture, see [Security](../concepts/security.md). For CLI auth commands, see [CLI Guide](../using/cli-guide.md).

## Audit And Rate Limits

API audit events are enabled by default.

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_API_AUDIT_ENABLED` / `api.audit.enabled` | Set to `false` to disable audit emission. |
| `VECTIS_API_AUDIT_DURABILITY_OVERRIDES` / `api.audit.durability_overrides` | Comma-separated `event=durability` overrides, such as `auth.success=disabled,run.triggered=best_effort`. |

API rate limits have embedded defaults for auth, token, and general routes. The shipped default keys live under `api.rate_limit.*`. The defaults are intended to protect the built-in auth surface from accidental or hostile bursts; tune them only when you understand the expected traffic shape.

When the API runs behind a trusted reverse proxy, configure client IP forwarding separately. See [Trusted Proxy Client IP](./deployment/trusted-proxy-client-ip.md).

## Database

Database settings are global. Every process that talks to SQL must use the same database driver and DSN.

| Variable | Purpose |
| --- | --- |
| `VECTIS_DATABASE_DRIVER` | `sqlite3` for local/single-node use, or `pgx` for PostgreSQL. |
| `VECTIS_DATABASE_DSN` | SQLite file path or PostgreSQL URL. If unset, SQLite defaults under the Vectis data directory. |

Applies to `vectis-api`, `vectis-cell-ingress`, `vectis-queue`, `vectis-worker`, `vectis-cron`, `vectis-reconciler`, `vectis-catalog`, and `vectis-cli database migrate`.

Runtime services wait for the expected schema; they do not apply migrations. Run migrations with:

```sh
vectis-cli database migrate
```

For migration policy and rollback planning, see [Database Migrations](../developing/migrations.md) and [Releases And Upgrades](../developing/releases.md).

## PostgreSQL Connection Pool {#postgresql-connection-pool-pgx-only}

When `VECTIS_DATABASE_DRIVER=pgx`, each DB-using process applies these `database/sql` pool settings after opening the database. SQLite ignores this block.

| Variable | Default | Purpose |
| --- | --- | --- |
| `VECTIS_DATABASE_PGX_MAX_OPEN_CONNS` | `25` | Maximum open connections per process. |
| `VECTIS_DATABASE_PGX_MAX_IDLE_CONNS` | `10` | Maximum idle connections per process, clamped to max open. |
| `VECTIS_DATABASE_PGX_CONN_MAX_LIFETIME` | `1h` | Maximum lifetime of a connection. |
| `VECTIS_DATABASE_PGX_CONN_MAX_IDLE_TIME` | `15m` | Maximum idle time before a connection is closed. |

These limits are per process. When you run multiple APIs, workers, cron, reconciler, and catalog instances, add the limits together when sizing Postgres.

## Internal gRPC TLS {#internal-grpc-tls}

Internal gRPC TLS settings are global across Vectis binaries.

| Variable | Purpose |
| --- | --- |
| `VECTIS_GRPC_TLS_INSECURE` | `true` means plaintext gRPC. `false` enables TLS and requires the relevant PEM files for each process role. |
| `VECTIS_GRPC_TLS_CA_FILE` | CA bundle used by clients to verify gRPC servers. |
| `VECTIS_GRPC_TLS_CERT_FILE` / `VECTIS_GRPC_TLS_KEY_FILE` | Server certificate and key for gRPC listeners. |
| `VECTIS_GRPC_TLS_CLIENT_CA_FILE` | If set on servers, requires client certificates signed by this CA. |
| `VECTIS_GRPC_TLS_CLIENT_CERT_FILE` / `VECTIS_GRPC_TLS_CLIENT_KEY_FILE` | Client certificate and key for mTLS. |
| `VECTIS_GRPC_TLS_SERVER_NAME` | Optional server-name override for outbound TLS verification. Useful when discovery returns an IP but the certificate is issued for a DNS name. |
| `VECTIS_GRPC_TLS_RELOAD_INTERVAL` | Positive duration to poll PEM files and reload them without restart. `0` disables polling. |

Standalone binaries default to plaintext gRPC. `vectis-local` normally bootstraps a local development CA and sets `VECTIS_GRPC_TLS_*` for child processes unless you pass `--grpc-insecure`. The Podman reference deployment also generates internal gRPC TLS material and mounts it into the Vectis containers.

| Role | Binaries | Required material when TLS is enabled |
| --- | --- | --- |
| gRPC listeners | `vectis-registry`, `vectis-queue`, `vectis-log`, worker-control listener in `vectis-worker` | Certificate and key. Queue/log also need a CA when they register with the registry. |
| gRPC clients | `vectis-api`, `vectis-cell-ingress`, `vectis-worker`, `vectis-cron`, `vectis-reconciler`, queue/log registration clients | CA bundle. Client cert/key only when servers require mTLS. |

For trust boundaries and what mTLS does or does not authorize today, see [Internal Service Trust](../concepts/internal-service-trust.md).

## Metrics TLS

`VECTIS_METRICS_TLS_*` settings apply to dedicated metrics listeners, not the API's main HTTP listener. API metrics are served on the same HTTP listener as the REST API.

| Variable | Purpose |
| --- | --- |
| `VECTIS_METRICS_TLS_INSECURE` | `true` means plaintext metrics HTTP. `false` enables HTTPS and requires cert/key files. |
| `VECTIS_METRICS_TLS_CERT_FILE` / `VECTIS_METRICS_TLS_KEY_FILE` | Server certificate and key for metrics listeners. |
| `VECTIS_METRICS_TLS_RELOAD_INTERVAL` | Positive duration to poll PEM files and reload them without restart. `0` disables polling. |

The dedicated metrics listeners are queue, worker, log, reconciler, catalog, and cell ingress. Keep metrics endpoints private; they are not authenticated. See [Security](../concepts/security.md).

## Discovery And Fixed Addresses {#service-discovery-vs-fixed-addresses}

Vectis can either discover services through `vectis-registry` or use fixed addresses.

| Pattern | Use when |
| --- | --- |
| Registry discovery | You want queue/log/worker-control addresses published and resolved dynamically. |
| Fixed addresses | You want fewer startup dependencies and already know the queue/log addresses. |

Role-specific settings override shared discovery settings when both are set.

Global API dispatch can route non-local execution cells to their private ingress endpoints with repeated `--cell-ingress-endpoint cell_id=url` flags or `VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS=iad-a=http://iad.example:8085,pdx-b=http://pdx.example:8085`. The API always keeps the local cell routed to the local queue; remote entries are only used when a run targets a different cell.

| What you are configuring | Shared setting segment | Role-specific examples |
| --- | --- | --- |
| Registry address | `DISCOVERY_REGISTRY_ADDRESS` | `API_REGISTRY_ADDRESS`, `WORKER_REGISTRY_ADDRESS`, `CRON_REGISTRY_ADDRESS`, `RECONCILER_REGISTRY_ADDRESS` |
| Queue address | `DISCOVERY_QUEUE_ADDRESS` or `DISCOVERY_QUEUE_RESOLVER_ADDRESS` | `API_QUEUE_ADDRESS`, `CELL_INGRESS_QUEUE_ADDRESS`, `WORKER_QUEUE_ADDRESS`, `CRON_QUEUE_ADDRESS`, `RECONCILER_QUEUE_ADDRESS` |
| Log gRPC address | `DISCOVERY_LOG_ADDRESS` or `DISCOVERY_LOG_GRPC_RESOLVER_ADDRESS` | `WORKER_LOG_ADDRESS` |
| Queue/log advertise address | `DISCOVERY_QUEUE_ADVERTISE_ADDRESS`, `DISCOVERY_LOG_GRPC_ADVERTISE_ADDRESS` | `QUEUE_ADVERTISE_ADDRESS`, `LOG_GRPC_ADVERTISE_ADDRESS` |

Replace the prefix with the service prefix. For example:

```sh
VECTIS_WORKER_DISCOVERY_REGISTRY_ADDRESS=localhost:8082
VECTIS_WORKER_WORKER_QUEUE_ADDRESS=localhost:8081
```

Registration toggles:

| Variable | Purpose |
| --- | --- |
| `VECTIS_QUEUE_QUEUE_REGISTER_WITH_REGISTRY` | Queue publishes its address to registry when enabled. |
| `VECTIS_LOG_LOG_GRPC_REGISTER_WITH_REGISTRY` | Log service publishes its gRPC address to registry when enabled. |
| `VECTIS_WORKER_WORKER_REGISTER_WITH_REGISTRY` | Worker publishes its worker-control address to registry when enabled. |

Discovery timing defaults include resolver refresh `10s`, poll timeout `5s`, error refresh `2s`, and registration heartbeat `45s`.

For failure behavior with and without registry, see [Failure Domains](../concepts/failure-domains.md#registry-down).

## Logs And Tracing

| Setting | Purpose |
| --- | --- |
| `VECTIS_<PREFIX>_LOG_LEVEL` | `debug`, `info`, `warn`, or `error` for a specific service. |
| `VECTIS_LOG_FORMAT=json` | Emits structured service logs as JSON on stderr. |
| `VECTIS_LOG_DIR=/path/to/dir` | Mirrors structured service logs to per-component `.jsonl` files. |
| `VECTIS_API_SERVER_LOG_FORMAT=json` | Emits API HTTP access logs as JSON on stderr, excluding `/health/*` and `/metrics`. |

Incoming API request IDs are handled as follows: a valid `X-Request-ID` or `X-Correlation-ID` is reused and echoed as `X-Request-ID`; otherwise the API generates a new UUID.

OpenTelemetry trace export is disabled unless configured:

| Variable | Purpose |
| --- | --- |
| `OTEL_TRACES_EXPORTER=otlp` | Enables OTLP trace export. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint, such as `http://127.0.0.1:4318`. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | OTLP transport/protocol, such as `http/protobuf`. |

## Queue, Logs, And Local Data

| Data | Default local path |
| --- | --- |
| SQLite database | `$XDG_DATA_HOME/vectis/db.sqlite3` |
| Queue persistence | `$XDG_DATA_HOME/vectis/queue` |
| Run log files | `$XDG_DATA_HOME/vectis/jobs` |
| `vectis-local` TLS material | `$XDG_DATA_HOME/vectis/local-tls` |

Queue persistence is configured with `VECTIS_QUEUE_PERSISTENCE_DIR` or `vectis-queue --persistence-dir`. An empty persistence directory disables queue persistence.

Treat database files, queue persistence, log storage, deployment secrets, and TLS material as part of the backup set when they hold production data. See [Backup And Restore](./reliability/backup-restore.md).

`vectis-cli local reset --dry-run` shows which local Vectis config, data, cache, token, TLS, and deployment-secret paths would be removed. `vectis-cli local reset --yes` removes those local paths; it does not stop running services or remove container volumes.

## Default Ports

| Surface | Default port |
| --- | --- |
| API HTTP and API `/metrics` | `8080` |
| Queue gRPC | `8081` |
| Registry gRPC | `8082` |
| Log gRPC | `8083` |
| Log HTTP/SSE | `8084` |
| Docs HTTP | `8088` |
| Queue metrics | `9081` |
| Worker metrics | `9082` |
| Log metrics | `9083` |
| Worker-control gRPC | `9084` in static mode |
| Reconciler metrics | `9085` |

## Reference Deployment Notes

`vectis-cli deploy podman up` generates local deployment secrets and TLS material under the deployment config directory. Set `VECTIS_DEPLOY_CONFIG_DIR` to choose where rendered manifests and local deployment secrets are stored.

The Podman reference deployment:

- enables internal gRPC TLS for Vectis containers;
- enables TLS from Vectis containers to the bundled Postgres instance;
- enables HTTPS for queue, worker, and log metrics scrapes;
- runs Prometheus, Grafana, Jaeger, OpenSearch, and Fluent Bit as a reference observability stack.

Treat the reference deployment as a helpful starting point, not a production security boundary by itself. Rotate generated secrets into your platform's secret store for shared environments. See [Reference Deployment Posture](./deployment/reference-deployment-posture.md).

## Related Documentation

| Topic | Document |
| --- | --- |
| Components and flows | [Architecture](../concepts/architecture.md) |
| Security posture | [Security](../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../concepts/internal-service-trust.md) |
| Failure behavior and probes | [Failure Domains](../concepts/failure-domains.md) |
| Log streaming behavior | [Log Streaming](../using/log-streaming.md) |
| Runbooks and alerts | [Runbooks](./reliability/runbooks.md) |
| Repair recipes | [Repair Runbooks](./reliability/repair-runbooks.md) |
| Dispatch handoff triage | [Dispatch Visibility](./reliability/dispatch-visibility.md) |
| Backup and restore | [Backup And Restore](./reliability/backup-restore.md) |
| Trusted proxy client IPs | [Trusted Proxy Client IP](./deployment/trusted-proxy-client-ip.md) |
| Releases and upgrades | [Releases And Upgrades](../developing/releases.md) |
