# Configuration

This page is for people running Vectis: local developers, platform engineers, and operators wiring staging or production. It explains the settings you are most likely to touch, where defaults come from, and which knobs affect service discovery, storage, TLS, metrics, and authentication.

For service roles and data flow, see [Architecture](../concepts/architecture.md). For multi-cell routing, see [Multi-Cell Operation](./multi-cell.md). For security posture, see [Security](../concepts/security.md). For startup and outage behavior, see [Failure Domains](../concepts/failure-domains.md). For terms such as job, run, queue, and dispatch, see [Glossary](../concepts/glossary.md).

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

Some settings are global and intentionally do not use a service prefix, such as `VECTIS_CELL_ID`, `VECTIS_DATABASE_*`, `VECTIS_GLOBAL_DATABASE_DSN`, `VECTIS_CELL_DATABASE_DSN`, `VECTIS_GRPC_TLS_*`, `VECTIS_METRICS_TLS_*`, and `VECTIS_API_AUTH_*`.

## Common Settings {#common-operator-settings}

| Goal | Set |
| --- | --- |
| Change API HTTP port | `VECTIS_API_SERVER_PORT` or `vectis-api --port` |
| Bind API HTTP to another interface | `VECTIS_API_SERVER_HOST=0.0.0.0` or `vectis-api --host 0.0.0.0` |
| Expose local API and docs from a dev host | `vectis-local --host 0.0.0.0` |
| Add local execution cells for routing tests | `vectis-local --cell pdx-b --cell sjc-c` |
| Set the execution cell identity | `VECTIS_CELL_ID=local` |
| Bind private cell ingress to another interface | `VECTIS_CELL_INGRESS_HOST=0.0.0.0` or `vectis-cell-ingress --host 0.0.0.0` |
| Route API dispatch to a remote cell | `vectis-api --cell-ingress-endpoint iad-a=http://iad.example:8085` |
| Enable API authentication | `VECTIS_API_AUTH_ENABLED=true` and, for a new database, `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` |
| Select authorization engine | `VECTIS_API_AUTHZ_ENGINE=hierarchical_rbac` or `authenticated_full` |
| Set PostgreSQL | `VECTIS_DATABASE_DRIVER=pgx` and `VECTIS_DATABASE_DSN=postgres://...`, or role-specific `VECTIS_GLOBAL_DATABASE_DSN` / `VECTIS_CELL_DATABASE_DSN` |
| Tune PostgreSQL pool | `VECTIS_DATABASE_PGX_*` |
| Use structured service logs | `VECTIS_LOG_FORMAT=json` |
| Mirror service logs to files | `VECTIS_LOG_DIR=/path/to/dir` |
| Enable API access logs | `VECTIS_API_SERVER_LOG_FORMAT=json` |
| Pin worker to a queue address | `VECTIS_WORKER_QUEUE_ADDRESS=host:8081` |
| Persist queue backlog to disk | `VECTIS_QUEUE_PERSISTENCE_DIR=/path/to/queue-shard` |
| Change reconciler interval | `VECTIS_RECONCILER_INTERVAL=30s` |
| Change reconciler failover TTL | `VECTIS_RECONCILER_LEASE_TTL=2m` |
| Set cron claim TTL | `VECTIS_CRON_CLAIM_TTL=5m` or `vectis-cron --claim-ttl 5m` |
| Name a cron replica in claim records | `VECTIS_CRON_INSTANCE_ID=cron-a` or `vectis-cron --instance-id cron-a` |
| Change catalog event drain interval | `VECTIS_CATALOG_INTERVAL=1s` |
| Fan in cell-local catalog events | `vectis-catalog --cell-database-dsn pdx-b=/path/to/pdx.db` |
| Run `vectis-local` with plaintext internal gRPC | `vectis-local --grpc-insecure` or `VECTIS_LOCAL_GRPC_INSECURE=true` |

## Service Prefixes

Use these prefixes when building service-specific environment variable names.

| Program | Env prefix | Useful flags |
| --- | --- | --- |
| `vectis-api` | `VECTIS_API_SERVER` | `--host`, `--port`, `--cell-ingress-endpoint` |
| `vectis-cell-ingress` | `VECTIS_CELL_INGRESS` | `--host`, `--port`, `--metrics-port`, `--repair-interval`, `--queue-address`, `--registry-address` |
| `vectis-queue` | `VECTIS_QUEUE` | `--port`, `--metrics-port`, `--pool`, `--instance-id`, `--persistence-dir`, `--persistence-snapshot-every` |
| `vectis-registry` | `VECTIS_REGISTRY` | `--port` |
| `vectis-log` | `VECTIS_LOG` | `--instance-id`, `--storage-dir`, `--storage-read-only-min-free-bytes`, `--metrics-port`, `--max-run-buffers` |
| `vectis-worker` | `VECTIS_WORKER` | `--metrics-port` |
| `vectis-cron` | `VECTIS_CRON` | `--instance-id`, `--claim-ttl` |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `--interval`, `--lease-ttl`, `--metrics-port` |
| `vectis-catalog` | `VECTIS_CATALOG` | `--interval`, `--batch-size`, `--metrics-port`, `--cell-database-dsn` |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | see `vectis-log-forwarder --help` |
| `vectis-docs` | `VECTIS_DOCS` | `--host`, `--port`, `--dir` |
| `vectis-local` | `VECTIS_LOCAL` | `--host`, `--cell`, `--docs-port`, `--docs-dir`, `--log-level`, `--grpc-insecure` |
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

Database driver settings are global. DSNs can be shared for single-node deployments or split by global and cell roles.

| Variable | Purpose |
| --- | --- |
| `VECTIS_DATABASE_DRIVER` | `sqlite3` for local/single-node use, or `pgx` for PostgreSQL. |
| `VECTIS_DATABASE_DSN` | Shared SQLite file path or PostgreSQL URL. If unset, SQLite defaults under the Vectis data directory. |
| `VECTIS_GLOBAL_DATABASE_DSN` | Overrides the shared DSN for global services: API, cron, reconciler, and catalog. |
| `VECTIS_CELL_DATABASE_DSN` | Overrides the shared DSN for cell-local services: cell ingress and workers. |
| `VECTIS_CATALOG_CELL_DATABASE_DSNS` | Comma-separated `cell_id=dsn` list that lets `vectis-catalog` fan in pending catalog events from cell-local databases. |

`vectis-local` uses split SQLite files by default when no database DSN is set: one global DB and one DB for each local execution cell. Standalone services keep using `VECTIS_DATABASE_DSN` unless the matching role-specific DSN is set. Multi-cell `vectis-local --cell ...` currently requires the default managed SQLite layout so each local cell gets its own DB.

When global and cell databases are split, workers record status changes into the cell-local catalog event inbox. Run `vectis-catalog` against the global database and pass each cell database with repeated `--cell-database-dsn cell_id=dsn` flags, or with `VECTIS_CATALOG_CELL_DATABASE_DSNS=iad-a=/path/iad.db,pdx-b=/path/pdx.db`. `vectis-catalog` also backfills missing catalog events from observed run and execution state before draining an inbox, which repairs the narrow case where a state transition committed but the matching catalog event write did not. `vectis-local` wires this automatically for its managed local cells. See [Multi-Cell Operation](./multi-cell.md) for the full stack shape and repair flow.

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

Global producers can route execution cells to private ingress endpoints with repeated `vectis-api --cell-ingress-endpoint cell_id=url`, `VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS=iad-a=http://iad.example:8085,pdx-b=http://pdx.example:8085`, or shared `VECTIS_CELL_INGRESS_ENDPOINTS`. `vectis-reconciler` and `vectis-cron` use the same shared endpoint map unless their role-specific endpoint variables are set. When the local cell has an ingress endpoint configured, producers send local executions through ingress instead of writing directly to the local queue. If `VECTIS_GLOBAL_DATABASE_DSN` and `VECTIS_CELL_DATABASE_DSN` point at different databases, configure an ingress endpoint for every execution target, including the local cell; direct local queue fallback is disabled.

For local routing tests, `vectis-local --cell pdx-b` starts an additional queue, cell ingress, and worker for `pdx-b`, pins those cell-local processes to their queue, and publishes all local ingress endpoints through `VECTIS_CELL_INGRESS_ENDPOINTS`.

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

When registry discovery is used, multiple `vectis-queue` instances may register as a pool. Each queue needs one stable `VECTIS_QUEUE_INSTANCE_ID` / `--instance-id`; if it is omitted, `vectis-queue` derives a stable ID from the system hostname and queue port. Producers choose among discovered queue shards; workers ack back to the shard encoded in the delivery ID.

`VECTIS_QUEUE_POOL` / `--pool` names the local queue pool used when deriving the default persistence path. If `VECTIS_QUEUE_PERSISTENCE_DIR` / `--persistence-dir` is omitted, the queue uses `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>`. Set a persistence directory explicitly only when you want to pin storage layout. An explicitly empty persistence directory disables queue persistence.

Queue instance IDs must be unique among active queue processes registered in the same registry, except during a controlled replacement of the same shard with the same persistence directory. If two active queues register the same instance ID, the registry treats them as the same logical shard and the later registration wins; workers may route acks to the wrong process. If two active queues point at the same persistence directory, the second queue refuses to start.

When registry discovery is used, multiple `vectis-log` instances may register as run shards. Each log shard needs one stable `VECTIS_LOG_INSTANCE_ID` / `--instance-id`; if it is omitted, `vectis-log` derives a stable ID from the system hostname and log gRPC port. DB-aware clients record the chosen shard in `job_runs.log_shard_id` and route future reads/writes for that run back to the assigned shard. When a worker sends logs through a local `vectis-log-forwarder`, it stamps the assigned shard into the socket/spool protocol so the DB-free forwarder preserves the same route. Unassigned runs fall back to deterministic `run_id` hashing. Keep instance IDs and storage directories stable across restarts. If two active log processes point at the same storage directory, the second log process refuses to start.

`VECTIS_LOG_STORAGE_DIR` / `--storage-dir` stores durable run log files. If omitted, the log service uses `$XDG_DATA_HOME/vectis/log/<instance-id>`. `VECTIS_LOG_STORAGE_READ_ONLY_MIN_FREE_BYTES` / `--storage-read-only-min-free-bytes` defaults to `1073741824` (1 GiB). Below that threshold, the shard advertises `read_only` for new runs and refuses the first append for a run that does not already have a log file; stored logs remain readable, and existing assigned run files can continue to receive appends. Set the value to `0` to disable the threshold.

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
| Queue persistence | `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>` |
| Run log files | `$XDG_DATA_HOME/vectis/log/<instance-id>` |
| `vectis-local` TLS material | `$XDG_DATA_HOME/vectis/local-tls` |

Queue persistence is configured with `VECTIS_QUEUE_PERSISTENCE_DIR` or `vectis-queue --persistence-dir`. When unset, the default path is derived from `VECTIS_QUEUE_POOL` / `--pool` and `VECTIS_QUEUE_INSTANCE_ID` / `--instance-id`. An empty persistence directory disables queue persistence.

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

Each extra `vectis-local --cell` uses the default cell-local ports plus `100` per additional cell. For example, the first extra cell uses queue `8181`, cell ingress `8185`, queue metrics `9181`, worker metrics `9182`, and cell ingress metrics `9187`. Multi-cell local workers use ephemeral worker-control ports.

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
