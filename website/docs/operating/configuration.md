# Configuration

This page is for people running Vectis: local developers, platform engineers, and operators wiring staging or production. It explains the settings you are most likely to touch, where defaults come from, and which knobs affect service discovery, storage, TLS, metrics, and authentication.

For service roles and data flow, see [Architecture](../concepts/architecture.md). For production-required setting groups and secret inventory, see [Production Config And Secrets Contract](./deployment/production-config-contract.md). For multi-cell routing, see [Multi-Cell Operation](./multi-cell.md). For security posture, see [Security](../concepts/security.md). For startup and outage behavior, see [Failure Domains](../concepts/failure-domains.md). For terms such as job, run, queue, and dispatch, see [Glossary](../concepts/glossary.md).

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

Some settings are global and intentionally do not use a service prefix, such as `VECTIS_CELL_ID`, `VECTIS_DATABASE_*`, `VECTIS_GLOBAL_DATABASE_DSN`, `VECTIS_CELL_DATABASE_DSN`, `VECTIS_GRPC_TLS_*`, `VECTIS_METRICS_TLS_*`, `VECTIS_DISPATCH_START_TTL`, `VECTIS_API_AUTH_*`, and `VECTIS_ACTION_REGISTRY_*`.

`VECTIS_DISPATCH_START_TTL` sets how long a root or task execution may remain dispatchable before Vectis refuses to start it. The default is `24h`. Producers stamp the deadline into the execution envelope, queues drop expired deliveries instead of redelivering them, the worker refuses a database claim after the deadline, and the reconciler marks expired queued executions failed with failure code `dispatch_expired`.

## Common Settings {#common-operator-settings}

| Goal | Set |
| --- | --- |
| Change API HTTP port | `VECTIS_API_SERVER_PORT` or `vectis-api --port` |
| Bind API HTTP to another interface | `VECTIS_API_SERVER_HOST=0.0.0.0` or `vectis-api --host 0.0.0.0` |
| Expose local API and docs from a dev host | `VECTIS_DOCS_ALLOWED_HOSTS=<dev-host> vectis-local --host 0.0.0.0` |
| Add local execution cells for routing tests | `vectis-local --cell pdx-b --cell sjc-c` |
| Run a local multi-instance HA exercise cell | `vectis-local --profile ha` or `VECTIS_LOCAL_PROFILE=ha` |
| Run local source-only jobs from a checkout | `vectis-local --source-only --source-repository vectis-local=/path/to/repo` |
| Run a local SPIFFE secret-resolution smoke test | `vectis-local --spiffe-trust-domain vectis.internal` |
| Run the Podman HA reference profile | `vectis-cli deploy podman --profile ha up` |
| Set the execution cell identity | `VECTIS_CELL_ID=local` |
| Bind private cell ingress to another interface | `VECTIS_CELL_INGRESS_HOST=0.0.0.0`, internal mTLS via `VECTIS_GRPC_TLS_*`, plus a matching static endpoint or `VECTIS_CELL_INGRESS_ALLOWED_HOSTS=<ingress-host>` |
| Route API dispatch to a remote cell | `vectis-api --cell-ingress-endpoint iad-a=https://iad.example:8085` |
| Enable API authentication | `VECTIS_API_AUTH_ENABLED=true` and, for a new database, `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` |
| Select authorization engine | `VECTIS_API_AUTHZ_ENGINE=hierarchical_rbac` or `authenticated_full` |
| Enable local custom actions | `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS=/path/to/actions`; use `vectis-cli actions list` to inspect |
| Require custom action digest pins | `VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS=true` |
| Set PostgreSQL | `VECTIS_DATABASE_DRIVER=pgx` and `VECTIS_DATABASE_DSN=postgres://...`, or role-specific `VECTIS_GLOBAL_DATABASE_DSN` / `VECTIS_CELL_DATABASE_DSN` |
| Tune PostgreSQL pool | `VECTIS_DATABASE_PGX_*` |
| Use structured service logs | `VECTIS_LOG_FORMAT=json` |
| Mirror service logs to files | `VECTIS_LOG_DIR=/path/to/dir` |
| Enable API access logs | `VECTIS_API_SERVER_LOG_FORMAT=json` |
| Serve docs behind an external hostname | `VECTIS_DOCS_ALLOWED_HOSTS=docs.example.com` or `vectis-docs --allowed-host docs.example.com` |
| Pin worker to a queue address | `VECTIS_WORKER_QUEUE_ADDRESS=host:8081` |
| Run worker commands through a Lima VM | `VECTIS_WORKER_EXECUTION_BACKEND=lima`, `VECTIS_WORKER_LIMA_INSTANCE=vectis-worker`, and `VECTIS_WORKER_WORKSPACE_ROOT=/path/mounted/in/guest` |
| Pin worker to an orchestrator address | `VECTIS_WORKER_ORCHESTRATOR_ADDRESS=host:8087` |
| Persist queue backlog to disk | `VECTIS_QUEUE_PERSISTENCE_DIR=/path/to/queue-shard` |
| Expire queued work that never starts | `VECTIS_DISPATCH_START_TTL=24h` |
| Expose a dedicated metrics listener off-host | Set the service `--metrics-host` flag or `VECTIS_<SERVICE>_METRICS_HOST=0.0.0.0` plus `VECTIS_METRICS_ALLOWED_HOSTS=<scrape-host>` |
| Change reconciler interval | `VECTIS_RECONCILER_INTERVAL=30s` |
| Change reconciler failover TTL | `VECTIS_RECONCILER_LEASE_TTL=2m` |
| Set cron claim TTL | `VECTIS_CRON_CLAIM_TTL=5m` or `vectis-cron --claim-ttl 5m` |
| Name a cron replica in claim records | `VECTIS_CRON_INSTANCE_ID=cron-a` or `vectis-cron --instance-id cron-a` |
| Change catalog event drain interval | `VECTIS_CATALOG_INTERVAL=1s` |
| Fan in cell-local catalog events | `vectis-catalog --cell-database-dsn pdx-b=/path/to/pdx.db` |
| Run `vectis-local` with plaintext internal gRPC | `vectis-local --grpc-insecure` or `VECTIS_LOCAL_GRPC_INSECURE=true`; local secrets are disabled in this mode |

## Source Repositories

`vectis-api` can reconcile source repository registrations at startup. This is the declarative path for configure-as-code instances: declare the repository list, start the API, then trigger jobs directly from source or source-backed schedules.

Use `VECTIS_SOURCE_REPOSITORIES` or `VECTIS_API_SERVER_SOURCE_REPOSITORIES` with a JSON array:

```sh
export VECTIS_SOURCE_REPOSITORIES='[
  {
    "repository_id": "vectis-local",
    "checkout_mode": "managed",
    "canonical_url": "https://git.example.com/acme/vectis.git",
    "default_ref": "main",
    "authoring_mode": "read_only",
    "enabled": true
  }
]'
```

Each entry accepts `repository_id`, `namespace`, `source_kind`, `checkout_path`, `checkout_mode`, `authoring_mode`, `canonical_url`, `default_ref`, `credential_ref`, and `enabled`. `namespace` defaults to `/`, `source_kind` defaults to `local_checkout`, `checkout_mode` defaults to `external`, `authoring_mode` defaults to `read_only`, and `enabled` defaults to `true`. For `checkout_mode=managed`, omit `checkout_path`; Vectis derives a stable path under `source.checkout_root` / `VECTIS_SOURCE_CHECKOUT_ROOT`.

Managed checkout sync can use `credential_ref` for private repositories. Configure `vectis-api` with `--source-credentials-encryptedfs-root` / `VECTIS_API_SERVER_SOURCE_CREDENTIALS_ENCRYPTEDFS_ROOT` and `--source-credentials-encryptedfs-key-file` / `VECTIS_API_SERVER_SOURCE_CREDENTIALS_ENCRYPTEDFS_KEY_FILE`; the referenced encryptedfs secret may contain either a raw HTTPS token, JSON like `{"username":"oauth2","token":"..."}` or `{"username":"git","password":"..."}`, raw SSH private key material, or JSON like `{"ssh_private_key":"...","known_hosts":"git.example ssh-ed25519 ..."}`. HTTPS credentials are supplied to `git clone` and `git fetch` through askpass environment variables, while SSH credentials are supplied through a temporary identity file and `GIT_SSH_COMMAND`. Include `known_hosts` to pin SSH host keys; when it is omitted, OpenSSH's normal host-key verification applies. Credentials are not embedded in the remote URL.

Startup reconciliation creates missing repository registrations and updates changed checkout, authoring, default ref, credential, and enabled fields. It does not delete repositories omitted from config. If a configured repository already exists in a different namespace, startup fails so operators can make an explicit move plan.

Repository responses expose `declared`, so operators can distinguish repositories still present in current config from stale rows left for cleanup or history. Use `vectis-cli sources list --stale` to find omitted repositories. Declared repositories cannot be deleted through the API; remove them from config first, then disable or delete the stale row only if no source schedules or source provenance still reference it.

Set `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP=true` to also sync enabled configured repositories during `vectis-api` startup. This is off by default so large repositories do not surprise-block deployments. When enabled, external checkouts are probed and managed checkouts are cloned or fetched; sync status, ref, commit, timestamps, and errors are persisted on the repository record. A failed startup sync fails API startup, and the sync operation uses `source.sync_running_timeout` / `VECTIS_SOURCE_SYNC_RUNNING_TIMEOUT` as its timeout window.

Set `source.sync_configured_repositories_interval` / `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_INTERVAL` to a positive duration to refresh enabled configured repositories in the background while `vectis-api` is running. The default is `0s`, which disables periodic sync. Use `source.sync_configured_repositories_max_concurrency` / `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_MAX_CONCURRENCY` to limit concurrent checkout probes or fetches, and `source.sync_configured_repositories_failure_backoff` / `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_FAILURE_BACKOFF` to skip repositories with recent failed syncs before retrying them. Periodic sync records failures on repository status and logs them, but it does not stop the API process or block startup.

Declare source-backed cron schedules with `VECTIS_SOURCE_SCHEDULES` or `VECTIS_API_SERVER_SOURCE_SCHEDULES`:

```sh
export VECTIS_SOURCE_SCHEDULES='[
  {
    "schedule_id": "nightly-build",
    "repository_id": "vectis-local",
    "job_id": "build.nightly",
    "cron_spec": "0 2 * * *",
    "ref": "main",
    "enabled": true
  }
]'
```

Each schedule entry accepts `schedule_id`, `repository_id`, `job_id`, `cron_spec`, `ref`, `path`, and `enabled`. `schedule_id` is the stable reconcile key. `enabled` defaults to `true`. If `path` is omitted, Vectis derives the definition path from the job ID using the default `.vectis/jobs/...` layout. Startup reconciliation creates missing schedules and updates changed repository, job, cron, ref, path, and enabled fields; it does not delete schedules omitted from config. Enabled schedules must reference an enabled configured repository. Use `GET /api/v1/source-schedules` or `GET /api/v1/source-repositories/{id}/schedules` to inspect reconciled schedules, whether each row is still declared in current config, configured ref/path, active override, next run time, and effective ref/path. Use `PATCH /api/v1/source-schedules/{schedule_id}` or `vectis-cli sources disable-schedule` to stop stale rows that are no longer declared; declared schedules may be re-updated by later config reconciliation. Delete stale disabled rows with `DELETE /api/v1/source-schedules/{schedule_id}` or `vectis-cli sources delete-schedule <schedule-id> --yes` after clearing any active override.

For production hotfixes, operators can set a temporary source schedule override with `PUT /api/v1/source-schedules/{schedule_id}/override` or `vectis-cli sources override`. Overrides can replace the configured ref, path, or both until the fix lands in the declared repository location. Config reconciliation preserves the active override; clear it explicitly with `DELETE /api/v1/source-schedules/{schedule_id}/override` or `vectis-cli sources clear-override` to return future runs to the configured ref/path.

For source-only deployments, declare at least one source repository and use `GET /api/v1/source/status` or `vectis-cli health check` to verify that the API reports source repository persistence configured and at least one enabled source repository available for reusable job triggers and source schedules. For local development, `vectis-local --source-only --source-repository vectis-local=/path/to/repo` sets the API source repository environment for you and enables startup sync for the declared checkout.

## Service Prefixes

Use these prefixes when building service-specific environment variable names.

| Program | Env prefix | Useful flags |
| --- | --- | --- |
| `vectis-api` | `VECTIS_API_SERVER` | `--host`, `--port`, `--cell-ingress-endpoint`, `--tls-cert-file`, `--tls-key-file` |
| `vectis-cell-ingress` | `VECTIS_CELL_INGRESS` | `--host`, `--port`, `--allowed-host`, `--metrics-host`, `--metrics-port`, `--repair-interval`, `--queue-address`, `--registry-address` |
| `vectis-queue` | `VECTIS_QUEUE` | `--port`, `--metrics-host`, `--metrics-port`, `--pool`, `--instance-id`, `--persistence-dir`, `--persistence-snapshot-every` |
| `vectis-orchestrator` | `VECTIS_ORCHESTRATOR` | `--port`, `--metrics-port`, `--shards` |
| `vectis-registry` | `VECTIS_REGISTRY` | `--port`; cluster membership uses `VECTIS_REGISTRY_CLUSTER_*` |
| `vectis-log` | `VECTIS_LOG` | `--instance-id`, `--storage-dir`, `--storage-read-only-min-free-bytes`, `--grpc-port`, `--metrics-host`, `--metrics-port`, `--max-run-buffers` |
| `vectis-artifact` | `VECTIS_ARTIFACT` | `--instance-id`, `--storage-dir`, `--storage-read-only-min-free-bytes`, `--grpc-port`, `--metrics-host`, `--metrics-port` |
| `vectis-secrets` | `VECTIS_SECRETS` | `--port`, `--metrics-host`, `--metrics-port`, `--encryptedfs-root`, `--encryptedfs-key-file`, `--allow-secret` |
| `vectis-spiffe` | `VECTIS_SPIFFE` | `--trust-domain`, `--data-dir`, `--runtime-dir`, `--workload-socket`, `--registration-socket`, `--bundle-file`, `--selector`, `--x509-svid-ttl`, `--init-only` |
| `vectis-worker` | `VECTIS_WORKER` | `--metrics-host`, `--metrics-port`, `--artifact-max-bytes`, `--artifact-max-run-bytes`, `--artifact-max-count`, `--core-socket`, `--core-shell-socket`, `--core-connect-timeout`, `--secrets-address`; use `VECTIS_WORKER_QUEUE_ADDRESS`, `VECTIS_WORKER_LOG_ADDRESS`, `VECTIS_WORKER_ORCHESTRATOR_ADDRESS`, and `VECTIS_WORKER_SECRETS_ADDRESS` to pin internal dependencies |
| `vectis-worker-core` | `VECTIS_WORKER_CORE` | `--socket`, `--execution-backend`, `--workspace-root`, `--lima-instance`, `--lima-start` |
| `vectis-cron` | `VECTIS_CRON` | `--instance-id`, `--claim-ttl` |
| `vectis-reconciler` | `VECTIS_RECONCILER` | `--interval`, `--lease-ttl`, `--metrics-host`, `--metrics-port` |
| `vectis-catalog` | `VECTIS_CATALOG` | `--interval`, `--batch-size`, `--metrics-host`, `--metrics-port`, `--cell-database-dsn` |
| `vectis-log-forwarder` | `VECTIS_LOG_FORWARDER` | `--socket`, `--lockfile`, `--spool-dir`, `--metrics-host`, `--metrics-port` |
| `vectis-docs` | `VECTIS_DOCS` | `--host`, `--port`, `--dir`, `--allowed-host`, `--tls-cert-file`, `--tls-key-file` |
| `vectis-local` | `VECTIS_LOCAL` | `--profile`, `--host`, `--cell`, `--docs-port`, `--docs-dir`, `--log-level`, `--grpc-insecure`, `--http-tls`, `--tls-dir`, `--source-only`, `--source-repository`; local SPIFFE smoke-test flags: `--spiffe-trust-domain`, `--spiffe-dir`, `--spiffe-runtime-dir`, `--spiffe-parent-id`, `--spiffe-selector`; subcommands: `init`, `install-cert` |
| `vectis-cli` | none for normal API commands | `VECTIS_API_TOKEN` for auth; `VECTIS_DATABASE_*` for `database migrate` |

The API client IP trust setting is an intentionally separate API-wide variable: `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS`.

## HTTP API Authentication {#http-api-authentication-vectis-api}

API authentication is off by default for local development. Enable it before exposing Vectis to shared or untrusted networks.

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_API_AUTH_ENABLED` / `api.auth.enabled` | Enables Bearer-token authentication on protected API routes after setup. |
| `VECTIS_API_AUTH_BOOTSTRAP_TOKEN` / `api.auth.bootstrap_token` | Shared secret for `POST /api/v1/setup/complete` on a new database. Must be at least 16 characters until setup is recorded in the database. |
| `VECTIS_API_AUTHZ_ENGINE` / `api.authz.engine` | Selects authorization policy: `hierarchical_rbac` by default, or `authenticated_full` for simpler trusted setups. |
| `VECTIS_API_ALLOWED_HOSTS` / `api.host_validation.allowed_hosts` | Comma-separated exact Host header allowlist for the browser-facing API. Defaults to the API listen host plus loopback names; configure external DNS names when serving behind an ingress or binding to `0.0.0.0`. |

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

API Host, CORS, CSRF, request-target, method, media-type, body-policy, and rate-limit rejects emit sanitized warning logs and increment `vectis_api_security_rejections_total`. Alert on sustained or sudden increases by `reason`; the metric uses only `reason`, `route`, and `status` labels to avoid attacker-controlled cardinality.

API CORS is closed by default. Same-origin `Origin` headers, matching the browser-facing scheme, host, and port, are allowed without CORS response headers. Set `api.cors.allowed_origins` or `VECTIS_API_CORS_ALLOWED_ORIGINS` to a comma-separated list of exact browser origins, such as `https://ui.example.com` or `http://localhost:3000`. Non-local browser frontends must use `https://`; `http://` origins are accepted only for loopback or localhost development. Origins with wildcards, `null`, paths, query strings, user info, or non-HTTP schemes are rejected. Allowed browser requests receive credentialed CORS headers; disallowed cross-origin actual requests and preflights are rejected before route handling.

API Host header validation is enabled by default. Requests whose `Host` does not match `api.host_validation.allowed_hosts` / `VECTIS_API_ALLOWED_HOSTS` are rejected before route handling. Allowed host entries are hostnames or IP literals, optionally with a port; wildcard, URL, path, query, and userinfo forms are rejected at startup.

Docs Host header validation is also enabled by default. `vectis-docs` accepts the docs bind host plus loopback names unless `--allowed-host` / `VECTIS_DOCS_ALLOWED_HOSTS` is set. Configure the browser-facing docs hostname when publishing docs behind an ingress, reverse proxy, or non-loopback bind address.

Dedicated metrics Host header validation is enabled by default. Metrics listeners accept their bind host plus loopback names unless `metrics_allowed_hosts`, `metrics.allowed_hosts`, `VECTIS_<SERVICE>_METRICS_ALLOWED_HOSTS`, or `VECTIS_METRICS_ALLOWED_HOSTS` is set. Configure the expected scraper hostname when publishing metrics outside localhost or binding a metrics listener to `0.0.0.0`.

Cell ingress Host header validation is enabled by default. `vectis-cell-ingress` accepts the ingress bind host, loopback names, and the Host from the local cell's static `cell_ingress.endpoints` / `VECTIS_CELL_INGRESS_ENDPOINTS` entry. If the producer route map is not present on the cell process, set `--allowed-host` / `VECTIS_CELL_INGRESS_ALLOWED_HOSTS` or `cell_ingress.allowed_hosts` explicitly. Cell ingress HTTP uses internal mTLS when `VECTIS_GRPC_TLS_INSECURE=false`; `vectis-cell-ingress` requires `grpc_tls.cert_file`, `grpc_tls.key_file`, and `grpc_tls.client_ca_file`, while producers using `https://` cell ingress endpoints require `grpc_tls.ca_file`, `grpc_tls.client_cert_file`, and `grpc_tls.client_key_file`. `vectis-cell-ingress` refuses startup without mTLS when the ingress bind host or the local cell's static endpoint is non-loopback.

Internal service identity allowlists are disabled by default. Configure them with exact comma-separated SPIFFE URI SANs when internal callers use mTLS client certificates:

| Variable / key | Protected listener |
| --- | --- |
| `VECTIS_SERVICE_IDENTITY_REGISTRY_ALLOWED_CLIENT_IDENTITIES` / `service_identity.registry_allowed_client_identities` | `vectis-registry` gRPC |
| `VECTIS_SERVICE_IDENTITY_QUEUE_ALLOWED_CLIENT_IDENTITIES` / `service_identity.queue_allowed_client_identities` | `vectis-queue` gRPC |
| `VECTIS_SERVICE_IDENTITY_LOG_ALLOWED_CLIENT_IDENTITIES` / `service_identity.log_allowed_client_identities` | `vectis-log` gRPC |
| `VECTIS_SERVICE_IDENTITY_ARTIFACT_ALLOWED_CLIENT_IDENTITIES` / `service_identity.artifact_allowed_client_identities` | `vectis-artifact` gRPC |
| `VECTIS_SERVICE_IDENTITY_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES` / `service_identity.orchestrator_allowed_client_identities` | `vectis-orchestrator` gRPC |
| `VECTIS_SERVICE_IDENTITY_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES` / `service_identity.worker_control_allowed_client_identities` | Worker-control gRPC |
| `VECTIS_SERVICE_IDENTITY_SECRETS_ALLOWED_CLIENT_IDENTITIES` / `service_identity.secrets_allowed_client_identities` | `vectis-secrets` gRPC |
| `VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES` / `service_identity.cell_ingress_allowed_producer_identities` | Cell ingress `POST /cell/v1/executions` |

Short aliases such as `VECTIS_QUEUE_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_REGISTRY_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_LOG_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_ARTIFACT_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES`, `VECTIS_SECRETS_ALLOWED_CLIENT_IDENTITIES`, and `VECTIS_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES` are also accepted. Each configured identity must be a `spiffe://` URI with a trust domain and workload path. Any non-empty allowlist requires `VECTIS_GRPC_TLS_INSECURE=false` and `VECTIS_GRPC_TLS_CLIENT_CA_FILE` so the listener verifies client certificates before checking URI SANs.

Worker artifact uploads are capped by `VECTIS_WORKER_ARTIFACT_MAX_BYTES` / `worker.artifact_max_bytes` / `--artifact-max-bytes`. The default is `1073741824` bytes (1 GiB), and `0` disables the worker-level cap. `builtins/upload-artifact` can set `max_bytes` to use a lower per-node limit, but it cannot raise an upload above the worker cap. Runs are also capped by `VECTIS_WORKER_ARTIFACT_MAX_RUN_BYTES` / `worker.artifact_max_run_bytes` / `--artifact-max-run-bytes`, default `10737418240` bytes (10 GiB), and `VECTIS_WORKER_ARTIFACT_MAX_COUNT` / `worker.artifact_max_count` / `--artifact-max-count`, default `1000`; set either to `0` to disable that per-run quota.

For job secret resolution, `vectis-secrets` performs its own per-execution authorization inside the secrets RPC. It verifies the caller's mTLS client certificate, derives the expected execution SPIFFE ID from the active execution record, and requires an exact match. Keep `service_identity.secrets_allowed_client_identities` empty unless you intentionally want an additional exact-match allowlist for known caller identities; a static worker service identity allowlist will block dynamic per-execution SVID callers.

Worker execution identity is disabled by default. Enable it when you want workers to derive an expected SPIFFE ID for each accepted execution before action code runs:

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED` / `worker.execution_identity.enabled` | Enables per-execution identity derivation. Jobs without an execution envelope fail closed when this is enabled. |
| `VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN` / `worker.execution_identity.trust_domain` | SPIFFE trust domain used for derived execution IDs, such as `prod.example`. Required when enabled. |
| `VECTIS_WORKER_EXECUTION_IDENTITY_PATH_TEMPLATE` / `worker.execution_identity.path_template` | Path template for derived execution IDs. Defaults to `/cell/{cell}/namespace/{namespace}/job/{job}/run/{run}/execution/{execution}`. |

Supported template placeholders are `{cell}`, `{namespace}`, `{job}`, `{run}`, `{run_index}`, `{segment}`, `{execution}`, `{attempt}`, `{definition_version}`, and `{definition_hash}`. The derived identity is attached to Vectis' in-process action state and tracing. It is not exported to shell process environment variables. By itself, execution identity derivation does not fetch SPIFFE SVIDs; enable `worker.spiffe.enabled` to require the worker's Workload API source to return the exact derived ID before action code runs. Configure the same `worker.execution_identity.*` values on `vectis-secrets`; the broker uses those settings to derive the expected caller identity from the active execution row.

Worker SPIFFE integration is disabled by default. Enable it only when a SPIFFE Workload API is available to the worker process:

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_WORKER_SPIFFE_ENABLED` / `worker.spiffe.enabled` | Enables worker-side SPIFFE Workload API integration and makes every execution acquire a matching X.509-SVID before action code runs. Requires `worker.execution_identity.enabled=true` and an explicit Workload API address. |
| `VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS` / `worker.spiffe.workload_api_address` | SPIFFE Workload API address, such as `unix:///run/vectis/spiffe/workload.sock`. |
| `VECTIS_WORKER_SPIFFE_FETCH_TIMEOUT` / `worker.spiffe.fetch_timeout` | Maximum time to wait for a Workload API SVID fetch during pre-action execution SVID acquisition. Defaults to `5s` and must be positive. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED` / `worker.spiffe.registration.enabled` | Enables worker-controlled SPIFFE Entry API registration before the worker fetches the execution SVID. Requires `worker.spiffe.enabled=true`. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS` / `worker.spiffe.registration.server_address` | SPIFFE Entry API Unix socket address, such as `unix:///run/vectis/spiffe/registration.sock`. The first implementation only supports local `unix:///...` addresses. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID` / `worker.spiffe.registration.parent_id` | Parent SPIFFE ID for execution registration entries, usually the attested worker node or agent identity. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS` / `worker.spiffe.registration.selectors` | Trusted workload selectors in `type:value` form, for example `unix:uid:1000` or `k8s:sa:vectis:worker`. Values may contain additional `:` characters. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_X509_SVID_TTL` / `worker.spiffe.registration.x509_svid_ttl` | Optional X.509-SVID TTL to set on created SPIFFE entries. Defaults to `0s`, which leaves the SPIFFE authority default in effect. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_MIN_TTL` / `worker.spiffe.registration.min_ttl` | Optional minimum accepted lifetime for generated registration intents. Defaults to `0s`. |
| `VECTIS_WORKER_SPIFFE_REGISTRATION_MAX_TTL` / `worker.spiffe.registration.max_ttl` | Optional maximum accepted lifetime for generated registration intents. Defaults to `0s`. |

Execution SVID acquisition happens inside Vectis-controlled worker code. When a task declares secrets, the worker uses the matched X.509-SVID as the gRPC client certificate for the `vectis-secrets` resolution call. Vectis does not export the Workload API socket, SVID, private key, derived SPIFFE ID, SPIFFE Entry API socket, or registration authority credentials to shell commands. When `worker.spiffe.registration.enabled=true`, the worker creates or renews a SPIFFE entry before fetching the execution SVID and releases only entries it created or previously tagged with Vectis' deterministic registration hint. Pre-existing operator-managed entries can satisfy SVID acquisition but are not updated or deleted by Vectis. Workers emit `vectis_worker_spiffe_svid_checks_total` with fixed `outcome` and `reason` labels for the SVID gate.

Secret resolution requires internal gRPC TLS with server certificates, `grpc_tls.ca_file` for workers, and `grpc_tls.client_ca_file` for `vectis-secrets` so workload client certificates are requested and verified. `vectis-secrets` fails startup when gRPC TLS is enabled without `grpc_tls.client_ca_file`.

For local end-to-end testing, `vectis-local` starts an embedded development-only `vectis-spiffe` authority when local gRPC TLS is enabled, exports its trust bundle, passes worker execution identity and SPIFFE registration settings to child processes, starts `vectis-secrets` with encryptedfs enabled, and writes a combined client-CA bundle containing the generated local Vectis CA plus the local SPIFFE bundle. This lets ordinary local service mTLS and dynamic per-execution SVID client certificates work in the same process group without external identity binaries. `vectis-local --grpc-insecure` skips the embedded authority and secrets service because plaintext gRPC cannot authenticate execution SVID client certificates. See [Local SPIFFE Secrets Smoke Test](./deployment/local-spiffe-secrets-smoke-test.md).

`vectis-secrets` routes secret refs by URI scheme. `encryptedfs://...` is the first built-in provider scheme; future external providers such as Vault or Knox can register their own schemes behind the same authorization and delivery path.

Secret access policy is default-deny. Configure one or more allow rules with repeated `--allow-secret`, `VECTIS_SECRETS_POLICY_ALLOW`, or `secrets.policy.allow`. Each rule uses semicolon-separated `key=value` parts:

```text
namespace=/teams/build;job=release;task=publish;ref=encryptedfs://teams/build/npm-token
```

`namespace`, `job`, and `task` default to `*` when omitted; `ref` is required. Values support exact match or a trailing `*` prefix wildcard, so `namespace=/teams/*;ref=encryptedfs://teams/*` allows matching refs only for executions in namespaces below `/teams/`. Every requested secret in a resolution call must match at least one allow rule.

Create encryptedfs envelope files with `vectis-cli secrets encryptedfs put`. The command reads secret plaintext from stdin by default, refuses to overwrite existing envelope files unless `--force` is passed, and only creates a missing key file when `--create-key` is set:

```sh
./bin/vectis-cli secrets encryptedfs put encryptedfs://teams/build/npm-token \
  --from-file npm-token.txt \
  --root /var/lib/vectis/secrets \
  --key-file /etc/vectis/secrets.key \
  --create-key
```

`vectis-secrets` emits structured resolve logs with run, execution, namespace, job, task, provider, outcome, reason, and secret count metadata. It does not log claim tokens, secret plaintext, delivery paths, or requested refs. Prometheus exposes `vectis_secrets_resolve_requests_total` and `vectis_secrets_resolve_duration_seconds` with low-cardinality `outcome`, `reason`, and `provider` labels.

SPIFFE registration expectations:

1. The X.509-SVID SPIFFE ID must exactly match the worker's derived execution identity. With the default template, that shape is `spiffe://<trust-domain>/cell/<cell>/namespace/<namespace>/job/<job>/run/<run>/execution/<execution>`.
2. Namespace `/` renders as `namespace/root`. Non-root namespace paths are trimmed, split on `/`, and URL-escaped segment by segment, so registration tooling should use the same template rules as Vectis.
3. Vectis models each planned SPIFFE entry as a registration intent: exact execution SPIFFE ID, parent SPIFFE ID, trusted selectors, expiry, and safe execution metadata. The intent key is a deterministic `sha256:` value derived from the execution SPIFFE ID, parent ID, and normalized selectors so a registrar can make creation idempotent while renewing expiry independently.
4. Registration selectors should identify the trusted worker workload or worker runtime boundary, such as a service account, systemd unit, UID, binary path, pod identity, or node-attested worker placement. They should not depend on job-controlled files, command arguments, or environment variables.
5. Registration lifecycle should be as narrow as the deployment can support: create or make available the execution identity before the worker starts the task, and expire or remove it after the execution reaches a terminal state. Avoid long-lived registrations that let a worker fetch broad sets of historical or unrelated execution IDs.
6. The built-in registrar uses the SPIFFE Entry API through a local Unix socket. Protect that socket as registration authority and expose it only to the Vectis worker process or a dedicated registrar component.
7. The registration workflow should live outside arbitrary job actions. Do not give shell steps the SPIFFE Workload API socket, the SPIFFE Entry API socket, the SVID private key, or registration authority credentials; secret brokering consumes the worker-owned acquired SVID instead.

## Action Registry

Built-in actions are always available. Local custom actions are enabled by pointing the action registry at one or more manifest roots. The same global action registry settings are read by API validation, the CLI, cron, reconciler, and workers.

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS` / `action_registry.local_roots` | Comma-separated local filesystem roots containing `<namespace>/<name>/action.json` manifests. |
| `VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES` / `action_registry.allowed_namespaces` | Optional comma-separated namespace allowlist for custom actions. Builtins are still allowed. |
| `VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES` / `action_registry.allowed_sources` | Optional comma-separated source allowlist, such as `local_filesystem` or `oci`. |
| `VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS` / `action_registry.require_digest_pins` | When `true`, custom action references must use `@sha256:<digest>` instead of version selectors. |

Use `vectis-cli actions resolve <uses>` to see the descriptor and digest for a friendly action reference. See [Adding Actions](../developing/actions.md) for the local manifest shape and [CLI Guide](../using/cli-guide.md#resolve-an-action) for the discovery workflow.

For security removals, do not only delete the manifest. Leave a descriptor tombstone with the original digest and `status = "revoked"` or `status = "purged"`. Default validation and execution policy blocks those statuses, while `vectis-cli actions list --ignore-policy` and `vectis-cli actions resolve <uses> --ignore-policy` let operators inspect the tombstone reason.

API sessions and rate-limit buckets use the API cache backend. The default is `api.cache.backend = "database"`, which stores shared state in the configured SQL database so multiple API replicas see the same sessions and enforce one rate-limit budget. Set `api.cache.backend = "memory"` or `VECTIS_API_CACHE_BACKEND=memory` only when per-process sessions and limits are acceptable; memory mode cleans expired entries but still logs a warning when API auth is enabled because sessions and limits are not shared across replicas. Login sessions have an absolute expiry from `api.session.ttl` / `VECTIS_API_SESSION_TTL` and an idle expiry from `api.session.idle_ttl` / `VECTIS_API_SESSION_IDLE_TTL`; the defaults are `168h` and `24h`. Browser session cookies use only host-only `Secure` `__Host-vectis_session` and `__Host-vectis_csrf` names with `Path=/`; the session cookie is HttpOnly, both cookies are SameSite=Lax, and Vectis does not issue or accept unprefixed fallback names. Browser cookie auth requires HTTPS or local TLS. When API auth is enabled behind an HTTPS ingress, edge proxy, or load balancer, set `api.session.cookie_secure = true` / `VECTIS_API_SESSION_COOKIE_SECURE=true` explicitly as the deployment's browser-facing HTTPS assertion; trusted proxy CIDRs still let Vectis trust `X-Forwarded-Proto` / `Forwarded: proto=https` for request-aware behavior, but they do not satisfy startup secure-cookie validation. Use `api.session.allow_insecure_cookies = true` / `VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES=true` only for local HTTP workflows that use bearer API tokens or login `return_token`; it does not make Vectis issue insecure browser cookies. Duplicate or malformed `Origin`, CORS preflight, `Sec-Fetch-*`, `X-Forwarded-*`, `X-Real-IP`, and `Forwarded` headers are rejected before browser request checks. Browser Fetch Metadata must describe API-style requests, not document navigations or subresource loads. Browser-marked cross-site requests without `Origin` are rejected before route handling; cross-site requests with `Origin` must pass CORS. Cookie-authenticated requests with `Sec-Fetch-Site: cross-site` are rejected, including safe reads; unsafe cookie-authenticated requests also require `X-CSRF-Token` and an `Origin` or `Referer` matching the browser-facing scheme, host, and port.

The API can serve browser-facing HTTPS directly with `--tls-cert-file` and `--tls-key-file`, or with `VECTIS_API_TLS_CERT_FILE` / `VECTIS_API_TLS_KEY_FILE`. `VECTIS_API_TLS_RELOAD_INTERVAL` enables polling reloads for rotated files. This is separate from internal gRPC TLS and metrics TLS.

`vectis-docs` accepts the same shape through `--tls-cert-file`, `--tls-key-file`, and `--tls-reload-interval`, or `VECTIS_DOCS_TLS_CERT_FILE`, `VECTIS_DOCS_TLS_KEY_FILE`, and `VECTIS_DOCS_TLS_RELOAD_INTERVAL`.

API and docs responses set browser hardening headers by default, including `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, `Cross-Origin-Opener-Policy`, `Cross-Origin-Resource-Policy`, `Cross-Origin-Embedder-Policy`, `Origin-Agent-Cluster`, `X-Permitted-Cross-Domain-Policies`, `X-Download-Options`, and Content Security Policy. `Strict-Transport-Security` is sent on direct HTTPS API/docs/metrics/cell-ingress requests; API responses behind a TLS-terminating proxy also send it when trusted proxy CIDRs allow Vectis to trust the forwarded original scheme. API HSTS defaults to `max-age=31536000`; set `api.hsts.max_age_seconds` / `VECTIS_API_HSTS_MAX_AGE_SECONDS`, `api.hsts.include_subdomains` / `VECTIS_API_HSTS_INCLUDE_SUBDOMAINS`, and `api.hsts.preload` / `VECTIS_API_HSTS_PRELOAD` to tune it. `preload=true` is rejected unless `include_subdomains=true` and `max_age_seconds >= 31536000`, because preload policies are intentionally sticky in browsers. The docs CSP allows same-origin script/style assets but does not allow inline scripts, inline styles, or inline event handlers. Protected API routes default to `Cache-Control: no-store` unless a streaming handler explicitly manages cache headers. Setup and login routes also explicitly set `no-store`, including validation and rate-limit failures before handler logic. The docs server rejects untrusted Host headers and non-`GET`/`HEAD` methods before static file handling. Docs static file serving disables directory listings, hides dotfile paths, and rejects symlinks in a configured local docs directory when they resolve outside the docs root.

Successful logout clears the Vectis session cookies and sends `Clear-Site-Data: "cache", "storage"` to remove browser-local origin data. Vectis does not use the `cookies` directive for that header because it can clear cookies for the same domain and subdomains; logout clears only the Vectis auth cookies explicitly.

API routes reject request bodies unless the route inventory explicitly declares a JSON body policy. Declared JSON body routes enforce `application/json` media types and per-route size caps before parsing, including smaller caps for auth/user/token/control routes and a larger cap for job definitions.

API routes also reject query parameters unless the route inventory explicitly declares the key. Query strings must parse cleanly, and repeated keys are rejected before route handlers run so handlers never silently ignore typos or conflicting values.

API routes also enforce response `Accept` negotiation. JSON routes accept absent `Accept`, `application/json`, `application/*`, `*/*`, or weighted lists that include JSON. SSE routes accept `text/event-stream` or compatible wildcards. API metrics accept Prometheus text or OpenMetrics response types. Health probe OK responses carry no representation and allow any `Accept` value. Incompatible `Accept` values return `not_acceptable` with status `406` before route handlers run.

API route matching returns JSON API errors for invalid request targets, unknown routes, and method mismatches. Requests must use origin-form, unescaped, canonical API paths; absolute-form proxy request targets, `OPTIONS *`, percent-encoded path text, duplicate slash paths, dot segments, and trailing slash aliases are rejected before route handlers run. Method mismatches include an `Allow` header, TRACE/TRACK/CONNECT are rejected, and method override headers such as `X-HTTP-Method`, `X-HTTP-Method-Override`, and `X-Method-Override` return `method_override_forbidden` before route handlers run.

Docs, cell ingress, and dedicated metrics listeners also reject unsafe request targets before handler logic runs, including absolute-form proxy request targets, `OPTIONS *`, percent-encoded path text, duplicate slash paths, and dot segments. They also reject untrusted Host headers and method override headers before method dispatch. Read-only docs, metrics, and cell-ingress health routes reject request bodies. Cell ingress route matching is similarly narrow: health endpoints allow `GET`/`HEAD`, execution submission allows `POST`, and other paths or methods return JSON errors before handler logic runs. Cell ingress execution submission requires mTLS for non-loopback ingress, requires `application/json`, caps request bodies at 2 MiB, and enforces JSON-compatible `Accept` values. Cell ingress responses use the shared baseline security headers and `Cache-Control: no-store`.

HTTP servers for the API, docs, cell ingress, and metrics endpoints cap request headers at 32 KiB. Keep reverse proxy and ingress header limits at or below that size so oversized requests are rejected before reaching Vectis.

Per-service metrics servers bind to `localhost` by default and only serve `GET`/`HEAD /metrics` with a trusted Host header and no request body; other paths, methods, or Host headers are rejected before the Prometheus handler runs. Dedicated metrics listeners enforce Prometheus text or OpenMetrics-compatible `Accept` values. Set the relevant service's `--metrics-host` flag or `VECTIS_<SERVICE>_METRICS_HOST` only when a trusted scraper needs off-host access, and pair that with `VECTIS_METRICS_ALLOWED_HOSTS` or `VECTIS_<SERVICE>_METRICS_ALLOWED_HOSTS`. Metrics responses use baseline security headers and `Cache-Control: no-store` because metrics can disclose operational state.

API rate limits have embedded defaults for auth, token, and general routes. The shipped limit keys live under `api.rate_limit.*`. The defaults are intended to protect the built-in auth surface from accidental or hostile bursts; tune them only when you understand the expected traffic shape.

When the API runs behind a trusted reverse proxy, configure forwarded client IP and original scheme handling separately. Vectis expects the proxy that connects directly to the API to overwrite untrusted forwarding headers into one sanitized shape; duplicate or ambiguous forwarded headers are rejected before route handling. See [Trusted Proxy Headers](./deployment/trusted-proxy-client-ip.md).

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

Standalone binaries default to plaintext gRPC. `vectis-local` normally bootstraps a local development CA and sets `VECTIS_GRPC_TLS_*` for child processes unless you pass `--grpc-insecure`; plaintext local mode also skips `vectis-secrets`. The same generated server certificate can also be used for local API/docs HTTPS. Run `vectis-local init` as your normal user to create or renew the files, then run `vectis-local install-cert` with elevated privileges if your OS requires that to trust the generated CA. The `install-cert` command only installs the CA certificate; it does not create files, migrate databases, or start services. In normal runs, `--http-tls=auto` uses HTTPS when the generated certificate verifies against the system trust store, `--http-tls=on` forces HTTPS with the generated cert, and `--http-tls=off` keeps API/docs on HTTP. The Podman reference deployment also generates internal gRPC TLS material and mounts it into the Vectis containers.

| Role | Binaries | Required material when TLS is enabled |
| --- | --- | --- |
| gRPC listeners | `vectis-registry`, `vectis-queue`, `vectis-log`, `vectis-artifact`, `vectis-orchestrator`, `vectis-secrets`, worker-control listener in `vectis-worker` | Certificate and key. Queue/log/artifact/orchestrator also need a CA when they register with the registry; secrets also needs a client CA to verify execution SVID callers. |
| gRPC clients | `vectis-api`, `vectis-cell-ingress`, `vectis-worker`, `vectis-cron`, `vectis-reconciler`, queue/log/artifact/orchestrator registration clients | CA bundle. Client cert/key only when servers require mTLS. |

For trust boundaries and what mTLS does or does not authorize today, see [Internal Service Trust](../concepts/internal-service-trust.md).

## Metrics TLS

`VECTIS_METRICS_TLS_*` settings apply to dedicated metrics listeners, not the API's main HTTP listener. API metrics are served on the same HTTP listener as the REST API and require API admin auth when API auth is enabled.

| Variable | Purpose |
| --- | --- |
| `VECTIS_METRICS_TLS_INSECURE` | `true` means plaintext metrics HTTP. `false` enables HTTPS and requires cert/key files. |
| `VECTIS_METRICS_TLS_CERT_FILE` / `VECTIS_METRICS_TLS_KEY_FILE` | Server certificate and key for metrics listeners. |
| `VECTIS_METRICS_TLS_RELOAD_INTERVAL` | Positive duration to poll PEM files and reload them without restart. `0` disables polling. |

The dedicated metrics listeners are queue, orchestrator, worker, log, artifact, log-forwarder, secrets, reconciler, catalog, and cell ingress. They bind to `localhost` by default; set each service's `--metrics-host` / `VECTIS_<SERVICE>_METRICS_HOST` only for trusted scrape networks, and configure `VECTIS_METRICS_ALLOWED_HOSTS` or `VECTIS_<SERVICE>_METRICS_ALLOWED_HOSTS` for the expected scraper Host header. Keep dedicated metrics endpoints private; they are not authenticated. See [Security](../concepts/security.md).

## Discovery And Fixed Addresses {#service-discovery-vs-fixed-addresses}

Vectis can either discover services through `vectis-registry` or use fixed addresses.

| Pattern | Use when |
| --- | --- |
| Registry discovery | You want queue, orchestrator, log, artifact, and worker-control addresses published and resolved dynamically. |
| Fixed addresses | You want fewer startup dependencies and already know the queue/orchestrator/log/artifact addresses. |

Role-specific settings override shared discovery settings when both are set.

Global producers can route execution cells to private ingress endpoints with repeated `vectis-api --cell-ingress-endpoint cell_id=url`, `VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS=iad-a=https://iad.example:8085,pdx-b=https://pdx.example:8085`, or shared `VECTIS_CELL_INGRESS_ENDPOINTS`. `vectis-reconciler` and `vectis-cron` use the same shared endpoint map unless their role-specific endpoint variables are set. Non-loopback ingress endpoints must use `https://` and the shared internal mTLS settings. Cell ingress Host validation also reads the shared static endpoint map for the local cell, so prefer keeping the common map available to the matching `vectis-cell-ingress` process. When the local cell has an ingress endpoint configured, producers send local executions through ingress instead of writing directly to the local queue. If `VECTIS_GLOBAL_DATABASE_DSN` and `VECTIS_CELL_DATABASE_DSN` point at different databases, configure an ingress endpoint for every execution target, including the local cell; direct local queue fallback is disabled.

For local routing tests, `vectis-local --cell pdx-b` starts an additional queue, cell ingress, and worker for `pdx-b`, pins those cell-local processes to their queue, and publishes all local ingress endpoints through `VECTIS_CELL_INGRESS_ENDPOINTS`. By default those endpoints are `https://` and use the generated local mTLS material; `vectis-local --grpc-insecure` switches them to loopback `http://`.

| What you are configuring | Shared setting segment | Role-specific examples |
| --- | --- | --- |
| Registry address | `DISCOVERY_REGISTRY_ADDRESS` | `API_REGISTRY_ADDRESS`, `WORKER_REGISTRY_ADDRESS`, `CRON_REGISTRY_ADDRESS`, `RECONCILER_REGISTRY_ADDRESS` |
| Queue address | `DISCOVERY_QUEUE_ADDRESS` or `DISCOVERY_QUEUE_RESOLVER_ADDRESS` | `API_QUEUE_ADDRESS`, `CELL_INGRESS_QUEUE_ADDRESS`, `WORKER_QUEUE_ADDRESS`, `CRON_QUEUE_ADDRESS`, `RECONCILER_QUEUE_ADDRESS` |
| Log gRPC address | `DISCOVERY_LOG_ADDRESS` or `DISCOVERY_LOG_GRPC_RESOLVER_ADDRESS` | `WORKER_LOG_ADDRESS` |
| Artifact gRPC address | `DISCOVERY_ARTIFACT_ADDRESS` or `DISCOVERY_ARTIFACT_GRPC_RESOLVER_ADDRESS` | `ARTIFACT_GRPC_RESOLVER_ADDRESS` |
| Orchestrator address | `DISCOVERY_ORCHESTRATOR_ADDRESS` | `WORKER_ORCHESTRATOR_ADDRESS` |
| Queue/log/artifact/orchestrator advertise address | `DISCOVERY_QUEUE_ADVERTISE_ADDRESS`, `DISCOVERY_LOG_GRPC_ADVERTISE_ADDRESS`, `DISCOVERY_ARTIFACT_GRPC_ADVERTISE_ADDRESS` | `VECTIS_QUEUE_ADVERTISE_ADDRESS`, `VECTIS_LOG_GRPC_ADVERTISE_ADDRESS`, `VECTIS_ARTIFACT_GRPC_ADVERTISE_ADDRESS`, `VECTIS_ORCHESTRATOR_ADVERTISE_ADDRESS` |

Replace the prefix with the service prefix. For example:

```sh
VECTIS_WORKER_DISCOVERY_REGISTRY_ADDRESS=localhost:8082
VECTIS_WORKER_QUEUE_ADDRESS=localhost:8081
VECTIS_WORKER_ORCHESTRATOR_ADDRESS=localhost:8087
```

For a multi-registry cell, set the unscoped registry list on every service that uses discovery:

```sh
VECTIS_DISCOVERY_REGISTRY_ADDRESSES=reg-a:8082,reg-b:8082,reg-c:8082
```

Registration toggles:

| Variable | Purpose |
| --- | --- |
| `VECTIS_QUEUE_REGISTER_WITH_REGISTRY` | Queue publishes its address to registry when enabled. |
| `VECTIS_LOG_GRPC_REGISTER_WITH_REGISTRY` | Log service publishes its gRPC address to registry when enabled. |
| `VECTIS_ARTIFACT_GRPC_REGISTER_WITH_REGISTRY` | Artifact service publishes its gRPC address to registry when enabled. |
| `VECTIS_ORCHESTRATOR_REGISTER_WITH_REGISTRY` | Orchestrator publishes its address to registry when enabled. |
| `VECTIS_WORKER_REGISTER_WITH_REGISTRY` | Worker publishes its worker-control address to registry when enabled. |

Envelope-backed worker deliveries complete through the orchestrator task boundary, activate child task executions, and enqueue task continuations from the worker event path. Worker deliveries must include a `run_id` and execution envelope; missing or invalid run/envelope metadata is treated as malformed work rather than falling back to whole-run execution.

Registry address settings may contain multiple comma-separated or space-separated registry addresses. Discovery clients fail over between configured targets. Registering services prefer a stable sponsor from that address set and fail over to another target on errors; they do not write every heartbeat to every registry node. For multi-node registry HA, the registry nodes still need deliberate static cluster membership and gossip configuration; otherwise they are independent registries with failover from the client's point of view but no converged shared state.

When registry discovery is used, multiple `vectis-queue` instances may register as a pool. Each queue needs one stable `VECTIS_QUEUE_INSTANCE_ID` / `--instance-id`; if it is omitted, `vectis-queue` derives a stable ID from the system hostname and queue port. Producers choose among discovered queue shards; workers ack back to the shard encoded in the delivery ID.

`VECTIS_QUEUE_POOL` / `--pool` names the local queue pool used when deriving the default persistence path. If `VECTIS_QUEUE_PERSISTENCE_DIR` / `--persistence-dir` is omitted, the queue uses `$XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>`. Set a persistence directory explicitly only when you want to pin storage layout. An explicitly empty persistence directory disables queue persistence.

Queue instance IDs must be unique among active queue processes registered in the same registry, except during a controlled replacement of the same shard with the same persistence directory. If two active queues register the same instance ID, the registry treats them as the same logical shard and the later registration wins; workers may route acks to the wrong process. If two active queues point at the same persistence directory, the second queue refuses to start.

When registry discovery is used, multiple `vectis-log` instances may register as run shards. Each log shard needs one stable `VECTIS_LOG_INSTANCE_ID` / `--instance-id`; if it is omitted, `vectis-log` derives a stable ID from the system hostname and log gRPC port. DB-aware clients record the chosen shard in `job_runs.log_shard_id` and route future reads/writes for that run back to the assigned shard. When a worker sends logs through a local `vectis-log-forwarder`, it stamps the assigned shard into the socket/spool protocol so the DB-free forwarder preserves the same route. Unassigned runs fall back to deterministic `run_id` hashing. Keep instance IDs and storage directories stable across restarts. If two active log processes point at the same storage directory, the second log process refuses to start.

`VECTIS_LOG_STORAGE_DIR` / `--storage-dir` stores durable run log files. If omitted, the log service uses `$XDG_DATA_HOME/vectis/log/<instance-id>`. `VECTIS_LOG_STORAGE_READ_ONLY_MIN_FREE_BYTES` / `--storage-read-only-min-free-bytes` defaults to `1073741824` (1 GiB). Below that threshold, the shard advertises `read_only` for new runs and refuses the first append for a run that does not already have a log file; stored logs remain readable, and existing assigned run files can continue to receive appends. Set the value to `0` to disable the threshold.

When registry discovery is used, multiple `vectis-artifact` instances may register as independent content-addressed blob shards. Each artifact shard needs one stable `VECTIS_ARTIFACT_INSTANCE_ID` / `--instance-id`; if it is omitted, `vectis-artifact` derives a stable ID from the system hostname and artifact gRPC port. Keep instance IDs and storage directories stable across restarts. If two active artifact processes point at the same storage directory, the second artifact process refuses to start.

`VECTIS_ARTIFACT_STORAGE_DIR` / `--storage-dir` stores durable content-addressed blobs. If omitted, the artifact service uses `$XDG_DATA_HOME/vectis/artifact/<instance-id>`. `VECTIS_ARTIFACT_STORAGE_READ_ONLY_MIN_FREE_BYTES` / `--storage-read-only-min-free-bytes` defaults to `1073741824` (1 GiB). Below that threshold, the shard advertises `read_only` for new blobs and rejects uploads for blobs it does not already have; existing stored blobs remain readable. Set the value to `0` to disable the threshold. Worker-originated upload and run quotas are controlled separately with `VECTIS_WORKER_ARTIFACT_MAX_*`.

Artifact service metrics include local CAS pressure gauges: `vectis_artifact_storage_blobs`, `vectis_artifact_storage_bytes`, `vectis_artifact_storage_free_bytes`, `vectis_artifact_storage_free_inodes`, and `vectis_artifact_storage_new_blob_writable`.

Discovery timing defaults include resolver refresh `10s`, poll timeout `5s`, error refresh `2s`, and registration heartbeat `45s`.

For failure behavior with and without registry, see [Failure Domains](../concepts/failure-domains.md#registry-down).

## Worker Execution Backend

`vectis-worker` owns queue claims, leases, cancellation, finalization, logs, artifacts, and policy gates. By default it delegates action execution to `vectis-worker-core` over a Unix domain socket. `vectis-worker-core` defaults to the `host` execution backend. In that mode, built-in actions execute as child processes on the worker-core host inside the per-run workspace. This is compatible with existing deployments, but it is not a security sandbox.

Jobs can declare `default_isolation: "host"` or `default_isolation: "vm"` as the default for their action tree. Individual nodes can declare `isolation: "host"` or `isolation: "vm"` to override that default. Nodes that omit `isolation` inherit the nearest parent `builtins/sequence` isolation, then the job default, then the worker backend default. A worker must have a matching provider for the effective isolation level; Vectis does not silently fall back from `vm` to `host`.

The first VM-oriented backend is `lima`, intended for macOS worker isolation experiments with a prepared [Lima](https://lima-vm.io/) instance:

```sh
vectis-worker-core \
  --execution-backend lima \
  --workspace-root /Users/ci/vectis-workspaces \
  --lima-instance vectis-worker \
  --lima-start
```

Equivalent environment variables:

| Variable / key | Purpose |
| --- | --- |
| `VECTIS_WORKER_CORE_EXECUTION_BACKEND` | `host` by default, or `lima` to run action commands through `limactl shell`. |
| `VECTIS_WORKER_CORE_WORKSPACE_ROOT` | Parent directory for automatically-created run workspaces. For Lima, set this to a path that exists and is writable inside the guest. Empty uses the host OS temp directory. |
| `VECTIS_WORKER_CORE_LIMA_INSTANCE` | Required Lima instance name for the `lima` backend. |
| `VECTIS_WORKER_CORE_LIMA_PATH` | Path to `limactl`; defaults to `limactl` from `PATH`. |
| `VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT` | Optional guest-side parent directory for Lima workspaces, such as `/tmp/vectis-workspaces`. When set, commands for the same run use the same guest workspace even if the host workspace mount is read-only. |
| `VECTIS_WORKER_CORE_LIMA_START` | Passes `--start` to `limactl shell` before each command. It starts an existing instance; it does not create or configure one. |
| `VECTIS_WORKER_CORE_LIMA_PRESERVE_ENV` | Passes `--preserve-env` to `limactl shell`. Off by default to avoid leaking host environment variables into the guest. |

The Lima backend sets the worker-core inherited action isolation to `vm` and registers Lima as the VM command provider. A job can request `default_isolation: "host"` or a node can request `isolation: "host"` explicitly for host-side actions on that core.

Workers describe their configured core and publish execution metadata for routing and operator inspection: `worker.execution.backend`, `worker.execution.default_isolation`, and comma-separated `worker.execution.supported_isolation`. Host cores advertise `host` with supported isolation `host`; Lima cores advertise backend `lima`, default isolation `vm`, and supported isolation `host,vm`. Workers also send the same supported-isolation list when polling the queue, so a host-only worker skips queued VM work instead of dequeuing and failing it.

The Lima backend does not silently fall back to host execution. Startup fails if `backend=lima` is selected without an instance name. Command execution fails if the Lima instance is unavailable. A host-default core fails any node that requests `isolation: "vm"` because no VM provider is registered. If `VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT` is empty, the run workspace path must be visible and writable inside the guest; configure `VECTIS_WORKER_CORE_WORKSPACE_ROOT` and Lima mounts accordingly. If `VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT` is set, Vectis maps each run workspace to a same-named guest directory under that root and creates it before each command.

To smoke test the VM provider against a prepared instance from a development checkout:

```sh
VECTIS_TEST_LIMA_INSTANCE=vectis-worker make test-lima
```

By default, that test creates a temporary workspace under the development checkout, so the checkout path must also be writable inside the Lima guest. Set `VECTIS_TEST_LIMA_WORKSPACE_ROOT=/path/mounted/in/guest` to test another host-mounted workspace root, or `VECTIS_TEST_LIMA_GUEST_WORKSPACE_ROOT=/tmp/vectis-workspaces` to test the guest-owned workspace mode. Set `VECTIS_TEST_LIMA_START=1` when the prepared instance may be stopped and the test should pass `--start` to `limactl shell`.

This backend is a first command-execution boundary, not the full profile-aware isolation system described in [ADR 0009](../developing/architecture-decisions/0009-worker-execution-containment-providers.md). Worker placement, richer policy checks, container backends, disposable VM lifecycle, and provider-specific security posture docs remain follow-up work.

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

`vectis-cli local reset --dry-run` shows which local Vectis config, data, cache, token, TLS, deployment-secret, and configured local durable paths would be removed. `vectis-cli local reset --yes` removes those local paths, including non-default `VECTIS_QUEUE_PERSISTENCE_DIR`, `VECTIS_LOG_STORAGE_DIR`, `VECTIS_LOG_FORWARDER_SPOOL_DIR`, and `VECTIS_ARTIFACT_STORAGE_DIR` values when they are set; it does not stop running services or remove container volumes.

## Default Ports

| Surface | Default port |
| --- | --- |
| API HTTP and API `/metrics` | `8080` |
| Queue gRPC | `8081` |
| Registry gRPC | `8082` |
| Log gRPC | `8083` |
| Log HTTP/SSE | `8084` |
| Artifact gRPC | `8086` |
| Orchestrator gRPC | `8087` |
| Docs HTTP | `8088` |
| Queue metrics | `9081` |
| Worker metrics | `9082` |
| Log metrics | `9083` |
| Worker-control gRPC | `9084` in static mode |
| Reconciler metrics | `9085` |
| Catalog metrics | `9086` |
| Cell ingress metrics | `9087` |
| Log-forwarder metrics | `9088` |
| Artifact metrics | `9089` |
| Orchestrator metrics | `9090` |

Each extra `vectis-local --cell` uses the default cell-local ports plus `100` per additional cell. For example, the first extra cell uses queue `8181`, cell ingress `8185`, queue metrics `9181`, worker metrics `9182`, and cell ingress metrics `9187`. Multi-cell local workers use ephemeral worker-control ports.

## Reference Deployment Notes

`vectis-cli deploy podman up` generates local deployment secrets and TLS material under the deployment config directory. Set `VECTIS_DEPLOY_CONFIG_DIR` to choose where rendered manifests and local deployment secrets are stored.

The Podman reference deployment:

- enables internal gRPC TLS for Vectis containers;
- enables TLS from Vectis containers to the bundled Postgres instance;
- enables HTTPS for queue, worker, and log metrics scrapes;
- starts `vectis-spiffe` and wires workers plus `vectis-secrets` for per-execution SVID-authenticated encryptedfs secret resolution;
- runs Prometheus, Grafana, Jaeger, OpenSearch, and Fluent Bit as a reference observability stack.

Treat the reference deployment as a helpful starting point, not a production security boundary by itself. Rotate generated secrets into your platform's secret store for shared environments. See [Reference Deployment Posture](./deployment/reference-deployment-posture.md).

## Related Documentation

| Topic | Document |
| --- | --- |
| Components and flows | [Architecture](../concepts/architecture.md) |
| Production config contract | [Production Config And Secrets Contract](./deployment/production-config-contract.md) |
| Security posture | [Security](../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../concepts/internal-service-trust.md) |
| Failure behavior and probes | [Failure Domains](../concepts/failure-domains.md) |
| Log streaming behavior | [Log Streaming](../using/log-streaming.md) |
| Runbooks and alerts | [Runbooks](./reliability/runbooks.md) |
| Repair recipes | [Repair Runbooks](./reliability/repair-runbooks.md) |
| Dispatch handoff triage | [Dispatch Visibility](./reliability/dispatch-visibility.md) |
| Backup and restore | [Backup And Restore](./reliability/backup-restore.md) |
| Trusted proxy headers | [Trusted Proxy Headers](./deployment/trusted-proxy-client-ip.md) |
| Releases and upgrades | [Releases And Upgrades](../developing/releases.md) |
