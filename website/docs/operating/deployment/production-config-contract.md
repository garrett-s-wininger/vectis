# Production Config And Secrets Contract

This page defines the minimum configuration and secret material a
[Production Topology v1](./production-topology-v1.md) deployment must manage.
It is a contract for operators and config-management systems, not a complete
list of every Vectis setting.

Use this page when writing `/etc/vectis/*.env`, Kubernetes Secrets, systemd
drop-ins, Nomad variables, Ansible roles, or any other production configuration
source. For a concrete systemd environment-file starting point, see the
[Production Environment Template](./production-env-template.md). For the
operator-facing hardening checklist, see
[Production Security Checklist](./production-security-checklist.md). For the
complete embedded defaults catalog, see
[Configuration Key Reference](../reference/configuration-key-reference.md).

## Ownership Model

Vectis binaries read configuration from embedded defaults, environment
variables, and flags. Production operators own the live values.

| Owner | Responsibility |
| --- | --- |
| Vectis | Defaults, validation, service-specific env names, health checks, migration command, and docs for supported settings. |
| Config management | Writes live env files, secret mounts, service placement, allowed hosts, DSNs, TLS paths, storage paths, and instance IDs. |
| Secret manager | Stores bootstrap token, API tokens, database credentials, TLS private keys, encryptedfs keys, Knox auth tokens, SPIFFE CA material, and recovery credentials. |
| Platform | Keeps internal endpoints private, provides durable volumes, rotates certificates/secrets, monitors hosts and PostgreSQL, and runs backups. |

Do not rely on checked-in `env/*.example` files as production config. They are
examples and may include local/demo defaults.

## Required Secret Inventory

| Secret | Used by | Handling rule |
| --- | --- | --- |
| PostgreSQL password or DSN | Every DB-backed service and `vectis-cli database migrate` | Store in a secret manager; rotate through the database platform and restart DB-backed services with the new DSN. |
| API bootstrap token | `vectis-api` during initial setup | Required only until setup is complete. Remove or rotate after setup where practical. |
| Operator API tokens or credentials | Operators, automation, CI callers | Store outside repo/config templates. Revoke and recreate after exposure. |
| API/browser TLS private key, if API serves HTTPS directly | `vectis-api` | Store as managed TLS material; rotate through the edge or service reload path. |
| Docs TLS private key, if docs serves HTTPS directly | `vectis-docs` | Same handling as API TLS; docs should normally be operator-only. |
| Internal gRPC CA and private keys | gRPC listeners and clients | Required when `VECTIS_GRPC_TLS_INSECURE=false`; protect as internal service trust material. |
| Metrics TLS private keys | Dedicated metrics listeners | Required when `VECTIS_METRICS_TLS_INSECURE=false`; keep metrics endpoints private even with TLS. |
| SPIFFE CA material | `vectis-spiffe` or external SPIFFE authority | Treat as identity authority material. Back up and rotate through a deliberate authority process. |
| SPIFFE Workload API / Entry API authority sockets | Workers or dedicated registrar | Expose only to worker-controlled code or registrar components, never arbitrary job actions. |
| encryptedfs key file | `vectis-secrets` and operator secret-loading flow | Store separately from encrypted envelopes where possible. Back up with the same sensitivity as plaintext secrets. |
| encryptedfs secret envelopes | `vectis-secrets` | Store on durable private storage; include in backup and retention policy. |
| Knox auth token file | `vectis-secrets` when the Knox provider is enabled | Store as deploy secret material. Rotate through Knox and restart or reload the broker process with the new token. |
| Log-forwarder spool contents | `vectis-log-forwarder` | May contain job output. Protect and back up according to log policy. |

## Required Common Config

Every production service should get a common baseline:

```sh
VECTIS_CELL_ID=local
VECTIS_LOG_FORMAT=json
VECTIS_LOG_DIR=/var/log/vectis/components
VECTIS_DATABASE_DRIVER=pgx
VECTIS_DATABASE_DSN=postgres://vectis:<secret>@postgres.internal:5432/vectis?sslmode=require
```

For PostgreSQL, size pool limits per process and add them across replicas:

```sh
VECTIS_DATABASE_PGX_MAX_OPEN_CONNS=25
VECTIS_DATABASE_PGX_MAX_IDLE_CONNS=10
VECTIS_DATABASE_PGX_CONN_MAX_LIFETIME=1h
VECTIS_DATABASE_PGX_CONN_MAX_IDLE_TIME=15m
```

For split global/cell databases, use role-specific DSNs instead of a single
shared DSN:

```sh
VECTIS_GLOBAL_DATABASE_DSN=postgres://vectis-global:<secret>@postgres.internal:5432/vectis_global?sslmode=require
VECTIS_CELL_DATABASE_DSN=postgres://vectis-cell:<secret>@postgres.internal:5432/vectis_cell_local?sslmode=require
VECTIS_CATALOG_CELL_DATABASE_DSNS=local=postgres://vectis-cell:<secret>@postgres.internal:5432/vectis_cell_local?sslmode=require
```

## API Edge Contract

Before exposing `vectis-api` beyond an operator-only loopback environment:

```sh
VECTIS_API_AUTH_ENABLED=true
VECTIS_API_AUTH_BOOTSTRAP_TOKEN=<secret>
VECTIS_API_AUTHZ_ENGINE=hierarchical_rbac
VECTIS_API_ALLOWED_HOSTS=ci.example.com
VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS=10.0.0.0/24
VECTIS_API_SESSION_COOKIE_SECURE=true
```

Required edge behavior:

| Requirement | Why it matters |
| --- | --- |
| HTTPS browser-facing origin | Browser session cookies are `Secure`; direct HTTP browser login is not a production posture. |
| Allowed Host list matches public DNS | Prevents Host-header confusion and DNS-rebinding paths. |
| Trusted proxy CIDRs only include the direct proxy tier | Rate limits, audit logs, client IPs, HSTS, CORS, and CSRF depend on trusted forwarded metadata. |
| Proxy overwrites forwarded headers | Vectis rejects duplicate or malformed forwarded headers; the edge should sanitize before forwarding. |
| Direct listener access is blocked | Clients should not bypass TLS, auth controls, rate limits, or proxy header policy. |
| API and edge header limits are at or below 32 KiB | Oversized headers should be rejected before they consume handler resources. |

If API auth is enabled and the database is new, complete setup, create durable
operator credentials, and remove or rotate the bootstrap token where practical.

## Internal Service Contract

Internal Vectis endpoints are private infrastructure. In production v1:

- keep queue, registry, orchestrator, log, artifact, secrets, cell ingress,
  worker-control, metrics, and database ports off public networks;
- use fixed addresses or registry discovery deliberately;
- enable internal TLS or mTLS wherever traffic crosses host or network
  boundaries;
- configure exact SPIFFE URI SAN allowlists where the deployment can issue
  stable service identities.

Internal gRPC TLS baseline:

```sh
VECTIS_GRPC_TLS_INSECURE=false
VECTIS_GRPC_TLS_CA_FILE=/etc/vectis/tls/grpc-ca.pem
VECTIS_GRPC_TLS_CERT_FILE=/etc/vectis/tls/server.pem
VECTIS_GRPC_TLS_KEY_FILE=/etc/vectis/tls/server.key
VECTIS_GRPC_TLS_CLIENT_CA_FILE=/etc/vectis/tls/client-ca.pem
VECTIS_GRPC_TLS_CLIENT_CERT_FILE=/etc/vectis/tls/client.pem
VECTIS_GRPC_TLS_CLIENT_KEY_FILE=/etc/vectis/tls/client.key
```

When service identity allowlists are set, each value must be an exact
`spiffe://` URI SAN and the listener must verify client certificates:

```sh
VECTIS_SERVICE_IDENTITY_QUEUE_ALLOWED_CLIENT_IDENTITIES=spiffe://prod.example/vectis/api,spiffe://prod.example/vectis/worker
VECTIS_SERVICE_IDENTITY_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES=spiffe://prod.example/vectis/worker
VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES=spiffe://prod.example/vectis/api,spiffe://prod.example/vectis/cron,spiffe://prod.example/vectis/reconciler
```

Keep `VECTIS_SERVICE_IDENTITY_SECRETS_ALLOWED_CLIENT_IDENTITIES` empty unless
you intentionally want an additional static allowlist. Dynamic per-execution
SVID callers can be blocked by a static worker service identity allowlist; the
secrets broker already verifies the execution SVID against active execution
state.

## Discovery And Stable Identity

Choose one discovery posture per deployment:

| Posture | Required config |
| --- | --- |
| Registry discovery | `VECTIS_DISCOVERY_REGISTRY_ADDRESS` or `VECTIS_DISCOVERY_REGISTRY_ADDRESSES`, plus service registration flags and advertise addresses where needed. |
| Fixed addresses | Role-specific queue, orchestrator, log, and artifact addresses such as `VECTIS_WORKER_QUEUE_ADDRESS` and `VECTIS_WORKER_ORCHESTRATOR_ADDRESS`. |

Every stateful shard needs a stable instance ID. Queue, log, and local artifact
storage also need durable, exclusive storage paths:

```sh
VECTIS_QUEUE_POOL=default
VECTIS_QUEUE_INSTANCE_ID=queue-1
VECTIS_QUEUE_PERSISTENCE_DIR=/var/lib/vectis/queue/default/queue-1

VECTIS_LOG_INSTANCE_ID=log-1
VECTIS_LOG_STORAGE_DIR=/var/lib/vectis/log/log-1

VECTIS_ARTIFACT_INSTANCE_ID=artifact-1
VECTIS_ARTIFACT_STORAGE_DIR=/var/lib/vectis/artifact/artifact-1
```

Do not run two active queue, log, or local artifact processes against the same
storage directory. Do not reuse active instance IDs for different shards.

To store artifact blobs in an S3-compatible object store instead of a local
artifact directory, configure the artifact service with the S3 backend:

```sh
VECTIS_ARTIFACT_STORAGE_BACKEND=s3
VECTIS_ARTIFACT_STORAGE_S3_ENDPOINT=https://s3.internal
VECTIS_ARTIFACT_STORAGE_S3_REGION=us-east-1
VECTIS_ARTIFACT_STORAGE_S3_BUCKET=vectis-artifacts
VECTIS_ARTIFACT_STORAGE_S3_PREFIX=prod
VECTIS_ARTIFACT_STORAGE_S3_ACCESS_KEY_ID=vectis-artifact
VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/vectis-artifact-s3
VECTIS_ARTIFACT_STORAGE_S3_PATH_STYLE=true
```

The S3 backend changes where artifact bytes are stored; it does not change the
internal artifact gRPC API, artifact shard identity, or run metadata routing.
Keep the bucket private and apply operator-managed retention, backup, lifecycle,
and credential rotation controls. For local compatibility validation, run
`make s3-smoke-up` and `make s3-smoke-check` to exercise both public and
access-key-enforced S3 endpoints, or point `go run ./extensions/artifacts/s3/smoke`
at a managed S3-compatible endpoint.

## Worker And Isolation Contract

Workers need explicit routing to their local execution boundary and internal
services:

```sh
VECTIS_WORKER_QUEUE_ADDRESS=queue.internal:8081
VECTIS_WORKER_ORCHESTRATOR_ADDRESS=orchestrator.internal:8087
VECTIS_WORKER_LOG_ADDRESS=log.internal:8083
VECTIS_WORKER_ARTIFACT_ADDRESS=artifact.internal:8086
VECTIS_WORKER_CORE_SOCKET=/run/vectis/worker-core.sock
VECTIS_WORKER_CORE_SHELL_SOCKET=/run/vectis/worker-core-shell.sock
```

Worker-core host execution is not a security sandbox. For untrusted workloads,
place workers on isolated hosts or configure a VM-capable backend and route
untrusted jobs to workers that advertise VM isolation.

For Lima-backed VM execution:

```sh
VECTIS_WORKER_CORE_EXECUTION_BACKEND=lima
VECTIS_WORKER_CORE_LIMA_INSTANCE=vectis-worker
VECTIS_WORKER_CORE_LIMA_START=true
VECTIS_WORKER_CORE_WORKSPACE_ROOT=/var/lib/vectis/workspaces
VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT=/var/tmp/vectis-workspaces
```

Jobs that require `isolation: "vm"` must only be routed to workers whose core
advertises VM support. Vectis does not silently fall back from VM to host.

## Secret Resolution Contract

When `vectis-secrets` is enabled:

```sh
VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_ROOT=/var/lib/vectis/secrets/envelopes
VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_KEY_FILE=/etc/vectis/secrets/encryptedfs.key
VECTIS_SECRETS_POLICY_ALLOW=namespace=/teams/build;job=release;ref=encryptedfs://teams/build/*
# Optional Knox provider:
# VECTIS_SECRETS_PROVIDERS_KNOX_URL=https://knox.internal.example
# VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN_FILE=/etc/vectis/secrets/knox-token
# VECTIS_SECRETS_PROVIDERS_KNOX_CA_FILE=/etc/vectis/secrets/knox-ca.crt
# VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_CERT_FILE=/etc/vectis/secrets/knox-client.crt
# VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_KEY_FILE=/etc/vectis/secrets/knox-client.key
# VECTIS_SECRETS_POLICY_ALLOW=namespace=/teams/build;job=release;ref=knox://release/*
```

For Knox compatibility validation, run `make knox-smoke` to clone/build a local
Knox smoke image and resolve a seeded secret through a live Knox server process.
For a managed endpoint, point `make knox-smoke-check` at a known `knox://` ref
and the expected secret SHA-256 digest. The smoke resolves the primary key
version and can also verify wrong-token denial and missing-key behavior.

Also configure matching execution identity settings on workers and
`vectis-secrets`:

```sh
VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED=true
VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN=prod.example
VECTIS_WORKER_EXECUTION_IDENTITY_PATH_TEMPLATE=/cell/{cell}/namespace/{namespace}/job/{job}/run/{run}/execution/{execution}
```

When SPIFFE SVID acquisition is required:

```sh
VECTIS_WORKER_SPIFFE_ENABLED=true
VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS=unix:///run/vectis/spiffe/workload.sock
VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED=true
VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS=unix:///run/vectis/spiffe/registration.sock
VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID=spiffe://prod.example/vectis/worker-node
VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS=unix:uid:1000
```

Do not mount SPIFFE Workload API sockets, SPIFFE Entry API sockets, SVID private
keys, registration credentials, encryptedfs keys, Knox auth tokens, or broad secret-store
credentials into arbitrary job processes.

## Metrics And Logs Contract

Dedicated metrics listeners are not user-facing APIs. Keep them private and
configure Host allowlists for trusted scrapers:

```sh
VECTIS_METRICS_ALLOWED_HOSTS=prometheus.internal
VECTIS_METRICS_TLS_INSECURE=false
VECTIS_METRICS_TLS_CERT_FILE=/etc/vectis/tls/metrics.pem
VECTIS_METRICS_TLS_KEY_FILE=/etc/vectis/tls/metrics.key
```

At handoff, monitoring must cover:

- Vectis metrics and health checks;
- host disk and inode pressure for queue, log, artifact, spool, and secret
  directories;
- PostgreSQL availability, pool pressure, backups, and slow queries;
- API security rejection spikes and audit drops;
- queue backlog and DLQ growth;
- reconciler failures and stuck queued runs;
- worker failure ratios and worker/core lifecycle;
- log append failures, shard route failures, and forwarder spool backlog;
- artifact storage pressure and upload failures;
- secret-resolution failures when `vectis-secrets` is enabled.

Use [Production Monitoring Contract](../reliability/production-monitoring.md)
for the full monitoring ownership model, starter alert set, known gaps, and
handoff checklist.

## Backup And Retention Contract

Backups must include:

- PostgreSQL database;
- queue persistence directories;
- log storage;
- artifact storage;
- log-forwarder spools;
- encryptedfs envelopes and key files;
- SPIFFE CA material;
- TLS/private key material;
- live config, rendered manifests, dashboards, and alert rules.

Retention must be an explicit operator policy. Set
`VECTIS_RETENTION_CLEANUP_*` defaults for cleanup windows, evidence freshness,
required backup/audit/hold-review gates, and the cleanup evidence manifest path
if recurring jobs use one, then either schedule
`vectis-cli retention cleanup` or assign it to a recurring runbook. Use
[Retention And Storage Pressure](../reliability/retention.md#production-scheduling)
for dry-run/apply scheduling patterns. Keep audit retention aligned with
security policy, and keep idempotency retention longer than realistic client
retry windows.

Run and record the
[Production v1 backup/restore drill](../reliability/backup-restore.md#production-v1-drill)
before production handoff and before releases whose rollback plan depends on
database restore.

## Preflight Checklist

Before starting production traffic, confirm:

1. No production service uses the checked-in example DSN or plaintext demo TLS defaults.
2. `vectis-cli database migrate` has run against every production database.
3. API auth is enabled and setup bootstrap lifecycle is understood.
4. Public API and docs Hosts match allowed-host configuration.
5. Trusted proxy CIDRs include only the direct proxy tier.
6. Internal service endpoints are private.
7. gRPC TLS/mTLS settings match on clients and servers.
8. Stateful shard instance IDs and storage paths are stable and unique.
9. Worker/core sockets are private to the worker failure domain.
10. Secret-resolution policy is default-deny with specific allow rules.
11. Backup, restore, retention, and monitoring ownership is recorded.

## Related Documentation

| Topic | Document |
| --- | --- |
| Production topology | [Production Topology v1](./production-topology-v1.md) |
| Linux deployment runbook | [Production Linux Deployment](./production-linux.md) |
| Configuration reference | [Configuration](../configuration.md) |
| Security baseline | [Security](../../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Secrets and redaction | [Secrets And Redaction](./secrets-and-redaction.md) |
| Trusted proxy headers | [Trusted Proxy Headers](./trusted-proxy-client-ip.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Production monitoring | [Production Monitoring Contract](../reliability/production-monitoring.md) |
| Retention | [Retention And Storage Pressure](../reliability/retention.md) |
