# Production Environment Template

This page gives operators a concrete starting point for a small production-v1
Linux deployment using systemd environment files. Treat every value as a
template. Do not commit filled secret values back to the repository.

The examples assume one host or one tightly controlled private network running
the standard standalone service set. Split the files by host when services are
placed on separate machines, and keep only the variables each host needs.

## File Layout

Use one common file plus optional service-specific files:

| File | Purpose |
| --- | --- |
| `/etc/vectis/vectis.env` | Shared cell, logging, database, discovery, TLS, and API edge settings. |
| `/etc/vectis/api.env` | API auth, allowed Hosts, proxy, cookie, and public listener settings. |
| `/etc/vectis/queue.env` | Queue shard identity and persistence settings. |
| `/etc/vectis/log.env` | Log shard identity and storage settings. |
| `/etc/vectis/artifact.env` | Artifact shard identity and storage settings. |
| `/etc/vectis/orchestrator.env` | Orchestrator listener and shard settings. |
| `/etc/vectis/worker-core.env` | Worker-core execution backend and workspace settings. |
| `/etc/vectis/worker.env` | Worker dependency addresses, control listener, and worker-core sockets. |
| `/etc/vectis/secrets.env` | Secret broker listener, encryptedfs root, key file, and policy. |
| `/etc/vectis/spiffe.env` | SPIFFE trust domain, CA storage, sockets, selector, and bundle path. |
| `/etc/vectis/cron.env` | Schedule producer identity and lease settings. |
| `/etc/vectis/scm-poller.env` | SCM poll producer identity, claim settings, and provider credentials. |
| `/etc/vectis/scm-gerrit-stream.env` | Optional Gerrit stream-events producer settings. |
| `/etc/vectis/reconciler.env` | Repair loop interval, lease, and metrics settings. |
| `/etc/vectis/catalog.env` | Catalog fan-in settings when multi-cell is enabled. |
| `/etc/vectis/docs.env` | Operator docs listener settings if `vectis-docs` is enabled. |

Rendered package artifacts include example env files. Live files remain
operator-owned, and should be written by config management or a secret manager
integration.

## Common Environment

`/etc/vectis/vectis.env`:

```sh
XDG_DATA_HOME=/var/lib
XDG_CACHE_HOME=/var/cache
XDG_CONFIG_HOME=/etc
XDG_RUNTIME_DIR=/run

VECTIS_CELL_ID=local
VECTIS_LOG_FORMAT=json
VECTIS_LOG_DIR=/var/log/vectis/components

VECTIS_DATABASE_DRIVER=pgx
VECTIS_DATABASE_DSN=postgres://vectis:<secret-from-secret-manager>@postgres.internal:5432/vectis?sslmode=require
VECTIS_DATABASE_PGX_MAX_OPEN_CONNS=25
VECTIS_DATABASE_PGX_MAX_IDLE_CONNS=10
VECTIS_DATABASE_PGX_CONN_MAX_LIFETIME=1h
VECTIS_DATABASE_PGX_CONN_MAX_IDLE_TIME=15m

VECTIS_RETENTION_CLEANUP_TERMINAL_RUN_AGE=720h
VECTIS_RETENTION_CLEANUP_JOB_DEFINITION_AGE=720h
VECTIS_RETENTION_CLEANUP_IDEMPOTENCY_AGE=48h
VECTIS_RETENTION_CLEANUP_AUDIT_AGE=8760h
VECTIS_RETENTION_CLEANUP_ARTIFACT_BLOB_AGE=720h
VECTIS_RETENTION_CLEANUP_EVIDENCE_MANIFEST=/var/lib/vectis/ops/retention-cleanup-evidence.json
VECTIS_RETENTION_CLEANUP_REQUIRE_BACKUP_MANIFEST=true
VECTIS_RETENTION_CLEANUP_BACKUP_MAX_AGE=24h
VECTIS_RETENTION_CLEANUP_BACKUP_STORAGE_MAX_AGE=24h
VECTIS_RETENTION_CLEANUP_REQUIRE_AUDIT_EXPORT=true
VECTIS_RETENTION_CLEANUP_AUDIT_EXPORT_MAX_AGE=24h
VECTIS_RETENTION_CLEANUP_REQUIRE_HOLD_REVIEW=true
VECTIS_RETENTION_CLEANUP_HOLD_REVIEW_MAX_AGE=24h

VECTIS_DISCOVERY_REGISTRY_ADDRESS=registry.internal:8082

VECTIS_GRPC_TLS_INSECURE=false
VECTIS_GRPC_TLS_CA_FILE=/etc/vectis/tls/internal-ca.pem
VECTIS_GRPC_TLS_CERT_FILE=/etc/vectis/tls/service.pem
VECTIS_GRPC_TLS_KEY_FILE=/etc/vectis/tls/service-key.pem

VECTIS_METRICS_TLS_INSECURE=false
VECTIS_METRICS_TLS_CERT_FILE=/etc/vectis/tls/metrics.pem
VECTIS_METRICS_TLS_KEY_FILE=/etc/vectis/tls/metrics-key.pem
```

If all internal services are on the same private host for an initial deployment,
operators may choose loopback-only plaintext gRPC while the host boundary is
being built out. Do not use plaintext for internal traffic that crosses hosts,
networks, or trust boundaries.

## API

`/etc/vectis/api.env`:

```sh
VECTIS_API_SERVER_HOST=0.0.0.0
VECTIS_API_SERVER_PORT=8080

VECTIS_API_AUTH_ENABLED=true
VECTIS_API_AUTH_BOOTSTRAP_TOKEN=<temporary-secret-from-secret-manager>
VECTIS_API_ALLOWED_HOSTS=ci.example.com
VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS=10.0.0.0/24
VECTIS_API_SESSION_COOKIE_SECURE=true
```

Expose `vectis-api` through an HTTPS edge or configure direct HTTPS according
to the deployment policy. Keep the bootstrap token temporary; remove or rotate
it after setup when the environment allows.

## Registry, Queue, Log, Artifact, And Orchestrator

`/etc/vectis/registry.env`:

```sh
VECTIS_REGISTRY_PORT=8082
```

`/etc/vectis/queue.env`:

```sh
VECTIS_QUEUE_PORT=8081
VECTIS_QUEUE_METRICS_PORT=9081
VECTIS_QUEUE_POOL=default
VECTIS_QUEUE_INSTANCE_ID=queue-1
VECTIS_QUEUE_PERSISTENCE_DIR=/var/lib/vectis/queue/default/queue-1
VECTIS_QUEUE_PERSISTENCE_SNAPSHOT_EVERY=128
VECTIS_QUEUE_REGISTER_WITH_REGISTRY=true
```

`/etc/vectis/log.env`:

```sh
VECTIS_LOG_GRPC_PORT=8083
VECTIS_LOG_METRICS_PORT=9083
VECTIS_LOG_INSTANCE_ID=log-1
VECTIS_LOG_STORAGE_DIR=/var/lib/vectis/log/log-1
VECTIS_LOG_STORAGE_READ_ONLY_MIN_FREE_BYTES=1073741824
```

`/etc/vectis/artifact.env`:

```sh
VECTIS_ARTIFACT_GRPC_PORT=8086
VECTIS_ARTIFACT_METRICS_PORT=9089
VECTIS_ARTIFACT_INSTANCE_ID=artifact-1
VECTIS_ARTIFACT_STORAGE_DIR=/var/lib/vectis/artifact/artifact-1
VECTIS_ARTIFACT_STORAGE_READ_ONLY_MIN_FREE_BYTES=1073741824
VECTIS_ARTIFACT_GRPC_REGISTER_WITH_REGISTRY=true
```

`/etc/vectis/orchestrator.env`:

```sh
VECTIS_ORCHESTRATOR_PORT=8087
VECTIS_ORCHESTRATOR_METRICS_PORT=9090
VECTIS_ORCHESTRATOR_SHARDS=0
VECTIS_ORCHESTRATOR_REGISTER_WITH_REGISTRY=true
```

Use stable instance IDs and durable, backed-up storage paths. For multiple
queue, log, or artifact shards, give every shard a unique instance ID and a
separate persistence or storage directory.

## Workers

`/etc/vectis/worker-core.env`:

```sh
VECTIS_WORKER_CORE_SOCKET=/run/vectis/worker-core.sock
VECTIS_WORKER_CORE_METRICS_PORT=9092
VECTIS_WORKER_CORE_EXECUTION_BACKEND=host
VECTIS_WORKER_CORE_WORKSPACE_ROOT=/var/lib/vectis/workspaces
VECTIS_WORKER_CORE_CHECKOUT_CACHE_ROOT=/var/lib/vectis/checkout-cache
VECTIS_WORKER_CORE_LIMA_INSTANCE=
VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT=
VECTIS_WORKER_CORE_LIMA_START=false
```

`/etc/vectis/worker.env`:

```sh
VECTIS_WORKER_METRICS_PORT=9082
VECTIS_WORKER_CONTROL_MODE=static
VECTIS_WORKER_CONTROL_PORT=9084

VECTIS_WORKER_QUEUE_ADDRESS=queue.internal:8081
VECTIS_WORKER_ORCHESTRATOR_ADDRESS=orchestrator.internal:8087
VECTIS_WORKER_LOG_ADDRESS=log.internal:8083
VECTIS_WORKER_ARTIFACT_ADDRESS=artifact.internal:8086

VECTIS_WORKER_CORE_SOCKET=/run/vectis/worker-core.sock
VECTIS_WORKER_CORE_SHELL_SOCKET=/run/vectis/worker-core-shell.sock
VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_INTERVAL=5m
VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_TIMEOUT=30m
VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_JITTER_RATIO=0.2
```

Run each `vectis-worker` with a paired `vectis-worker-core` unless a different
provider topology has been deliberately tested. Host execution is a
compatibility mode, not a sandbox. Use VM isolation for untrusted work and
record the worker/provider boundary in the release or deployment evidence.

## Secrets And SPIFFE

`/etc/vectis/spiffe.env`:

```sh
VECTIS_SPIFFE_TRUST_DOMAIN=vectis.internal
VECTIS_SPIFFE_DATA_DIR=/var/lib/vectis/spiffe
VECTIS_SPIFFE_RUNTIME_DIR=/run/vectis/spiffe
VECTIS_SPIFFE_WORKLOAD_SOCKET=/run/vectis/spiffe/workload.sock
VECTIS_SPIFFE_REGISTRATION_SOCKET=/run/vectis/spiffe/registration.sock
VECTIS_SPIFFE_BUNDLE_FILE=/var/lib/vectis/spiffe/bundle.pem
VECTIS_SPIFFE_SELECTOR=systemd:unit:vectis-worker.service
VECTIS_SPIFFE_X509_SVID_TTL=5m
```

`/etc/vectis/secrets.env`:

```sh
VECTIS_SECRETS_PORT=8090
VECTIS_SECRETS_METRICS_PORT=9091
VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_ROOT=/var/lib/vectis/secrets/envelopes
VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_KEY_FILE=/etc/vectis/secrets/encryptedfs.key
VECTIS_SECRETS_POLICY_ALLOW=namespace=/teams/build;job=release;task=*;ref=encryptedfs://teams/build/*
# Optional Knox provider:
# VECTIS_SECRETS_PROVIDERS_KNOX_URL=https://knox.internal.example
# VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN_FILE=/etc/vectis/secrets/knox-token
# VECTIS_SECRETS_POLICY_ALLOW=namespace=/teams/build;job=release;task=*;ref=knox://release/*
```

Store SPIFFE CA material, encryptedfs keys, Knox auth tokens, API bootstrap
tokens, database passwords, and TLS private keys in the operator's secret
manager. Keep Workload API and registration sockets private to trusted service
code.

## Cron, SCM Poller, Reconciler, Catalog, And Docs

`/etc/vectis/cron.env`:

```sh
VECTIS_CRON_INSTANCE_ID=cron-1
VECTIS_CRON_CLAIM_TTL=5m
```

`/etc/vectis/scm-poller.env`:

```sh
VECTIS_SCM_POLLER_INSTANCE_ID=scm-poller-1
VECTIS_SCM_POLLER_INTERVAL=30s
VECTIS_SCM_POLLER_CLAIM_TTL=5m
# Optional: pin queue discovery for this producer instead of using the shared registry.
# VECTIS_SCM_POLLER_QUEUE_ADDRESS=queue.internal:8081
# VECTIS_SCM_POLLER_REGISTRY_ADDRESS=registry.internal:8082

# Required only for Gerrit instances that do not allow anonymous query access.
VECTIS_SCM_POLLER_PROVIDERS_GERRIT_USERNAME=ci-bot
VECTIS_SCM_POLLER_PROVIDERS_GERRIT_PASSWORD_FILE=/run/secrets/vectis/gerrit-http-password
```

`/etc/vectis/scm-gerrit-stream.env`:

```sh
VECTIS_SCM_GERRIT_STREAM_URL=https://gerrit.example.com
VECTIS_SCM_GERRIT_STREAM_INSTANCE_ID=gerrit-stream-1
VECTIS_SCM_GERRIT_STREAM_TRANSPORT=ssh
VECTIS_SCM_GERRIT_STREAM_SSH_HOST=gerrit.example.com
VECTIS_SCM_GERRIT_STREAM_SSH_PORT=29418
VECTIS_SCM_GERRIT_STREAM_SSH_USER=ci-bot
VECTIS_SCM_GERRIT_STREAM_SSH_KEY_FILE=/etc/vectis/ssh/gerrit-stream
VECTIS_SCM_GERRIT_STREAM_SSH_KNOWN_HOSTS_FILE=/etc/vectis/ssh/known_hosts
# Optional: pin queue discovery for this producer instead of using the shared registry.
# VECTIS_SCM_GERRIT_STREAM_QUEUE_ADDRESS=queue.internal:8081
# VECTIS_SCM_GERRIT_STREAM_REGISTRY_ADDRESS=registry.internal:8082
```

`/etc/vectis/reconciler.env`:

```sh
VECTIS_RECONCILER_INTERVAL=30s
VECTIS_RECONCILER_LEASE_TTL=2m
VECTIS_RECONCILER_METRICS_PORT=9085
```

`/etc/vectis/catalog.env`:

```sh
VECTIS_CATALOG_INTERVAL=1s
VECTIS_CATALOG_BATCH_SIZE=100
VECTIS_CATALOG_METRICS_PORT=9086
```

`/etc/vectis/docs.env`:

```sh
VECTIS_DOCS_HOST=127.0.0.1
VECTIS_DOCS_PORT=8088
VECTIS_DOCS_LOG_LEVEL=info
```

Enable cron only when schedules should fire. Enable `vectis-scm-poller` only
when stored jobs use SCM polling triggers, and enable `vectis-scm-gerrit-stream`
only when a Gerrit stream-events subscription is part of the deployment. Enable
catalog when multi-cell fan-in is used. Publish docs only on an
operator-controlled network.

## Host Checklist

Before starting units on a host:

1. Confirm `/etc/vectis/*.env` files are owned by `root:vectis` and readable
   only by the service group.
2. Confirm `/etc/vectis/secrets/*` and TLS private keys are not world-readable.
3. Run `systemd-sysusers` and `systemd-tmpfiles` for Vectis.
4. Confirm queue, log, artifact, secrets, SPIFFE, workspace, and spool
   directories are on the intended durable storage.
5. Confirm firewalls expose only the expected API edge and private service
   ports.
6. Run migrations before starting database-backed services.
7. Start infrastructure before producers and workers.
8. Run `vectis-cli health check --strict` and a smoke job after startup.

Record the final file set, service placement, storage paths, and any plaintext
loopback-only exceptions in the production readiness evidence record.
