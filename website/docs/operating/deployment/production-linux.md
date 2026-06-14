# Production Linux Deployment

This runbook turns [Production Topology v1](./production-topology-v1.md) into a
Linux operating flow. It is written for operators using DEB/RPM packages,
rendered systemd artifacts, or config management that installs the same files.

Vectis owns the service artifact contract: binaries, unit names, environment
file shape, system user, directories, and migration ordering. Operators still
own host placement, live `/etc/vectis/*.env` files, secrets, TLS material,
PostgreSQL, firewalls, service enablement, observability, backups, and retention
scheduling. The required production settings and secret inventory are summarized
in [Production Config And Secrets Contract](./production-config-contract.md).

## Current Packaging Boundary

The production package lane installs the standard standalone service set from
`deploy/linux/services.toml`: common files, migration unit, registry, queue,
orchestrator, log, log-forwarder, artifact, API, cell-ingress, worker-core,
worker, cron, catalog, reconciler, secrets, SPIFFE authority, docs, and CLI
packages.

Packages place files on disk. They do not decide host placement, write live
secrets, enable units, start services, or replace operator-owned supervision and
configuration policy.

## Before You Start

Decide these deployment facts first:

| Decision | Production v1 expectation |
| --- | --- |
| Site and cell model | Start with one site and one execution cell unless multi-cell is intentionally enabled. |
| Database | Use PostgreSQL. Have backup, restore, and migration access before starting Vectis services. |
| API edge | Terminate HTTPS at the edge or serve HTTPS directly; configure allowed Hosts and trusted proxy CIDRs. |
| Internal network | Keep gRPC, metrics, worker-control, cell-ingress, database, registry, log, artifact, and secrets endpoints private. |
| Service discovery | Use registry discovery deliberately, or pin queue, orchestrator, log, and artifact addresses where the platform makes that safer. |
| Storage | Assign durable directories or volumes for queue persistence, logs, artifacts, secret envelopes, SPIFFE CA material, and log-forwarder spools. |
| Secrets | Store bootstrap token, API tokens, PostgreSQL credentials, TLS keys, SPIFFE CA material, and encryptedfs keys in a secret manager. |
| Observability | Scrape Vectis metrics and also monitor hosts, filesystems, and PostgreSQL directly. |
| Retention | Schedule `vectis-cli retention cleanup` or assign it to an operator runbook. |

## Install Artifacts

Use one of these paths.

For packages:

```sh
make package-linux
```

Install the package set for the services this host should run. The package lane
places files on disk only; it does not run maintainer scripts, call
`systemctl`, or write live production config.

For rendered artifacts:

```sh
vectis-cli deploy linux render --output artifacts/deploy/linux
```

Then install the rendered unit files, sysusers, tmpfiles, and env examples using
the rendered `install/manifest.tsv` or your config-management equivalent.

After package or artifact install, create the service user and directories:

```sh
systemd-sysusers /usr/lib/sysusers.d/vectis.conf
systemd-tmpfiles --create /usr/lib/tmpfiles.d/vectis.conf
systemctl daemon-reload
```

## Write Live Configuration

The rendered `env/*.example` files are examples. Production configuration lives
in operator-managed files such as `/etc/vectis/vectis.env` and service-specific
env files.

For a concrete systemd environment-file starting point, use the
[Production Environment Template](./production-env-template.md).

At minimum, set the common environment:

```sh
VECTIS_CELL_ID=local
VECTIS_LOG_FORMAT=json
VECTIS_LOG_DIR=/var/log/vectis/components
VECTIS_DATABASE_DRIVER=pgx
VECTIS_DATABASE_DSN=postgres://vectis:<secret>@postgres.internal:5432/vectis?sslmode=require
VECTIS_DATABASE_PGX_MAX_OPEN_CONNS=25
VECTIS_DATABASE_PGX_MAX_IDLE_CONNS=10
VECTIS_DISCOVERY_REGISTRY_ADDRESS=registry.internal:8082
```

For shared or production-like deployments, do not keep the rendered example
defaults for plaintext internal traffic. Configure `VECTIS_GRPC_TLS_*`,
`VECTIS_METRICS_TLS_*`, and service identity allowlists where internal traffic
crosses host or network boundaries. If the API is behind an HTTPS edge, set API
auth, allowed Hosts, trusted proxy CIDRs, and browser-facing cookie posture:

```sh
VECTIS_API_AUTH_ENABLED=true
VECTIS_API_AUTH_BOOTSTRAP_TOKEN=<secret-from-secret-manager>
VECTIS_API_ALLOWED_HOSTS=ci.example.com
VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS=10.0.0.0/24
VECTIS_API_SESSION_COOKIE_SECURE=true
```

Configure every shard with stable identity and storage:

```sh
VECTIS_QUEUE_INSTANCE_ID=queue-1
VECTIS_QUEUE_PERSISTENCE_DIR=/var/lib/vectis/queue/default/queue-1
VECTIS_LOG_INSTANCE_ID=log-1
VECTIS_LOG_STORAGE_DIR=/var/lib/vectis/log/log-1
VECTIS_ARTIFACT_INSTANCE_ID=artifact-1
VECTIS_ARTIFACT_STORAGE_DIR=/var/lib/vectis/artifact/artifact-1
```

Configure workers with private dependency addresses or registry discovery, plus
a paired worker-core socket when worker core is used:

```sh
VECTIS_WORKER_QUEUE_ADDRESS=queue.internal:8081
VECTIS_WORKER_ORCHESTRATOR_ADDRESS=orchestrator.internal:8087
VECTIS_WORKER_LOG_ADDRESS=log.internal:8083
VECTIS_WORKER_ARTIFACT_ADDRESS=artifact.internal:8086
VECTIS_WORKER_CORE_SOCKET=/run/vectis/worker-core.sock
```

When secret resolution is enabled, also configure execution identity,
`vectis-spiffe`, and `vectis-secrets` consistently. Keep Workload API and Entry
API sockets private to worker-controlled code, not arbitrary job processes.

## Run Migrations

Runtime services wait for the expected schema; they do not apply migrations.
Run migrations before starting or rolling new binaries:

```sh
VECTIS_DATABASE_DRIVER=pgx \
VECTIS_DATABASE_DSN='postgres://vectis:<secret>@postgres.internal:5432/vectis?sslmode=require' \
vectis-cli database migrate
```

For split global and cell databases, run the migration command once for each
database with the matching DSN. Do not start workers, cron, reconciler, catalog,
or API against a database whose schema status is unknown.

## Start Services

Start infrastructure before producers and workers. In a systemd deployment, the
rendered units express the local ordering that exists today, but an operator
still needs to account for services supervised outside the current package set.

Recommended dependency order:

1. PostgreSQL and platform-managed secret/TLS material.
2. `vectis-registry`, or confirm pinned addresses are configured.
3. `vectis-queue` shards.
4. `vectis-orchestrator`.
5. `vectis-log` shards and `vectis-artifact` shards.
6. `vectis-spiffe` and `vectis-secrets`, if enabled.
7. `vectis-cell-ingress`, if multi-cell or private cell handoff is enabled.
8. `vectis-api`.
9. `vectis-worker-core` instances.
10. `vectis-worker` instances.
11. `vectis-cron`, if schedules are used.
12. `vectis-reconciler`.
13. `vectis-catalog`, if multi-cell fan-in is used.
14. `vectis-log-forwarder`, where worker-host spooling is used.
15. `vectis-docs`, if published for operators.

For the package-managed standalone services on a host:

```sh
systemctl enable vectis-registry.service vectis-queue.service vectis-log.service
systemctl enable vectis-api.service vectis-worker.service vectis-reconciler.service
systemctl start vectis.target
```

Enable only the units that should run on that host. Do not enable
`vectis-cron.service` unless schedules should fire from that deployment. Do not
publish `vectis-docs.service` outside an operator-controlled network unless its
Host allowlist and edge access controls are configured.

## Complete API Setup

When API auth is enabled on a new database:

1. Start `vectis-api` with `VECTIS_API_AUTH_BOOTSTRAP_TOKEN`.
2. Complete setup through the API or CLI.
3. Create durable operator credentials or API tokens.
4. Remove or rotate the bootstrap token where the deployment model allows.
5. Store recovery credentials in the operator secret manager.

The bootstrap token is not a standing administrator password. A database that
already has setup completed does not need it for normal startup.

## Verify The Deployment

Run these before declaring the deployment ready:

```sh
vectis-cli health check --strict
```

Then exercise the workflow:

1. Check `GET /health/live` and `GET /health/ready` through the API edge.
2. Confirm API auth setup state and log in with an expected operator identity.
3. Trigger a small known-safe job.
4. Confirm the run reaches a terminal state.
5. Stream logs with `vectis-cli logs run <run-id>`.
6. List run artifacts if the job produces any.
7. Inspect dispatch events for the run.
8. Confirm queue, reconciler, worker, log, artifact, audit, and database-pool metrics are fresh.
9. Confirm host disk, PostgreSQL, and filesystem telemetry are visible outside Vectis.
10. Confirm alert routing and runbook links are installed in the production telemetry system.

## Rollout And Restart Rules

For planned rollout:

1. Take or verify a recent backup.
2. Pause cron if scheduled work should not start during the change.
3. Run migrations before rolling binaries when the release requires it.
4. Roll API replicas behind readiness checks.
5. Restart queue, orchestrator, registry, log, and artifact during a quiet window when possible.
6. Roll workers gradually and allow graceful drain.
7. Keep the reconciler running, or restart it last.
8. Run `vectis-cli health check --strict` and a smoke run afterward.

Worker termination windows should be long enough for ordinary job finalization.
Abrupt worker death relies on leases, queue redelivery, and repair. Restart a
worker and its paired worker core together unless the provider topology has a
documented independent restart contract.

## Backup, Retention, And Monitoring Handoff

Before the deployment is handed to operators:

- run and record the [Production v1 backup/restore drill](../reliability/backup-restore.md#production-v1-drill);
- define retention windows for runs, idempotency keys, audit rows, logs, and artifact blobs;
- schedule or assign `vectis-cli retention cleanup` using the [production scheduling guidance](../reliability/retention.md#production-scheduling);
- install alert rules for queue backlog, DLQ growth, reconciler failures, worker failure ratio, log append failures, artifact/storage pressure, audit drops, retry exhaustion, DB pool saturation, and API security rejection spikes;
- record all service instance IDs and durable storage paths.

## Related Documentation

| Topic | Document |
| --- | --- |
| Production topology | [Production Topology v1](./production-topology-v1.md) |
| Environment-file template | [Production Environment Template](./production-env-template.md) |
| Config and secrets contract | [Production Config And Secrets Contract](./production-config-contract.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Scaling and restarts | [Scaling And Restarts](./scaling-and-restarts.md) |
| Configuration | [Configuration](../configuration.md) |
| Trusted proxy headers | [Trusted Proxy Headers](./trusted-proxy-client-ip.md) |
| Secrets and redaction | [Secrets And Redaction](./secrets-and-redaction.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Production monitoring | [Production Monitoring Contract](../reliability/production-monitoring.md) |
| Production drills | [Production Drills](../reliability/production-drills.md) |
| Retention | [Retention And Storage Pressure](../reliability/retention.md) |
| Runbooks and alerts | [Runbooks And Alerts](../reliability/runbooks.md) |
