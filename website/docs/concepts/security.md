# Security

This page describes Vectis' current security posture for self-hosted operators. It is not a penetration test report or formal threat model. It is a practical map of what Vectis protects today, what the platform expects you to protect, and which controls matter most before exposing a deployment beyond a local machine.

For outage behavior, see [Failure Domains](./failure-domains.md). For environment variables, ports, TLS files, and secret placement, see [Configuration](../operating/configuration.md).

## Quick Posture

| Surface | Current behavior | Operator baseline |
| --- | --- | --- |
| HTTP API | Authentication is off by default for local and development use. Local users, API tokens, RBAC, rate limits, and audit logging are available when API auth is enabled. | Enable API auth outside throwaway local use, terminate HTTPS at the edge, and restrict who can reach the API. |
| Internal gRPC | Queue, registry, and log service traffic can use optional TLS or mTLS. Standalone binaries default to plaintext unless configured. | Keep internal ports private. Use TLS or mTLS on shared networks. |
| Service authorization | Internal gRPC servers do not yet enforce application-level per-service authorization. mTLS can verify certificates, but Vectis does not map cert identities to per-RPC allow/deny rules yet. | Treat network reachability to queue, registry, log, and worker-control paths as sensitive. |
| Metrics | Prometheus endpoints are unauthenticated. Some deployments enable TLS for dedicated metrics listeners. | Scrape from a trusted network only, or block metrics at the edge. |
| Database and files | SQL, SQLite files, queue persistence, logs, and backups may contain sensitive operational state. Vectis does not encrypt these at rest itself. | Use platform disk or volume encryption, filesystem permissions, and secret-store controls. |
| Jobs and logs | Job definitions can cause workers to execute code. Logs may contain credentials or personal data emitted by build steps. | Limit who can define or trigger jobs, isolate workers where needed, and restrict log access. |

## HTTP API Authentication

By default, `api.auth.enabled=false` in embedded defaults. In that mode, job and run routes behave like an open local API. That is convenient for quick development, but it is not the posture to use on an exposed network.

Enable API authentication with:

```sh
VECTIS_API_AUTH_ENABLED=true
```

On a fresh database, configure a shared bootstrap secret before completing setup:

```sh
VECTIS_API_AUTH_BOOTSTRAP_TOKEN=<at-least-16-characters>
```

Until setup completes, health, metrics, and `/api/v1/setup/*` remain available. Other API routes return `503 setup_required`. After setup, clients use:

```http
Authorization: Bearer <api_token>
```

Vectis stores local user passwords with bcrypt. API tokens are generated from 32 random bytes and stored as SHA-256 hashes for lookup. Treat a database leak as credential exposure: rotate API tokens and bootstrap secrets if the database is compromised. Login tokens created by `POST /api/v1/login` default to a one-week expiry.

API errors use a stable JSON envelope with a `code` value such as `setup_required`, `authentication_required`, `authorization_denied`, or `auth_unavailable`. Integrations should key off those codes. The public route and error contract lives in [API Reference](../using/api-reference.md).

## Authorization

`api.authz.engine` selects the authorization policy after setup.

| Engine | Use when | Behavior |
| --- | --- | --- |
| `hierarchical_rbac` | You want the normal multi-user model. | Namespace-scoped RBAC with inherited permissions. Roles are `viewer`, `trigger`, `operator`, and `admin`; inheritance can stop at a namespace with `break_inheritance`. |
| `authenticated_full` | You want a simple authenticated demo or trusted single-team deployment. | Any authenticated principal may perform non-setup actions. |

The CLI exposes routine auth and admin flows such as login, logout, token management, users, namespaces, role bindings, and health checks. See [CLI Guide](../using/cli-guide.md).

## Internal Service Traffic

Vectis services communicate over gRPC:

| Service | Role |
| --- | --- |
| `vectis-queue` | Accepts enqueues and dispatches work to workers. |
| `vectis-registry` | Provides service discovery when discovery is enabled. |
| `vectis-log` | Receives worker log chunks and serves stored log streams. |
| API, worker, cron, reconciler | Dial queue, registry, or log depending on role and configuration. |

Standalone binaries default to plaintext internal gRPC (`VECTIS_GRPC_TLS_INSECURE=true`) unless TLS is configured. `vectis-local` bootstraps development TLS by default and injects the relevant `VECTIS_GRPC_TLS_*` variables into child processes. The Podman reference deployment also generates and mounts internal gRPC TLS material.

When `VECTIS_GRPC_TLS_INSECURE=false`, listeners need certificate and key files, and gRPC clients need a CA bundle. Optional mTLS can verify peer certificates when a client CA is configured, but Vectis does not yet use certificate identity as an application authorization policy.

For the service identity matrix, private port guidance, and checklist for new internal RPCs, see [Internal Service Trust](./internal-service-trust.md).

## Metrics

`vectis-api` serves `/metrics` on the same HTTP listener as the REST API. `vectis-queue`, `vectis-worker`, `vectis-log`, and `vectis-reconciler` use dedicated metrics listeners by default.

Metrics endpoints are not authenticated. The Podman reference deployment enables HTTPS on the dedicated queue, worker, and log metrics listeners through `VECTIS_METRICS_TLS_*`; API metrics remain on the API HTTP listener until API TLS is added separately.

Keep metrics scrape paths on trusted networks. Metrics are operational data, but they can still reveal deployment shape, service health, traffic patterns, error rates, and names of internal components.

## Secrets, Storage, and Logs

Protect these as sensitive:

| Item | Why it matters |
| --- | --- |
| `VECTIS_DATABASE_DSN` | Often contains database credentials. Avoid committing it or printing it in logs. |
| Bootstrap and API tokens | Grant administrative or API access depending on setup state and scopes. |
| CLI token files | Let local CLI commands authenticate as a user. File permissions matter. |
| SQLite files and SQL backups | Contain job definitions, runs, users, token hashes, namespaces, role bindings, audit records, and idempotency keys. |
| Queue persistence and log storage | May contain queued work, execution state, or sensitive output from jobs. |
| Generated deploy TLS material | Establishes trust for internal gRPC or metrics endpoints in reference deployments. |

Use your platform's secret store and storage controls: Kubernetes secrets, vault agents, container runtime permissions, restricted host users, encrypted volumes, and backup access policies. Vectis does not currently provide its own encryption-at-rest layer for SQLite files, queue WALs, log storage, or backups.

For the current sensitive-surface inventory and redaction guidance, see [Secrets and Redaction](../operating/deployment/secrets-and-redaction.md).

## Job Execution Risk

Workers run actions from job definitions. A shell action or checkout source should be treated as code execution capability.

Only trusted users should define jobs or trigger runs in a shared deployment. If jobs come from less-trusted teams or repositories, isolate workers with the controls available in your environment: separate hosts, containers, service accounts, read-only roots, restricted network access, and careful secret mounting.

Vectis rejects checkout URLs that embed user info, such as `https://user:token@example.com/org/repo.git`, because those credentials can leak through persisted job definitions, logs, or process surfaces. Build steps can still print secrets themselves, so log access remains sensitive.

## Audit and Abuse Controls

API audit events are enabled by default. Operators can disable audit emission or override per-event durability with `api.audit.*` or `VECTIS_API_AUDIT_*` settings. Dropped audit events and flush failures remain observable through audit metrics and health checks.

The auth surface also has bounded request sizes and token parsing limits:

| Limit | Purpose |
| --- | --- |
| Setup and login JSON body cap | Limits memory and parsing work on hostile requests. |
| Bearer token length cap | Prevents oversized authorization headers from causing extra CPU or memory work. |
| Admin username and password bounds | Keeps setup input predictable. |

Fuzz targets for auth parsing, token hashing, and route-to-action mapping are available through `make fuzz-api-auth`.

## Recommended Baseline

Use this as the minimum checklist before treating a deployment as shared or production-like:

1. Enable API auth.
2. Complete setup and remove or rotate the bootstrap secret.
3. Terminate HTTPS at an ingress, reverse proxy, or platform edge.
4. Keep queue, registry, log, worker-control, and metrics ports off public networks.
5. Enable internal gRPC TLS or mTLS on shared networks.
6. Store database DSNs, API tokens, bootstrap tokens, and deploy TLS material in a secret manager.
7. Restrict who can define jobs, trigger jobs, and read logs.
8. Protect SQL, SQLite, queue, log, and backup storage like sensitive application data.
9. Monitor audit drops, audit flush failures, auth failures, queue health, and service readiness.

## Reporting Security Issues

If you discover a security vulnerability in this project, please report it privately to the maintainers through the repository contact path or GitHub Security Advisories, if enabled, before public disclosure.

## Related Documentation

| Topic | Document |
| --- | --- |
| Architecture and protocols | [Architecture](./architecture.md) |
| Environment variables, TLS, and discovery | [Configuration](../operating/configuration.md) |
| Failure and dependency behavior | [Failure Domains](./failure-domains.md) |
| Internal service trust | [Internal Service Trust](./internal-service-trust.md) |
| Secrets and redaction | [Secrets and Redaction](../operating/deployment/secrets-and-redaction.md) |
| Design decisions | [Architecture Decisions](../developing/architecture-decisions/index.md) |
