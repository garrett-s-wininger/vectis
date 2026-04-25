# Security posture

This document describes **how Vectis handles trust and sensitive data today**, what is **out of scope** for the application, and **practical guidance** for self-hosted deployments. It is **not** a penetration test report or formal threat model.

For outage behavior, see [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md). For configuration and secrets placement, see [CONFIGURATION.md](CONFIGURATION.md).

## Threat model (summary)

**In scope for operators**

- Who may call the **HTTP API** and from where.
- Protection of **database credentials** and other **environment secrets**.
- **Network** placement of API, workers, gRPC services, and the database.
- **Job definitions** and **schedules** as untrusted input (they drive shell execution).

**Largely out of scope in shipped code today**

- OIDC/LDAP, and internal gRPC peer authentication beyond optional TLS (see below).
- **mTLS** or token verification between internal gRPC peers (connections are typically **plaintext** unless you wrap transport externally).
- **Encryption at rest** for SQLite files, queue WALs, or log storage (relies on **disk** or **volume** encryption from the platform).
- **Secret scanning** of log streams or job output.

## HTTP API

### Default and optional API authentication

- **Default (`api.auth.enabled=false` in embedded defaults):** the REST surface behaves as before: **no** application-level authentication on job/run routes.
- **When enabled (`api.auth.enabled=true` or `VECTIS_API_AUTH_ENABLED=true`):** HTTP auth policy is on. Whether initial setup is still required is **read from the database** (not a separate "mode" string). Until setup completes, only health/metrics and **`/api/v1/setup/*`** are usable for normal clients; other routes return **503** `setup_required`. After setup, clients must send **`Authorization: Bearer <api_token>`** for mutating and data routes. For a fresh database, configure the shared bootstrap secret with **`VECTIS_API_AUTH_BOOTSTRAP_TOKEN`** (or `api.auth.bootstrap_token`)—at least **16 characters**—until setup has been completed once; see [CONFIGURATION.md](CONFIGURATION.md).
- **Password storage:** local user passwords are stored with **bcrypt** (`password_hash`). Bcrypt **embeds a per-password salt and work factor** in the encoded hash string; there is no separate `salt` column.
- **API tokens:** stored as **SHA-256** hex of the plaintext token (for constant-time lookup by hash). Tokens are generated with **32 random bytes** (64 hex chars); treat leaks of the DB as credential exposure—rotate tokens and bootstrap secrets if the database is compromised. Login tokens (created via **`POST /api/v1/login`**) default to a **1-week expiry**.
- **Stable error codes:** authentication and setup endpoints return JSON with an **`error`** string (e.g. `setup_required`, `authentication_required`, `authorization_denied`, `auth_unavailable`); integrators should key off these values. Definitions live in **`internal/api/auth_errors.go`**.
- **Authorization engines:** `api.authz.engine` selects the post-setup authorization policy:
  - **`hierarchical_rbac`** (default) — Namespace-scoped role-based access control with inheritance. Roles are `viewer`, `trigger`, `operator`, and `admin`. Permissions flow down the namespace tree unless `break_inheritance` is set on a namespace.
  - **`authenticated_full`** — Any authenticated principal may perform any non-setup action (simple setups / demos).
- **Abuse limits:** setup and login JSON bodies are capped (**64 KiB**); extracted Bearer material is capped (**4096** bytes) to limit CPU/memory work on hostile headers. Admin username/password lengths are bounded (see `internal/api/auth_limits.go`).
- **Trust boundary:** For production exposure beyond a private network, prefer **TLS at the edge**, **network policy**, and eventually **full IdP integration** (roadmap). Same practical posture as internal gRPC: do not expose the API to untrusted networks without controls.

### TLS

- The default dev-oriented stack does not terminate TLS inside `vectis-api`; use your **edge** or **ingress** for HTTPS in production.

### Review and fuzzing

- Run **`make fuzz-api-auth`** (or `go test -fuzz=...` on targets under `internal/api` and `internal/api/authz`) to stress **Bearer parsing**, **token hashing**, and **route→action** mapping. This complements unit tests; it is not a substitute for a full pentest.

## Internal services (gRPC)

**Registry**, **queue**, and **log** listen for **gRPC**; **API**, **worker**, **cron**, and **reconciler** are **gRPC clients** (they dial the registry and/or queue). **Optional TLS** for all of that traffic uses global **`VECTIS_GRPC_TLS_*`** variables (see [CONFIGURATION.md](CONFIGURATION.md) §Internal gRPC TLS). Standalone binaries default to **plaintext** (`VECTIS_GRPC_TLS_INSECURE=true`); **`vectis-local` bootstraps dev TLS by default** and injects those env vars into children (opt out with **`--grpc-insecure`**), including **`VECTIS_GRPC_TLS_SERVER_NAME=localhost`** so clients that resolve peers to **`127.0.0.1`** still verify the dev leaf. The **Podman kube spec** ([`deploy/podman/kube-spec.yaml`](../deploy/podman/kube-spec.yaml)) also sets **`VECTIS_GRPC_TLS_*`** on all Vectis containers via an init-generated volume (TLS **on** for that deploy path), and enables **TLS to Postgres** inside the pod (**`sslmode=verify-full`** from application containers). When **`VECTIS_GRPC_TLS_INSECURE=false`**, each binary validates required PEM paths for its role (listeners need cert/key; **api**, **worker**, **cron**, and **reconciler** need a CA bundle to verify peers). If you expose any of these ports beyond a trust zone without TLS, **assume compromise** of queue/log/registry implies ability to enqueue, observe, or disrupt work.

## Prometheus `/metrics`

**`vectis-api`** serves **`GET /metrics`** on the **same HTTP listener** as REST (see [ARCHITECTURE.md](ARCHITECTURE.md)). **`vectis-queue`**, **`vectis-worker`**, and **`vectis-log`** expose **`/metrics`** on **separate** listen ports ([CONFIGURATION.md](CONFIGURATION.md)); the **Podman kube spec** enables **HTTPS** on those three listeners via **`VECTIS_METRICS_TLS_*`**, while the API scrape target remains **HTTP** until API TLS is introduced separately. These endpoints are **not authenticated** and return **Prometheus** text (plus optional OpenMetrics). Restrict them to **trusted networks** (e.g. scrape from Prometheus inside the cluster/pod only) or block them at your edge—same practical posture as internal gRPC.

## Secrets and configuration

- **Database:** `VECTIS_DATABASE_DSN` (and related env) often contains **passwords**. Follow your platform’s **secret store** practice (Kubernetes secrets, vault agents, etc.); avoid committing DSNs to repos or logging them.
- **Environment variables** are visible to any process/user that can read the service environment on the host—**file permissions**, **container runtime**, and **orchestrator RBAC** matter.
- **SQLite files** and **queue persistence** directories hold durable state; restrict **filesystem permissions** and backups access like any sensitive data store.

## Job execution and logs

- Workers run **actions** defined in the job graph (e.g. **shell**). Treat job definitions and any **checkout** sources as **arbitrary code** capability: only let **trusted users** define jobs or trigger runs, or isolate workers accordingly (containers, separate accounts, read-only roots where possible).
- **Log streams** and **stored log files** (`vectis-log`) may contain **credentials or PII** emitted by build steps. Restrict access to log HTTP/gRPC endpoints and log storage paths.

## Roadmap (product)

Further **AuthN/AuthZ** (OIDC, LDAP, internal gRPC tokens/mTLS) is tracked in [PLANNING.md](PLANNING.md) §3 Milestone B. RBAC and local auth are shipped; **Edge controls** remain important even when API auth is enabled.

## Reporting security issues

If you discover a **security vulnerability** in this project, please report it **privately** to the maintainers (repository contact or GitHub **Security Advisories**, if enabled) before public disclosure.

## Related documentation

| Topic | Document |
| --- | --- |
| Architecture and protocols | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Environment variables and discovery | [CONFIGURATION.md](CONFIGURATION.md) |
| Failure and dependency behavior | [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md) |
| Design decisions | [adr/README.md](adr/README.md) |
