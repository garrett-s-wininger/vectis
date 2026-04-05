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

- Built-in **authentication** or **authorization** on REST.
- **mTLS** or token verification between internal gRPC peers (connections are typically **plaintext** unless you wrap transport externally).
- **Encryption at rest** for SQLite files, queue WALs, or log storage (relies on **disk** or **volume** encryption from the platform).
- **Secret scanning** of log streams or job output.

## HTTP API

- There is **no application-level authentication** on the REST surface documented in [ARCHITECTURE.md](ARCHITECTURE.md). Any client that can reach the API can use exposed routes (create/update/delete jobs, trigger runs, list runs, streams where implemented).
- **Trust boundary:** Treat the API as **exposed only to networks and principals you fully trust**, or place **TLS termination, authentication, and authorization** in front of it (reverse proxy, API gateway, mesh, private VPC, etc.).
- **TLS:** The default dev-oriented stack does not terminate TLS inside `vectis-api`; use your **edge** or **ingress** for HTTPS in production.

## Internal services (gRPC)

Registry, queue, and log communicate over **gRPC** between processes. The codebase does not ship a standard **credentials** or **TLS** story for these calls—assume **the same trust zone** as your workers and API (e.g. private network). If you expose any of these ports beyond that zone, **assume compromise** of queue/log/registry implies ability to enqueue, observe, or disrupt work.

## Prometheus `/metrics`

**`vectis-api`** serves **`GET /metrics`** on the **same HTTP listener** as REST (see [ARCHITECTURE.md](ARCHITECTURE.md)). **`vectis-queue`**, **`vectis-worker`**, and **`vectis-log`** expose **`/metrics`** on **separate** listen ports ([CONFIGURATION.md](CONFIGURATION.md)). These endpoints are **not authenticated** and return **Prometheus** text (plus optional OpenMetrics). Restrict them to **trusted networks** (e.g. scrape from Prometheus inside the cluster/pod only) or block them at your edge—same practical posture as internal gRPC.

## Secrets and configuration

- **Database:** `VECTIS_DATABASE_DSN` (and related env) often contains **passwords**. Follow your platform’s **secret store** practice (Kubernetes secrets, vault agents, etc.); avoid committing DSNs to repos or logging them.
- **Environment variables** are visible to any process/user that can read the service environment on the host—**file permissions**, **container runtime**, and **orchestrator RBAC** matter.
- **SQLite files** and **queue persistence** directories hold durable state; restrict **filesystem permissions** and backups access like any sensitive data store.

## Job execution and logs

- Workers run **actions** defined in the job graph (e.g. **shell**). Treat job definitions and any **checkout** sources as **arbitrary code** capability: only let **trusted users** define jobs or trigger runs, or isolate workers accordingly (containers, separate accounts, read-only roots where possible).
- **Log streams** and **stored log files** (`vectis-log`) may contain **credentials or PII** emitted by build steps. Restrict access to log HTTP/gRPC endpoints and log storage paths.

## Roadmap (product)

Authentication, authorization, and related hardening for APIs beyond trusted networks are **planned** at a high level ([PLANNING.md](PLANNING.md) §3 Milestone B). Until shipped, **edge controls** remain the primary mitigation.

## Reporting security issues

If you discover a **security vulnerability** in this project, please report it **privately** to the maintainers (repository contact or GitHub **Security Advisories**, if enabled) before public disclosure.

## Related documentation

| Topic | Document |
| --- | --- |
| Architecture and protocols | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Environment variables and discovery | [CONFIGURATION.md](CONFIGURATION.md) |
| Failure and dependency behavior | [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md) |
| Design decisions | [adr/README.md](adr/README.md) |
