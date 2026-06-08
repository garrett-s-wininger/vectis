# Security

This page describes Vectis' current security posture for self-hosted operators. It is not a penetration test report or formal threat model. It is a practical map of what Vectis protects today, what the platform expects you to protect, and which controls matter most before exposing a deployment beyond a local machine.

For outage behavior, see [Failure Domains](./failure-domains.md). For environment variables, ports, TLS files, and secret placement, see [Configuration](../operating/configuration.md).

## Quick Posture

| Surface | Current behavior | Operator baseline |
| --- | --- | --- |
| HTTP API | Authentication is off by default for local and development use. Local users, API tokens, RBAC, rate limits, audit logging, and direct HTTPS are available when configured. | Enable API auth outside throwaway local use, serve or terminate HTTPS, and restrict who can reach the API. |
| Internal gRPC | Queue, registry, log, and worker-control traffic can use optional TLS/mTLS. Standalone binaries default to plaintext unless configured. | Keep internal ports private. Use TLS/mTLS and service identity allowlists on shared networks. |
| Cell ingress HTTP | Cell ingress is an internal execution submission surface. It uses the internal TLS/mTLS material when exposed off-loopback and can require exact producer SPIFFE identities for execution submissions. It does not perform end-user authentication or RBAC; global producers authorize work before dispatch. | Keep cell ingress private, reachable only by approved global producers, and use mTLS plus producer identity allowlists for non-loopback ingress endpoints. |
| Service authorization | Internal gRPC listeners can enforce role-level exact SPIFFE URI SAN allowlists when configured. This is listener authorization, not a full per-RPC policy language. | Configure expected service identities and still treat network reachability to queue, registry, log, and worker-control paths as sensitive. |
| Metrics | API metrics use the API route auth policy when API auth is enabled. Dedicated service metrics listeners are unauthenticated and bind to localhost by default. Some deployments enable TLS for dedicated metrics listeners. | Scrape metrics from a trusted network, require API auth for API metrics, and only set dedicated metrics `--metrics-host` values for trusted scrape networks. |
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

Until setup completes, health and `/api/v1/setup/*` remain available. Other API routes, including API metrics and diagnostics, return `503 setup_required`. After setup, clients use:

```http
Authorization: Bearer <api_token>
```

Vectis stores local user passwords with bcrypt. API tokens are generated from 32 random bytes and stored as SHA-256 hashes for lookup. Login sessions are separate expiring cache entries keyed by the same SHA-256 token hash format; with the default database cache backend, API replicas share session state through SQL. Browser logins receive a host-only `Secure` HttpOnly `__Host-vectis_session` cookie plus a host-only `Secure` readable `__Host-vectis_csrf` cookie/response field, and the raw session token is omitted from the JSON response unless `return_token` is requested for a non-browser client. Vectis does not issue or accept unprefixed fallback browser session cookies, so browser cookie auth requires HTTPS or local TLS. Behind an HTTPS edge or load balancer, set `VECTIS_API_SESSION_COOKIE_SECURE=true` explicitly as the browser-facing HTTPS assertion; trusted proxy CIDRs let Vectis trust `X-Forwarded-Proto` / `Forwarded: proto=https` for request-aware behavior, but they do not satisfy startup secure-cookie validation because direct HTTP browser logins cannot persist `Secure` cookies. Internal gRPC or metrics TLS does not make browser-facing API cookies usable over HTTP. The API rejects untrusted Host headers before route handling; configure `VECTIS_API_ALLOWED_HOSTS` for external DNS names. The docs server applies the same style of Host allowlist through `VECTIS_DOCS_ALLOWED_HOSTS` / `--allowed-host` when published behind an external hostname. Duplicate or malformed browser metadata headers such as `Origin`, CORS preflight headers, and `Sec-Fetch-*` are rejected before CORS and Fetch Metadata checks. Browser requests with Fetch Metadata must use API request shapes such as `Sec-Fetch-Mode: cors` or `same-origin` with `Sec-Fetch-Dest: empty`; document navigations and subresource loads are rejected before route handling. Browser-marked cross-site requests without `Origin` are rejected before route handling; cross-site requests with `Origin` must pass CORS. Cookie-authenticated requests with `Sec-Fetch-Site: cross-site` are rejected, including safe reads. Unsafe cookie-authenticated requests must send `X-CSRF-Token` and an `Origin` or `Referer` matching the browser-facing request scheme, host, and port; requests without origin metadata are rejected. Successful logout deletes server-side session state, clears the canonical Vectis session and CSRF cookies, and sends `Clear-Site-Data: "cache", "storage"` so browser-local origin data is purged without clearing unrelated domain or subdomain cookies. Password changes and user disables revoke API tokens and login sessions; re-enabling a user does not resurrect old credentials. Treat a database leak as credential exposure: rotate API tokens, clear sessions, and rotate bootstrap secrets if the database is compromised. Login sessions created by `POST /api/v1/login` default to a one-week absolute expiry and a 24-hour idle expiry.

Browser-facing API and docs responses include baseline security headers: `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `Permissions-Policy`, `Cross-Origin-Opener-Policy: same-origin`, `Cross-Origin-Resource-Policy: same-origin`, `Cross-Origin-Embedder-Policy: require-corp`, `Origin-Agent-Cluster: ?1`, `X-Permitted-Cross-Domain-Policies: none`, `X-Download-Options: noopen`, and a Content Security Policy. API responses use a strict `default-src 'none'` policy; docs use a static-site policy that allows same-origin assets and forbids inline scripts, inline styles, and inline event handlers. Direct HTTPS requests receive `Strict-Transport-Security`; API responses behind a TLS-terminating proxy also receive it when the proxy is trusted and forwards `https` as the original scheme. API HSTS defaults to a one-year max-age and can opt into `includeSubDomains` and `preload`; preload is rejected unless the policy also includes subdomains and keeps at least a one-year max-age. Protected API routes default to `Cache-Control: no-store`; setup and login routes explicitly set `no-store` even when reachable before normal authentication. Event-streaming routes set `Cache-Control: no-cache` in their handlers. The docs server is read-only and rejects untrusted Host headers, non-`GET`/`HEAD` methods, and request bodies before static file handling. Docs static file serving also disables directory listings, hides dotfile paths, and prevents symlinks in a configured local docs directory from escaping the docs root.

Browser cross-origin API access is closed by default. Same-origin `Origin` headers, matching the browser-facing scheme, host, and port, are allowed without CORS response headers. Cross-origin browser requests are rejected before route handling unless the operator allows the exact origin with `api.cors.allowed_origins` / `VECTIS_API_CORS_ALLOWED_ORIGINS`; only exact `https://` origins are accepted for non-local frontends. Exact `http://` origins are limited to loopback or localhost development. Wildcard credentialed CORS is not supported.

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

When `VECTIS_GRPC_TLS_INSECURE=false`, listeners need certificate and key files, and gRPC clients need a CA bundle. mTLS verifies peer certificates when a client CA is configured. Service identity allowlists can then require the client certificate leaf to contain an exact `spiffe://` URI SAN for each protected listener role: registry, queue, log, worker-control, and cell-ingress execution producers. Empty allowlists leave this role-level authorization layer disabled.

For the service identity matrix, private port guidance, and checklist for new internal RPCs, see [Internal Service Trust](./internal-service-trust.md).

## Cell Ingress

`vectis-cell-ingress` accepts routed executions on `POST /cell/v1/executions` and hands them to the cell-local queue after durable acceptance in the cell database. It is not a public API and does not replace the global API's authentication, RBAC, audit, and namespace checks.

Restrict cell ingress reachability to trusted global producers such as `vectis-api`, `vectis-cron`, and `vectis-reconciler`. Keep it on private networks or behind internal load balancers, service mesh policy, firewall rules, or platform network policy. Cell ingress uses the same `VECTIS_GRPC_TLS_*` material as internal gRPC for HTTP mTLS: non-loopback ingress requires `VECTIS_GRPC_TLS_INSECURE=false`, a server cert/key, a client CA on `vectis-cell-ingress`, and client cert/key material on producers. Set `VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES` to require exact producer SPIFFE URI SANs on execution submissions. Cell ingress only accepts trusted Host headers, bodyless `GET`/`HEAD` health checks, and mTLS-protected JSON `POST /cell/v1/executions` submissions capped at 2 MiB; unknown routes, method mismatches, unsafe request targets, wrong media types, and unexpected request bodies return JSON errors. Its Host allowlist defaults to the bind host, loopback, and the local cell's static ingress endpoint Host. Cell ingress responses use the shared baseline security headers and `no-store` so routed execution state and errors are not cached by intermediaries. Metrics endpoints only serve bodyless `GET`/`HEAD /metrics` with trusted Host headers and reject untrusted Host headers or unsafe request targets before the Prometheus handler runs. They also use baseline security headers plus `no-store`, but they can still reveal operational state. Health and metrics endpoints should follow the same private-network rule.

For the multi-cell routing and fan-in shape, see [Multi-Cell Operation](../operating/multi-cell.md).

## Metrics

`vectis-api` serves `/metrics` on the same HTTP listener as the REST API. When API authentication is enabled, API metrics require the same authenticated admin posture as other operational API routes. `vectis-queue`, `vectis-worker`, `vectis-log`, `vectis-log-forwarder`, `vectis-reconciler`, `vectis-catalog`, and `vectis-cell-ingress` use dedicated metrics listeners by default.

Dedicated service metrics endpoints are not authenticated and bind to `localhost` by default. Set a service's `--metrics-host` flag or `VECTIS_<SERVICE>_METRICS_HOST` only when a trusted scraper needs off-host access. The Podman reference deployment enables HTTPS on the dedicated queue, worker, and log metrics listeners through `VECTIS_METRICS_TLS_*`; standalone log-forwarders use the same metrics TLS settings when their metrics listener is enabled. API metrics remain on the API listener, so they use the same direct HTTPS settings as the REST API when API TLS is configured.

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

API web-security rejections are also observable. Host allowlist failures, denied CORS origins and preflights, CSRF failures, request-target rejects, query-parameter rejects, method override header rejects, method rejects, media-type rejects, body-policy rejects, and rate-limit rejects emit sanitized warning logs and increment `vectis_api_security_rejections_total` with low-cardinality `reason`, `route`, and `status` labels. Credential headers and CSRF token values are not logged.

The API also has bounded request sizes and token parsing limits:

| Limit | Purpose |
| --- | --- |
| Route-declared request body policy | Rejects request bodies on routes that do not explicitly accept them and enforces JSON `Content-Type` on JSON routes. |
| Route-declared query policy | Rejects malformed, repeated, or undeclared query parameters before handlers silently ignore or partially interpret them. |
| JSON body caps | Limits memory and parsing work on hostile requests; job-definition routes have a larger dedicated cap. |
| HTTP request header cap | Bounds parser memory for oversized header attacks across API, docs, cell ingress, and metrics HTTP servers. |
| Trusted Host header allowlist | Reduces Host-header confusion and DNS-rebinding risk for API, docs, cell ingress, and dedicated metrics requests. |
| Response `Accept` negotiation | Rejects requests for response media types a route does not produce. API JSON, SSE, API metrics, dedicated metrics, and cell ingress JSON responses each declare their allowed media types; no-body health probe OK responses explicitly opt out. |
| Strict request-target guard | Rejects absolute-form proxy targets, `OPTIONS *`, escaped path text, duplicate slash paths, and dot segments across API, docs, cell ingress, and metrics HTTP servers. API routes also reject trailing slash aliases. |
| Strict route and method guard | Returns JSON API errors for unknown routes or method mismatches, preserves `Allow`, rejects TRACE/TRACK/CONNECT, and rejects method override headers. |
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
6. Configure service identity allowlists for expected internal SPIFFE IDs.
7. Store database DSNs, API tokens, bootstrap tokens, and deploy TLS material in a secret manager.
8. Restrict who can define jobs, trigger jobs, and read logs.
9. Protect SQL, SQLite, queue, log, and backup storage like sensitive application data.
10. Monitor audit drops, audit flush failures, auth failures, queue health, and service readiness.

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
