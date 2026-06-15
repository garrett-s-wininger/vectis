# API Reference

The Vectis HTTP API is for scripts, dashboards, operators, and integrations that need to talk to the API server directly. If you are running jobs by hand, start with the [CLI Guide](./cli-guide.md); the CLI uses the same API paths and is usually the friendlier interface.

This page covers the shipped v1 REST surface. gRPC contracts live in `api/proto/`, and generated Go lives in `api/gen/go/`. Compatibility rules for REST, gRPC, CLI JSON, configuration, and schema changes are in [Compatibility](../concepts/compatibility.md).

For client generators and gateway policy tooling, use the machine-readable [OpenAPI Specification](./openapi-specification.md). For the complete machine-readable error `code` catalog, see [API Error Code Reference](./api-error-code-reference.md).

For the role matrix and token-scope behavior behind the route `Auth action` column, see [Authorization Reference](../operating/reference/authorization-reference.md).

## When To Use The API

Use the HTTP API when you want to:

- submit or trigger jobs from another system
- build a dashboard around runs, logs, health, or queue state
- manage users, namespaces, tokens, or role bindings from automation
- write smoke tests that check API readiness before deploying workers

For local experimentation, `./bin/vectis-cli jobs run <file> --follow` is quicker. For repeatable automation, the API gives you explicit status codes, error codes, and retry behavior.

## Base URL

A local `vectis-local` stack exposes the API at:

```text
http://localhost:8080
```

If you have initialized and trusted the generated local CA with `vectis-local init` and `vectis-local install-cert`, `vectis-local` may advertise `https://localhost:8080` instead. Examples below use the HTTP local default. Replace it with the URL printed by your local stack or with your deployed API URL.

## Common Flows

### Run A Job Definition Once

Use an ephemeral run when the definition does not need to be stored first:

```sh
curl -sS \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  --data-binary @examples/sequenced.json \
  http://localhost:8080/api/v1/jobs/run
```

The response includes the run ID:

```json
{
  "id": "2b196bc5-0f7f-47e7-8fb1-2e4f6db8b6f0",
  "run_id": "8018d8f0-1bf7-488a-9375-3c4d4cd1ff7c"
}
```

Then inspect or stream that run:

```sh
curl -sS http://localhost:8080/api/v1/runs/<run-id>
curl -N http://localhost:8080/api/v1/runs/<run-id>/logs
```

### Use A Source-Defined Job

When job definitions live in a registered source repository, use the jobs API with `repository_id` to list, inspect, trigger, and watch them:

```sh
curl -sS 'http://localhost:8080/api/v1/jobs?repository_id=vectis-local&ref=main'
curl -sS 'http://localhost:8080/api/v1/jobs/build?repository_id=vectis-local&ref=main'
```

For managed repositories with `authoring_mode: "local_commit"`, the same jobs API can create, update, and delete source definitions by committing to the repository checkout:

```sh
curl -sS \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{"repository_id":"vectis-local","job_id":"build","branch":"main","message":"Add build job","job":{"root":{"id":"root","uses":"builtins/shell","with":{"command":"make test"}}}}' \
  http://localhost:8080/api/v1/jobs

curl -sS \
  -X PUT \
  -H 'Content-Type: application/json' \
  -d '{"repository_id":"vectis-local","branch":"main","message":"Update build job","job":{"root":{"id":"root","uses":"builtins/shell","with":{"command":"make test-quick"}}}}' \
  http://localhost:8080/api/v1/jobs/build

curl -sS \
  -X DELETE \
  'http://localhost:8080/api/v1/jobs/build?repository_id=vectis-local&branch=main&message=Delete%20build%20job'
```

Then trigger the job by job ID:

```sh
curl -sS \
  -X POST \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"repository_id":"vectis-local","ref":"main"}' \
  http://localhost:8080/api/v1/jobs/trigger/build
```

Source-backed triggers create a durable definition snapshot, run row, and source provenance. The trigger body may include `path` to override the default `.vectis/jobs/<job_id>.json` layout, and may include `cell_id` or `target_cell_id` to select one execution cell. List or stream runs for the source-backed job with:

```sh
curl -sS 'http://localhost:8080/api/v1/jobs/build/runs?repository_id=vectis-local'
curl -N 'http://localhost:8080/api/v1/sse/jobs/build/runs?repository_id=vectis-local'
```

`POST /api/v1/jobs/run` and `POST /api/v1/jobs/trigger/{id}` accept an optional `cell_id` field to select one target execution cell. `target_cell_id` is accepted as an alias. If omitted, Vectis uses the API process's configured cell.

### Replay A Completed Run

Replay creates a fresh run from the source run's captured definition version. It does not reuse or overwrite the source run ID.

```sh
curl -sS \
  -X POST \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"cell_id":"pdx-b"}' \
  http://localhost:8080/api/v1/runs/<run-id>/replay
```

If `cell_id` is omitted, Vectis targets the source run's owning cell. Replay accepts only one target cell; use a new replay request for a different target. Queued and running source runs cannot be replayed.

### Check Whether The System Is Ready

Use the health endpoints for process-level checks:

```sh
curl -f http://localhost:8080/health/live
curl -f http://localhost:8080/health/ready
```

Use the v1 diagnostic endpoints when you need to understand why a stack is unhealthy:

```sh
curl -sS http://localhost:8080/api/v1/schema/status
curl -sS http://localhost:8080/api/v1/queue/backlog
curl -sS http://localhost:8080/api/v1/log/reachable
```

## Request Conventions

JSON routes expect `Content-Type: application/json`. Job create and run routes accept either the job definition as the whole body or, for namespace selection, a wrapper:

```json
{
  "namespace": "/team-a",
  "job": {
    "id": "sequenced-job",
    "root": {
      "id": "root",
      "uses": "builtins/sequence",
      "ports": {
        "steps": {
          "nodes": []
        }
      }
    }
  }
}
```

Ephemeral runs do not require a top-level job `id`. Reusable source-backed jobs require an `id` unless the client supplies a job ID separately. Job definition rules are documented in [Job Definition Validation](./job-validation.md).

Node inputs can also be bound from earlier node outputs in the same local execution scope with `inputs.<field>.from.node` and `inputs.<field>.from.output`; static `with.<field>` and bound `inputs.<field>` are mutually exclusive.

## Authentication

When `api.auth.enabled=false`, API routes are accepted without bearer tokens. When auth is enabled, non-browser clients send:

```http
Authorization: Bearer <api_token>
```

Health endpoints and `POST /api/v1/login` are public. Setup routes use setup-specific authorization while the first admin is being created. Diagnostics, API metrics, and data routes authorize the action listed in the route table below; namespace-scoped resources are hidden with `404` when the caller is not allowed to see that namespace. Browser-facing requests must use a trusted `Host` value from the API host allowlist.

`POST /api/v1/login` creates an expiring server-side session. Browser clients receive a `Secure` HttpOnly `__Host-vectis_session` cookie plus a `Secure` readable `__Host-vectis_csrf` cookie and `csrf_token` response field. Cookie-authenticated browser login requires HTTPS or local TLS because Vectis does not issue or accept unprefixed fallback session cookies. Browser-marked cross-site requests without `Origin` are rejected before route handling; cross-site requests with `Origin` must pass CORS. Browser requests with Fetch Metadata must use API request shapes such as `Sec-Fetch-Mode: cors` or `same-origin` and `Sec-Fetch-Dest: empty`; document navigations and subresource loads are rejected. Cookie-authenticated requests carrying `Sec-Fetch-Site: cross-site` are rejected even when other metadata is present. Unsafe cookie-authenticated requests must copy the CSRF token into `X-CSRF-Token` and include an `Origin` or `Referer` matching the browser-facing scheme, host, and port; requests without origin metadata are rejected.

Password changes and user disables revoke that user's API tokens and login sessions. Re-enabling a disabled user does not resurrect old credentials.

Non-browser clients that need a bearer session token can request one explicitly:

```sh
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"<password>","return_token":true}' \
  http://localhost:8080/api/v1/login
```

Then send the returned `token` on later requests:

```http
Authorization: Bearer <api_token>
```

## Error Envelopes

General API errors use this stable v1 envelope:

```json
{
  "code": "invalid_request_body",
  "message": "invalid request body",
  "details": {"field": "optional structured details"}
}
```

All error responses use `Content-Type: application/json; charset=utf-8` and `X-Content-Type-Options: nosniff`. Browser-facing API responses also include baseline security headers such as `X-Frame-Options`, `Referrer-Policy`, `Permissions-Policy`, `Cross-Origin-Opener-Policy`, `Cross-Origin-Resource-Policy`, `Cross-Origin-Embedder-Policy`, `Origin-Agent-Cluster`, and a strict Content Security Policy. Protected API responses default to `Cache-Control: no-store`; event streams use `Cache-Control: no-cache`. Direct HTTPS requests include the configured `Strict-Transport-Security` policy; responses behind a trusted TLS-terminating proxy include it when the proxy forwards the original scheme as HTTPS. `401` responses add `WWW-Authenticate: Bearer`.

CORS is closed unless the operator configures exact allowed origins. Same-origin `Origin` headers, matching the browser-facing scheme, host, and port, are allowed without CORS response headers. Credentialed browser CORS never uses `*`; non-local browser origins must use HTTPS, while HTTP origins are limited to loopback or localhost development. Disallowed cross-origin actual requests and preflights are rejected before route handling. Preflights are accepted only for allowed origins, methods, and request headers.

Host header validation is enabled by default. Requests with untrusted, wildcard, URL-shaped, or otherwise invalid `Host` values are rejected before route handling.

Security-control rejections for Host validation, CORS checks, Fetch Metadata checks, CSRF checks, request-target checks, request-header checks, query-parameter checks, method override headers, method checks, response `Accept` negotiation, media-type checks, request body policy, and rate limits are logged with sanitized fields and counted by the `vectis_api_security_rejections_total` metric.

The API server caps request headers at 32 KiB. Requests above that parser limit are rejected by the HTTP server before route handling. Malformed HTTP framing, such as conflicting `Content-Length` headers or unsupported `Transfer-Encoding` values, is rejected by the HTTP parser before API route handling.

Before CORS and Fetch Metadata checks read browser-supplied metadata, the API rejects duplicate or malformed `Origin`, `Access-Control-Request-Method`, `Access-Control-Request-Headers`, and `Sec-Fetch-*` headers with `invalid_request_header`.

Routes reject request bodies unless the route explicitly accepts a JSON body. JSON routes enforce `application/json` and a per-route body cap before parsing; job-definition routes have a larger cap than auth, user, token, namespace, and control routes. Optional JSON routes allow an absent body without `Content-Type`, but any present body must use JSON.

Requests must use origin-form, unescaped, canonical API paths. Absolute-form proxy request targets, `OPTIONS *`, percent-encoded path text, duplicate slash paths, dot segments, and trailing slash aliases return `invalid_request_target`. Unknown routes return `route_not_found`. Method mismatches return `method_not_allowed` with an `Allow` header; TRACE, TRACK, and CONNECT are always rejected. Method override headers such as `X-HTTP-Method`, `X-HTTP-Method-Override`, and `X-Method-Override` return `method_override_forbidden`.

Routes also validate selected request headers before handler logic. Duplicate singleton security headers such as `Authorization`, `Content-Type`, `X-CSRF-Token`, `Origin`, `Referer`, and `Idempotency-Key` return `invalid_request_header`. Routes that do not document idempotency support reject `Idempotency-Key`.

Routes also validate query parameters before handler logic. Query strings must parse cleanly; keys must be declared for the route; and repeated keys are rejected. Routes with no declared query policy reject all query parameters with `invalid_query_parameter`.

Routes also validate the response `Accept` header. JSON routes accept an absent `Accept`, `application/json`, compatible wildcards such as `application/*` or `*/*`, and weighted lists that include JSON. SSE routes require `text/event-stream` or a compatible wildcard. Metrics routes accept Prometheus text or OpenMetrics response types. Health probe OK responses carry no representation and allow any `Accept` value. Incompatible response negotiation returns `not_acceptable`.

The `code` field is intended for clients and scripts. The `message` field is human-readable and may become clearer over time without changing the machine meaning. `details` is optional structured data whose shape depends on `code`; clients should ignore unknown detail keys.

Common status meanings:

| Status | Meaning |
| --- | --- |
| `400` | Invalid JSON, invalid IDs, missing required fields, unexpected request bodies, malformed or unsupported request headers or query parameters, forbidden method override headers, or invalid state transition input. |
| `401` | Missing, malformed, expired, or invalid bearer credentials. |
| `403` | Authenticated principal is not allowed to perform a visible global action. |
| `404` | Resource is absent or hidden by namespace authorization. |
| `406` | Route cannot produce any media type allowed by the request `Accept` header. |
| `409` | Resource conflict, duplicate create, idempotency conflict, or invalid run repair state. |
| `413` | Request body exceeds the route limit. |
| `415` | JSON route received a non-JSON content type. |
| `429` | The configured rate-limit backend rejected the request. `Retry-After` is set. |
| `500` | Unexpected server error. |
| `503` | Database, auth persistence, queue, or setup state is not ready. |

Common v1 error codes:

This table highlights high-frequency codes. The full catalog, including subsystem-specific job, run, artifact, log, catalog, and RBAC codes, lives in [API Error Code Reference](./api-error-code-reference.md).

| Code | Typical status | Meaning |
| --- | --- | --- |
| `invalid_request_body` | `400` | JSON could not be decoded or did not match the expected request shape. |
| `invalid_request_header` | `400` | The request supplied a duplicated or malformed security header, malformed idempotency key, or idempotency key on a route that does not accept it. |
| `invalid_host_header` | `400` | The request `Host` header is invalid or not in the API host allowlist. |
| `invalid_request_target` | `400` | The request target is not an origin-form, unescaped, canonical API path. |
| `invalid_query_parameter` | `400` | The request supplied a malformed, repeated, or unsupported query parameter. |
| `method_override_forbidden` | `400` | The request supplied a method override header, which Vectis does not honor. |
| `authentication_required` | `401` | Missing, malformed, expired, or invalid bearer credentials. |
| `authorization_denied` | `403` | Authenticated principal is not allowed to perform the requested visible action. |
| `not_acceptable` | `406` | The route cannot produce any media type allowed by the request `Accept` header. |
| `auth_unavailable` | `503` | Authentication persistence or configuration is not available. |
| `setup_required` | `503` | Initial setup must be completed before using the requested route. |
| `bootstrap_not_configured` | `503` | Initial setup needs a sufficiently long configured bootstrap token. |
| `invalid_bootstrap_token` | `401` | The supplied setup bootstrap token does not match the server configuration. |
| `cors_origin_forbidden` | `403` | A cross-origin request came from an origin that is not in the CORS allowlist. |
| `unsupported_media_type` | `415` | A JSON route received a non-JSON `Content-Type`. |
| `request_body_not_allowed` | `400` | The route does not accept a request body. |
| `request_body_too_large` | `413` | The request body exceeded the route limit. |
| `route_not_found` | `404` | No API route matched the request path. |
| `method_not_allowed` | `405` | The route exists but does not accept the request method; `Allow` is set. |
| `database_unavailable` | `503` | The configured SQL database is temporarily unavailable. |
| `queue_not_ready` | `503` | The API cannot currently hand work to the queue. |
| `server_shutting_down` | `503` | The API has started shutdown drain and should not receive new requests. |
| `fetch_metadata_forbidden` | `403` | A browser-marked request used forbidden Fetch Metadata, such as cross-site without `Origin`, a document navigation, a subresource destination, or cookie-authenticated cross-site metadata. |
| `csrf_origin_forbidden` | `403` | A cookie-authenticated unsafe request omitted origin metadata or came from a mismatched origin. |
| `csrf_token_required` | `403` | A cookie-authenticated unsafe request is missing a valid `X-CSRF-Token`. |
| `rate_limit_exceeded` | `429` | The request exceeded the configured rate limit; `Retry-After` is set. |
| `idempotency_key_reused` | `409` | The same idempotency key was reused with a different request body or target. |
| `idempotency_in_progress` | `409` | The original idempotent request has not completed yet. |
| `validation_failed` | `400` | Job definition semantic validation failed; see [Job Definition Validation](./job-validation.md). |
| `not_found` / resource-specific `*_not_found` | `404` | Resource is absent or hidden by namespace authorization. |
| `internal_error` | `500` | Unexpected server error. |

## Endpoint Families

The table below is the exact route inventory. Read it by family:

| Family | Use it for |
| --- | --- |
| Health and diagnostics | Process health plus authenticated schema state, queue pressure, log reachability, and Prometheus metrics. |
| Jobs | List, inspect, trigger, and watch reusable job definitions from source repositories. |
| Source repositories | Register, sync, browse, and administer Git checkouts that supply source-backed job definitions. |
| Ephemeral runs | Submit one-off job definitions without storing them first. |
| Runs, logs, and artifacts | Inspect run status, list/download artifacts, stream logs, cancel active work, or repair orphaned runs. |
| Setup and auth | First-admin setup, login, token lifecycle, password changes. |
| Users and namespaces | User administration, namespace tree management, and namespace role bindings. |

## Response Behavior

List routes use `limit` and `cursor` query parameters where implemented. Paginated responses include `next_cursor` when another page is available. Omit `cursor` to read the first page.

`GET /api/v1/jobs?repository_id=<repo>` lists triggerable source-backed jobs from a registered source repository. It accepts `ref`, `path`, `limit`, and path `cursor`. `repository_id` is required for reusable jobs. Source-backed job list, definition, authoring, and trigger responses include `repository_sync` so clients can see whether the local checkout's last recorded sync succeeded, failed, is still running, or has never completed.

`GET /api/v1/jobs/{id}?repository_id=<repo>` resolves one source-backed job definition by job ID, optional `ref`, and optional `path` override. `repository_id` is required.

`POST /api/v1/jobs` commits a source-backed job definition through repository authoring. The JSON body must include `repository_id` and accepts `job_id`, `ref`/`branch`, `path`, `message`, `expected_head`, and `job`. A duplicate source path returns `409 source_definition_already_exists`; use `PUT /api/v1/jobs/{id}` to update it or choose another `job_id`/`path`.

`PUT /api/v1/jobs/{id}` commits a source-backed definition update and accepts the same authoring fields as source create. The body or query must include `repository_id`. `DELETE /api/v1/jobs/{id}?repository_id=<repo>` commits a source-backed definition deletion and returns the delete commit as source provenance; optional `ref`/`branch`, `path`, `message`, and `expected_head` query parameters control the delete commit. Authoring requires a managed repository with `authoring_mode: "local_commit"`; otherwise these routes return `409 source_authoring_unavailable`. When `expected_head` no longer matches the branch head, writes return `409 source_conflict`; refresh the branch head and retry with an updated `expected_head`.

`GET /api/v1/jobs/{id}/runs?repository_id=<repo>` is backed by the global run catalog and scopes results to recorded source provenance for that repository and job ID. It returns summary run records from `job_runs`, including `owning_cell`, and accepts `after_index`, `since`, and `cell_id`/`owning_cell` filters.

Run submission routes can target one cell with `cell_id`/`target_cell_id`. Replay defaults to the source run's owning cell and can override it with one `cell_id`/`target_cell_id`. Non-local targets require the API to be configured with matching private cell ingress endpoints.

Run list/detail responses include audit metadata such as `definition_version`, `definition_hash`, `owning_cell`, trigger invocation fields, requested cells, and `execution_payload_hash`. When a run uses a source-backed definition snapshot, these responses also include `source` provenance with repository id, requested ref, resolved commit, definition path, and blob SHA. Run detail also includes `dispatch_summary`, `dispatch_events`, and a compact `task_completion` summary when task records exist. The dispatch summary groups dispatch events by producer source so tooling can compare API, cron, and reconciler handoff attempts before reading the raw chronological event trail. Queued run detail may include `next_action` with `task_completion_pending` when task-level progress explains what the run is waiting on, or `task_continuation_pending` when a child task execution is waiting for redispatch; orphaned task-run detail may include `task_finalization_repair_pending` when the stored task summary can already reduce to a terminal state. Failed run detail may include `next_action=security_gate_failed` and `latest_failed_security_event` when the newest failed worker-controlled SVID or secret-resolution gate explains the failure. `GET /api/v1/runs/{id}/definition` returns the immutable job definition snapshot captured for that run, with the run id, job id, definition version/hash, and source provenance when available; it does not reread the source repository. `GET /api/v1/runs/{id}/tasks` includes task-attempt execution identity, execution status, execution lease owner/expiry when an attempt is owned by a worker, and redacted `security_events` for worker-controlled SVID checks and secret resolution. Security events expose outcome, reason, provider kind, and counts only; claim tokens, secret refs, secret values, delivery paths, SVIDs, and private key material are not exposed. In multi-cell deployments, cell catalog fan-in copies these security events into the global run catalog with the same redacted shape. `GET /api/v1/runs/{id}/artifacts` lists artifact manifests recorded by the run and accepts optional `task_id`, `task_attempt_id`, and `execution_id` filters. `GET /api/v1/runs/{id}/artifacts/{name}/download` streams the exact blob bytes uploaded by the worker; Vectis does not transform, compress, or expand archives before serving them. The frozen execution payload itself is available only through the operator-scoped execution-payload route.

`POST /api/v1/jobs/run`, `POST /api/v1/jobs/trigger/{id}`, `POST /api/v1/source-repositories/{id}/jobs/{job_id}/trigger`, and `POST /api/v1/runs/{id}/replay` accept `Idempotency-Key`. Use this header when a client might retry after a timeout or dropped connection. Keys must be 1-255 visible ASCII characters and cannot contain whitespace or commas. Other routes reject the header. Retry guidance for each route family is in [Idempotency And Retries](./idempotency-and-retries.md).

Reusable job routes are source-backed and require `repository_id`. One-off runs remain available through `POST /api/v1/jobs/run`.

`vectis-api` also reconciles source repository registrations declared in `source.repositories` or `VECTIS_SOURCE_REPOSITORIES` at startup. Declarations create missing repositories and update changed checkout, authoring, default ref, credential, and enabled fields; they do not delete omitted repositories or move a repository between namespaces. Source repository responses expose `declared`, which is false when the database row is no longer present in current source repository config. Use `GET /api/v1/source/status` to inspect source-mode capabilities such as whether source repositories/schedules are configured, how many repository and schedule declarations the API currently sees, and persisted repository/schedule totals by enabled, stale, sync, and override state. Set `source.sync_configured_repositories_on_startup=true` or `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP=true` to sync enabled configured repositories during startup; this is disabled by default to avoid deployment delays for large repositories, and a failed startup sync fails API startup after persisting the repository sync error. Set `source.sync_configured_repositories_interval` or `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_INTERVAL` to a positive duration to refresh enabled configured repositories in the background, with `source.sync_configured_repositories_max_concurrency` and `source.sync_configured_repositories_failure_backoff` controlling fetch/probe concurrency and failed-repo retry pressure. Declare source cron schedules in `source.schedules` or `VECTIS_SOURCE_SCHEDULES`; each schedule uses `schedule_id` as its reconcile key and triggers directly from a repository `ref` and definition `path`, or from the default `.vectis/jobs/...` path derived from `job_id`. Use `GET /api/v1/source-schedules` or `GET /api/v1/source-repositories/{id}/schedules` to inspect the reconciled schedule rows, whether each row is still declared in current config, next run time, enabled state, configured ref/path, active override, effective ref/path, and repository sync status.

Source repository routes currently register local Git checkouts with `source_kind: "local_checkout"` and a `checkout_path`; each checkout path can be registered once. Repositories include `checkout_mode` (`external` for caller-owned checkout paths, `managed` for Vectis-owned checkout materialization), `authoring_mode` (`read_only` by default, `local_commit` for small-install local authoring, or `external_change_request` for future code-host integrations), `declared`, an `authoring` capability block, and a `sync` object with the last sync status/evidence. Clients should use `authoring.write_definitions` to decide whether source definition authoring is currently available; `authoring.reason` explains disabled, read-only, or not-yet-configured modes. When creating a managed repository, omit `checkout_path` to let Vectis derive a stable path under `source.checkout_root` (default `{{data_home}}/vectis/source-checkouts`). Sync probes external checkouts; for managed checkouts, sync clones from `canonical_url` when the checkout is missing or empty, fetches `origin` when it already exists, advances the local branch ref for the requested default ref, and persists `sync.status`, ref, commit, timestamps, and any checkout error. If `credential_ref` is set, `vectis-api` resolves it through its configured secrets resolver and supplies the resulting HTTPS token or username/password material through askpass, or SSH private key material through a temporary identity file and `GIT_SSH_COMMAND`; SSH payloads may include `known_hosts` to pin host keys, otherwise OpenSSH's normal host-key verification applies. A duplicate sync request for the same repository while one is already running returns `202` with the repository response showing `sync.status: "running"` and `Retry-After: 1`; running reservations older than `source.sync_running_timeout` (default `15m`) may be reclaimed by a later sync request. Update a repository to change its checkout path, checkout mode, authoring mode, default ref, metadata, or enabled state. Delete only unused repository registrations; a declared repository, or a repository with source schedules or recorded source provenance, returns `409` and should be removed from config or disabled instead so scheduled references, historical runs, and definition metadata remain resolvable. Check repository status to verify that the checkout path exists, is a Git work tree, whether a credential ref is configured, and that the configured default ref resolves when present. List branches with optional `prefix` and `limit` query parameters to populate ref pickers without scanning repository contents; managed repositories return fetched `origin` branches as plain branch names. Browse repository trees with `ref`, `path`, `recursive`, `limit`, and path `cursor` query parameters to choose definition paths at a pinned commit without reading file contents. Discover candidate job definitions with `ref`, `path` (default `.vectis/jobs`), `limit`, and path `cursor`; this returns JSON blob paths, blob SHAs, and sizes without loading file contents. Branch, tree, definition, and source-job responses include `truncated: true` when the response hit its limit before exhausting matching repository content; tree, definition, and source-job responses also include `next_cursor` when another request can continue from the returned path. List source repository jobs to derive triggerable `job_id`s from definition paths without reading file contents; invalid names or duplicate derived IDs are returned separately. Resolve a definition from source with `ref` (optional when the repository has `default_ref`) and `path` to preview the canonical JSON plus resolved commit and blob SHA; managed repositories also resolve fetched remote branches by plain branch name, such as `feature/build`, after sync. Read a source repository job definition with `ref` and optional `path`; without `path`, the job ID maps to the default `.vectis/jobs` layout. With `authoring_mode: "local_commit"`, create, update, and delete source job definitions through `/api/v1/jobs` plus `repository_id`; these operations create local commits and do not push to the remote. Trigger directly from a source repository with `ref`, optional `path`, and optional `cell_id`/`target_cell_id`; Vectis creates a durable definition snapshot, run row, and source provenance. For user-facing job workflows, prefer the `/api/v1/jobs` facade with `repository_id`; the `/api/v1/source-repositories/.../jobs` routes remain useful for explicit repository-scoped tooling and diagnostics. Source-backed cron schedules expose `declared`, which is false when the database row is no longer present in current source schedule config, and schedule list responses include `repository_sync` when the source repository state is available. `PATCH /api/v1/source-schedules/{schedule_id}` updates only `enabled`, so operators can stop stale schedules without changing configured source fields. `DELETE /api/v1/source-schedules/{schedule_id}` removes only source-backed schedules that are stale, disabled, and clear of active overrides; it returns `409` while the schedule is still declared, enabled, or overridden. Schedules can also carry a temporary hotfix override for `ref`, `path`, and `reason`; schedule responses expose `configured_ref`/`configured_path`, effective `ref`/`path`, and the active `override` object when one is set. Config reconciliation preserves active overrides, so clear them explicitly once the configured repository location contains the fix. List source repository job runs and stream source repository run logs to read run history scoped by repository provenance, even when non-source runs share the same job ID.

Streaming routes return `text/event-stream`. Use `curl -N`, `EventSource`, or another SSE-capable client for `GET /api/v1/sse/jobs/{id}/runs`, `GET /api/v1/sse/jobs/{id}/runs?repository_id=<repo>`, `GET /api/v1/sse/source-repositories/{id}/jobs/{job_id}/runs`, `GET /api/v1/runs/{id}/logs`, and `GET /api/v1/source-repositories/{id}/jobs/{job_id}/runs/{run_id}/logs`.

## Routes

Rate-limit categories are configured under `api.rate_limit.*`. `general`, `auth`, and `token` buckets use the configured API cache backend; the default `database` backend shares buckets across API replicas.

| Method | Path | Purpose | Auth action | Rate limit | Success |
| --- | --- | --- | --- | --- | --- |
| GET | `/health/live` | Liveness probe | Public | none | `200` empty |
| GET | `/health/ready` | Readiness probe | Public | none | `200` empty |
| GET | `/api/v1/version` | Build and cell identity info | `admin:*` | none | `200` JSON version |
| GET | `/api/v1/schema/status` | Migration schema status | `admin:*` | none | `200` JSON schema status |
| GET | `/api/v1/reconciler/heartbeat` | Reconciler last-activity signal | `admin:*` | none | `200` JSON heartbeat |
| GET | `/api/v1/audit/drops` | Audit event drop count | `admin:*` | none | `200` JSON drop count |
| GET | `/api/v1/db/pool-stats` | Database connection pool stats | `admin:*` | none | `200` JSON pool stats |
| GET | `/api/v1/queue/backlog` | Count of queued runs with per-cell counts when available | `admin:*` | none | `200` JSON backlog |
| GET | `/api/v1/reconciler/stuck-runs` | Count of stuck queued runs, pending task continuations, and pending orphaned task-finalization repairs, including per-cell counts when available | `admin:*` | none | `200` JSON stuck runs |
| GET | `/api/v1/cells/status` | Cell readiness summary, route checks, queued/stuck counts, task repair counts, and catalog counts without exposing private route URLs | `admin:*` | none | `200` JSON cell status |
| GET | `/api/v1/log/reachable` | Log service gRPC connectivity | `admin:*` | none | `200` JSON reachable |
| GET | `/api/v1/audit/flush-failures` | Audit flush failure count | `admin:*` | none | `200` JSON flush failures |
| GET | `/api/v1/cron/status` | Cron schedule count, due/claimed schedule pressure, and oldest due timestamp | `admin:*` | none | `200` JSON cron status |
| GET | `/api/v1/catalog/status` | Cell catalog inbox summary, including per-source-cell counts when available | `admin:*` | none | `200` JSON catalog status |
| GET | `/api/v1/source/status` | Source-mode capabilities, persistence wiring, declaration counts, and repository/schedule summaries | Public | none | `200` JSON source status |
| GET | `/metrics` | Prometheus metrics | `admin:*` | none | `200` metrics text |
| POST | `/api/v1/cells/{cell_id}/catalog-events` | Record a cell status event into the global catalog inbox | `run:operator` | general | `202` JSON event |
| GET | `/api/v1/source-schedules` | List source-backed cron schedules in a namespace | `job:read` | general | `200` JSON schedule list |
| PATCH | `/api/v1/source-schedules/{schedule_id}` | Enable or disable one source-backed cron schedule | `job:write` | general | `200` JSON schedule |
| DELETE | `/api/v1/source-schedules/{schedule_id}` | Delete one stale disabled source-backed cron schedule with no active override | `job:write` | general | `204` empty |
| PUT | `/api/v1/source-schedules/{schedule_id}/override` | Set a temporary source ref or path override for a source-backed cron schedule | `job:write` | general | `200` JSON schedule |
| DELETE | `/api/v1/source-schedules/{schedule_id}/override` | Clear a source-backed cron schedule override and return to configured ref/path resolution | `job:write` | general | `200` JSON schedule |
| GET | `/api/v1/source-repositories` | List registered source repositories in a namespace | `job:read` | general | `200` JSON list |
| POST | `/api/v1/source-repositories` | Register a source repository checkout | `job:write` | general | `201` JSON repository |
| GET | `/api/v1/source-repositories/{id}` | Get one source repository registration | `job:read` | general | `200` JSON repository |
| PUT | `/api/v1/source-repositories/{id}` | Update a source repository checkout registration | `job:write` | general | `200` JSON repository |
| DELETE | `/api/v1/source-repositories/{id}` | Delete an unused source repository registration without removing checkout files | `job:write` | general | `204` empty; `409` when the repository is still declared, or when source schedules or source provenance reference the repository |
| POST | `/api/v1/source-repositories/{id}/sync` | Probe or refresh a source repository checkout and persist sync status/evidence | `job:write` | general | `200` JSON repository; `202` JSON repository when already running |
| GET | `/api/v1/source-repositories/{id}/status` | Check whether a source repository checkout is usable and whether its default ref resolves | `job:read` | general | `200` JSON status |
| GET | `/api/v1/source-repositories/{id}/schedules` | List source-backed cron schedules for one repository | `job:read` | general | `200` JSON schedule list |
| GET | `/api/v1/source-repositories/{id}/refs/branches` | List branch refs from the repository checkout with optional `prefix` and `limit` query parameters | `job:read` | general | `200` JSON branches |
| GET | `/api/v1/source-repositories/{id}/tree` | List tree entries from a repository ref with optional `path`, `recursive`, and `limit` query parameters | `job:read` | general | `200` JSON tree entries |
| GET | `/api/v1/source-repositories/{id}/definitions` | Discover JSON job definition files from a repository ref without loading file contents | `job:read` | general | `200` JSON definition files |
| GET | `/api/v1/source-repositories/{id}/jobs` | List triggerable config-as-code jobs derived from repository definition paths | `job:read` | general | `200` JSON source jobs |
| POST | `/api/v1/source-repositories/{id}/definitions/resolve` | Resolve and validate one job definition from a repository ref and path without storing it | `job:read` | general | `200` JSON definition |
| GET | `/api/v1/source-repositories/{id}/jobs/{job_id}/definition` | Resolve and validate one source repository job definition by job id, optional ref, and optional path override | `job:read` | general | `200` JSON definition |
| PUT | `/api/v1/source-repositories/{id}/jobs/{job_id}/definition` | Commit a source job definition into a managed checkout | `job:write` | general | `200` JSON definition |
| GET | `/api/v1/source-repositories/{id}/jobs/{job_id}/runs` | List runs for a source repository job, scoped by recorded source provenance | `run:read` | general | `200` JSON list |
| GET | `/api/v1/source-repositories/{id}/jobs/{job_id}/runs/{run_id}/logs` | Stream logs for a source repository job run, scoped by recorded source provenance | `run:read` | general | `200` `text/event-stream` |
| POST | `/api/v1/source-repositories/{id}/jobs/{job_id}/trigger` | Start a run directly from a repository definition | `run:trigger` | general | `202` JSON run |
| GET | `/api/v1/jobs` | List source repository jobs; requires `repository_id` | `job:read` | general | `200` JSON list |
| POST | `/api/v1/jobs` | Commit a source job definition; body requires `repository_id` | `job:write` | general | `200` JSON source definition |
| GET | `/api/v1/jobs/{id}` | Resolve one source repository job; requires `repository_id` | `job:read` | general | `200` JSON definition |
| PUT | `/api/v1/jobs/{id}` | Commit a source job definition update; body or query requires `repository_id` | `job:write` | general | `200` JSON source definition |
| DELETE | `/api/v1/jobs/{id}` | Commit a source definition deletion; requires `repository_id` | `job:write` | general | `200` JSON source deletion |
| POST | `/api/v1/jobs/run` | Start an ephemeral run from JSON body, optionally targeting `cell_id` | `run:trigger` | general | `202` JSON run |
| POST | `/api/v1/jobs/trigger/{id}` | Trigger a source repository job; body requires `repository_id` | `run:trigger` | general | `202` JSON run |
| GET | `/api/v1/jobs/{id}/runs` | List global catalog runs for one source-backed job; requires `repository_id` | `run:read` | general | `200` JSON list |
| GET | `/api/v1/sse/jobs/{id}/runs` | Stream run events for one source-backed job; requires `repository_id` | `run:read` | general | `200` `text/event-stream` |
| GET | `/api/v1/sse/source-repositories/{id}/jobs/{job_id}/runs` | Stream run events for a source repository job | `run:read` | general | `200` `text/event-stream` |
| GET | `/api/v1/runs/{id}` | Get one run, including audit metadata, dispatch summary/events, and task completion summary when present | `run:read` | general | `200` JSON run |
| GET | `/api/v1/runs/{id}/definition` | Get the frozen job definition snapshot for one run | `run:read` | general | `200` JSON definition |
| GET | `/api/v1/runs/{id}/tasks` | List task graph nodes and task attempts for one run | `run:read` | general | `200` JSON list |
| GET | `/api/v1/runs/{id}/artifacts` | List artifact manifests for one run, optionally filtering by `task_id`, `task_attempt_id`, or `execution_id` | `run:read` | general | `200` JSON list |
| GET | `/api/v1/runs/{id}/artifacts/{name}` | Get one artifact manifest by name | `run:read` | general | `200` JSON artifact |
| GET | `/api/v1/runs/{id}/artifacts/{name}/download` | Download one artifact blob by name | `run:read` | general | `200` artifact bytes |
| GET | `/api/v1/runs/{id}/execution-payload` | Get the frozen execution payload for one run | `run:operator` | general | `200` JSON payload |
| POST | `/api/v1/runs/{id}/replay` | Create a new run from the source run's captured definition version, optionally targeting `cell_id` | `run:operator` | general | `202` JSON run |
| POST | `/api/v1/runs/{id}/cancel` | Record durable cancellation intent, with worker-control fast path when reachable | `run:operator` | general | `204` empty or `202` JSON pending result |
| POST | `/api/v1/runs/{id}/repair/mark-succeeded` | Resolve an orphaned run as succeeded | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/repair/mark-failed` | Resolve an orphaned run as failed | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/repair/mark-cancelled` | Resolve an orphaned run as cancelled | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/repair/mark-abandoned` | Resolve an orphaned run as abandoned | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/repair/mark-queued` | Requeue a run from a repairable state | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/force-fail` | Force a run into failed state | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/force-requeue` | Requeue a run from a repairable state | `run:operator` | general | `202` JSON result |
| GET | `/api/v1/runs/{id}/logs` | Stream run logs as SSE | `run:read` | general | `200` `text/event-stream` |
| GET | `/api/v1/setup/status` | Report whether initial setup is complete and API auth is enabled | `setup:status` | auth | `200` JSON status |
| POST | `/api/v1/setup/complete` | Create the first admin account | `setup:complete` | auth | `200` JSON token |
| POST | `/api/v1/login` | Exchange username/password for a session | Public | auth | `200` JSON session and cookies |
| POST | `/api/v1/logout` | Invalidate the current login session | `api:any` | auth | `204` empty |
| GET | `/api/v1/tokens` | List API tokens visible to the caller | `api:any` | token | `200` JSON list |
| POST | `/api/v1/tokens` | Create an API token | `api:any` | token | `201` JSON token metadata and plaintext token |
| DELETE | `/api/v1/tokens/{id}` | Delete an API token | `api:any` | token | `204` empty |
| POST | `/api/v1/users/change-password` | Change the caller password | `api:any` | auth | `204` empty |
| GET | `/api/v1/users` | List users | `user:admin` | general | `200` JSON list |
| POST | `/api/v1/users` | Create a user | `user:admin` | general | `201` JSON user |
| GET | `/api/v1/users/{id}` | Get one user | `user:admin` | general | `200` JSON user |
| PUT | `/api/v1/users/{id}` | Enable or disable a user; disabling revokes API tokens and login sessions | `user:admin` | general | `204` empty |
| DELETE | `/api/v1/users/{id}` | Delete a user | `user:admin` | general | `204` empty |
| GET | `/api/v1/namespaces` | List namespaces visible to the caller | `job:read` | general | `200` JSON list |
| POST | `/api/v1/namespaces` | Create a namespace | `admin:*` | general | `201` JSON namespace |
| GET | `/api/v1/namespaces/{id}` | Get one namespace | `job:read` | general | `200` JSON namespace |
| DELETE | `/api/v1/namespaces/{id}` | Delete an empty namespace | `admin:*` | general | `204` empty |
| GET | `/api/v1/namespaces/{id}/bindings` | List namespace role bindings | `admin:*` | general | `200` JSON list |
| POST | `/api/v1/namespaces/{id}/bindings` | Create a namespace role binding | `admin:*` | general | `201` JSON binding |
| DELETE | `/api/v1/namespaces/{id}/bindings/{user_id}` | Delete a namespace role binding | `admin:*` | general | `204` empty |
