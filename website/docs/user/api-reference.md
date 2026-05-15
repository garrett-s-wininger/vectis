# API Reference

This reference describes the shipped HTTP API surface for Vectis v1. gRPC contracts are in `api/proto/`; generated Go is in `api/gen/go/`. Compatibility rules for REST, gRPC, CLI JSON, configuration, and schema changes are in [COMPATIBILITY.md](../general/compatibility.md).

## Authentication

When `api.auth.enabled=false`, API routes are accepted without bearer tokens. When auth is enabled, clients send:

```http
Authorization: Bearer <api_token>
```

Health endpoints, `/metrics`, and `POST /api/v1/login` are public. Setup routes use setup-specific authorization while the first admin is being created. Data routes authorize the action listed in the route table below; namespace-scoped resources are hidden with `404` when the caller is not allowed to see that namespace.

## Error Envelopes

General API errors use this stable v1 envelope:

```json
{
  "code": "invalid_request_body",
  "message": "invalid request body",
  "details": {"field": "optional structured details"}
}
```

All error responses use `Content-Type: application/json; charset=utf-8` and `X-Content-Type-Options: nosniff`. `401` responses add `WWW-Authenticate: Bearer`.

The `code` field is intended for clients and scripts. The `message` field is human-readable and may become clearer over time without changing the machine meaning. `details` is optional structured data whose shape depends on `code`; clients should ignore unknown detail keys.

Common status meanings:

| Status | Meaning |
| --- | --- |
| `400` | Invalid JSON, invalid IDs, missing required fields, or invalid state transition input. |
| `401` | Missing, malformed, expired, or invalid bearer credentials. |
| `403` | Authenticated principal is not allowed to perform a visible global action. |
| `404` | Resource is absent or hidden by namespace authorization. |
| `409` | Resource conflict, duplicate create, idempotency conflict, or invalid run repair state. |
| `413` | Request body exceeds the route limit. |
| `415` | JSON route received a non-JSON content type. |
| `429` | In-process rate limit rejected the request. `Retry-After` is set. |
| `500` | Unexpected server error. |
| `503` | Database, auth persistence, queue, or setup state is not ready. |

Common v1 error codes:

| Code | Typical status | Meaning |
| --- | --- | --- |
| `invalid_request_body` | `400` | JSON could not be decoded or did not match the expected request shape. |
| `authentication_required` | `401` | Missing, malformed, expired, or invalid bearer credentials. |
| `authorization_denied` | `403` | Authenticated principal is not allowed to perform the requested visible action. |
| `auth_unavailable` | `503` | Authentication persistence or configuration is not available. |
| `setup_required` | `503` | Initial setup must be completed before using the requested route. |
| `bootstrap_not_configured` | `503` | Initial setup needs a sufficiently long configured bootstrap token. |
| `invalid_bootstrap_token` | `401` | The supplied setup bootstrap token does not match the server configuration. |
| `unsupported_media_type` | `415` | A JSON route received a non-JSON `Content-Type`. |
| `request_body_too_large` | `413` | The request body exceeded the route limit. |
| `database_unavailable` | `503` | The configured SQL database is temporarily unavailable. |
| `queue_not_ready` | `503` | The API cannot currently hand work to the queue. |
| `rate_limit_exceeded` | `429` | The request exceeded the in-process rate limit; `Retry-After` is set. |
| `idempotency_key_reused` | `409` | The same idempotency key was reused with a different request body or target. |
| `idempotency_in_progress` | `409` | The original idempotent request has not completed yet. |
| `validation_failed` | `400` | Job definition semantic validation failed; see [JOB_VALIDATION.md](job-validation.md). |
| `not_found` / resource-specific `*_not_found` | `404` | Resource is absent or hidden by namespace authorization. |
| `internal_error` | `500` | Unexpected server error. |

## Pagination

List routes use `limit` and `cursor` query parameters where implemented. `limit` is capped by the server. Paginated responses include `next_cursor` when another page is available. Omit `cursor` to read the first page.

## Idempotency

`POST /api/v1/jobs/run` and `POST /api/v1/jobs/trigger/{id}` accept `Idempotency-Key`. Keys are scoped by authenticated principal plus operation. Reusing a key with the same request replays the completed JSON response. Reusing a key with a different request returns `409 idempotency_key_reused`; retrying while the first request is still in progress returns `409 idempotency_in_progress`.

Other mutating routes do not currently persist idempotency records. Retry guidance for each route family is in [IDEMPOTENCY_AND_RETRIES.md](idempotency-and-retries.md).

## Streaming

`GET /api/v1/sse/jobs/{id}/runs` streams run events as server-sent events for one job. `GET /api/v1/runs/{id}/logs` streams log chunks as server-sent events. Log event data is JSON with `timestamp`, `stream`, `sequence`, `data`, and optional `completed`. The current HTTP log endpoint does not expose public replay controls; reconnecting clients should be prepared for historical chunks to be sent again until bounded replay support lands.

## Run Foundation Fields

`GET /api/v1/runs/{id}` includes the run's captured `definition_version`, optional `definition_hash`, and `owning_cell` in addition to status, timestamps, failure fields, and `dispatch_events`. New single-cell deployments use `owning_cell="local"` until multi-cell routing is introduced.

## Routes

Rate-limit categories are configured under `api.rate_limit.*`. `general`, `auth`, and `token` buckets are per in-process API replica.

| Method | Path | Purpose | Auth action | Rate limit | Success |
| --- | --- | --- | --- | --- | --- |
| GET | `/health/live` | Liveness probe | Public | none | `200` empty |
| GET | `/health/ready` | Readiness probe | Public | none | `200` empty |
| GET | `/api/v1/version` | Build version info | Public | none | `200` JSON version |
| GET | `/api/v1/schema/status` | Migration schema status | Public | none | `200` JSON schema status |
| GET | `/api/v1/reconciler/heartbeat` | Reconciler last-activity signal | Public | none | `200` JSON heartbeat |
| GET | `/api/v1/audit/drops` | Audit event drop count | Public | none | `200` JSON drop count |
| GET | `/api/v1/db/pool-stats` | Database connection pool stats | Public | none | `200` JSON pool stats |
| GET | `/api/v1/queue/backlog` | Count of queued runs | Public | none | `200` JSON backlog |
| GET | `/api/v1/reconciler/stuck-runs` | Count of stuck (undispatched) queued runs | Public | none | `200` JSON stuck runs |
| GET | `/api/v1/log/reachable` | Log service gRPC connectivity | Public | none | `200` JSON reachable |
| GET | `/api/v1/audit/flush-failures` | Audit flush failure count | Public | none | `200` JSON flush failures |
| GET | `/api/v1/cron/status` | Cron schedule count and activity | Public | none | `200` JSON cron status |
| GET | `/metrics` | Prometheus metrics | Public | none | `200` metrics text |
| GET | `/api/v1/jobs` | List visible job definitions | `job:read` | general | `200` JSON list |
| POST | `/api/v1/jobs` | Create a stored job definition | `job:write` | general | `201` JSON job |
| GET | `/api/v1/jobs/{id}` | Get one job definition | `job:read` | general | `200` JSON job |
| PUT | `/api/v1/jobs/{id}` | Replace a job definition | `job:write` | general | `200` JSON job |
| DELETE | `/api/v1/jobs/{id}` | Delete a job definition | `job:write` | general | `204` empty |
| POST | `/api/v1/jobs/run` | Start an ephemeral run from JSON body | `run:trigger` | general | `202` JSON run |
| POST | `/api/v1/jobs/trigger/{id}` | Start a run from a stored job | `run:trigger` | general | `202` JSON run |
| GET | `/api/v1/jobs/{id}/runs` | List runs for one job | `run:read` | general | `200` JSON list |
| GET | `/api/v1/sse/jobs/{id}/runs` | Stream run events for one job | `run:read` | general | `200` `text/event-stream` |
| GET | `/api/v1/runs/{id}` | Get one run, including dispatch events | `run:read` | general | `200` JSON run |
| POST | `/api/v1/runs/{id}/cancel` | Request cancellation through worker control | `run:operator` | general | `202` JSON result |
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
| POST | `/api/v1/login` | Exchange username/password for an API token | Public | auth | `200` JSON token |
| GET | `/api/v1/tokens` | List API tokens visible to the caller | `api:any` | token | `200` JSON list |
| POST | `/api/v1/tokens` | Create an API token | `api:any` | token | `201` JSON token metadata and plaintext token |
| DELETE | `/api/v1/tokens/{id}` | Delete an API token | `api:any` | token | `204` empty |
| POST | `/api/v1/users/change-password` | Change the caller password | `api:any` | auth | `204` empty |
| GET | `/api/v1/users` | List users | `user:admin` | general | `200` JSON list |
| POST | `/api/v1/users` | Create a user | `user:admin` | general | `201` JSON user |
| GET | `/api/v1/users/{id}` | Get one user | `user:admin` | general | `200` JSON user |
| PUT | `/api/v1/users/{id}` | Update a user | `user:admin` | general | `200` JSON user |
| DELETE | `/api/v1/users/{id}` | Delete a user | `user:admin` | general | `204` empty |
| GET | `/api/v1/namespaces` | List namespaces visible to the caller | `job:read` | general | `200` JSON list |
| POST | `/api/v1/namespaces` | Create a namespace | `admin:*` | general | `201` JSON namespace |
| GET | `/api/v1/namespaces/{id}` | Get one namespace | `job:read` | general | `200` JSON namespace |
| DELETE | `/api/v1/namespaces/{id}` | Delete an empty namespace | `admin:*` | general | `204` empty |
| GET | `/api/v1/namespaces/{id}/bindings` | List namespace role bindings | `admin:*` | general | `200` JSON list |
| POST | `/api/v1/namespaces/{id}/bindings` | Create a namespace role binding | `admin:*` | general | `201` JSON binding |
| DELETE | `/api/v1/namespaces/{id}/bindings/{user_id}` | Delete a namespace role binding | `admin:*` | general | `204` empty |
