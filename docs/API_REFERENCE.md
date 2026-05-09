# API Reference

This reference describes the shipped HTTP API surface for Vectis v1. gRPC contracts are in `api/proto/`; generated Go is in `api/gen/go/`.

## Authentication

When `api.auth.enabled=false`, API routes are accepted without bearer tokens. When auth is enabled, clients send:

```http
Authorization: Bearer <api_token>
```

Health endpoints, `/metrics`, and `POST /api/v1/login` are public. Setup routes use setup-specific authorization while the first admin is being created. Data routes authorize the action listed in the route table below; namespace-scoped resources are hidden with `404` when the caller is not allowed to see that namespace.

## Error Envelopes

General API errors use:

```json
{
  "code": "invalid_request_body",
  "message": "invalid request body",
  "details": {"field": "optional structured details"}
}
```

Setup/auth middleware and a few token paths keep the v1 legacy auth envelope:

```json
{
  "error": "authentication_required",
  "detail": "optional detail"
}
```

Both envelopes use `Content-Type: application/json; charset=utf-8`. Auth responses add `Cache-Control: no-store`; `401` responses add `WWW-Authenticate: Bearer`.

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

## Pagination

List routes use `limit` and `cursor` query parameters where implemented. `limit` is capped by the server. Paginated responses include `next_cursor` when another page is available. Omit `cursor` to read the first page.

## Idempotency

`POST /api/v1/jobs/run` and `POST /api/v1/jobs/trigger/{id}` accept `Idempotency-Key`. Keys are scoped by authenticated principal plus operation. Reusing a key with the same request replays the completed JSON response. Reusing a key with a different request returns `409 idempotency_key_reused`; retrying while the first request is still in progress returns `409 idempotency_in_progress`.

## Streaming

`GET /api/v1/sse/jobs/{id}/runs` streams run events as server-sent events for one job. `GET /api/v1/runs/{id}/logs` streams log chunks as server-sent events. Log event data is JSON with `timestamp`, `stream`, `sequence`, `data`, and optional `completed`. The current HTTP log endpoint does not expose public replay controls; reconnecting clients should be prepared for historical chunks to be sent again until bounded replay support lands.

## Routes

Rate-limit categories are configured under `api.rate_limit.*`. `general`, `auth`, and `token` buckets are per in-process API replica.

| Method | Path | Purpose | Auth action | Rate limit | Success |
| --- | --- | --- | --- | --- | --- |
| GET | `/health/live` | Liveness probe | Public | none | `200` empty |
| GET | `/health/ready` | Readiness probe; checks DB and queue client readiness when configured | Public | none | `200` empty |
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
| POST | `/api/v1/runs/{id}/force-fail` | Force a run into failed state | `run:operator` | general | `204` empty |
| POST | `/api/v1/runs/{id}/force-requeue` | Requeue a run from a repairable state | `run:operator` | general | `202` JSON result |
| GET | `/api/v1/runs/{id}/logs` | Stream run logs as SSE | `run:read` | general | `200` `text/event-stream` |
| GET | `/api/v1/setup/status` | Report whether initial setup is complete | `setup:status` | auth | `200` JSON status |
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
