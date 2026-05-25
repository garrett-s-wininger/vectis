# API Reference

The Vectis HTTP API is for scripts, dashboards, operators, and integrations that need to talk to the API server directly. If you are running jobs by hand, start with the [CLI Guide](./cli-guide.md); the CLI uses the same API paths and is usually the friendlier interface.

This page covers the shipped v1 REST surface. gRPC contracts live in `api/proto/`, and generated Go lives in `api/gen/go/`. Compatibility rules for REST, gRPC, CLI JSON, configuration, and schema changes are in [Compatibility](../concepts/compatibility.md).

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

Examples below use that address. Replace it with your API URL in deployed environments.

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

### Store Then Trigger A Job

Use stored jobs when the same definition will be triggered repeatedly:

```sh
curl -sS \
  -H 'Content-Type: application/json' \
  --data-binary @examples/sequenced.json \
  http://localhost:8080/api/v1/jobs
```

Then trigger it by job ID:

```sh
curl -sS \
  -X POST \
  -H "Idempotency-Key: $(uuidgen)" \
  http://localhost:8080/api/v1/jobs/trigger/sequenced-job
```

Both `POST /api/v1/jobs/run` and `POST /api/v1/jobs/trigger/{id}` accept an optional `cell_id` field to select the target execution cell. `target_cell_id` is accepted as an alias. If omitted, Vectis uses the API process's configured cell.

Stored-job triggers can fan out to multiple cells in one request by sending `cell_ids` or `target_cell_ids`:

```sh
curl -sS \
  -X POST \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"cell_ids":["iad-a","pdx-b"]}' \
  http://localhost:8080/api/v1/jobs/trigger/sequenced-job
```

Multi-cell trigger responses return one run per target cell:

```json
{
  "job_id": "sequenced-job",
  "runs": [
    {"run_id": "0fd7f565-a774-42e3-8e46-7c6f4f4c2c71", "run_index": 1, "cell_id": "iad-a"},
    {"run_id": "1d811208-886f-4262-9a02-a21df616f0b7", "run_index": 2, "cell_id": "pdx-b"}
  ]
}
```

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
      "steps": []
    }
  }
}
```

Ephemeral runs do not require a top-level job `id`. Stored jobs do. Job definition rules are documented in [Job Definition Validation](./job-validation.md).

## Authentication

When `api.auth.enabled=false`, API routes are accepted without bearer tokens. When auth is enabled, clients send:

```http
Authorization: Bearer <api_token>
```

Health endpoints, `/metrics`, and `POST /api/v1/login` are public. Setup routes use setup-specific authorization while the first admin is being created. Data routes authorize the action listed in the route table below; namespace-scoped resources are hidden with `404` when the caller is not allowed to see that namespace.

To request a token with username/password credentials:

```sh
curl -sS \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"<password>"}' \
  http://localhost:8080/api/v1/login
```

Then send the returned token on later requests:

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
| `validation_failed` | `400` | Job definition semantic validation failed; see [Job Definition Validation](./job-validation.md). |
| `not_found` / resource-specific `*_not_found` | `404` | Resource is absent or hidden by namespace authorization. |
| `internal_error` | `500` | Unexpected server error. |

## Endpoint Families

The table below is the exact route inventory. Read it by family:

| Family | Use it for |
| --- | --- |
| Health and diagnostics | Process health, schema state, queue pressure, log reachability, and Prometheus metrics. |
| Jobs | Store, replace, delete, list, and trigger reusable job definitions. |
| Ephemeral runs | Submit one-off job definitions without storing them first. |
| Runs and logs | Inspect run status, stream logs, cancel active work, or repair orphaned runs. |
| Setup and auth | First-admin setup, login, token lifecycle, password changes. |
| Users and namespaces | User administration, namespace tree management, and namespace role bindings. |

## Response Behavior

List routes use `limit` and `cursor` query parameters where implemented. Paginated responses include `next_cursor` when another page is available. Omit `cursor` to read the first page.

`GET /api/v1/jobs/{id}/runs` is backed by the global run catalog, not by per-cell fan-out. It returns summary run records from `job_runs`, including `owning_cell`, and accepts `after_index`, `since`, and `cell_id`/`owning_cell` filters.

`POST /api/v1/jobs/run` and `POST /api/v1/jobs/trigger/{id}` accept `Idempotency-Key`. Use this header when a client might retry after a timeout or dropped connection. Retry guidance for each route family is in [Idempotency And Retries](./idempotency-and-retries.md).

Streaming routes return `text/event-stream`. Use `curl -N`, `EventSource`, or another SSE-capable client for `GET /api/v1/sse/jobs/{id}/runs` and `GET /api/v1/runs/{id}/logs`.

## Routes

Rate-limit categories are configured under `api.rate_limit.*`. `general`, `auth`, and `token` buckets are per in-process API replica.

| Method | Path | Purpose | Auth action | Rate limit | Success |
| --- | --- | --- | --- | --- | --- |
| GET | `/health/live` | Liveness probe | Public | none | `200` empty |
| GET | `/health/ready` | Readiness probe | Public | none | `200` empty |
| GET | `/api/v1/version` | Build and cell identity info | Public | none | `200` JSON version |
| GET | `/api/v1/schema/status` | Migration schema status | Public | none | `200` JSON schema status |
| GET | `/api/v1/reconciler/heartbeat` | Reconciler last-activity signal | Public | none | `200` JSON heartbeat |
| GET | `/api/v1/audit/drops` | Audit event drop count | Public | none | `200` JSON drop count |
| GET | `/api/v1/db/pool-stats` | Database connection pool stats | Public | none | `200` JSON pool stats |
| GET | `/api/v1/queue/backlog` | Count of queued runs | Public | none | `200` JSON backlog |
| GET | `/api/v1/reconciler/stuck-runs` | Count of stuck (undispatched) queued runs | Public | none | `200` JSON stuck runs |
| GET | `/api/v1/log/reachable` | Log service gRPC connectivity | Public | none | `200` JSON reachable |
| GET | `/api/v1/audit/flush-failures` | Audit flush failure count | Public | none | `200` JSON flush failures |
| GET | `/api/v1/cron/status` | Cron schedule count and activity | Public | none | `200` JSON cron status |
| GET | `/api/v1/catalog/status` | Cell catalog inbox summary | Public | none | `200` JSON catalog status |
| GET | `/metrics` | Prometheus metrics | Public | none | `200` metrics text |
| POST | `/api/v1/cells/{cell_id}/catalog-events` | Record a cell status event into the global catalog inbox | `run:operator` | general | `202` JSON event |
| GET | `/api/v1/jobs` | List visible job definitions | `job:read` | general | `200` JSON list |
| POST | `/api/v1/jobs` | Create a stored job definition | `job:write` | general | `201` JSON job |
| GET | `/api/v1/jobs/{id}` | Get one job definition | `job:read` | general | `200` JSON job |
| PUT | `/api/v1/jobs/{id}` | Replace a job definition | `job:write` | general | `200` JSON job |
| DELETE | `/api/v1/jobs/{id}` | Delete a job definition | `job:write` | general | `204` empty |
| POST | `/api/v1/jobs/run` | Start an ephemeral run from JSON body, optionally targeting `cell_id` | `run:trigger` | general | `202` JSON run |
| POST | `/api/v1/jobs/trigger/{id}` | Start one or more runs from a stored job, optionally targeting `cell_id` or `cell_ids` | `run:trigger` | general | `202` JSON run |
| GET | `/api/v1/jobs/{id}/runs` | List global catalog runs for one job, optionally filtering by `cell_id` | `run:read` | general | `200` JSON list |
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
