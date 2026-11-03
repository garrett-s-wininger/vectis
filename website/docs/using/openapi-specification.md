# OpenAPI Specification

Use the [interactive API explorer](/api) to browse operations, parameters, responses, and schemas. It uses the bundled specification without making requests to a live installation.

Vectis publishes a machine-readable OpenAPI 3.0 specification for the v1 HTTP API:

[Download `openapi/v1.json`](/openapi/v1.json)

The spec is intended for API client generation, integration review, gateway policy checks, and quick route discovery. It covers the current shipped REST surface, including health checks, operational diagnostics, jobs, runs, artifacts, setup, login, token management, users, namespaces, and role bindings. Pair it with [API Error Code Reference](./api-error-code-reference.md) when clients need exact retry and triage behavior for API error `code` values.

## Current Coverage

The first version is complete for:

- route paths and HTTP methods
- required path parameters, including source repository, job, run, and schedule identifiers
- public versus bearer-protected operations
- Vectis authorization action metadata through `x-vectis-auth-action`
- request bodies for job submission, trigger options, setup, login, tokens, users, namespaces, bindings, replay, and repair operations
- primary response media types, including JSON, server-sent events, Prometheus text, and artifact downloads
- the common API error envelope

Query and idempotency-header parameters cover the core workflows. Some Source operations still need detailed query-parameter and request-body schemas, and some endpoint-specific JSON responses use a generic object schema. The hand-written API reference remains the more detailed human source for those contracts.

## Authentication Notes

Protected operations use the `bearerAuth` security scheme in the OpenAPI document because that is the production integration posture:

```http
Authorization: Bearer <api_token>
```

Local deployments may run with `api.auth.enabled=false`; in that mode Vectis accepts protected routes without bearer credentials. The OpenAPI document still marks those operations as protected so generated clients are safe by default.

Setup and login routes are intentionally unauthenticated. `POST /api/v1/setup/complete` authenticates the bootstrap request with the `bootstrap_token` JSON field instead of the bearer scheme.

## Streams

OpenAPI can describe the media type for Vectis streams, but it does not fully model event framing. Use these docs for stream semantics:

| Stream | More detail |
| --- | --- |
| `GET /api/v1/runs/{id}/logs` | [Log Streaming](./log-streaming.md) |
| `GET /api/v1/sse/jobs/{id}/runs` | [API Reference](./api-reference.md) |
| `GET /api/v1/sse/source-repositories/{id}/jobs/{job_id}/runs` | [SSE And Streaming Reference](./streaming-reference.md) |
| `GET /api/v1/source-repositories/{id}/jobs/{job_id}/runs/{run_id}/logs` | [Log Streaming](./log-streaming.md) |

## Keeping It Current

The API route inventory test checks that every route registered by `vectis-api` has a matching method and path in the OpenAPI file. `mage lint` also runs the OpenAPI contract linter, which checks operation IDs, tags, explicit security metadata, default error responses, local `$ref` targets, and required parameter declarations for every path placeholder. Parameter validation resolves shared references and honors operation overrides of path-level declarations. A contract test also checks that every SSE route advertises `text/event-stream`. When adding an HTTP route, update both the handler inventory and `website/static/openapi/v1.json`.

For compatibility rules, see [Compatibility](../concepts/compatibility.md).
