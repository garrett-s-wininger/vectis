# OpenAPI Specification

Vectis publishes a machine-readable OpenAPI 3.0 specification for the v1 HTTP API:

[Download `openapi/v1.json`](/openapi/v1.json)

The spec is intended for API client generation, integration review, gateway policy checks, and quick route discovery. It covers the current shipped REST surface, including health checks, operational diagnostics, jobs, runs, artifacts, setup, login, token management, users, namespaces, and role bindings.

## Current Coverage

The first version is complete for:

- route paths and HTTP methods
- path, query, and idempotency-header parameters
- public versus bearer-protected operations
- Vectis authorization action metadata through `x-vectis-auth-action`
- request bodies for job submission, trigger options, setup, login, tokens, users, namespaces, bindings, replay, and repair operations
- primary response media types, including JSON, server-sent events, Prometheus text, and artifact downloads
- the common API error envelope

Some endpoint-specific JSON response bodies still use a generic object schema while the hand-written API reference remains the more detailed human source. Tightening those nested response schemas is expected to be incremental.

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

## Keeping It Current

The API route inventory test checks that every route registered by `vectis-api` has a matching method and path in the OpenAPI file. When adding an HTTP route, update both the handler inventory and `website/static/openapi/v1.json`.

For compatibility rules, see [Compatibility](../concepts/compatibility.md).
