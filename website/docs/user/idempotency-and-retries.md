# API Idempotency And Retry Semantics

Vectis mutating routes are not all equivalent under client retry. This document defines the v1 client contract.

## Header Contract

The supported retry header is:

```http
Idempotency-Key: <opaque-client-key>
```

Keys should be unique for one intended mutation. Use at least 128 bits of randomness or a stable operation ID from the caller. Do not put secrets in keys.

## Supported Endpoints

| Endpoint | Scope | Replay behavior |
| --- | --- | --- |
| `POST /api/v1/jobs/trigger/{id}` | Authenticated principal plus stored-trigger operation | Same key and same request replays the recorded `202` response. |
| `POST /api/v1/jobs/run` | Authenticated principal plus ephemeral-run operation | Same key and same request replays the recorded `202` response. |

If the same key is reused for a different request, the API returns `409 idempotency_key_reused`. If the first request has reserved the key but not recorded a response yet, the API returns `409 idempotency_in_progress`; clients should retry later with the same key.

Anonymous requests are scoped as anonymous for idempotency. Authenticated requests are scoped to the local user ID, so two users can safely use the same key without colliding.

## CLI Support

`vectis-cli trigger` and `vectis-cli run` accept `--idempotency-key`:

```sh
vectis-cli trigger build-main --idempotency-key "$(uuidgen)"
vectis-cli run job.json --idempotency-key "$(uuidgen)"
```

When a network error or timeout leaves the client unsure whether the API accepted the request, retry the same command with the same key.

## Mutator Retry Classification

| Route | Retry guidance |
| --- | --- |
| `POST /api/v1/jobs/trigger/{id}` | Use `Idempotency-Key` for safe retries. |
| `POST /api/v1/jobs/run` | Use `Idempotency-Key` for safe retries. |
| `POST /api/v1/jobs` | Treat as naturally conflicting by job ID; retry only if the client can tolerate `409 job_already_exists`. |
| `PUT /api/v1/jobs/{id}` | Retry same body safely after network errors; it replaces the definition. |
| `DELETE /api/v1/jobs/{id}` | Retry only if `404` after retry is acceptable as "already deleted". |
| `POST /api/v1/runs/{id}/cancel` | Retry same run ID safely while the run remains cancelable; terminal runs can return conflict. |
| `POST /api/v1/runs/{id}/force-fail` | Manual operator action; do not blindly retry unless the operator confirms the target state. |
| `POST /api/v1/runs/{id}/force-requeue` | Manual repair action; do not blindly retry because run state may change between attempts. |
| `POST /api/v1/setup/complete` | Retry only during bootstrap; `409 setup_already_complete` means setup already succeeded or another actor completed it. |
| `POST /api/v1/login` | Safe to retry; creates a new login token on each success. |
| `POST /api/v1/tokens` | Not idempotent; retry can create multiple tokens. Delete duplicates if this happens. |
| `DELETE /api/v1/tokens/{id}` | Retry only if `404` is acceptable as "already deleted". |
| User, namespace, and role-binding creates | Retry only if duplicate/conflict responses are acceptable. |
| User, namespace, and role-binding updates/deletes | Retry same target only when the caller treats missing resources as already converged. |

## Retention

Idempotency rows are durable SQL data today and do not have an expiry job yet. Operators should treat them as part of database backup and retention planning. A future retention command should expire old completed keys after the documented safe retry window while preserving enough history to avoid accidental duplicate runs during normal client retry periods.
