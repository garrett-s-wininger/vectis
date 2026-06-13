# Idempotency And Retries

Retries are part of normal automation. A client can time out, a connection can drop, or a process can crash after sending a request but before reading the response.

For job-submission routes, Vectis supports idempotency keys so clients can retry without accidentally starting duplicate runs.

## The Short Version

Use an idempotency key when you submit work that should happen once:

```sh
./bin/vectis-cli jobs run examples/sequenced.json \
  --idempotency-key "$(uuidgen)"
```

If the command times out or the connection drops, retry with the same key:

```sh
./bin/vectis-cli jobs run examples/sequenced.json \
  --idempotency-key "<same-key-as-before>"
```

Use a new key for a new intended run.

## Supported Routes

Only these routes currently record idempotency keys:

| Route | Use it for | Safe retry behavior |
| --- | --- | --- |
| `POST /api/v1/jobs/run` | Start an ephemeral run from a job definition. | Same key and same request returns the same `202` response. |
| `POST /api/v1/jobs/trigger/{id}` | Trigger a stored job. | Same key and same trigger returns the same `202` response. |
| `POST /api/v1/runs/{id}/replay` | Create a fresh run from a completed source run's captured definition version. | Same key, source run, and replay target returns the same `202` response. |

Other routes may be safe to retry for other reasons, but they do not replay a recorded response.

## CLI Usage

One-off runs, stored-job triggers, and replay requests accept `--idempotency-key`:

```sh
./bin/vectis-cli jobs run job.json --idempotency-key "$(uuidgen)"
./bin/vectis-cli jobs trigger build-main --idempotency-key "$(uuidgen)"
./bin/vectis-cli runs replay <run-id> --idempotency-key "$(uuidgen)"
```

For scripts, generate the key once and keep it with the operation you are trying to complete:

```sh
key="$(uuidgen)"

./bin/vectis-cli jobs trigger build-main --idempotency-key "$key"
```

If the script cannot tell whether the trigger succeeded, retry with that same `key`.

## API Usage

Send the key in the `Idempotency-Key` header:

```sh
curl -sS \
  -X POST \
  -H "Idempotency-Key: $(uuidgen)" \
  http://localhost:8080/api/v1/jobs/trigger/build-main
```

Keys are opaque client strings. Use at least 128 bits of randomness, or use a stable operation ID from the system calling Vectis. Keys must be 1-255 visible ASCII characters and cannot contain whitespace or commas. Do not put secrets, tokens, passwords, or customer data in the key.

## What Vectis Compares

Vectis stores the key with a scope and a request hash.

For stored-job triggers, the scope includes:

- the authenticated principal, or anonymous access when auth is disabled
- the stored job ID
- the trigger operation

For ephemeral runs, the scope includes:

- the authenticated principal, or anonymous access when auth is disabled
- the namespace
- the raw job definition request body

For run replay, the scope includes:

- the authenticated principal, or anonymous access when auth is disabled
- the source run ID
- the replay operation

This means two users can safely use the same key without colliding. It also means the same user should not reuse one key for two different intended runs.

## Responses To Expect

| Situation | Response |
| --- | --- |
| First request with a new key succeeds. | `202` with the created run response. |
| Retry with the same key and same request after success. | `202` with the recorded response. |
| Retry after the API committed the run but crashed before caching the response. | `202` with the recovered original run response. |
| Retry while the first request is still in progress. | `409 idempotency_in_progress`. Retry later with the same key. |
| Reuse the same key for a different request. | `409 idempotency_key_reused`. Stop and create a new key for the new operation. |
| Send `Idempotency-Key` to a route that does not document support for it, or send an invalid key. | `400 invalid_request_header`. Remove the header or generate a valid key. |

When you get `idempotency_in_progress`, wait briefly and retry with the same key. When you get `idempotency_key_reused`, do not keep retrying that key with a changed request.

## Route-By-Route Retry Guidance

| Route | Guidance |
| --- | --- |
| `POST /api/v1/jobs/run` | Use `Idempotency-Key` for safe retries. |
| `POST /api/v1/jobs/trigger/{id}` | Use `Idempotency-Key` for safe retries. |
| `POST /api/v1/runs/{id}/replay` | Use `Idempotency-Key` for safe retries. If you pass `cell_id`, retry with the same target cell. |
| `POST /api/v1/jobs` | Retry only if `409 job_already_exists` is acceptable as "already stored." |
| `PUT /api/v1/jobs/{id}` | Retrying the same body is generally safe because the route replaces the definition. |
| `DELETE /api/v1/jobs/{id}` | Retry only if `404` is acceptable as "already deleted." |
| `POST /api/v1/runs/{id}/cancel` | Retry while the run is still cancelable. Terminal runs may reject the request. |
| `POST /api/v1/runs/{id}/force-fail` | Manual operator action. Do not retry blindly. |
| `POST /api/v1/runs/{id}/force-requeue` | Manual repair action. Do not retry blindly because run state can change. |
| `POST /api/v1/setup/complete` | Retry only during bootstrap. `409 setup_already_complete` means setup already happened. |
| `POST /api/v1/login` | Safe to retry, but each success creates a new session. |
| `POST /api/v1/tokens` | Not idempotent. Retry can create duplicate tokens. |
| `DELETE /api/v1/tokens/{id}` | Retry only if `404` is acceptable as "already deleted." |
| User, namespace, and role-binding creates | Retry only if duplicate or conflict responses are acceptable. |
| User, namespace, and role-binding updates/deletes | Retry only when missing resources count as already converged. |

## Handling Rate Limits

If the API returns `429 rate_limit_exceeded`, it also sets `Retry-After`.

Clients should wait at least that long before retrying. Keep the same idempotency key when retrying a job submission that already had one.

## Cleanup And Retention

Idempotency records are stored in SQL so retries survive API restarts and can recover a committed run response after an API crash. Operators can prune old records with retention cleanup:

```sh
./bin/vectis-cli retention cleanup --dry-run
./bin/vectis-cli retention cleanup --yes
```

The `--idempotency-age` flag controls how old idempotency records must be before they are eligible for deletion. Keep the retention window longer than the normal maximum time a client might retry the same operation.
