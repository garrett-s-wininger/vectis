# SSE And Streaming Reference

Vectis exposes live run activity over Server-Sent Events (SSE). Use this page when building an API client, dashboard, proxy rule, or reconnect loop. For everyday terminal usage, start with [Log Streaming](./log-streaming.md).

Vectis does not use WebSockets for these routes.

## Routes

| Route | Purpose | Auth | Replay |
| --- | --- | --- | --- |
| `GET /api/v1/sse/jobs/{id}/runs` | Subscribe to future run-created events for one stored job. | `run:read` | No SSE replay; pair with `GET /api/v1/jobs/{id}/runs?after_index=<n>` for durable catch-up. |
| `GET /api/v1/runs/{id}/logs` | Stream and replay log chunks for one run. | `run:read` | Yes, with `since_sequence`, `Last-Event-ID`, `tail`, and `replay_limit`. |

Clients should send the `Accept` header with `text/event-stream`. Compatible wildcards such as `*/*` also pass response negotiation, but explicit SSE accepts are clearer for API clients and proxies.

Successful streams use:

| Header | Value |
| --- | --- |
| `Content-Type` | `text/event-stream; charset=utf-8` |
| `Cache-Control` | `no-cache` |
| `Connection` | `keep-alive` |
| `X-Accel-Buffering` | `no` |

Each stream starts with an SSE comment:

```text
: connected

```

Job-run streams also send `: keep-alive` comments every 30 seconds. SSE clients should ignore comment lines that begin with `:`.

## Job Run Events

`GET /api/v1/sse/jobs/{id}/runs` emits unnamed SSE `data:` events. The payload is a JSON object:

```text
data: {"run_id":"run-123","run_index":7}

```

| Field | Meaning |
| --- | --- |
| `run_id` | New run ID. |
| `run_index` | Monotonic run index for that job. |

This stream is a live notification channel, not a durable event log. A slow subscriber can miss events, and reconnecting does not replay missed events. Durable clients should keep the largest `run_index` they have processed and use the normal list route with `after_index` around every subscription:

```sh
curl 'http://localhost:8080/api/v1/jobs/<job-id>/runs?after_index=<last-run-index>'
curl -N -H 'Accept: text/event-stream' \
  http://localhost:8080/api/v1/sse/jobs/<job-id>/runs
```

That is the same shape the CLI uses for `vectis-cli logs job <job-id>`: list runs after the last known index, subscribe to future run events, and repeat on disconnect.

## Run Log Events

`GET /api/v1/runs/{id}/logs` emits unnamed SSE `data:` events. Normal log chunks include an SSE `id` equal to the run-local `sequence`:

```text
id: 12
data: {"timestamp":"2026-05-16T12:00:00Z","stream":0,"sequence":12,"data":"hello\n"}

```

| Field | Meaning |
| --- | --- |
| `timestamp` | RFC3339 timestamp for the log chunk. |
| `stream` | Numeric stream: `0` is `STREAM_STDOUT`, `1` is `STREAM_STDERR`, and `2` is `STREAM_CONTROL`. |
| `sequence` | Run-local sequence number. Use it for de-duplication and reconnect cursors. |
| `data` | Log text for stdout/stderr. For control chunks, this is a JSON string with control metadata. |
| `completed` | Optional numeric completion outcome: omitted or `0` is `RUN_OUTCOME_UNSPECIFIED`, `1` is `RUN_OUTCOME_SUCCESS`, `2` is `RUN_OUTCOME_FAILURE`, and `3` is `RUN_OUTCOME_UNKNOWN`. |

Control chunks use `stream: 2` and put a small JSON document inside the outer `data` string:

```text
id: 13
data: {"timestamp":"2026-05-16T12:00:01Z","stream":2,"sequence":13,"data":"{\"event\":\"completed\",\"status\":\"success\"}","completed":1}

```

Known control metadata:

| Inner field | Values |
| --- | --- |
| `event` | `start`, `completed`, or `replay_truncated`. |
| `status` | For `completed`: `success`, `failure`, `aborted`, `cancelled`, `abandoned`, or `unknown`. |
| `synthetic` | Present and `true` when the log service or API generated a completion event from stream EOF or terminal run state. |
| `limit` | For `replay_truncated`: the replay cap that stopped the stream. |

Clients should stop following a run after a `completed` control event or a non-zero `completed` field.

## Replay And Reconnect

The log route can replay historical chunks before it continues live streaming:

| Control | Scope | Meaning |
| --- | --- | --- |
| `since_sequence=<n>` | Query | Replay chunks with `sequence > n`. Must be a non-negative integer. |
| `Last-Event-ID: <n>` | Header | SSE reconnect cursor. Used only when `since_sequence` is absent. Must be a non-negative integer. |
| `tail=<n>` | Query | Replay only the latest `n` chunks after the sequence filter. Must be between `1` and `50000`. |
| `replay_limit=<n>` | Query | Cap replayed historical chunks. Must be between `1` and `50000`; default is `10000`. |

When `tail` is set, the API also caps `replay_limit` to `tail` if the requested replay limit is larger.

If replay exceeds the active `replay_limit`, the stream sends a control chunk and closes:

```text
data: {"timestamp":"2026-05-16T12:00:02Z","stream":2,"sequence":-1,"data":"{\"event\":\"replay_truncated\",\"limit\":10000}"}

```

The truncated control chunk has `sequence: -1`, so it does not include an SSE `id:` line. A client that receives `replay_truncated` should reconnect from the largest real sequence it processed.

## Client Pattern

For a resilient log client:

1. Store the largest `sequence` processed per `run_id`.
2. De-duplicate by `(run_id, sequence)`.
3. Reconnect with `Last-Event-ID` or `since_sequence`.
4. Treat comment frames as keep-alives.
5. Stop on a `completed` control event or non-zero `completed` field.
6. On `replay_truncated`, reconnect from the largest real sequence already processed.

For a resilient job watcher:

1. Store the largest `run_index` processed per job.
2. Use `GET /api/v1/jobs/{id}/runs?after_index=<n>` to catch up.
3. Subscribe to `GET /api/v1/sse/jobs/{id}/runs`.
4. On disconnect, return to the list route before resubscribing.

## Errors

Before a stream is established, routes return the normal JSON API error envelope.

| Code | Typical status | Meaning |
| --- | --- | --- |
| `job_not_found` | `404` | Stored job does not exist or is hidden by namespace authorization. |
| `run_not_found` | `404` | Run does not exist or is hidden by namespace authorization. |
| `log_service_unavailable` | `503` | API has no usable log service connection. |
| `log_service_error` | `502` | API could not connect to or read from the log service. |
| `invalid_since_sequence` | `400` | `since_sequence` or `Last-Event-ID` is not a non-negative integer. |
| `invalid_tail` | `400` | `tail` is outside `1..50000`. |
| `invalid_replay_limit` | `400` | `replay_limit` is outside `1..50000`. |
| `streaming_unsupported` | `500` | The HTTP response writer cannot flush streaming responses. |

After a stream is established, network, proxy, API, or log-service interruptions usually appear as a closed stream. Clients should reconnect using the patterns above.

## Proxy And Security Notes

Disable response buffering and use long enough read timeouts for `/api/v1/sse/...` and `/api/v1/runs/.../logs`. Vectis sends `X-Accel-Buffering: no`, but edge proxies and load balancers still need streaming-friendly settings.

SSE routes require `run:read`. Namespace-hidden jobs and runs are returned as `job_not_found` or `run_not_found` so route behavior does not reveal hidden resources.

Logs can contain command text, build output, and failure details. Avoid writing secrets to stdout or stderr in job commands, and treat log access as sensitive run read access.
