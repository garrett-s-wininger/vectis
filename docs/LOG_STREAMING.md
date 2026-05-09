# Log Streaming

Vectis log delivery has three layers:

- Workers send log chunks to `vectis-log` over gRPC `StreamLogs`.
- `vectis-log` stores run logs as JSONL files and serves historical chunks through gRPC `GetLogs`.
- The API exposes run logs to HTTP clients as Server-Sent Events (SSE) at `GET /api/v1/runs/{id}/logs`.

The CLI `vectis-cli logs run <run-id>` uses the API SSE endpoint. `vectis-cli logs job <job-id>` subscribes to API run-created events, then follows logs for runs created after the subscription starts.

## Ordering And Events

Log chunks carry:

- `timestamp`
- `stream`
- `sequence`
- `data`
- optional `completed`

Sequences are scoped to one run. Clients should use sequence numbers to order chunks and to detect duplicates after reconnects.

The API writes SSE messages with JSON payloads in `data:` lines and sets the SSE event `id` to the chunk sequence when the sequence is non-negative. A stream starts with an SSE comment (`: connected`) so clients know the HTTP stream is established.

When the log service stream ends without a completion chunk, the API performs a bounded database lookup. If the run already reached a terminal state, the API sends a synthetic completion event with sequence `-1`.

## Replay And Reconnect Contract

`GET /api/v1/runs/{id}/logs` accepts:

| Control | Meaning |
| --- | --- |
| `since_sequence=<n>` | Replay only chunks with sequence greater than `n`. |
| `Last-Event-ID: <n>` | SSE reconnect equivalent to `since_sequence` when the query parameter is absent. |
| `tail=<n>` | Replay only the last `n` available historical chunks, then continue live streaming. |
| `replay_limit=<n>` | Maximum historical chunks to replay before closing with a `replay_truncated` control event. Default `10000`, maximum `50000`. |

`tail` also caps the replay limit to the requested tail size. If replay is truncated, the stream sends a control chunk whose `data` contains `{"event":"replay_truncated","limit":...}` and then closes. Reconnect with the last event ID to continue replaying the next page.

## Client Guidance

- Treat the stream as SSE, not WebSocket.
- Preserve the largest sequence number seen for each run.
- De-duplicate repeated chunks by run ID and sequence when reconnecting.
- Reconnect with `Last-Event-ID` or `since_sequence` after network loss.
- Use `tail` for dashboard-style "latest logs" views that do not need the full run history.
- Stop following when `completed` is present or when the HTTP request is canceled by the caller.
- Use API auth/RBAC exactly as for run read access; logs may contain sensitive job output.

## Operator Guidance

- `vectis-log` storage is durable only when its storage directory is on durable storage.
- Worker durable log streaming can spool locally while the log service is temporarily unavailable.
- `VECTIS_LOG_MAX_RUN_BUFFERS` bounds terminal in-memory buffers; it does not bound persisted JSONL files.
- Persisted log retention belongs with the broader retention policy. Until cleanup exists, plan storage capacity for the full retention period you need.

## Known Gaps

- No load-test envelope for many concurrent SSE clients yet.
- No persisted-log cleanup command yet.
