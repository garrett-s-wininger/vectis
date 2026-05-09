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

Sequences are scoped to one run. Clients should use sequence numbers to order chunks and to detect duplicates if a future replay API returns overlapping data.

The API writes SSE messages with JSON payloads in `data:` lines. A stream starts with an SSE comment (`: connected`) so clients know the HTTP stream is established.

When the log service stream ends without a completion chunk, the API performs a bounded database lookup. If the run already reached a terminal state, the API sends a synthetic completion event with sequence `-1`.

## Replay And Reconnect Contract

Current behavior:

- The API log SSE endpoint streams from the log service without an HTTP `since_sequence`, `Last-Event-ID`, `tail`, or page-size parameter.
- The log service gRPC API supports `GetLogs(since_sequence)`, but the public API does not yet expose that resume control.
- A reconnecting HTTP client should currently reconnect to `GET /api/v1/runs/{id}/logs` and be prepared to receive historical chunks again.
- Historical replay is not bounded by a documented API page size today.

Planned behavior should add explicit HTTP resume and bound controls before treating this endpoint as suitable for very large logs or many reconnecting clients.

## Client Guidance

- Treat the stream as SSE, not WebSocket.
- Preserve the largest sequence number seen for each run.
- De-duplicate repeated chunks by run ID and sequence when reconnecting.
- Stop following when `completed` is present or when the HTTP request is canceled by the caller.
- Use API auth/RBAC exactly as for run read access; logs may contain sensitive job output.

## Operator Guidance

- `vectis-log` storage is durable only when its storage directory is on durable storage.
- Worker durable log streaming can spool locally while the log service is temporarily unavailable.
- `VECTIS_LOG_MAX_RUN_BUFFERS` bounds terminal in-memory buffers; it does not bound persisted JSONL files.
- Persisted log retention belongs with the broader retention policy. Until cleanup exists, plan storage capacity for the full retention period you need.

## Known Gaps

- No public HTTP resume parameter yet.
- No `tail` or page-size limit on API replay yet.
- No load-test envelope for many concurrent SSE clients yet.
- No persisted-log cleanup command yet.
