# Log Streaming

Vectis streams logs while a run is executing and can replay logs that have already been written. Most people should start with the CLI; use the HTTP API when you are building an integration or dashboard.

## Follow One Run

If you already have a run ID, stream its logs with:

```sh
./bin/vectis-cli logs run <run-id>
```

This connects to the API log stream and prints output until the run completes or you press `Ctrl+C`.

You can also pipe a run ID into the command:

```sh
printf '%s\n' <run-id> | ./bin/vectis-cli logs run -
```

That is useful in shell scripts where one command produces the ID and the next command follows logs.

## Follow A Job's Future Runs

To keep a terminal open for the next runs of a reusable source-backed job:

```sh
./bin/vectis-cli logs job <job-id> --repository <repo> --follow
```

`logs job` subscribes to run-created events for that job and follows runs created after the subscription is active. It does not replay old runs.

For a one-command workflow, `jobs run` and `jobs trigger` both support `--follow`:

```sh
./bin/vectis-cli jobs run examples/sequenced.json --follow
./bin/vectis-cli jobs trigger sequenced-job --repository vectis-local --follow
```

## Filter Output

By default, Vectis prints both stdout and stderr. To show only one stream:

```sh
./bin/vectis-cli logs run <run-id> --stdout
./bin/vectis-cli logs run <run-id> --stderr
```

The same flags work with `logs job`:

```sh
./bin/vectis-cli logs job sequenced-job --stderr
```

Stderr lines are prefixed with `[stderr]` in the CLI output.

## What You Will See

The CLI prints run control messages and log lines together. For example:

```text
Connected to logs for run 2b196bc5-0f7f-47e7-8fb1-2e4f6db8b6f0
Streaming logs... (press Ctrl+C to exit)

=== Run 2b196bc5-0f7f-47e7-8fb1-2e4f6db8b6f0 started ===
$ echo hello
hello
Run 2b196bc5-0f7f-47e7-8fb1-2e4f6db8b6f0 finished successfully.
```

If the API cannot reach the log service, the CLI reports that the log service is temporarily unavailable. The run may still continue; try again after the log service recovers.

## Use The API Directly

The run log API is Server-Sent Events (SSE), not WebSocket:

```sh
curl -N http://localhost:8080/api/v1/runs/<run-id>/logs
```

Each event has a JSON payload:

```json
{
  "timestamp": "2026-05-16T12:00:00Z",
  "stream": 1,
  "sequence": 12,
  "data": "hello\n"
}
```

The `stream` value identifies stdout, stderr, or control events. The `sequence` value is scoped to one run.

## Replay And Reconnect

The API can replay historical chunks before continuing live streaming:

| Control | Use it when |
| --- | --- |
| `since_sequence=<n>` | You saved the last sequence number and want only newer chunks. |
| `Last-Event-ID: <n>` | Your SSE client is reconnecting after receiving event ID `n`. |
| `tail=<n>` | You only want the latest `n` chunks before following live output. |
| `replay_limit=<n>` | You want to cap how many historical chunks the API sends. |

For a resilient client:

- keep the largest sequence number seen for each run
- de-duplicate chunks by run ID and sequence
- reconnect with `Last-Event-ID` or `since_sequence`
- stop following when a completion event arrives or the user cancels

If a replay is too large, the stream sends a control event with `{"event":"replay_truncated"}` and closes. Reconnect with the last event ID to continue.

## Follow Run Creation Events

The job run-event API is also SSE:

```sh
curl -N 'http://localhost:8080/api/v1/sse/jobs/<job-id>/runs?repository_id=<repo>'
```

Use it when you are building a dashboard or automation that wants to notice new runs for a reusable source-backed job, then connect to each run's log stream.

## How Logs Move Through Vectis

The usual path is:

1. A worker sends log chunks to `vectis-log`.
2. `vectis-log` writes the run log to JSONL storage.
3. The API reads from `vectis-log` and exposes the stream over SSE.
4. The CLI or your integration consumes the API stream.

For local development, this mostly stays invisible. For operations, make sure the `vectis-log` storage directory is on durable storage if you need logs to survive restarts.

## Security

Logs can contain job output, command text, and failure details. Treat log access like run read access:

- API auth and RBAC apply to log routes.
- Namespace-hidden runs return `404`.
- Avoid writing secrets to stdout or stderr in job commands.
