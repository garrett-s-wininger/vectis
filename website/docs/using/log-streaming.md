# Log Streaming

Vectis streams logs while a run is executing and can replay logs that have already been written. Most people should start with the CLI; use the HTTP API when you are building an integration or dashboard.

For the exact SSE payloads, replay limits, reconnect cursors, and proxy notes, see [SSE And Streaming Reference](./streaming-reference.md).

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

For the full route contract, including `Last-Event-ID`, `tail`, `replay_limit`, and completion control messages, see [SSE And Streaming Reference](./streaming-reference.md).

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

## Task Output In The Console

Select a task in the run graph to inspect its output. The console initially selects the latest attempt; use **Attempt** to inspect an earlier attempt or all attempts for that task. **Output → All run output** includes other tasks and records without execution attribution. Records written by older workers cannot be reliably assigned to a task and remain available in the run-wide view.

To exercise the viewer locally, open **Runs → Run Once → Load JSON file**, select `examples/log-viewer.json`, review it, and click **Submit run**. File loading stays in the browser until submission; invalid or unreadable files leave the current draft intact. Run Once uses the same code text-area component as the job editor. The CLI alternative is `./bin/vectis-cli jobs run examples/log-viewer.json --follow`. The demo uses separate tasks for stdout, stderr, and a line split across two writes. It deliberately fails with exit code 7 after the parallel output tasks finish. Select each task in the graph, then compare **All run output**.

The console replays the latest 500 run log records and retains up to 5,000 records while open. Task filtering applies to that recent window; an empty task view does not prove the task never produced output. Use the CLI or API replay controls above for older output. Changing tasks and attempts uses the existing stream rather than opening more subscriptions.

Completed-run compaction preserves execution attempt history, including execution IDs, outcomes, and timing. A redispatched run uses new execution IDs for its child tasks, so earlier and latest output remain distinguishable after completion. Attempt metadata follows run retention; log payloads follow the log retention window independently. A `builtins/retry` loop within an action is one execution attempt, rather than a new dispatch attempt.

Output is displayed as received, including partial-line chunks. Connection errors leave previously received output visible. Tasks without an execution attempt show an explicit empty state rather than generated worker messages.

### Upgrading Execution Attribution

Upgrade the log service and log forwarders before workers. Execution attribution adds an optional protobuf field and a durable log-record extension. The new log service reads existing records without attribution; older log-service binaries cannot read the new extended records. Before downgrading, restore a matching log-storage snapshot or use a fresh log directory. No database migration is required.
