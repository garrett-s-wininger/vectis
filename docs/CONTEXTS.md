# Context And Deadline Policy

Vectis uses contexts for three different lifetimes:

- Request-scoped work: tied to an HTTP or gRPC request and its deadline.
- Service lifecycle work: tied to a process, supervisor, watcher, or shutdown signal.
- Intentional detached work: allowed to outlive the request that initiated it, with a documented repair path or bounded timeout.

## Rules Of Thumb

- HTTP handlers should use the request context, usually through helper timeouts such as `handlerDBCtx`.
- External request-scoped calls should stop when the request is canceled unless the code explicitly documents detachment.
- Background work should have an owner: process lifecycle, managed client lifecycle, worker run lifecycle, or a bounded cleanup timeout.
- `context.Background()` in production code should be rare and classified.
- Tests may use `context.Background()` freely when the test controls completion another way.

## Production Background Context Inventory

| Location | Owner | Bound / stop condition | Reason |
| --- | --- | --- | --- |
| `internal/cli/shutdown_context.go` | Process signal handler | SIGINT/SIGTERM | Root context for commands. |
| `internal/cli/shutdown.go`, `internal/cli/http_server.go` | Deferred shutdown helper | Explicit timeout | Allows cleanup even after caller context is canceled. |
| `cmd/worker/main.go` `runCtx` | Worker run drain lifecycle | Current run finishes or process exits | Claimed runs should finalize during graceful shutdown. |
| `internal/api/server.go` async enqueue trace context | Post-202 enqueue goroutine | Enqueue retry policy and reconciler backstop | Preserve trace linkage without inheriting request cancellation. |
| `internal/api/server.go` log completion fallback | One-shot fallback lookup | 5 second timeout | SSE stream may outlive the original handler DB timeout. |
| `internal/queueclient/managed_queue.go` reconnect | Managed queue client lifecycle | Reconnect attempt returns | Reconnect should not be canceled by the short watch/readiness context. |
| `internal/database/database.go` migration lock/unlock | Migration command/startup migration check | Advisory lock timeout; unlock best effort | Unlock should be attempted even when migration context expires. |
| `internal/api/audit/async.go` flush | Async auditor lifecycle | 5 second timeout | Flush should finish or fail during shutdown independent of caller request. |
| `internal/job/durable_log_stream.go` stream context | One run's durable log stream | Stream close/cancel | Log stream sender must continue while the run is active. |
| `internal/job/log_spool_forwarder.go` batch send | Log-forwarder batch lifecycle | 30 second timeout | Forwarder batches need a bounded send attempt. |
| `internal/logserver/server.go`, `internal/queue/server.go` metric events | Process metrics | Immediate counter/gauge update | Metrics are best-effort and do not carry request cancellation semantics. |

## Review Checklist

For new production `context.Background()` uses:

- Is this a process/lifecycle root?
- If it outlives a request, what stops it?
- Is there a timeout?
- Is there a durable repair path if it fails?
- Should trace context be preserved without cancellation?
- Would `context.WithoutCancel` or a child of an existing service context be clearer?

## Intentional Exceptions

Async enqueue after HTTP 202 is intentionally detached from the request. The database run row is already durable, enqueue retries are bounded, and `vectis-reconciler` repairs queued runs that missed handoff.

Worker drain is intentionally detached from the shutdown signal for the current run. A graceful worker shutdown stops dequeuing new work but lets the claimed run finalize when possible.
