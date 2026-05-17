# Context And Deadline Policy

Use this page when adding request handling, background work, shutdown behavior, or long-running service loops.

Contexts in Vectis express ownership. Before choosing `r.Context()`, a service root, `context.Background()`, or a timeout, decide what owns the work and what should stop it.

## Lifetime Types

| Lifetime | Owner | Typical examples |
| --- | --- | --- |
| Request-scoped work | HTTP or gRPC request | API DB calls, authorization checks, response construction, request-bound downstream calls. |
| Service lifecycle work | Process, supervisor, watcher, or shutdown signal | Server loops, metrics listeners, resolver polling, managed clients, cron/reconciler loops. |
| Run lifecycle work | Claimed run or worker execution | Worker execution, lease renewal, finalization, log streaming for the active run. |
| Intentional detached work | A documented repair path or bounded timeout | Async enqueue after HTTP `202`, shutdown cleanup, best-effort flush. |

## Rules Of Thumb

- HTTP handlers should use the request context, usually through helper timeouts such as `handlerDBCtx`.
- External request-scoped calls should stop when the request is canceled unless the code explicitly documents detachment.
- Background work should have an owner: process lifecycle, managed client lifecycle, worker run lifecycle, or a bounded cleanup timeout.
- `context.Background()` in production code should be rare and classified.
- Tests may use `context.Background()` freely when the test controls completion another way.

## Choosing A Context

| Work | Preferred context |
| --- | --- |
| API handler DB query | Request-derived context, usually `handlerDBCtx`. |
| API handler downstream call needed for the response | Request-derived context with a bounded timeout. |
| Work accepted before responding `202` but completed afterward | Detached context with trace linkage, bounded retry, and durable repair path. |
| Daemon startup check before a server exists | Short `context.WithTimeout(context.Background(), ...)`. |
| Service loop | Root process context from command/shutdown handling. |
| Cleanup during shutdown | Fresh bounded timeout so cleanup can finish even if the caller was canceled. |
| Worker executing a claimed run | Run lifecycle context that survives graceful shutdown until finalization. |
| Remote run cancel | Explicit cancellation signal for that run's execution context. |

If a task outlives the request that started it, document what stops it and how operators repair it if it fails.

## Production Background Context Inventory

| Location | Owner | Bound / stop condition | Reason |
| --- | --- | --- | --- |
| `internal/cli/shutdown_context.go` | Process signal handler | SIGINT/SIGTERM | Root context for commands. |
| `internal/cli/shutdown.go`, `internal/cli/http_server.go` | Deferred shutdown helper | Explicit timeout | Allows cleanup even after caller context is canceled. |
| `cmd/api/main.go` startup validation and registry client construction | API startup | 10 second timeout | Startup checks need a bounded root before the HTTP server exists. Worker address lookups after startup use the request-bound cancel context. |
| `cmd/cli/main.go` log stream commands | CLI invocation | Ctrl+C, stream completion, or reconnect attempt end | Interactive commands create their own cancellable roots because there is no inbound request context. |
| `cmd/local/main.go` health polling | Local supervisor startup | Health check timeout | Local stack startup needs a bounded wait while child processes become healthy. |
| `cmd/worker/main.go` `runCtx` | Worker run drain lifecycle | Current run finishes or process exits | Claimed runs should finalize during graceful shutdown. |
| `internal/api/server.go` async enqueue trace context | Post-202 enqueue goroutine | Enqueue retry policy and reconciler backstop | Preserve trace linkage without inheriting request cancellation. |
| `internal/api/server.go` log completion fallback | One-shot fallback lookup | 5 second timeout | SSE stream may outlive the original handler DB timeout. |
| `internal/queueclient/managed_queue.go` reconnect | Managed queue client lifecycle | Reconnect attempt returns | Reconnect should not be canceled by the short watch/readiness context. |
| `internal/resolver/registry.go` resolver poll | gRPC resolver lifecycle | Poll timeout or resolver close | Resolver refresh is owned by gRPC client connection state, not an HTTP request. |
| `internal/database/database.go` migration lock/unlock | Migration command/startup migration check | Advisory lock timeout; unlock best effort | Unlock should be attempted even when migration context expires. |
| `internal/api/audit/async.go` flush | Async auditor lifecycle | 5 second timeout | Flush should finish or fail during shutdown independent of caller request. |
| `internal/job/durable_log_stream.go` stream context | One run's durable log stream | Stream close/cancel | Log stream sender must continue while the run is active. |
| `internal/job/log_spool_forwarder.go` batch send | Log-forwarder batch lifecycle | 30 second timeout | Forwarder batches need a bounded send attempt. |
| `internal/logserver/server.go`, `internal/queue/server.go` metric events | Process metrics | Immediate counter/gauge update | Metrics are best-effort and do not carry request cancellation semantics. |
| `internal/observability/jobtrace.go`, `internal/queue/server.go` trace extraction | Trace propagation helper | Immediate span/context construction | Trace context can be reconstructed without an active caller context. |

## Review Checklist

For new production `context.Background()` uses:

- Is this a process/lifecycle root?
- If it outlives a request, what stops it?
- Is there a timeout?
- Is there a durable repair path if it fails?
- Should trace context be preserved without cancellation?
- Would `context.WithoutCancel` or a child of an existing service context be clearer?

For new request-derived work:

- Should cancellation abort the operation immediately?
- Does the operation need its own shorter timeout?
- Can returning early leave durable state behind?
- Does the caller need an idempotency key or repair path?

## Intentional Exceptions

Async enqueue after HTTP 202 is intentionally detached from the request. API shutdown does not join these goroutines; the database run row is already durable, enqueue retries are bounded, and `vectis-reconciler` repairs queued runs that missed handoff.

Worker drain is intentionally detached from the shutdown signal for the current run. A graceful worker shutdown stops dequeuing new work but lets the claimed run finalize when possible.

## Related Docs

| Need | Doc |
| --- | --- |
| Retry and give-up behavior | [Retry And Backoff Policy](./retry-policy.md) |
| Async enqueue decision | [ADR 0001](./architecture-decisions/0001-async-enqueue-after-http-202.md) |
| Reconciler repair process | [Dispatch Visibility](../operating/reliability/dispatch-visibility.md) |
| Scaling and restart expectations | [Scaling And Restarts](../operating/deployment/scaling-and-restarts.md) |
