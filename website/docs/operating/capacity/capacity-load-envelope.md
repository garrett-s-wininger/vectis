# Capacity And Load Envelope

This is the initial capacity contract for Vectis. It defines what we measure, where operators should watch pressure, and which ranges are known-safe versus still requiring validation. It does not claim production throughput numbers until benchmark output is captured for a named deployment.

For component replica-count and rolling-restart semantics, see [Scaling And Restarts](../deployment/scaling-and-restarts.md). For developer and release validation, see [Capacity And Performance Checks](../../developing/performance/capacity-checks.md).

## Repeatable Benchmark

Run the local queue benchmark:

```sh
make capacity-benchmark
```

Useful knobs:

| Variable | Default | Purpose |
| --- | --- | --- |
| `VECTIS_CAPACITY_BENCHTIME` | `2s` | Go benchmark duration per scenario. |
| `VECTIS_CAPACITY_COUNT` | `1` | Repetition count. Use `3` or more for baseline capture. |
| `VECTIS_CAPACITY_QUEUE_BENCH` | Queue round-trip, concurrent, sustained, and latency benches | Override to focus on one scenario. |

The benchmark script prints a human-readable summary first and keeps the raw Go benchmark output in a separate section for baseline records. Capture the command, git commit, Go version, OS/CPU, database driver/DSN when relevant, the summary, and the raw output.

## Scenarios To Track

| Scenario | What to vary | Primary signals |
| --- | --- | --- |
| Trigger burst | concurrent `POST /api/v1/jobs/trigger/{id}` clients | HTTP latency/status, queued-run age, dispatch events, DB pool waits |
| Sustained builds/day | trigger rate over time | queue depth, run completion latency, reconciler repairs |
| Queue backlog | producers faster than workers | pending depth, DLQ moves, dequeue latency |
| Worker scale | worker process count | claim conflicts, worker job duration, DB pool saturation |
| Large logs | lines per run and bytes per line | log append failures, replay truncation, storage bytes |
| SSE readers | concurrent run-log and run-event clients | active streams, replay chunk counts, dropped channels |
| Cron schedules | number and frequency of schedules | schedule-to-run latency, duplicate or missed enqueue |
| Auth/API load | login/token/list calls | rate-limit rejects, auth DB latency |

## Initial Envelope

| Area | Known-safe range | Needs validation before relying on it |
| --- | --- | --- |
| API replicas | One API process | Horizontal API replicas, because rate limiting is in-process per replica. |
| Queue | One active queue service | Multiple active queue replicas; queue is not a shared distributed queue today. |
| Workers | Multiple workers, one run per process | Large worker fleets and DB pool sizing under high claim/renew/finalize rates. |
| Cron | One cron process | Multiple cron replicas; duplicate schedule firing needs an HA decision. |
| Reconciler | One reconciler process | Multiple reconcilers under heavy queued-run repair load. |
| Logs | Durable local JSONL storage plus bounded in-memory terminal buffers | Very large persisted logs and many replaying clients. |
| SQLite | Local/dev and small single-node deployments | High-concurrency API, worker, cron, and reconciler load. |
| Postgres | Recommended for multi-service deployments | Exact pool sizes and trigger/worker scale must be benchmarked per deployment. |

The envelope above is a throughput and pressure statement. The scale-out contract in [Scaling And Restarts](../deployment/scaling-and-restarts.md) remains the source of truth for whether a component is safe to run as multiple active replicas.

## Regression Gate

For now, `make capacity-benchmark` is a manual release and pre-feature check, not a per-PR gate. Promote a subset to CI only after baseline variance is understood on the CI hardware.

Feature work that changes queueing, run state, log streaming, cron, or API hot paths should state whether it changes this envelope and include benchmark output when it plausibly does.

## Overload Behavior

- Trigger success means the run row is durable; enqueue may still be asynchronous and repaired by the reconciler.
- Queue overload should show up as queue depth growth and older queued runs.
- Worker overload should show up as longer queued-to-running latency.
- Log replay overload is bounded by the HTTP replay controls in [LOG_STREAMING.md](../../using/log-streaming.md).
- API overload can return `429` when rate limiting is enabled or `503` when dependencies are unavailable.
