# Capacity And Performance Checks

Use these checks when development work might change Vectis throughput, latency, queue behavior, log behavior, or the published operating envelope.

This is a developer and release-validation page. It is not a production operator drill framework yet. Operators should use [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) for the current operating contract and [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md) for safe replica-count behavior.

## When To Run Checks

Run these checks before:

- changing queue, orchestrator, worker, log streaming, cron, reconciler, catalog, or API hot paths;
- changing database schema or query patterns that affect run creation, claiming, finalization, or log lookup;
- changing retry, idempotency, dispatch, or repair behavior;
- publishing a release that claims a new capacity envelope.

For small code changes, a local benchmark and a short note may be enough. For release claims or changes to the operator envelope, capture repeatable evidence in a staging or dedicated test environment.

## Before Generating Load

Use a local, CI, staging, or dedicated test environment. Do not point ad hoc performance experiments at production.

Before you start:

1. Pick the question the check should answer, such as "did this queue change regress p99 enqueue/dequeue latency?" or "does this worker count drain 200 queued runs?"
2. Confirm the environment is disposable, isolated, or backed up.
3. Confirm the workload is safe to run repeatedly.
4. Record the current deployment shape and resource limits.
5. Run `vectis-cli health check --strict` and either start from a clean baseline or record known warnings.
6. Decide the stop conditions before the check begins.

## Evidence Level

Match the evidence to the risk of the change:

| Change type | Minimum evidence |
| --- | --- |
| Queue internals, delivery, persistence, or retry paths | `make perf SUITE=queue` output before/after, plus `benchstat` comparison and notes on variance. |
| API trigger, run state, dispatch, or idempotency path | `make perf SUITE=dal` when SQL hot paths changed, plus a deployed-stack check if concurrency behavior changed. |
| Job executor, built-in action, or durable worker log flush path | `make perf SUITE=job` output before/after, plus macro or deployed-stack evidence if worker throughput changes. |
| Worker claim, lease, finalization, or log forwarding path | `make perf SUITE=macro` for local flow slices, plus deployed-stack check with worker count, DB pool settings, queue depth, terminal outcomes, and log health. |
| Log streaming, replay, or storage path | Deployed-stack check with concurrent readers, log volume, replay behavior, and storage/spool pressure. |
| Release note that changes the operating envelope | Repeatable check record, raw output, observed limiting component, and docs update. |

## Check Record

Capture this for every meaningful performance check:

| Field | Record |
| --- | --- |
| Date and owner | Who ran the check and when. |
| Code and build | Git commit, release version, build flags, and container image tags. |
| Deployment shape | API, cell ingress, queue, orchestrator, log, artifact, secrets, worker-core, worker, log-forwarder, cron, reconciler, and catalog counts. |
| Database | Driver, DSN class, pool settings, host size, and storage class. |
| Queue, logs, artifacts, and secrets | Queue persistence path, log storage medium, artifact storage path, job secret store path, spool location, and free space. |
| Workload | Exact command, script, job definition, client count, trigger rate, and duration. |
| Observability | Benchmark output, Prometheus snapshots, dashboard screenshots, and notable service logs. |
| Result | Pass/fail decision, observed limit, and follow-up issues. |

Keep the raw output. Summaries are helpful, but raw output is what lets the next check compare honestly.

## Local Queue Benchmark Check

Use this check when queue behavior, run handoff, or release confidence is the main question. It currently exercises queue benchmarks under `internal/queue`; it does not prove full API, database, worker, or log-service capacity by itself.

The performance harness builds `bin/vectis-perf` and writes raw benchmark output, a JSON summary, a Markdown summary, and an offline HTML report under `artifacts/perf/`. Keep raw outputs when you need an honest before/after comparison; use `report.html` when you want to scan metrics and query hot spots visually.

By default, the harness only writes artifacts and prints console output. To host the generated files after a run, opt in with `PERF_ARGS='--serve'`; the harness prints the `report.html` URL and waits until Ctrl-C.

1. Set benchmark duration and repetition count:

```sh
VECTIS_PERF_RUN_NAME=main VECTIS_PERF_BENCHTIME=5s VECTIS_PERF_COUNT=3 make perf SUITE=queue
```

2. Save the artifact directory printed by the harness.
3. Record Go version, OS, CPU model, and whether the run used a laptop, CI worker, or staging host.
4. Compare queue ops/sec and p95/p99 latency with the previous baseline for the same machine class:

```sh
make perf-compare BASELINE=artifacts/perf/main/go-bench.txt CURRENT=artifacts/perf/pr/go-bench.txt
```

5. Investigate changes larger than normal local variance before changing the published envelope.

Use shorter runs for quick local checks and repeated longer runs for baseline changes.

Useful knobs:

| Variable | Default | Purpose |
| --- | --- | --- |
| `VECTIS_PERF_BENCHTIME` | `2s` | Go benchmark duration per scenario. |
| `VECTIS_PERF_COUNT` | `1` | Repetition count. Use `3` or more for baseline capture. |
| `VECTIS_PERF_QUEUE_BENCH` | Queue round-trip, concurrent, sustained, and latency benches | Override to focus on one scenario. |
| `VECTIS_PERF_DAL_BENCH` | DAL hot-path benchmarks | Override to focus on one DAL scenario. |
| `VECTIS_PERF_JOB_BENCH` | Job executor benchmarks | Override to focus on one executor scenario. |
| `VECTIS_PERF_MACRO_BENCH` | API-to-terminal macro benchmarks | Override to focus on one macro scenario. |
| `VECTIS_PERF_MACRO_DATABASES` | unset | Optional comma-separated macro database matrix, such as `sqlite3,pgx_podman,pgx_podman_unsafe`. |
| `VECTIS_PERF_DATABASE_DRIVER` | `sqlite3` | Database driver used by macro benchmarks when not running a matrix. |
| `VECTIS_PERF_DATABASE_DSN` | `:memory:` for SQLite, required for `pgx` | Macro benchmark database DSN. Use only disposable databases. |
| `VECTIS_PERF_POSTGRES_DSN` | unset | Postgres DSN used for `pgx` matrix entries, so SQLite entries can keep using `:memory:`. |
| `VECTIS_PERF_POSTGRES_IMAGE` | `postgres:18-alpine` | Postgres image used by `pgx_podman` matrix entries. |
| `VECTIS_PERF_POSTGRES_DURABILITY` | `safe` | Podman Postgres durability profile. `pgx_podman_unsafe` sets this to `unsafe`. |
| `VECTIS_PERF_POSTGRES_PORT` | auto-selected | Optional fixed localhost port for `pgx_podman` matrix entries. |
| `VECTIS_PERF_PODMAN` | `podman` | Podman binary used by `pgx_podman` matrix entries. |
| `VECTIS_PERF_SQLITE_DSN` | unset | Optional SQLite DSN used for `sqlite3` matrix entries. |
| `VECTIS_PERF_DATABASE_MAX_OPEN_CONNS` | `1` for SQLite, `32` for Postgres | Macro benchmark SQL max open connections. |
| `VECTIS_PERF_DATABASE_MAX_IDLE_CONNS` | `1` for SQLite, up to `16` for Postgres | Macro benchmark SQL max idle connections. |
| `VECTIS_PERF_PG_STAT_STATEMENTS` | enabled for Postgres macro benchmarks | Set to `false` to skip `pg_stat_statements` reset and top-query output. |
| `VECTIS_PERF_PG_STAT_STATEMENTS_OUTPUT` | harness-generated | Optional direct `go test` output file for Postgres top-query lines. |
| `VECTIS_PERF_TRIGGER_CLIENTS` | `4` | Concurrent trigger clients used by macro trigger-to-terminal benchmarks. |
| `VECTIS_PERF_WORKERS` | `4` | Concurrent worker loops used by macro worker and trigger-to-terminal benchmarks. |
| `VECTIS_PERF_ARTIFACT_DIR` | `artifacts/perf` | Directory where harness artifacts are written. |
| `VECTIS_PERF_RUN_NAME` | timestamp and suite | Optional artifact run directory name. |
| `VECTIS_PERF_BASELINE` | unset | Optional baseline Go benchmark output for `benchstat` comparison during a queue run. |
| `VECTIS_PERF_SERVE` | `false` | Set to `true` or pass `--serve` to host the run directory after benchmark output. |
| `VECTIS_PERF_SERVE_HOST` | `127.0.0.1` | Host used by the opt-in artifact server. |
| `VECTIS_PERF_SERVE_PORT` | `0` | Port used by the opt-in artifact server. `0` auto-selects a free port. |
| `GO` | `go` | Go binary used to build the perf harness and run Go benchmarks. |
| `BENCHSTAT` | `benchstat` | `benchstat` binary used for benchmark comparisons. |

## Local DAL Benchmark Check

Use this check when a change affects run creation, idempotency, dispatch visibility, worker claims, lease renewal, finalization, or run-listing queries.

```sh
VECTIS_PERF_RUN_NAME=main-dal VECTIS_PERF_BENCHTIME=5s VECTIS_PERF_COUNT=3 make perf SUITE=dal
```

The DAL suite is SQLite-backed for local repeatability. Use it as a fast regression tripwire, then run a deployed-stack check against Postgres before making claims about production scale.

## Local Job Executor Benchmark Check

Use this check when a change affects executor behavior, built-in action overhead, workspace setup, subprocess creation, or durable worker log flush.

```sh
VECTIS_PERF_RUN_NAME=main-job VECTIS_PERF_BENCHTIME=5s VECTIS_PERF_COUNT=3 make perf SUITE=job
```

To compare process-spawn overhead against an in-process action result, focus the executor suite:

```sh
VECTIS_PERF_JOB_BENCH='BenchmarkExecutor_Execute(ShellTrue|ResultTrue)' make perf SUITE=job
```

## Local Macro Benchmark Check

Use this check when the architectural question crosses component boundaries. The macro suite includes in-process sequential and concurrent no-op API trigger paths through run creation, async queue enqueue, queue dequeue/ack, worker-style DB claim, shell execution, and terminal status update. It also includes a log-heavy variant that exercises worker durable log flush plus local log-store replay.

```sh
VECTIS_PERF_RUN_NAME=main-macro VECTIS_PERF_BENCHTIME=5s VECTIS_PERF_COUNT=3 make perf SUITE=macro
```

SQLite is the default local backend and uses an in-memory database with one SQL connection. To compare the same macro slice against disposable Postgres, use the Podman-backed matrix entry:

```sh
VECTIS_PERF_MACRO_DATABASES=sqlite3,pgx_podman,pgx_podman_unsafe \
make perf SUITE=macro
```

The matrix tags parsed benchmark rows with `db_sqlite3`, `db_pgx_podman`, or `db_pgx_podman_unsafe`. The Podman entries start `postgres:18-alpine`, preload `pg_stat_statements`, run the Go benchmark with `VECTIS_PERF_DATABASE_DRIVER=pgx`, and force-remove the disposable container afterward. The Podman machine or socket must already be running; on macOS, start it with `podman machine start`.

The `pgx_podman_unsafe` entry disables Postgres durability settings with `fsync=off`, `synchronous_commit=off`, and `full_page_writes=off`. Use it only for disposable local measurement. It approximates a lower bound for client/server, query, lock, and index overhead; it is not a deployable configuration.

The harness mirrors macro database settings into `VECTIS_DATABASE_DRIVER` for the benchmark child process so DAL SQL placeholder rebinding follows the selected backend. For Postgres macro benchmarks, the harness also writes `pg-stat-statements*.txt` sidecar artifacts, appends their `# pg_stat_statements` lines to the raw output after the Go benchmark rows, and includes the parsed final-iteration hot list in `summary.json` and `report.html`. Those lines show the top statements after setup has been reset out of the sample; when Go runs calibration passes, use the largest `iterations=` value for the representative sample.

To use an existing disposable Postgres database instead, pass a DSN and use `pgx`:

```sh
VECTIS_PERF_MACRO_DATABASES=sqlite3,pgx \
VECTIS_PERF_POSTGRES_DSN='postgres://vectis:vectis@127.0.0.1:5432/vectis_perf?sslmode=disable' \
make perf SUITE=macro
```

The DSN-backed matrix does not reset the Postgres database; benchmark job IDs are unique, but the database should still be disposable. To collect query-level stats with a DSN-backed Postgres database, preload and create the `pg_stat_statements` extension or set `VECTIS_PERF_PG_STAT_STATEMENTS=false`.

For worker scaling checks, either set `VECTIS_PERF_WORKERS` on a normal macro run or focus on the checked-in scaling curve:

```sh
VECTIS_PERF_MACRO_BENCH=BenchmarkMacro_WorkerScale_ClaimAckComplete make perf SUITE=macro
```

Use the result action checks when you want to remove subprocess creation from the macro workload and isolate worker, database, queue, and log-flush overhead:

```sh
VECTIS_PERF_MACRO_BENCH=BenchmarkMacro_WorkerScale_ResultActionClaimAckComplete make perf SUITE=macro
VECTIS_PERF_MACRO_BENCH=BenchmarkMacro_ResultActionWorkerClaimAckComplete make perf SUITE=macro
```

This is a fast macro regression check, not a deployment capacity claim. Follow it with the deployed stack check when worker count, Postgres, log service, network, TLS, or service process boundaries matter.

## Deployed Stack Check

Use this check when the question involves a real API, database, queue, orchestrator, worker fleet, log service, or dashboard.

1. Start a reference or staging stack with Postgres and durable log storage.
2. Run `vectis-cli health check --strict`.
3. Create one small source-backed shell job that is safe to run many times.
4. Trigger a small warm-up batch and confirm each run reaches a terminal status.
5. Trigger a measured burst of runs with idempotency keys.
6. Increase workers in steps and record queued-to-running and running-to-terminal latency at each step.
7. Stream logs from multiple clients for at least one active run.
8. Inspect metrics for queue depth, worker outcomes, API request status, DB pool waits, log drops, replay truncation, and reconciler re-enqueues.
9. Stop at the first sustained error, saturation, or operator-visible degradation and record the limit.
10. Let the system drain, then rerun `vectis-cli health check --strict`.

Do not treat a larger worker count as a win unless queue depth drains, DB pool pressure stays acceptable, and logs remain usable.

## What To Watch

| Area | Signals |
| --- | --- |
| API | Request status, latency, `429`, `503`, and dependency readiness. |
| Queue | Pending depth, in-flight deliveries, DLQ entries, dequeue rate, and expired requeues. |
| Workers | Jobs received, terminal outcomes, job duration, claim conflicts, and host CPU/memory/disk. |
| Database | Pool waits, open/in-use connections, query latency, storage growth, and host pressure. |
| Logs | Append failures, stream disconnects, replay truncation, spool growth, and storage free space. |
| Reconciler | Re-enqueue attempts, failures, and repeated repair for the same runs. |
| Users | CLI/API responsiveness and whether operators can still inspect runs and logs during load. |

Use `vectis-cli health check --format json` when you want to capture health evidence alongside Prometheus or dashboard snapshots.

## Stop Conditions

Stop the check and record the limit when any of these are sustained:

- API readiness fails or trigger requests return dependency errors.
- Queue depth grows without draining after load stops.
- DLQ receives unexpected entries.
- Worker terminal outcomes do not match the workload.
- DB pool waits continue climbing.
- Log streams become unusable or log storage/spool free space approaches the warning threshold.
- Reconciler repair attempts repeatedly fail.
- Operators can no longer inspect runs, dispatch events, or logs.

## Exit Criteria

A check passes when:

- No run is duplicated.
- No completed run is left nonterminal.
- Queue depth returns to baseline after load stops.
- DLQ remains empty unless the workload intentionally causes failures.
- API and log streams recover cleanly after clients disconnect.
- The observed limits fit within [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md), or the envelope is updated.

## After The Check

1. Save raw output and dashboard snapshots with the check record.
2. Record the observed safe range and the first limiting component.
3. Open follow-up issues for code, config, dashboards, alerts, or docs.
4. Update [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) when the check changes the known-safe range.
5. If retention, backups, or restore assumptions changed, update the reliability docs before relying on the new operating point.

## PR And Release Notes

When performance evidence matters, include:

- the command or workload generator used;
- the environment and deployment shape;
- before/after results or baseline comparison;
- the first limiting component, if found;
- whether operator docs or release notes need an envelope change.

If a change plausibly affects capacity but no check was run, say why. That is still useful review context.

## Future Representative Workloads

The current checks are intentionally simple. A fuller framework should add representative workloads before Vectis makes stronger production throughput claims:

| Workload shape | What it should cover |
| --- | --- |
| Small fast jobs | Queue handoff, execution claiming, and terminal-state churn. |
| Log-heavy jobs | Log append, replay, storage growth, and reader fan-out. |
| Cron bursts | Schedule-to-run latency and duplicate/missed enqueue detection. |
| Mixed API/admin traffic | Job listing, run lookup, auth/token paths, and rate-limit behavior under build traffic. |
| Failure-and-repair load | Reconciler behavior, retry exhaustion, DLQ pressure, and dispatch visibility. |

When those workloads exist, keep the operator docs focused on how to size and monitor a deployment, and keep the raw performance methodology here.

## Related Docs

| Need | Doc |
| --- | --- |
| Current operator envelope | [Capacity And Load Envelope](../../operating/capacity/capacity-load-envelope.md) |
| Scaling and restart behavior | [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md) |
| First-response signals | [Runbooks And Alerts](../../operating/reliability/runbooks.md) |
| Health check IDs and JSON output | [Health Check Catalog](../../operating/reference/health-check-catalog.md) |
| Backup and restore assumptions | [Backup And Restore](../../operating/reliability/backup-restore.md) |
