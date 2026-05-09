# Capacity Drills

Capacity drills keep the operating envelope honest. Run them before major releases, before onboarding a larger workload class, and after changing queue, API, worker, log, cron, or database behavior.

## Drill Record

For every drill capture:

- Date, operator, git commit, build version.
- Deployment shape: API/queue/log/cron/reconciler/worker counts.
- Database driver, DSN class, pool settings, and host size.
- Log storage medium and free space.
- Exact command or workload generator configuration.
- Benchmark output, Prometheus snapshots, and notable logs.
- Pass/fail decision and follow-up issues.

## Benchmark Drill

1. Run `make capacity-benchmark`.
2. Save the raw benchmark output.
3. Compare queue ops/sec and p95/p99 latency metrics with the previous baseline for the same machine class.
4. Investigate changes larger than normal local variance before changing the published envelope.

## Deployed Stack Drill

1. Start a reference stack with Postgres and durable log storage.
2. Create one small stored shell job.
3. Trigger a burst of runs with idempotency keys.
4. Increase workers in steps and record queued-to-running and running-to-terminal latency.
5. Stream logs from multiple clients for at least one active run.
6. Inspect metrics for queue depth, worker outcomes, API request status, DB pool waits, log drops, and replay truncation.
7. Stop at the first sustained error, saturation, or operator-visible degradation and record the limit.

## Exit Criteria

A drill passes when:

- No run is duplicated.
- No completed run is left nonterminal.
- Queue depth returns to baseline after load stops.
- DLQ remains empty unless the workload intentionally causes failures.
- API and log streams recover cleanly after clients disconnect.
- The observed limits fit within [CAPACITY_LOAD_ENVELOPE.md](CAPACITY_LOAD_ENVELOPE.md), or the envelope is updated.
