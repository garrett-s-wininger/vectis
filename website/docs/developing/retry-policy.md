# Retry And Backoff Policy

Use this page when adding or changing retry behavior in Vectis.

Retries are part of the correctness contract. A retry loop should have a bounded purpose, observable behavior, and a clear give-up outcome. Operator-facing repair steps belong in [Repair Runbooks](../operating/reliability/repair-runbooks.md); this page explains the maintainer policy behind those behaviors.

## Design Rules

| Rule | Why it matters |
| --- | --- |
| Retry only operations that are safe to repeat. | Retries must not create duplicate runs, duplicate tokens, or hidden state changes. |
| Make give-up state explicit. | Operators need to know whether work is queued, orphaned, failed, spooled, or left for repair. |
| Prefer bounded retries for request/command paths. | Users and supervisors need a clear success/failure result. |
| Use long-running backoff only in service loops. | Workers and forwarders can self-heal while the process remains alive. |
| Emit common retry metrics when using shared retry helpers. | Alerts need stable labels and exhaustion counters. |
| Keep labels low-cardinality. | Retry metrics must not include run IDs, job IDs, addresses, SQL text, or error strings. |
| Add operator knobs only for real tradeoffs. | Tunables should control cadence, persistence, or dependency selection, not every internal constant. |

## Metrics

Common retry helpers emit:

- `vectis_retries_total`
- `vectis_retries_exhausted_total`
- `vectis_retry_delay_seconds`

The common labels are `component`, `service`, and `operation`. `component` preserves the existing stable retry-path name. `service` and `operation` are derived from names such as `api.enqueue`, `queue/enqueue`, or `registry:register`; legacy single-token names use operation `retry`. Do not put run IDs, job IDs, addresses, SQL text, or error strings in any retry label. Some retry paths also emit spans, logs, or domain-specific counters instead of the common retry metrics; those gaps are called out below.

## Idempotency And Safety

Before adding a retry loop, decide which safety mechanism makes repetition acceptable:

| Mechanism | Example |
| --- | --- |
| Idempotency key | API clients retrying job submission after a timeout. |
| Durable run row plus worker claim | Duplicate queue handoff should not execute the same run twice. |
| Lease or delivery token | Workers can distinguish ownership before writing final state. |
| Spool file | Log-forwarder batches survive process or network failure. |
| Read-only operation | Dial, health, list, and lookup retries do not mutate durable state. |

If none of these apply, do not add a blind retry loop. Return an explicit error and document the manual repair path.

## Retry Inventory

| Path | Applies to | Attempts / deadline | Delay | Jitter | Current common metrics | Give-up outcome |
| --- | --- | --- | --- | --- | --- | --- |
| Shared `backoff.Retryer` default | Internal helpers | 5 tries | 500ms exponential, uncapped unless caller supplies a cap | No | Yes when caller passes metrics | Returns last error. |
| Generic gRPC client connect | Registry client and lower-level networking callers | 5 tries | 500ms exponential, uncapped | No | `component=networking` | Startup or dial path fails. |
| Pinned queue/log/artifact/orchestrator resolver dial | API, worker, cron, reconciler, log-forwarder, artifact clients | 5 tries | 500ms exponential, uncapped | No | `component=resolver` | Startup or dependency dial fails. |
| Registry registration | Queue, log, artifact, and orchestrator when registration is enabled | 5 tries | 500ms exponential, uncapped | No | `component=registry` | Process startup fails when registration is required. |
| Queue enqueue | API async enqueue, cron, reconciler | 6 attempts | 80ms exponential capped at 2s | Full jitter | API emits `vectis_api_run_enqueue_total`; reconciler emits `vectis_reconciler_reenqueue_total`; traces include enqueue attempt events | Triggered run can remain queued until reconciler retries, or the cycle logs failure. |
| Task continuation enqueue | Worker direct fan-out after orchestrator completion | One enqueue attempt for each activated child returned by the orchestrator | Caller path decides; no dedicated continuation retry policy yet | No | Worker traces include `task.dispatch.direct`; queue backlog and worker outcome metrics show pressure | Worker marks the task delivery failed if a child continuation cannot be handed to the queue. |
| Worker dequeue loop | Worker | Indefinite while process is running | 500ms exponential capped at 30s | No | Domain logs, no common counter today | Worker stops receiving new jobs until queue recovers. |
| Worker orchestrator claim/lease/finalize recovery | Worker | Claim returns the orchestrator decision; lease renew retries on the next interval; completion retries 4 attempts | 150ms exponential capped at 2s for completion | No | Domain logs/spans, no common counter today | Run can be left for queue delivery, catalog visibility, or operator/reconciler recovery depending on where exhaustion occurs. |
| Worker database visibility recovery | Worker | 4 attempts for orphan/failure marking; catalog event writes are best effort in the worker path | 150ms exponential capped at 2s for mark paths; 500ms capped at 30s for DB backoff | No | Domain logs/spans and `vectis_worker_db_unavailable` | Run visibility or repair state can lag the hot execution state until the database recovers. |
| Durable log stream send | Worker task delivery execution | Retries until close flush deadline | 150ms exponential capped at 2s | No | Domain logs, no common counter today | Run fails if logs cannot flush before the deadline. |
| Postgres migration advisory lock | `vectis-cli database migrate` and startup migration checks through database helpers | 60s deadline | 750ms fixed sleep | No | No | Migration command returns failure. |
| Reconciler enqueue | Reconciler | Queue enqueue policy above for each selected run | Queue enqueue policy above | Queue enqueue policy above | Reconciler outcome metrics plus enqueue traces | Run remains queued for a later reconciler cycle or manual retry. |
| Log-forwarder delivery | Log-forwarder | Retries through persisted local spool behavior | See log-forwarder configuration and logs | No | Domain logs, no common counter today | Batch remains spooled or is quarantined when corrupt. |

## Operator-Tunable Inputs

Most retry attempt counts and backoff constants are currently code defaults. Operator knobs affect retry behavior through surrounding dependency, persistence, and cadence settings:

| Goal | Setting |
| --- | --- |
| Avoid registry dial/registration on dependent startup | Pin queue/log/artifact/orchestrator addresses with the role-specific discovery variables in [Configuration](../operating/configuration.md#service-discovery-vs-fixed-addresses). |
| Change resolver refresh after discovery errors | Override discovery timing settings: resolver refresh 10s, poll timeout 5s, error refresh 2s, registration heartbeat 45s by default. |
| Control how often queued runs are redispatched | `VECTIS_RECONCILER_INTERVAL` / `--interval` on `vectis-reconciler`. |
| Preserve queue backlog across queue restarts | Keep queue persistence enabled using the derived default path, or set `VECTIS_QUEUE_PERSISTENCE_DIR` / `--persistence-dir` explicitly. Empty disables queue persistence. |
| Preserve worker-to-log-forwarder batches | Configure a durable spool directory for the log-forwarder. |
| Bound database connection churn during outages | Tune the Postgres `VECTIS_DATABASE_PGX_*` pool settings. |

When adding a new retry policy, prefer a named setting only when operators can make a useful tradeoff. Otherwise keep the code default and document the failure outcome here.

## Alert Examples

Prometheus-style examples:

```promql
increase(vectis_retries_exhausted_total[10m]) > 0
```

Page when any common retry loop exhausts. Route by `component`.

```promql
sum by (component) (rate(vectis_retries_total[5m])) > 1
```

Warn when a retry path is continuously retrying. Tune the threshold per environment.

```promql
histogram_quantile(0.95, sum by (le, component) (rate(vectis_retry_delay_seconds_bucket[10m]))) > 5
```

Warn when common retry paths spend most of their time near long delays.

For paths that do not yet use common retry metrics, alert on the corresponding domain signal: API readiness failures, queued-run age, reconciler enqueue failures, worker job failure outcomes, log append failures, and log-forwarder spool growth.

## Repair Guidance

Keep this section as a short index. Detailed operator steps belong in [Repair Runbooks](../operating/reliability/repair-runbooks.md), [Dispatch Visibility](../operating/reliability/dispatch-visibility.md), and [Health Check Catalog](../operating/reference/health-check-catalog.md).

- Queue enqueue exhaustion after API 202: keep `vectis-reconciler` running, inspect queued run age, and use `vectis-cli runs retry` only for manual intervention when automatic redispatch is insufficient.
- Registry or pinned dial exhaustion at startup: verify address, DNS, network policy, and `VECTIS_GRPC_TLS_SERVER_NAME` / certificate SANs.
- Worker ack/finalize exhaustion: inspect run status, worker logs, `vectis_worker_lifecycle_state`, `vectis_worker_db_unavailable`, orchestrator health, database availability, and queue delivery state. Avoid restarting repeatedly without checking whether the orchestrator and database can accept final state writes.
- Log retry exhaustion: inspect `vectis-log` health, storage directory writability, and worker spool warnings before retrying the run.
- Migration lock exhaustion: confirm no other migrator is still running, then rerun `vectis-cli database migrate` after the lock holder exits.

## Adding Or Changing Retries

New retry loops should document:

- Owning binary and operation.
- Max attempts or deadline.
- Base delay, cap, and jitter.
- Whether common retry metrics are emitted.
- What state is left behind when retries exhaust.
- The operator action that repairs the state.

Also update tests for:

- success on first attempt;
- success after transient failure;
- exhaustion behavior;
- context cancellation or shutdown;
- permanent errors that should not back off;
- metrics, logs, or spans when the path is observable.

## Related Docs

| Need | Doc |
| --- | --- |
| Client retry behavior | [Idempotency And Retries](../using/idempotency-and-retries.md) |
| Operator repair steps | [Repair Runbooks](../operating/reliability/repair-runbooks.md) |
| Queue handoff repair | [Dispatch Visibility](../operating/reliability/dispatch-visibility.md) |
| Retry alerts | [Runbooks And Alerts](../operating/reliability/runbooks.md) |
| Configuration knobs | [Configuration](../operating/configuration.md) |
