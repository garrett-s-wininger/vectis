# Retry And Backoff Policy

This page gives operators one place to answer: "what is Vectis retrying, for how long, and what happens when it gives up?"

## Metrics

Common retry helpers emit:

- `vectis_retries_total`
- `vectis_retries_exhausted_total`
- `vectis_retry_delay_seconds`

The common labels are `component`, `service`, and `operation`. `component` preserves the existing stable retry-path name. `service` and `operation` are derived from names such as `api.enqueue`, `queue/enqueue`, or `registry:register`; legacy single-token names use operation `retry`. Do not put run IDs, job IDs, addresses, SQL text, or error strings in any retry label. Some retry paths also emit spans, logs, or domain-specific counters instead of the common retry metrics; those gaps are called out below.

## Retry Inventory

| Path | Applies to | Attempts / deadline | Delay | Jitter | Current common metrics | Give-up outcome |
| --- | --- | --- | --- | --- | --- | --- |
| Shared `backoff.Retryer` default | Internal helpers | 5 tries | 500ms exponential, uncapped unless caller supplies a cap | No | Yes when caller passes metrics | Returns last error. |
| Generic gRPC client connect | Registry client and lower-level networking callers | 5 tries | 500ms exponential, uncapped | No | `component=networking` | Startup or dial path fails. |
| Pinned queue/log resolver dial | API, worker, cron, reconciler, log-forwarder | 5 tries | 500ms exponential, uncapped | No | `component=resolver` | Startup or dependency dial fails. |
| Registry registration | Queue and log when registration is enabled | 5 tries | 500ms exponential, uncapped | No | `component=registry` | Process startup fails when registration is required. |
| Queue enqueue | API async enqueue, cron, reconciler | 6 attempts | 80ms exponential capped at 2s | Full jitter | No common counter today; traces include enqueue attempt events | Triggered run can remain queued until reconciler retries, or the cycle logs failure. |
| Worker dequeue loop | Worker | Indefinite while process is running | 500ms exponential capped at 30s | No | Domain logs, no common counter today | Worker stops receiving new jobs until queue recovers. |
| Worker database claim/lease/finalize recovery | Worker | Indefinite for between-job DB recovery; 4 attempts for ack/finalize paths | 150ms exponential capped at 2s for ack/finalize; 500ms capped at 30s for DB backoff | No | Domain logs/spans, no common counter today | Run can be orphaned, failed, or left for lease/reconciler recovery depending on where exhaustion occurs. |
| Durable log stream send | Worker job execution | Retries until close flush deadline | 150ms exponential capped at 2s | No | Domain logs, no common counter today | Run fails if logs cannot flush before the deadline. |
| Postgres migration advisory lock | `vectis-cli database migrate` and startup migration checks through database helpers | 60s deadline | 750ms fixed sleep | No | No | Migration command returns failure. |
| Reconciler enqueue | Reconciler | Queue enqueue policy above for each selected run | Queue enqueue policy above | Queue enqueue policy above | Reconciler outcome metrics plus enqueue traces | Run remains queued for a later reconciler cycle or manual retry. |
| Log-forwarder delivery | Log-forwarder | Retries through persisted local spool behavior | See log-forwarder configuration and logs | No | Domain logs, no common counter today | Batch remains spooled or is quarantined when corrupt. |

## Operator-Tunable Inputs

Most retry attempt counts and backoff constants are currently code defaults. The operator knobs that affect retry behavior today are the surrounding dependency and cadence settings:

| Goal | Setting |
| --- | --- |
| Avoid registry dial/registration on dependent startup | Pin queue/log addresses with the role-specific discovery variables in [CONFIGURATION.md](../operator/configuration.md#service-discovery-vs-fixed-addresses). |
| Change resolver refresh after discovery errors | Override discovery timing settings: resolver refresh 10s, poll timeout 5s, error refresh 2s, registration heartbeat 45s by default. |
| Control how often queued runs are redispatched | `VECTIS_RECONCILER_INTERVAL` / `--interval` on `vectis-reconciler`. |
| Preserve queue backlog across queue restarts | Set `VECTIS_QUEUE_PERSISTENCE_DIR` or `--persistence-dir`. Empty disables queue persistence. |
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

- Queue enqueue exhaustion after API 202: keep `vectis-reconciler` running, inspect queued run age, and use `vectis-cli runs retry` only for manual intervention when automatic redispatch is insufficient.
- Registry or pinned dial exhaustion at startup: verify address, DNS, network policy, and `VECTIS_GRPC_TLS_SERVER_NAME` / certificate SANs.
- Worker ack/finalize exhaustion: inspect run status, worker logs, database availability, and queue delivery state. Avoid restarting repeatedly without checking whether the database can accept final status writes.
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
