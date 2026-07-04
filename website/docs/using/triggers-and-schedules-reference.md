# Triggers And Schedules Reference

Vectis creates runs through trigger paths. The current public user paths are one-off job submission, stored-job trigger, and run replay. Cron schedules are durable database-backed schedules processed by `vectis-cron`; the shipped HTTP/CLI surface exposes cron status, but not a public create/update schedule API.

For run lifecycle states after a trigger creates work, see [Run, Task, And Queue State Reference](../operating/reference/run-state-reference.md). For retry behavior on client-submitted trigger requests, see [Idempotency And Retries](./idempotency-and-retries.md).

## Trigger Paths

| Path | CLI | HTTP route | Trigger type | Definition source | Notes |
| --- | --- | --- | --- | --- | --- |
| Ephemeral run | `vectis-cli jobs run PATH_OR_-` | `POST /api/v1/jobs/run` | `manual` | Request body job JSON | Stores an ephemeral definition, creates one run, and returns `202 Accepted`. |
| Stored-job trigger | `vectis-cli jobs trigger <job-id>` | `POST /api/v1/jobs/trigger/{id}` | `manual` | Latest stored job definition version | Can target multiple cells with repeated `--cell`; returns one or more run IDs. |
| Replay | `vectis-cli runs replay <run-id>` | `POST /api/v1/runs/{id}/replay` | `replay` | Captured source run definition payload | Creates a new run and records `replay_of_run_id`; it does not mutate the source run. |
| Cron schedule fire | `vectis-cron` process | Internal service loop; status via `GET /api/v1/cron/status` | `cron` | Stored job definition version at fire time | Uses durable schedule claims and one run per schedule/time pair. |

`webhook` is a stored trigger type value reserved in the data model. There is no public webhook trigger route in the current API.

## Run Audit Fields

Run list/detail responses include trigger lineage when present:

| Field | Meaning |
| --- | --- |
| `trigger_invocation_id` | Durable invocation identifier created when a trigger path starts a run. |
| `trigger_id` | Stored trigger row ID when the invocation came from a stored trigger, such as cron. |
| `trigger_type` | `manual`, `cron`, `replay`, or reserved `webhook`. |
| `trigger_payload_hash` | Hash of trigger payload metadata. Payload contents are not exposed through run responses. |
| `requested_cells` | Target cell IDs requested by the trigger path. |
| `replay_of_run_id` | Source run ID when a run was created by replay. |
| `definition_version` | Stored job definition version used for the run. |
| `definition_hash` | Hash of the job definition used for the run. |
| `execution_payload_hash` | Hash of the frozen execution payload handed to the execution cell. |

These fields are audit and troubleshooting metadata. Use the run ID and run index as the primary run identity.

## Manual Triggers

Stored-job trigger requests create a `trigger_invocations` row with `trigger_type=manual`, then create run rows for the selected cells and dispatch asynchronously. Queue or cell-ingress handoff happens after the API has accepted the request, so a successful trigger response means Vectis durably created the run, not that a worker has started it.

The CLI supports:

```sh
vectis-cli jobs trigger build-main --idempotency-key trigger-2026-06-19
vectis-cli jobs trigger build-main --cell iad-a --cell pdx-b
vectis-cli jobs run scratch.json --cell iad-a --idempotency-key run-2026-06-19
```

`POST /api/v1/jobs/trigger/{id}` and `POST /api/v1/jobs/run` accept `Idempotency-Key`. Reusing the same key for the same operation returns the stored response when available; reusing it for a different operation returns an idempotency conflict.

## Replay

Replay creates a fresh queued run from the source run's captured execution payload. It records `trigger_type=replay`, sets `replay_of_run_id`, and returns the new run ID and run index.

The CLI supports:

```sh
vectis-cli runs replay <run-id>
vectis-cli runs replay <run-id> --cell iad-a --idempotency-key replay-2026-06-19
```

Replay is not a repair operation. It leaves the source run unchanged and creates a separate run history entry. Source runs that are not replayable return HTTP `409` with error code `source_run_not_replayable`.

## Cron Schedule Model

Cron schedules use these SQL tables:

| Table | Purpose |
| --- | --- |
| `job_triggers` | Stored trigger metadata with `job_id`, `trigger_type`, `enabled`, and `home_cell`. |
| `cron_trigger_specs` | One cron schedule per trigger with `cron_spec`, `next_run_at`, `claim_token`, and `claimed_until`. |
| `cron_schedule_fires` | Idempotency fence for scheduled runs, keyed by `(schedule_id, scheduled_for)`. |
| `trigger_invocations` | Audit record for each manual, cron, or replay trigger invocation. |

`vectis-cron` uses a five-field cron parser: minute, hour, day of month, month, and day of week. Seconds are not part of the current schedule expression. The service waits until the next minute boundary and then polls every `60s`.

For each ready schedule, `vectis-cron`:

1. Reads enabled schedules where `next_run_at <= now` and no active claim exists.
2. Validates `cron_spec` and calculates the next scheduled fire time after the current time.
3. Calls `ClaimDue` with the observed `next_run_at`, a claim token, and `claimed_until`.
4. Creates or reuses the run for `(schedule_id, scheduled_for)` through `CreateScheduledRun`.
5. Dispatches the run to queue or cell ingress.
6. Calls `CompleteClaim` to advance `next_run_at` and clear claim fields.
7. Calls `ReleaseClaim` when dispatch fails before completion.

For overdue schedules, the stored `next_run_at` is the scheduled fire being claimed. The current wall-clock minute does not need to match `cron_spec`; after one catch-up firing, `vectis-cron` advances `next_run_at` to the next future time from the cron expression.

The claim token includes cron instance ID, schedule ID, scheduled time, and a local sequence number. Set `--instance-id` or `VECTIS_CRON_INSTANCE_ID` to make claim ownership readable in logs and database rows.

## Cron Configuration

| Key or flag | Default | Meaning |
| --- | --- | --- |
| `cron.claim_ttl` / `VECTIS_CRON_CLAIM_TTL` / `--claim-ttl` | `5m` | How long a cron instance owns a due schedule claim before another instance may claim it. |
| `VECTIS_CRON_INSTANCE_ID` / `--instance-id` | Hostname fallback | Stable cron instance identifier used in schedule claim tokens. |

Multiple `vectis-cron` instances can run against one shared database. They race on schedule claims; only the winner should create or reuse the scheduled run for that due tick.

## Cron Status

Operators can inspect schedule pressure with:

```sh
curl /api/v1/cron/status
vectis-cli health check --strict
```

`GET /api/v1/cron/status` returns:

| Field | Meaning |
| --- | --- |
| `schedule_count` | Enabled cron schedules. |
| `due_count` | Enabled schedules due and not currently held by an active claim. |
| `claimed_count` | Due schedules currently held by active claims. |
| `oldest_due_unix` | Oldest due schedule timestamp, as Unix seconds, when any enabled schedule is due. |
| `active` | `true` when at least one enabled schedule exists. |

The health check ID `cron.schedules` warns when schedules are due or held by active claims. A warning usually means checking `vectis-cron`, database connectivity, queue or cell-ingress handoff, and cron process logs.

## Boundaries

Current boundaries:

- no public schedule create/update/delete API;
- no public webhook trigger route;
- no seconds field in cron expressions;
- no built-in cron schedule sharding beyond database claim fencing;
- no guarantee that API acceptance means queue handoff has already completed.

Use run dispatch events and run detail when a trigger created a run but it has not reached a worker.

## Related Documentation

| Need | Document |
| --- | --- |
| Run status and dispatch interpretation | [Run, Task, And Queue State Reference](../operating/reference/run-state-reference.md) |
| API route contract | [API Reference](./api-reference.md) |
| Idempotent trigger retries | [Idempotency And Retries](./idempotency-and-retries.md) |
| SQL tables | [Database Schema](../operating/reference/database-schema.md#job-and-trigger-tables) |
| Cron health check | [Health Check Catalog](../operating/reference/health-check-catalog.md#active-checks) |
| Cron scaling behavior | [Scaling And Restarts](../operating/deployment/scaling-and-restarts.md#singleton-services) |
