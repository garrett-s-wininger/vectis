# Audit Event Catalog

This page catalogs API audit events written to `audit_log`. Use it when reviewing privileged changes, checking audit durability, building exports, or deciding which events deserve stronger durability in a deployment.

Audit events are emitted by `vectis-api`. They are separate from run dispatch history in `run_dispatch_events`, worker security evidence in `execution_security_events`, and catalog fan-in records in `cell_catalog_events`.

For storage fields and indexes, see [Database Schema Reference](./database-schema.md#audit_log). For audit configuration keys, see [Configuration Key Reference](./configuration-key-reference.md#api). For alerting signals, see [Metrics Catalog](./metrics-catalog.md#api-audit-sql-and-retention) and [Health Check Catalog](./health-check-catalog.md).

## Storage And Access

Audit emission is enabled by default through `api.audit.enabled`. When enabled, the API writes events to the application-owned SQL database through `audit_log`.

Use `GET /api/v1/audit/events`, `vectis-cli audit list --format json`, or `vectis-cli audit export --output audit-export.json` to review and retain audit-event evidence. The first-party list path supports `event_type`, `actor_id`, `target_id`, `correlation_id`, `since`, `until`, and `limit` filters. `since` and `until` accept RFC3339 timestamps or `YYYY-MM-DD` dates, and `limit` is bounded to 1-1000 rows. Results are ordered newest first by `created_at` and row ID.

| Field | Operator meaning |
| --- | --- |
| `event_type` | Stable event name from the catalog below. |
| `actor_id` | Local user that performed the action, when known. Anonymous setup failures and auth failures may be null. |
| `target_id` | Local numeric target when one simple target applies, such as a user, token, namespace, or binding subject. Job and run identifiers usually live in `metadata`. |
| `metadata` | Event-specific JSON. SQLite stores text; Postgres stores `JSONB`. |
| `ip_address` | Client IP after Vectis' trusted-proxy parsing. |
| `correlation_id` | Request correlation ID for joining audit rows with API logs and traces. |
| `created_at` | Event timestamp. |

Audit metadata can contain usernames, token labels, namespace paths, job IDs, run IDs, and operator-supplied repair reasons. Raw API tokens, passwords, CSRF/session tokens, and secret plaintext are not written by the audit emitters, but `audit_log` should still be treated as sensitive operational evidence.

For retention workflows, export the relevant range before deleting old audit rows, then pass that retained evidence to cleanup:

```sh
vectis-cli audit export --until 2026-07-01T00:00:00Z --output audit-export.json
vectis-cli retention cleanup --yes --audit-export audit-export.json --audit-export-max-age 24h
```

The retention gate accepts only unfiltered, untruncated exports whose range covers the audit cleanup cutoff and whose event hash matches the retained rows in the export file.

## Durability Policy

Every event has a default durability. Operators can override per-event behavior with `api.audit.durability_overrides` or `VECTIS_API_AUDIT_DURABILITY_OVERRIDES`, using comma-separated `event=durability` pairs such as:

```text
auth.success=disabled,run.triggered=best_effort,job.created=fail_closed
```

Unknown event names and unknown durability values are rejected during override parsing.

| Durability | Behavior |
| --- | --- |
| `disabled` | Do not emit the event. This also applies globally when `api.audit.enabled=false`. |
| `best_effort` | Enqueue asynchronously. If the audit buffer is unavailable or full, drop the event, log a warning, and increment the dropped-event metric. |
| `durable_best_effort` | Enqueue asynchronously when possible. If the event cannot be queued, attempt a synchronous insert before returning success to the API caller. Synchronous insert failures are logged and counted as audit flush failures. A failed async enqueue can still increment the API process-local dropped count before the synchronous fallback runs. |
| `fail_closed` | Attempt a synchronous insert and return any persistence error to the API audit caller. The API wrapper logs the error; current route handlers generally continue after that logged audit failure. |

The async auditor flushes batches in the background. Its default batch size is `100`, default flush interval is `1s`, and default in-memory buffer size is `1000` events.

## Durability Signals

| Signal | Meaning | First response |
| --- | --- | --- |
| `vectis_audit_events_dropped_total{event_type}` | A `best_effort` event was dropped before persistence, usually because the buffer was full or stopped. | Check API database health, API process pressure, and whether high-volume events should stay enabled. |
| `vectis_audit_flush_failures_total{event_count}` | A background flush or durable fallback insert failed. | Check database availability, SQL errors, and API logs with the same time window. |
| `GET /api/v1/audit/drops` | API process-local async enqueue failure count. For `best_effort`, this means a lost event; for `durable_best_effort`, the event may still have been persisted by synchronous fallback. | Used by `vectis-cli health check` as `audit.drops.recent`. |
| `GET /api/v1/audit/flush-failures` | API process-local flush-failure count. | Used by `vectis-cli health check` as `audit.flush.failures`. |

## Auth And Setup Events

| Event | Default durability | Emitted when | Target and metadata |
| --- | --- | --- | --- |
| `auth.success` | `best_effort` | Login succeeds, or a protected route authenticates through a session or API token. | `actor_id` is the authenticated user. Metadata may include `credential_type`, `credential_source`, `token_id`, `method`, and `username`. |
| `auth.failure` | `best_effort` | Login, session, API token, or token-scope authentication fails. | `actor_id` is set only when the user was resolved. Metadata may include `reason`, `username`, `credential_source`, and `token_id`. |
| `setup.bootstrap_failed` | `durable_best_effort` | Initial setup receives an invalid bootstrap token. | Metadata includes `reason=invalid_bootstrap_token`. |
| `setup.completed` | `fail_closed` | Initial auth setup creates the first local user and records setup completion. | `actor_id` and `target_id` are the new local user. Metadata includes `username`. |

Known `auth.failure` reasons include `invalid_session`, `invalid_token`, `token_scope_load_error`, `invalid_credentials`, and `user_disabled`.

## Identity And RBAC Events

| Event | Default durability | Emitted when | Target and metadata |
| --- | --- | --- | --- |
| `token.created` | `fail_closed` | An API token is created. | `target_id` is the token row. Metadata includes `label`, `expires_in`, `target_user`, and `scoped`. The raw token value is not stored. |
| `token.deleted` | `fail_closed` | An API token is deleted. | `target_id` is the token row. Metadata includes `owner_id`. |
| `password.changed` | `fail_closed` | A user password is changed by the user or an admin. | `target_id` is the affected user. Metadata includes `admin_override`. Password values are not stored. |
| `user.created` | `fail_closed` | A local user is created. | `target_id` is the new user. Metadata includes `username` and `generated_password`. |
| `user.updated` | `fail_closed` | A local user's enabled state changes. | `target_id` is the affected user. Metadata includes `enabled`. |
| `user.deleted` | `fail_closed` | A local user is deleted. | `target_id` is the deleted user. No metadata is emitted. |
| `namespace.created` | `durable_best_effort` | A namespace is created. | `target_id` is the namespace row. Metadata includes `name`, `parent_id`, and `path`. |
| `namespace.deleted` | `durable_best_effort` | A namespace is deleted. | `target_id` is the namespace row. Metadata includes `name` and `path`. |
| `binding.created` | `fail_closed` | A namespace role binding is created. | `target_id` is the local user receiving the role. Metadata includes `namespace_id` and `role`. |
| `binding.deleted` | `fail_closed` | A namespace role binding is deleted. | `target_id` is the local user losing the role. Metadata includes `namespace_id` and `role`. |

## Job And Run Events

| Event | Default durability | Emitted when | Target and metadata |
| --- | --- | --- | --- |
| `job.created` | `durable_best_effort` | A stored job is created. | `target_id` is empty. Metadata includes `job_id` and `namespace`. |
| `job.updated` | `durable_best_effort` | A stored job definition is updated. | `target_id` is empty. Metadata includes `job_id` and `namespace`. |
| `job.deleted` | `durable_best_effort` | A stored job is deleted. | `target_id` is empty. Metadata includes `job_id` and `namespace`. |
| `run.triggered` | `best_effort` | A stored, replayed, or ephemeral run is accepted by the API. | `target_id` is empty. Metadata includes `job_id`, `run_id`, `namespace`, and `invocation`; stored and replayed runs also include `run_index` and `target_cell`; ephemeral runs include `ephemeral=true`; replayed runs include `replay_of_run_id`. |
| `run.repair_marked` | `durable_best_effort` | An operator marks a run to a repair status. | `target_id` is empty. Metadata includes `run_id`, `namespace`, `status`, and optional `reason`. |
| `run.force_failed` | `durable_best_effort` | A legacy or force-fail repair path marks a run failed. | `target_id` is empty. Metadata includes `run_id`, `namespace`, and `reason`. |
| `run.force_requeued` | `durable_best_effort` | A legacy or force-requeue repair path requeues a run. | `target_id` is empty. Metadata includes `run_id` and `namespace`. |
| `run.cancelled` | `durable_best_effort` | A run cancellation request is recorded. | `target_id` is empty. Metadata includes `run_id`, `namespace`, and `delivery`, which is `worker_control` for fast-path delivery or `pending` for stored cancellation. |

## Source Repository Events

| Event | Default durability | Emitted when | Target and metadata |
| --- | --- | --- | --- |
| `source_repository.created` | `durable_best_effort` | A source repository registration is created. | `target_id` is empty. Metadata includes `repository_id`, `namespace`, and `source_kind`. |
| `source_repository.updated` | `durable_best_effort` | A source repository registration is updated. | `target_id` is empty. Metadata includes `repository_id`, `namespace`, `source_kind`, and `enabled`. |
| `source_repository.deleted` | `durable_best_effort` | A source repository registration is deleted. | `target_id` is empty. Metadata includes `repository_id`, `namespace`, and `source_kind`. |

## Related Docs

| Need | Doc |
| --- | --- |
| Audit table fields and indexes | [Database Schema Reference](./database-schema.md#audit_log) |
| Audit configuration keys and defaults | [Configuration Key Reference](./configuration-key-reference.md#api) |
| Audit drop and flush-failure metrics | [Metrics Catalog](./metrics-catalog.md#api-audit-sql-and-retention) |
| Health check IDs for audit durability | [Health Check Catalog](./health-check-catalog.md) |
| Run lifecycle and dispatch history | [Run, Task, And Queue State Reference](./run-state-reference.md) |
