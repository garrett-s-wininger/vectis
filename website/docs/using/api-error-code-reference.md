# API Error Code Reference

This page catalogs Vectis HTTP API error `code` values. Use it when writing clients, deciding retry behavior, triaging support tickets, or matching API failures to configuration, RBAC, route policy, and subsystem health.

For the route table and request/response shapes, see [API Reference](./api-reference.md). For auth actions and token scopes, see [Authorization Reference](../operating/reference/authorization-reference.md). For compatibility expectations, see [Compatibility](../concepts/compatibility.md).

## Error Envelope

Most API failures use this JSON envelope:

```json
{
  "code": "invalid_request_body",
  "message": "invalid request body",
  "details": {"field": "optional structured details"}
}
```

`code` is the stable machine-readable value. `message` is human-readable and may become clearer without changing the code meaning. `details` is optional and code-specific; clients should ignore unknown detail keys.

`401` responses include `WWW-Authenticate: Bearer`. `405` responses include `Allow`. `429` responses include `Retry-After`.

## Retry Guidance

| Class | Retry posture |
| --- | --- |
| Request shape and validation errors, usually `400` | Do not retry unchanged. Fix the request. |
| Authentication and authorization errors, usually `401` or `403` | Retry only after credentials, setup, RBAC, CSRF, CORS, or browser metadata are corrected. |
| Missing resources, usually `404` | Retry only if another process may create the resource. Namespace authorization can also hide a resource as `404`. |
| Conflicts, usually `409` | Retry only after reconciling state. Idempotency conflicts require the original request body or a new key. |
| Rate limits, `429` | Retry after `Retry-After` with jitter. |
| Bad gateway and unavailable dependency errors, `502` or `503` | Retry with backoff after checking dependency health. Use `Idempotency-Key` on supported mutation routes. |
| Internal errors, `500` | Retry cautiously with backoff only when the operation is idempotent or protected by `Idempotency-Key`; otherwise inspect server logs first. |

## Request And Security Controls

| Code | Typical status | Meaning |
| --- | --- | --- |
| `cors_origin_forbidden` | `403` | Browser CORS origin is not allowed. |
| `csrf_origin_forbidden` | `403` | Cookie-authenticated unsafe request has an invalid or missing same-origin `Origin` / `Referer`. |
| `csrf_token_required` | `403` | Cookie-authenticated unsafe request is missing a valid `X-CSRF-Token`. |
| `fetch_metadata_forbidden` | `403` | Browser Fetch Metadata headers describe a forbidden request shape. |
| `invalid_host_header` | `400` | `Host` is malformed or outside the API host allowlist. |
| `invalid_query_parameter` | `400` | Query string is malformed, repeated, or not allowed on the route. |
| `invalid_request_body` | `400` | JSON could not be decoded or did not match the route request shape. |
| `invalid_request_header` | `400` | A guarded header is duplicated, malformed, or not allowed on this route. |
| `invalid_request_target` | `400` | Request target is not a canonical origin-form API path. |
| `method_not_allowed` | `405` | Path exists but does not accept the request method. |
| `method_override_forbidden` | `400` | Method override headers are rejected. |
| `not_acceptable` | `406` | `Accept` does not allow any response type the route can produce. |
| `rate_limit_exceeded` | `429` | API rate limiter rejected the request. |
| `request_body_not_allowed` | `400` | Route does not accept a request body. |
| `request_body_too_large` | `413` | Request body exceeds the route limit. |
| `request_read_failed` | `500` | Server failed while reading the request body. |
| `route_not_found` | `404` | No API route matched the path. |
| `unsupported_media_type` | `415` | JSON route received a non-JSON `Content-Type`. |

## Auth And Setup

| Code | Typical status | Meaning |
| --- | --- | --- |
| `admin_password_too_short` | `400` | Initial setup admin password is too short. |
| `auth_not_configured` | `503` | API auth repository is unavailable to the route. |
| `auth_unavailable` | `503` | Authentication persistence or setup state cannot be used. |
| `authentication_required` | `401` | Bearer token, API token, or session credential is missing, invalid, expired, or malformed. |
| `authorization_denied` | `403` | Authenticated principal lacks the required action or namespace permission. |
| `bootstrap_not_configured` | `503` | Setup bootstrap token is missing or too short. |
| `invalid_admin_password` | `400` | Initial setup admin password is invalid. |
| `invalid_admin_username` | `400` | Initial setup admin username is invalid. |
| `invalid_bootstrap_token` | `401` | Setup bootstrap token does not match configuration. |
| `missing_admin_username` | `400` | Initial setup request omitted `admin_username`. |
| `missing_credentials` | `400` | Login request omitted username or password. |
| `setup_already_complete` | `409` | Initial setup was already completed. |
| `setup_required` | `503` | Initial setup must complete before the requested route can be used. |

## Users, Tokens, Namespaces, And RBAC

| Code | Typical status | Meaning |
| --- | --- | --- |
| `binding_already_exists` | `409` | Namespace role binding already exists. |
| `binding_not_found` | `404` | Namespace role binding does not exist. |
| `invalid_expires_in` | `400` | Token expiry duration is invalid. |
| `invalid_id` | `400` | Path ID is missing, malformed, or not positive. |
| `invalid_namespace_id` | `400` | Namespace ID is missing, malformed, or not positive. |
| `invalid_namespace_name` | `400` | Namespace name is invalid. |
| `invalid_new_password` | `400` | New password is invalid. |
| `invalid_password` | `400` / `401` | Password input is invalid, or current password authentication failed. |
| `invalid_role` | `400` | Role is not one of the supported namespace roles. |
| `invalid_scope_action` | `400` | Token scope action is unknown or setup-only. |
| `invalid_user_id` | `400` | User ID is missing, malformed, or not positive. |
| `invalid_username` | `400` | Username is invalid. |
| `last_admin_delete_forbidden` | `400` | The last root admin cannot be deleted. |
| `last_admin_disable_forbidden` | `400` | The last root admin cannot be disabled. |
| `missing_current_password` | `400` | Password change request omitted `current_password`. |
| `missing_enabled` | `400` | User update request omitted `enabled`. |
| `missing_label` | `400` | Token create request omitted `label`. |
| `missing_local_user_id` | `400` | Role-binding create request omitted `local_user_id`. |
| `missing_name` | `400` | Required name field or path value is missing. |
| `missing_namespace_path` | `400` | Namespaced token scope omitted `namespace_path`. |
| `missing_new_password` | `400` | Password change request omitted `new_password`. |
| `missing_role` | `400` | Role-binding request omitted `role`. |
| `missing_username` | `400` | User create request omitted `username`. |
| `namespace_already_exists` | `409` | Namespace path already exists. |
| `namespace_has_children` | `409` | Namespace cannot be deleted while children exist. |
| `namespace_has_jobs` | `409` | Namespace cannot be deleted while jobs exist. |
| `namespace_not_empty` | `409` | Namespace still has children or jobs. |
| `namespace_not_found` | `400` / `404` | Namespace path or ID does not exist, or is hidden by authorization. |
| `namespace_path_forbidden` | `400` | Global token scope included a namespace path. |
| `namespace_repository_unavailable` | `503` | Namespace repository is unavailable. |
| `namespaces_not_configured` | `503` | API server was not wired with namespace storage. |
| `new_password_too_short` | `400` | New password is too short. |
| `parent_namespace_not_found` | `404` | Parent namespace for a create request does not exist. |
| `password_too_short` | `400` | User password is too short. |
| `role_bindings_not_configured` | `503` | API server was not wired with role-binding storage. |
| `root_namespace_delete_forbidden` | `403` | Root namespace cannot be deleted. |
| `system_namespace_delete_forbidden` | `403` | A protected system namespace cannot be deleted. |
| `unsupported_namespace` | `400` | Route does not support the requested namespace path. |
| `scoped_token_scope_required` | `403` | A scoped token tried to create an unscoped token. |
| `self_delete_forbidden` | `400` | A user cannot delete itself. |
| `self_disable_forbidden` | `400` | A user cannot disable itself. |
| `token_not_found` | `404` | API token does not exist or is not visible to the caller. |
| `user_not_found` | `400` / `404` | User does not exist or is hidden by the route context. |
| `user_not_found_or_disabled` | `400` | Target user does not exist or is disabled. |
| `username_already_exists` | `409` | Username already exists. |

## Jobs, Runs, Logs, Artifacts, And Idempotency

| Code | Typical status | Meaning |
| --- | --- | --- |
| `artifact_blob_mismatch` | `502` | Artifact service returned bytes that do not match the manifest digest. |
| `artifact_blob_unavailable` | `502` | Artifact blob could not be read from the artifact service. |
| `artifact_not_found` | `404` | Artifact manifest does not exist for the run. |
| `artifact_service_error` | `502` | API could not connect to or read from artifact service. |
| `artifacts_not_configured` | `503` | Artifact repository is unavailable to the API. |
| `execution_payload_not_found` | `404` | Frozen execution payload is absent for the run. |
| `idempotency_in_progress` | `409` | Another request with the same idempotency key is still in progress. |
| `idempotency_key_reused` | `409` | Idempotency key was reused with a different request shape. |
| `invalid_after_index` | `400` | `after_index` must be a non-negative integer. |
| `invalid_cell_id` | `400` | Cell filter or target cell options are invalid or contradictory. |
| `invalid_job_definition` | `400` | Job document failed parsing or validation. `details.fields` may describe field-level failures. |
| `invalid_replay_limit` | `400` | Log replay limit is outside the allowed range. |
| `invalid_replay_options` | `400` | Replay body contains invalid or contradictory target-cell options. |
| `invalid_since` | `400` | `since` is not an RFC3339 timestamp or `YYYY-MM-DD` date. |
| `invalid_since_sequence` | `400` | Log sequence cursor is not a non-negative integer. |
| `invalid_stored_job_definition` | `500` | Stored job definition could not be parsed by the server. |
| `invalid_tail` | `400` | Log tail limit is outside the allowed range. |
| `invalid_trigger_options` | `400` | Trigger body contains invalid or contradictory target-cell options. |
| `invalid_version` | `400` | Job version query parameter is invalid. |
| `job_already_exists` | `409` | Stored job ID already exists. |
| `job_id_mismatch` | `400` | Path job ID and JSON body job ID differ. |
| `job_not_found` | `404` | Job does not exist or is hidden by namespace authorization. |
| `job_version_not_found` | `404` | Requested stored job definition version does not exist. |
| `log_service_error` | `502` | API could not connect to or read from the log service. |
| `log_service_unavailable` | `503` | API has no usable log service connection. |
| `missing_id` | `400` | Required path ID is missing. |
| `missing_job_id` | `400` | Job definition omitted a job ID. |
| `run_not_executing` | `409` | Run is not in a state that can accept the requested execution operation. |
| `run_not_found` | `404` | Run does not exist or is hidden by namespace authorization. |
| `run_repair_conflict` | `409` | Run cannot be repair-marked from its current status. |
| `run_requeue_conflict` | `409` | Run cannot be requeued from its current status. |
| `run_requeue_forbidden` | `409` | Succeeded run cannot be requeued. |
| `source_definition_not_found` | `404` | Replay source run's stored definition version is absent. |
| `source_run_not_replayable` | `409` | Source run cannot be replayed from its current status. |
| `streaming_unsupported` | `500` | Handler expected a streaming response writer but it was unavailable. |

## Source Repositories And Config-As-Code

| Code | Typical status | Meaning |
| --- | --- | --- |
| `checkout_path_forbidden` | `400` | API-supplied source repository checkout path is outside the configured source checkout root. |
| `incompatible_authoring_mode` | `400` | Requested source authoring mode cannot be used with the repository checkout mode. |
| `invalid_fallback_remote_url` | `400` | Source repository fallback remote URL is not safe to pass to Git. |
| `invalid_job_id` | `400` | Job ID cannot be mapped to a valid source definition path. |
| `invalid_recursive` | `400` | Source tree `recursive` query value is not a boolean. |
| `invalid_repository_id` | `400` | Repository ID cannot be used to derive a managed checkout path. |
| `invalid_run_definition` | `500` | Captured source run definition is invalid. |
| `invalid_source_reference` | `400` | Source ref, branch, path, expected head, or related source selector is invalid. |
| `invalid_source_schedule_override` | `400` | Source schedule override update is invalid. |
| `invalid_source_schedule_update` | `400` | Source schedule enable/disable update is invalid. |
| `missing_checkout_path` | `400` | External checkout repository registration omitted `checkout_path`. |
| `missing_job_definition` | `400` | Source authoring request omitted the job definition payload. |
| `missing_repository_id` | `400` | Reusable source-backed job route omitted `repository_id`. |
| `missing_run_id` | `400` | Source repository run-log route omitted `run_id`. |
| `missing_schedule_id` | `400` | Source schedule route omitted `schedule_id`. |
| `missing_source_schedule_enabled` | `400` | Source schedule patch omitted `enabled`. |
| `missing_source_schedule_override` | `400` | Source schedule override omitted both `ref` and `path`. |
| `repository_id_mismatch` | `400` | Repository ID in the request body does not match the route or query. |
| `run_definition_not_found` | `404` | Frozen run definition snapshot is missing. |
| `schedules_not_configured` | `503` | Source schedule storage is not configured. |
| `source_authoring_unavailable` | `409` | Source repository does not support the requested local definition write. |
| `source_busy` | `409` | Source repository is processing another checkout write; retry after it completes. |
| `source_conflict` | `409` | Source write conflicted with the current branch head or repository state. |
| `source_definition_already_exists` | `409` | A source definition already exists at the target path. |
| `source_file_too_large` | `413` | Source definition file exceeds the route size limit. |
| `source_job_conflict` | `409` | Source job write conflicted with another stored job or source mapping. |
| `source_not_found` | `404` | Source ref, tree entry, or definition path was not found. |
| `source_ref_hydration_in_flight` | `202` | Requested source ref is being hydrated by another API replica; retry after the `Retry-After` delay. |
| `source_repositories_not_configured` | `503` | Source repository storage is not configured. |
| `source_repository_conflict` | `409` | Source repository registration conflicts with an existing checkout path or repository. |
| `source_repository_declared` | `409` | Source repository is still declared in current config and cannot be deleted directly. |
| `source_repository_disabled` | `409` | Source repository is disabled for the requested operation. |
| `source_repository_in_use` | `409` | Source repository still has schedules or recorded source provenance. |
| `source_repository_not_found` | `404` | Source repository does not exist or is hidden by namespace authorization. |
| `source_run_lister_not_configured` | `503` | Source-backed run listing storage is not configured. |
| `source_run_starter_not_configured` | `503` | Source-backed run creation storage is not configured. |
| `source_schedule_declared` | `409` | Source schedule is still declared in current config. |
| `source_schedule_enabled` | `409` | Source schedule must be disabled before deletion. |
| `source_schedule_not_found` | `404` | Source schedule does not exist. |
| `source_schedule_override_active` | `409` | Source schedule override must be cleared before deletion. |
| `unsupported_authoring_mode` | `400` | Source repository authoring mode is unsupported. |
| `unsupported_checkout_mode` | `400` | Source repository checkout mode is unsupported. |
| `unsupported_worker_cache_mode` | `400` | Source repository worker cache mode is unsupported. |
| `unsupported_source_kind` | `400` | Source repository kind is unsupported. |

## Catalog, Cells, And Infrastructure

| Code | Typical status | Meaning |
| --- | --- | --- |
| `catalog_events_unavailable` | `503` | Cell catalog event inbox is not configured. |
| `database_not_ready` | `503` | Database readiness check failed or pool stats are not available. |
| `database_unavailable` | `503` | SQL database is temporarily unavailable during a handler operation. |
| `internal_error` | `500` | Unexpected server-side error. Check API logs with the request correlation ID. |
| `invalid_catalog_event` | `400` | Cell catalog event body is invalid. |
| `invalid_cell_ingress_endpoints` | `500` | Configured cell ingress endpoint map is invalid. |
| `missing_cell_id` | `400` | Cell catalog route omitted `cell_id`. |
| `missing_event_key` | `400` | Cell catalog event omitted `event_key`. |
| `missing_event_type` | `400` | Cell catalog event omitted `event_type`. |
| `missing_payload` | `400` | Cell catalog event omitted `payload`. |
| `queue_not_ready` | `503` | Queue is unavailable during readiness or dispatch. |
| `server_shutting_down` | `503` | API is draining and no longer ready for protected traffic. |

## Related Docs

| Need | Doc |
| --- | --- |
| Exact route table and common envelope behavior | [API Reference](./api-reference.md) |
| Auth actions, roles, and token scopes | [Authorization Reference](../operating/reference/authorization-reference.md) |
| Job validation detail shape | [Job Definition Validation](./job-validation.md) |
| Idempotency behavior and conflicts | [Idempotency And Retries](./idempotency-and-retries.md) |
| API security rejection metrics | [Metrics Catalog](../operating/reference/metrics-catalog.md#api-audit-sql-and-retention) |
