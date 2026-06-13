# Repair Runbooks

These recipes are for operators responding to failed health checks, alerts, restore drills, stuck runs, or stuck task finalization.

Start with the broad health check when the API is reachable:

```sh
vectis-cli health check --strict
```

For a run-specific issue, start with:

```sh
vectis-cli runs show <run-id>
```

That command shows run status and dispatch events without direct database access.

## Quick Map

| Symptom or check | Recipe |
| --- | --- |
| `queue.backlog.ratio`, queue backlog alert, queued run not starting | [Queued Runs Or Backlog](#queued-runs-or-backlog) |
| `cron.schedules`, scheduled run not starting | [Queued Runs Or Backlog](#queued-runs-or-backlog) |
| `reconciler.stuck.runs`, reconciler alert | [Reconciler Repair](#reconciler-repair) |
| `log.reachable`, log append/drop alert, log streaming failure | [Log Service Repair](#log-service-repair) |
| `audit.drops.recent`, `audit.flush.failures`, audit alert | [Audit Durability Repair](#audit-durability-repair) |
| `db.connection.pool`, DB pool alert | [Database Pool Pressure](#database-pool-pressure) |
| `db.schema.current`, API readiness schema error | [Schema Or Migration Repair](#schema-or-migration-repair) |
| API security rejection alert | [API Security Rejections](#api-security-rejections) |
| SPIFFE execution SVID check alert | [SPIFFE Execution SVID Checks](#spiffe-execution-svid-checks) |
| Secret resolution alert | [Secret Resolution](#secret-resolution) |
| Old retained records or SQL storage pressure | [Retention Cleanup](#retention-cleanup) |
| One run needs operator action | [Manual Run Intervention](#manual-run-intervention) |

## Before Manual Changes

Manual run repair can create confusing history if automatic repair is still working. Before retrying, requeueing, or marking a run terminal:

1. Read the run with `vectis-cli runs show <run-id>`.
2. Check whether a worker currently owns the run.
3. Check dispatch events for recent API, cron, or reconciler handoff attempts.
4. Fix underlying queue, database, log, registry, or TLS issues first.
5. Wait at least one reconciler interval when the run is queued and not urgent.
6. Prefer the narrowest action that matches the situation.

Do not requeue a succeeded run. Do not requeue a running run unless you have confirmed the worker is gone and duplicate execution is not possible.

## API Security Rejections

Use this when `VectisAPISecurityRejectionsSustained` or `VectisAPISecurityRejectionSpike` fires, or when API logs show repeated `API security rejection` warnings.

1. Start with the alert labels: `reason`, `route`, and `status`. They are intentionally low-cardinality and safe to use for grouping.
2. Check whether the affected `route` is `unknown`. A spike there usually points at scans, malformed request targets, Host header mismatches, or direct traffic around the intended edge.
3. Compare the first spike timestamp with deploys, ingress/proxy changes, API allowed-host changes, CORS origin changes, and browser UI releases.
4. Inspect edge logs for the same window. Confirm direct clients cannot bypass the proxy and that the proxy overwrites `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `Forwarded`.
5. For `invalid_request_header`, check duplicate or malformed browser/proxy headers first: `Origin`, CORS preflight headers, `Sec-Fetch-*`, `X-Forwarded-*`, `X-Real-IP`, and `Forwarded`.
6. For `invalid_host_header`, confirm `api.host_validation.allowed_hosts` includes the browser-facing API hostname and that the proxy preserves the expected `Host`.
7. For CORS or CSRF reasons, verify the browser-facing scheme, host, and port match the configured origins and trusted proxy original-HTTPS handling.
8. For `rate_limit_exceeded`, identify whether the key is collapsing behind an untrusted proxy, a client retry loop, or expected load that needs a deliberate rate-limit change.
9. If the traffic is hostile or unknown, block or challenge it at the edge. If it is legitimate, fix the caller or edge configuration before loosening Vectis security policy.
10. After repair, confirm the alert expression returns to baseline and keep API access logs enabled long enough to validate the affected clients.

## SPIFFE Execution SVID Checks

Use this when `VectisWorkerSPIFFESVIDCheckFailures` fires or when `vectis_worker_spiffe_svid_checks_total{outcome="failed"}` increases.

1. For a specific impacted run, start with `vectis-cli runs show <run-id>`. If it prints `next_action=security_gate_failed`, use `latest_failed_security_event` as the fastest pointer.
2. Then use `vectis-cli runs tasks <run-id>` and look for attempt `security` entries with `svid_check`; multi-cell catalog fan-in copies the same redacted events into the global run catalog.
3. For fleet-wide scope, use the `reason` label. It is intentionally low-cardinality: `mismatch`, `source_error`, `source_timeout`, `canceled`, `missing_identity`, `missing_source`, or `invalid_expected_id`.
4. For `mismatch`, compare the worker's configured `worker.execution_identity.*` template and trust domain with the SPIFFE registration entries that apply to that worker. The worker requires an exact X.509-SVID SPIFFE ID match for the execution, and registration intents should use the same execution SPIFFE ID, parent SPIFFE ID, trusted selectors, and bounded expiry that the registrar applies.
5. For `source_timeout`, check SPIFFE Workload API responsiveness, socket filesystem health, and whether `worker.spiffe.fetch_timeout` is too low for the deployment.
6. For `source_error`, check the `vectis-spiffe` process, Workload API socket path, filesystem permissions, and whether the worker can connect to `worker.spiffe.workload_api_address`.
7. For `canceled`, check whether the run was canceled by API/operator request or worker shutdown while the pre-action check was in progress; the starter alert excludes this reason.
8. For `missing_identity`, confirm the run reached the worker through cell execution dispatch with an execution envelope and that `worker.execution_identity.enabled=true` is configured consistently.
9. For `missing_source`, confirm `worker.spiffe.enabled=true` and `worker.spiffe.workload_api_address` are set on the worker process.
10. For `invalid_expected_id`, validate `worker.execution_identity.trust_domain` and `worker.execution_identity.path_template` against the allowed SPIFFE URI shape.
11. Do not fix these alerts by exposing the SPIFFE Workload API socket, SVID, private key, or derived identity to shell actions. Repair the worker-controlled SPIFFE registration or configuration path.
12. After repair, retry only the failed runs that are safe to re-run and confirm the failed-check counter no longer increases.

## Secret Resolution

Use this when `VectisSecretsResolveFailures` fires or when `vectis_secrets_resolve_requests_total{outcome!="success"}` increases unexpectedly.

1. For a specific impacted run, start with `vectis-cli runs show <run-id>`. If it prints `next_action=security_gate_failed`, use `latest_failed_security_event` as the fastest pointer.
2. Then use `vectis-cli runs tasks <run-id>` and look for attempt `security` entries with `secret_resolution`; multi-cell catalog fan-in copies the same redacted events into the global run catalog.
3. For fleet-wide scope, use the alert labels: `outcome`, `reason`, and `provider`. They are intentionally low-cardinality and do not include secret values, requested refs, run IDs, execution IDs, or SPIFFE IDs.
4. Check `vectis-secrets` logs around the alert window. Resolve logs include run, execution, namespace, job, task, provider, outcome, reason, and secret count metadata.
5. For `authorization_denied`, check worker SPIFFE SVID failures first with `vectis_worker_spiffe_svid_checks_total`, then compare `worker.execution_identity.*` settings on workers and `vectis-secrets`, and then inspect `--allow-secret` / `VECTIS_SECRETS_POLICY_ALLOW` rules for the run namespace, job, task, and requested refs.
6. For `provider_not_found`, confirm the requested ref scheme has a provider registered on the broker. For `encryptedfs://` refs, also confirm the envelope exists below `--encryptedfs-root`.
7. For `provider_denied`, inspect encryptedfs safety checks: unsafe refs, symlink escapes, invalid envelopes, decrypt failures from a wrong key, and oversized secret material.
8. For `provider_error`, check broker logs and process configuration for encryptedfs root readability, key-file readability, database reachability, TLS, and filesystem availability.
9. Confirm `vectis-secrets` gRPC health is serving and the metrics listener is scrapeable from the trusted monitoring network.
10. After repair, retry only runs that are safe to re-run and confirm the failed resolve counter no longer increases.

## Queued Runs Or Backlog

Use this when `health check` warns on `queue.backlog.ratio` or `cron.schedules`, scheduled or queued runs are not draining, or `VectisQueueBacklogGrowing` fires.

1. Run `vectis-cli health check --strict` and note failures for `api.ready`, `queue.backlog.ratio`, `cron.schedules`, and `reconciler.stuck.runs`.
2. For a specific run, run `vectis-cli runs show <run-id>` and inspect `status`, `next_action`, and `dispatch_events`.
3. If there is no dispatch event, follow [Queued With No Dispatch](./dispatch-visibility.md#runbook-queued-with-no-dispatch).
4. If dispatch failed, follow [Dispatch Failure](./dispatch-visibility.md#runbook-dispatch-failure).
5. For scheduled work, check `vectis-cron` process health, database access, and queue or cell-ingress handoff logs.
6. Confirm workers are running and receiving jobs with worker process health and `vectis_worker_jobs_received_total`.
7. Check queue gRPC health and queue metrics: pending jobs, in-flight deliveries, and DLQ size.
8. If queue persistence was disabled or lost after restart, keep `vectis-reconciler` running and wait at least one reconciler interval before manual retry.
9. Use `vectis-cli runs retry <run-id>` only after automatic redispatch is not progressing and the run is not already running or succeeded.

## Reconciler Repair

Use this when `health check` warns on `reconciler.stuck.runs`, cannot read reconciler recovery visibility, or `VectisReconcilerReenqueueFailures` / `VectisReconcilerTaskFinalizationRepairFailures` fires.

1. Confirm API readiness with `vectis-cli health check --strict`.
2. Confirm at least one reconciler is running, and that only one instance is actively holding the service lease; see [Scaling And Restarts](../deployment/scaling-and-restarts.md).
3. Check reconciler process logs for database, queue, registry, or gRPC TLS errors.
4. Verify the reconciler is using the same global database as the API and cron: shared `VECTIS_DATABASE_DSN`, or `VECTIS_GLOBAL_DATABASE_DSN` when global/cell databases are split.
5. Verify queue resolution through the pinned queue address or registry path configured for the reconciler.
6. Inspect `vectis_reconciler_reenqueue_total`, `vectis_reconciler_task_finalization_repairs_total`, and `vectis_retries_exhausted_total`.
7. For impacted runs, use `vectis-cli runs show <run-id>` and `vectis-cli runs tasks <run-id>` to confirm whether queued dispatch, task continuation redispatch, or orphaned task finalization is stuck.
8. If the reconciler is healthy but one urgent run remains queued, use `vectis-cli runs retry <run-id>` after confirming no worker currently owns it.

## Log Service Repair

Use this when `health check` warns on `log.reachable`, log append failures increase, or log streaming fails.

1. Run `vectis-cli health check --format json` and inspect the `log.reachable` evidence.
2. Confirm `vectis-log` gRPC health is serving.
3. Confirm the log storage directory is mounted, writable, and has free space.
4. Verify API and worker log client resolution: pinned log address or registry registration.
5. Verify gRPC TLS settings: CA file, server name, and certificate SANs. See [Configuration](../configuration.md#internal-grpc-tls).
6. Check `vectis_log_storage_append_failures_total`, memory buffer drops, subscriber drops, and log service disk telemetry.
7. On worker hosts, check log-forwarder spool age and size if a forwarder is deployed.
8. After repair, trigger a small known-safe job and confirm `vectis-cli logs run <run-id>` returns fresh output.

## Audit Durability Repair

Use this when `health check` warns on `audit.drops.recent` or `audit.flush.failures`, or audit alerts fire.

1. Confirm API and database readiness with `vectis-cli health check --strict`.
2. Check API logs for audit buffer full, flush failure, or database write errors.
3. Inspect `vectis_audit_events_dropped_total` and `vectis_audit_flush_failures_total`.
4. Check database pool pressure with the `db.connection.pool` health check and DB metrics.
5. Reduce audit event pressure or increase API/database capacity before clearing the incident.
6. Record dropped-event windows in the incident record; dropped audit events cannot be reconstructed from Vectis state.

## Database Pool Pressure

Use this when `health check` warns on `db.connection.pool` or the DB pool alert fires.

1. Run `vectis-cli health check --format json` and record `db.connection.pool` evidence.
2. Check whether API, cron, reconciler, workers, or other DB-using processes recently scaled up.
3. Check database availability, slow queries, locks, and server-side connection limits.
4. Review Vectis pool settings; see [Configuration](../configuration.md#postgresql-connection-pool-pgx-only).
5. If the database is overloaded, reduce replica count or workload pressure before raising pool limits.
6. Confirm `db.connection.pool` returns to `pass` and API 5xx rates recover.

## Schema Or Migration Repair

Use this when `health check` fails `db.schema.current`, API readiness reports database/schema issues, or a restore drill reaches migration checks.

1. Stop workers, cron, reconciler, and catalog if the schema state is uncertain.
2. Confirm `VECTIS_DATABASE_DRIVER` and `VECTIS_DATABASE_DSN` point at the intended database. For split global/cell deployments, run this once for the global DSN and once for the cell DSN.
3. Run the migration from the same network/config context used by the deployment:

```sh
vectis-cli database migrate
```

4. Restart API first.
5. Run `vectis-cli health check --strict`.
6. Restart workers, cron, reconciler, and catalog after `api.ready` and `db.schema.current` pass.
7. For restore-specific order and partial-restore outcomes, use [Backup And Restore](./backup-restore.md).

## Retention Cleanup

Use this when SQL storage pressure grows, old retained records alert, or a maintenance window calls for cleanup.

1. Read [Retention](./retention.md) and confirm the retention window, scheduling posture, and backup freshness expectations are acceptable for the environment.
2. Preview cleanup:

```sh
vectis-cli retention cleanup --dry-run
```

3. Include `--log-storage-dir` only when pruning local durable run log files for the same deployment.
4. Include `--artifact-storage-dir` only when pruning local artifact CAS blobs for the same deployment. Apply-time artifact blob pruning requires the artifact storage directory lock, so stop that shard or use a maintenance window.
5. Review delete counts, cutoffs, backup freshness, and any incident/restore holds before applying.
6. Apply during a maintenance window:

```sh
vectis-cli retention cleanup --yes
```

7. Run `vectis-cli health check --strict`.
8. Check storage pressure metrics after cleanup.

## Manual Run Intervention

Use this when an operator must change one run after automatic recovery has had time to act.

1. Read the run:

```sh
vectis-cli runs show <run-id>
```

2. Inspect status, failure reason, orphan reason, lease/worker ownership if visible, and dispatch events.
3. Prefer automatic repair first: fix queue, orchestrator, log, database, registry, TLS, and reconciler issues, then wait at least one reconciler interval for queued runs.
4. To retry a failed, orphaned, or safely queued run:

```sh
vectis-cli runs retry <run-id>
```

5. To request cancellation of a currently executing run:

```sh
vectis-cli runs cancel <run-id>
```

6. To mark a run failed after external investigation:

```sh
vectis-cli runs fail <run-id> --reason "operator reason"
```

7. Re-read the run and confirm the terminal or queued state is the one you intended.

## After Repair

1. Run `vectis-cli health check --strict`.
2. Trigger a small known-safe job.
3. Confirm the run reaches a terminal state.
4. Confirm logs are readable.
5. Check the relevant metrics for at least one reconciler interval.
6. Record the root cause, impacted run IDs, commands used, and any data loss or audit gaps.

## Related Documentation

| Topic | Document |
| --- | --- |
| Reliability landing page | [Runbooks And Alerts](./runbooks.md) |
| Dispatch event triage | [Dispatch Visibility](./dispatch-visibility.md) |
| Retention policy | [Retention](./retention.md) |
| Backup and restore | [Backup And Restore](./backup-restore.md) |
| Health check catalog | [Health Check Catalog](../reference/health-check-catalog.md) |
| Scaling and restart posture | [Scaling And Restarts](../deployment/scaling-and-restarts.md) |
