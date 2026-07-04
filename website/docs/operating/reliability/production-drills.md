# Production Drills

Use these drills to prove that a production-v1 deployment can be upgraded,
restored, and operated without relying on memory. Attach completed drill output
to the release or deployment evidence record.

Each drill should name the deployment, Vectis version, operator, date, service
placement, storage paths, and any waivers. Do not paste secrets into drill
records.

## Preflight Drill

Run this before a first production handoff and before a release candidate is
called production-ready.

| Step | Evidence |
| --- | --- |
| Confirm topology | Deployment matches [Production Topology v1](../deployment/production-topology-v1.md) or release notes describe the deliberate exception. |
| Confirm config | `/etc/vectis/*.env` or platform config matches the [Production Environment Template](../deployment/production-env-template.md) and secret inventory. |
| Confirm artifacts | Package, container, binary, systemd, and docs artifacts are built from one commit. |
| Confirm migrations | `vectis-cli database migrate` has been run or explicitly declared unnecessary for every database. |
| Confirm health | `vectis-cli health check --strict` passes or every warning has a named owner; `vectis-cli health check --json` is saved as evidence. |
| Confirm smoke | A known-safe job reaches terminal status, streams logs, and verifies artifacts/secrets when in scope. |
| Confirm monitoring | Alerts, dashboards, service logs, host disk telemetry, and Postgres monitoring are visible. |
| Confirm backup | The latest backup set covers database, queue, logs, artifacts, secrets, TLS, config, and observability customization. |
| Confirm retention | Cleanup schedule or manual owner is recorded. |

Stop the handoff when API readiness, database schema status, queue visibility,
log reachability, artifact storage, or worker execution is not understood.

## Upgrade Drill

Use this for release rehearsals and planned production upgrades.

1. Read the release notes and complete the
   [Production Readiness Evidence](../../developing/production-readiness-evidence.md)
   record through the artifact, migration, rollback, and known-risk sections.
2. Verify a recent backup or take a fresh one when the release notes require it.
3. Pause cron if scheduled work should not start during the change.
4. Drain or stop workers when the release does not allow mixed old/new worker
   execution.
5. Run migrations before rolling database-backed services when required.
6. Roll registry, queue, orchestrator, log, artifact, SPIFFE, and secrets during
   a quiet window when possible.
7. Roll cell ingress and API behind readiness checks.
8. Roll worker-core and workers gradually, keeping paired worker/core instances
   together.
9. Restart cron, SCM trigger producers, reconciler, catalog, log-forwarder, and
   docs according to the deployment plan.
10. Run `vectis-cli health check --strict`.
11. Save `vectis-cli health check --json` output as the machine-readable health
   evidence artifact.
12. Trigger a known-safe job, confirm terminal status, stream logs, verify
   artifacts/secrets when in scope, and inspect dispatch events.
13. Watch retry exhaustion, queue backlog, DLQ depth, worker failures, log and
   artifact failures, DB pool pressure, and API security rejections for at least
   one reconciler interval.

Record the exact commands, health output, smoke run ID, terminal status, and
any waivers.

## Rollback Drill

Use this before a risky upgrade and whenever release notes name database restore
or roll-forward repair as the rollback path.

| Rollback path | Drill proof |
| --- | --- |
| Previous artifacts only | Old binaries start against the current schema and pass health plus smoke. |
| Database restore plus previous artifacts | Backup can restore into an isolated environment and old binaries pass health plus smoke. |
| Roll-forward repair | Repair command or manual procedure is rehearsed against copied data and leaves health checks clean. |
| Down migration | Release notes explicitly say the down migration is production-safe and the rehearsal proves old binaries work afterward. |

Rollback rehearsal should include the same operator smoke as upgrade rehearsal.
If rollback needs a database restore, also run the restore smoke test from
[Backup, Restore, And Disaster Recovery](./backup-restore.md#restore-smoke-test).

## Restore Drill

Use this to exercise disaster recovery without waiting for an incident.

For the Podman reference deployment, the automated e2e drill uses
`podman volume export` / `podman volume import` for the Postgres, queue, log,
artifact, secrets, and SPIFFE volumes, then verifies restored logs, restored
artifact download, secret resolution, and a new post-restore run. Production
deployments should replace that media step with the database and storage
platform's documented backup/restore mechanism.

1. Pick a backup set and record database, queue, log, artifact, secret, TLS,
   config, and observability backup identifiers.
2. Restore into an isolated environment when possible.
3. Restore config, secrets, TLS material, manifests, and durable storage paths.
4. Restore PostgreSQL through the database platform's documented process.
5. Run `vectis-cli database migrate` for every restored database.
6. Start registry, queue, orchestrator, log, artifact, SPIFFE, and secrets.
7. Start cell ingress and API.
8. Start worker-core, workers, cron, SCM trigger producers, reconciler, catalog,
   and log-forwarder.
9. Run `vectis-cli health check --strict`.
10. Save `vectis-cli health check --json` output as the machine-readable health
    evidence artifact.
11. Verify restored jobs/runs, restored logs, restored artifacts, and restored
    secret resolution when those backup pieces were included.
12. Trigger a new known-safe job and verify terminal status, logs, dispatch
    events, and artifacts/secrets when in scope.
13. Confirm fresh metrics, service logs, dashboards, and alert routing.

Do not run retention cleanup against the restored environment until the smoke
test passes and the restore point is accepted.

## Evidence Checklist

Every completed drill should capture:

- deployment name, environment, Vectis version, and Git commit;
- operator and reviewer;
- start and finish timestamps;
- commands run and links to raw output;
- `vectis-cli health check --strict` result;
- `vectis-cli health check --json` artifact;
- smoke run ID, terminal status, and log/artifact/secret result;
- backup identifiers and restore point when relevant;
- service instance IDs and durable storage paths;
- risks, waivers, missing evidence, and follow-up issues.

Keep the checklist with the
[Production Readiness Evidence](../../developing/production-readiness-evidence.md)
record for release candidates and with the incident or operations record for
real recoveries.

## Related Documentation

| Topic | Document |
| --- | --- |
| Production readiness evidence | [Production Readiness Evidence](../../developing/production-readiness-evidence.md) |
| Production Linux deployment | [Production Linux Deployment](../deployment/production-linux.md) |
| Backup and restore | [Backup, Restore, And Disaster Recovery](./backup-restore.md) |
| Production monitoring | [Production Monitoring Contract](./production-monitoring.md) |
| Repair runbooks | [Repair Runbooks](./repair-runbooks.md) |
