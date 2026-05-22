# Releases And Upgrades

Use this page when preparing a Vectis release or evaluating whether a deployment can safely upgrade.

Maintainers use it to decide what evidence and release notes a release must include. Operators use the same release notes to decide whether an upgrade can roll, needs downtime, or needs a backup-and-restore rollback plan.

Related policy lives in [Database Migrations](./migrations.md), [Upgrade Compatibility Matrix](../operating/reference/upgrade-compatibility-matrix.md), [Capacity And Performance Checks](./performance/capacity-checks.md), [Production Topology v1](../operating/deployment/production-topology-v1.md), and [Backup And Restore](../operating/reliability/backup-restore.md).

## Versioning And Traceability

Vectis uses one release version across all `vectis-*` binaries and container images. A release should be tagged from a single commit, and all artifacts for that release must be built from that tag.

Mage build targets stamp binaries with:

- `internal/version.Version`
- `internal/version.Commit`
- `internal/version.BuildDate`

Every binary wires Cobra version output through the shared CLI helper. Use `vectis-<name> --version` or `vectis-cli --version` when troubleshooting mixed artifacts.

## Artifact Policy

For a release, publish matching artifacts for:

- Binaries: `vectis-api`, `vectis-artifact`, `vectis-catalog`, `vectis-cell-ingress`, `vectis-cli`, `vectis-cron`, `vectis-docs`, `vectis-local`, `vectis-log`, `vectis-log-forwarder`, `vectis-orchestrator`, `vectis-queue`, `vectis-reconciler`, `vectis-registry`, `vectis-secrets`, `vectis-spiffe`, `vectis-ui`, `vectis-worker`, `vectis-worker-core`.
- Container images for the deployable components.
- Generated protobuf Go code already committed in `api/gen/go/`.
- Release notes and upgrade notes.

Container tags should include the release version and may also include the source commit. Avoid publishing mutable operational tags, such as `latest`, as the only documented upgrade target.

## Supported Version Skew

Default policy: run one Vectis release version across all long-running binaries.

Short rolling-upgrade skew is acceptable only when the release notes say the changed surfaces are compatible. Surfaces to assess:

- SQL schema and migrations.
- REST API behavior and response shape.
- gRPC protobuf contracts.
- Queue payload expectations.
- Worker execution behavior.
- Log streaming behavior.
- Config and environment variables.
- Metrics, health checks, dashboards, and alert names.

When release notes do not explicitly allow skew, operators should stop dependent services, run migrations if needed, deploy the full release, and then restart in dependency order.

## Release Risk Classes

Use the highest matching class when writing release notes:

| Class | Examples | Release note requirement |
| --- | --- | --- |
| Patch-safe | Bug fix with no schema, API, config, queue, orchestrator, worker, log, or auth behavior change. | Standard upgrade notes and smoke test. |
| Operator-visible | Config default, metric, health check, log format, dashboard, port, TLS, auth, RBAC, or runbook behavior changed. | Call out the changed surface and required operator action. |
| Compatibility-sensitive | SQL migration, REST/gRPC shape, queue payload, run state, idempotency, retry, dispatch, or worker behavior changed. | State version-skew support, migration order, and rollback path. |
| Capacity-sensitive | API hot path, queue, orchestrator, worker, log streaming, cron, reconciler, catalog, or database query path changed. | Include performance evidence or explain why no check was needed. |

## Release Notes Template

Each release should include the following sections. Keep empty sections only
when the release explicitly says "none" or "not production-ready."

```md
## Vectis <version>

Source commit: <git-sha>
Release date: <date>
Production readiness: production-ready | not production-ready | alpha-only
Production evidence: <link to completed evidence record or "not applicable">

### Artifact Set

- Binaries:
- Container images:
- Linux packages:
- Generated systemd artifacts:
- Docs artifact:

### Compatibility And Skew

- Supported version skew:
- Components that must roll together:
- Old-binary/new-schema behavior:
- New-binary/old-schema behavior:

### Breaking Changes

- None, or list every operator/user-visible break.

### API And gRPC Changes

- REST:
- gRPC/protobuf:
- Removed or reserved fields:

### Config, Security, And Secrets

- New or changed env/config:
- TLS, mTLS, service identity, auth, RBAC, allowed Host, or proxy changes:
- Secret-manager or SPIFFE/secrets changes:

### Migrations

- Schema changed: yes | no
- Migration command:
- Rollback path:
- Fresh backup required before upgrade: yes | no

### Operations

- Ports/probes/health checks:
- Metrics/logs/dashboards/alerts:
- Retention or durable storage changes:
- Runbook changes:

### Capacity

- Capacity-sensitive change: yes | no
- Evidence link or waiver:
- Published envelope changed: yes | no

### Smoke And Drill Evidence

- Readiness report:
- `mage releaseLocalValidate`:
- `mage testQuick`:
- Postgres integration, when required:
- Linux package/artifact smoke:
- VM deploy/package lanes, when required:
- SQLite/local upgrade smoke:
- Postgres/reference upgrade smoke:
- Operator smoke run:
- Security checklist result:

### Upgrade Instructions

- SQLite/local:
- Postgres/reference or production-v1:
- Required downtime or sequencing:

### Rollback Instructions

- Rollback choice: previous artifacts | database restore | roll-forward repair | safe down migration
- Commands or procedure:
- Data-loss or compatibility caveats:

### Known Risks And Waivers

- Risk/waiver:
- Owner:
- Expiration or follow-up:
```

Schema changes must follow [Database Migrations](./migrations.md), including the production rollback note for each release. Capacity-sensitive changes should follow [Capacity And Performance Checks](./performance/capacity-checks.md).

## Release Readiness Report

Use the readiness report when preparing a release candidate:

```sh
mage releaseReadinessReport
```

The target runs `tools/release-readiness` and writes a bundle under
`artifacts/release-readiness/<run>/`:

- `summary.json`: machine-readable release identity, check results, artifact
  checksums, and skip or waiver state.
- `report.md`: maintainer-facing summary suitable for review or release notes.
- `checksums.txt`: SHA-256 inventory for discovered release artifacts.
- `logs/*.log`: stdout/stderr for each selected command check.

The default `local` profile is `git-clean,docs-npm-audit,release-local`, which
verifies that the worktree is clean, records the docs dependency audit posture,
and then runs `mage releaseLocalValidate`. The npm audit gate fails on high
or critical findings while preserving lower-severity counts in the report. Select
heavier profiles or explicit checks when the release touches more surfaces:

```sh
RELEASE_READINESS_PROFILE=candidate mage releaseReadinessReport

RELEASE_READINESS_CHECKS=git-clean,docs-npm-audit,release-local,postgres-integration,package-linux \
RELEASE_READINESS_ARGS='--artifact-roots bin,artifacts/packages,artifacts/deploy,website/static/openapi/v1.json' \
mage releaseReadinessReport
```

Use skips for unavailable local infrastructure and waivers for deliberately
accepted release risk:

```sh
RELEASE_READINESS_PROFILE=full \
RELEASE_READINESS_ARGS='--skip postgres-integration="no local Postgres" --waive vm-e2e="no prepared RPM guest; DEB smoke covered"' \
mage releaseReadinessReport
```

Add `RELEASE_READINESS_ARGS='--strict'` when skipped or waived selected checks
should fail the run. Add `RELEASE_READINESS_ARGS='--fail-fast'` when a failed
gate should stop later selected checks. Use
`go run ./tools/release-readiness --list-checks` to list available profiles and
checks.

## Maintainer Release Checklist

1. Confirm the tree is clean except intended release changes.
2. Run `mage proto` and verify generated files are committed when protos changed.
3. Run `mage releaseReadinessReport` for the local release lane and keep the generated bundle with the release notes.
4. Run `mage testPostgresIntegration` for any database, migration, DAL, queue, reconciler, auth, or deploy-sensitive change.
5. If the local release lane was skipped or failed before the build step, build all binaries with `mage build`; this also embeds the docs site into `vectis-docs`.
6. Build container images with the release tag.
7. Verify `vectis-cli --version` and one daemon `--version` show the release version, commit, and build date.
8. Review [Database Migrations](./migrations.md) requirements for every schema change.
9. Run or cite [Capacity And Performance Checks](./performance/capacity-checks.md) for capacity-sensitive changes.
10. Update docs for any changed API, config, deployment, security, metrics, capacity, or runbook behavior.
11. Draft release notes with the template above.
12. Complete [Production Readiness Evidence](./production-readiness-evidence.md) when the release will be called production-ready.
13. Smoke test SQLite/local upgrade.
14. Smoke test Postgres/reference upgrade when deploy or database behavior changed.
15. Run or waive VM-backed deploy/package lanes when Linux install, package, worker isolation, or VM provider behavior changed.
16. Tag the release commit and publish artifacts from that tag.

## Production Readiness Gate

Use this gate before publishing a release as production-ready, or before telling
operators a release is safe for a production-v1 deployment. If a release is
developer-only, alpha-only, or intentionally not production-ready, say that in
the release notes and list the missing gate items.

For production-ready release candidates, complete a
[Production Readiness Evidence](./production-readiness-evidence.md) record and
keep it with the release notes and release artifacts.

| Gate | Evidence |
| --- | --- |
| Topology fit | Release notes state whether the release stays inside [Production Topology v1](../operating/deployment/production-topology-v1.md), changes that contract, or introduces an experimental shape. |
| Artifact coverage | All required production-v1 binaries, containers, packages, or operator-supervised services are built from one commit and versioned together. Any Linux packaging gaps are called out. |
| Config and secrets | New or changed config is documented in [Production Config And Secrets Contract](../operating/deployment/production-config-contract.md) or linked release notes, including secret-manager, TLS, identity, and allowed-host effects. |
| Migrations | Schema changes satisfy [Database Migrations](./migrations.md), include old/new binary compatibility notes, and name the rollback path. |
| Backup/restore | Release notes say whether a fresh backup is required before upgrade and whether rollback uses previous artifacts, database restore, roll-forward repair, or safe down migration. |
| Deployment smoke | Linux package/artifact smoke and Postgres/reference smoke results are recorded when deployment behavior changed. |
| VM/package lanes | VM-backed deploy/package lanes are run or explicitly waived with the reason when Linux install, package, worker isolation, or VM provider behavior changed. |
| Monitoring | New, renamed, or removed metrics/health checks/alerts are documented, and [Production Monitoring Contract](../operating/reliability/production-monitoring.md) remains accurate. |
| Retention | Any new durable data surface has retention behavior, cleanup safety, and backup expectations documented. |
| Security | API auth, RBAC, TLS, service identity, secrets, redaction, worker isolation, and public surface changes are called out. |
| Capacity | Capacity-sensitive changes cite benchmark or deployed-stack evidence, or explain why no new evidence was needed. |
| Operator smoke | Upgrade smoke test reaches terminal job status, streams logs, and verifies artifacts/secrets when those features are in scope. |
| Known risks | Release notes include unresolved production risks, manual verification steps, and any follow-up work required before wider rollout. |

Production readiness does not require every experimental feature to be
production-ready. It does require release notes to draw a clear line between the
supported production-v1 path and anything outside it.

## SQLite / Local Upgrade Runbook

1. Back up the SQLite database, queue persistence, log storage, secrets, and local TLS material if they matter for this environment.
2. Stop `vectis-local` or standalone services.
3. Install the new artifacts.
4. Run `vectis-cli database migrate` with the restored or active SQLite DSN.
5. Start services.
6. Run the upgrade smoke test.

Rollback usually means restoring the pre-upgrade backup and previous artifacts unless the release notes explicitly say a down migration is production-safe.

## Postgres / Reference Deploy Upgrade Runbook

1. Back up Postgres, queue persistence, log storage, artifact storage, secret material, TLS material, and live config according to [Backup And Restore](../operating/reliability/backup-restore.md).
2. Read release notes for required downtime, allowed skew, and migration rollback path.
3. Stop cron and workers first if the release does not allow mixed execution.
4. Run `vectis-cli database migrate` against the Postgres DSN.
5. Roll registry, queue, orchestrator, log, artifact, spiffe, secrets, cell ingress, API, worker-core, workers, cron, reconciler, catalog, log-forwarder, and docs according to the release notes.
6. Run the upgrade smoke test.
7. Watch retry exhaustion, queued-run age workarounds, worker failures, log/artifact failures, DB pool pressure, and API readiness for at least one reconciler interval.

Rollback depends on the migration note. Use previous artifacts alone only when the release notes state old binaries tolerate the migrated schema. Otherwise restore the database backup or apply the documented roll-forward repair.

## Upgrade Smoke Test

Run after every upgrade:

1. `vectis-cli --version` and one daemon `--version` report the expected release.
2. API `GET /health/live` and `GET /health/ready` return healthy status.
3. If auth is enabled, login/setup state behaves as expected.
4. `vectis-cli jobs list --repository <repo>` succeeds.
5. `vectis-cli runs list <job-id> --repository <repo>` succeeds for a known job.
6. Trigger a known-safe job.
7. Confirm the run reaches a terminal state.
8. Fetch or stream the run logs.
9. Confirm reconciler metrics do not show repeated enqueue failures.
10. Confirm dashboards and logs show the new version or commit where available.

For capacity-sensitive releases, also watch queue depth, DB pool waits, worker outcomes, log stream behavior, and API request status for the period called out in the release notes.

## Rollback Choices

Choose one, and document it in release notes:

- Previous artifacts only: allowed when binaries are compatible with the current schema and config.
- Database restore plus previous artifacts: required when schema/data changes are not downgrade-safe.
- Roll-forward repair: preferred when the new release is mostly healthy but a data correction is needed.
- Down migration: allowed only when release notes explicitly say it is production-safe.

## Related Docs

| Need | Doc |
| --- | --- |
| Schema compatibility and rollback | [Database Migrations](./migrations.md) |
| Performance evidence for hot-path changes | [Capacity And Performance Checks](./performance/capacity-checks.md) |
| Production topology gate | [Production Topology v1](../operating/deployment/production-topology-v1.md) |
| Production config and secrets | [Production Config And Secrets Contract](../operating/deployment/production-config-contract.md) |
| Production monitoring | [Production Monitoring Contract](../operating/reliability/production-monitoring.md) |
| Production security checklist | [Production Security Checklist](../operating/deployment/production-security-checklist.md) |
| Production drills | [Production Drills](../operating/reliability/production-drills.md) |
| Operator capacity envelope | [Capacity And Load Envelope](../operating/capacity/capacity-load-envelope.md) |
| Upgrade backup and restore planning | [Backup And Restore](../operating/reliability/backup-restore.md) |
| Scaling and restart order | [Scaling And Restarts](../operating/deployment/scaling-and-restarts.md) |
