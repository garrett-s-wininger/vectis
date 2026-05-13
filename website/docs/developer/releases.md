# Releases And Upgrades

This page defines how maintainers cut Vectis releases and how operators evaluate upgrades.

## Versioning And Traceability

Vectis uses one release version across all `vectis-*` binaries and container images. A release should be tagged from a single commit, and all artifacts for that release must be built from that tag.

The Makefile stamps binaries with:

- `internal/version.Version`
- `internal/version.Commit`
- `internal/version.BuildDate`

Every binary wires Cobra version output through the shared CLI helper. Use `vectis-<name> --version` or `vectis-cli --version` when troubleshooting mixed artifacts.

## Artifact Policy

For a release, publish matching artifacts for:

- Binaries: `vectis-api`, `vectis-cli`, `vectis-cron`, `vectis-local`, `vectis-log`, `vectis-log-forwarder`, `vectis-queue`, `vectis-reconciler`, `vectis-registry`, `vectis-worker`.
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

When release notes do not explicitly allow skew, operators should stop dependent services, run migrations if needed, deploy the full release, and then restart in dependency order.

## Release Notes Template

Each release should include:

- Version and source commit.
- Artifact list and image tags.
- Breaking changes.
- REST API changes.
- gRPC/protobuf changes, including reserved or removed fields if any.
- Config/env changes and default changes.
- Database migrations, with old-binary/new-schema and new-binary/old-schema assessment.
- Operational changes: ports, probes, metrics, logs, dashboards, TLS, auth, RBAC.
- Upgrade instructions for SQLite/local and Postgres/reference deploys.
- Rollback instructions: previous artifacts only, database restore, roll-forward repair, or explicitly safe down migration.
- Known risks and manual verification steps.

Schema changes must follow [MIGRATIONS.md](migrations.md), including the production rollback note for each release.

## Maintainer Release Checklist

1. Confirm the tree is clean except intended release changes.
2. Run `make proto` and verify generated files are committed when protos changed.
3. Run `make test-quick`.
4. Run `make test-postgres-integration` for any database, migration, DAL, queue, reconciler, auth, or deploy-sensitive change.
5. Build all binaries with `make build`.
6. Build container images with the release tag.
7. Verify `vectis-cli --version` and one daemon `--version` show the release version, commit, and build date.
8. Review `docs/MIGRATIONS.md` requirements for every schema change.
9. Update docs for any changed API, config, deployment, security, metrics, or runbook behavior.
10. Draft release notes with the template above.
11. Smoke test SQLite/local upgrade.
12. Smoke test Postgres/reference upgrade when deploy or database behavior changed.
13. Tag the release commit and publish artifacts from that tag.

## SQLite / Local Upgrade Runbook

1. Back up the SQLite database, queue persistence, log storage, secrets, and local TLS material if they matter for this environment.
2. Stop `vectis-local` or standalone services.
3. Install the new artifacts.
4. Run `vectis-cli database migrate` with the restored or active SQLite DSN.
5. Start services.
6. Run the upgrade smoke test.

Rollback usually means restoring the pre-upgrade backup and previous artifacts unless the release notes explicitly say a down migration is production-safe.

## Postgres / Reference Deploy Upgrade Runbook

1. Back up Postgres and deployment secrets/TLS material.
2. Read release notes for required downtime, allowed skew, and migration rollback path.
3. Stop cron and workers first if the release does not allow mixed execution.
4. Run `vectis-cli database migrate` against the Postgres DSN.
5. Roll registry, queue, log, API, workers, cron, and reconciler according to the release notes.
6. Run the upgrade smoke test.
7. Watch retry exhaustion, queued-run age, worker failures, and API readiness for at least one reconciler interval.

Rollback depends on the migration note. Use previous artifacts alone only when the release notes state old binaries tolerate the migrated schema. Otherwise restore the database backup or apply the documented roll-forward repair.

## Upgrade Smoke Test

Run after every upgrade:

1. `vectis-cli --version` and one daemon `--version` report the expected release.
2. API `GET /health/live` and `GET /health/ready` return healthy status.
3. If auth is enabled, login/setup state behaves as expected.
4. `vectis-cli jobs list` succeeds.
5. `vectis-cli runs list <job-id>` succeeds for a known job.
6. Trigger a known-safe job.
7. Confirm the run reaches a terminal state.
8. Fetch or stream the run logs.
9. Confirm reconciler metrics do not show repeated enqueue failures.
10. Confirm dashboards and logs show the new version or commit where available.

## Rollback Choices

Choose one, and document it in release notes:

- Previous artifacts only: allowed when binaries are compatible with the current schema and config.
- Database restore plus previous artifacts: required when schema/data changes are not downgrade-safe.
- Roll-forward repair: preferred when the new release is mostly healthy but a data correction is needed.
- Down migration: allowed only when release notes explicitly say it is production-safe.
