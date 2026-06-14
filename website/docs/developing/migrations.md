# Database Migrations

Use this page when a code change adds, changes, or removes durable SQL state.

Vectis uses embedded SQL migrations for the supported database backends:

- SQLite: `internal/migrations/sqlite/`
- PostgreSQL: `internal/migrations/postgres/`

Runtime services wait for the expected schema, but they do not apply migrations. Deployment and admin flows should run `vectis-cli database migrate` before starting or rolling new binaries.

The accepted policy is captured in [ADR 0004](./architecture-decisions/0004-migration-compatibility-and-rollback.md). Future release notes should link back here when a release includes schema changes.

Release notes and upgrade checklists for schema changes belong in [Releases And Upgrades](./releases.md).

## Core Rules

| Rule | Why it matters |
| --- | --- |
| Every migration version exists for SQLite and Postgres. | Operators can trust both supported backends have the same semantic schema version. |
| Runtime services wait for schema; they do not migrate it. | Deployments control when schema changes happen. |
| Prefer expand/contract changes. | Rolling upgrades are safer when old and new binaries can both tolerate the transition. |
| Down migrations are required for development reversibility. | Tests can prove the graph can move down, even when production rollback uses a backup. |
| Production rollback is release-specific. | A `down` file is not automatically a safe production rollback plan. |

## Compatibility Rules

Prefer expand/contract changes:

1. Add new nullable columns, tables, or indexes first.
2. Deploy code that can read old and new shapes.
3. Backfill data separately when needed.
4. Only drop, rename, or tighten constraints after every supported binary version tolerates the new shape.

Avoid schema changes that require all services to stop at once unless the release notes explicitly call out a downtime migration.

## Authoring Flow

For every schema change:

1. Choose the next migration version and a descriptive name.
2. Add `NNN_name.up.sql` and `NNN_name.down.sql` under both `internal/migrations/sqlite/` and `internal/migrations/postgres/`.
3. Keep the operator-facing schema outcome the same across both backends, even when SQL syntax differs.
4. Update DAL repositories, domain types, API responses, and tests that depend on the new shape.
5. Confirm old-binary/new-schema and new-binary/old-schema behavior, or document why the release requires downtime.
6. Update release notes and operator docs when deployment, retention, repair, security, API behavior, or capacity changes.

## SQLite And Postgres

Every schema change must consider both backends. Keep the two migration directories aligned semantically even when SQL syntax differs.

Common differences:

- SQLite uses `INTEGER PRIMARY KEY AUTOINCREMENT`; Postgres uses `BIGSERIAL`.
- SQLite stores booleans as integers; Postgres uses `BOOLEAN`.
- Timestamp/text handling may differ. DAL code should normalize values before exposing them.
- Parameter placeholders differ in query code; use repository helpers such as `rebindQueryForPgx`.

When backend SQL diverges, document the reason in the migration review or release notes. The migration version number must still appear in both directories, with both `up` and `down` files, so reviewers and CI can tell that the two backends were considered together.

## Down Migrations

Down migrations are required so development and automated tests can prove the migration graph is reversible. They are not a promise that production rollback is always safe.

Production rollback should be planned per release. If a migration cannot safely preserve data on downgrade, document that in the release notes and prefer restoring from a database backup or rolling forward with a repair migration over relying on `down`.

Operator rollback choices are release-specific:

- Restore the database backup taken before migration when downgrade would lose information or reorder state.
- Roll forward with a repair migration when the new schema is sound but data needs correction.
- Run `down` only when the release notes explicitly say the down migration is production-safe for the operator's data shape.

## Release Checklist

For each release with schema changes, release notes must state:

- Whether old binaries can run against the new schema.
- Whether new binaries can run against the old schema before `vectis-cli database migrate`.
- Whether rolling upgrades with mixed binary versions are supported.
- Whether downtime or a coordinated service stop is required.
- The production rollback path: backup restore, roll-forward repair, or explicitly safe down migration.
- Whether the release passes the production readiness migration gate in [Releases And Upgrades](./releases.md#production-readiness-gate).

## Review Checklist

For every schema change:

- Add matching SQLite and Postgres migration files.
- Add or update down migrations.
- Run SQLite migration round-trip tests.
- Add a tagged Postgres smoke test when the change touches production-critical behavior.
- Update DAL methods and tests for the new shape.
- Confirm old binaries against new schema and new binaries against old schema behavior, or document why the release requires a coordinated rollout.
- Confirm the migration number exists in both `internal/migrations/sqlite/` and `internal/migrations/postgres/`.
- Update operator docs when the schema change affects deployment, retention, repair, security, or API behavior.

## Operator-Facing Notes

When a migration ships, release notes should tell operators:

| Question | Why operators need it |
| --- | --- |
| Can old binaries run against the migrated schema? | Determines whether rollback can use previous artifacts alone. |
| Can new binaries start before migration? | Determines rollout order and downtime. |
| Can binaries be mixed during a rolling upgrade? | Determines whether services can roll gradually. |
| Does the migration rewrite, delete, or reinterpret data? | Determines backup, restore, and audit expectations. |
| What is the rollback path? | Determines whether rollback means previous artifacts, database restore, roll-forward repair, or safe `down`. |

## Commands

```sh
go test ./internal/migrations
make test-postgres-integration
```

## Related Docs

| Need | Doc |
| --- | --- |
| Release note requirements | [Releases And Upgrades](./releases.md) |
| Accepted compatibility decision | [ADR 0004](./architecture-decisions/0004-migration-compatibility-and-rollback.md) |
| Schema repair during operations | [Repair Runbooks](../operating/reliability/repair-runbooks.md#schema-or-migration-repair) |
| Upgrade backup and restore planning | [Backup And Restore](../operating/reliability/backup-restore.md) |
