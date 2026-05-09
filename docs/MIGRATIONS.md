# Database Migrations

Vectis uses embedded SQL migrations for the supported database backends:

- SQLite: `internal/migrations/sqlite/`
- PostgreSQL: `internal/migrations/postgres/`

Runtime services wait for the expected schema, but they do not apply migrations. Deployment and admin flows should run `vectis-cli migrate` before starting or rolling new binaries.

The accepted policy is captured in [ADR 0004](adr/0004-migration-compatibility-and-rollback.md). Future release notes should link back here when a release includes schema changes.

## Compatibility Rules

Prefer expand/contract changes:

1. Add new nullable columns, tables, or indexes first.
2. Deploy code that can read old and new shapes.
3. Backfill data separately when needed.
4. Only drop, rename, or tighten constraints after every supported binary version tolerates the new shape.

Avoid schema changes that require all services to stop at once unless the release notes explicitly call out a downtime migration.

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
- Whether new binaries can run against the old schema before `vectis-cli migrate`.
- Whether rolling upgrades with mixed binary versions are supported.
- Whether downtime or a coordinated service stop is required.
- The production rollback path: backup restore, roll-forward repair, or explicitly safe down migration.

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

## Commands

```sh
go test ./internal/migrations
make test-postgres-integration
```
