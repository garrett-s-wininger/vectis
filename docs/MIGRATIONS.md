# Database Migrations

Vectis uses embedded SQL migrations for the supported database backends:

- SQLite: `internal/migrations/sqlite/`
- PostgreSQL: `internal/migrations/postgres/`

Runtime services wait for the expected schema, but they do not apply migrations. Deployment and admin flows should run `vectis-cli migrate` before starting or rolling new binaries.

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

## Down Migrations

Down migrations are required so development and automated tests can prove the migration graph is reversible. They are not a promise that production rollback is always safe.

Production rollback should be planned per release. If a migration cannot safely preserve data on downgrade, document that in the release notes and prefer restoring from a database backup over relying on `down`.

## Review Checklist

For every schema change:

- Add matching SQLite and Postgres migration files.
- Add or update down migrations.
- Run SQLite migration round-trip tests.
- Add a tagged Postgres smoke test when the change touches production-critical behavior.
- Update DAL methods and tests for the new shape.
- Confirm old binaries against new schema and new binaries against old schema behavior, or document why the release requires a coordinated rollout.
- Update operator docs when the schema change affects deployment, retention, repair, security, or API behavior.

## Commands

```sh
go test ./internal/migrations
make test-postgres-integration
```
