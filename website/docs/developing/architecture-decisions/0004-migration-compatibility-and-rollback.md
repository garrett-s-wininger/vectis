# ADR 0004: Migration Compatibility And Rollback

## Status

Accepted

## Context

Vectis supports SQLite for local and small deployments and Postgres for production-oriented deployments. The same binaries may be rolled out gradually, and runtime services wait for the expected schema instead of applying migrations themselves.

Operators need database changes to have a predictable compatibility and rollback story. Maintainers need migration review to cover behavior across both supported backends, not just whether the SQL applies cleanly.

## Decision

Schema changes must use an expand/contract rollout unless the release notes explicitly call out downtime or a coordinated stop-the-world upgrade.

Each migration version must exist for both supported backends. SQL may differ between `internal/migrations/sqlite/` and `internal/migrations/postgres/`, but the semantic version and operator-facing outcome must match.

Release notes for any schema change must assess:

- Old binary against new schema.
- New binary against old schema.
- Whether the rollout allows mixed binary versions.
- Whether rollback means restoring a database backup, rolling forward with a repair migration, or running a down migration.

Down migrations are required for development reversibility and automated confidence. They are not, by themselves, a production rollback promise. A production rollback that could lose or corrupt data must be documented as backup restore or roll-forward repair.

## Consequences

- CI should fail when a migration version exists for only one backend.
- Operators can read a release note and know whether a migration allows rolling upgrades.
- Maintainers must keep migration review focused on compatibility, not just SQL syntax.
- Some releases may choose explicit downtime when compatibility would be more dangerous than a coordinated rollout.

## References

- [Database Migrations](../migrations.md)
- [Releases](../releases.md)
- `internal/migrations/sqlite/`
- `internal/migrations/postgres/`
