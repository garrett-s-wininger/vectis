# Compatibility And Deprecation

This page defines the compatibility contract for Vectis clients, operators, and alternate component implementations. It complements [API_REFERENCE.md](../using/api-reference.md), [MIGRATIONS.md](../developing/migrations.md), and [RELEASES.md](../developing/releases.md).

Vectis is still pre-1.0 unless a release tag says otherwise. Even before 1.0, maintainers should treat the surfaces below as operator-facing contracts and call out incompatible changes in release notes.

## REST API

The shipped HTTP API is versioned under `/api/v1`. Within v1:

- Existing routes should not change method, path, authentication action, success status, or success response meaning without release-note treatment as a breaking change.
- JSON response objects may add fields. Clients should ignore unknown fields.
- Existing JSON fields should not be removed, renamed, or changed to an incompatible type within v1.
- Error responses use the envelopes documented in [API_REFERENCE.md](../using/api-reference.md). New `code` values may be added for more specific failures; existing code meanings should remain stable.
- `404` can mean either absent or hidden by namespace authorization. Clients must not use `404` to infer access to another namespace.
- Pagination cursors are opaque client tokens even when they currently look numeric. Clients should pass them back unchanged.

A future v2 API should use a new path prefix, for example `/api/v2`, and may run alongside v1 during a transition window. Release notes must state the overlap period and any v1 deprecation schedule.

## Idempotency And Retries

Only routes listed in [API_REFERENCE.md](../using/api-reference.md) and [IDEMPOTENCY_AND_RETRIES.md](../using/idempotency-and-retries.md) accept `Idempotency-Key`. Adding idempotency support to another mutating route is compatible. Removing idempotency support, changing key scope, or changing replay behavior is breaking.

Clients should treat `409 idempotency_key_reused` and `409 idempotency_in_progress` as stable v1 codes.

## gRPC And Protobuf

Protobuf contracts live in `api/proto/`; generated Go in `api/gen/go/` is committed but not hand-edited.

Compatible protobuf changes:

- Add new fields with new tag numbers.
- Add new enum values when receivers already tolerate unknown values or the release notes explain the rollout order.
- Add new RPCs without changing existing RPC behavior.

Incompatible protobuf changes:

- Reuse a tag number or field name for a different meaning.
- Remove a field without reserving its tag and name.
- Rename or change the type of an existing field.
- Change an RPC request, response, or error meaning in a way that breaks existing callers.

When removing a proto field or message, reserve the old tag and name in the `.proto` file and mention the removal in release notes.

## CLI Output

Human-readable CLI output is allowed to improve between releases. Machine-readable CLI output is a stronger contract:

- Commands with `--json` should keep existing field names and compatible field types.
- New JSON fields may be added.
- Exit code `0` means success. Non-zero means the requested operation did not complete successfully.
- Scripts should prefer `--json` where available and should not parse human tables.

Adding `--json` to more commands is compatible. Removing a `--json` field or changing its type is breaking unless release notes provide a migration path.

## Configuration

Environment variables, flags, and documented config keys are operator-facing contracts:

- Adding a new optional setting is compatible.
- Changing a default can be operationally breaking and must be called out in release notes.
- Removing or renaming a setting requires a deprecation window unless the setting was explicitly marked experimental.
- Service env prefixes are part of the contract for each binary; update [CONFIGURATION.md](../operating/configuration.md) and `cmd/AGENTS.md` together when they change.

## Schema And Releases

Database compatibility follows [MIGRATIONS.md](../developing/migrations.md) and [ADR 0004](../developing/architecture-decisions/0004-migration-compatibility-and-rollback.md). Release notes must say whether old binaries can run against the new schema, whether new binaries can start before migration, and whether mixed binary versions are supported.

Default deployment guidance remains: run one Vectis release version across long-running services unless [RELEASES.md](../developing/releases.md) or release notes explicitly allow version skew.

## Deprecation Policy

Deprecations should include:

- The affected surface and replacement.
- The first release that emits docs or warnings.
- The earliest release where removal may happen.
- Operator impact and rollback notes.

For REST v1, prefer additive replacement over removal. For config and CLI, keep the old spelling as an alias for at least one release when practical.
