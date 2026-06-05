# Compatibility

This page explains what Vectis clients and operators can rely on between releases. It covers public API behavior, CLI output, configuration, database migrations, and service version skew.

Vectis is still pre-1.0 unless a release tag says otherwise. Breaking changes can still happen. The user/operator contract is that incompatible changes should be visible in release notes, with enough detail to update clients, scripts, config, or rollout plans.

For upgrade procedure, see [Releases And Upgrades](../developing/releases.md). For database migration rules, see [Database Migrations](../developing/migrations.md).

## Compatibility At A Glance

| Surface | User/operator expectation | Safe additive changes |
| --- | --- | --- |
| REST API v1 | Method, path, auth action, success status, response meaning, and documented error `code` meanings stay stable unless release notes say otherwise. | New routes, new response fields, and more specific error codes. |
| Idempotency | Documented `Idempotency-Key` routes keep their retry and conflict behavior. | More mutating routes may gain idempotency support. |
| CLI automation | Exit code semantics and `--json` field compatibility are stronger contracts than human-readable tables. | New commands, new flags, new JSON fields, and improved human-readable output. |
| Configuration | Documented env vars, flags, config keys, service env prefixes, and default meanings do not disappear silently. | New optional settings and new deploy examples. |
| Protobuf/gRPC | Existing field tag meanings, RPC names, and request/response semantics remain compatible. | New fields, enum values, services, and RPCs when old callers remain compatible. |
| Database schema | Release notes explain upgrade order, rollback options, and version skew when schema changes matter. | Expand/contract schema changes, new tables, columns, indexes, and metadata. |

Clients should ignore unknown JSON fields and treat pagination cursors as opaque strings. Operators should read release notes before changing versions, especially when a release mentions database, auth, queue, worker, log, or config behavior.

## REST API V1

The shipped HTTP API lives under `/api/v1`. For users, v1 means existing routes are intended to remain stable unless release notes identify a breaking change.

| You can rely on | What that means |
| --- | --- |
| Method and path | A route such as `POST /api/v1/jobs/trigger/{id}` will not move or change method silently. |
| Authentication action | Required auth/RBAC actions should not become more restrictive without an upgrade note. |
| Success status | A successful request should keep the same status class and documented meaning. |
| Response shape | Existing fields should keep compatible names, types, and meanings. |
| Error envelope | Error responses use documented `code` values. Existing code meanings should stay stable. |

Additive changes are allowed. Vectis may add new routes, new response fields, and more specific error codes. Clients should ignore fields they do not understand.

Important client rules:

- Do not infer whether another namespace exists from `404`; it can mean either absent or hidden by authorization.
- Treat pagination cursors as opaque tokens, even when they look numeric today.
- Key retry and error handling off documented error `code` values rather than full message text.

For route details and error envelopes, see [API Reference](../using/api-reference.md).

## Idempotency And Retries

Only documented routes accept `Idempotency-Key`. For clients, the important question is whether a retry can safely refer to the same operation.

| Behavior | User/operator expectation |
| --- | --- |
| A route documents `Idempotency-Key` support | Retrying with the same key should follow the documented replay behavior. |
| A route does not document idempotency | The API rejects `Idempotency-Key` instead of silently ignoring retry intent. |
| `400 invalid_request_header` | The idempotency key was malformed, duplicated, or sent to a route that does not accept it. |
| `409 idempotency_key_reused` | The key was reused for a different operation or incompatible payload. |
| `409 idempotency_in_progress` | The original operation is still being processed. |

Clients should treat `409 idempotency_key_reused` and `409 idempotency_in_progress` as stable v1 codes. For route-by-route retry guidance, see [Idempotency And Retries](../using/idempotency-and-retries.md).

## CLI Automation

Human-readable CLI output may improve over time. Tables can be rearranged, labels can become clearer, and wording can change.

Machine-readable output is a stronger contract:

| You can rely on | What that means |
| --- | --- |
| Exit code `0` | The requested operation completed successfully. |
| Non-zero exit | The requested operation did not complete successfully. |
| `--json` fields | Existing field names and compatible field types should remain stable. |
| New JSON fields | Compatible; scripts should ignore fields they do not need. |

Scripts should prefer `--json` where available and avoid parsing human-readable tables.

## Configuration

Configuration is an operator-facing contract. This includes documented environment variables, flags, config keys, service env prefixes, and default behavior.

| You can rely on | What that means |
| --- | --- |
| Documented settings | Env vars, flags, and config keys should not disappear silently. |
| Defaults | Default changes that affect production behavior should be called out in release notes. |
| Service env prefixes | Prefix changes are breaking for operators and automation. |
| Deprecated spellings | When practical, old spellings should remain as aliases for at least one release. |

See [Configuration](../operating/configuration.md) for the current settings and prefixes.

## gRPC And Protobuf

Protobuf contracts live in `api/proto/`. Generated Go code in `api/gen/go/` is committed, but it is not hand-edited.

For implementers of alternate components or integrations, these protobuf changes should be compatible:

- Add a new field with a new tag number.
- Add a new enum value when receivers tolerate unknown values or release notes explain the rollout order.
- Add a new RPC without changing existing RPC behavior.
- Add a new service without requiring old clients or servers to use it.

These protobuf changes are incompatible unless release notes describe a coordinated migration:

- Reuse a tag number or field name for a different meaning.
- Remove a field without reserving its tag and name.
- Rename or change the type of an existing field.
- Change request, response, or error meaning in a way that breaks existing callers.

If a proto field or message disappears without a reserved tag/name and migration note, treat that as a compatibility issue.

## Database And Upgrades

Database compatibility follows the migration policy in [Database Migrations](../developing/migrations.md) and [ADR 0004](../developing/architecture-decisions/0004-migration-compatibility-and-rollback.md).

When release notes mention schema changes, operators should look for these answers:

| Question | Why it matters |
| --- | --- |
| Can old binaries run against the new schema? | Determines whether rollback can use previous artifacts only. |
| Can new binaries start before migration? | Determines rollout order. |
| Are mixed binary versions supported? | Determines whether rolling upgrades are safe. |
| Is downtime or a coordinated stop required? | Determines maintenance window needs. |
| What is the rollback path? | Determines whether to restore backup, roll forward, or run a down migration. |

Default guidance: run one Vectis release version across all long-running services unless release notes explicitly allow version skew.

## Deprecations

When a feature, setting, route, field, or behavior is deprecated, users and operators should expect the notice to explain:

| Field | What to look for |
| --- | --- |
| Affected surface | API route, JSON field, CLI output, config key, env var, proto field, or behavior. |
| Replacement | What to use instead. |
| First notice | The release where docs, warnings, or release notes first mention the deprecation. |
| Earliest removal | The earliest release where the old behavior may disappear. |
| Operator impact | What changes in deployment, rollback, scripts, or clients. |

For users, the safest migration path is additive replacement first, removal later. For config and CLI, expect old spellings or outputs to remain as aliases for at least one release when practical.

## What Counts As Breaking

Treat these as breaking unless release notes explicitly say the surface was experimental:

- Removing, renaming, or changing the meaning of an existing REST route.
- Removing an existing JSON field or changing its type.
- Changing a stable error `code` meaning.
- Removing idempotency support or changing replay semantics for a documented idempotent route.
- Removing a CLI `--json` field or changing its type.
- Renaming an env var, flag, config key, or service env prefix.
- Changing defaults in a way that can alter production behavior.
- Requiring a coordinated service stop where rolling upgrade was previously safe.
- Making a database migration that old binaries cannot tolerate without documenting the upgrade and rollback path.

## Maintainer Implications

The sections above describe what users and operators can rely on. The maintainer-side implication is simple: if a change violates one of those expectations, it needs breaking-change treatment in release notes and, where practical, a deprecation path.

For the detailed release checklist and migration review process, use [Releases And Upgrades](../developing/releases.md) and [Database Migrations](../developing/migrations.md).
