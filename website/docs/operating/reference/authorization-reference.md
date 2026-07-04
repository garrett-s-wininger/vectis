# Authorization Reference

This page catalogs Vectis HTTP API authorization actions, namespace roles, token scope behavior, and route families. Use it when designing user access, creating scoped API tokens, auditing role bindings, or reviewing whether a route should be protected by a different action.

For the exact route inventory, see [API Reference](../../using/api-reference.md#routes). For local user, token, session, and browser security posture, see [Security](../../concepts/security.md). For SQL storage of users, role bindings, and token scopes, see [Database Schema Reference](./database-schema.md#namespace-and-authorization-tables).

## Enforcement Modes

Authentication and authorization are separate controls.

| State or engine | Behavior |
| --- | --- |
| API auth disabled | Route auth is not enforced. This is the local/development posture and should not be used for exposed deployments. |
| Setup pending | Only setup actions are allowed. Normal protected API actions return `setup_required`. |
| `hierarchical_rbac` | Default post-setup engine. Local users receive namespace roles, roles inherit down the namespace tree, and token scopes can only reduce the user's effective permission. |
| `authenticated_full` | Any authenticated principal may perform non-setup actions. Scoped API tokens still need a matching scope action. |
| Unknown authz engine or missing RBAC repositories | Denies protected requests after setup. |

The access middleware first checks the route's coarse action without a namespace resource. Namespace-aware handlers then resolve the concrete namespace from a job, run, namespace path, or namespace ID and perform the resource-specific check. A caller can therefore pass the first route gate and still receive `authorization_denied` after the handler identifies the target namespace.

## Action Catalog

| Action | Namespace scope | Used for |
| --- | --- | --- |
| `job:read` | Yes | Reading stored jobs and namespaces. |
| `job:write` | Yes | Creating, updating, and deleting stored jobs. |
| `run:trigger` | Yes | Starting ephemeral runs and triggering stored jobs. |
| `run:read` | Yes | Reading runs, tasks, logs, artifacts, and run event streams. |
| `run:operator` | Yes | Replay, cancellation, run repair, and frozen execution payload access. |
| `admin:*` | Yes | Operational diagnostics, metrics, namespace administration, and namespace role-binding administration. |
| `catalog:ingest` | No | Cell catalog event ingestion. Under `hierarchical_rbac`, this requires `admin` on the root namespace. |
| `user:admin` | No | Local user administration. Under `hierarchical_rbac`, this requires `admin` on the root namespace. |
| `setup:status` | No | Reading initial setup status. |
| `setup:complete` | No | Completing first-admin setup with the bootstrap token. |
| `api:any` | No | Authenticated self-service routes such as logout, API token management, and password changes. Under `hierarchical_rbac`, any authenticated user can use these routes; scoped tokens must include `api:any`. |

`setup:status` and `setup:complete` are setup-only actions. They are not valid API token scopes.

## Role Matrix

Namespace roles grant actions at the namespace where they are bound and, unless inheritance is broken, to descendant namespaces.

| Role | Direct actions | Operator intent |
| --- | --- | --- |
| `viewer` | `job:read`, `run:read` | Inspect jobs, runs, logs, tasks, and artifacts. |
| `trigger` | `job:read`, `run:read`, `run:trigger` | Run existing jobs or submit one-off runs without editing stored jobs. |
| `operator` | `job:read`, `run:read`, `run:trigger`, `run:operator` | Operate runs: replay, cancel, repair, and inspect execution payloads. |
| `admin` | `job:read`, `job:write`, `run:read`, `run:trigger`, `run:operator`, `admin:*`, `api:any` | Manage namespace-local jobs and namespace administration surfaces. Root `admin` also grants `catalog:ingest` and `user:admin`. |

Notes:

- `admin` on a non-root namespace does not grant `catalog:ingest` or `user:admin`.
- `api:any` routes are self-service routes for authenticated principals; the `admin` role includes `api:any`, but the RBAC engine does not require a namespace role for ordinary self-service access.
- Non-namespaced checks for actions other than `api:any`, `catalog:ingest`, and `user:admin` require the user to hold a role granting that action somewhere in the namespace tree. Handlers that know a concrete namespace perform a stricter namespace check afterward.

## Namespace Inheritance

Namespaces form a tree rooted at `/`.

| Rule | Effect |
| --- | --- |
| Role inheritance | A role binding on `/teams` can authorize matching actions on `/teams/build` and deeper descendants. |
| `break_inheritance` | When set on a namespace, inherited roles from ancestors are cleared at that namespace and below. |
| Explicit descendant role | A role bound below a break still applies at that namespace and its descendants. |
| Missing namespace or repository error | Authorization fails closed. |

The namespace path is the authorization boundary. A stored job, run, log stream, artifact, and repair route resolves back to a namespace before the handler finishes the authorization decision. Direct inline runs created through `POST /api/v1/jobs/run` resolve to the system `/ephemeral` namespace.

## Token Scopes

Unscoped API tokens act as the owning user. Scoped API tokens are an intersection: the user must already have the base RBAC permission, and the token must also carry a matching scope.

| Scope rule | Behavior |
| --- | --- |
| Action validation | Scope actions must be registered actions other than `setup:status` or `setup:complete`. |
| Namespace path required | `job:read`, `job:write`, `run:trigger`, `run:read`, `run:operator`, and `admin:*` scopes require a `namespace_path` when created through the API. |
| Namespace path forbidden | `api:any`, `catalog:ingest`, and `user:admin` scopes cannot include a `namespace_path`. |
| Exact namespace | A namespace scope applies to the exact namespace it names. |
| Propagation | `propagate=true` lets the scope apply to descendants unless inheritance is broken between the scoped namespace and the target namespace. |
| Creation guard | A caller can only create scopes for actions and namespaces the caller is already authorized to use. Scoped tokens cannot create broader scopes or unscoped escalation tokens. |

Example scoped token request:

```json
{
  "label": "team-a-reader",
  "scopes": [
    {
      "action": "job:read",
      "namespace_path": "/team-a",
      "propagate": true
    },
    {
      "action": "run:read",
      "namespace_path": "/team-a",
      "propagate": true
    },
    {
      "action": "api:any"
    }
  ]
}
```

## Route Families

| Route family | Required action |
| --- | --- |
| `/health/live`, `/health/ready`, and `POST /api/v1/login` | Public |
| `GET /api/v1/setup/status` | `setup:status` |
| `POST /api/v1/setup/complete` | `setup:complete` |
| Operational diagnostics and `/metrics` | `admin:*` |
| Job reads and namespace reads | `job:read` |
| Stored job writes | `job:write` |
| Ephemeral run and stored-job trigger routes | `run:trigger` |
| Run detail, task, log, artifact, and SSE read routes | `run:read` |
| Run replay, cancellation, repair, force-repair, and execution payload routes | `run:operator` |
| Cell catalog event ingestion | `catalog:ingest` |
| Logout, token management, and password changes | `api:any` |
| User administration | `user:admin` |
| Namespace creation, deletion, and role-binding administration | `admin:*` |

Use [API Reference](../../using/api-reference.md#routes) for the exact method and path list.

## Related Docs

| Need | Doc |
| --- | --- |
| Exact route table and route auth action labels | [API Reference](../../using/api-reference.md#routes) |
| Machine-readable route contract | [OpenAPI Specification](../../using/openapi-specification.md) |
| CLI commands for users, tokens, namespaces, and role bindings | [CLI Guide](../../using/cli-guide.md) |
| Auth, token, session, and deployment security posture | [Security](../../concepts/security.md) |
| Authorization tables and constraints | [Database Schema Reference](./database-schema.md#namespace-and-authorization-tables) |
