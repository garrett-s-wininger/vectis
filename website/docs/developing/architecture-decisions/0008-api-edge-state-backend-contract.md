# ADR 0008: API edge-state backend contract

## Status

Accepted

## Context

API replicas are mostly stateless, but some edge behavior needs a shared backend when the API is fanned out. Rate-limit buckets are the first concrete example. Future features may need the same shape of shared edge state without making the HTTP API depend directly on a specific external system.

Options for shared API edge state included:

- **Keep all edge state process-local** — simplest, but only works when per-replica behavior is acceptable.
- **Hard-code Redis** — gives a familiar shared backend, but makes a new dependency the default shape.
- **Backend contract** — preserves backend choice while allowing SQL, a hash-owner implementation, or a Redis proxy.

## Decision

Use an interface-backed backend contract for API edge state. The shared contract is `internal/cache.Service`, which owns login sessions and atomic "allow this request key under this rule" decisions. `internal/api/ratelimit.RateLimiter` remains the HTTP middleware adapter.

The default implementation stores sessions and rate-limit buckets in the shared SQL database. The in-process cache remains available for deployments that explicitly want per-replica state. Future implementations can:

- map each key to an owner with a simple hash ring and forward requests to that owner;
- proxy requests to Redis or another external store;
- keep the route middleware unchanged while changing only backend wiring.

This contract is separate from `vectis-registry`. The registry may help discover an owner or proxy endpoint, but it is not itself the shared edge-state store.

## Consequences

- Single-node and small deployments can still choose an in-memory no-dependency backend.
- Multi-replica API deployments get shared login sessions and rate-limit budgets by default.
- The route middleware stays stable while backend implementations evolve.
- Backend implementations must preserve the same key semantics, rule semantics, and retry-after behavior.

## References

- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- [Configuration](../../operating/configuration.md)
- `internal/cache/cache.go` — API cache backend contract
- `internal/cache/sql.go` — default shared SQL implementation
- `internal/api/ratelimit/ratelimiter.go` — HTTP rate-limit adapter contract
