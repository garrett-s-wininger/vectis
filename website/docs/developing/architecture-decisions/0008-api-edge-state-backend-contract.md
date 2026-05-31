# ADR 0008: API edge-state backend contract

## Status

Accepted

## Context

API replicas are mostly stateless, but some edge behavior is intentionally process-local today. Rate-limit buckets are the first concrete example: the default token bucket is simple and dependency-free, but adding API replicas raises the effective limit. Future features may need the same shape of shared edge state without making the HTTP API depend directly on a specific external system.

Options for shared API edge state included:

- **Keep all edge state process-local** — simplest, but only works when per-replica behavior is acceptable.
- **Hard-code Redis** — gives a familiar shared backend, but makes a new dependency the default shape.
- **Backend contract** — preserves the local default while allowing a hash-owner implementation or Redis proxy later.

## Decision

Use an interface-backed backend contract for API edge state. The first contract is `internal/api/ratelimit.RateLimiter`, which owns the atomic "allow this request key under this rule" decision.

The default implementation remains an in-process token bucket. Future implementations can:

- map each key to an owner with a simple hash ring and forward requests to that owner;
- proxy requests to Redis or another external store;
- keep the route middleware unchanged while changing only backend wiring.

This contract is separate from `vectis-registry`. The registry may help discover an owner or proxy endpoint, but it is not itself the shared edge-state store.

## Consequences

- Single-node and small deployments keep a no-dependency default.
- Multi-replica API deployments have a clear upgrade path for shared rate-limit budgets.
- The route middleware stays stable while backend implementations evolve.
- Backend implementations must preserve the same key semantics, rule semantics, and retry-after behavior.

## References

- [Scaling And Restarts](../../operating/deployment/scaling-and-restarts.md)
- [Configuration](../../operating/configuration.md)
- `internal/api/ratelimit/ratelimiter.go` — rate-limit backend contract
- `internal/api/ratelimit/memory.go` — default in-process implementation
