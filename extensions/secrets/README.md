# Secret Provider Extensions

Secret provider extensions resolve external secret references behind the
cell-local `vectis-secrets` broker. The broker owns execution-claim validation,
SPIFFE identity checks, access policy, redacted observability, and worker file
materialization. Providers own only provider-specific reference validation,
authentication, lookup, and error mapping.

The provider boundary is not yet a public SDK. Current providers still live in
`internal/secrets` until the reusable contract is extracted into a supported
package or provider protocol. When that happens, provider implementations such
as Knox should move here without changing the worker-facing secret delivery
path.
