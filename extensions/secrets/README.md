# Secret Provider Extensions

Secret provider extensions resolve external secret references behind the
cell-local `vectis-secrets` broker. The broker owns execution-claim validation,
SPIFFE identity checks, access policy, redacted observability, and worker file
materialization. Providers own only provider-specific reference validation,
authentication, lookup, and error mapping.

The reusable provider contract lives in `sdk/secrets`. Standard provider
implementations live here once their packaging and operational shape are stable
enough to reuse outside core Vectis. The first standard provider extensions are
`encryptedfs/` for local encrypted-file storage and `knox/` for Knox
primary-version reads.

Provider packages should return `sdk/secrets.ErrNotFound` or
`sdk/secrets.ErrDenied` when those outcomes are known so the broker can preserve
accurate audit and metrics classification.
