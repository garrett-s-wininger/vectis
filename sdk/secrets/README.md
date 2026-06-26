# Secrets SDK

`sdk/secrets` owns the extension-facing contract for secret providers behind the
Vectis secrets broker. Providers implement `Provider`, validate references, and
return materialized files through `Bundle`.

The broker remains responsible for SPIFFE caller authentication, execution-claim
authorization, audit, metrics, and protocol conversion. Provider implementations
should return `ErrNotFound` or `ErrDenied` when those outcomes are known so the
broker can classify resolution failures correctly.

Providers should run `sdk/secrets/conformance.RunProviderSuite` to verify the
shared provider contract: stable provider kind, supported and unsupported
reference validation, file delivery, unsupported delivery rejection, and
sentinel error preservation.
