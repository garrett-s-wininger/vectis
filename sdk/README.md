# Vectis SDKs

Packages under `sdk/` are extension-facing contracts. They may wrap generated
protobuf types, provide conformance tests, or expose helper APIs for extension
authors.

SDK packages must not import `vectis/internal/...` or other control-plane
implementation packages. They may depend on:

- generated protobuf types in `api/gen/go`;
- other `sdk/...` packages;
- standard library or external dependencies already accepted by the module.

Current SDKs:

| Package | Purpose |
| --- | --- |
| `sdk/action` | Public descriptor, reference, schema, lifecycle, and digest types for action extensions. |
| `sdk/secrets` | Public provider, request, identity, bundle, and error types for secret-provider extensions. |
| `sdk/workercore` | Go SDK for worker-core execution providers. |
| `sdk/workercore/conformance` | Provider conformance tests for worker-core implementations. |

Run `make test-boundaries` before widening SDK dependencies, and run
`make sdk-worker-core-protos` when a non-Go worker-core provider needs the
supported protobuf closure.
