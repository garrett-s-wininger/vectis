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
| `sdk/workercore` | Go SDK for worker-core execution providers. |
| `sdk/workercore/conformance` | Provider conformance tests for worker-core implementations. |
