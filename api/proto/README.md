# Protobuf Contracts

Most files in this directory define internal Vectis service-to-service gRPC
contracts. The worker-core protocol is intentionally extension-facing.

## Worker Core Consumers

Go worker-core providers should usually import the SDK instead of using the raw
generated service directly:

```go
import (
    api "vectis/api/gen/go"
    "vectis/sdk/workercore"
)
```

Non-Go providers should generate bindings from:

- `worker_core.proto`
- `common.proto`
- `secrets.proto`

Run `make sdk-worker-core-protos` to copy that supported proto closure to
`artifacts/sdk/workercore/proto/`.

`worker_core.proto` defines both the worker-facing `WorkerCoreService` and the
worker shell callback `WorkerCoreShellService`. Keep providers on the protocol
version reported by `sdk/workercore.ProtocolVersion` and validated by
`DescribeCore`.

Tests in this directory keep the worker-core import closure small and
extension-friendly. Adding new imports to `worker_core.proto` should be treated
as a protocol-surface decision because non-Go providers must be able to generate
the full closure.
