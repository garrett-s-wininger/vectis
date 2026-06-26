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

`worker_core.proto` defines both the worker-facing `WorkerCoreService` and the
worker shell callback `WorkerCoreShellService`. Keep providers on the protocol
version reported by `sdk/workercore.ProtocolVersion` and validated by
`DescribeCore`.
