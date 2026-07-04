# API — Protobuf and gRPC

Hand-written protos under [`proto/`](proto/); **`mage proto`** regenerates [`gen/go/`](gen/go/) — never edit generated files.

This directory is for gRPC contracts and generated protobuf code. Most services
are internal Vectis component contracts. Worker-core protos are also an
extension-facing contract consumed by `sdk/workercore` providers. The public
HTTP API lives in [`../internal/api/`](../internal/api/) and is documented in
[`../website/docs/using/api-reference.md`](../website/docs/using/api-reference.md).

**Codegen:** `mage proto` invokes local `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`. Keep each `.proto` file's `go_package` option aligned with `vectis/api/gen/go;api`.

**Imports:** `api "vectis/api/gen/go"` (common convention).

**Proto packages:** each `.proto` file uses a flat package name matching its concern (`package common`, `package queue`, `package log`, etc.). Core job tree types live in [`common.proto`](proto/common.proto) (`Job`, `Node`, `LogChunk`, `Stream`, …).

## gRPC service definitions

| Proto service | Service name | Registers in |
|---------------|--------------|--------------|
| [`queue.proto`](proto/queue.proto) | `QueueService` | `internal/queue/` |
| [`log.proto`](proto/log.proto) | `LogService` | `internal/logserver/` |
| [`artifact.proto`](proto/artifact.proto) | `ArtifactService` | `internal/artifact/` |
| [`orchestrator.proto`](proto/orchestrator.proto) | `OrchestratorService` | `internal/orchestrator/` |
| [`registry.proto`](proto/registry.proto) | `RegistryService` | `internal/registry/` |
| [`worker_control.proto`](proto/worker_control.proto) | `WorkerControlService` | `cmd/worker/` |
| [`worker_core.proto`](proto/worker_core.proto) | `WorkerCoreService`, `WorkerCoreShellService` | `sdk/workercore/`, `internal/workercore/` |
| [`secrets.proto`](proto/secrets.proto) | `SecretsService` | `internal/secrets/` |

## Clients

`grpc.ClientConn` → `api.NewXClient(conn)`; domain-facing types in [`../internal/interfaces/`](../internal/interfaces/). TLS and dial options: [`../internal/tlsconfig/`](../internal/tlsconfig/), discovery: [`../internal/config/defaults.toml`](../internal/config/defaults.toml) `[grpc_tls]`, `[discovery]`.

## Adding or changing an RPC

1. Edit the relevant `proto/*.proto` — add message types and an RPC to the existing service (or create a new `.proto` + service).
2. `mage proto` — regenerates `api/gen/go/`.
3. Register the server implementation in the directory listed in the table above, using the generated `RegisterXxxServer`.
4. If a consumer needs a domain-facing abstraction, add an interface in `internal/interfaces/` and wire a wrapper.
5. Update docs when the RPC changes operator-visible behavior, component topology, or compatibility expectations.

## Conventions

- Prefer unary RPCs unless streaming is required for backpressure or partial results.
- Use `common.Empty` for messages with no fields.
- Keep `go_package` explicit and stable so raw `protoc` can regenerate without network-backed tooling.
- Treat `worker_core.proto`, plus its `common.proto` and `secrets.proto` import
  surface, as consumable by extension authors. Prefer additive changes and
  document compatibility-impacting protocol changes in
  [`../website/docs/developing/worker-core-sdk.md`](../website/docs/developing/worker-core-sdk.md).
