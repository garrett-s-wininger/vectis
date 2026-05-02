# API — Protobuf and gRPC

Hand-written protos under [`proto/`](proto/); **`make proto`** (see root [`Makefile`](../Makefile)) regenerates [`gen/go/`](gen/go/) — never edit generated files.

**Buf** (lint, breaking, codegen): read repo-root [`../buf.yaml`](../buf.yaml) and [`../buf.gen.yaml`](../buf.gen.yaml); do not duplicate plugin or `go_package_prefix` settings here.

**Imports:** `api "vectis/api/gen/go"` (common convention).

**Proto packages:** each `.proto` file uses a flat package name matching its concern (`package common`, `package queue`, `package log`, etc.). Core job tree types live in [`common.proto`](proto/common.proto) (`Job`, `Node`, `LogChunk`, `Stream`, …).

## gRPC service definitions

| Proto service | Service name | Registers in |
|---------------|--------------|--------------|
| [`queue.proto`](proto/queue.proto) | `QueueService` | `internal/queue/` |
| [`log.proto`](proto/log.proto) | `LogService` | `internal/logserver/` |
| [`registry.proto`](proto/registry.proto) | `RegistryService` | `internal/registry/` |
| [`worker_control.proto`](proto/worker_control.proto) | `WorkerControlService` | `cmd/worker/` |

## Clients

`grpc.ClientConn` → `api.NewXClient(conn)`; domain-facing types in [`../internal/interfaces/`](../internal/interfaces/). TLS and dial options: [`../internal/tlsconfig/`](../internal/tlsconfig/), discovery: [`../internal/config/defaults.toml`](../internal/config/defaults.toml) `[grpc_tls]`, `[discovery]`.

## Adding a new RPC

1. Edit the relevant `proto/*.proto` — add message types and an RPC to the existing service (or create a new `.proto` + service).
2. `make proto` — regenerates `api/gen/go/`.
3. Register the server implementation in the directory listed in the table above, using the generated `RegisterXxxServer`.
4. If a consumer needs a domain-facing abstraction, add an interface in `internal/interfaces/` and wire a wrapper.

## Conventions

- Prefer unary RPCs unless streaming is required for backpressure or partial results.
- Use `common.Empty` for messages with no fields.
- Keep proto packages flat (`package queue` not `package vectis.queue`); the Buf config handles `go_package` mapping.
