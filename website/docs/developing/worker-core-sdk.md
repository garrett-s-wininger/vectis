# Worker Core SDK

The worker-core boundary is the supported extension point for bringing an external execution system to Vectis. Use it when a provider should execute a claimed task while Vectis keeps ownership of queue claims, leases, cancellation intent, logs, artifacts, policy gates, and final task state.

The Go SDK lives in `sdk/workercore`. It wraps the generated WorkerCore gRPC API and the shell callback API so a provider does not need to hand-roll protobuf conversion, Unix-socket serving, log streaming, or artifact publishing.

## Contract Shape

Implement `workercore.Core`:

```go
type Core interface {
    Describe(context.Context) (workercore.Description, error)
    ExecuteTask(context.Context, workercore.Task) (workercore.Result, error)
    CancelTask(context.Context, workercore.CancelRequest) error
}
```

`Describe` reports protocol version, capabilities, metadata, and supported isolation levels. `ExecuteTask` receives the Vectis job, task key, and shell-owned session. `CancelTask` is called when Vectis observes a remote or durable cancellation request for the active execution.

The result/error split is intentional:

| Return | Meaning |
| --- | --- |
| `workercore.Success(), nil` | The task completed successfully. |
| `workercore.Failure("message"), nil` | The external execution completed and the task failed. |
| `workercore.Unknown("message"), nil` | The provider cannot prove success or failure, usually due to cancellation or lost external state. |
| `Result{}, err` | The worker-core implementation or transport failed before it could produce a task outcome. |

Adapters should observe the provided context and implement `CancelTask`. When cancellation is requested, stop launching new external work, request cancellation in the external system when possible, and return `Unknown(...)` if the final external outcome is not known. `CancelTask` should be best-effort and idempotent: if the session is already complete or unknown, return nil unless the provider itself is unhealthy.

## Capabilities

Use the SDK constants when reporting standard behavior:

| Capability | Meaning |
| --- | --- |
| `workercore.CapabilityExecute` | The core can execute tasks. |
| `workercore.CapabilityCancelTask` | The core accepts explicit task cancellation requests. |
| `workercore.CapabilityShellLogCallback` | The core can stream logs through the worker shell callback socket. |
| `workercore.CapabilityShellArtifactPush` | The core can publish artifacts through the worker shell callback socket. |

Providers can add their own namespaced capabilities for provider-specific behavior, such as a Jenkins or Kubernetes integration feature flag.

## Shell Callbacks

The worker shell registers a per-execution callback socket and includes it in the task session. A core can use:

```go
stream, err := task.Session.OpenLogStream(ctx)
artifact, err := task.Session.PublishArtifact(ctx, workercore.ArtifactRequest{...})
```

Only use these helpers when `task.Session.LogsEnabled()` or `task.Session.ArtifactsEnabled()` is true. The shell validates the session ID and forwards logs/artifacts through Vectis-owned services.

## Serving A Core

The SDK can serve a core over the same Unix-domain socket transport used by `vectis-worker`:

```go
server, listener, err := workercore.NewUnixCoreServer(socketPath, myCore, workercore.ServiceOptions{})
if err != nil {
    return err
}
return server.Serve(listener)
```

The repository includes a minimal runnable example in `examples/worker-core-external`.

## Conformance

Use `sdk/workercore/conformance` in provider tests:

```go
conformance.RunCoreSuite(t, func(t *testing.T) workercore.Core {
    return myCore
}, conformance.Options{
    RequireLogCallback:      true,
    RequireArtifactCallback: true,
})
```

The suite checks description shape, standard capabilities, a simple Vectis task execution, explicit cancellation, and optional shell callback behavior. Keep provider-specific tests for external-system behavior, retry policy, cleanup, and credential handling alongside the conformance suite.

## Non-Goals

The worker-core SDK does not replace the queue, registry, reconciler, database, task-finalization, or service-discovery contracts. Those components carry Vectis control-plane invariants and remain internal implementation surfaces unless they receive their own explicit SDK contract later.
