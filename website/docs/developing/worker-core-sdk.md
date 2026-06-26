# Worker Core SDK

The worker-core boundary is the supported extension point for bringing an external execution system to Vectis. Use it when a provider should execute a claimed task while Vectis keeps ownership of queue claims, leases, cancellation intent, logs, artifacts, policy gates, and final task state.

The Go SDK lives in `sdk/workercore`. It wraps the generated WorkerCore gRPC API and the shell callback API so a provider does not need to hand-roll protobuf conversion, Unix-socket serving, log streaming, or artifact publishing.

The worker-core protocol is an extension contract, not an implementation
detail. Go providers should import `sdk/workercore` and generated message types
from `api/gen/go`. Non-Go providers should generate from `api/proto/worker_core.proto`
and its imports, currently `api/proto/common.proto` and `api/proto/secrets.proto`.
Run `make sdk-worker-core-protos` to copy that supported proto closure to
`artifacts/sdk/workercore/proto/`.
Changes to those protocol files follow the repository's gRPC compatibility
rules.

In-repository standard providers live under `extensions/worker-core/`. Tests
enforce that SDK and extension packages do not import `vectis/internal/...` or
other core implementation packages. Run `make test-boundaries` for the narrow
extension-contract checks. If a provider needs something from `internal/`,
extract a supported SDK or protobuf contract first.

## When To Use It

Use a worker core when Vectis is the source of truth for the work and the provider is the place where that work runs. Use an action when the provider is the source of truth for the side effect.

| Integration shape | Extension point |
| --- | --- |
| Trigger an existing Jenkins pipeline with parameters | Action |
| Run normal Vectis tasks on Jenkins agents | Worker core |
| Deploy through Argo CD or another domain controller | Action |
| Run a Vectis task in a Kubernetes Job or VM | Worker core |
| Create an issue, run a scanner, call a release API | Action |

Externality is not the deciding factor. The deciding factor is ownership: actions delegate domain side effects; worker cores delegate execution while preserving Vectis semantics.

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
| `workercore.Failure("message"), nil` | The external execution completed and the task failed. Uses `worker-core.execution_failed`. |
| `workercore.FailureWithReason("provider.reason", "message"), nil` | The external execution completed and the task failed with a provider-specific reason code. |
| `workercore.Unknown("message"), nil` | The provider cannot prove success or failure, usually due to lost external state. Uses `worker-core.unknown`. |
| `workercore.Cancelled("message"), nil` | The provider stopped because Vectis or the external system cancelled the task. Uses `worker-core.cancelled`. |
| `workercore.ExternalUnavailable("message"), nil` | The provider cannot determine the task outcome because the external system is unavailable. Uses `worker-core.external_unavailable`. |
| `Result{}, err` | The worker-core implementation or transport failed before it could produce a task outcome. |

Adapters should observe the provided context and implement `CancelTask`. When cancellation is requested, stop launching new external work, request cancellation in the external system when possible, and return `Cancelled(...)` if the final external outcome is not known. `CancelTask` should be best-effort and idempotent: if the session is already complete or unknown, return nil unless the provider itself is unhealthy.

Reason codes are stable strings carried over the worker-core protocol so the worker can preserve provider intent separately from human-readable messages. Use the SDK constants for common cases and namespaced provider strings for provider-specific states, such as `jenkins.queue_timeout` or `kubernetes.pod_unschedulable`.

Vectis maps worker-core results into run state conservatively. `Failure(...)` and `FailureWithReason(...)` fail the task execution. `Cancelled(...)` finalizes the execution as cancelled. `Unknown(...)`, `UnknownWithReason(...)`, and `ExternalUnavailable(...)` mark the run orphaned with `worker_core_unknown` so an operator or reconciler path can repair the run once the external outcome is known.

This tranche does not add a structured external execution reference field. Providers should emit durable external IDs or URLs through logs and artifacts, and include provider-specific reason codes in task results. A future protocol revision can add first-class execution references once provider integrations prove the common shape.

## Capabilities

Use the SDK constants when reporting standard behavior:

| Capability | Meaning |
| --- | --- |
| `workercore.CapabilityExecute` | The core can execute tasks. |
| `workercore.CapabilityCancelTask` | The core accepts explicit task cancellation requests. |
| `workercore.CapabilityShellLogCallback` | The core can stream logs through the worker shell callback socket. |
| `workercore.CapabilityShellArtifactPush` | The core can publish artifacts through the worker shell callback socket. |

Providers can add their own namespaced capabilities for provider-specific behavior, such as a Jenkins or Kubernetes integration feature flag.

The worker validates `Describe` during startup. A core used by `vectis-worker` must report the current protocol version and all four standard capabilities above, because the worker shell always relies on execute, cancellation, log callback, and artifact callback semantics when handing out task sessions.

## Deployment Topology

Run `vectis-worker` and its core as a paired local failure domain. The worker connects to the core socket configured by `--core-socket` / `VECTIS_WORKER_CORE_SOCKET`; the core serves `--socket` / `VECTIS_WORKER_CORE_SOCKET`. The worker also exposes a shell callback socket configured by `--core-shell-socket` / `VECTIS_WORKER_CORE_SHELL_SOCKET`, and each task session gives the core that callback endpoint for logs and artifacts.

Preferred deployment shapes are a same-pod sidecar or a same-host supervisor group with a private shared runtime directory. The UDS files should be writable only by the worker/core service account. Treat network-forwarded worker-core sockets as out of scope for this contract unless the provider adds its own transport security, lifecycle, and conformance tests.

The worker should start only after the core socket is reachable. On startup it calls `Describe` and rejects incompatible protocol versions or missing standard capabilities before dequeuing work.

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

The repository includes a minimal runnable example in
`examples/worker-core-external` and a Kubernetes Job-backed executable-task
provider in `extensions/worker-core/kubernetes`.

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

If the provider is served as a standalone Unix-socket process, also test the worker-facing protocol directly:

```go
conformance.RunCoreServerSuite(t, func(t *testing.T) string {
    return startProviderAndReturnSocket(t)
}, conformance.Options{
    RequireLogCallback:      true,
    RequireArtifactCallback: true,
})
```

The suites check description shape, required capabilities, a simple Vectis task execution, explicit cancellation, and optional shell callback behavior. Keep provider-specific tests for external-system behavior, retry policy, cleanup, and credential handling alongside the conformance suite.

## Non-Goals

The worker-core SDK does not replace the queue, registry, reconciler, database, task-finalization, or service-discovery contracts. Those components carry Vectis control-plane invariants and remain internal implementation surfaces unless they receive their own explicit SDK contract later.
