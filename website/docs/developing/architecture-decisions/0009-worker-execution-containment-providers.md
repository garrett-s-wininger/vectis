# ADR 0009: Worker execution containment providers

## Status

Accepted

## Context

Workers currently provide workspace separation, not a security boundary. `internal/job/executor.go` creates a per-run directory, and built-in actions such as `builtins/shell` and `builtins/checkout` start host processes in that directory. That keeps run files from colliding and makes cleanup predictable, but the process still shares the worker user, host kernel, process namespace, network view, and any credentials or mounts available to the worker.

That posture is useful for trusted local development and trusted CI, but it is not enough for operators who want to test less-trusted repositories, enforce stricter resource limits, or support environments where workload isolation is a procurement or compliance requirement.

The available primitives differ by operating system:

- Linux has namespaces, cgroups, seccomp, LSMs, and OCI container runtimes.
- macOS has sandboxing and virtualization primitives, while common container workflows usually run Linux containers inside a VM.
- Windows has job objects, containers, AppContainer, Hyper-V, and WSL-shaped Linux environments.

Vectis needs an execution model that can use those primitives without making each action know how to start a container, boot a VM, or clean up provider-specific state.

## Decision

Treat worker execution containment as an explicit worker-local provider behind a common runner boundary.

The default provider remains `host`. It preserves current behavior: create a per-run workspace and execute built-in actions as host child processes. Documentation and user-facing output must describe this as workspace separation, not as a secure sandbox.

Add stronger providers in layers:

1. `container`: run the whole job or segment inside an OCI-style container where a supported runtime is available. The provider owns image selection, workspace mounts, environment filtering, resource limits, network mode, read-only root configuration, user mapping, and cleanup. Container providers improve filesystem, process, and resource isolation, but they still share a kernel with the container runtime's host or VM.
2. `vm`: run the whole job or segment inside a disposable VM created from a prepared image or snapshot. The provider owns boot, guest transport, workspace transfer or mount, network policy, resource limits, cancellation, log forwarding, and teardown. VM providers are the target for stronger untrusted-workload isolation, at higher startup and operational cost.

The runner boundary should sit under the worker and above action implementation:

- Workers still claim runs, renew leases, open durable log streams, observe cancellation, and finalize status.
- A runner owns the per-run execution environment, command execution adapter, workspace lifecycle, environment policy, resource policy, and cleanup.
- Built-in actions keep their stable semantics. `builtins/checkout` still means "clone into the workspace", and `builtins/shell` still means "run this command in the workspace"; the configured runner decides where that command actually runs.
- The default execution unit is a whole run or cell-local segment, not one isolated environment per node. That preserves the common checkout-then-build workspace flow. Per-step isolation can be considered later only with explicit artifact, cache, and workspace semantics.
- Jobs should request portable execution profiles or capabilities rather than raw runtime-specific flags. Workers should advertise or be assigned the profiles they can satisfy. A job that needs `container` or `vm` containment should not silently fall back to `host`.

Provider configuration must be operator-controlled. Job authors can select an allowed profile, but should not be able to directly request privileged containers, arbitrary host mounts, host networking, VM images, or device access unless policy explicitly permits it.

## Consequences

- Vectis can offer a graduated isolation story without changing the job graph action model.
- Existing deployments keep the no-extra-dependency host path.
- Container support becomes the first practical improvement for many operators, especially Linux workers and Podman/Docker-based deployments, while docs remain clear that containers are not equivalent to VM isolation.
- VM support gives a stronger target for hostile or regulated workloads, but requires image lifecycle, guest tooling, startup latency, artifact transfer, network policy, and cleanup design.
- Worker scheduling needs profile awareness before Vectis can safely mix host, container, and VM workers in the same cell.
- Cancellation must become provider-aware: first signal the running command or guest, then enforce a bounded cleanup path for containers or VMs.
- Observability must include the selected provider/profile and provider setup failures so operators can distinguish "job failed" from "runner could not prepare the environment."
- Security documentation must avoid blanket claims. Each provider needs an explicit posture, assumptions, and known limits.

## Implementation Milestones

1. Introduce the runner boundary while keeping the current `host` behavior unchanged.
2. Move direct process creation behind the runner-provided command executor so built-in actions do not instantiate host execution by default.
3. Add worker execution profiles with validation, documentation, and no silent fallback across isolation levels.
4. Implement a container provider with conservative defaults: non-privileged, minimal mounts, environment allowlist, configurable image, resource limits, cleanup on cancellation, and documented runtime requirements.
5. Add placement and operational checks so workers only receive runs whose requested profile they can satisfy.
6. Implement VM providers behind the same runner contract, starting with one supported backend before expanding provider-specific drivers.
7. Add run metadata, metrics, and log annotations for provider, profile, setup time, cleanup outcome, and policy denial.

## Non-Goals

- Vectis will not treat separate directories as a secure sandbox.
- Vectis will not let job definitions directly pass through arbitrary container or VM runtime flags.
- Vectis will not optimize for shared mutable workspaces across cells.
- Vectis will not claim that one provider is sufficient for all threat models.

## References

- [Planning](../roadmap/planning.md)
- [Architecture](../../concepts/architecture.md)
- [Security](../../concepts/security.md)
- [Failure Domains](../../concepts/failure-domains.md)
- [Adding Actions](../actions.md)
- [ADR 0006: Global coordination and cell-local execution](./0006-global-coordination-cell-local-execution.md)
- `internal/job/executor.go` - current per-run workspace lifecycle and job execution
- `internal/action/builtins/shell.go` - current shell action process start
- `internal/action/builtins/checkout.go` - current checkout action process start
