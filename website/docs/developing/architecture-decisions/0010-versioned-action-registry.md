# ADR 0010: Versioned action registry

## Status

Accepted

## Context

Vectis originally resolved every job node `uses` value through the in-process builtin registry. That kept execution simple, but it made custom action implementations a source change instead of a user extension point.

Users need readable action references, such as `examples/greet@v1`, while operators need reproducible runs. A version selector is convenient for authors, but a run, retry, or replay should execute the same implementation that validation accepted. The control plane also needs enough metadata to validate `with` fields before a worker accepts work.

This action registry is separate from `vectis-registry`, which is the service discovery registry. The action registry resolves action references and implementation metadata; it does not find live service addresses.

Design constraints:

- Existing builtin action references must keep working, including short builtin names.
- Validation must stay available before job storage or ephemeral run dispatch.
- Run creation must freeze resolved action implementations so worker execution is deterministic.
- Workers should not load arbitrary user code into the Vectis process.
- Custom process actions must receive Vectis-controlled execution state and a sanitized command environment, not ambient worker secrets.

## Decision

Introduce a versioned action registry contract that resolves a user-facing action reference into an immutable action descriptor.

Action references use this shape:

```text
<namespace>/<name>[@<selector>]
```

Examples:

```text
builtins/script
builtins/script@v1
examples/greet@v1
examples/greet@sha256:<64-hex-digest>
```

Selectors may be a compatibility version or channel, such as `v1`, or an immutable digest, such as `sha256:<hex>`. Non-builtin custom references use a namespace to avoid collisions.

The registry returns a descriptor with:

| Field | Purpose |
| --- | --- |
| `canonical_name` | Stable namespaced name, for example `examples/greet`. |
| `display_name` | Friendly UI/docs label. |
| `version` | Resolved compatibility or semantic version. |
| `digest` | Immutable hash of the descriptor. |
| `source` | Builtin, local filesystem, OCI artifact, or future remote catalog source. |
| `runtime` | Execution adapter, such as builtin, process, container, WASM, or gRPC. |
| `runtime_config` | Runtime-specific settings, such as a process command. |
| `input_schema` | Declarative validation schema for `with` and bound `inputs`. |
| `port_schema` | Declarative child-port contract for control-flow actions. |
| `local_only` | Whether the action can contain only local child execution. |
| `capabilities` | Declared needs such as process launch, network, workspace read/write, or future secrets access. |
| `status` | Lifecycle state: active, yanked, revoked, or purged. |
| `status_reason` | Operator-facing explanation for non-active descriptors. |

Builtin actions are the first registry source. A local filesystem manifest source is the first custom source. Local manifests live under:

```text
<root>/<namespace>/<name>/action.json
<root>/<namespace>/<name>/<version>/action.json
```

The first custom runtime is `process`, which executes out of process through the worker command adapter. Vectis does not dynamically link user-supplied Go code into the worker. The process adapter maps Vectis action inputs into `VECTIS_INPUT_<FIELD_NAME>` environment variables and uses the same sanitized process environment as builtins.

The API, CLI, cron service, and reconciler use the configured descriptor resolver. Workers also use it to execute custom actions from frozen action locks. The execution envelope records action locks, and task spec hashes include resolved action digests so task-level retries and repair decisions see implementation changes.

Removing an action is represented as lifecycle metadata, not only as absence. A `yanked` descriptor is hidden from default discovery and rejected for version-selector references, but digest-pinned historical execution may continue. `revoked` and `purged` descriptors block new runs and historical execution. Operators should keep tombstone descriptors with the original digest for vulnerable or intentionally unavailable implementations so old runs remain explainable and blocked by policy. If the live descriptor is absent, frozen action locks remain the audit source for historical runs, but absence alone is not a security revocation.

Resolution policy is configurable by namespace, source, and digest pin requirements. Builtins bypass custom-action namespace/source pinning for compatibility.

## Consequences

- Job authors get readable action names without giving up deterministic runs.
- API validation remains a control-plane responsibility rather than arbitrary plugin execution.
- Workers can execute custom actions without loading untrusted code into the worker process.
- Runs, retries, and replays have a stronger audit trail because execution payloads record resolved implementation descriptors.
- Action descriptor digests become part of the task materialization contract.
- Operators gain policy controls for custom namespaces, sources, and digest pinning.
- Removed actions have an explicit tombstone path, so security revocations can block historical execution while preserving audit metadata.
- Digest verification does not prove publisher identity. Signatures and attestations can layer on the descriptor/package digest later.

## Open Questions

- Should future pipeline-as-code expose separate `name`, `version`, and `digest` fields before normalizing to `uses` plus locks?
- Which custom runtime should follow local process: container, WASM, or gRPC sidecar?
- How much input-schema expressiveness is needed before pipeline-as-code lands?
- Should output schemas become part of descriptors when more workflows consume structured outputs?
- Where should signature trust policy live once digest pinning is not enough?

## References

- [Adding Actions](../actions.md)
- [CLI Guide](../../using/cli-guide.md#resolve-an-action)
- [Configuration](../../operating/configuration.md#action-registry)
- [ADR 0006: Global coordination and cell-local execution](./0006-global-coordination-cell-local-execution.md)
- `internal/action/actionregistry/` - action references, descriptors, policy, local manifests, and locks
- `internal/action/actionconfig/` - config-backed registry assembly
- `internal/action/custom/process.go` - process runtime adapter
- `internal/job/validation/validation.go` - action validation path
- `internal/job/task_plan.go` - task spec hashing for task-level execution
