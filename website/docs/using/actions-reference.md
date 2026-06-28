# Actions Reference

Actions are the executable nodes in a Vectis job tree. Job nodes select an action with `uses`, pass static values through `with`, optionally bind values through `inputs`, and connect child nodes through ports when the action is a control-flow node.

For the job JSON field layout, see [Job Definition Reference](./job-definition-reference.md). For custom action authoring, see [Adding Actions](../developing/actions.md).

## References

Action references use this shape:

```text
<namespace>/<name>[@<selector>]
```

Examples:

```text
builtins/shell
builtins/shell@v1
examples/greet@sha256:<64-hex-digest>
```

| Part | Contract |
| --- | --- |
| `namespace` | Lowercase namespace segment. Built-ins use `builtins`. |
| `name` | Lowercase action name segment. |
| `selector` | Optional version selector such as `v1`, or digest selector such as `sha256:<64-hex-digest>`. |
| No selector | Resolves according to the configured action registry. Built-in examples prefer unselected canonical names. |

Short built-in names such as `shell` can resolve internally, but job files should use canonical names such as `builtins/shell` so stored definitions stay clear.

## Built-In Actions

| Action | Required `with` fields | Optional `with` fields | Ports | Local-only | Behavior |
| --- | --- | --- | --- | --- | --- |
| `builtins/shell` | `command` | `outputs` | None | No | Runs `sh -c` in the workspace. If `outputs` is set, reads that workspace-relative JSON object file after success and returns its keys as node outputs. |
| `builtins/test` | `command` | None | None | No | Runs a predicate command. Exit `0` returns `result: true`; exit `1` returns `result: false`; other execution errors fail the action. |
| `builtins/checkout` | `url` | None | None | No | Runs `git clone <url> .` with terminal prompts disabled. HTTP(S) URLs with embedded credentials are rejected. |
| `builtins/upload-artifact` | `name`, `path` | `content_type`, `metadata_json`, `max_bytes` | None | No | Publishes a workspace-relative file as a run artifact and returns an `artifact` object. |
| `builtins/sequence` | None | `execution` | `steps` | No | Runs child nodes in order and stops on first failure. |
| `builtins/parallel` | None | `execution` | `branches` | No | Runs branches concurrently when local, or fans out distributed child task executions by default. |
| `builtins/if` | None | `execution` | `condition`, `then`, `else` | Yes | Runs one condition node, reads its boolean `result`, then runs the matching branch. |
| `builtins/retry` | None | `attempts` | `body` | Yes | Runs the body until success or attempts are exhausted. Default attempts is `3`. |
| `builtins/timeout` | `duration` | None | `body` | Yes | Runs the body with a positive Go duration such as `30s`, `5m`, or `1h`. |
| `builtins/finally` | None | `execution` | `body`, `always` | Yes | Runs `always` after `body`; body failure remains final unless cleanup is the only failure. |
| `builtins/fallback` | None | `execution` | `choices` | Yes | Runs choices in order and returns the first success. |
| `builtins/result` | `success` | None | None | No | Returns success when `success` parses as `true`, otherwise failure. |

`execution` is reserved for Vectis execution policy. When omitted, `builtins/parallel` defaults to distributed execution and other built-ins default to local execution.

### Timeout Composition

`builtins/timeout` is an explicit local wrapper, not a job-level default. It cancels its `body` when the duration expires; process-launching children receive the normal worker cancellation behavior, including Unix process-group termination on supported hosts.

Timeout bodies must stay within one local execution scope. A distributed boundary inside the body is rejected during validation. Because `builtins/parallel` defaults to distributed execution, set `with.execution: local` when using parallel branches inside a timeout, or place separate timeout nodes inside each distributed branch.

Ordering matters when combining timeout and retry:

| Shape | Meaning |
| --- | --- |
| `timeout { retry { shell } }` | One total deadline for all retry attempts. |
| `retry { timeout { shell } }` | Each retry attempt gets its own deadline. |

Use the first shape for a total task budget, and the second when each attempt may legitimately take the full duration.

## Ports

| Port | Action | Cardinality | Ordering |
| --- | --- | --- | --- |
| `steps` | `builtins/sequence` | Any number | Ordered |
| `branches` | `builtins/parallel` | Any number | Concurrent when local, task fan-out when distributed |
| `condition` | `builtins/if` | Exactly one | Ordered |
| `then` | `builtins/if` | Any number | Ordered |
| `else` | `builtins/if` | Any number | Ordered |
| `body` | `builtins/retry`, `builtins/timeout`, `builtins/finally` | At least one | Ordered |
| `always` | `builtins/finally` | At least one | Ordered |
| `choices` | `builtins/fallback` | At least one | Ordered |

`steps` is shorthand for a node's primary port. For `builtins/sequence`, `steps` means `ports.steps.nodes`. For `builtins/parallel`, `steps` means `ports.branches.nodes`. Do not set `steps` and the matching explicit primary port on the same node.

## Descriptor Contract

The action registry resolves `uses` into a descriptor. Descriptor JSON includes:

| Field | Meaning |
| --- | --- |
| `canonical_name` | Stable action name, such as `builtins/shell` or `examples/greet`. |
| `display_name` | Optional human-readable name. |
| `version` | Compatibility selector value, such as `v1`. |
| `digest` | Immutable descriptor digest, currently `sha256:<64-hex-digest>`. |
| `source` | Descriptor source: `builtin`, `local_filesystem`, or reserved `oci`. |
| `runtime` | Runtime kind: `builtin`, `process`, `container`, `wasm`, or `grpc`. |
| `runtime_config` | Runtime-specific key/value configuration. |
| `input_schema` | Static `with` field schema. |
| `port_schema` | Accepted child ports and cardinality. |
| `local_only` | `true` when the action cannot be a distributed execution boundary. |
| `capabilities` | Declared capabilities such as `process_launch` or `network`. |
| `status` | Lifecycle status. Empty status means `active`. |
| `status_reason` | Optional reason for yanked, revoked, or purged descriptors. |

Descriptor digests are computed from stable descriptor fields. The digest is part of task materialization, so retries and repair can detect action implementation drift.

## Capabilities

Descriptor capabilities are operator-visible metadata. Current capability values are:

| Capability | Meaning |
| --- | --- |
| `process_launch` | Action starts local or delegated processes. |
| `network` | Action may use network access. |
| `workspace_read` | Action reads files from the workspace. |
| `workspace_write` | Action writes files in the workspace. |
| `secrets` | Action may intentionally consume Vectis-provided job secrets. |

Current built-in descriptors report capabilities for process-oriented actions:

| Built-in | Capabilities |
| --- | --- |
| `builtins/shell` | `process_launch`, `workspace_read`, `workspace_write` |
| `builtins/test` | `process_launch`, `workspace_read`, `workspace_write` |
| `builtins/checkout` | `network`, `process_launch`, `workspace_write` |

Other built-ins currently report no explicit capabilities.

## Lifecycle Status

| Status | Resolution behavior |
| --- | --- |
| `active` | Listed and usable. Empty status is normalized to `active`. |
| `yanked` | Hidden from normal listing and rejected for version-selector or unselected references; digest-pinned historical execution can proceed. |
| `revoked` | Blocks new resolution and frozen descriptor execution. |
| `purged` | Blocks new resolution and frozen descriptor execution. |

When removing an action for safety, keep a tombstone descriptor with the original name, version, digest, runtime, and status. Absence alone is not a security revocation for historical execution payloads.

## Custom Local Actions

Vectis can resolve descriptor-only custom actions from local filesystem roots configured with `action_registry.local_roots` or `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS`.

Each local action manifest is named `action.json` and uses schema version `1`:

```json
{
  "schema_version": 1,
  "name": "examples/greet",
  "display_name": "Greet",
  "version": "v1",
  "runtime": "process",
  "runtime_config": {
    "command": "./greet"
  },
  "input_schema": {
    "fields": [
      {"name": "name", "type": "string", "required": true}
    ]
  },
  "capabilities": ["process_launch", "workspace_read"]
}
```

Manifest rules:

- `name` must match the requested `namespace/name`;
- `version` is required and must be a valid selector that is not `sha256:...`;
- `digest` is optional and must be `sha256:<64-hex-digest>` when supplied;
- local runtime may be `process`, `container`, `wasm`, or `grpc`;
- local runtime `builtin` is reserved;
- local `process` actions do not support `port_schema`;
- input field name `execution` is reserved;
- input field types currently support `string`, `url`, and `number`;
- unknown manifest fields are rejected.

The configured resolver loads built-ins automatically and then adds local manifest roots.

## Policy

Operators can restrict custom action use:

| Key | Environment | Meaning |
| --- | --- | --- |
| `action_registry.local_roots` | `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS` | Local manifest roots to load. |
| `action_registry.allowed_namespaces` | `VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES` | Optional namespace allowlist for custom actions. |
| `action_registry.allowed_sources` | `VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES` | Optional source allowlist, such as `local_filesystem`. |
| `action_registry.require_digest_pins` | `VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS` | Requires custom action references to use digest selectors. |

Built-ins bypass custom-action namespace, source, and digest-pin policy for compatibility. Custom actions must satisfy policy unless a CLI discovery command uses `--ignore-policy`.

## CLI Discovery

Use the CLI to inspect descriptors and find digest-pinned references. The subcommands are `actions list` and `actions resolve`:

```sh
vectis-cli actions list
vectis-cli actions resolve examples/greet@v1
vectis-cli actions resolve examples/greet@v1 --ignore-policy
```

`actions resolve` returns `reference`, `resolved_reference`, and `descriptor` in JSON mode. `resolved_reference` is the canonical digest-pinned reference to use when preparing a pinned job definition.

Use `--ignore-policy` only for discovery, for example when `action_registry.require_digest_pins=true` and you need to resolve a version selector to find its digest.

## Related Documentation

| Need | Document |
| --- | --- |
| Job JSON field contract | [Job Definition Reference](./job-definition-reference.md) |
| Built-in artifact action | [Artifacts](./artifacts.md) |
| Job secrets | [Secrets Reference](./secrets-reference.md) |
| Custom action development | [Adding Actions](../developing/actions.md) |
| Action registry ADR | [ADR 0010: Versioned Action Registry](../developing/architecture-decisions/0010-versioned-action-registry.md) |
| Config keys | [Configuration Key Reference](../operating/reference/configuration-key-reference.md#secrets-and-action-registry) |
