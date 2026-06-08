# Adding Actions

Actions are the executable nodes in a Vectis job tree. User-facing jobs refer to them through the node `uses` field, such as `builtins/shell`, `builtins/checkout`, or `builtins/result`.

This page is for contributors adding or changing actions in Vectis itself. If you are writing job files, start with [Your First Job](../using/your-first-job.md) and [Job Definition Validation](../using/job-validation.md).

## Where Actions Live

Built-in actions live under `internal/action/builtins/`. Each action provides:

- a stable action type, returned by `Type()`
- input validation, implemented by `ValidateWith`
- runtime behavior, implemented by `Execute`

The built-in registry resolves the `uses` value from a job node to one of these action implementations.

Actions that need to execute commands should use the worker-provided execution path instead of creating host child processes directly. The current host runner preserves existing behavior, but [ADR 0009](./architecture-decisions/0009-worker-execution-containment-providers.md) makes the runner boundary the path for future container and VM providers.

Custom user actions should not be added as new builtins unless they are meant to ship with Vectis itself. The extension point for user-owned actions is the action registry: it resolves friendly names such as `examples/greet@v1` to versioned descriptors with immutable digests before worker execution. See [ADR 0010: Versioned action registry](./architecture-decisions/0010-versioned-action-registry.md) for the architectural contract.

## Local Descriptor Registry

Vectis can resolve descriptor-only custom actions from local filesystem roots. Configure roots with `action_registry.local_roots` or the comma-separated `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS` environment variable. Builtins remain available automatically.

The local source looks for manifests at:

```text
<root>/<namespace>/<name>/action.json
<root>/<namespace>/<name>/<version>/action.json
```

A minimal manifest looks like:

```json
{
  "schema_version": 1,
  "name": "examples/greet",
  "display_name": "Greet",
  "version": "v1",
  "runtime": "process",
  "runtime_config": {
    "command": "echo \"Hello, ${VECTIS_INPUT_NAME}\""
  },
  "input_schema": {
    "fields": [
      {"name": "name", "type": "string", "required": true}
    ]
  },
  "capabilities": ["process_launch"]
}
```

The repository includes this example at `examples/actions/examples/greet/action.json`. Use `examples/actions` as the local registry root. Job authors call the action by setting its input, not by writing a shell command:

```json
{
  "id": "custom-greet-job",
  "root": {
    "id": "say-hello",
    "uses": "examples/greet@v1",
    "with": {
      "name": "Vectis"
    }
  }
}
```

The matching example job is `examples/custom-greet.json`.

The API, CLI, cron service, and reconciler use the configured descriptor resolver during validation and run-envelope creation. Workers use the configured resolver to execute local `process` actions from frozen action locks. Container, WASM, and gRPC custom runtimes are still follow-up implementation steps.

Operators can restrict custom actions by namespace, source, and digest pinning policy. For example, `VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES=examples` allows `examples/greet@v1`, while `VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS=true` requires a digest-pinned reference such as `examples/greet@sha256:<64-hex-digest>`.

Descriptors may also carry lifecycle status:

| Status | Behavior |
| --- | --- |
| `active` or omitted | Available for validation, new runs, retries, and replays. |
| `yanked` | Hidden from default discovery and rejected for version-selector references. Digest-pinned historical execution can still proceed. |
| `revoked` | Blocked for new runs and historical execution. Use for known security or correctness issues. |
| `purged` | Blocked for new runs and historical execution because the implementation is intentionally unavailable. |

When removing an action because of a vulnerability, keep a tombstone manifest with the old `name`, `version`, `digest`, `runtime`, and `status: "revoked"` or `status: "purged"`. Historical execution envelopes keep frozen action locks for auditability. If the live manifest disappears entirely, workers can still explain and may execute from the frozen descriptor; a tombstone is what deliberately blocks a vulnerable digest.

Use the CLI to inspect configured actions:

```sh
./bin/vectis-cli actions list
```

Resolve a friendly reference to discover the digest to pin:

```sh
./bin/vectis-cli actions resolve examples/greet@v1
```

If digest pins are already required, use `--ignore-policy` while preparing the pinned reference:

```sh
./bin/vectis-cli actions resolve examples/greet@v1 --ignore-policy
```

`--ignore-policy` also shows yanked, revoked, and purged tombstones so operators can inspect why an action was removed.

For `runtime: "process"`, set `runtime_config.command` to the command the worker should run. If a worker resolves the descriptor from a local manifest, relative commands run from that manifest's directory. The process receives a sanitized environment plus action metadata and inputs:

```text
VECTIS_ACTION_NAME
VECTIS_ACTION_VERSION
VECTIS_ACTION_DIGEST
VECTIS_WORKSPACE
VECTIS_INPUT_<FIELD_NAME>
```

## Validation Contract

Every built-in action should validate its `with` map before execution. Good validation catches user mistakes while the job is being created or submitted, instead of waiting for a worker to fail later.

When adding a built-in action:

- implement `ValidateWith`
- reject unknown `with` keys unless the action intentionally accepts an open map
- validate required fields before execution
- validate field shape and unsafe values, not just presence
- avoid accepting plaintext secrets in `with`
- add unit tests for valid input, missing fields, invalid values, and unknown keys
- add an API-path test when validation behavior changes through job create, update, or ephemeral run

Keep validation messages direct. They appear in `details.fields` on API errors and should help users fix the job document without reading the source.

## Field Paths

The job validator adds the action's field errors under the node path. For example, if `builtins/shell` rejects a missing `command` on the root node, the API returns a field path like:

```text
root.with.command
```

If the same error happens in the first child step, the path includes the step index:

```text
root.steps[0].with.command
```

Use action field names that make these paths obvious to users.

## Pipeline-As-Code

Pipeline-as-code should eventually layer more validation before storage:

1. Parse the user-facing pipeline file.
2. Normalize it into the canonical job graph.
3. Validate dependencies, conditionals, matrix expansion, and concurrency declarations.
4. Validate policy-sensitive declarations such as secrets, cache, artifacts, and environments.
5. Run the same action-input validation described here.
6. Store only the normalized, validated representation.

The current JSON/proto job validator should remain reusable as the action-input validation layer after pipeline syntax lands.

Process-launching actions must use the `ExecutionState` command environment rather than `os.Environ()`. The default child environment is intentionally small so worker deployment secrets and future SPIRE Workload API sockets are not inherited by arbitrary job commands.
