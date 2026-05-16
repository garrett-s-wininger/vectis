# Adding Actions

Actions are the executable nodes in a Vectis job tree. User-facing jobs refer to them through the node `uses` field, such as `builtins/shell` or `builtins/checkout`.

This page is for contributors adding or changing actions in Vectis itself. If you are writing job files, start with [Your First Job](../using/your-first-job.md) and [Job Definition Validation](../using/job-validation.md).

## Where Actions Live

Built-in actions live under `internal/action/builtins/`. Each action provides:

- a stable action type, returned by `Type()`
- input validation, implemented by `ValidateWith`
- runtime behavior, implemented by `Execute`

The built-in registry resolves the `uses` value from a job node to one of these action implementations.

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
