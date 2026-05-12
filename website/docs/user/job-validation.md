# Job Definition Validation

Vectis validates job definitions before storing them and before starting ephemeral runs. The same validator is used by:

- `POST /api/v1/jobs`
- `PUT /api/v1/jobs/{id}`
- `POST /api/v1/jobs/run`

## Current Contract

Stored jobs must provide `id`; ephemeral runs may omit `id` because the API generates one. Every job must provide `root`.

Tree validation checks:

| Rule | Default |
| --- | --- |
| Maximum node count | `256` |
| Maximum depth | `32` |
| Node IDs | Required and unique within the job |
| Node `uses` | Required and resolvable through the action registry |
| Action `with` fields | Validated by the resolved action |

The current built-ins validate:

| Action | Required `with` | Optional `with` | Notes |
| --- | --- | --- | --- |
| `builtins/shell` | `command` | none | Empty commands and unknown keys are rejected. |
| `builtins/checkout` | `url` | none | Accepts HTTP(S) URLs without embedded credentials and SCP-style Git URLs. Unknown keys are rejected. |
| `builtins/sequence` | none | any key | Sequence ignores `with` today; this is intentional compatibility until pipeline syntax gives sequence nodes first-class fields. |

## API Error Shape

Invalid jobs return:

```json
{
  "code": "invalid_job_definition",
  "message": "invalid job definition",
  "details": {
    "error": "root.id: is required; root.uses: unknown action \"builtins/not-real\"",
    "fields": [
      {"field": "root.id", "message": "is required"},
      {"field": "root.uses", "message": "unknown action \"builtins/not-real\""}
    ]
  }
}
```

`details.error` is retained for v1 compatibility. New clients should prefer `details.fields` so they do not need to parse human-readable text.

## New Built-In Action Checklist

Every new built-in action must:

- Implement `ValidateWith`.
- Reject unknown `with` keys unless the action explicitly documents an open-ended map.
- Validate required fields before execution.
- Avoid accepting plaintext secrets in `with`.
- Add unit tests for valid input, missing required fields, invalid field values, and unknown keys.
- Add an API-path test when the action changes validation behavior visible through job create, update, or ephemeral run.

## Pipeline-As-Code Validation Phases

Pipeline-as-code should layer additional validation before storage:

1. Parse the user-facing pipeline document.
2. Normalize it into the canonical job graph.
3. Validate static graph references, dependencies, conditionals, matrix expansion, and concurrency declarations.
4. Validate action `with` fields through the action registry.
5. Validate policy-sensitive declarations such as secrets, cache, artifacts, and environments.
6. Store only the normalized, validated representation.

The current JSON/proto job validator is phase 4 plus bounded tree-shape checks. It should remain reusable after pipeline syntax lands.
