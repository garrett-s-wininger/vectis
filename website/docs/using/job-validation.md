# Job Definition Validation

Vectis validates a job before it stores it or starts a run. Validation is there to catch mistakes early, before work reaches a worker.

You will usually see validation while using:

- `./bin/vectis-cli jobs create <file>`
- `./bin/vectis-cli jobs run <file>`
- `POST /api/v1/jobs`
- `PUT /api/v1/jobs/{id}`
- `POST /api/v1/jobs/run`

If you are new to the job format, we suggest you start with [Your First Job](./your-first-job.md). This page is the next stop when Vectis says a job is invalid.

## The Smallest Valid Shape

A stored job needs an `id` and a `root` node:

```json
{
  "id": "hello",
  "root": {
    "id": "say-hello",
    "uses": "builtins/shell",
    "with": {
      "command": "echo hello"
    }
  }
}
```

An ephemeral run can omit the top-level job `id` because the API creates a run ID for you:

```json
{
  "root": {
    "id": "say-hello",
    "uses": "builtins/shell",
    "with": {
      "command": "echo hello"
    }
  }
}
```

The top-level job `id` is the stored job's name. Node `id` values identify steps inside that job. They are different things, even when the names look similar.

## What Vectis Checks

Every job must have:

| Part | Rule |
| --- | --- |
| Job `id` | Required for stored jobs. Optional for ephemeral runs. |
| `root` | Required. This is the first node Vectis executes. |
| Node `id` | Required on every node and unique within the job. |
| Node `uses` | Required and must name a known action. |
| Node `with` | Must match the fields accepted by the selected action. |
| Tree size | Up to `256` nodes. |
| Tree depth | Up to `32` levels. |

Vectis reports all validation errors it can find in one response, so fixing one field may reveal another issue nearby.

## Built-In Actions

These are the built-in actions that the validator knows today:

| Action | Required `with` | Optional `with` | Notes |
| --- | --- | --- | --- |
| `builtins/shell` | `command` | none | Runs the command with `sh -c`. Empty commands and unknown keys are rejected. |
| `builtins/checkout` | `url` | none | Accepts HTTP(S) clone URLs without embedded credentials and SCP-style Git URLs. |
| `builtins/sequence` | none | any key | Runs child `steps` in order. `with` is currently ignored for compatibility. |

The action name can include the `builtins/` prefix. The built-in registry also accepts short names internally, but docs and examples use the full form so job files stay clear.

## Reading Validation Errors

API validation errors use this shape:

```json
{
  "code": "invalid_job_definition",
  "message": "invalid job definition",
  "details": {
    "fields": [
      {"field": "root.id", "message": "is required"},
      {"field": "root.uses", "message": "unknown action \"builtins/not-real\""}
    ]
  }
}
```

Each entry in `details.fields` points to a field path in the job document:

| Field path | Meaning |
| --- | --- |
| `id` | The stored job ID is missing or invalid. |
| `root` | The job has no root node. |
| `root.id` | The root node is missing its node ID. |
| `root.uses` | The root node is missing or names an unknown action. |
| `root.with.command` | The root action rejected its `command` input. |
| `root.steps[0].id` | The first child step has an ID problem. |

## Common Fixes

### Missing Job ID

This fails when you store a job:

```json
{
  "root": {
    "id": "say-hello",
    "uses": "builtins/shell",
    "with": {
      "command": "echo hello"
    }
  }
}
```

Add a top-level `id`:

```json
{
  "id": "hello",
  "root": {
    "id": "say-hello",
    "uses": "builtins/shell",
    "with": {
      "command": "echo hello"
    }
  }
}
```

If you are using `jobs run` or `POST /api/v1/jobs/run`, the top-level `id` is optional.

### Duplicate Node IDs

This fails because both steps use `id: "test"`:

```json
{
  "id": "duplicate-step",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {"id": "test", "uses": "builtins/shell", "with": {"command": "echo one"}},
      {"id": "test", "uses": "builtins/shell", "with": {"command": "echo two"}}
    ]
  }
}
```

Give each node its own ID:

```json
{
  "id": "unique-steps",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {"id": "test-one", "uses": "builtins/shell", "with": {"command": "echo one"}},
      {"id": "test-two", "uses": "builtins/shell", "with": {"command": "echo two"}}
    ]
  }
}
```

### Unknown Action

This fails because Vectis does not know `builtins/not-real`:

```json
{
  "id": "bad-action",
  "root": {
    "id": "root",
    "uses": "builtins/not-real"
  }
}
```

Use one of the supported actions, such as `builtins/shell`, `builtins/checkout`, or `builtins/sequence`.

### Invalid `with` Fields

This fails because `builtins/shell` needs `command`, not `cmd`:

```json
{
  "id": "bad-shell",
  "root": {
    "id": "root",
    "uses": "builtins/shell",
    "with": {
      "cmd": "echo hello"
    }
  }
}
```

Use the action's documented field name:

```json
{
  "id": "good-shell",
  "root": {
    "id": "root",
    "uses": "builtins/shell",
    "with": {
      "command": "echo hello"
    }
  }
}
```

### Checkout URL With Embedded Credentials

This fails because credentials are embedded in an HTTP(S) clone URL:

```json
{
  "id": "bad-checkout",
  "root": {
    "id": "checkout",
    "uses": "builtins/checkout",
    "with": {
      "url": "https://token@example.com/org/repo.git"
    }
  }
}
```

Use a URL without inline credentials:

```json
{
  "id": "good-checkout",
  "root": {
    "id": "checkout",
    "uses": "builtins/checkout",
    "with": {
      "url": "https://example.com/org/repo.git"
    }
  }
}
```

Secrets should come from a secret-aware mechanism, not from the job definition itself. Shell and checkout actions do not inherit the worker service environment; they run with a minimal Vectis-built environment so deployment secrets such as database DSNs, TLS settings, bootstrap material, and SPIRE endpoint sockets are not passed as ambient child-process variables.

## Validation Boundaries

Validation checks the job shape and action inputs. It does not prove that runtime dependencies exist.

For example, Vectis can check that `builtins/checkout` has a URL, but the worker may still fail later if the repository is unreachable. Vectis can check that `builtins/shell` has a command, but it cannot know whether that command will succeed on the worker.
