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

| Action | Required `with` | Ports | Notes |
| --- | --- | --- | --- |
| `builtins/shell` | `command` | none | Runs the command with `sh -c`. Empty commands and unknown action keys are rejected. |
| `builtins/test` | `command` | none | Runs the command as a predicate. Exit `0` returns `outputs.result=true`, exit `1` returns `outputs.result=false`, and other execution errors fail the action. |
| `builtins/checkout` | `url` | none | Accepts HTTP(S) clone URLs without embedded credentials and SCP-style Git URLs. Unknown action keys are rejected. |
| `builtins/sequence` | none | `steps` | Runs child nodes in order. Defaults to `execution: "local"`, so children run in the same worker workspace unless a distributed boundary is reached. Unknown optional `with` keys are tolerated for compatibility. |
| `builtins/parallel` | none | `branches` | Runs branch nodes concurrently when local, or fans them out as task executions when distributed. Defaults to `execution: "distributed"`. Unknown optional `with` keys are tolerated for compatibility. |
| `builtins/if` | none | `condition`, `then`, `else` | Runs exactly one `condition` node, reads its `outputs.result` boolean, then runs the ordered `then` or `else` port. Condition execution failures fail the `if`. Local-only until durable skipped-branch semantics exist. |
| `builtins/retry` | optional `attempts` | `body` | Runs the ordered `body` port until it succeeds or attempts are exhausted. Defaults to `attempts: "3"`. Local-only until durable retry attempts are modeled. |
| `builtins/timeout` | `duration` | `body` | Runs the ordered `body` port with a deadline such as `30s`, `5m`, or `1h`. Local-only until durable timeout recovery is modeled. |
| `builtins/finally` | none | `body`, `always` | Runs `body`, then always runs `always`. Body failure remains the final failure unless cleanup is the only failure. Local-only until durable finalizer semantics are modeled. |

The action name can include the `builtins/` prefix. The built-in registry also accepts short names internally, but docs and examples use the full form so job files stay clear.

Child nodes can be attached through typed ports:

```json
{
  "id": "checks",
  "uses": "builtins/parallel",
  "ports": {
    "branches": {
      "nodes": [
        {"id": "unit", "uses": "builtins/shell", "with": {"command": "go test ./..."}},
        {"id": "lint", "uses": "builtins/shell", "with": {"command": "go vet ./..."}}
      ]
    }
  }
}
```

The legacy `steps` field remains shorthand for a node's primary port. For `builtins/sequence`, `steps` means `ports.steps.nodes`; for `builtins/parallel`, `steps` means `ports.branches.nodes`. Do not set both `steps` and the matching primary port on the same node.

Conditionals are modeled with nodes instead of an expression language:

```json
{
  "id": "deploy-gate",
  "uses": "builtins/if",
  "ports": {
    "condition": {
      "nodes": [
        {"id": "has-changes", "uses": "builtins/test", "with": {"command": "test -f deploy.changed"}}
      ]
    },
    "then": {
      "nodes": [
        {"id": "deploy", "uses": "builtins/shell", "with": {"command": "make deploy"}}
      ]
    },
    "else": {
      "nodes": [
        {"id": "skip-note", "uses": "builtins/shell", "with": {"command": "echo no deploy"}}
      ]
    }
  }
}
```

Execution policy nodes wrap local child subtrees:

```json
{
  "id": "retry-build",
  "uses": "builtins/retry",
  "with": {"attempts": "3"},
  "ports": {
    "body": {
      "nodes": [
        {
          "id": "timed-build",
          "uses": "builtins/timeout",
          "with": {"duration": "5m"},
          "ports": {
            "body": {
              "nodes": [
                {"id": "build", "uses": "builtins/shell", "with": {"command": "make build"}}
              ]
            }
          }
        }
      ]
    }
  }
}
```

`with.execution` is scheduling metadata accepted on any node and is not passed to the action implementation. It controls whether the node runs inside the current task or is materialized as a task boundary:

| Value | Meaning |
| --- | --- |
| `local` | The node is eligible to run inside the current task execution and workspace. |
| `distributed` | The node is inserted as a task execution and may run on another worker. It should not depend on a shared mutable workspace. |

When omitted, `builtins/parallel` defaults to `distributed`; other built-ins default to `local`.

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
| `root.ports.branches.nodes[0].id` | The first node attached to the `branches` port has an ID problem. |

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

Use one of the supported actions, such as `builtins/shell`, `builtins/checkout`, `builtins/sequence`, or `builtins/parallel`.

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
