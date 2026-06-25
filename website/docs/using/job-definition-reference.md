# Job Definition Reference

This page is the compact field reference for the JSON job documents submitted with `vectis-cli jobs run`, `vectis-cli jobs create`, and the v1 jobs API. For a walkthrough, start with [Your First Job](./your-first-job.md). For action descriptor, selector, and digest-pin details, see [Actions Reference](./actions-reference.md). For examples of validation failures and fixes, see [Job Definition Validation](./job-validation.md).

## JSON Shape

Job documents use the protobuf JSON field names shown here:

```json
{
  "id": "example-job",
  "default_isolation": "host",
  "secrets": [
    {
      "id": "github_token",
      "ref": "spiffe://example.local/secret/github-token",
      "delivery": {
        "type": "file",
        "path": "github/token"
      },
      "task_keys": ["checkout"]
    }
  ],
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "with": {
      "execution": "local"
    },
    "steps": [
      {
        "id": "hello",
        "uses": "builtins/script",
        "with": {
          "script": "echo hello"
        }
      }
    ],
    "ports": {
      "steps": {
        "nodes": []
      }
    },
    "inputs": {
      "command": {
        "from": {
          "node": "script-command",
          "output": "command"
        }
      }
    },
    "isolation": "host"
  }
}
```

Most jobs use either `steps` or `ports`, not both. The `inputs` example above is shown for field shape; it would normally be attached to a node that accepts a `command` input, such as `builtins/test`.

## Top-Level Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | Stored jobs only | Stable job identifier used by `jobs create`, `jobs trigger`, and stored job history. One-off runs can omit it. |
| `root` | Yes | First node in the job tree. |
| `default_isolation` | No | Default execution boundary for nodes that do not set `isolation`. Supported values are `host` and `vm`. |
| `secrets` | No | Secret references that the cell-local secrets broker can materialize for selected task keys. |
| `run_id` | System field | Run identifier attached to persisted or in-flight job payloads. Users normally do not set it. |
| `delivery_id` | System field | Queue delivery identifier attached during dispatch. Users normally do not set it. |

## Node Fields

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | Yes | Unique node ID within the job. The ID is used by logs, task rows, input bindings, and optional secret `task_keys`. |
| `uses` | Yes | Action reference. Use canonical names such as `builtins/script`; short built-in names are accepted internally but are less clear in job files. |
| `with` | Action-specific | Static string inputs for the selected action. Unknown keys are rejected by strict actions. |
| `inputs` | No | Bound inputs from earlier node outputs, shaped as `inputs.<field>.from.node` and `inputs.<field>.from.output`. |
| `steps` | No | Shorthand child list for the node's primary port. |
| `ports` | No | Explicit typed child ports. Each port has a `nodes` array. |
| `isolation` | No | Node-level override for `default_isolation`. Supported values are `host` and `vm`. |

Action references can include selectors when the resolver supports them, such as `builtins/script@v1` or `builtins/script@sha256:<digest>`. Built-in examples use unselected canonical names.

## Static Inputs

`with` is a string map. Even numeric and boolean-looking values are written as strings:

```json
{
  "id": "retry-build",
  "uses": "builtins/retry",
  "with": {
    "attempts": "3"
  }
}
```

`with.execution` is special scheduling metadata accepted on any node. It is not passed to the action implementation.

| Value | Meaning |
| --- | --- |
| `local` | Run the node inside the current task execution and workspace when possible. |
| `distributed` | Materialize the node as a task boundary that may run on another worker. |

When omitted, `builtins/parallel` defaults to `distributed`; other built-ins default to `local`. Local-only policy nodes reject `execution: "distributed"` and child subtrees that contain a distributed boundary.

## Bound Inputs

`inputs` binds an action input field to an output from an earlier node:

```json
{
  "id": "gate",
  "uses": "builtins/test",
  "inputs": {
    "command": {
      "from": {
        "node": "script-command",
        "output": "command"
      }
    }
  }
}
```

Bound input names must be accepted by the selected action. A node cannot set the same field in both `with` and `inputs`. The referenced `from.node` must be an earlier node in the same local execution scope, and `from.output` must name an output produced by that earlier node.

## Ports And Steps

`ports` is the explicit form for child nodes:

```json
{
  "id": "checks",
  "uses": "builtins/parallel",
  "ports": {
    "branches": {
      "nodes": [
        {"id": "unit", "uses": "builtins/script", "with": {"script": "go test ./..."}},
        {"id": "lint", "uses": "builtins/script", "with": {"script": "go vet ./..."}}
      ]
    }
  }
}
```

`steps` is shorthand for a node's primary port. For `builtins/sequence`, `steps` means `ports.steps.nodes`. For `builtins/parallel`, `steps` means `ports.branches.nodes`. Do not set `steps` and the matching explicit primary port on the same node.

| Port | Used by | Cardinality | Order |
| --- | --- | --- | --- |
| `steps` | `builtins/sequence` | Any number | Ordered |
| `branches` | `builtins/parallel` | Any number | Concurrent when local, task fan-out when distributed |
| `condition` | `builtins/if` | Exactly one | Ordered |
| `then` | `builtins/if` | Any number | Ordered |
| `else` | `builtins/if` | Any number | Ordered |
| `body` | `builtins/retry`, `builtins/timeout`, `builtins/finally` | At least one | Ordered |
| `always` | `builtins/finally` | At least one | Ordered |
| `choices` | `builtins/fallback` | At least one | Ordered |

## Secrets

Top-level `secrets` describe references that the cell-local secrets broker can resolve. Job definitions carry references and delivery instructions, not the secret values themselves. See [Secrets Reference](./secrets-reference.md) for provider, policy, materialization, and troubleshooting details.

| Field | Required | Meaning |
| --- | --- | --- |
| `id` | Yes | Secret identifier. It must start with a letter or underscore and then contain only letters, numbers, underscores, dots, or dashes. |
| `ref` | Yes | Provider URI with a scheme, such as a SPIFFE-backed secret URI. Embedded credentials are rejected. |
| `delivery` | Yes | Delivery method. Vectis currently supports file delivery. |
| `delivery.type` | Yes | Use `file` in JSON. The protobuf enum name is also accepted, but examples prefer the short alias. |
| `delivery.path` | Yes for file delivery | Relative slash-separated path below the worker secrets directory. Absolute paths, backslashes, empty path segments, `.`, and `..` are rejected. |
| `task_keys` | No | Optional list of node IDs allowed to receive the secret. When present, every entry must match a node `id`. |

Workers materialize file-delivered secrets below the configured secrets directory for the task. Actions should read those files explicitly instead of expecting deployment secrets to appear as ambient environment variables.

## Built-In Actions

| Action | Static `with` fields | Bound `inputs` | Ports | Local-only | Outputs and behavior |
| --- | --- | --- | --- | --- | --- |
| `builtins/script` | Required `script`; optional `runner`, `outputs` | `script`, `runner`, `outputs` | None | No | Writes the script to a temporary workspace file and runs it with the selected runner. Omitted or `auto` runners use PowerShell on Windows and `sh` elsewhere. |
| `builtins/test` | Required `command`; optional `runner` | `command`, `runner` | None | No | Runs a predicate command with the selected runner. Omitted or `auto` runners use PowerShell on Windows and `sh` elsewhere. Exit `0` returns `result: true`; exit `1` returns `result: false`; other execution errors fail the action. |
| `builtins/checkout` | Required `url`; optional `fetch_refspecs`, `ref` | `url`, `fetch_refspecs`, `ref` | None | No | Runs `git clone <url> .` with terminal prompts disabled. HTTP(S) URLs with embedded credentials are rejected; SCP-style Git URLs are accepted. When worker persistent cache handles the URL, `origin` stays on the declared remote and `vectis-cache` exposes the local mirror; `fetch_refspecs` demand-hydrates source refs into the cache when needed, then fetches whitespace-separated refspecs locally on cache hits or from `origin` after direct clones. `ref` fetches a single ref from the active remote and checks out `FETCH_HEAD` detached. |
| `builtins/gerrit-review` | Required `url`, `change`, `message`, `username`, `password_file`; optional `revision`, `label`, `value`, `tag` | `url`, `change`, `revision`, `message`, `label`, `value`, `tag`, `username`, `password_file` | None | No | Posts a Gerrit review message and optional label vote using HTTP basic auth from a workspace-relative password file. `revision` defaults to `current`; `value` is required when `label` is set. |
| `builtins/upload-artifact` | Required `name`, `path`; optional `content_type`, `metadata_json`, `max_bytes` | None | None | No | Publishes a workspace-relative file as a run artifact and returns an `artifact` object with blob and size metadata. See [Artifacts](./artifacts.md). |
| `builtins/sequence` | Optional `execution` | None | Primary `steps` | No | Runs child nodes in order and stops on the first failure. Returns the last child outputs on success. |
| `builtins/parallel` | Optional `execution` | None | Primary `branches` | No | Runs branches concurrently when local, or fans them out as distributed task executions by default. |
| `builtins/if` | Optional `execution` | None | Required `condition`; optional `then`, `else` | Yes | Runs one condition node, reads its `result` boolean, then runs the matching branch. |
| `builtins/retry` | Optional `attempts` | `attempts` | Required `body` | Yes | Runs the body until it succeeds or attempts are exhausted. The default is `3`. |
| `builtins/timeout` | Required `duration` | `duration` | Required `body` | Yes | Runs the body with a positive Go duration such as `30s`, `5m`, or `1h`. |
| `builtins/finally` | Optional `execution` | None | Required `body`, required `always` | Yes | Runs `always` after `body`. Body failure remains the final failure unless cleanup is the only failure. |
| `builtins/fallback` | Optional `execution` | None | Required `choices` | Yes | Runs choices in order and returns the first success. If all fail, the final failure is returned. |
| `builtins/result` | Required `success` | None | None | No | Returns success when `success` parses as `true` and failure when it parses as `false`. |

## Limits

Validation enforces the current structural limits before a job is stored or run:

| Limit | Value |
| --- | --- |
| Maximum nodes | `256` |
| Maximum tree depth | `32` |
| Supported isolation values | `host`, `vm` |
| Supported secret delivery types | `file` |
| Supported execution values | `local`, `distributed` |

For the validation error envelope, common fixes, and examples of invalid documents, see [Job Definition Validation](./job-validation.md).
