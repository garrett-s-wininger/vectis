# Your First Job

This guide explains how to write the JSON job definitions that Vectis runs today. It is for people who have started a local Vectis stack and want to understand what they are submitting with `vectis-cli jobs run` or `vectis-cli jobs create`.

If you have not run Vectis locally yet, start with [Getting Started](../getting-started.md).

## The Smallest Useful Job

A Vectis job has a `root` node. A node says which action to run with `uses`, and passes action-specific settings in `with`.

```json
{
  "id": "hello-job",
  "root": {
    "id": "say-hello",
    "uses": "builtins/shell",
    "with": {
      "command": "echo 'Hello from Vectis'"
    }
  }
}
```

Save that as `hello.json`, then run it once:

```sh
./bin/vectis-cli jobs run hello.json --follow
```

`--follow` streams logs for the run that was just created.

## Job IDs And Node IDs

There are two IDs to keep straight:

| Field | What it identifies | When you need it |
| --- | --- | --- |
| `id` at the top level | The job definition | Required when storing a reusable job with `jobs create`. |
| `id` inside a node | One step in the job tree | Required for each node, and must be unique within the job. |

For one-off runs, `jobs run` can accept a job without a top-level `id`; the API can generate one. For stored jobs, include the top-level `id` so you can trigger it later.

## One-Off Run Or Stored Job?

Use a one-off run when you are experimenting:

```sh
./bin/vectis-cli jobs run hello.json --follow
```

Use a stored job when you want to trigger the same definition repeatedly:

```sh
./bin/vectis-cli jobs create hello.json
./bin/vectis-cli jobs trigger hello-job --follow
```

You can list stored jobs with:

```sh
./bin/vectis-cli jobs list
```

## Multiple Steps

Use `builtins/sequence` when you want child steps to run in order. Each child is another node.

```json
{
  "id": "multi-step-job",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "hello",
        "uses": "builtins/shell",
        "with": {
          "command": "echo 'Hello from step one'"
        }
      },
      {
        "id": "where-am-i",
        "uses": "builtins/shell",
        "with": {
          "command": "pwd && ls -la"
        }
      }
    ]
  }
}
```

If one step fails, the sequence stops and the run fails.

The repository includes a working version of this pattern at `examples/sequenced.json`.

`steps` is shorthand for the `builtins/sequence` primary port. The equivalent explicit form is `ports.steps.nodes`, which is friendlier for generated jobs and visual node editors:

```json
{
  "id": "multi-step-job",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "ports": {
      "steps": {
        "nodes": [
          {"id": "hello", "uses": "builtins/shell", "with": {"command": "echo hello"}},
          {"id": "where-am-i", "uses": "builtins/shell", "with": {"command": "pwd"}}
        ]
      }
    }
  }
}
```

## Passing Outputs Between Nodes

A shell step can publish structured outputs by writing a JSON object to a workspace-relative file and setting `with.outputs`.

Later nodes can bind accepted inputs from earlier outputs in the same local execution scope with `inputs`:

```json
{
  "id": "dataflow-job",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "make-command",
        "uses": "builtins/shell",
        "with": {
          "command": "printf '{\"command\":\"true\"}' > outputs.json",
          "outputs": "outputs.json"
        }
      },
      {
        "id": "gate",
        "uses": "builtins/test",
        "inputs": {
          "command": {
            "from": {
              "node": "make-command",
              "output": "command"
            }
          }
        }
      }
    ]
  }
}
```

Use either `with.command` or `inputs.command` on a node, not both.

## Optional Action Isolation

Each job can set a default command boundary with `default_isolation`, and each node can override it with `isolation`. The supported values are `host` and `vm`:

```json
{
  "id": "vm-test-job",
  "default_isolation": "vm",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "test",
        "uses": "builtins/shell",
        "with": {
          "command": "go test ./..."
        }
      },
      {
        "id": "host-summary",
        "uses": "builtins/shell",
        "isolation": "host",
        "with": {
          "command": "echo 'summary step on host'"
        }
      }
    ]
  }
}
```

If `isolation` is omitted, the node inherits the nearest parent sequence's isolation, then the job default, then the worker backend default. A worker must have a matching backend configured for `vm`; otherwise the run fails instead of falling back to host execution.

## Checkout Then Build

Use `builtins/checkout` to clone a Git repository into the run workspace. Shell steps after checkout run from that workspace.

```json
{
  "id": "checkout-and-test",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "checkout",
        "uses": "builtins/checkout",
        "with": {
          "url": "https://github.com/example/project.git"
        }
      },
      {
        "id": "test",
        "uses": "builtins/shell",
        "with": {
          "command": "go test ./..."
        }
      }
    ]
  }
}
```

Do not put credentials in HTTP(S) checkout URLs. Vectis rejects URLs like `https://user:token@example.com/org/repo.git` because they can leak through logs, process lists, or persisted job definitions.

Credential-free SSH-style URLs such as `git@github.com:org/repo.git` are accepted, but the worker host still needs the right SSH configuration outside Vectis.

## Built-In Actions

These are the built-ins available today:

| Action | Required `with` fields | What it does |
| --- | --- | --- |
| `builtins/shell` | `command` | Runs `sh -c <command>` in the run workspace. Optional `outputs` reads a workspace-relative JSON output file after success. |
| `builtins/test` | `command` | Runs a predicate command and returns a boolean `result` output. |
| `builtins/checkout` | `url` | Runs `git clone <url> .` in the run workspace. |
| `builtins/sequence` | none | Runs child `steps` in order. |
| `builtins/parallel` | none | Runs child `branches` concurrently or fans them out across workers. |
| `builtins/if` | none | Runs a `condition` node, then runs either the `then` or `else` port. |
| `builtins/retry` | optional `attempts` | Retries a local `body` port until it succeeds or attempts are exhausted. |
| `builtins/timeout` | `duration` | Runs a local `body` port with a deadline such as `30s` or `5m`. |
| `builtins/finally` | none | Runs `body`, then always runs cleanup nodes from the `always` port. |
| `builtins/fallback` | none | Runs local `choices` in order and returns the first success. |
| `builtins/upload-artifact` | `name`, `path` | Publishes a workspace-relative file as a run artifact. Optional `max_bytes` can lower the worker upload cap for this node. |

Actions are intentionally small right now. Pipeline-as-code and richer action syntax are future work; today, JSON is the source format.

Artifacts are produced with explicit `builtins/upload-artifact` steps. Vectis does not automatically collect files from the workspace or read a top-level artifact declaration yet. Uploaded bytes are stored and downloaded unchanged, so create archives yourself when you want multiple files in one artifact.

## Common Validation Errors

Vectis validates jobs before storing them or starting a one-off run.

| Error | How to fix it |
| --- | --- |
| `root` is missing | Add a `root` object to the job. |
| Node `id` is missing | Add an `id` to every node, including child steps. |
| Duplicate node ID | Rename one of the duplicate node IDs. |
| Unknown action | Check the `uses` value and any configured action registry roots. Builtins work without configuration; local custom actions require `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS`. |
| Unsupported `isolation` | Use `host` or `vm`. |
| Missing `command` for `builtins/shell` | Add `with.command`. |
| Missing `url` for `builtins/checkout` | Add `with.url`. |
| Unknown key in `with` | Remove fields the selected action does not understand. |

For the full validation contract, including limits and API error shape, see [Job Definition Validation](./job-validation.md).

## A Good First Workflow

When you are learning or debugging, this loop is usually enough:

1. Edit a local JSON file.
2. Run it once with `./bin/vectis-cli jobs run <file> --follow`.
3. If it works and you want to reuse it, store it with `./bin/vectis-cli jobs create <file>`.
4. Trigger stored runs with `./bin/vectis-cli jobs trigger <job-id> --follow`.
5. Inspect history with `./bin/vectis-cli runs list <job-id>`.

This keeps experimentation cheap while still giving you a path to a reusable job.
