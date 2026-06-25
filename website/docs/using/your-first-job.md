# Your First Job

This guide explains how to write the JSON job definitions that Vectis runs today. It is for people who have started a local Vectis stack and want to understand what they are submitting with `vectis-cli jobs run` or repository-backed `vectis-cli jobs create --repository`.

If you have not run Vectis locally yet, start with [Getting Started](../getting-started.md). If you want the compact field-by-field version instead of a walkthrough, keep [Job Definition Reference](./job-definition-reference.md) open beside this guide.

## The Smallest Useful Job

A Vectis job has a `root` node. A node says which action to run with `uses`, and passes action-specific settings in `with`.

```json
{
  "id": "hello-job",
  "root": {
    "id": "say-hello",
    "uses": "builtins/script",
    "with": {
      "script": "echo 'Hello from Vectis'"
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
| `id` at the top level | The reusable job definition | Required when creating a reusable source-backed job unless you pass `--job-id`. |
| `id` inside a node | One step in the job tree | Required for each node, and must be unique within the job. |

For one-off runs, `jobs run` can accept a job without a top-level `id`; the API can generate one. For reusable source-backed jobs, include the top-level `id` or pass `--job-id` so you can trigger it later.

## One-Off Run Or Reusable Job?

Use a one-off run when you are experimenting:

```sh
./bin/vectis-cli jobs run hello.json --follow
```

Use a reusable source-backed job when you want to trigger the same definition repeatedly:

```sh
./bin/vectis-cli jobs create hello.json --repository vectis-local --branch main --message "Add hello job"
./bin/vectis-cli jobs trigger hello-job --repository vectis-local --ref main --follow
```

You can list reusable jobs from a repository with:

```sh
./bin/vectis-cli jobs list --repository vectis-local --ref main
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
        "uses": "builtins/script",
        "with": {
          "script": "echo 'Hello from step one'"
        }
      },
      {
        "id": "where-am-i",
        "uses": "builtins/script",
        "with": {
          "script": "pwd"
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
          {"id": "hello", "uses": "builtins/script", "with": {"script": "echo hello"}},
          {"id": "where-am-i", "uses": "builtins/script", "with": {"script": "pwd"}}
        ]
      }
    }
  }
}
```

## Passing Outputs Between Nodes

A script step can publish structured outputs by writing a JSON object to a workspace-relative file and setting `with.outputs`.

Later nodes can bind accepted inputs from earlier outputs in the same local execution scope with `inputs`:

```json
{
  "id": "dataflow-job",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "script-command",
        "uses": "builtins/script",
        "with": {
          "runner": "sh",
          "script": "printf '{\"command\":\"exit 0\"}' > outputs.json",
          "outputs": "outputs.json"
        }
      },
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
    ]
  }
}
```

For a node that accepts `command`, such as `builtins/test`, use either `with.command` or `inputs.command`, not both.

For multi-line or OS-aware script bodies, omit `runner` or set it to `auto` to use PowerShell on Windows workers and `sh` on other workers; set `runner` explicitly for Bash, Batch, PowerShell, Python, or Node scripts.

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
        "uses": "builtins/script",
        "with": {
          "script": "go test ./..."
        }
      },
      {
        "id": "host-summary",
        "uses": "builtins/script",
        "isolation": "host",
        "with": {
          "script": "echo 'summary step on host'"
        }
      }
    ]
  }
}
```

If `isolation` is omitted, the node inherits the nearest parent sequence's isolation, then the job default, then the worker backend default. A worker must have a matching backend configured for `vm`; otherwise the run fails instead of falling back to host execution.

## Checkout Then Build

Use `builtins/checkout` to clone a Git repository into the run workspace. Script steps after checkout run from that workspace.

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
        "uses": "builtins/script",
        "with": {
          "script": "go test ./..."
        }
      }
    ]
  }
}
```

Do not put credentials in HTTP(S) checkout URLs. Vectis rejects URLs like `https://user:token@example.com/org/repo.git` because they can leak through logs, process lists, or persisted job definitions.

Credential-free SSH-style URLs such as `git@github.com:org/repo.git` are accepted, but the worker host still needs the right SSH configuration outside Vectis.

## Secret Files

Jobs can declare secret references at the top level. The worker asks the cell-local `vectis-secrets` broker to resolve only the secrets for the current task, writes them under `.vectis/secrets` in the workspace, sets `VECTIS_SECRETS_DIR` for the action process, and removes the directory after the task finishes.

Secret resolution requires per-execution SPIFFE identity: enable worker execution identity and SPIFFE SVID acquisition, configure `vectis-secrets` with the same `worker.execution_identity.*` values, run internal gRPC with client certificate verification so the broker can authenticate the execution SVID, and add a matching `--allow-secret` policy rule such as `namespace=*;job=secret-example;task=publish;ref=encryptedfs://team/npm-token`.

The broker routes secret refs by URI scheme. With the first built-in provider, `encryptedfs://team/npm-token` maps to an encrypted envelope file below the broker's `--encryptedfs-root`, for example `<root>/team/npm-token`. Operators can create that envelope with the CLI:

```sh
./bin/vectis-cli secrets encryptedfs put encryptedfs://team/npm-token \
  --from-file npm-token.txt \
  --root /var/lib/vectis/secrets \
  --key-file /etc/vectis/secrets.key \
  --create-key
```

The broker decrypts that envelope with `--encryptedfs-key-file` before handing the worker a task-scoped secret file:

```json
{
  "id": "secret-example",
  "secrets": [
    {
      "id": "npm-token",
      "ref": "encryptedfs://team/npm-token",
      "delivery": {
        "type": "file",
        "path": "npm/token"
      },
      "task_keys": ["publish"]
    }
  ],
  "root": {
    "id": "publish",
    "uses": "builtins/script",
    "with": {
      "script": "npm publish --//registry.npmjs.org/:_authToken=\"$(cat \"$VECTIS_SECRETS_DIR/npm/token\")\""
    }
  }
}
```

The repository includes a smoke-test version of this pattern at `examples/secrets.json`. See [Secrets Reference](./secrets-reference.md) for the full field contract, provider rules, policy syntax, and redacted troubleshooting signals.

## Built-In Actions

These are the built-ins available today:

| Action | Required `with` fields | What it does |
| --- | --- | --- |
| `builtins/script` | `script` | Writes the script to a temporary workspace file and runs it with the selected runner. Optional `outputs` reads a workspace-relative JSON output file after success. |
| `builtins/test` | `command`; optional `runner` | Runs a predicate command and returns a boolean `result` output. |
| `builtins/checkout` | `url` | Checks out a Git repository into the run workspace. Optional `ref` fetches a ref from `origin` and checks out `FETCH_HEAD`; optional `fetch_refspecs` fetches additional refspecs. |
| `builtins/gerrit-review` | `url`, `change`, `message`, `username`, `password_file` | Posts a Gerrit review message and optional label vote using a workspace-relative password file. |
| `builtins/sequence` | none | Runs child `steps` in order. |
| `builtins/parallel` | none | Runs child `branches` concurrently or fans them out across workers. |
| `builtins/if` | none | Runs a `condition` node, then runs either the `then` or `else` port. |
| `builtins/retry` | optional `attempts` | Retries a local `body` port until it succeeds or attempts are exhausted. |
| `builtins/timeout` | `duration` | Runs a local `body` port with a deadline such as `30s` or `5m`. |
| `builtins/finally` | none | Runs `body`, then always runs cleanup nodes from the `always` port. |
| `builtins/fallback` | none | Runs local `choices` in order and returns the first success. |
| `builtins/result` | `success` | Returns success when `success` is `true` and failure when `false`. |
| `builtins/upload-artifact` | `name`, `path` | Publishes a workspace-relative file as a run artifact. Optional `max_bytes` can lower the worker upload cap for this node. |

Actions are intentionally small right now. Pipeline-as-code and richer action syntax are future work; today, JSON is the source format.

Artifacts are produced with explicit `builtins/upload-artifact` steps. Vectis does not automatically collect files from the workspace or read a top-level artifact declaration yet. Uploaded bytes are stored and downloaded unchanged, so create archives yourself when you want multiple files in one artifact. See [Artifacts](./artifacts.md) for list/download commands, manifest fields, and operator notes.

## Common Validation Errors

Vectis validates jobs before storing them or starting a one-off run.

| Error | How to fix it |
| --- | --- |
| `root` is missing | Add a `root` object to the job. |
| Node `id` is missing | Add an `id` to every node, including child steps. |
| Duplicate node ID | Rename one of the duplicate node IDs. |
| Unknown action | Check the `uses` value and any configured action registry roots. Builtins work without configuration; local custom actions require `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS`. |
| Unsupported `isolation` | Use `host` or `vm`. |
| Missing `script` for `builtins/script` | Add `with.script`. |
| Missing `url` for `builtins/checkout` | Add `with.url`. |
| Unknown key in `with` | Remove fields the selected action does not understand. |

For the full validation contract, including limits and API error shape, see [Job Definition Validation](./job-validation.md).

## A Good First Workflow

When you are learning or debugging, this loop is usually enough:

1. Edit a local JSON file.
2. Run it once with `./bin/vectis-cli jobs run <file> --follow`.
3. If it works and you want to reuse it, commit it with `./bin/vectis-cli jobs create <file> --repository <repo> --branch <branch>`.
4. Trigger future runs with `./bin/vectis-cli jobs trigger <job-id> --repository <repo> --follow`.
5. Inspect history with `./bin/vectis-cli runs list <job-id> --repository <repo>`.

This keeps experimentation cheap while still giving you a path to a reusable job.
