# CLI Guide

`vectis-cli` is the everyday way to talk to a Vectis API from a terminal. Use it to submit jobs, trigger stored jobs, follow logs, inspect runs, check health, and perform local/operator maintenance.

This guide is task-based. For a compact command inventory, see [CLI Operational Coverage](../operating/reference/cli-operational-coverage.md).

## Before You Start

Build the CLI from the repository root:

```sh
make build
```

The binary is:

```sh
./bin/vectis-cli
```

By default, the CLI talks to:

```text
http://localhost:8080
```

That matches the API started by `./bin/vectis-local`.

## Authentication

Local development defaults to API authentication off, so most workflow commands work without a token.

When API authentication is enabled, log in once:

```sh
./bin/vectis-cli auth login --username <username>
```

The CLI prompts for a password, requests a bearer-capable login session, and saves the returned session token in your user config directory. Later commands use that saved token automatically.

You can also provide a token for one shell session:

```sh
export VECTIS_API_TOKEN=<token>
```

To remove the locally saved token:

```sh
./bin/vectis-cli auth logout
```

Logout invalidates the server-side session when the saved token is a login session, then removes the local token file. Durable API tokens are managed separately with `vectis-cli auth token delete`.

## Check Health

Start here when you want to know whether the API and its dependencies are healthy:

```sh
./bin/vectis-cli health check
```

For CI or stricter smoke tests, treat warnings as failures:

```sh
./bin/vectis-cli health check --strict
```

For automation, emit JSON:

```sh
./bin/vectis-cli health check --json
```

The health check covers API liveness/readiness, schema state, queue backlog, reconciler visibility, stuck root or task dispatch, catalog inbox health, log reachability, audit durability, and database pool pressure.

## Inspect Cells

In multi-cell deployments, inspect execution cell routing, dispatch repair pressure, and fan-in pressure directly:

```sh
./bin/vectis-cli cells status
```

For automation, use the global output format flag:

```sh
./bin/vectis-cli cells status --format json
```

## Resolve An Action

Configure local examples when you want to try custom actions from this repository:

```sh
export VECTIS_ACTION_REGISTRY_LOCAL_ROOTS=examples/actions
```

Use `actions list` to inspect builtins and configured local custom actions:

```sh
./bin/vectis-cli actions list
```

Use `actions resolve` to check how a custom action reference resolves and to find the immutable digest for pinning:

```sh
./bin/vectis-cli actions resolve examples/greet@v1
```

For automation, emit JSON:

```sh
./bin/vectis-cli actions resolve examples/greet@v1 --format json
```

If action policy already requires digest pins or hides a namespace/source while you are preparing configuration, use `--ignore-policy`:

```sh
./bin/vectis-cli actions list --ignore-policy
./bin/vectis-cli actions resolve examples/greet@v1 --ignore-policy
```

`actions list` shows descriptor lifecycle status. Default policy hides yanked, revoked, and purged custom actions. Use `--ignore-policy` to inspect tombstones and status reasons when an action was removed.

Run the repository's hello-world custom action example with:

```sh
./bin/vectis-cli jobs run examples/custom-greet.json --follow
```

## Run A Job Once

Use `jobs run` for experimentation or one-off work:

```sh
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

`--follow` streams logs for the run that was just created.

Without `--follow`, the command prints the `run_id`:

```sh
./bin/vectis-cli jobs run examples/sequenced.json
```

Route a one-off run to a specific execution cell:

```sh
./bin/vectis-cli jobs run examples/sequenced.json --cell pdx-b
```

Use that ID later with:

```sh
./bin/vectis-cli runs show <run-id>
./bin/vectis-cli logs run <run-id>
```

For safe client retries after a network error, pass an idempotency key:

```sh
./bin/vectis-cli jobs run examples/sequenced.json --idempotency-key "$(uuidgen)"
```

## Store And Trigger Jobs

Create a reusable stored job:

```sh
./bin/vectis-cli jobs create examples/sequenced.json
```

List stored jobs:

```sh
./bin/vectis-cli jobs list
```

Show a stored definition:

```sh
./bin/vectis-cli jobs show sequenced-job
```

Trigger a stored job and stream logs:

```sh
./bin/vectis-cli jobs trigger sequenced-job --follow
```

Trigger without following:

```sh
./bin/vectis-cli jobs trigger sequenced-job
```

Trigger a stored job in specific execution cells:

```sh
./bin/vectis-cli jobs trigger sequenced-job --cell local --cell pdx-b
```

When more than one cell is targeted, the command prints one run per cell. Use `runs list <job-id>` or `runs show <run-id>` to inspect the global run catalog.

Edit a stored job in `$EDITOR`:

```sh
./bin/vectis-cli jobs edit sequenced-job
```

Delete a stored job:

```sh
./bin/vectis-cli jobs delete sequenced-job --yes
```

Deleting a job removes the stored definition and prevents future triggers. It does not erase historical runs.

## Inspect Runs

Show one run:

```sh
./bin/vectis-cli runs show <run-id>
```

The detail output includes `owning_cell` when the run belongs to a named execution cell.
It also prints audit fields such as the definition hash, trigger invocation, requested cells, and frozen execution payload hash when those fields are available.
When task records exist, the output includes a compact task completion summary.

List the task graph nodes and task attempts recorded for one run:

```sh
./bin/vectis-cli runs tasks <run-id>
```

Attempt rows include execution ID/status and worker lease owner/expiry when the execution is actively owned.

List artifact manifests recorded for one run:

```sh
./bin/vectis-cli runs artifacts list <run-id>
```

Download an artifact by name:

```sh
./bin/vectis-cli runs artifacts download <run-id> coverage --output coverage.txt
```

Use `--output -` only when you want raw artifact bytes on stdout, for example in a pipeline.

Show the frozen execution payload captured for a run:

```sh
./bin/vectis-cli runs payload <run-id>
```

Replay a completed run as a fresh queued run using the source run's captured definition version:

```sh
./bin/vectis-cli runs replay <run-id>
```

Replay back to a named execution cell, or make the request safe to retry:

```sh
./bin/vectis-cli runs replay <run-id> --cell pdx-b
./bin/vectis-cli runs replay <run-id> --idempotency-key "$(uuidgen)"
```

List runs for a stored job:

```sh
./bin/vectis-cli runs list sequenced-job
```

Filter a stored job's runs to one execution cell:

```sh
./bin/vectis-cli runs list sequenced-job --cell pdx-b
```

Limit the number of runs:

```sh
./bin/vectis-cli runs list sequenced-job --limit 10
```

Cancel an executing run:

```sh
./bin/vectis-cli runs cancel <run-id>
```

Cancellation goes through the worker control path, so it only applies when the run is executing and the worker can be reached.

## Stream Logs

Stream logs for one run:

```sh
./bin/vectis-cli logs run <run-id>
```

Follow future runs for a stored job:

```sh
./bin/vectis-cli logs job sequenced-job
```

`logs job` follows runs created after you connect. It is useful when you want a terminal open before triggering the next run.

Filter to one stream when needed:

```sh
./bin/vectis-cli logs run <run-id> --stdout
./bin/vectis-cli logs run <run-id> --stderr
```

## Manage Users, Tokens, And Roles

These commands require API authentication and enough RBAC permission.

List users:

```sh
./bin/vectis-cli users list
```

Create a user:

```sh
./bin/vectis-cli users create alice
```

Create an API token for yourself:

```sh
./bin/vectis-cli auth tokens create --label laptop --expires-in 3m
```

List your tokens:

```sh
./bin/vectis-cli auth tokens list
```

Create a namespace:

```sh
./bin/vectis-cli namespaces create team-a
```

Grant a role:

```sh
./bin/vectis-cli role-bindings grant <namespace-id> <user-id> viewer
```

Roles are documented with the auth model in [Security Posture](../concepts/security.md).

## Local Development Cleanup

Preview local cleanup first:

```sh
./bin/vectis-cli local reset --dry-run
```

Apply local cleanup:

```sh
./bin/vectis-cli local reset --yes
```

This removes local Vectis config, data, cache, CLI tokens, and generated deployment state. It does not stop running services or delete remote/container volumes.

## Operator Commands

These commands are useful, but they change durable state or deployment state. Use them deliberately.

| Task | Command |
| --- | --- |
| Apply embedded database migrations | `./bin/vectis-cli database migrate` |
| Preview retention cleanup | `./bin/vectis-cli retention cleanup --dry-run` |
| Apply retention cleanup | `./bin/vectis-cli retention cleanup --yes` |
| Generate Podman deployment secrets | `./bin/vectis-cli deploy podman init` |
| Render the Podman manifest | `./bin/vectis-cli deploy podman render` |
| Render the Podman HA profile | `./bin/vectis-cli deploy podman --profile ha render` |
| Start or replace the Podman reference deployment | `./bin/vectis-cli deploy podman up` |
| Start or replace the Podman HA profile | `./bin/vectis-cli deploy podman --profile ha up` |
| Show Podman deployment status | `./bin/vectis-cli deploy podman status` |
| Stop the Podman deployment | `./bin/vectis-cli deploy podman down` |

For operational context, see [Configuration](../operating/configuration.md), [Retention And Storage Pressure](../operating/reliability/retention.md), and [Reference Deployment Posture](../operating/deployment/reference-deployment-posture.md).

## Good Daily Loop

For local development, a comfortable CLI loop looks like this:

1. Start `./bin/vectis-local`.
2. Run `./bin/vectis-cli health check`.
3. Edit a job JSON file.
4. Run it with `./bin/vectis-cli jobs run <file> --follow`.
5. Store it once it works: `./bin/vectis-cli jobs create <file>`.
6. Trigger future runs with `./bin/vectis-cli jobs trigger <job-id> --follow`.
7. Inspect history with `./bin/vectis-cli runs list <job-id>`.

That keeps experimentation fast while still exercising the same API and worker path used by stored jobs.
