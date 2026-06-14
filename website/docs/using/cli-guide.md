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

Logout invalidates the server-side session when the saved token is a login session, then removes the local token file. Durable API tokens are managed separately with `vectis-cli auth tokens delete`.

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
./bin/vectis-cli health check --format json
```

The health check covers API liveness/readiness, schema state, queue backlog, cron schedule backlog, reconciler visibility, stuck queued dispatch, pending task continuations, orphaned task finalization, catalog inbox health, source-only readiness, source repository and schedule health, log reachability, audit durability, and database pool pressure.

## Inspect Cells

In multi-cell deployments, inspect execution cell routing, dispatch/task repair pressure, and fan-in pressure directly. The table includes a per-cell `READY` summary plus check details for routing, dispatch, and catalog fan-in:

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

`runs show` includes a `dispatch_summary` that groups API, cron, and reconciler handoff attempts by producer, plus the raw `dispatch_events` audit trail when events exist.

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

## Trigger Jobs From Source

Register a source repository checkout:

```sh
./bin/vectis-cli sources register vectis-local /srv/vectis-repo --default-ref main
```

Operators can also declare repositories with `VECTIS_SOURCE_REPOSITORIES` so `vectis-api` reconciles them on startup. Set `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP=true` when those configured repositories should also be cloned, fetched, or probed before the API starts serving. Set `VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_INTERVAL` to periodically refresh enabled configured repositories in the background; use the matching max-concurrency and failure-backoff settings to control large-repository pressure. Source cron schedules can be declared with `VECTIS_SOURCE_SCHEDULES`; they reference a configured repository, stable `schedule_id`, `job_id`, cron expression, and optional `ref` or path override.

List repositories and find stale rows omitted from current config:

```sh
./bin/vectis-cli sources list
./bin/vectis-cli sources list --stale
```

List reconciled source schedules:

```sh
./bin/vectis-cli sources schedules
./bin/vectis-cli sources schedules vectis-local
./bin/vectis-cli sources schedules --overrides
./bin/vectis-cli sources schedules --stale
./bin/vectis-cli sources disable-schedule old-nightly
./bin/vectis-cli sources enable-schedule old-nightly
./bin/vectis-cli sources delete-schedule old-nightly --yes
```

Temporarily point a source schedule at a hotfix ref or definition path, then clear the override after the fix lands in the configured location:

```sh
./bin/vectis-cli sources override nightly-build --ref hotfix/build --path .vectis/jobs/build-hotfix.json --reason "production hotfix"
./bin/vectis-cli sources clear-override nightly-build
```

For a Vectis-managed checkout, omit the checkout path and provide the canonical clone URL:

```sh
./bin/vectis-cli sources register vectis-local --checkout-mode managed --canonical-url https://git.example.com/acme/vectis.git --default-ref main
```

Inspect or update the repository registration:

```sh
./bin/vectis-cli sources get vectis-local
./bin/vectis-cli sources update vectis-local --default-ref main --authoring-mode local_commit
./bin/vectis-cli sources update vectis-local --disable
./bin/vectis-cli sources update vectis-local --enable
```

Delete an unused repository registration without touching checkout files:

```sh
./bin/vectis-cli sources delete vectis-local --yes
```

Declared repositories, repositories with source schedules, and repositories with recorded source provenance cannot be deleted; remove the declaration or disable them instead so scheduled references, historical runs, and stored definition versions can still resolve their repository metadata.

Inspect source-control readiness, declaration counts, stale rows, sync summaries, and active schedule overrides:

```sh
./bin/vectis-cli sources overview
```

Sync the repository, then list triggerable jobs discovered under `.vectis/jobs`:

```sh
./bin/vectis-cli sources sync vectis-local
./bin/vectis-cli sources status vectis-local
./bin/vectis-cli sources branches vectis-local --prefix feature/
./bin/vectis-cli sources tree vectis-local --ref main --path .vectis --recursive
./bin/vectis-cli sources definitions vectis-local --ref main
./bin/vectis-cli sources jobs vectis-local --ref main
```

Use `sources definitions` to inspect candidate JSON files without reading file contents, and `sources jobs` to see the triggerable job IDs derived from those paths. When branch, tree, definition, job, or import output reaches its limit, non-JSON output prints a truncation notice; tree, definition, job, and import commands also print a `--cursor` value when another request can continue from the last returned path.

For hybrid or migration deployments that still use stored jobs, preview and import source definitions:

```sh
./bin/vectis-cli sources resolve vectis-local .vectis/jobs/build.json --ref main
./bin/vectis-cli sources import vectis-local --ref main --dry-run
./bin/vectis-cli sources import vectis-local --ref main --update-existing
```

Create or update a specific stored job from a repository definition, then inspect the recorded source provenance:

```sh
./bin/vectis-cli jobs source create build vectis-local --ref main --namespace /
./bin/vectis-cli jobs source update build vectis-local --ref main
./bin/vectis-cli jobs source show build --version 1
./bin/vectis-cli jobs source definition build --version 1
```

Pass a definition path as the third positional argument when a job does not use the default `.vectis/jobs/<job-id>.json` layout.

Inspect a source-defined job definition at a specific ref:

```sh
./bin/vectis-cli sources show vectis-local build --ref main
```

For a managed repository with `authoring_mode=local_commit`, write a definition back into source without creating a stored job row:

```sh
./bin/vectis-cli sources write vectis-local build ./build.json --branch main --message "Update build job"
./bin/vectis-cli sources write vectis-local build ./build.json --branch main --expected-head <commit>
```

Trigger a source-defined job without creating a stored job row:

```sh
./bin/vectis-cli sources trigger vectis-local build --ref main --follow
```

List runs recorded for a source-defined job:

```sh
./bin/vectis-cli sources runs vectis-local build
```

Stream logs for the latest source-defined run, or for a specific source run:

```sh
./bin/vectis-cli sources logs vectis-local build
./bin/vectis-cli sources logs vectis-local build <run-id>
```

Use `sources trigger` with `source.stored_jobs_enabled=false` when the instance should run only repository-defined jobs.

## Inspect Runs

Show one run:

```sh
./bin/vectis-cli runs show <run-id>
```

The detail output includes `owning_cell` when the run belongs to a named execution cell.
It also prints audit fields such as the definition hash, trigger invocation, requested cells, and frozen execution payload hash when those fields are available.
When task records exist, the output includes a compact task completion summary.
If the run failed during a worker-controlled SVID or secret-resolution gate, `runs show` prints `next_action=security_gate_failed`, a redacted `latest_failed_security_event`, and retry guidance to fix that gate before retrying or replaying.

Show the frozen job definition snapshot captured for a run:

```sh
./bin/vectis-cli runs definition <run-id>
```

Use `--format json` to include the run id, job id, definition version/hash, and source provenance alongside the definition JSON.

List the task graph nodes and task attempts recorded for one run:

```sh
./bin/vectis-cli runs tasks <run-id>
```

Attempt rows include execution ID/status and worker lease owner/expiry when the execution is actively owned. If worker-controlled SVID checks or secret resolution ran for an attempt, the row also includes a redacted `security` summary with outcome, reason, provider kind, and counts.

List artifact manifests recorded for one run:

```sh
./bin/vectis-cli runs artifacts list <run-id>
./bin/vectis-cli runs artifacts list <run-id> --task-id <task-id>
./bin/vectis-cli runs artifacts list <run-id> --task-attempt-id <task-attempt-id>
./bin/vectis-cli runs artifacts list <run-id> --execution-id <execution-id>
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

For source-only jobs, use the source-scoped log route so runs are matched by repository provenance:

```sh
./bin/vectis-cli sources logs vectis-local build --follow
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

This removes local Vectis config, data, cache, CLI tokens, generated deployment state, and configured local durable paths such as log-forwarder spools. It does not stop running services or delete remote/container volumes.

## Operator Commands

These commands are useful, but they change durable state or deployment state. Use them deliberately.

| Task | Command |
| --- | --- |
| Apply embedded database migrations | `./bin/vectis-cli database migrate` |
| Preview retention cleanup | `./bin/vectis-cli retention cleanup --dry-run` |
| Apply retention cleanup | `./bin/vectis-cli retention cleanup --yes` |
| Write an encryptedfs job secret | `./bin/vectis-cli secrets encryptedfs put encryptedfs://team/npm-token --from-file npm-token.txt --root /var/lib/vectis/secrets --key-file /etc/vectis/secrets.key --create-key` |
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
