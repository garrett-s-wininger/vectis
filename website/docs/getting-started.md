# Getting Started

This guide is for your first local run of Vectis. You will build the binaries, start the full local stack, run a small job, and watch its logs.

It assumes you are working from a clone of the Vectis repository on a development machine. For production deployment, start here to learn the shape of the system, then continue to [Configuration](./operating/configuration.md) and [Reference Deployment Posture](./operating/deployment/reference-deployment-posture.md).

## What You Will Start

`vectis-local` starts the services that make up a local Vectis stack:

| Service | What it does |
| --- | --- |
| API | Accepts HTTP and CLI requests. |
| Queue | Holds work until a worker takes it. |
| Worker | Executes jobs. |
| Log service | Stores and streams run output. |
| Registry | Lets services find one another. |
| Cron and reconciler | Handle scheduled work and repair missed queue handoffs. |
| UI | Serves the browser application locally. |
| Docs | Serves the docs site locally. |

The local stack uses SQLite by default and stores data under your user data directory, usually `~/.local/share/vectis` when `XDG_DATA_HOME` is not set.

## Prerequisites

You need:

- Go `1.25.11` or newer.
- CGO enabled, which is the normal Go default, because local SQLite uses `mattn/go-sqlite3`.
- A shell where you can run `mage`.

You do not need Postgres, Podman, or Kubernetes for this local path.

## Build Vectis

From the repository root:

```sh
mage build
```

This creates binaries under `bin/`, including `vectis-local`, `vectis-cli`, `vectis-ui`, and `vectis-docs`.

The default build also builds this docs site and the browser UI, then embeds them into `vectis-docs` and `vectis-ui`.
If you want a faster development build without local web assets, run:

```sh
SKIP_WEB_BUILD=1 mage build
```

## Start The Local Stack

In one terminal, run:

```sh
./bin/vectis-local
```

Leave this process running. It supervises the local API, cell ingress, queue, worker, worker core, secrets service, log service, artifact service, registry, cron, reconciler, catalog, UI, and docs site.

By default, `vectis-local` creates local TLS material. Internal gRPC uses it immediately. The local API and docs use HTTPS automatically when that generated CA is already trusted by the system store, or when you start with `--http-tls=on`. Otherwise they keep using HTTP and log the trust-store setup command.

To prepare trusted local HTTPS without running the full stack as an elevated user:

```sh
./bin/vectis-local init
sudo ./bin/vectis-local install-cert
```

`install-cert` only installs the generated CA certificate; it does not generate files, migrate databases, or start services.

When local HTTPS is not enabled, the public API listens on:

```text
http://localhost:8080
```

and the docs site listens on:

```text
http://localhost:8088
```

The UI listens on:

```text
http://localhost:8089
```

`vectis-local` enables HTTP API authentication by default. On first start with a fresh database, the log prints a local setup bootstrap token and stores it at:

```text
~/.local/share/vectis/local-bootstrap-token
```

Open the UI, enter that bootstrap token, and create the first admin account. `vectis-ui` owns first-load browser routing: it sends unauthenticated browsers to setup or login, then serves the React shell for allowed app routes. The browser receives an HttpOnly UI session cookie; `vectis-ui` keeps the API token server-side when proxying browser API calls. Use `./bin/vectis-local --auth=false` only when you intentionally want an unauthenticated local API.

If your browser is on a different machine than the dev shell, start the stack with:

```sh
./bin/vectis-local --host 0.0.0.0
```

Then open the API, UI, or docs using the dev machine's address. If you open docs with a hostname other than `localhost`, set `VECTIS_DOCS_ALLOWED_HOSTS` to that hostname before starting `vectis-local`; docs Host validation stays strict even when the listener binds to `0.0.0.0`. Use this only on a trusted network or behind your own access controls.

`vectis-local` serves the UI from the `vectis-ui` binary and docs from the `vectis-docs` binary. If you built with `SKIP_WEB_BUILD=1`, `vectis-local` logs a warning and continues without those web servers. You can also start the stack with `./bin/vectis-local --ui=false` or `./bin/vectis-local --docs=false`.

## Check Health

In a second terminal, run:

```sh
./bin/vectis-cli auth login
```

Then check the local stack:

```sh
./bin/vectis-cli health check
```

You should see a list of checks for the API, database schema, queue, reconciler, log service, and related operational signals. A fresh local stack should be healthy enough to accept jobs.

If the command cannot connect, confirm `vectis-local` is still running and that the API is listening on port `8080`.

## Run Your First Job

Vectis jobs are JSON documents. The repository includes a small example at `examples/sequenced.json`.

Run it once and stream the logs:

```sh
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

This submits an ephemeral job: Vectis stores enough definition data to recover the run, but it does not save the job as a reusable named job.

You should see log output from the example steps, including:

```text
Hello from Vectis!
Running multiple steps in sequence
```

## Save And Trigger A Reusable Job

If you want to save a job definition and trigger it later, commit it to a registered source repository:

```sh
./bin/vectis-cli jobs create examples/sequenced.json --repository vectis-local --branch main --message "Add sequenced job"
```

If you already created this example during an earlier run, update it with `./bin/vectis-cli jobs edit sequenced-job --repository vectis-local --branch main` or delete it with `./bin/vectis-cli jobs delete sequenced-job --repository vectis-local --branch main --yes`.

Then trigger it:

```sh
./bin/vectis-cli jobs trigger sequenced-job --repository vectis-local --ref main --follow
```

The `--follow` flag streams logs for the run that was just created.

You can list reusable jobs from the repository with:

```sh
./bin/vectis-cli jobs list --repository vectis-local --ref main
```

And list recent runs for that source-backed job with:

```sh
./bin/vectis-cli runs list sequenced-job --repository vectis-local
```

## Inspect A Run Later

If you ran a job without `--follow`, the CLI prints a `run_id`. Use that ID to inspect status or stream logs:

```sh
./bin/vectis-cli runs show <run-id>
./bin/vectis-cli logs run <run-id>
```

Runs move through durable states in the database. The queue hands work to workers, but the database is the source of truth for run status.

## Stop Or Reset Local State

Stop the local stack with `Ctrl+C` in the terminal running `vectis-local`.

To see what local files Vectis would remove during cleanup:

```sh
./bin/vectis-cli local reset --dry-run
```

To remove local Vectis data and generated deployment state:

```sh
./bin/vectis-cli local reset --yes
```

Only run reset when you are comfortable deleting local Vectis state.

## Where To Go Next

| If you want to... | Read next |
| --- | --- |
| Write your own job definition | [Your First Job](./using/your-first-job.md) |
| Learn the everyday CLI workflows | [CLI Guide](./using/cli-guide.md) |
| Understand jobs, runs, workers, and queues | [Architecture](./concepts/architecture.md) |
| Learn the job JSON shape and validation rules | [Job Definition Validation](./using/job-validation.md) |
| Use the HTTP API directly | [API Reference](./using/api-reference.md) |
| Configure ports, databases, TLS, and discovery | [Configuration](./operating/configuration.md) |
| Try multi-cell routing locally | [Multi-Cell Operation](./operating/multi-cell.md) |
| Exercise SPIFFE-backed local secret resolution | [Local SPIFFE Secrets Smoke Test](./operating/deployment/local-spiffe-secrets-smoke-test.md) |
| Run a Podman reference deployment | [Reference Deployment Posture](./operating/deployment/reference-deployment-posture.md) |
| Troubleshoot stuck runs or service health | [Repair Runbooks](./operating/reliability/repair-runbooks.md) |
