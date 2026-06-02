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
| Docs | Serves the docs site locally. |

The local stack uses SQLite by default and stores data under your user data directory, usually `~/.local/share/vectis` when `XDG_DATA_HOME` is not set.

## Prerequisites

You need:

- Go `1.25.10` or newer.
- CGO enabled, which is the normal Go default, because local SQLite uses `mattn/go-sqlite3`.
- A shell where you can run `make`.

You do not need Postgres, Podman, or Kubernetes for this local path.

## Build Vectis

From the repository root:

```sh
make build
```

This creates binaries under `bin/`, including `vectis-local`, `vectis-cli`, and `vectis-docs`.

The default build also builds this docs site and embeds it into `vectis-docs`.
If you want a faster development build without local docs, run:

```sh
SKIP_WEB_BUILD=1 make build
```

## Start The Local Stack

In one terminal, run:

```sh
./bin/vectis-local
```

Leave this process running. It supervises the local API, cell ingress, queue, worker, log service, registry, cron, reconciler, catalog, and docs site.

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

If your browser is on a different machine than the dev shell, start the stack with:

```sh
./bin/vectis-local --host 0.0.0.0
```

Then open the API or docs using the dev machine's address. Use this only on a trusted network or behind your own access controls.

`vectis-local` serves docs from the `vectis-docs` binary. If you built with `SKIP_WEB_BUILD=1`, `vectis-local` logs a warning and continues without the docs site. You can also start the stack with `./bin/vectis-local --docs=false`.

## Check Health

In a second terminal, run:

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

## Store And Trigger A Job

If you want to save a job definition and trigger it later, create a stored job:

```sh
./bin/vectis-cli jobs create examples/sequenced.json
```

If you already created this example during an earlier run, the CLI will say the job already exists. That is fine; you can keep using it or delete it with `./bin/vectis-cli jobs delete sequenced-job --yes`.

Then trigger it:

```sh
./bin/vectis-cli jobs trigger sequenced-job --follow
```

The `--follow` flag streams logs for the run that was just created.

You can list stored jobs with:

```sh
./bin/vectis-cli jobs list
```

And list recent runs for a stored job with:

```sh
./bin/vectis-cli runs list sequenced-job
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
| Run a Podman reference deployment | [Reference Deployment Posture](./operating/deployment/reference-deployment-posture.md) |
| Troubleshoot stuck runs or service health | [Repair Runbooks](./operating/reliability/repair-runbooks.md) |
