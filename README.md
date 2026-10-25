# Vectis

Vectis is a self-hosted job runner for CI/CD-style workflows and other repeatable automation. You define a job, submit it to the API or CLI, and Vectis queues the work, runs it on a worker, stores run history, and streams logs back to you.

Vectis is developer alpha software for trying the model and developing Vectis itself. Production and untrusted workloads should wait.

## Quick Start

On Unix, install the local toolchain, build, and start the stack from the repository root:

```bash
scripts/dev-doctor.sh --install --yes
./.tools/bin/mage build
./bin/vectis-local
```

`./.tools/bin/mage` uses the Go installed by the doctor. `mage build` produces the local services, supervisor, workers, and CLI.

On Windows, run `.\scripts\dev-doctor.ps1`, then `mage build` and `.\bin\vectis-local`. Toolchain, CGO, protobuf, and Windows build notes are in [CONTRIBUTING.md](CONTRIBUTING.md).

`vectis-local` enables API authentication and finishes first-admin setup once the API is healthy. In a terminal it prompts for the admin username and password. Without a terminal it creates user `admin`, writes the password to `~/.local/share/vectis/local-admin-password` when `XDG_DATA_HOME` is unset, and saves the CLI token for later commands. Pass `--setup=off` to keep the bootstrap token and finish setup yourself.

Check that the stack is healthy, then run the included example:

```bash
./bin/vectis-cli health check
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

That is the smallest useful loop: install, build, start, create the first admin, check health, run a job.

## What Starts Locally

`vectis-local` starts the services built by `mage build`. The API listens on `http://localhost:8080`.

| Service | What it does |
| --- | --- |
| API | Accepts HTTP and CLI requests. |
| Queue | Holds work until a worker takes it. |
| Worker | Executes jobs. |
| Log service | Receives and serves run logs. |
| Artifact service | Stores content-addressed artifact blobs. |
| Registry | Lets services find each other locally. |
| Cron | Evaluates schedules. |
| Reconciler | Repairs queued runs that missed queue handoff. |

The browser UI and docs site start from `vectis-ui` and `vectis-docs`. Those binaries come from `buildFull`:

```bash
scripts/dev-doctor.sh --install --yes --install-frontend
./.tools/bin/mage buildFull
./bin/vectis-local
```

After setup, the UI is at `http://localhost:8089` and the docs site is at `http://localhost:8088`. Sign in with the admin username and password from the `vectis-local` terminal. On Windows, install the frontend toolchain from [CONTRIBUTING.md](CONTRIBUTING.md), then run `mage buildFull`.

To reach the stack from another machine, run `./bin/vectis-local --host 0.0.0.0` and use that machine's address on a trusted network. Local data paths are in [Configuration](./website/docs/operating/configuration.md).

Press `Ctrl+C` in the `vectis-local` terminal to stop the stack. Inspect or remove local state with:

```bash
./bin/vectis-cli local reset --dry-run
./bin/vectis-cli local reset --yes
```

## Common Workflows

Run a one-off job:

```bash
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

Store a job and trigger it later. `vectis-local` registers managed repository `vectis-local` with `local_commit` authoring, which is what `jobs create` writes to:

```bash
./bin/vectis-cli jobs create examples/sequenced.json --repository vectis-local --branch main --message "Add sequenced job"
./bin/vectis-cli jobs trigger sequenced-job --repository vectis-local --ref main --follow
```

Inspect a one-off run with the `run_id` printed by `jobs run`:

```bash
./bin/vectis-cli runs show <run-id>
./bin/vectis-cli logs run <run-id>
```

List runs for the reusable job after it is stored:

```bash
./bin/vectis-cli runs list sequenced-job --repository vectis-local
```

More checked-in jobs are ordered in [examples/README.md](examples/README.md).

Continue in the docs:

| Start here | When you need |
| --- | --- |
| [Getting Started](./website/docs/getting-started.md) | A slower walkthrough of the local stack and first run. |
| [Your First Job](./website/docs/using/your-first-job.md) | How to write the JSON job definitions Vectis runs today. |
| [CLI Guide](./website/docs/using/cli-guide.md) | Everyday `vectis-cli` commands. |
| [Configuration](./website/docs/operating/configuration.md) | Environment variables, flags, discovery, storage, and TLS settings. |
| [Security](./website/docs/concepts/security.md) | Trust boundaries, auth, tokens, RBAC, and deployment cautions. |

## Deployment

For local development, use `vectis-local`.

For a fuller reference deployment, build the component images and start the Podman stack:

```bash
mage imagesComponents
./bin/vectis-cli deploy podman up
```

That path is documented in [Reference Deployment Posture](./website/docs/operating/deployment/reference-deployment-posture.md). The production-oriented target is [Production Topology v1](./website/docs/operating/deployment/production-topology-v1.md). Read [Security](./website/docs/concepts/security.md) before exposing the API beyond a trusted network.

## Development

Toolchain setup, tests, protobuf generation, Windows builds, and the rest of the docs map live in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

See [LICENSE](LICENSE).
