# Vectis

Vectis is a self-hosted job runner for CI/CD-style workflows and other repeatable automation. You define a job, submit it to the API or CLI, and Vectis queues the work, runs it on a worker, stores run history, and streams logs back to you.

The project is still pre-production, but the local stack is useful today for trying the model, building examples, and developing Vectis itself.

## Quick Start

Build Vectis:

```bash
make build
```

Start the local stack:

```bash
./bin/vectis-local
```

In another terminal, check that the stack is healthy:

```bash
./bin/vectis-cli health check
```

Run the included example job and follow its logs:

```bash
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

That is the smallest useful loop: build, start, check health, run a job.

## What Starts Locally

`vectis-local` starts a complete local Vectis stack:

| Service | What it does |
| --- | --- |
| API | Accepts HTTP and CLI requests. |
| Queue | Holds work until a worker takes it. |
| Worker | Executes jobs. |
| Log service | Receives and serves run logs. |
| Registry | Lets services find each other locally. |
| Cron | Evaluates schedules. |
| Reconciler | Repairs queued runs that missed queue handoff. |

By default, the API listens on `http://localhost:8080`. Local data is stored under your user data directory; see [Configuration](./website/docs/operating/configuration.md) for exact paths and overrides.

To stop the local stack, press `Ctrl+C` in the terminal running `vectis-local`.

To inspect or remove local state:

```bash
./bin/vectis-cli local reset --dry-run
./bin/vectis-cli local reset --yes
```

## Requirements

- Go `1.25.10+` as declared in [go.mod](go.mod).
- CGO enabled for local SQLite use. This is the normal Go default on most developer machines.
- `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` only if you need to regenerate protobuf code with `make proto`.

## Learn The Basics

The docs site is the best place to continue:

| Start here | When you need |
| --- | --- |
| [Getting Started](./website/docs/getting-started.md) | A slower walkthrough of the local stack and first run. |
| [Your First Job](./website/docs/using/your-first-job.md) | How to write the JSON job definitions Vectis runs today. |
| [CLI Guide](./website/docs/using/cli-guide.md) | Everyday `vectis-cli` commands. |
| [API Reference](./website/docs/using/api-reference.md) | HTTP routes, request shapes, auth actions, and error envelopes. |
| [Configuration](./website/docs/operating/configuration.md) | Environment variables, flags, discovery, storage, and TLS settings. |
| [Architecture](./website/docs/concepts/architecture.md) | The current component model and data flows. |
| [Security](./website/docs/concepts/security.md) | Trust boundaries, auth, tokens, RBAC, and deployment cautions. |
| [Planning](./website/docs/developing/roadmap/planning.md) | Product direction, deferred work, and future federation notes. |

## Common Workflows

Run a one-off job:

```bash
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

Store a job and trigger it later:

```bash
./bin/vectis-cli jobs create examples/sequenced.json
./bin/vectis-cli jobs trigger sequenced-job --follow
```

Inspect run history:

```bash
./bin/vectis-cli runs list sequenced-job
./bin/vectis-cli runs show <run-id>
./bin/vectis-cli logs run <run-id>
```

Run tests:

```bash
make test
```

Regenerate protobuf stubs after editing `api/proto/`:

```bash
make proto
```

## Deployment

For local development, use `vectis-local`.

For a fuller reference deployment, Vectis includes a Podman-based stack with Postgres, persistent queue/log storage, Prometheus, Grafana, Jaeger, OpenSearch, and generated local secrets:

```bash
make images-components
./bin/vectis-cli deploy podman up
```

That deployment path is documented in [Reference Deployment Posture](./website/docs/operating/deployment/reference-deployment-posture.md), [Configuration](./website/docs/operating/configuration.md), and [Scaling And Restarts](./website/docs/operating/deployment/scaling-and-restarts.md).

Do not expose the API to untrusted networks without reading [Security](./website/docs/concepts/security.md) and enabling appropriate controls.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development loop, tests, protobuf generation, and conventions.

Architecture decisions live in [ADRs](./website/docs/developing/architecture-decisions/index.md). Compatibility expectations live in [Compatibility](./website/docs/concepts/compatibility.md).

## License

See [LICENSE](LICENSE).
