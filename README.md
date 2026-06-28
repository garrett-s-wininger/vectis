# Vectis

Vectis is a self-hosted job runner for CI/CD-style workflows and other repeatable automation. You define a job, submit it to the API or CLI, and Vectis queues the work, runs it on a worker, stores run history, and streams logs back to you.

Vectis is developer alpha software: useful today for trying the model, building examples, and developing Vectis itself, but not yet recommended for production or untrusted workloads.

## Quick Start

Build Vectis:

```bash
make build
```

This builds the docs site and embeds it into `vectis-docs`. For a faster local
build without the docs binary, use `SKIP_WEB_BUILD=1 make build`; `vectis-local`
will continue without local docs if `vectis-docs` is not present.

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
| Artifact service | Stores content-addressed artifact blobs. |
| Registry | Lets services find each other locally. |
| Cron | Evaluates schedules. |
| Reconciler | Repairs queued runs that missed queue handoff. |
| Docs | Serves this documentation site from the `vectis-docs` binary. |

By default, the API listens on `http://localhost:8080` and the bundled docs site listens on `http://localhost:8088`. If you need to reach the local stack from another machine, for example over SSH to a dev host, run `./bin/vectis-local --host 0.0.0.0` and use the dev host's address. Only do that on a trusted network. Local data is stored under your user data directory; see [Configuration](./website/docs/operating/configuration.md) for exact paths and overrides.

For local multi-cell routing tests, add execution cells with repeated `--cell` flags:

```bash
./bin/vectis-local --cell pdx-b --cell sjc-c
```

To stop the local stack, press `Ctrl+C` in the terminal running `vectis-local`.

To inspect or remove local state:

```bash
./bin/vectis-cli local reset --dry-run
./bin/vectis-cli local reset --yes
```

## Requirements

- Go `1.25.10+` as declared in [go.mod](go.mod).
- CGO enabled for local SQLite use. This is the normal Go default on most developer machines.
- Node.js `20+` and npm for the default `make build`, which embeds the docs site into `vectis-docs`. Use `SKIP_WEB_BUILD=1 make build` to skip this.
- Mage, `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` for the portable build targets and protobuf regeneration.

Run `scripts/dev-doctor.sh` on POSIX shells or `.\scripts\dev-doctor.ps1` on Windows PowerShell for a preflight check with install guidance. On Unix, `scripts/dev-doctor.sh --install --yes` installs the required toolchain with the local package manager plus repo-local Go, Node.js, Mage, and protobuf tools under `.tools/`; source `.tools/env.sh` afterward. Pass `--install-go-tools` / `-InstallGoTools` to install only the Go-based tools directly.

Portable build targets are moving to Mage. Use `mage -l` to list the targets after running the doctor install and loading the repo-local toolchain environment. The current Makefile delegates `doctor`, `proto`, `build-container`, and `test-quick` to Mage so those targets have one implementation.

To verify the Unix bootstrap from a clean base image, run `scripts/dev-doctor-container.sh`. It copies the repo into a container, runs the installer, and then runs the default smoke target chain; override with `VECTIS_SMOKE_TARGETS='scripts/dev-doctor.sh && make proto' scripts/dev-doctor-container.sh` when you want a narrower check.

## Learn The Basics

The docs site is the best place to continue:

| Start here | When you need |
| --- | --- |
| [Getting Started](./website/docs/getting-started.md) | A slower walkthrough of the local stack and first run. |
| [Your First Job](./website/docs/using/your-first-job.md) | How to write the JSON job definitions Vectis runs today. |
| [Job Definition Reference](./website/docs/using/job-definition-reference.md) | Field-by-field job JSON shape, ports, inputs, secrets, limits, and built-in action settings. |
| [Actions Reference](./website/docs/using/actions-reference.md) | Built-in actions, action descriptors, selectors, digest pins, capabilities, and custom action policy. |
| [Secrets Reference](./website/docs/using/secrets-reference.md) | Job secret references, encryptedfs envelopes, broker policy, file delivery, and redacted troubleshooting signals. |
| [Triggers And Schedules Reference](./website/docs/using/triggers-and-schedules-reference.md) | Manual triggers, replay, cron schedule processing, trigger audit fields, and cron status signals. |
| [Artifacts](./website/docs/using/artifacts.md) | Upload, list, download, operate, and troubleshoot run artifacts. |
| [CLI Guide](./website/docs/using/cli-guide.md) | Everyday `vectis-cli` commands. |
| [API Reference](./website/docs/using/api-reference.md) | HTTP routes, request shapes, auth actions, and error envelopes. |
| [API Error Code Reference](./website/docs/using/api-error-code-reference.md) | Stable API error codes, status meanings, and retry posture. |
| [OpenAPI Specification](./website/docs/using/openapi-specification.md) | Machine-readable v1 HTTP API contract. |
| [SSE And Streaming Reference](./website/docs/using/streaming-reference.md) | SSE routes, payloads, replay controls, reconnect behavior, and proxy notes. |
| [Configuration](./website/docs/operating/configuration.md) | Environment variables, flags, discovery, storage, and TLS settings. |
| [Configuration Key Reference](./website/docs/operating/reference/configuration-key-reference.md) | Embedded defaults, config paths, and env-only runtime knobs. |
| [Run, Task, And Queue State Reference](./website/docs/operating/reference/run-state-reference.md) | Lifecycle states, queue delivery states, repair hints, and operator triage. |
| [Internal gRPC Service Reference](./website/docs/operating/reference/internal-grpc-service-reference.md) | Internal protobuf services, RPC ownership, listeners, discovery, TLS, and compatibility notes. |
| [Upgrade Compatibility Matrix](./website/docs/operating/reference/upgrade-compatibility-matrix.md) | Operator upgrade questions, surface compatibility, rollout order, schema readiness, and rollback choices. |
| [Authorization Reference](./website/docs/operating/reference/authorization-reference.md) | API auth actions, namespace roles, token scopes, and route families. |
| [Audit Event Catalog](./website/docs/operating/reference/audit-event-catalog.md) | Audit event names, metadata fields, durability defaults, and operator signals. |
| [Database Schema](./website/docs/operating/reference/database-schema.md) | SQL tables, fields, constraints, indexes, and operational notes. |
| [Metrics Catalog](./website/docs/operating/reference/metrics-catalog.md) | Prometheus metric names, labels, and operator interpretation. |
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

Try the secrets-backed example after running the local SPIFFE secrets smoke-test setup:

```bash
./bin/vectis-cli jobs run examples/secrets.json --follow
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

For a fuller reference deployment, Vectis includes a Podman-based stack with Postgres, persistent queue/log/artifact/secrets storage, the Vectis SPIFFE authority, bundled docs, Prometheus, Grafana, Jaeger, OpenSearch, and generated local secrets:

```bash
make images-components
./bin/vectis-cli deploy podman up
```

That deployment path is documented in [Reference Deployment Posture](./website/docs/operating/deployment/reference-deployment-posture.md), [Configuration](./website/docs/operating/configuration.md), and [Scaling And Restarts](./website/docs/operating/deployment/scaling-and-restarts.md). For the conservative production-oriented operating target, see [Production Topology v1](./website/docs/operating/deployment/production-topology-v1.md); for the Linux operator flow, see [Production Linux Deployment](./website/docs/operating/deployment/production-linux.md) and [Production Config And Secrets Contract](./website/docs/operating/deployment/production-config-contract.md).

Do not expose the API to untrusted networks without reading [Security](./website/docs/concepts/security.md) and enabling appropriate controls.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development loop, tests, protobuf generation, and conventions.

Architecture decisions live in [ADRs](./website/docs/developing/architecture-decisions/index.md). Compatibility expectations live in [Compatibility](./website/docs/concepts/compatibility.md).

## License

See [LICENSE](LICENSE).
