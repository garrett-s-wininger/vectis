# Contributing to Vectis

## Pull Requests

Vectis is not accepting outside pull requests yet. The design is still settling, and the public feature surface is deliberately small while the project hardens.

Everything below is still useful if you are building from source, maintaining a fork, or working inside the project.

## Start Here

For a normal local development loop:

```bash
mage build
./bin/vectis-local
```

In another terminal:

```bash
./bin/vectis-cli health check
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

That confirms the binaries build, the local stack starts, the API is reachable, and a worker can execute a job.

## Prerequisites

- Go `1.25.10+`, matching [go.mod](go.mod).
- Git.
- CGO enabled for the default local SQLite build. This is the normal Go default on most developer machines.

Optional tools:

- `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` when editing files under [api/proto/](api/proto/).
- Podman when using the reference deployment commands.
- Java and `/opt/tla+/tla2tools.jar` when running formal verification.

## Build

```bash
mage build
```

This writes the Vectis binaries to `bin/`.

The default build also builds the docs site and embeds it into `vectis-docs`.
Use `SKIP_WEB_BUILD=1 mage build` for a faster local build without the docs
binary; `vectis-local` will continue without local docs when that binary is
absent.

For container-oriented static builds:

```bash
mage buildContainer
```

Container builds disable SQLite with `CGO_ENABLED=0` and `-tags=nosqlite`, so they use the Postgres driver path.

## Test

Use the smallest test command that gives useful feedback:

```bash
mage testQuick       # fast unit feedback
mage testFault       # targeted injected-failure checks
mage testProperty    # generated invariant checks
mage test            # all default Go tests
mage testIntegration # integration tests with the integration build tag
mage testRace        # race detector
mage lintAPIRoutes   # parser-backed security lint for public API route opt-outs
mage lint            # route security lint plus golangci-lint
```

For a narrow package loop:

```bash
go test ./internal/api/...
```

API auth fuzz targets are available when you need them:

```bash
mage fuzzAPIAuth
go test -fuzz=FuzzBearerToken -fuzztime=1m ./internal/api
```

## Format And Dependencies

```bash
mage format
```

This runs the repository's formatting and module cleanup workflow.

## Protobuf

Source `.proto` files live in [api/proto/](api/proto/). Generated Go lives in [api/gen/go/](api/gen/go/) and should not be edited by hand.

After changing protobufs:

```bash
mage proto
```

Commit the generated files with the proto change.

## Running Services

Use `vectis-local` for the normal local stack:

```bash
mage build
./bin/vectis-local
```

For the Podman reference deployment:

```bash
mage imagesComponents
./bin/vectis-cli deploy podman up
```

For single-service debugging, build first, then run the matching binary from `bin/`. Each service's flags and startup behavior live under `cmd/<name>/main.go`; shared defaults live in [internal/config/defaults.toml](internal/config/defaults.toml). For operator-facing configuration, use [Configuration](./website/docs/operating/configuration.md).

If you change dashboard JSON under [deploy/grafana/dashboards/](deploy/grafana/dashboards/), regenerate the Podman ConfigMap bundle:

```bash
mage podmanGrafanaConfigmaps
```

## Where To Change Things

| Area | Start here |
| --- | --- |
| HTTP API, auth, RBAC | `internal/api/` |
| SQL schema and repositories | `internal/migrations/`, `internal/dal/` |
| gRPC contracts | `api/proto/`, then `mage proto` |
| Queue and queue client behavior | `internal/queue/`, `internal/queueclient/` |
| Worker execution and actions | `internal/job/`, `internal/action/`, `cmd/worker/` |
| Log ingest and forwarding | `internal/logserver/`, `internal/logforwarder/` |
| Service discovery | `internal/registry/`, `internal/resolver/` |
| Cron and repair | `internal/cron/`, `internal/reconciler/` |
| Docs site | `website/docs/`, `website/src/css/custom.css`, `website/sidebars.js` |
| Reference deployment | `deploy/` |

## Documentation And Design

Use the docs site as the durable home for design and operator-facing behavior:

| Need | Document |
| --- | --- |
| Current system shape | [Architecture](./website/docs/concepts/architecture.md) |
| Config, env, storage, TLS | [Configuration](./website/docs/operating/configuration.md) |
| Failure behavior | [Failure Domains](./website/docs/concepts/failure-domains.md) |
| Security model | [Security](./website/docs/concepts/security.md) |
| Compatibility promises | [Compatibility](./website/docs/concepts/compatibility.md) |
| Database migrations | [Migrations](./website/docs/developing/migrations.md) |
| Release discipline | [Releases](./website/docs/developing/releases.md) |
| Product direction | [Planning](./website/docs/developing/roadmap/planning.md) |
| Major decisions | [Architecture Decision Records](./website/docs/developing/architecture-decisions/index.md) |

When behavior changes, update the doc closest to the affected reader. User-facing CLI or API behavior belongs under `Using Vectis`; deployment and repair behavior belongs under `Operating Vectis`; maintainer process belongs under `Developing Vectis`.

## Before A Change Is Ready

For internal work or forked contributions, aim for:

- Focused changes that match nearby package style.
- Tests appropriate to the risk and surface area.
- `mage proto` and committed generated Go when protobufs change.
- Migration notes and release notes when schema behavior changes.
- Docs updates when API, CLI, config, deployment, security, metrics, capacity, or runbook behavior changes.

When outside contributions open later, this section will become the pull request checklist.
