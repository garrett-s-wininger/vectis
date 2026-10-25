# Contributing to Vectis

## Pull Requests

Vectis is not accepting outside pull requests yet. The design is still settling, and the public feature surface is deliberately small while the project hardens.

Everything below is still useful if you are building from source, maintaining a fork, or working inside the project.

## Start Here

For a normal local development loop, install the toolchain first, then build and start:

```bash
scripts/dev-doctor.sh --install --yes
./.tools/bin/mage build
./bin/vectis-local
```

`./.tools/bin/mage` uses the doctor-installed Go. `mage build` is the local Go lane. Run `mage buildFull` when the loop needs the browser UI or docs server, after `scripts/dev-doctor.sh --install-frontend`. On Windows, start from the PowerShell doctor in [Developer Environment](#developer-environment).

In another terminal, after `vectis-local` reports ready:

```bash
./bin/vectis-cli health check
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

`vectis-local` creates the first admin and saves the CLI token. A terminal prompts for the username and password. A non-interactive start writes the generated password under the local data directory. Pass `--setup=off` to leave that setup for the bootstrap token. That confirms the binaries build, the local stack starts, the API accepts an authenticated client, and a worker can execute a job.

## Prerequisites

- Go `1.25.11+`, matching [go.mod](go.mod).
- Git.
- CGO enabled for the default local SQLite build. This is the normal Go default on most developer machines.

Optional tools:

- Node.js `20.19+` and npm when working on docs/UI frontend lanes.
- `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc` when editing files under [api/proto/](api/proto/).
- Podman when using the reference deployment commands.
- Java and `/opt/tla+/tla2tools.jar` when running formal verification.

## Developer Environment

The docs-site copy of this guide lives in [Development Environment](./website/docs/developing/development-environment.md).

Run the doctor script for your platform before the first build and whenever the local toolchain changes:

```bash
scripts/dev-doctor.sh
```

```powershell
.\scripts\dev-doctor.ps1
```

The doctor checks Go, Git, Mage, protobuf tooling, SQLite/CGO readiness, optional frontend tooling, optional container/formal tools, and Windows symlink support. Use `scripts/dev-doctor.sh --install --yes` on Unix-like systems to install the standard local Go toolchain. `./.tools/bin/mage` uses that Go without a PATH change; source `.tools/env.sh` when you want `go`, `mage`, and `protoc` as bare commands in the current shell. Add `--install-frontend` when you also want repo-local Node.js for docs/UI work. Use `--install-go-tools` or `-InstallGoTools` when you only need Mage and protobuf Go plugins installed through `go install`.

### Unix

The normal Unix setup is intentionally boring:

```bash
scripts/dev-doctor.sh --install --yes
. .tools/env.sh
mage testQuick
```

If you are working on docs or the browser UI, add the frontend toolchain:

```bash
scripts/dev-doctor.sh --install-frontend
. .tools/env.sh
mage buildFrontend
```

Local SQLite tests need CGO and a C compiler. On common Linux distributions the doctor installer pulls in GCC and libc development headers. macOS developers should have Xcode Command Line Tools available.

To verify the Unix bootstrap from a clean base image, run `scripts/dev-doctor-container.sh`. It copies the repo into a container, runs the installer, and then runs the default smoke target chain. Override with `VECTIS_SMOKE_TARGETS='scripts/dev-doctor.sh && mage proto' scripts/dev-doctor-container.sh` for a narrower check.

Most Unix developers can use the system default temp directory. If your `/tmp` is remote, encrypted, very slow, or mounted with restrictions that affect tests, create a fast local temp directory and opt in for test commands:

```bash
mkdir -p "$HOME/.cache/vectis/tmp"
export VECTIS_TEST_TEMPDIR="$HOME/.cache/vectis/tmp"
mage testQuick
```

### Windows

Use PowerShell for the supported Windows development lane:

```powershell
.\scripts\dev-doctor.ps1
```

For local SQLite tests, Go's CGO path needs a GCC-compatible C compiler. Use MinGW/UCRT GCC from MSYS2 or LLVM `clang` in GCC-compatible mode. MSVC `cl.exe` and `clang-cl` are not supported by this SQLite CGO lane. After installing a compiler, open a new PowerShell session or refresh `PATH` before rerunning the doctor.

Enable Windows Developer Mode, or run from an elevated shell, so checkout-cache tests can create directory symlinks. Without symlink permission those tests skip the symlink-dependent path, which is useful for a partial check but not the full Windows development signal.

The current Windows compile baseline is:

```powershell
mage testWindowsCompile
mage buildWindows
```

Both use the `nosqlite` build tag and default to `windows/amd64`. Override `WINDOWS_GOARCH` or `WINDOWS_OUT_DIR` when needed. `mage testWindowsSQLiteCompile` compile-checks the native SQLite/CGO lane with the GCC-compatible compiler described above. On Unix, that SQLite cross-compile can use Zig when `zig` is on `PATH`.

For best test performance, use a Dev Drive or another fast local filesystem for the repository, Go caches, and optional Vectis test temp directory. Example:

```powershell
go env -w GOCACHE=D:\Caches\go-build
go env -w GOMODCACHE=D:\Caches\go-mod
New-Item -ItemType Directory -Force D:\Caches\tmp | Out-Null
$env:VECTIS_TEST_TEMPDIR = 'D:\Caches\tmp'
mage testQuick
```

`VECTIS_TEST_TEMPDIR` is opt-in and cross-platform. When set, Mage passes it to Go tests as `GOTMPDIR`, `TEMP`, `TMP`, and `TMPDIR`, which keeps Go scratch files and `os.TempDir()` users on the chosen filesystem. When unset, tests use the normal OS temp location. Set it to `0`, `off`, or `false` to disable the override in a shell that inherited it. To persist it for future PowerShell sessions:

```powershell
[Environment]::SetEnvironmentVariable('VECTIS_TEST_TEMPDIR', 'D:\Caches\tmp', 'User')
```

## Build

```bash
mage build
```

This writes the Vectis binaries to `bin/`. `mage -l` lists the portable targets after the doctor install. Source `.tools/env.sh` when you want `mage` as a bare command.

The default build is Go-only: it builds the backend services, local supervisor,
SCM trigger services, workers, and CLI without running npm.

Frontend lanes are explicit:

```bash
mage buildDocs     # docs assets + vectis-docs
mage buildUI       # browser UI assets + vectis-ui
mage buildFrontend # docs + UI assets and serving binaries
mage buildFull     # default Go binaries plus docs/UI assets and binaries
```

`vectis-local` starts `vectis-ui` and `vectis-docs` when those binaries are
present, and logs a warning while continuing without them when they are absent.

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

On Windows, `mage testQuick` defaults to a longer timeout because filesystem and process creation are slower than on Unix. Keep `GOCACHE`, `GOMODCACHE`, and optionally `VECTIS_TEST_TEMPDIR` on a fast local filesystem for the best feedback loop.

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

Run `mage buildFull` instead when you want the local browser UI and docs servers
available from the same stack.

For local multi-cell routing, add execution cells with repeated `--cell` flags:

```bash
./bin/vectis-local --cell pdx-b --cell sjc-c
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
| First local run | [Getting Started](./website/docs/getting-started.md) |
| Job JSON | [Your First Job](./website/docs/using/your-first-job.md) |
| Job definition fields | [Job Definition Reference](./website/docs/using/job-definition-reference.md) |
| Built-in and custom actions | [Actions Reference](./website/docs/using/actions-reference.md) |
| Secrets | [Secrets Reference](./website/docs/using/secrets-reference.md) |
| Triggers and schedules | [Triggers And Schedules Reference](./website/docs/using/triggers-and-schedules-reference.md) |
| Artifacts | [Artifacts](./website/docs/using/artifacts.md) |
| CLI | [CLI Guide](./website/docs/using/cli-guide.md) |
| HTTP API | [API Reference](./website/docs/using/api-reference.md) |
| API error codes | [API Error Code Reference](./website/docs/using/api-error-code-reference.md) |
| OpenAPI | [OpenAPI Specification](./website/docs/using/openapi-specification.md) |
| SSE | [SSE And Streaming Reference](./website/docs/using/streaming-reference.md) |
| Current system shape | [Architecture](./website/docs/concepts/architecture.md) |
| Config, env, storage, TLS | [Configuration](./website/docs/operating/configuration.md) |
| Config keys | [Configuration Key Reference](./website/docs/operating/reference/configuration-key-reference.md) |
| Run and queue state | [Run, Task, And Queue State Reference](./website/docs/operating/reference/run-state-reference.md) |
| Internal gRPC | [Internal gRPC Service Reference](./website/docs/operating/reference/internal-grpc-service-reference.md) |
| Upgrades | [Upgrade Compatibility Matrix](./website/docs/operating/reference/upgrade-compatibility-matrix.md) |
| Authorization | [Authorization Reference](./website/docs/operating/reference/authorization-reference.md) |
| Audit events | [Audit Event Catalog](./website/docs/operating/reference/audit-event-catalog.md) |
| Database schema | [Database Schema](./website/docs/operating/reference/database-schema.md) |
| Metrics | [Metrics Catalog](./website/docs/operating/reference/metrics-catalog.md) |
| Failure behavior | [Failure Domains](./website/docs/concepts/failure-domains.md) |
| Security model | [Security](./website/docs/concepts/security.md) |
| Compatibility promises | [Compatibility](./website/docs/concepts/compatibility.md) |
| Database migrations | [Migrations](./website/docs/developing/migrations.md) |
| Release discipline | [Releases](./website/docs/developing/releases.md) |
| Reference deployment | [Reference Deployment Posture](./website/docs/operating/deployment/reference-deployment-posture.md) |
| Scaling and restarts | [Scaling And Restarts](./website/docs/operating/deployment/scaling-and-restarts.md) |
| Production topology | [Production Topology v1](./website/docs/operating/deployment/production-topology-v1.md) |
| Production Linux | [Production Linux Deployment](./website/docs/operating/deployment/production-linux.md) |
| Production config contract | [Production Config And Secrets Contract](./website/docs/operating/deployment/production-config-contract.md) |
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
