# Contributing to Vectis

## Pull requests

We are **not accepting pull requests** right now. The design is still settling and the feature surface is deliberately small; we want both to stabilize and grow before taking outside contributions.

Everything below still applies if you are building from source, experimenting locally, or maintaining a fork.

## Prerequisites

- **Go** version matching [`go.mod`](go.mod) (currently 1.25.7+)
- **Git**
- **SQLite** for the default local database stack: `github.com/mattn/go-sqlite3` links against the system `libsqlite3`. Schema is applied from embedded SQL under [`internal/migrations/sqlite/`](internal/migrations/sqlite/) (baseline `001_initial`).

Optional:

- **Buf** for protobuf codegen — `make proto` runs `npx @bufbuild/buf` unless you set `BUF` to a local `buf` binary in the environment or [Makefile](Makefile).

## Build

```bash
make build
```

Outputs `bin/vectis-api`, `bin/vectis-cli`, `bin/vectis-cron`, `bin/vectis-local`, `bin/vectis-log`, `bin/vectis-queue`, `bin/vectis-reconciler`, `bin/vectis-registry`, `bin/vectis-worker`.

Static binaries (for containers, etc.):

```bash
make build-static
```

Container images ([`build/Containerfile`](build/Containerfile)) use **`make build-container`** with **`CGO_ENABLED=0`** and **`-tags=nosqlite`**, so binaries link **pgx only** (Postgres). That produces a **static enough** binary for `scratch` (no C toolchain: pure Go does not need musl/gcc). Local `make build` still includes SQLite via CGO. To build the same way locally: `CGO_ENABLED=0 make build-container`.

## Tests

```bash
make test              # all packages: go test ./...
make test-integration  # integration tests: go test -tags=integration ./...
make test-race         # race detector: go test -race ./...
```

Scoped tests:

```bash
go test ./internal/api/...
```

## Formatting and modules

```bash
make format   # go fix, go fmt, go mod tidy
```

## Protobuf / gRPC

Sources live under [`api/proto/`](api/proto/). Generated Go is under [`api/gen/go/`](api/gen/go/) (do not hand-edit).

After changing `.proto` files:

```bash
make proto
```

Requires a working Buf invocation (`npx` + network on first run, unless Buf is installed locally and `BUF` is overridden).

## Running services

**Full stack (typical):**

```bash
make build
./bin/vectis-local
```

For Postgres (Podman/Kube), use `make deploy-podman`.

**Single service** (for debugging): run the matching binary from `bin/` after `make build`. Each `cmd/<name>/main.go` defines flags and startup; components discover queue/log addresses via **registry** when that pattern is used (see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) or [docs/PLANNING.md](docs/PLANNING.md) §2).

Ensure SQLite’s parent directory exists if you open the DB outside `vectis-local` (see `database.OpenDB` / `GetDBPath`).

## Configuration

Embedded defaults: [`internal/config/defaults.toml`](internal/config/defaults.toml). Some binaries also honor environment variables (`VECTIS_*` where wired); prefer reading the relevant `cmd/*/main.go` and `internal/config` for truth.

## Design and roadmap

**As-built** architecture: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Large design decisions and **target vs shipped** behavior are documented in [docs/PLANNING.md](docs/PLANNING.md). Prefer updating PLANNING (or [docs/FEDERATION.md](docs/FEDERATION.md) for deferred multi-site material) instead of duplicating long design text in this guide; keep topology/protocol details in ARCHITECTURE when they change.

## When pull requests are welcome

Once we open the project to contributions, we will expect:

- `make test` (and `make test-integration` if you touch integration surfaces).
- `make proto` and committed `api/gen/go/` when `.proto` files change.
- Focused commits that match existing style and naming in the packages you touch.
