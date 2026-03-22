# Vectis

Vectis is a self-hosted build/CI-style system: multiple Go services (API, queue, worker, log, registry, cron, reconciler) coordinated over gRPC and a small REST surface. Jobs are defined as JSON matching the protobuf [`Job`](api/proto/common.proto) graph (`id`, `run_id`, `root` node tree with `uses` / `with` / `steps`).

**Docs:** [PLANNING.md](PLANNING.md) (architecture, roadmap, target vs shipped). Deferred multi-site notes: [docs/FEDERATION.md](docs/FEDERATION.md). **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md).

## Requirements

- [Go](https://go.dev/) **1.25.7+** (see `go.mod`)
- To regenerate protobufs: [Buf](https://buf.build/) — `make proto` uses `npx @bufbuild/buf` by default (`BUF` in the [Makefile](Makefile))

## Quick start

```bash
make build
./bin/vectis-local
```

`vectis-local` migrates SQLite, then starts registry, queue, log, worker, cron, reconciler, and API (see `cmd/local/main.go`).

- **REST API:** `http://localhost:8080` (defaults in [`internal/config/defaults.toml`](internal/config/defaults.toml))
- **Default ports:** API `8080`, queue `8081`, registry `8082`, log gRPC `8083`, log SSE `8084`

### SQLite data directory

With default config, the DB file is:

`$XDG_DATA_HOME/vectis/db.sqlite3`

If `XDG_DATA_HOME` is unset, that is usually `~/.local/share/vectis/db.sqlite3`.

### CLI

`./bin/vectis-cli` talks to the API (create/list/trigger jobs, stream logs, etc.). Run `./bin/vectis-cli --help` for commands.

## Shipped REST (summary)

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` / `POST` | `/api/v1/jobs` | List / create job definitions |
| `GET` / `PUT` / `DELETE` | `/api/v1/jobs/{id}` | Get / update / delete definition |
| `POST` | `/api/v1/jobs/run` | Run from JSON body |
| `POST` | `/api/v1/jobs/trigger/{id}` | New run from stored definition |
| `GET` | `/api/v1/jobs/{id}/runs` | List runs |
| `GET` | `/api/v1/sse/jobs/{id}/runs` | SSE for run events |

There is no authentication on the API in the default stack; do not expose it untrusted networks.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for tests, protobuf generation, and running individual binaries.

## License

See [LICENSE](LICENSE).
