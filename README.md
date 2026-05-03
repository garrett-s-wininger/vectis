# Vectis

Vectis is a self-hosted build/CI-style system: multiple Go services (API, queue, worker, log, registry, cron, reconciler) coordinated over gRPC and a small REST surface. Jobs are defined as JSON matching the protobuf [`Job`](api/proto/common.proto) graph (`id`, `run_id`, `root` node tree with `uses` / `with` / `steps`).

**Docs:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (as-built components, protocols, data flows). [docs/CONFIGURATION.md](docs/CONFIGURATION.md) (environment variables, flags, discovery). [docs/GLOSSARY.md](docs/GLOSSARY.md) (terms). [docs/PLANNING.md](docs/PLANNING.md) (§1 goals and deploy posture; §2+ roadmap and target vs shipped). [docs/FAILURE_DOMAINS.md](docs/FAILURE_DOMAINS.md) (dependency outages, expectations, current behavior). Deferred multi-site notes: [docs/FEDERATION.md](docs/FEDERATION.md). **ADRs** (design decisions): [docs/adr/README.md](docs/adr/README.md). **Security posture:** [docs/SECURITY.md](docs/SECURITY.md). **Contributing:** [CONTRIBUTING.md](CONTRIBUTING.md).

## Requirements

- [Go](https://go.dev/) **1.25.7+** (see `go.mod`)
- To regenerate protobufs: [Buf](https://buf.build/) — `make proto` uses `npx @bufbuild/buf` by default (`BUF` in the [Makefile](Makefile))

## Quick start

```bash
make build
./bin/vectis-local
```

`vectis-local` starts registry, queue, log, worker, cron, reconciler, and API and initializes the local SQLite schema (see `cmd/local/main.go`). Runtime binaries only **wait** for the schema—they do not migrate. By default it **bootstraps TLS** for internal gRPC (material under your XDG data dir); use **`--grpc-insecure`** for plaintext gRPC.

- **REST API:** `http://localhost:8080` (defaults in [`internal/config/defaults.toml`](internal/config/defaults.toml))
- **Default ports:** API `8080`, queue `8081`, registry `8082`, log gRPC `8083`, log SSE `8084`

### Configuration

Embedded defaults live in [`internal/config/defaults.toml`](internal/config/defaults.toml). Each binary sets a **viper env prefix** (`AutomaticEnv()`): nested config keys use underscores and are prefixed (e.g. API listen port flag/env key `port` → `VECTIS_API_SERVER_PORT`; `api.registry.address` → `VECTIS_API_SERVER_API_REGISTRY_ADDRESS`).

| Binary | Env prefix |
| --- | --- |
| `vectis-api-server` | `VECTIS_API_SERVER` |
| `vectis-queue` | `VECTIS_QUEUE` |
| `vectis-registry` | `VECTIS_REGISTRY` |
| `vectis-worker` | `VECTIS_WORKER` |
| `vectis-cron` | `VECTIS_CRON` |
| `vectis-reconciler` | `VECTIS_RECONCILER` |

The **`[discovery]`** block supplies shared fallbacks (registry URL, optional queue/log resolver pins, `registry_resolver_refresh`). Per-role settings (e.g. `worker.queue.address`, `api.queue.address`) take precedence over `discovery.*` when both are set.

### SQLite data directory

With default config, the DB file is:

`$XDG_DATA_HOME/vectis/db.sqlite3`

If `XDG_DATA_HOME` is unset, that is usually `~/.local/share/vectis/db.sqlite3`.

`vectis-log` also stores run logs durably by default under:

`$XDG_DATA_HOME/vectis/jobs`

Override with `VECTIS_LOG_STORAGE_DIR` when needed.

### Postgres configuration (Podman/Kube)

When using Postgres, set:

- `VECTIS_DATABASE_DRIVER=pgx`
- `VECTIS_DATABASE_DSN=postgres://USER:PASSWORD@HOST:5432/DB?sslmode=disable`

The Pod spec in [`deploy/podman/kube-spec.yaml`](deploy/podman/kube-spec.yaml) wires these env vars for all database-backed services (including `vectis-log`), and provisions persistent volumes for Postgres (`vectis-postgres-data`), queue persistence (`vectis-queue-data`), and durable run logs (`vectis-log-data`). **Internal gRPC** (registry, queue, log, and all clients) uses **TLS**: init container **`vectis-pod-tls-init`** (Alpine) generates a pod-local gRPC CA and server certificate (SAN **localhost** / **127.0.0.1**) into **`vectis-grpc-tls`**, and a **separate Postgres** CA + server cert into **`vectis-postgres-tls`**. Vectis containers mount gRPC material at **`/run/vectis/grpc-tls`** with **`VECTIS_GRPC_TLS_*`** from ConfigMap **`vectis-grpc-tls-env`** (see [docs/CONFIGURATION.md](docs/CONFIGURATION.md) §Internal gRPC TLS). Database clients in the pod mount **`ca.pem`** only and use **`sslmode=verify-full`** in **`VECTIS_DATABASE_DSN`**. The **`postgres`** container enables **`ssl=on`** with those PEM files. The init container runs **`apk add openssl`** and needs **outbound network** on first pull of packages if the image layer is cold. The same spec includes **Prometheus** and **Grafana** scraping **`/metrics`**: **queue**, **worker**, and **log** use **HTTPS** on their metrics ports (**`VECTIS_METRICS_TLS_*`**, same leaf PEMs as gRPC); **Prometheus** mounts the gRPC CA and scrapes with **`tls_config.ca_file`**. **API** `/metrics` stays **HTTP** on **8080** until a separate API TLS story exists (ports in [docs/CONFIGURATION.md](docs/CONFIGURATION.md); overview dashboard under `deploy/grafana/dashboards/`). The Podman bundle runs **Jaeger 2.17.0** in split mode (**collector + query**) with an in-pod OpenSearch backend; Vectis services export OTLP to **`http://127.0.0.1:4318`** (collector), and the Jaeger UI is published on **http://localhost:16686**. Vectis service logs are JSON on stderr for `podman logs`, mirrored into a shared JSONL volume, shipped by Fluent Bit into the same OpenSearch instance as daily `vectis-logs-*` indices, and viewable through OpenSearch Dashboards on **http://localhost:5601** or the Grafana **OpenSearch Logs** data source. Regenerate embedded Grafana ConfigMaps with **`make grafana-kube-configmaps`** after editing dashboards.

For a simple admin workflow, use:

```bash
make deploy-podman
```

`make deploy-podman` runs `podman play kube --replace`, then `./bin/vectis-cli migrate` on the **host** against Postgres on **`127.0.0.1` and the `hostPort` published in the spec** (default **15432**, see `postgres` ports in [`deploy/podman/kube-spec.yaml`](deploy/podman/kube-spec.yaml)). **Postgres** in the pod serves **TLS** (`ssl=on`); in-pod services use **`sslmode=verify-full`** with the CA from the shared volume. The default **`VECTIS_DATABASE_DSN`** for **`migrate`** uses **`sslmode=require`** so the CLI encrypts without a local CA file. Override `VECTIS_POSTGRES_HOST_PORT` / `VECTIS_DATABASE_DSN` if you change the published port, credentials, or TLS posture.

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

Application-level API authentication is **off** in the default stack (`api.auth.enabled=false`); you can enable it with environment or config—see [docs/CONFIGURATION.md](docs/CONFIGURATION.md) and [docs/SECURITY.md](docs/SECURITY.md). Do not expose the API to untrusted networks without appropriate controls.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for tests, protobuf generation, and running individual binaries.

## License

See [LICENSE](LICENSE).
