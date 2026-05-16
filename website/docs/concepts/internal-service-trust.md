# Internal Service Trust

Vectis internal services communicate over gRPC and HTTP. This page explains which service-to-service paths exist, what each path can do, and how to protect them in a self-hosted deployment.

For the overall security posture, see [Security](./security.md). For startup and outage behavior, see [Failure Domains](./failure-domains.md). For exact environment variables and ports, see [Configuration](../operating/configuration.md).

## Trust Model

Vectis currently relies on three layers for internal service trust:

| Layer | What it provides | What it does not provide |
| --- | --- | --- |
| Network placement | Keeps queue, registry, log, worker-control, and metrics endpoints away from untrusted clients. | It does not identify a caller once the caller is on the trusted network. |
| TLS or mTLS | Encrypts internal gRPC traffic and can verify certificates when configured. | Vectis does not yet map certificate identities to per-RPC authorization rules. |
| Per-run cancel token | Lets the API prove it is cancelling the run that a worker currently owns. | It is not a general worker-control authentication system. Worker-control reachability still matters. |

Internal gRPC servers do not currently enforce application-level service authorization. Optional mTLS can verify peer certificates, but Vectis does not yet say "this certificate may call this RPC and that one may not."

## Deployment Defaults

| Deployment shape | Default trust posture |
| --- | --- |
| Standalone binaries | Internal gRPC may be plaintext by default. Use only on trusted hosts or networks unless TLS is configured. |
| `vectis-local` | Bootstraps local development TLS unless `--grpc-insecure` is used. It is a development supervisor, not a production isolation boundary. |
| Podman reference deployment | Generates internal gRPC and metrics TLS material. Treat it as a reference deployment with demo assumptions unless you replace secrets and lock down exposure. |
| Production | Keep internal ports private. Use TLS or mTLS on shared networks. Restrict metrics and log access. |

## Service Paths

| Caller | Calls | Purpose | Trust note |
| --- | --- | --- | --- |
| API | Queue | Dispatch runs. | Queue access can create work; keep queue reachable only from producers and workers. |
| API | Log service | Read and stream run logs. | Logs may contain sensitive job output. |
| API | Registry | Resolve queue, log, and worker addresses when discovery is used. | Registry controls where clients dial next. |
| API | Worker-control | Request cancellation of a currently running run. | Requires worker resolution and the run's cancel token; still keep worker-control private. |
| Worker | Queue | Dequeue, ack, and recover deliveries. | Queue access can consume and affect work delivery. |
| Worker | Log service or local log-forwarder | Send job log chunks. | Log ingest is part of normal execution. |
| Worker | Registry | Resolve queue/log and publish worker-control address when registration is enabled. | Worker registration enables remote cancel routing. |
| Cron | Queue | Enqueue scheduled runs. | Cron is a producer; protect it like other enqueue paths. |
| Cron | Registry | Resolve queue when discovery is used. | Pin queue address if you want to avoid this dependency. |
| Reconciler | Queue | Redispatch queued runs that missed queue handoff. | Reconciler can reintroduce work to the queue. |
| Reconciler | Registry | Resolve queue when discovery is used. | Pin queue address if you want to avoid this dependency. |
| Queue | Registry | Publish queue address when registration is enabled. | Consumers trust this address for future dials. |
| Log service | Registry | Publish log address when registration is enabled. | Consumers trust this address for future dials. |
| Metrics scraper | API, queue, worker, log, reconciler metrics listeners | Observe service health and pressure. | Metrics are unauthenticated; scrape from trusted networks only. |

## Ports To Keep Private

| Surface | Default | Why it is sensitive |
| --- | --- | --- |
| API HTTP | `8080` | Public entry point. Expose only with auth, TLS or edge controls, and rate-limit posture understood. |
| Queue gRPC | `8081` | Accepts enqueues and serves deliveries. |
| Registry gRPC | `8082` | Provides internal service addresses. |
| Log gRPC | `8083` | Accepts job log chunks. |
| Log HTTP/SSE | `8084` | Serves run logs. Prefer access through API/RBAC when possible. |
| Worker-control gRPC | `9084` by default in static mode | Accepts run cancellation requests for the currently owned run. |
| Queue metrics | `9081` | Exposes operational state and traffic shape. |
| Worker metrics | `9082` | Exposes worker health, outcomes, and pressure. |
| Log metrics | `9083` | Exposes log ingest and stream pressure. |
| Reconciler metrics | `9085` | Exposes repair activity and dependency failures. |

Worker-control can also use an ephemeral port or a configured port range. When workers register with the registry, the published worker-control address is what the API uses for remote cancellation.

## TLS And mTLS

Internal gRPC TLS is controlled by `VECTIS_GRPC_TLS_*` settings:

| Setting group | Purpose |
| --- | --- |
| `VECTIS_GRPC_TLS_INSECURE=false` | Enables TLS for internal gRPC clients and servers. |
| `VECTIS_GRPC_TLS_CA_FILE` | Lets clients verify internal service certificates. |
| `VECTIS_GRPC_TLS_CERT_FILE` and `VECTIS_GRPC_TLS_KEY_FILE` | Let servers present a certificate. |
| `VECTIS_GRPC_TLS_CLIENT_CA_FILE` | Lets servers require and verify client certificates. |
| `VECTIS_GRPC_TLS_CLIENT_CERT_FILE` and `VECTIS_GRPC_TLS_CLIENT_KEY_FILE` | Let clients present a certificate for mTLS. |
| `VECTIS_GRPC_TLS_SERVER_NAME` | Sets the name clients verify in the server certificate. Useful when discovery resolves to an address that differs from the certificate name. |

Metrics TLS uses `VECTIS_METRICS_TLS_*` for dedicated metrics listeners. API metrics are served on the API HTTP listener.

Use TLS or mTLS when internal traffic crosses shared infrastructure. Still keep internal ports private, because mTLS identity is not yet an application authorization policy inside Vectis.

## Registry And Pinned Addresses

Registry discovery is convenient, but it also becomes a trust dependency: clients rely on registry answers for where to send queue, log, and worker-control traffic.

Use pinned addresses when you want to remove registry from a process startup path:

| Goal | Approach |
| --- | --- |
| Worker should not need registry to find queue/log. | Pin queue and log addresses in worker configuration. |
| API, cron, or reconciler should not need registry to find queue. | Pin the queue address for those processes. |
| Queue/log should not need registry at startup. | Disable registration for those services and configure consumers with fixed addresses. |
| API should cancel running jobs through workers. | Ensure workers publish reachable worker-control addresses, usually through registry. |

For outage behavior with and without registry, see [Failure Domains](./failure-domains.md#registry-down).

## Remote Cancel Boundary

Remote cancel uses the worker-control gRPC service:

1. A client calls `POST /api/v1/runs/{id}/cancel` or `vectis-cli runs cancel <run-id>`.
2. The API checks namespace authorization and loads the running run's lease owner and cancel token from the database.
3. The API resolves the worker-control address for the lease owner.
4. The API sends `CancelRun` to that worker with the run ID and cancel token.
5. The worker accepts the request only if it is currently executing that run and the token matches.

This protects against cancelling the wrong run, but the worker-control endpoint should still be private. It is a control path, and a reachable control path is operationally sensitive even with per-run tokens.

## Production Baseline

Use this baseline for shared or production-like environments:

1. Keep queue, registry, log gRPC, log HTTP, worker-control, and metrics listeners off public networks.
2. Enable internal gRPC TLS or mTLS when services run across shared hosts, clusters, or networks.
3. Pin addresses when you do not want registry to be a startup dependency.
4. Allow only expected callers to reach each internal surface with network policy, firewall rules, or equivalent controls.
5. Scrape metrics from a trusted network; do not expose metrics directly to users.
6. Prefer API/RBAC-mediated log access over direct log HTTP access.
7. Rotate generated TLS material and deployment secrets using your platform's normal secret lifecycle.
