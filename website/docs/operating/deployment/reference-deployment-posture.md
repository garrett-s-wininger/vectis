# Reference Deployment Posture

`vectis-cli deploy podman up` starts a full Vectis stack for demos, local staging, and integration testing. It is useful because it shows the pieces working together: API, queue, orchestrator, registry, log service, artifact service, worker core, worker, secrets broker, Vectis SPIFFE authority, cron, reconciler, catalog, docs, Postgres, metrics, traces, logs, and dashboards.

It is not a turnkey production architecture.

For the conservative production-oriented operating target, start with
[Production Topology v1](./production-topology-v1.md). Treat the Podman profile
as a reference map for that topology, not as the production control plane
itself.

## Use It For

| Use case | Fit |
| --- | --- |
| Trying the full stack locally | Good fit. You get the Vectis services plus bundled observability. |
| Demoing Vectis | Good fit. It shows API, jobs, logs, metrics, traces, and dashboards together. |
| Integration testing | Good fit when local Podman is acceptable and data can be recreated. |
| Staging reference | Useful as a topology example, but review secrets, storage, auth, and network exposure. |
| Production | Reference only. Use it as a map, not as the final operating model. |

## Deployment Profiles

The Podman reference deployment has two profiles:

| Profile | Command | Shape |
| --- | --- | --- |
| `simple` | `vectis-cli deploy podman --profile simple up` | Default single-replica cell for local demos and smoke tests, including `vectis-secrets` and `vectis-spiffe`. |
| `ha` | `vectis-cli deploy podman --profile ha up` | Local HA exercise profile with three registries, two API replicas, one orchestrator, two queue shards, two log shards, two artifact shards, two workers, one worker-core, one cell-local secrets broker, one Vectis SPIFFE authority, two cron instances, and two reconciler instances. |

Both profiles keep one Postgres database and the bundled observability stack. The HA profile is for validating the single-cell scale-out contract on one Podman host; it is not a production HA architecture by itself.

## What It Provides

The reference deployment gives you a working single-site topology:

| Area | Included behavior |
| --- | --- |
| Database | Bundled Postgres with generated password and TLS inside the pod. |
| Internal gRPC | Generated CA and server certificate, mounted into Vectis containers. |
| Queue/log/artifact/secrets storage | Persistent volume claims for queue, log, artifact data, encryptedfs secret envelopes, and Vectis SPIFFE CA material. The HA profile gives each queue, log, and artifact shard its own subdirectory on those volumes. |
| Workload identity | `vectis-spiffe` exposes Workload API and Entry API Unix sockets inside the pod so workers can register per-execution SVIDs. |
| Secret resolution | `vectis-secrets` runs with encryptedfs enabled, mounts the generated encryptedfs key, and verifies workload client certificates against the Vectis SPIFFE bundle. |
| Metrics | Prometheus scraping API, queue, orchestrator, worker, log, artifact, secrets, reconciler, catalog, and cell ingress metrics; log-forwarder metrics when deployed. |
| Dashboards | Grafana with a provisioned Vectis overview dashboard. |
| Traces | Jaeger collector/query backed by the bundled OpenSearch instance. |
| Service logs | Structured Vectis logs tailed by Fluent Bit into OpenSearch. |
| Docs | Static Vectis docs published on `8088`. |
| Local secrets | Generated Podman deployment secrets stored under the deploy config directory, including the encryptedfs key rendered into the pod secret. |

This is intentionally convenient. Convenience is the point. The tradeoff is that several pieces are bundled, localhost-published, or generated locally instead of integrated with your production platform.

## What You Still Own

Before treating this topology as shared, staging-like, or production-like, decide how you will handle:

| Responsibility | What to decide |
| --- | --- |
| API authentication | Enable API auth before exposing the API beyond a trusted local environment. |
| Network exposure | Restrict API, Postgres, metrics, gRPC, Grafana, Jaeger, OpenSearch, and Prometheus ports to trusted networks. |
| Secrets | Move generated secrets and encryptedfs material into an operator-controlled secret store and define rotation. |
| Database durability | Use a Postgres backup and restore process you have tested. |
| Storage | Put queue persistence, log storage, artifact storage, encryptedfs secret storage, SPIFFE CA material, and observability storage on volumes with known retention and backup behavior. |
| Telemetry | Replace or harden bundled observability before relying on it for production alerting. |
| Capacity | Set resource requests/limits and size Postgres, queue, orchestrator, log, artifact, secrets, cell ingress, worker-core, and worker capacity for expected load. |
| Runbooks | Install alert rules and repair procedures in the telemetry system operators actually use. |

For the broader security baseline, see [Security](../../concepts/security.md). For internal port boundaries, see [Internal Service Trust](../../concepts/internal-service-trust.md). For configuration details, see [Configuration](../configuration.md).

## Bootstrap Token Lifecycle

The deploy command generates or reuses a bootstrap token in the local deployment secret material. That token only matters when API authentication is enabled and initial setup has not been completed.

Use this lifecycle when API auth is enabled:

1. Start the reference deployment with the generated or operator-provided bootstrap token.
2. Complete API setup.
3. Create durable operator credentials or API tokens.
4. Remove or rotate the bootstrap token where your deployment model allows it.
5. Store recovery credentials in your normal secret manager.

If the bootstrap token is lost before setup completes, rotate or recreate the deploy secret material and restart the API with the new token. If setup already completed, recover through normal operator credentials or database restore. Do not treat the bootstrap token as a standing admin password.

## Demo Defaults To Revisit

The reference pod publishes several local ports so you can inspect the stack quickly. That is helpful on a workstation and risky on an untrusted network.

| Default | Why to revisit it |
| --- | --- |
| API published on `8080` | Enable auth and put the API behind your normal edge/TLS controls before shared use. |
| Docs published on `8088` | Useful for local operators, but still a public HTTP surface if the host is shared. |
| Postgres published on `15432` | Keep database access private and backed up. |
| Grafana on `3000`, Prometheus on `9090`, Jaeger on `16686`, OpenSearch Dashboards on `5601` | Treat bundled observability as demo-grade unless hardened and access-controlled. |
| Generated TLS, SPIFFE, and secret material | Rotate into your platform secret lifecycle for shared environments. |
| Bundled observability storage | Decide retention, backup, and disk pressure behavior before relying on it. |

## HTTP Edge Requirements

The reference pod exposes API and docs listeners directly on host ports. Before putting either listener behind a shared or production-facing hostname, move browser traffic through a controlled HTTPS edge.

| Requirement | Why it matters |
| --- | --- |
| Terminate HTTPS at the edge or serve API/docs HTTPS directly | Browser session cookies are `Secure` and require a browser-facing HTTPS origin. |
| Set `api.session.cookie_secure = true` when API auth is enabled behind HTTPS | Startup validation then matches the intended browser-facing security posture. |
| Configure `api.host_validation.allowed_hosts` for the external API hostname | Host validation should match the public DNS name, not only the pod's listen address. |
| Configure `VECTIS_DOCS_ALLOWED_HOSTS` for the external docs hostname | Docs Host validation should match the public DNS name when docs are published outside localhost. |
| Configure trusted proxy CIDRs only for the proxies that connect to `vectis-api` | Rate limits, audit logs, access logs, HSTS, CORS, and CSRF use trusted forwarded client IP and scheme information. |
| Overwrite forwarded headers at the proxy | Vectis rejects duplicate or malformed `X-Forwarded-*`, `X-Real-IP`, and `Forwarded` headers before route handling. |
| Block direct client access to the API listener around the proxy | Otherwise clients can bypass edge TLS, access controls, and forwarding-header sanitization. |
| Keep API and edge header limits at or below 32 KiB | Oversized headers should be rejected before they consume Vectis handler resources. |
| Preserve streaming for logs and run SSE routes | Disable response buffering and use long enough read timeouts for `/api/v1/sse/...` and `/api/v1/runs/.../logs`; see [SSE And Streaming Reference](../../using/streaming-reference.md). |

Apply the same HTTPS and access-control decision to the docs listener if it is published outside an operator-only network.

## Production Boundary Checklist

Use the reference deployment as a starting checklist, then replace the demo assumptions:

1. Bring your own Postgres or harden the bundled one with TLS, backups, restore drills, and monitored capacity.
2. Enable API auth and complete setup.
3. Put the API behind HTTPS, trusted proxy configuration, allowed-host validation, and access controls.
4. Keep internal gRPC, metrics, cell ingress, worker-control, log, and database ports private; configure cell ingress and metrics allowed Hosts if trusted internal clients must reach them off-host.
5. Use internal TLS or mTLS consistently.
6. Store bootstrap token, API tokens, Postgres password, encryptedfs key, external-provider auth tokens, SPIFFE CA, and TLS keys in a secret manager.
7. Decide whether bundled Prometheus, Grafana, Jaeger, OpenSearch, and Fluent Bit are temporary or production-managed.
8. Put queue persistence, log storage, and observability data on durable volumes with clear retention.
9. Wire health probes to API HTTP health and gRPC health where supported.
10. Install alert rules and runbooks in the production telemetry system.

## Reference Restore Drill

The Podman reference restore drill treats the generated deploy config directory
and named Podman volumes as the backup unit. It covers `vectis-postgres-data`,
`vectis-queue-data`, `vectis-log-data`, `vectis-artifact-data`,
`vectis-secrets-data`, and `vectis-spiffe-data`.

The e2e drill starts the `simple` profile, seeds an encryptedfs smoke secret,
runs a job that writes logs and uploads an artifact, stops the stack, exports
those volumes as platform media, removes the pod resources, imports the volume
archives, restarts the stack, confirms the pre-restore run/log/artifact, and
runs a second secret/log/artifact smoke job. It also records generated Podman
expected-topology evidence plus archive hashes and smoke run IDs.

That is reference-deployment evidence, not a production backup strategy. Shared
or production-like environments should use managed Postgres backups, managed
secret storage, and the storage platform's snapshot/restore process, then keep
the same post-restore health and smoke checks.

## Smoke Test

After `vectis-cli deploy podman up`:

1. Confirm migrations applied successfully.
2. Check API `GET /health/live` and `GET /health/ready`.
3. If API auth is enabled, complete setup and log in with `vectis-cli login`.
4. Create or confirm a small known-safe job.
5. Trigger the job.
6. Confirm the run reaches a terminal state.
7. Stream logs with `vectis-cli logs run <run-id>`.
8. Check Prometheus targets.
9. Open Grafana and confirm the overview dashboard receives fresh samples.
10. Open the bundled docs at `http://localhost:8088`.
11. Check Jaeger and OpenSearch only as reference observability unless you have hardened them.

The Podman reference profile provisions `vectis-spiffe` and the encryptedfs-backed `vectis-secrets` broker. For a smaller workstation smoke test without the Podman observability stack, use the [Local SPIFFE Secrets Smoke Test](./local-spiffe-secrets-smoke-test.md) with `vectis-local`.

## Related Documentation

| Topic | Document |
| --- | --- |
| Configuration and ports | [Configuration](../configuration.md) |
| Production topology | [Production Topology v1](./production-topology-v1.md) |
| Production Linux runbook | [Production Linux Deployment](./production-linux.md) |
| Security posture | [Security](../../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Local SPIFFE secret smoke test | [Local SPIFFE Secrets Smoke Test](./local-spiffe-secrets-smoke-test.md) |
| Scaling and restarts | [Scaling And Restarts](./scaling-and-restarts.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Runbooks and alerts | [Runbooks](../reliability/runbooks.md) |
