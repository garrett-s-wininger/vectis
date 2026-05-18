# Reference Deployment Posture

`vectis-cli deploy podman up` starts a full Vectis stack for demos, local staging, and integration testing. It is useful because it shows the pieces working together: API, queue, registry, log service, worker, cron, reconciler, docs, Postgres, metrics, traces, logs, and dashboards.

It is not a turnkey production architecture.

## Use It For

| Use case | Fit |
| --- | --- |
| Trying the full stack locally | Good fit. You get the Vectis services plus bundled observability. |
| Demoing Vectis | Good fit. It shows API, jobs, logs, metrics, traces, and dashboards together. |
| Integration testing | Good fit when local Podman is acceptable and data can be recreated. |
| Staging reference | Useful as a topology example, but review secrets, storage, auth, and network exposure. |
| Production | Reference only. Use it as a map, not as the final operating model. |

## What It Provides

The reference deployment gives you a working single-site topology:

| Area | Included behavior |
| --- | --- |
| Database | Bundled Postgres with generated password and TLS inside the pod. |
| Internal gRPC | Generated CA and server certificate, mounted into Vectis containers. |
| Queue/log storage | Persistent volume claims for queue and log data. |
| Metrics | Prometheus scraping API, queue, worker, log, and reconciler metrics. |
| Dashboards | Grafana with a provisioned Vectis overview dashboard. |
| Traces | Jaeger collector/query backed by the bundled OpenSearch instance. |
| Service logs | Structured Vectis logs tailed by Fluent Bit into OpenSearch. |
| Docs | Static Vectis docs published on `8088`. |
| Local secrets | Generated Podman deployment secrets stored under the deploy config directory. |

This is intentionally convenient. Convenience is the point. The tradeoff is that several pieces are bundled, localhost-published, or generated locally instead of integrated with your production platform.

## What You Still Own

Before treating this topology as shared, staging-like, or production-like, decide how you will handle:

| Responsibility | What to decide |
| --- | --- |
| API authentication | Enable API auth before exposing the API beyond a trusted local environment. |
| Network exposure | Restrict API, Postgres, metrics, gRPC, Grafana, Jaeger, OpenSearch, and Prometheus ports to trusted networks. |
| Secrets | Move generated secrets into an operator-controlled secret store and define rotation. |
| Database durability | Use a Postgres backup and restore process you have tested. |
| Storage | Put queue persistence, log storage, and observability storage on volumes with known retention and backup behavior. |
| Telemetry | Replace or harden bundled observability before relying on it for production alerting. |
| Capacity | Set resource requests/limits and size Postgres, queue, log, and worker capacity for expected load. |
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
| Generated TLS and secret material | Rotate into your platform secret lifecycle for shared environments. |
| Bundled observability storage | Decide retention, backup, and disk pressure behavior before relying on it. |

## Production Boundary Checklist

Use the reference deployment as a starting checklist, then replace the demo assumptions:

1. Bring your own Postgres or harden the bundled one with TLS, backups, restore drills, and monitored capacity.
2. Enable API auth and complete setup.
3. Put the API behind HTTPS, trusted proxy configuration, and access controls.
4. Keep internal gRPC, metrics, worker-control, log, and database ports private.
5. Use internal TLS or mTLS consistently.
6. Store bootstrap token, API tokens, Postgres password, and TLS keys in a secret manager.
7. Decide whether bundled Prometheus, Grafana, Jaeger, OpenSearch, and Fluent Bit are temporary or production-managed.
8. Put queue persistence, log storage, and observability data on durable volumes with clear retention.
9. Wire health probes to API HTTP health and gRPC health where supported.
10. Install alert rules and runbooks in the production telemetry system.

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

## Related Documentation

| Topic | Document |
| --- | --- |
| Configuration and ports | [Configuration](../configuration.md) |
| Security posture | [Security](../../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Scaling and restarts | [Scaling And Restarts](./scaling-and-restarts.md) |
| Backup and restore | [Backup And Restore](../reliability/backup-restore.md) |
| Runbooks and alerts | [Runbooks](../reliability/runbooks.md) |
