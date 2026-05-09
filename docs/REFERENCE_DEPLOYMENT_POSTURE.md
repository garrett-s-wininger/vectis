# Reference Deployment Posture

`vectis-cli deploy podman up` is a reference deployment for demos, local staging, and integration testing. It is not a turnkey production architecture.

## Posture Matrix

| Environment | Fit | Expected changes before real use |
| --- | --- | --- |
| Local dev | Good default for trying the full stack | Keep data disposable; use `vectis-cli reset --dry-run` before cleanup. |
| Podman demo | Good for showing API, worker, logs, metrics, traces, and dashboards together | Treat generated secrets and bundled observability as demo material unless you manage them like production secrets. |
| Staging/reference | Useful as a repeatable topology | Enable API auth, set explicit backup/restore process, review resource limits, and wire real alert routing. |
| Production | Reference only | Bring your own Postgres, backup system, telemetry backends, network policy, TLS material, secret rotation, and capacity plan. |

## Bootstrap Token Lifecycle

1. Generate or reuse the deploy bootstrap token through the deploy command's secret material.
2. Start the reference deployment.
3. Complete API setup with the bootstrap token.
4. Create durable operator credentials or tokens.
5. Remove the bootstrap token from runtime environment where the deployment model allows it, or rotate/re-render secrets and restart the API.
6. Store recovery credentials in the operator's secret manager.

If the bootstrap token is lost before setup completes, rotate/recreate the deploy secret material and restart the API with the new token. If setup already completed, recover through normal operator credentials or the database restore process; do not rely on the bootstrap token as a standing admin password.

## Demo-Only Defaults

The reference pod includes Prometheus, Grafana, Jaeger, OpenSearch, and Fluent Bit to make the system observable immediately. Any bundled or default observability credentials, single-pod storage, and localhost-published ports should be treated as non-production defaults.

Before exposing the deployment beyond a trusted workstation or staging network:

- Enable HTTP API auth.
- Restrict API, metrics, gRPC, database, OpenSearch, Grafana, Jaeger, and Prometheus ports to trusted networks.
- Rotate generated secrets into an operator-controlled secret store.
- Use durable Postgres backups.
- Put queue persistence, log storage, and observability storage on volumes with known retention and backup behavior.
- Define resource requests/limits outside the demo pod shape.

## Production Boundary Checklist

- BYO Postgres with TLS, backups, restore drills, and monitored capacity.
- BYO telemetry backends or a hardened telemetry deployment.
- Network policy or firewall rules for every internal gRPC and metrics listener.
- API auth enabled and setup completed.
- Internal TLS or mTLS configured consistently.
- Secret rotation process for bootstrap token, API tokens, Postgres password, and TLS keys.
- Health probes wired to API HTTP health and gRPC service health where supported.
- Alert rules and runbooks installed in the production telemetry system.

## Podman Deploy Smoke Test

After `vectis-cli deploy podman up`:

1. Confirm migrations applied successfully.
2. Check API `GET /health/live` and `GET /health/ready`.
3. Complete setup if auth is enabled.
4. Log in with `vectis-cli login`.
5. Create or confirm a small known-safe job.
6. Trigger the job.
7. Confirm the run reaches a terminal state.
8. Stream logs with `vectis-cli logs run <run-id>`.
9. Check Prometheus targets.
10. Open Grafana and confirm the overview dashboard receives fresh samples.
11. Check Jaeger/OpenSearch only as demo observability unless you have hardened them.
