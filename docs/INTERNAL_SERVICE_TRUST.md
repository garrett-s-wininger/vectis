# Internal Service Trust

Vectis internal services communicate over gRPC. Standalone defaults favor local development; production deployments must define network boundaries and TLS posture explicitly.

## Current Trust Assumptions

| Deployment shape | Current assumption |
| --- | --- |
| Standalone binaries | gRPC may be plaintext by default; run only on trusted hosts/networks unless TLS is configured. |
| `vectis-local` | Bootstraps local TLS unless disabled; intended for development, not production isolation. |
| Podman reference deploy | Generates internal gRPC and metrics TLS material; still a reference topology with demo assumptions. |
| Production | Operators should keep internal ports private, use TLS or mTLS where networks are shared, and restrict metrics/log access. |

Internal gRPC servers do not currently enforce application-level service authorization. Optional mTLS verifies peer certificates when configured, but Vectis does not yet map certificate identities to per-RPC allow/deny rules.

## Service Identity Matrix

| Caller | Calls | Purpose |
| --- | --- | --- |
| API | Queue, log, registry when discovery is used | Enqueue runs, stream logs, discover services. |
| Worker | Queue, log, registry when discovery is used | Dequeue/ack work, stream logs, discover services. |
| Cron | Queue, registry when discovery is used | Enqueue scheduled runs. |
| Reconciler | Queue, registry when discovery is used | Redispatch queued runs. |
| Queue | Registry when registration is enabled | Publish queue address. |
| Log | Registry when registration is enabled | Publish log address. |
| CLI/admin | API; database for `migrate`; deploy-local services during bootstrap | Operator workflows and migrations. |
| Metrics scraper | API/queue/worker/log/reconciler metrics HTTP | Observability only. |

## Ports To Keep Private

| Surface | Default | Exposure guidance |
| --- | --- | --- |
| Queue gRPC | 8081 | Internal only: API, workers, cron, reconciler. |
| Registry gRPC | 8082 | Internal only when discovery is used. |
| Log gRPC | 8083 | Internal only: workers and API. |
| Log HTTP/SSE | 8084 | Restrict to trusted clients or proxy through API/RBAC. |
| Queue metrics | 9081 | Prometheus or trusted scraper only. |
| Worker metrics | 9082 | Prometheus or trusted scraper only. |
| Log metrics | 9083 | Prometheus or trusted scraper only. |
| Reconciler metrics | 9085 | Prometheus or trusted scraper only. |
| API HTTP | 8080 | Expose only with auth, TLS/edge controls, and rate-limit posture understood. |

## Production Recommendations

- Prefer private networks plus TLS for all gRPC traffic.
- Use mTLS when internal networks are shared with untrusted workloads.
- Keep metrics endpoints off public networks; they are not authenticated.
- Pin queue/log addresses when you do not want registry to be a startup dependency.
- Treat worker-control reachability as sensitive even when per-run cancel tokens are used.
- Rotate TLS material using the configured reload intervals or process restarts.

## Future Internal RPC Checklist

Before adding an internal RPC, document:

- Which caller identities need it.
- Whether it is safe on plaintext development networks only.
- Whether mTLS identity should be required in production.
- What data or control capability the RPC exposes.
- Which port and deployment boundary protect it.
- Which metrics/logs reveal misuse or failure.
- How it behaves during rolling upgrades.

## Open Design Choices

- Whether production should require mTLS rather than recommend it.
- Whether Vectis should add application-level service tokens.
- Whether certificate identities should map to per-RPC authorization rules.
