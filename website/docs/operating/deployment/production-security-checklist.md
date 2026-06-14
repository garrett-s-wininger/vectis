# Production Security Checklist

Use this checklist before exposing a production-v1 deployment to users or
shared infrastructure. It complements the broader [Security](../../concepts/security.md)
and [Internal Service Trust](../../concepts/internal-service-trust.md) pages.

Record the completed checklist in the production readiness evidence record.

## API Edge

| Check | Required posture |
| --- | --- |
| API auth | `VECTIS_API_AUTH_ENABLED=true` outside throwaway local use. |
| Bootstrap token | Set only for initial setup, store in a secret manager, and remove or rotate after setup where practical. |
| HTTPS | Serve HTTPS directly or terminate it at a trusted edge. Browser-facing API access must be HTTPS. |
| Secure cookies | Set `VECTIS_API_SESSION_COOKIE_SECURE=true` for auth-enabled edge TLS deployments. |
| Allowed Hosts | Set `VECTIS_API_ALLOWED_HOSTS` to the external DNS names users will use. |
| Trusted proxy CIDRs | Set `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS` only to the proxy networks that inject trusted forwarding headers. |
| Rate-limit posture | Confirm rate-limit settings, API security rejection metrics, and edge limits are visible. |
| Direct listener exposure | Block untrusted direct access to the API listener when traffic should enter through the edge. |

Trusted proxy headers do not replace API auth or HTTPS. They only let Vectis
trust selected forwarding metadata from approved proxy peers.

## Internal Service Network

Keep these surfaces private:

- registry gRPC;
- queue gRPC;
- orchestrator gRPC;
- log gRPC and direct log HTTP/SSE;
- artifact gRPC;
- secrets gRPC;
- cell ingress;
- worker-control gRPC;
- dedicated metrics listeners;
- Postgres;
- raw queue, log, artifact, secret, SPIFFE, and workspace volumes.

Use network policy, host firewalls, private subnets, service mesh policy, or
equivalent controls. Direct log and artifact service access should be an
operator path; normal user access should go through API authorization.

## TLS, mTLS, And Service Identity

| Check | Required posture |
| --- | --- |
| Internal gRPC | Use `VECTIS_GRPC_TLS_INSECURE=false` when traffic crosses hosts, networks, or trust boundaries. |
| Server certs | Configure `VECTIS_GRPC_TLS_CERT_FILE` and `VECTIS_GRPC_TLS_KEY_FILE` for internal servers. |
| Client verification | Configure `VECTIS_GRPC_TLS_CLIENT_CA_FILE` on listeners that should require client certificates. |
| Client certs | Configure `VECTIS_GRPC_TLS_CLIENT_CERT_FILE` and `VECTIS_GRPC_TLS_CLIENT_KEY_FILE` for clients in mTLS deployments. |
| Server name | Set `VECTIS_GRPC_TLS_SERVER_NAME` when discovery or pinned addresses do not match certificate names. |
| Metrics TLS | Use `VECTIS_METRICS_TLS_INSECURE=false` with metrics cert/key when scraping across untrusted links. |
| Service allowlists | Set `VECTIS_SERVICE_IDENTITY_*_ALLOWED_*` for listener roles that should accept only known SPIFFE URI SANs. |

Service identity allowlists require verified mTLS. They are defense in depth
for listener roles, not a replacement for keeping internal ports private.

## Secrets And SPIFFE

| Check | Required posture |
| --- | --- |
| Secret manager | Store API bootstrap token, API tokens, Postgres credentials, TLS keys, SPIFFE CA material, encryptedfs keys, and recovery credentials outside the repository. |
| Secret policy | Keep `VECTIS_SECRETS_POLICY_ALLOW` narrow by namespace, job, task, and ref. Avoid broad wildcard policies in production. |
| Execution identity | Enable and test worker execution identity before using Vectis-mediated job secrets. |
| Workload API socket | Keep the SPIFFE Workload API socket private to worker-controlled code. |
| Registration socket | Keep the SPIFFE Entry API or registration socket private to trusted worker service code. |
| CA material | Back up and rotate SPIFFE authority material through the operator's identity lifecycle. |
| Secret delivery | Prefer task-scoped file delivery. Do not pass job secrets through job JSON, command text, or worker service environment variables. |

The derived execution SPIFFE ID is not secret, but the SVID private key,
Workload API socket, registration socket, encryptedfs key, and secret envelopes
are sensitive.

## Files, Volumes, And Backups

| Path or data | Required posture |
| --- | --- |
| `/etc/vectis/*.env` | Owned by `root:vectis`, mode `0640` or stricter. |
| TLS private keys | Readable only by the service identity that needs them. |
| `/etc/vectis/secrets/*` | Not world-readable; managed by secret tooling where possible. |
| Queue persistence | Durable, private, backed up when queue restore is required. |
| Log storage | Durable, private, retained and deleted according to policy. |
| Artifact storage | Durable, private, backed up when artifact restore is required. |
| Secret envelopes | Durable, private, backed up with matching encryptedfs keys. |
| SPIFFE CA data | Durable, private, backed up and rotated deliberately. |
| Worker workspaces | Private to workers and cleaned according to retention policy. |
| Backups | Protected like production data and covered by retention policy. |

Vectis does not provide general at-rest encryption for database rows, queue
persistence, logs, artifact blobs, or backups. Use platform storage encryption
and access controls.

## Workers And Job Authors

| Check | Required posture |
| --- | --- |
| Worker environment | Keep worker service environment small; it is not a job secret-delivery channel. |
| Host execution | Treat host execution as compatibility mode, not a sandbox. |
| Untrusted jobs | Use VM isolation and document the worker/provider boundary before accepting untrusted workloads. |
| Volumes and sockets | Do not mount broad host credentials, secret stores, SPIFFE sockets, or registry sockets into job-visible paths. |
| Job authoring | Restrict who can create or update jobs that run on privileged or credentialed workers. |
| Logs | Assume jobs can print secrets unless job authors and injected credentials are controlled. |

Worker restart and cancellation behavior is an operational boundary, not a
security sandbox. Keep worker-control private even though cancel requests carry
per-run tokens.

## Evidence To Record

Capture these items before handoff:

- API edge URL, allowed Hosts, trusted proxy CIDRs, and auth state;
- internal surfaces exposed by firewall or network policy;
- TLS/mTLS mode and certificate authority ownership;
- service identity allowlists in use;
- secret-manager paths or references without plaintext values;
- file and volume ownership checks;
- SPIFFE/secrets smoke result when enabled;
- `vectis-cli health check --strict` output;
- known security waivers, owners, and expiration dates.

## Related Documentation

| Topic | Document |
| --- | --- |
| Security posture | [Security](../../concepts/security.md) |
| Internal service trust | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Production environment template | [Production Environment Template](./production-env-template.md) |
| Production config contract | [Production Config And Secrets Contract](./production-config-contract.md) |
| Secrets and redaction | [Secrets And Redaction](./secrets-and-redaction.md) |
| Trusted proxy headers | [Trusted Proxy Headers](./trusted-proxy-client-ip.md) |
