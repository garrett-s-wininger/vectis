# Secrets And Redaction

Vectis does not yet provide a job secret backend or a general run-log redaction layer. Treat job definitions, action output, service logs, run logs, database backups, queue persistence, and deployment config as potentially sensitive.

This page is for operators deciding where secrets may appear and how to reduce accidental exposure.

For the broader security posture, see [Security](../../concepts/security.md). For reference deployment secret handling, see [Reference Deployment Posture](./reference-deployment-posture.md). For backup inventory, see [Backup And Restore](../reliability/backup-restore.md).

## Current Posture

| Capability | Current behavior |
| --- | --- |
| API token storage | API tokens are stored hashed in the database. Plaintext API tokens are only shown when created. Login session tokens are returned only when a non-browser client requests `return_token`. |
| Password storage | Local user passwords are stored as bcrypt hashes. |
| Bootstrap token | Used for initial setup when API auth is enabled. It remains a deploy/config secret until removed or rotated. |
| Checkout URL validation | HTTP(S) checkout URLs with embedded user info are rejected. |
| Log redaction | There is no general-purpose run-log redaction layer. Jobs can print secrets. |
| Job secret backend | Not shipped today. Job definitions should not contain plaintext secrets. |
| Execution identity | Workers can derive an expected per-execution SPIFFE ID for future secret policy, but Vectis does not yet fetch SPIRE SVIDs or release secrets from that identity. |
| Storage encryption | Vectis relies on platform disk, volume, database, and backup encryption. |

## Sensitive Surfaces

| Surface | Why it matters | Operator handling |
| --- | --- | --- |
| API tokens | Plaintext tokens allow API access until revoked or expired. | Store in user config or a secret manager. Revoke and recreate after exposure. |
| CLI token file | Lets local CLI commands authenticate as the saved user. | Protect OS user config permissions; remove with `vectis-cli auth logout` when no longer needed. |
| Bootstrap token | Can complete initial setup on a fresh authenticated deployment. | Rotate or remove after setup where practical; do not treat as a standing admin password. |
| Database DSN | Often contains Postgres credentials. | Keep in secret stores or protected env; avoid logging. |
| Generated deploy secrets | Include Postgres password, bootstrap token, and rendered DSNs. | Protect the deploy config directory; rotate into platform-managed secrets for shared environments. |
| Job definitions | Persisted in SQL and may include commands, URLs, and action inputs. | Do not put plaintext credentials in job JSON. |
| Checkout URLs | Credentialed URLs can leak through persisted definitions, logs, or process surfaces. | Use public URLs or credential-free SSH/SCP-style URLs. |
| Shell output | Jobs can echo arbitrary environment, files, credentials, or source data. | Restrict job authors, worker environments, and mounted secrets. |
| Run logs | May contain credentials, PII, source, or build output. | Store, retain, back up, and delete as sensitive data. |
| Service logs | May include paths, usernames, run IDs, config errors, and operational context. | Structured logs help routing, but they are not a scrubber. |
| Backups | Can contain database records, logs, queue state, tokens hashes, and deploy secrets. | Apply the same access controls and retention policy as production data. |

## What Not To Put In Jobs

Avoid putting secrets in:

- job JSON;
- shell command text;
- HTTP(S) clone URLs;
- action inputs;
- long-lived worker environment variables visible to all jobs;
- files mounted into workers unless every job author on that worker is trusted to read them.

If a job needs credentials today, prefer short-lived credentials delivered by your runtime environment and limit which workers can see them. Vectis currently assumes job authors are trusted not to print, persist, or exfiltrate those values.

When `worker.execution_identity.enabled=true`, workers fail closed for jobs that lack a Vectis execution envelope and derive an expected `spiffe://` identity for accepted executions. That identity is not a secret and is not placed in shell environment variables. Use it to plan secret-store policy and SPIRE registration paths; do not mount a broad SPIRE Workload API socket directly into untrusted job processes unless the runtime isolation layer can ensure the job receives only its own scoped SVID.

## Checkout URL Policy

For `builtins/checkout`, use unauthenticated public URLs or credential-free SSH/SCP-style URLs:

```text
git@github.com:org/repo.git
```

HTTP(S) URLs with user info are rejected:

```text
https://user:token@example.com/org/repo.git
```

Those URLs are rejected because they can leak through persisted job definitions, logs, process arguments, and debugging output. Vectis also defensively redacts URL user info in logs, but validation is the primary protection.

## Operator Baseline

Use this baseline for shared or production-like deployments:

1. Enable API authentication before exposing the API.
2. Store database DSNs, bootstrap tokens, API tokens, deploy secrets, and TLS keys in a secret manager.
3. Rotate the bootstrap token after setup where your deployment model allows it.
4. Restrict who can create or update job definitions.
5. Run workers with the smallest practical environment and identity.
6. Keep queue persistence, run logs, service logs, database backups, and deploy config directories private.
7. Restrict direct access to log service, metrics, gRPC, OpenSearch, Grafana, Jaeger, and raw volumes.
8. Treat log retention and backup retention as security decisions, not only storage decisions.
9. Revoke API tokens and rotate deploy secrets after suspected exposure.

## If A Secret Leaks

| Leaked item | First response |
| --- | --- |
| API token | Delete/revoke the token and create a new one. Review audit logs for suspicious use. |
| CLI token file | Remove the file or run `vectis-cli auth logout`; revoke the server-side token if exposure is possible. |
| Bootstrap token before setup | Rotate or recreate deploy secret material and restart the API with the new token. |
| Bootstrap token after setup | Rotate/remove it from runtime config where practical; setup state in the DB limits its future use. |
| Database DSN or Postgres password | Rotate database credentials and update every DB-using service. |
| TLS private key | Rotate the key/certificate pair and update every service that trusts it. |
| Secret in run logs | Rotate the secret, restrict log access, and apply your log-retention/deletion process. |
| Secret in job definition | Rotate the secret, update/delete the job definition, and inspect backups or exports that may contain it. |

## Related Documentation

| Topic | Document |
| --- | --- |
| Security posture | [Security](../../concepts/security.md) |
| Internal trust boundaries | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Configuration and secret variables | [Configuration](../configuration.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Backup inventory and restore | [Backup And Restore](../reliability/backup-restore.md) |
