# Secrets And Redaction

Vectis provides a job secret path through the cell-local `vectis-secrets` broker with encrypted filesystem and Knox providers, but it does not provide a general run-log redaction layer. Treat job definitions, action output, service logs, run logs, database backups, queue persistence, secret stores, key files, and deployment config as potentially sensitive.

This page is for operators deciding where secrets may appear and how to reduce accidental exposure.

For the broader security posture, see [Security](../../concepts/security.md). For the job secret contract, see [Secrets Reference](../../using/secrets-reference.md). For the production secret inventory and required config groups, see [Production Config And Secrets Contract](./production-config-contract.md). For the hardening checklist, see [Production Security Checklist](./production-security-checklist.md). For reference deployment secret handling, see [Reference Deployment Posture](./reference-deployment-posture.md). For an end-to-end local SPIFFE secret-resolution exercise, see [Local SPIFFE Secrets Smoke Test](./local-spiffe-secrets-smoke-test.md). For backup inventory, see [Backup And Restore](../reliability/backup-restore.md).

## Current Posture

| Capability | Current behavior |
| --- | --- |
| API token storage | API tokens are stored hashed in the database. Plaintext API tokens are only shown when created. Login session tokens are returned only when a non-browser client requests `return_token`. |
| Password storage | Local user passwords are stored as bcrypt hashes. |
| Bootstrap token | Used for initial setup when API auth is enabled. It remains a deploy/config secret until removed or rotated. |
| Checkout URL validation | HTTP(S) checkout URLs with embedded user info are rejected. |
| Log redaction | There is no general-purpose run-log redaction layer. Jobs can print secrets. |
| Job secret backend | `vectis-secrets` can resolve top-level job secret references from encrypted filesystem envelopes or a configured Knox service and deliver task-scoped files under `.vectis/secrets`. Operators create encryptedfs envelopes with `vectis-cli secrets encryptedfs put` or manage Knox keys in Knox; the broker denies resolution unless an operator access-policy rule matches the execution scope and ref. Resolve logs, metrics, run detail, run task security events, and multi-cell catalog fan-in expose fixed outcome/reason/provider/count metadata without claim tokens, secret plaintext, delivery paths, or requested refs. Job definitions should not contain plaintext secrets. |
| Execution identity | Workers can derive an expected per-execution SPIFFE ID, and SPIFFE-enabled workers require their Workload API source to return a matching X.509-SVID before action code runs. Secret resolution uses that SVID as the mTLS client certificate, and the broker requires it to match the active execution. Run detail, task security events, and multi-cell catalog fan-in record only the SVID check outcome and reason, not SVID material. |
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
| Shell environment | Process-launching built-ins receive a minimal Vectis-built environment, not the worker service environment. | Do not rely on worker env vars for job secrets; use a secret-aware delivery path when one exists. |
| Shell output | Jobs can echo arbitrary environment, files, credentials, or source data. | Restrict job authors, worker file mounts, and any credentials deliberately injected into jobs. |
| Run logs | May contain credentials, PII, source, or build output. | Store, retain, back up, and delete as sensitive data. |
| Service logs | May include paths, usernames, run IDs, config errors, and operational context. | Structured logs help routing, but they are not a scrubber. |
| Backups | Can contain database records, logs, queue state, tokens hashes, and deploy secrets. | Apply the same access controls and retention policy as production data. |

## What Not To Put In Jobs

Avoid putting secrets in:

- job JSON;
- shell command text;
- HTTP(S) clone URLs;
- action inputs;
- long-lived worker environment variables as a job secret-delivery mechanism;
- files mounted into workers unless every job author on that worker is trusted to read them.

If a job needs credentials today, prefer short-lived credentials delivered by your runtime environment and limit which workers can see them. Vectis does not pass the worker service environment through to shell or checkout child processes, but files, sockets, and volumes visible to the worker runtime can still be visible to job commands. Vectis currently assumes job authors are trusted not to print, persist, or exfiltrate deliberately provided credentials.

When `worker.execution_identity.enabled=true`, workers fail closed for jobs that lack a Vectis execution envelope and derive an expected `spiffe://` identity for accepted executions. With `worker.spiffe.enabled=true`, the worker also requires its configured SPIFFE Workload API source to return an X.509-SVID whose SPIFFE ID exactly matches that derived identity before action code runs. When `worker.spiffe.registration.enabled=true`, the worker can create and renew matching SPIFFE Entry API registrations through a protected local Unix socket before it asks the Workload API for the SVID. When the task declares secrets, Vectis uses that SVID only inside worker-controlled code as the mTLS client certificate for `vectis-secrets`; the broker derives the expected identity from the active execution record and requires a match. `vectis-cli runs show <run-id>` points failed gate runs at `next_action=security_gate_failed`, a redacted `latest_failed_security_event`, and retry guidance. `vectis-cli runs tasks <run-id>` can show redacted per-attempt SVID and secret-resolution outcomes for deeper troubleshooting, but those rows contain only event type, outcome, reason, provider kind, and counts. Multi-cell catalog fan-in copies the same redacted shape into the global run catalog. The derived identity is not a secret, and Vectis does not place the identity, SVID, private key, Workload API socket, SPIFFE Entry API socket, or registration authority credentials in shell environment variables. Use it to plan secret-store policy and SPIFFE registration paths; do not mount broad SPIFFE Workload API or SPIFFE Entry API sockets directly into untrusted job processes.

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

For `builtins/gerrit-review`, put the Gerrit HTTP password in a task-scoped
secret file and pass that workspace-relative path through `password_file`.
Do not place the password in the job definition or Gerrit URL.

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
| Job secret contract | [Secrets Reference](../../using/secrets-reference.md) |
| Configuration and secret variables | [Configuration](../configuration.md) |
| Production config contract | [Production Config And Secrets Contract](./production-config-contract.md) |
| Production security checklist | [Production Security Checklist](./production-security-checklist.md) |
| Local SPIFFE secret smoke test | [Local SPIFFE Secrets Smoke Test](./local-spiffe-secrets-smoke-test.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
| Backup inventory and restore | [Backup And Restore](../reliability/backup-restore.md) |
