# Secrets And Redaction

Vectis does not yet provide a job secret backend or a general run-log redaction layer. Operators should treat job definitions, action output, service logs, run logs, backups, and deployment config as potentially sensitive.

## Sensitive Surfaces

| Surface | Risk | Current posture |
| --- | --- | --- |
| API tokens | Token replay if plaintext leaks | Stored hashed; CLI token file is local user config. |
| Bootstrap token | Initial setup authority | Config/deploy secret; remove or rotate after setup where practical. |
| Database DSN | May include credentials | Environment/config secret; avoid logging. |
| Generated deploy secrets | Postgres password, bootstrap token, rendered DSNs | Stored under deploy config directory; protect with filesystem and secret-manager controls. |
| Job definitions | May include URLs, commands, future inputs | Stored in SQL; do not put plaintext secrets in job JSON. |
| Checkout URLs | Credentialed URLs can leak in logs and process args | HTTP(S) checkout URLs with embedded credentials are rejected; logs redact URL userinfo defensively. |
| Shell output | Jobs can echo arbitrary secrets | No general redaction layer; operators must restrict environment and authors. |
| Run logs | May contain credentials, PII, source, or build output | Store, back up, retain, and delete as sensitive data. |
| Service logs | May include config, errors, run IDs, usernames, and paths | Structured logs help routing, but are not a secret scrubber. |

## Operator Guidance

- Do not pass secrets through job JSON, shell commands, clone URLs, or unbounded environment variables.
- Prefer deploy/runtime secret managers and short-lived credentials.
- Run workers with a minimal environment and a dedicated OS/container identity.
- Treat log storage and log backups as sensitive.
- Restrict access to `/metrics`, gRPC, API, OpenSearch, Grafana, Jaeger, and raw log volumes.
- Rotate bootstrap and deploy secrets after initial setup or after any suspected exposure.

## Checkout URL Policy

For `builtins/checkout`, use unauthenticated public URLs or credential-free SSH/SCP-style URLs such as `git@github.com:org/repo.git`. HTTP(S) URLs that include userinfo, such as `https://user:token@example.com/org/repo.git`, are rejected by validation because they can leak through logs, process lists, and persisted job definitions.

## Future Secret Reference Model

Future job definitions should refer to secrets by name, not by value. A minimal model should define:

- Secret scope: namespace, project, or job.
- Allowed consumers: action types and worker pools.
- Injection method: environment, file, or action input.
- Redaction behavior: known exact values, derived values, and non-redactable output.
- Audit events for read/use/rotation.

Until that exists, Vectis should be used only where job authors are trusted not to print or store secrets accidentally.
