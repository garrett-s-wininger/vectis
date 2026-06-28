# Secrets Reference

Vectis job secrets are task-scoped file material resolved by the cell-local `vectis-secrets` broker. A job definition carries secret references, delivery instructions, and optional task scope; it does not carry plaintext secret values.

The current implementation has one delivery type, `file`, and one built-in provider, `encryptedfs`. The worker asks the broker to resolve only the references visible to the current task, writes the returned files under `.vectis/secrets`, sets `VECTIS_SECRETS_DIR` for the action process, and removes the directory after the task finishes.

For the deployment security model and sensitive-surface inventory, see [Secrets And Redaction](../operating/deployment/secrets-and-redaction.md). For the full configuration key catalog, see [Configuration Key Reference](../operating/reference/configuration-key-reference.md).

## Job Definition Shape

Top-level `secrets` entries live beside `id` and `root`:

```json
{
  "id": "secret-example",
  "secrets": [
    {
      "id": "npm-token",
      "ref": "encryptedfs://team/npm-token",
      "delivery": {
        "type": "file",
        "path": "npm/token"
      },
      "task_keys": ["publish"]
    }
  ],
  "root": {
    "id": "publish",
    "uses": "builtins/shell",
    "with": {
      "command": "npm publish --//registry.npmjs.org/:_authToken=\"$(cat \"$VECTIS_SECRETS_DIR/npm/token\")\""
    }
  }
}
```

## Fields

| Field | Required | Contract |
| --- | --- | --- |
| `secrets` | No | Top-level array of secret references for the job. |
| `id` | Yes | Secret identifier. It must start with a letter or underscore, then contain only letters, numbers, underscores, dots, or dashes. IDs must be unique within the job. |
| `ref` | Yes | Provider URI with a scheme, such as `encryptedfs://team/npm-token`. Embedded credentials are rejected. |
| `delivery` | Yes | Delivery object. Vectis currently supports only file delivery. |
| `delivery.type` | Yes | Use `file` in JSON. The protobuf enum value is `SECRET_DELIVERY_TYPE_FILE`. |
| `delivery.path` | Yes for `file` | Relative slash-separated path below the worker secrets directory. Absolute paths, backslashes, empty segments, `.`, and `..` are rejected. |
| `task_keys` | No | Optional node IDs allowed to receive the secret. Every value must match a job node `id`. |

If `task_keys` is omitted or empty, the secret is visible to every task in the job. If `task_keys` is present, only those task executions are allowed to receive the reference. The root task key also matches the root node's `id`, so a root node can be scoped by its explicit node ID.

## File Delivery

The broker returns `SecretFileMaterial` records with `id`, `path`, `data`, and `mode`. The worker materializes those records below the workspace-relative `.vectis/secrets` directory:

| Behavior | Detail |
| --- | --- |
| Environment | `VECTIS_SECRETS_DIR` is added to the action process only when secret files are materialized. |
| Directory mode | The secrets directory and parent directories are created with mode `0700`. |
| File mode | File material defaults to mode `0400`; returned modes are masked to permission bits. |
| Duplicate paths | Materialization uses exclusive create, so two secret files resolving to the same `delivery.path` fail the task. |
| Cleanup | The worker removes `.vectis/secrets` before materializing and defers cleanup after the task completes. |
| Ambient env | Shell and checkout actions do not receive the worker service environment as a secret-delivery mechanism. |

Actions should read the file they need from `VECTIS_SECRETS_DIR`. Vectis does not inject secret values directly into environment variables.

## Encryptedfs Provider

`encryptedfs` is the built-in provider scheme. A ref such as `encryptedfs://team/npm-token` maps to an encrypted envelope below the broker's configured provider root, for example `<root>/team/npm-token`.

| Contract | Detail |
| --- | --- |
| Ref scheme | `encryptedfs` |
| Ref path | Host and path are joined into one relative slash-separated path. For example, `encryptedfs://team/npm-token` resolves to `team/npm-token`. |
| Rejected ref parts | Credentials, query strings, fragments, absolute paths, backslashes, empty path segments, `.`, and `..`. |
| Envelope format | JSON envelope, version `1`, algorithm `AES-256-GCM`. |
| Key size | `32` bytes. Key files may contain raw 32 bytes, hex, standard or URL-safe base64, raw base64, or a 32-character string. |
| Secret size | Plaintext is limited to `1048576` bytes (`1 MiB`) by default. |
| Envelope files | CLI-created envelope files are written with mode `0600`; parent directories are created with mode `0700`. |

Operators can create or update an envelope with the CLI:

```sh
./bin/vectis-cli secrets encryptedfs put encryptedfs://team/npm-token \
  --from-file npm-token.txt \
  --root /var/lib/vectis/secrets/envelopes \
  --key-file /etc/vectis/secrets/encryptedfs.key
```

By default, `put` reads plaintext from stdin when `--from-file` is omitted or set to `-`, refuses to overwrite an existing envelope, and requires `--key-file`. Add `--create-key` to create a missing encryptedfs key file, or `--force` to overwrite an existing envelope.

## Resolution Flow

Secret resolution is private worker-to-broker gRPC:

1. The worker claims a task execution and obtains an `execution_claim_token`.
2. The worker selects references with `ReferencesForTask` based on the current task key.
3. The worker requires a workload X.509-SVID when `worker.secrets.address` is enabled.
4. The worker sends a `ResolveSecretsRequest` to the `ResolveSecrets` RPC on `SecretsService` with `run_id`, `execution_id`, `execution_claim_token`, and the selected `secrets`.
5. The broker reads the mTLS peer SPIFFE URI SAN, validates the active execution claim, re-derives execution scope from the database, evaluates secret access policy, and resolves provider material.
6. The worker materializes returned files and runs the action.

The protobuf response is `ResolveSecretsResponse`; its `files` array contains `SecretFileMaterial` records with `id`, `path`, `data`, and `mode`.

## Authorization

`vectis-secrets` authorizes every resolve request before provider access:

| Gate | Requirement |
| --- | --- |
| mTLS peer identity | The caller must present a client certificate with a SPIFFE URI SAN. |
| Execution identity binding | `run_id`, `execution_id`, the peer SPIFFE ID, and the broker-derived execution scope must match. |
| Active claim | `execution_claim_token` must validate for the active execution. |
| Access policy | Every requested `ref` must match at least one allow rule. |

An active execution claim requires the run itself to still be `running`; orphaned, terminal, expired, or otherwise non-running runs cannot receive new secret material.

Secret access policy is default-deny. Configure allow rules with repeated `--allow-secret`, `VECTIS_SECRETS_POLICY_ALLOW`, or `secrets.policy.allow`.

Rules are semicolon-separated `key=value` parts:

```text
namespace=/teams/build;job=release;task=publish;ref=encryptedfs://teams/build/npm-token
```

| Key | Alias | Match behavior |
| --- | --- | --- |
| `namespace` | `namespace_path` | Normalized with a leading `/`; supports exact match, `*`, and trailing-prefix `*`. |
| `job` | `job_id` | Exact match, `*`, or trailing-prefix `*`. |
| `task` | `task_key` | Exact match, `*`, or trailing-prefix `*`. |
| `ref` | `secret`, `secret_ref` | Exact match, `*`, or trailing-prefix `*`; required in every rule. |

`namespace`, `job`, and `task` default to `*` when omitted. Unknown keys and duplicate keys are invalid.

## Service Configuration

These are the main knobs that connect job secret resolution. The full list of flags, environment variables, config paths, and defaults is in [Configuration Key Reference](../operating/reference/configuration-key-reference.md).

| Component | Key | Purpose |
| --- | --- | --- |
| Worker | `worker.secrets.address` / `VECTIS_WORKER_SECRETS_ADDRESS` / `--secrets-address` | Address of `vectis-secrets`. Empty, `disabled`, `none`, `off`, or `-` disables broker-backed resolution. |
| Secrets broker | `secrets.encryptedfs.root` / `VECTIS_SECRETS_ENCRYPTEDFS_ROOT` / `--encryptedfs-root` | Root directory that stores encryptedfs envelope files. |
| Secrets broker | `secrets.encryptedfs.key_file` / `VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE` / `--encryptedfs-key-file` | Key file used to decrypt encryptedfs envelopes. |
| Secrets broker | `secrets.policy.allow` / `VECTIS_SECRETS_POLICY_ALLOW` / `--allow-secret` | Default-deny access-policy allow rules. |
| Worker and secrets broker | `worker.execution_identity.*` | Shared settings used to derive the expected execution SPIFFE ID. Required when encryptedfs is configured. |
| Worker | `worker.spiffe.enabled` and `worker.spiffe.*` | SPIFFE Workload API and optional registration settings used to obtain the execution X.509-SVID. |
| Internal gRPC | `grpc_tls.*` | Secret resolution requires mTLS in practice; `vectis-secrets` needs `grpc_tls.client_ca_file` so workload client certificates are requested and verified. |
| Secrets gRPC listener | `service_identity.secrets_allowed_client_identities` | Optional static exact-match client identity allowlist. Keep it empty for dynamic per-execution SVID callers unless deliberately adding a second gate. |

When the encryptedfs provider is configured, `vectis-secrets` refuses to start unless `worker.execution_identity.enabled` is `true` and the execution identity config is valid. Workers that resolve job secrets need a matching execution X.509-SVID for the task.

## Observability

Secret resolution emits redacted service metrics, service logs, and run security events. These surfaces expose outcome, reason, provider kind, and counts; they do not expose plaintext secret values, refs, delivery paths, claim tokens, private keys, or SVID material.

| Surface | Fields |
| --- | --- |
| Metrics | `vectis_secrets_resolve_requests_total` and `vectis_secrets_resolve_duration_seconds`, each labeled with `outcome`, `reason`, and `provider`. |
| Broker logs | `outcome`, `reason`, `provider`, `run_id`, `execution_id`, `secrets`, `files`, plus available execution scope such as `namespace`, `job_id`, `task_key`, `cell_id`, and `peer_spiffe_id`. |
| Run detail | Failed runs may include `latest_failed_security_event` with the newest failed `svid_check` or `secret_resolution` event. |
| Task detail | `GET /api/v1/runs/{id}/tasks` includes redacted `security_events` for task attempts. |
| CLI | `vectis-cli runs show <run-id>` prints `next_action=security_gate_failed`, a redacted `latest_failed_security_event`, and retry guidance when a security gate explains the failure. |

Resolution outcomes are `success`, `denied`, `not_found`, and `failed`.

Resolution reasons are:

| Reason | Meaning |
| --- | --- |
| `ok` | Resolution succeeded. |
| `unknown` | Fallback reason when a failure did not provide a more specific label. |
| `missing_provider` | The worker or broker has no configured resolver/provider for the requested refs. |
| `missing_authorizer` | The broker was started without an authorizer. |
| `authorization_denied` | The broker rejected identity binding, active execution claim validation, or access policy. |
| `provider_denied` | The provider rejected access, envelope validity, size, or path safety. |
| `provider_not_found` | The provider could not find the requested secret or provider scheme. |
| `provider_error` | Provider access or worker resolver setup failed for another reason. |
| `invalid_bundle` | The provider returned material that failed bundle validation. |

Provider kind is the URI scheme when all requested refs share one scheme, `mixed` when a task asks for multiple schemes, and `unknown` when no scheme can be determined.

## Boundaries

Vectis currently does not provide:

- environment-variable delivery for job secrets;
- plaintext secret values in job JSON;
- a public HTTP secret CRUD API;
- built-in Vault, Kubernetes Secret, cloud KMS, or cloud secret-manager providers;
- a general-purpose run-log redaction layer.

Job code that receives a secret file can still print or copy it. Treat job authors, run logs, workspaces, artifact uploads, service logs, database backups, and encryptedfs key material as sensitive surfaces.
