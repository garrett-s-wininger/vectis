# Local SPIFFE Secrets Smoke Test

This runbook exercises the first end-to-end Vectis secret path on one development machine:

1. `vectis-local` starts the Vectis stack, including `vectis-secrets` with encryptedfs enabled.
2. The bundled `vectis-spiffe` authority provides per-execution X.509-SVIDs through SPIRE-compatible APIs.
3. The worker creates a bounded registration entry before fetching the execution SVID.
4. The worker uses that SVID as its client certificate when resolving a declared job secret.
5. The job receives the secret as a task-scoped file under `.vectis/secrets`.

By default, `vectis-local` starts an embedded development-only `vectis-spiffe` authority for the current user when local gRPC TLS is enabled. It persists a local CA, serves a Workload API socket and a registration API socket, exports the trust bundle, and wires those sockets into Vectis.

`vectis-spiffe` implements the SPIRE-compatible Workload API and Entry API contracts that Vectis workers use for this local path. For the underlying model, see the SPIRE Workload API and Server Entry API background in the [SPIRE Agent Configuration Reference](https://spiffe.io/docs/latest/deploying/spire_agent/) and SPIRE's [Registering workloads](https://spiffe.io/docs/latest/deploying/registering/) guide.

## What This Proves

| Check | Expected proof |
| --- | --- |
| Worker registration | The worker can create, renew, and release Vectis-managed registration entries through a protected local Unix socket. |
| SVID gate | The worker refuses to run action code unless the Workload API returns the exact derived execution SPIFFE ID. |
| Broker authentication | `vectis-secrets` accepts the execution SVID as the gRPC client certificate and derives the same expected execution identity from the active execution row. |
| Secret policy | The broker applies the encryptedfs provider and access policy before returning material. |
| File delivery | The action sees only a task-scoped file under `VECTIS_SECRETS_DIR`; Vectis does not place the secret value in the job definition or environment. |

## Local Authority

The bundled authority is the default local identity path. It creates local SPIFFE state under `$XDG_DATA_HOME/vectis/spiffe` and sockets under `$XDG_RUNTIME_DIR/vectis/spiffe` or the Vectis temp runtime fallback.

Use a dedicated `XDG_DATA_HOME` so the encryptedfs root, generated local TLS, and local SPIFFE state are easy to find:

```sh
export VECTIS_SMOKE_HOME="$PWD/.vectis-spiffe-smoke"
export XDG_DATA_HOME="$VECTIS_SMOKE_HOME/data"

./bin/vectis-local \
  --spiffe-trust-domain vectis.internal \
  --spiffe-fetch-timeout 5s \
  --spiffe-x509-svid-ttl 5m \
  --spiffe-registration-max-ttl 10m
```

Leave `vectis-local` running. It should log the local SPIFFE registration and workload sockets, then log that local SPIFFE execution identity is enabled. The local authority defaults to:

| Item | Managed default |
| --- | --- |
| Trust domain | `vectis.internal` unless `--spiffe-trust-domain` is set |
| Registration API socket | `$XDG_RUNTIME_DIR/vectis/spiffe/registration.sock` or temp runtime fallback |
| Workload API socket | `$XDG_RUNTIME_DIR/vectis/spiffe/workload.sock` or temp runtime fallback |
| Worker parent ID | `spiffe://<trust-domain>/vectis-spiffe/agent/local` unless `--spiffe-parent-id` is set |
| Worker selector | `unix:uid:<current uid>` unless one or more `--spiffe-selector` flags are set |

Do not use `--grpc-insecure` for this smoke test. Secret resolution needs gRPC TLS so `vectis-secrets` can request and verify the execution SVID client certificate; `vectis-local` skips the embedded authority and secrets service in plaintext mode.

## Add A Secret

In another terminal, keep the same `XDG_DATA_HOME`:

```sh
export VECTIS_SMOKE_HOME="$PWD/.vectis-spiffe-smoke"
export XDG_DATA_HOME="$VECTIS_SMOKE_HOME/data"

printf '%s' 'spiffe-secret' > "$VECTIS_SMOKE_HOME/token.txt"

./bin/vectis-cli secrets encryptedfs put encryptedfs://team/smoke-token \
  --from-file "$VECTIS_SMOKE_HOME/token.txt" \
  --root "$XDG_DATA_HOME/vectis/cells/local/secrets" \
  --key-file "$XDG_DATA_HOME/vectis/cells/local/secrets.key" \
  --force
```

`vectis-local` creates the encryptedfs key file before it starts `vectis-secrets`, so `--create-key` is normally unnecessary here.

## Run A Secret Job

Run the checked-in example job, which reads the file delivered by the broker without printing the secret value:

```sh
./bin/vectis-cli jobs run examples/secrets.json --follow
```

The run should succeed. If you want time to inspect the transient registration while the action is running, temporarily change the command to read the secret and then `sleep 30`.

## Verify

Check these signals:

| Signal | What to look for |
| --- | --- |
| Worker logs | `Configured SPIRE registration via server API`, then successful run completion. |
| Worker metrics | `vectis_worker_spire_svid_checks_total{outcome="success",reason="matched"}` increases. |
| Secret metrics | `vectis_secrets_resolve_requests_total{outcome="success",provider="encryptedfs"}` increases. |
| Run logs | The job succeeds without printing the secret value. |

With the default execution identity template, the execution SPIFFE ID has this shape:

```text
spiffe://vectis.internal/cell/local/namespace/root/job/secret-example/run/<run-id>/execution/<execution-id>
```

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `vectis-local` exits with a SPIFFE flag error | One of the required local identity settings is missing or malformed. |
| Worker SVID check `source_error` | The worker cannot open the Workload API socket, the authority is down, or socket permissions are wrong. |
| Worker SVID check `mismatch` | The registration entry does not match the derived execution SPIFFE ID, parent ID, or selectors the authority sees for the worker. |
| Worker registration errors | The registration API socket is unavailable or the worker process is not allowed to create entries. |
| Secret resolution `authorization_denied` | The broker rejected the mTLS peer identity, the active execution claim, or the secret access policy. Check that `worker.execution_identity.*` reaches both worker and `vectis-secrets`. |
| Secret resolution `provider_denied` | The encryptedfs envelope is invalid, the key is wrong, the path escaped the provider root, or the secret exceeds provider limits. |

For incident-style repair steps, see [Repair Runbooks](../reliability/repair-runbooks.md#spire-execution-svid-checks) and [Secret Resolution](../reliability/repair-runbooks.md#secret-resolution).

## Cleanup

Stop `vectis-local`. Vectis releases entries it created and tagged as managed; entry TTLs provide a second cleanup path if the worker exits abruptly.

To remove the local Vectis data from this smoke test:

```sh
rm -rf "$VECTIS_SMOKE_HOME"
```

Use the cleanup command only for the dedicated smoke-test directory, not for shared local state.
