# Knox Secret Provider

This standard secret-provider extension implements `sdk/secrets.Provider` for
Knox primary-version reads.

The provider accepts `knox:` and `knox://` references that resolve to Knox key
IDs, fetches the primary key version, and returns file material for the
cell-local `vectis-secrets` broker to deliver to workers.

## Real Service Smoke

The default local smoke clones upstream Knox source, builds a local smoke image
around Knox's real server/key-manager packages, starts a live mTLS
Knox-compatible process, and resolves a seeded key through the Vectis provider:

```sh
make knox-smoke
```

`knox-smoke-up` recreates the local fixture container so certs, seeded keys, and
environment overrides cannot go stale between runs. Use `make knox-smoke-down`,
`make knox-smoke-status`, and `make knox-smoke-logs` to manage the local
container after a run.

The smoke server normally generates `ca.crt`, `server.crt`, `server.key`,
`client.crt`, and `client.key` in `KNOX_SMOKE_CERT_DIR`. If all five files are
already present, it loads them instead. The Kubernetes smoke uses that path to
mount a short-lived Secret into both the Knox fixture and `vectis-secrets`.

To run the provider check against an already-running Knox-compatible endpoint,
point `knox-smoke-check` at a known secret:

```sh
KNOX_SMOKE_URL=https://knox.internal.example \
KNOX_SMOKE_AUTH_TOKEN_FILE=/run/secrets/vectis-knox-token \
KNOX_SMOKE_REF=knox://team/smoke_token \
KNOX_SMOKE_EXPECTED_SHA256=<sha256> \
make knox-smoke-check
```

The smoke resolves the configured primary key version, checks the delivered
file id/path and expected SHA-256 digest, optionally verifies that
`KNOX_SMOKE_WRONG_AUTH_TOKEN` is denied, and optionally verifies that
`KNOX_SMOKE_MISSING_REF` returns not found.

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `KNOX_SMOKE_REPO` | `https://github.com/pinterest/knox.git` | Upstream Knox repository cloned for the local smoke image. |
| `KNOX_SMOKE_REPO_REF` | pinned commit | Upstream Knox commit, tag, or fetched ref checked out for the local smoke image. |
| `KNOX_SMOKE_SOURCE_DIR` | `artifacts/integrations/knox/src` | Local clone path; ignored by git. |
| `KNOX_SMOKE_IMAGE` | `localhost/vectis-knox-smoke:dev-local` | Local Knox smoke image tag. |
| `KNOX_SMOKE_CONTAINER` | `vectis-knox` | Local Knox smoke container name. |
| `KNOX_SMOKE_PORT` | `19000` | Local Knox smoke host port. |
| `KNOX_SMOKE_CERT_DIR` | `artifacts/integrations/knox/certs` | Generated local CA and client/server certificates. |
| `KNOX_SMOKE_CERT_TTL` | `168h` | Validity window for generated local smoke certificates. |
| `KNOX_SMOKE_URL` | `https://127.0.0.1:19000` | Knox base URL. |
| `KNOX_SMOKE_KEY_ID` | `team:smoke_token` | Key seeded into the local Knox smoke server. |
| `KNOX_SMOKE_SECRET` | `knox-smoke-secret` | Local smoke secret data. |
| `KNOX_SMOKE_AUTH_TOKEN_FILE` | empty | File containing the Knox Authorization header value. |
| `KNOX_SMOKE_AUTH_TOKEN` | `0tknox-smoke` | Inline Knox Authorization header value; prefer the file for managed endpoints. |
| `KNOX_SMOKE_REF` | `knox://team/smoke_token` | Secret ref resolved by the smoke. |
| `KNOX_SMOKE_ID` | `smoke-token` | Expected delivered secret id. |
| `KNOX_SMOKE_PATH` | `smoke/token` | Expected delivered secret path. |
| `KNOX_SMOKE_EXPECTED_SHA256` | empty | Expected SHA-256 of the secret data. |
| `KNOX_SMOKE_EXPECTED_DATA` | `knox-smoke-secret` | Expected secret data, used only to derive SHA-256 for local checks. |
| `KNOX_SMOKE_WRONG_AUTH_TOKEN` | `0twrong-knox-smoke` | Optional token expected to be denied. |
| `KNOX_SMOKE_MISSING_REF` | `knox://team/missing_token` | Optional ref expected to be missing. |
| `KNOX_SMOKE_INSECURE_SKIP_VERIFY` | `false` | Skip TLS verification for local compatibility endpoints. |
| `KNOX_SMOKE_CA_FILE` | `artifacts/integrations/knox/certs/ca.crt` | Knox server CA certificate file. |
| `KNOX_SMOKE_CLIENT_CERT_FILE` | `artifacts/integrations/knox/certs/client.crt` | Optional mTLS client certificate file for managed Knox endpoints. |
| `KNOX_SMOKE_CLIENT_KEY_FILE` | `artifacts/integrations/knox/certs/client.key` | Optional mTLS client key file for managed Knox endpoints. |
| `KNOX_SMOKE_TIMEOUT` | `30s` | Maximum wait for the endpoint and smoke operations. |
