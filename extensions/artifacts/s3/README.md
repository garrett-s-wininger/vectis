# S3 Artifact Storage

`extensions/artifacts/s3` provides an S3-compatible blob store for
`vectis-artifact`.

Enable it on the artifact service:

```sh
VECTIS_ARTIFACT_STORAGE_BACKEND=s3
VECTIS_ARTIFACT_STORAGE_S3_ENDPOINT=http://seaweedfs-s3:8333
VECTIS_ARTIFACT_STORAGE_S3_REGION=us-east-1
VECTIS_ARTIFACT_STORAGE_S3_BUCKET=vectis-artifacts
VECTIS_ARTIFACT_STORAGE_S3_PREFIX=local
VECTIS_ARTIFACT_STORAGE_S3_ACCESS_KEY_ID=vectis
VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY_FILE=/run/secrets/vectis-s3-secret
VECTIS_ARTIFACT_STORAGE_S3_PATH_STYLE=true
```

Equivalent flags are available on `vectis-artifact`:
`--storage-backend=s3`, `--s3-endpoint`, `--s3-region`, `--s3-bucket`,
`--s3-prefix`, `--s3-access-key-id`, `--s3-secret-access-key-file`,
`--s3-session-token`, and `--s3-path-style`.

## Real Service Smoke

Provider unit tests use a fake HTTP server. The real-service smoke exercises an
actual S3-compatible endpoint with the same provider code that `vectis-artifact`
uses:

```sh
make s3-smoke-up
make s3-smoke-check
```

`s3-smoke-up` starts two local SeaweedFS S3 gateways with `CONTAINER_CMD`:
one public endpoint at `S3_SMOKE_ENDPOINT`, defaulting to
`http://127.0.0.1:18333`, and one credential-enforced endpoint at
`S3_SMOKE_AUTH_ENDPOINT`, defaulting to `http://127.0.0.1:18334`.
`s3-smoke-check` writes a blob, stats it, reads it back, and checks storage
stats through `go run ./extensions/artifacts/s3/smoke` in both modes.
The authenticated check also runs an unsigned request against the auth endpoint
and requires it to fail.

Focused lanes are available when debugging one mode:

```sh
make s3-smoke-public
make s3-smoke-auth
```

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `S3_SMOKE_IMAGE` | `docker.io/chrislusf/seaweedfs:latest` | Local S3-compatible image. |
| `S3_SMOKE_CONTAINER` | `vectis-seaweedfs-s3` | Reusable public-mode container name. |
| `S3_SMOKE_PORT` | `18333` | Host port mapped to the S3 gateway. |
| `S3_SMOKE_ENDPOINT` | `http://127.0.0.1:18333` | Public endpoint passed to the smoke binary. |
| `S3_SMOKE_BUCKET` | `vectis-artifacts` | Public bucket used by the smoke. |
| `S3_SMOKE_PREFIX` | `local-smoke` | Public object prefix used by the smoke. |
| `S3_SMOKE_CREATE_BUCKET` | `true` | Whether the public smoke creates the bucket first. |
| `S3_SMOKE_AUTH_CONTAINER` | `vectis-seaweedfs-s3-auth` | Reusable auth-mode container name. |
| `S3_SMOKE_AUTH_PORT` | `18334` | Host port mapped to the auth S3 gateway. |
| `S3_SMOKE_AUTH_ENDPOINT` | `http://127.0.0.1:18334` | Auth endpoint passed to the smoke binary. |
| `S3_SMOKE_AUTH_ACCESS_KEY_ID` | `vectis-smoke` | Access key accepted by the auth smoke config. |
| `S3_SMOKE_AUTH_SECRET_ACCESS_KEY` | `vectis-smoke-secret` | Secret key accepted by the auth smoke config. |
| `S3_SMOKE_AUTH_CONFIG` | `extensions/artifacts/s3/testdata/seaweedfs-s3-auth.json` | SeaweedFS auth config mounted into the auth container. |
| `S3_SMOKE_AUTH_NEGATIVE_TIMEOUT` | `5s` | Maximum wait for the unsigned rejection check. |
| `S3_SMOKE_TIMEOUT` | `30s` | Maximum wait for the endpoint and smoke operations. |

To point the same smoke at a separately managed endpoint:

```sh
go run ./extensions/artifacts/s3/smoke \
  --endpoint http://127.0.0.1:8333 \
  --bucket vectis-artifacts \
  --prefix manual-smoke \
  --create-bucket=false
```

The provider stores blobs under:

```text
<prefix>/blobs/sha256/<first-two>/<next-two>/<digest>.blob
```

Notes:

- The provider uses the Go standard library only; it signs requests with AWS
  SigV4 when access key and secret key are configured.
- Path-style URLs are the default because they work well with local providers
  such as SeaweedFS.
- `vectis-artifact` still owns the internal gRPC API and shard identity. S3
  storage changes where bytes live; it does not make artifact reads fail over
  automatically to a different shard.
- Filesystem free-byte and free-inode gauges are not emitted for this backend.
