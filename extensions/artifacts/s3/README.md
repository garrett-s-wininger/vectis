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
