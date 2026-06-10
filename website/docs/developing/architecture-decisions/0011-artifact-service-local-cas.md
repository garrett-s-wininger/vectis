# ADR 0011: Artifact service with local content-addressed storage

## Status

Accepted; first implementation slice completed.

## Context

Vectis records run state, queue handoffs, and logs. Build and CI workloads also need durable outputs such as binaries, test reports, coverage files, SBOMs, diagnostics, and cross-segment handoff files. The multi-cell design names artifacts as one of the explicit objects that can cross segment boundaries.

The first version should keep the deployment model self-hosted and inspectable. It should not require S3, object-storage credentials, or a separate storage operator just to make local development and small production deployments useful.

## Decision

Add a dedicated `vectis-artifact` service. The API remains the user-facing HTTP boundary for listing, authorization, and downloads. Artifact writes are worker-originated execution outputs; user-provided data should live in source, cache, package, or object systems that the workload can read directly.

The initial storage backend is a local content-addressed store on disk. Blob identity is:

- hash algorithm: `sha256`
- digest form: lowercase hex
- blob key: `sha256:<digest>`

The local layout is:

```text
<artifact-storage-dir>/
  artifact.lock
  tmp/
    <upload-id>.part
  blobs/
    sha256/
      ab/
        cd/
          <64-hex-digest>.blob
```

The service writes uploads to `tmp/`, hashes while streaming, verifies the final digest and byte count, fsyncs the file and parent directory, then atomically publishes the blob into `blobs/` without replacing an existing blob. If the blob already exists, the service verifies the existing size and digest, discards the temporary upload, and treats the upload as idempotent. The first version preserves uploaded bytes exactly. It does not compress, normalize, unpack, or index archives before storage.

Artifact metadata belongs in SQL, not in the CAS directory. Metadata records should describe the logical artifact:

- artifact ID
- namespace, job ID, run ID, and cell ID
- optional segment, task, node, and attempt identifiers
- artifact name and normalized path within the run
- blob key, size, media type, and optional checksum headers
- producing worker or service identity
- artifact shard ID
- created time and optional retention or expiry time

The CAS owns bytes. SQL owns names, listing, retention policy, authorization joins, and the route back to the owning artifact shard.

The normal write order is:

1. Resolve a writable artifact shard for the run's cell.
2. Stream bytes to `vectis-artifact` and receive a blob descriptor.
3. Insert or upsert the logical artifact metadata in SQL with an idempotency key scoped to the producer, run, and artifact path.

If step 3 fails after the blob is stored, the blob is an orphan and is safe for later garbage collection because no live SQL metadata references it.

## Service Boundaries

`vectis-api` exposes the public artifact API. The REST surface is run-scoped:

- list artifacts for a run
- fetch artifact metadata
- download artifact bytes

The API does not admit direct artifact uploads in the first release.

`vectis-worker` uploads produced artifacts at explicit artifact collection points. The first user-facing collection mechanism is `builtins/upload-artifact`, which publishes a workspace-relative file after prior steps have produced it. The first version does not add top-level job artifact declarations, action output declarations, implicit workspace scans, or automatic archive collection. The worker should not write directly into the artifact storage directory.

`vectis-artifact` exposes internal gRPC methods for streaming uploads, stat/read by blob key, and storage health. The first blob service should not require SQL access. The gRPC service does not make user authorization decisions; callers must be trusted Vectis services.

## Discovery And Locality

`COMPONENT_ARTIFACT` is part of the registry component enum. Artifact services register with `cell.id` metadata and a write-state metadata key similar to log storage:

- `artifact.write_state = writable`
- `artifact.write_state = read_only`

New uploads should choose a writable artifact shard in the run's execution cell. Metadata records store the chosen shard ID. Reads route to that shard through registry discovery or a pinned artifact address. Single-node Vectis is the degenerate case with one local artifact service and one local storage directory.

In multi-cell deployments, artifact blobs stay cell-local by default. The global control plane may catalog metadata, but it should not assume it can read bytes without contacting the owning cell or a future replication layer.

## Retention And Garbage Collection

Run retention deletes logical artifact metadata first. Blob deletion is a separate garbage-collection step:

1. Mark artifact metadata deleted or expired.
2. After a grace period, scan for blob keys with no live references.
3. Remove unreferenced blobs from the local CAS.

Initial defaults match the retention CLI policy: terminal runs and their artifact manifests are eligible after 30 days, and local artifact blob garbage collection is disabled unless the operator passes an artifact storage directory. When blob pruning is enabled, unreferenced CAS blobs use a 30 day grace period based on file mtime. A duration of `0` disables the corresponding cleanup surface.

This lets multiple logical artifacts share one blob and avoids deleting bytes that are still referenced by another run. The cleanup path should be operator-visible and safe to retry.

The artifact service should also have a read-only threshold for new blobs, matching the log service's disk-pressure posture. Stored artifacts remain readable while new uploads can be rejected before the disk is exhausted.

## Security

Artifact names are metadata, not filesystem paths. The service must never derive local paths from artifact names, user paths, content type, or archive entries. Only the validated digest path can locate blob bytes.

The API enforces namespace, RBAC, token scope, request size, and route-level audit behavior. Internal artifact gRPC should use the existing service identity model and accept only authorized Vectis service identities. Downloads should use API-mediated authorization first; direct public access to `vectis-artifact` is out of scope for the first version.

The first implementation should reject oversized artifacts, record producer identity, avoid logging artifact contents, and treat content type as advisory.

## Consequences

- Local development and single-node deployments get artifact support without external object storage.
- Content addressing gives deduplication, idempotent uploads, and a clean future migration path to object stores.
- Artifact availability depends on the owning shard and cell unless a later replication design changes that.
- SQL backup captures artifact metadata, but operators must also back up artifact storage directories when artifact bytes matter.
- Retention must handle two phases: metadata removal and CAS garbage collection.

## Deferred

- S3/GCS/Azure backends
- Cross-cell replication
- Signed direct download URLs
- Resumable multipart upload
- Artifact cache semantics for dependencies
- Job-level artifact declarations and action output auto-collection
- Compression or archive normalization before storage
- Archive indexing and selective file browsing inside archives
- Active/active shared artifact storage

## First Implementation Slice

The first implementation slice is complete:

- [x] Add `api/proto/artifact.proto` for streaming upload, stat, and read.
- [x] Add `vectis-artifact` and `internal/artifact` with a tested local CAS backend.
- [x] Add registry component and config defaults for artifact gRPC, metrics, storage path, and read-only threshold.
- [x] Add SQL migrations and DAL methods for run-scoped artifact metadata.
- [x] Expose list, metadata, and download routes in `vectis-api`.
- [x] Add CLI commands for listing and downloading run artifacts.
- [x] Add retention cleanup for artifact metadata and unreferenced blobs.
- [x] Add explicit worker-side uploads through `builtins/upload-artifact`.
- [x] Add worker-side per-upload and per-run artifact quotas.
- [x] Add run artifact list selectors for task, task attempt, and execution attribution.
- [x] Add run-scoped indexes for task, task attempt, and execution artifact selectors.
- [x] Decide first-version retention defaults, explicit collection semantics, and byte-preservation policy.

## Open Questions

None for the first implementation slice.

## References

- [Architecture](../../concepts/architecture.md)
- [ADR 0006: Global coordination and cell-local execution](./0006-global-coordination-cell-local-execution.md)
- `internal/logserver/storage.go` - existing local durable storage and disk-pressure precedent
- `internal/registry/metadata.go` - existing service metadata precedent
