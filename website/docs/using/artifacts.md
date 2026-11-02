# Artifacts

Artifacts are run-scoped files that a job explicitly publishes with `builtins/upload-artifact`. Vectis stores artifact metadata in SQL and stores artifact bytes in the internal `vectis-artifact` content-addressed storage service.

There is no public API route for user-initiated artifact upload. Jobs publish artifacts from the worker, and users read them back through the UI, CLI, or v1 runs API.

## Browse In The UI

Run details include an **Artifacts** panel with each artifact's name, original file path, size, producing task when known, and content type. For runs with multiple attempts, the **Attempt** selector above the execution graph controls the graph, logs, and artifacts together. **Latest** follows new retries; a numbered attempt stays pinned when switching tasks or receiving updates. The page always shows one attempt at a time; records without attempt attribution are excluded. Overall run status and the run timeline remain explicitly run-level. A task that did not execute in the selected attempt has no execution context; the UI does not fall back to another attempt.

The page initially shows logs and artifacts for all tasks in the latest attempt. Selecting a task in the graph filters both panels to that task. Select the same node again, or choose **Clear task selection**, to return to all tasks in the selected attempt. Task updates do not change your selection. Both panels always follow the shared attempt selection. **Load more artifacts** follows API pagination within that scope, with attempt filtering applied before pagination. Artifact names remain run-scoped: a later upload with the same name replaces its earlier manifest, so selecting an older attempt does not recover overwritten files. Publish distinct names when files from multiple attempts must remain downloadable.

The panel refreshes while a run is active and briefly after completion so final catalog updates can arrive. **Refresh** checks again without reloading the run. Existing rows remain visible if a refresh fails, with an error explaining that the list could not be updated. An empty list means no manifests are currently available for the chosen scope; it does not guarantee a blob is still available for an existing manifest.

**Download** uses the authenticated API route and the browser's native download handling. Files are served as attachments; the UI does not render artifact HTML or buffer whole archives in memory. A download failure is handled in a separate browser tab, preserving the run page. The artifact service must be available for the download to succeed.

The UI proxy permits same-origin browser attachment navigations only for artifact-download GETs. Other browser API navigations and cross-site requests remain blocked, and upstream `run:read` authorization still applies. Producer task, attempt, and execution IDs survive task compaction, so completed runs retain attribution and task filtering.

For a local demonstration, load `examples/artifacts.json` through **Runs → Run Once**. It publishes a text report and a gzip archive from separate tasks.

## Publish From A Job

Add `builtins/upload-artifact` after the step that creates the file:

```json
{
  "id": "coverage-job",
  "root": {
    "id": "root",
    "uses": "builtins/sequence",
    "steps": [
      {
        "id": "test",
        "uses": "builtins/script",
        "with": {
          "script": "go test ./... -coverprofile coverage.out"
        }
      },
      {
        "id": "upload-coverage",
        "uses": "builtins/upload-artifact",
        "with": {
          "name": "coverage",
          "path": "coverage.out",
          "content_type": "text/plain",
          "metadata_json": "{\"kind\":\"coverage\"}",
          "max_bytes": "10485760"
        }
      }
    ]
  }
}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | Yes | Run-scoped artifact name. Listing, manifest lookup, and download use this name. A later upload with the same `name` replaces the manifest pointer for that run. |
| `path` | Yes | Workspace-relative file path to upload. The path must stay inside the workspace and must name a file, not a directory. |
| `content_type` | No | MIME type returned on download. When omitted, downloads use `application/octet-stream`. Treat it as advisory metadata. |
| `metadata_json` | No | Valid JSON string stored with the manifest and returned as `metadata` by the API. |
| `max_bytes` | No | Non-negative per-node upload limit. It can lower, but not raise, the worker-level upload cap. |

Vectis uploads the exact bytes at `path`. It does not automatically collect files, expand directories, compress archives, or interpret a top-level `artifacts` stanza. Create an archive yourself when you want one artifact to contain multiple files.

## List And Download

The CLI lists artifact manifests for a run:

```sh
./bin/vectis-cli runs artifacts list <run-id>
./bin/vectis-cli runs artifacts list <run-id> --task-id <task-id>
./bin/vectis-cli runs artifacts list <run-id> --task-attempt-id <task-attempt-id>
./bin/vectis-cli runs artifacts list <run-id> --execution-id <execution-id>
```

The default text output includes `NAME`, `TASK`, `ATTEMPT`, `EXECUTION`, `PATH`, `CONTENT TYPE`, `SIZE`, `SHARD`, and `DIGEST`. Use `--format json` when automation needs the full manifest fields.

Download by run ID and artifact name:

```sh
./bin/vectis-cli runs artifacts download <run-id> coverage --output coverage.out
```

Use `--output -` only when you want raw artifact bytes on stdout, for example in a pipeline.

## API Routes

Artifact reads require the same `run:read` authorization used for run, task, log, and run-event inspection. Runs hidden by namespace authorization return `run_not_found` instead of revealing that the run exists.

| Method and path | Purpose |
| --- | --- |
| `GET /api/v1/runs/{id}/artifacts` | List artifact manifests for one run. Supports `limit`, `cursor`, `task_id`, `task_attempt_id`, `execution_id`, and a positive integer `attempt` filter. The attempt filter uses live or retained producer identity and excludes manifests without attempt attribution. |
| `GET /api/v1/runs/{id}/artifacts/{name}` | Get one artifact manifest by run-scoped `name`. |
| `GET /api/v1/runs/{id}/artifacts/{name}/download` | Stream artifact bytes. The API verifies the blob descriptor against the SQL manifest before serving bytes. |

List responses use the standard paginated shape:

```json
{
  "data": [
    {
      "id": 42,
      "run_id": "run-123",
      "task_id": "upload-coverage",
      "task_attempt_id": "attempt-123",
      "execution_id": "exec-123",
      "cell_id": "local",
      "name": "coverage",
      "path": "coverage.out",
      "content_type": "text/plain",
      "blob_key": "sha256:...",
      "blob_algorithm": "sha256",
      "blob_digest": "...",
      "size_bytes": 12345,
      "artifact_shard_id": "artifact-a",
      "metadata": {"kind": "coverage"},
      "created_at": 1710000000000000000,
      "updated_at": 1710000000000000000
    }
  ],
  "next_cursor": 42
}
```

Downloads set `Content-Type` from the manifest or `application/octet-stream`, set `Content-Length`, set an attachment `Content-Disposition`, send `X-Content-Type-Options: nosniff`, and use no-store cache headers. Vectis does not transform artifact bytes before returning them.

## Manifest Fields

| Field | Meaning |
| --- | --- |
| `id` | Local artifact manifest ID. Also used as the pagination cursor. |
| `run_id` | Owning run. |
| `task_id` | Task node that produced the artifact, when known. |
| `task_attempt_id` | Task attempt that produced the artifact, when known. |
| `execution_id` | Execution attempt that produced the artifact, when known. |
| `cell_id` | Cell where the artifact was produced. |
| `name` | Run-scoped artifact name. |
| `path` | Original workspace-relative path, or the name when no path was recorded. |
| `content_type` | Producer-supplied MIME type, when present. |
| `blob_key` | Internal CAS key, currently `sha256:<digest>`. |
| `blob_algorithm` | Digest algorithm, currently `sha256`. |
| `blob_digest` | Hex digest for the stored bytes. |
| `size_bytes` | Stored blob size in bytes. |
| `artifact_shard_id` | Artifact shard that owns the blob. |
| `metadata` | Optional raw JSON metadata from `metadata_json`. |
| `created_at` | Manifest creation timestamp as a Unix-nanosecond integer. |
| `updated_at` | Manifest update timestamp as a Unix-nanosecond integer. |

## Operator Notes

`vectis-artifact` stores local CAS blobs under its durable storage directory. Each artifact shard needs one stable `--instance-id` and one durable `--storage-dir`; do not start multiple active artifact processes against the same storage directory. The service takes a storage lock and refuses startup when another process already owns that directory.

Artifact scale-out comes from adding independent shards. Workers route new uploads to a writable shard by run ID. Manifests record `artifact_shard_id`, and API downloads resolve that shard through a pinned artifact address or the registry. Keep shard IDs stable for as long as manifests can reference their blobs.

The artifact service can enter read-only mode for new blobs when free space drops below `--storage-read-only-min-free-bytes` / `VECTIS_ARTIFACT_STORAGE_READ_ONLY_MIN_FREE_BYTES` / `artifact.storage_read_only_min_free_bytes`. Read-only shards continue to serve existing blobs, but new uploads routed there fail.

Worker-side upload quotas are controlled by:

| Limit | Config |
| --- | --- |
| Per upload | `worker.artifact_max_bytes` / `VECTIS_WORKER_ARTIFACT_MAX_BYTES` / `--artifact-max-bytes` |
| Per run bytes | `worker.artifact_max_run_bytes` / `VECTIS_WORKER_ARTIFACT_MAX_RUN_BYTES` / `--artifact-max-run-bytes` |
| Per run count | `worker.artifact_max_count` / `VECTIS_WORKER_ARTIFACT_MAX_COUNT` / `--artifact-max-count` |

Set a quota to `0` to disable it. A job's `max_bytes` can only make the per-upload limit stricter.

SQL backups preserve artifact manifests, not blob bytes. Back up artifact storage directories when artifact restore matters. Retention cleanup can prune SQL manifests and, when given `--artifact-storage-dir`, remove unreferenced local blobs after the configured artifact-blob retention window.

Watch the artifact storage metrics in [Metrics Catalog](../operating/reference/metrics-catalog.md#artifact-storage): `vectis_artifact_storage_blobs`, `vectis_artifact_storage_bytes`, `vectis_artifact_storage_free_bytes`, `vectis_artifact_storage_free_inodes`, and `vectis_artifact_storage_new_blob_writable`.

## Common Failures

| Symptom | Meaning |
| --- | --- |
| `artifact_not_found` | No manifest exists for that run and artifact name. |
| `artifacts_not_configured` | The API has no artifact manifest repository configured. |
| `artifact_service_error` | The API could not connect to or read from the owning artifact shard. |
| `artifact_blob_unavailable` | The manifest exists, but the blob could not be read from `vectis-artifact`. |
| `artifact_blob_mismatch` | The blob descriptor returned by the artifact service did not match the SQL manifest digest or size. |

For the complete API error table, see [API Error Code Reference](./api-error-code-reference.md). For database fields and indexes, see [Database Schema](../operating/reference/database-schema.md#run_artifacts).
