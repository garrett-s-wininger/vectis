# Kubernetes Reference Deployment

Vectis has an initial Kubernetes reference manifest under `deploy/kubernetes/`.
This lane exists to make Kubernetes deployment validation boring before using
Kubernetes as a deeper compute integration.

Render the manifest:

```sh
./bin/vectis-cli deploy kubernetes render --output artifacts/deploy/kubernetes/vectis.yaml
```

For local development from source:

```sh
make deploy-kubernetes-render
```

For local cluster validation, use kind as the cluster provider and choose the
container runtime with Make variables. The Make lane also creates the local
cluster, loads images, and runs the extended smoke matrix:

```sh
make k8s-kind-up
make k8s-kind-load-images
make k8s-kind-validate-core
make k8s-kind-run-worker-core-smoke
make k8s-kind-run-cancel-smoke
make k8s-kind-run-scale-smoke
make k8s-kind-run-orphan-smoke
make k8s-kind-run-repair-smoke
make k8s-kind-run-gerrit-stream-smoke
make k8s-kind-run-s3-artifact-smoke
make k8s-kind-run-knox-secrets-smoke
```

The generic `k8s-*` aliases dispatch through `K8S_PROVIDER`, which defaults to
`kind`. The kind lane defaults to Podman because the rest of the reference
container tooling already uses Podman:

| Variable | Default | Purpose |
|---|---:|---|
| `K8S_PROVIDER` | `kind` | Provider used by generic `k8s-*` aliases. |
| `K8S_CLUSTER` | `vectis` | Local cluster name. |
| `K8S_NAMESPACE` | `vectis` | Namespace rendered into the manifest. |
| `DEPLOY_KUBERNETES_MANIFEST` | `deploy/kubernetes/manifests.yaml.tmpl` | Manifest template used by render and core validation targets. |
| `DEPLOY_KUBERNETES_OUT` | `artifacts/deploy/kubernetes/vectis.yaml` | Rendered manifest path used by apply and validation targets. |
| `K8S_IMAGE_REGISTRY` | `localhost` | Local image registry/name prefix rendered for kind and used when tagging images. |
| `K8S_IMAGE_TAG` | `dev-local` | Local image tag built and loaded into kind. |
| `K8S_API_LOCAL_PORT` | `18080` | Local port used by the canonical smoke API port-forward. |
| `K8S_SMOKE_JOB` | `examples/e2e-canonical.json` | Job submitted by the workload smoke harness. |
| `K8S_SMOKE_CLI_IMAGE` | `localhost/vectis-cli:dev-local` | CLI image used by the smoke harness to seed the encryptedfs secret. |
| `K8S_SMOKE_SEED_SECRET` | `true` | Whether the smoke harness seeds `encryptedfs://team/smoke-token`. |
| `K8S_CANCEL_API_LOCAL_PORT` | `18082` | Local port used by the worker-control cancel smoke API port-forward. |
| `K8S_CANCEL_SMOKE_JOB` | `examples/e2e-kubernetes-cancel.json` | Long-running job submitted by the worker-control cancel smoke harness. |
| `K8S_SCALE_API_LOCAL_PORT` | `18083` | Local port used by the worker scaling smoke API port-forward. |
| `K8S_SCALE_SMOKE_JOB` | `examples/e2e-kubernetes-scale.json` | Distributed fanout job submitted by the worker scaling smoke harness. |
| `K8S_SCALE_WORKER_REPLICAS` | `3` | Temporary worker Deployment replica count used by the scaling smoke. |
| `K8S_SCALE_MIN_WORKERS` | `2` | Minimum distinct active worker lease owners required by the scaling smoke. |
| `K8S_ORPHAN_API_LOCAL_PORT` | `18084` | Local port used by the worker pod-loss orphan smoke API port-forward. |
| `K8S_ORPHAN_SMOKE_JOB` | `examples/e2e-kubernetes-orphan.json` | Long-running root task submitted by the pod-loss orphan smoke harness. |
| `K8S_ORPHAN_LEASE_TTL` | `30s` | Temporary `VECTIS_WORKER_EXECUTION_LEASE_TTL` used by the orphan smoke. |
| `K8S_ORPHAN_STABILITY` | `20s` | Time the orphan smoke requires the run to remain orphaned after lease expiry. |
| `K8S_REPAIR_API_LOCAL_PORT` | `18085` | Local port used by the explicit orphan repair smoke API port-forward. |
| `K8S_REPAIR_SMOKE_JOB` | `examples/e2e-kubernetes-repair.json` | Dynamic-cutoff job submitted by the explicit orphan repair smoke harness. |
| `K8S_REPAIR_LEASE_TTL` | `30s` | Temporary `VECTIS_WORKER_EXECUTION_LEASE_TTL` used by the repair smoke. |
| `K8S_REPAIR_READY_AFTER` | `75s` | Delay after repair job submission before force-requeue should take the success path. |
| `K8S_GERRIT_API_LOCAL_PORT` | `18086` | Local port used by the Gerrit stream smoke API port-forward. |
| `K8S_GERRIT_KEEP_FIXTURE` | `false` | Keep the Gerrit fixture after the optional stream smoke for local inspection. |
| `K8S_S3_API_LOCAL_PORT` | `18089` | Local port used by the S3 artifact smoke API port-forward. |
| `K8S_S3_LOCAL_PORT` | `18335` | Local port used by the in-cluster S3 fixture port-forward. |
| `K8S_S3_IMAGE` | `docker.io/chrislusf/seaweedfs:4.36` | SeaweedFS image applied inside the namespace by the optional S3 artifact smoke. |
| `K8S_S3_BUCKET` | `vectis-artifacts` | Bucket used by the S3 artifact smoke. |
| `K8S_S3_PREFIX` | `kubernetes-smoke` | Object prefix used by the S3 artifact smoke. |
| `K8S_S3_TEMP_DIR` | `/data/vectis/artifact/s3-tmp` | Writable directory used by `vectis-artifact` while hashing S3 uploads during the smoke. |
| `K8S_S3_KEEP_FIXTURE` | `false` | Keep the S3 fixture after the optional artifact smoke for local inspection. |
| `K8S_KNOX_API_LOCAL_PORT` | `18090` | Local port used by the Knox secrets smoke API port-forward. |
| `K8S_KNOX_LOCAL_PORT` | `19001` | Local port used by the Knox fixture port-forward. |
| `K8S_KNOX_IMAGE` | `localhost/vectis-knox-smoke:dev-local` | Knox smoke image built from the pinned Knox source and loaded into kind by the optional Knox smoke. |
| `K8S_KNOX_CLUSTER_URL` | `https://vectis-knox:9000` | In-cluster Knox URL passed to `vectis-secrets`. |
| `K8S_KNOX_REF` | `knox://team/smoke_token` | Secret ref used by the Knox-backed Kubernetes job. |
| `K8S_KNOX_KEEP_FIXTURE` | `false` | Keep the Knox fixture after the optional secrets smoke for local inspection. |
| `CONTAINER_CMD` | `podman` | Runtime command used to build and save images. |
| `IMAGE_REGISTRY` | unset | General image-build prefix; the kind target sets it from `K8S_IMAGE_REGISTRY`. |
| `KIND_PROVIDER` | `podman` | Provider passed to kind as `KIND_EXPERIMENTAL_PROVIDER`; set `auto` for kind autodetection. |
| `KUBECTL` | `kubectl` | Kubernetes client command. |
| `KUBECONFIG` | unset | Optional kubeconfig path for isolated local clusters. |

Docker or Docker-compatible Colima profiles can use the same targets:

```sh
make k8s-kind-validate CONTAINER_CMD=docker KIND_PROVIDER=docker
```

`make k8s-kind-validate` runs the configured `K8S_KIND_VALIDATE_STEPS`.
`k8s-kind-validate-core` uses the repo-local Go validation harness to render,
apply, wait for readiness, and run the canonical workload smoke; the later
targets run the worker-core, cancel, scale, orphan, and repair smokes.

The smoke harnesses can also be run directly:

```sh
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --cancel-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --scale-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --orphan-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --repair-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --gerrit-stream-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --s3-artifact-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --knox-secrets-only
```

The first manifest is a single-cell deployment. It includes Postgres, registry,
queue, orchestrator, log, artifact, secrets, API, docs, cron, SCM poller, a
disabled-by-default Gerrit stream bridge, reconciler, catalog, and a worker pod
that runs `vectis-worker` beside `vectis-worker-core` over a shared Unix socket
and shared log spool. The worker pod also runs a local `vectis-spiffe` sidecar
so secret resolution uses per-execution SVIDs over internal gRPC mTLS. The
worker registers its pod IP and worker-control port with the registry so the API
can issue fast cancel requests inside the cluster.

## Current Scope

The manifest is meant for local or staging validation. It deliberately does not
claim production security posture yet:

- internal gRPC TLS and SPIFFE CA material is generated into the rendered
  development manifest; pod templates include a checksum annotation so a
  re-rendered development PKI rolls every TLS-speaking workload in the same
  apply, but this is not a production PKI or rotation strategy;
- the default smoke job seeds and verifies encryptedfs secret delivery through
  `vectis-secrets`;
- the cancel smoke verifies the API-to-worker-control fast path by requiring
  immediate worker-control cancellation instead of the durable pending fallback;
- the scale smoke temporarily scales the worker Deployment, submits
  `examples/e2e-kubernetes-scale.json`, verifies distributed branches have
  multiple active `lease_owner` values, waits for the run to succeed, and
  restores the original replica count;
- the orphan smoke temporarily shortens the worker execution lease TTL, deletes
  the worker pod that owns a long-running root task, verifies the run becomes
  and remains `orphaned`, and checks no extra task attempt is created;
- the repair smoke creates pod-loss orphaned runs and then verifies explicit
  operator repair through `force-requeue` to a second successful attempt and
  `repair/mark-abandoned` to a terminal abandoned state;
- `vectis-scm-gerrit-stream` is rendered at `replicas: 0`; set
  `VECTIS_SCM_GERRIT_STREAM_*`, replace the `vectis-gerrit-stream-ssh` Secret,
  and scale the Deployment when using Gerrit `stream-events`;
- the optional Gerrit stream smoke starts a Gerrit fixture in-cluster, creates a
  stored Gerrit `scm_poll` job through the API, temporarily scales the SCM
  poller to zero to isolate the source under test, scales the stream bridge from
  zero to one replica, pushes a real change, and verifies the triggered run and
  audit metadata. It deletes the Gerrit fixture when the run ends unless
  `K8S_GERRIT_KEEP_FIXTURE=true` or `--gerrit-keep-fixture=true` is set;
- the optional S3 artifact smoke starts an authenticated SeaweedFS fixture
  in-cluster, verifies unsigned requests are rejected, creates the configured
  bucket, patches `statefulset/vectis-artifact` to use that S3-compatible
  endpoint, runs the canonical workload smoke, verifies artifact upload/download
  through the deployed API path, and then rolls `vectis-artifact` back. It
  deletes the S3 fixture when the run ends unless `K8S_S3_KEEP_FIXTURE=true` or
  `--s3-keep-fixture=true` is set;
- the optional Knox secrets smoke builds and loads a smoke image from the pinned
  Knox source, starts an mTLS Knox fixture in-cluster, verifies it directly
  through a port-forward, mounts the generated CA and client certificate bundle
  into `statefulset/vectis-secrets`, enables the Knox provider plus `knox://*`
  policy, submits `examples/e2e-kubernetes-knox.json`, and verifies the worker
  resolves `knox://team/smoke_token` through the deployed secrets service. It
  restores `vectis-secrets` and deletes the fixture when the run ends unless
  `K8S_KNOX_KEEP_FIXTURE=true` or `--knox-keep-fixture=true` is set;
- `vectis-cell-ingress` is not exposed yet;
- default Secret values are placeholders and must be overridden before shared use.

Use these render flags when preparing a cluster-specific manifest:

```sh
./bin/vectis-cli deploy kubernetes render \
  --namespace vectis \
  --image-registry registry.example.com/vectis \
  --image-tag "$VERSION" \
  --postgres-password "$POSTGRES_PASSWORD" \
  --bootstrap-token "$BOOTSTRAP_TOKEN" \
  --encryptedfs-key "$ENCRYPTEDFS_KEY" \
  --output artifacts/deploy/kubernetes/vectis.yaml
```

## Validation Direction

The Kubernetes deployment lane now validates the core workload path,
worker-control cancellation, scaled worker fanout, pod-loss orphan safety, and
explicit orphan repair. The optional Gerrit stream smoke validates the deployed
Gerrit `stream-events` trigger path when the cluster can pull or has loaded the
configured Gerrit image. The optional S3 artifact smoke validates
`vectis-artifact` against an in-cluster S3-compatible backend with access-key
auth and real artifact upload/download. The optional Knox secrets smoke
validates `vectis-secrets` against an in-cluster Knox provider with mTLS and
real worker secret materialization.
The next useful checks are:

1. Expose cell ingress once the mTLS edge contract is ready.
2. Add production-oriented certificate issuance and rotation checks.

For the current security posture and service dependencies, see
[Security](../../concepts/security.md) and
[Failure Domains](../../concepts/failure-domains.md).
