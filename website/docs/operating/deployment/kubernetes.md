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
explicit orphan repair.
The next useful checks are:

1. Expose cell ingress once the mTLS edge contract is ready.
2. Add production-oriented certificate issuance and rotation checks.

For the current security posture and service dependencies, see
[Security](../../concepts/security.md) and
[Failure Domains](../../concepts/failure-domains.md).
