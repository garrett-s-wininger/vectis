# Kubernetes Reference Deployment

This directory contains the first Kubernetes reference deployment for Vectis.
It is intentionally a simple single-cell manifest that proves the core stack can
run under Kubernetes with internal gRPC TLS and worker SPIFFE identity before
adding cell ingress or external providers.

Render the manifest:

```sh
go run ./cmd/cli deploy kubernetes render --output artifacts/deploy/kubernetes/vectis.yaml
```

Or:

```sh
make deploy-kubernetes-render
```

Validate an already reachable cluster by rendering the manifest, applying it,
waiting for workload readiness, and running the canonical workload smoke:

```sh
go run ./deploy/kubernetes/validate \
  --context kind-vectis \
  --output artifacts/deploy/kubernetes/vectis.yaml
```

For local cluster validation, the repo treats kind as the Kubernetes provider
and keeps the container runtime configurable. The defaults match the existing
Podman-based build posture:

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

Or run the full local lifecycle, image-load step, and smoke matrix:

```sh
make k8s-kind-validate
```

The local contract is:

| Variable | Default | Purpose |
|---|---:|---|
| `K8S_PROVIDER` | `kind` | Dispatch target for generic `k8s-*` aliases. |
| `K8S_CLUSTER` | `vectis` | Local cluster name. |
| `K8S_NAMESPACE` | `vectis` | Namespace rendered into the manifest. |
| `DEPLOY_KUBERNETES_MANIFEST` | `deploy/kubernetes/manifests.yaml.tmpl` | Manifest template used by render and core validation targets. |
| `DEPLOY_KUBERNETES_OUT` | `artifacts/deploy/kubernetes/vectis.yaml` | Rendered manifest path used by apply and validation targets. |
| `K8S_IMAGE_REGISTRY` | `localhost` | Local image registry/name prefix rendered for kind and used when tagging images. |
| `K8S_IMAGE_TAG` | `dev-local` | Tag used for images loaded into kind. |
| `K8S_KIND_VALIDATE_STEPS` | `k8s-kind-up ... k8s-kind-run-repair-smoke` | Ordered targets run by `make k8s-kind-validate`. |
| `K8S_VALIDATE_DIAGNOSTICS` | `1` | Whether `make k8s-kind-validate` captures diagnostics after the first failed step. |
| `K8S_DIAGNOSTICS_DIR` | `artifacts/deploy/kubernetes/diagnostics` | Base directory for Kubernetes diagnostic bundles. |
| `K8S_DIAGNOSTICS_LOG_TAIL` | `200` | Per-pod log lines captured by diagnostics. |
| `K8S_API_LOCAL_PORT` | `18080` | Local port used by the canonical smoke API port-forward. |
| `K8S_SMOKE_JOB` | `examples/e2e-canonical.json` | Job submitted by the workload smoke harness. |
| `K8S_SMOKE_CLI_IMAGE` | `localhost/vectis-cli:dev-local` | CLI image used by the smoke harness to seed the encryptedfs secret. |
| `K8S_SMOKE_SEED_SECRET` | `true` | Whether the smoke harness seeds `encryptedfs://team/smoke-token`. |
| `K8S_WORKER_CORE_API_LOCAL_PORT` | `18081` | Local port used by the Kubernetes worker-core provider smoke API port-forward. |
| `K8S_WORKER_CORE_SMOKE_JOB` | `examples/e2e-kubernetes-worker-core.json` | Leaf shell job submitted while the worker uses the Kubernetes worker-core provider. |
| `K8S_WORKER_CORE_SMOKE_IMAGE` | `localhost/vectis-worker-core-kubernetes:dev-local` | Temporary worker-core sidecar image used by the provider smoke. |
| `K8S_WORKER_CORE_TASK_IMAGE` | `localhost/vectis-worker:dev-local` | Task container image used by the Kubernetes worker-core provider. |
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
| `CONTAINER_CMD` | `podman` | Runtime used to build and save images. |
| `IMAGE_REGISTRY` | unset | General image-build prefix; the kind target sets it from `K8S_IMAGE_REGISTRY`. |
| `KIND_PROVIDER` | `podman` | Provider passed to kind as `KIND_EXPERIMENTAL_PROVIDER`; use `auto` to let kind detect. |
| `KUBECTL` | `kubectl` | Kubernetes client used for apply, rollout, and smoke checks. |
| `KUBECONFIG` | unset | Optional kubeconfig path for isolated local clusters. |

For Docker or a Docker-compatible Colima profile:

```sh
make k8s-kind-validate CONTAINER_CMD=docker KIND_PROVIDER=docker
```

For nerdctl-backed setups:

```sh
make k8s-kind-validate CONTAINER_CMD=nerdctl KIND_PROVIDER=nerdctl
```

`make k8s-kind-validate` runs `K8S_KIND_VALIDATE_STEPS` in order.
`k8s-kind-validate-core` renders the manifest, applies it, waits for core
workload readiness, and runs the canonical workload smoke through
`go run ./deploy/kubernetes/validate`; the later targets run the specialized
worker-core, cancel, scale, orphan, and repair smokes. If a step fails and
`K8S_VALIDATE_DIAGNOSTICS` is not `0`, it writes a timestamped bundle under
`K8S_DIAGNOSTICS_DIR` with context metadata, cluster info, nodes, namespace
resources, events, pod descriptions, and current/previous pod logs. Run
`make k8s-kind-diagnostics` to capture the same bundle on demand.

The default manifest creates:

- a `vectis` namespace;
- Postgres with a PVC;
- Vectis registry, queue, orchestrator, log, artifact, secrets, API, docs,
  cron, reconciler, catalog, worker, and worker-core workloads;
- PVCs for queue persistence, log storage, artifact storage, and encryptedfs
  secret envelopes;
- generated development gRPC TLS and SPIFFE CA material in a Kubernetes Secret;
- a single worker pod with `vectis-worker` and `vectis-worker-core` side by side
  over a shared Unix socket and shared log spool, plus a local `vectis-spiffe`
  sidecar for per-execution SVIDs;
- worker registry registration using the pod IP and worker-control port so the
  API can issue fast cancel requests inside the cluster.

The default TLS and SPIFFE material is generated by the renderer for local
validation. The rendered pod templates include a checksum annotation for that
development PKI so a re-rendered TLS Secret rolls every TLS-speaking workload in
the same apply. Do not use it as a production PKI baseline. The next Kubernetes
slices should add:

- cell ingress once mTLS is configured;
- production-oriented certificate issuance and rotation hooks.

`make k8s-kind-run-smoke` seeds `encryptedfs://team/smoke-token`, triggers
`examples/e2e-canonical.json`, streams logs, and downloads the artifact outputs.
Pass `K8S_SMOKE_JOB=examples/e2e-kubernetes.json K8S_SMOKE_SEED_SECRET=false`
to run the lower-level compute/action-registry smoke without secret delivery.

`make k8s-kind-run-worker-core-smoke` temporarily grants the worker pod
namespace-scoped Job/pod-log RBAC, swaps the `worker-core` sidecar to
`vectis-worker-core-kubernetes`, submits `examples/e2e-kubernetes-worker-core.json`,
and verifies both the Vectis run and the Kubernetes task Job complete. The
harness restores the worker Deployment and deletes the temporary RBAC before
exiting.

`make k8s-kind-run-cancel-smoke` submits
`examples/e2e-kubernetes-cancel.json`, waits for the long-running task to start,
and requires the API cancel endpoint to use the worker-control fast path. The
smoke fails if cancellation falls back to the durable pending path.

`make k8s-kind-run-scale-smoke` temporarily scales `deployment/vectis-worker`,
submits `examples/e2e-kubernetes-scale.json`, and polls run task attempts until
the distributed branches have at least `K8S_SCALE_MIN_WORKERS` distinct active
`lease_owner` values. The harness waits for the run to succeed and restores the
original worker replica count before exiting.

`make k8s-kind-run-orphan-smoke` temporarily scales `deployment/vectis-worker`
to one replica, sets `VECTIS_WORKER_EXECUTION_LEASE_TTL` to
`K8S_ORPHAN_LEASE_TTL`, submits `examples/e2e-kubernetes-orphan.json`, waits for
the root task to have an active `lease_owner`, force-deletes that worker pod,
and requires the run to become and remain `orphaned` with a single task attempt.
The harness restores the original worker lease TTL setting and replica count
before exiting.

`make k8s-kind-run-repair-smoke` uses the same pod-loss orphan setup twice.
First it submits `examples/e2e-kubernetes-repair.json`, force-deletes the owning
worker pod, waits for the run to become `orphaned`, explicitly calls
`POST /api/v1/runs/{id}/force-requeue`, and requires the second root attempt to
succeed. Then it creates another orphaned run and verifies
`POST /api/v1/runs/{id}/repair/mark-abandoned` moves it to `abandoned` without
creating another task attempt.

The smoke harness is a Go entrypoint and can also be run directly:

```sh
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --worker-core-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --cancel-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --scale-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --orphan-only
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --repair-only
```

Build local component images before loading or pushing them to a cluster:

```sh
make k8s-kind-build-images
```

Use `--image-registry` and `--image-tag` when rendering manifests for an image
registry:

```sh
go run ./cmd/cli deploy kubernetes render \
  --image-registry registry.example.com/vectis \
  --image-tag "$(git rev-parse --short=12 HEAD)" \
  --output artifacts/deploy/kubernetes/vectis.yaml
```

The default rendered Secret values are development placeholders. Override them
with `--postgres-password`, `--bootstrap-token`, and `--encryptedfs-key` before
using the manifest in a shared cluster.
