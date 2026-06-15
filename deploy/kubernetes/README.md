# Kubernetes Reference Deployment

This directory contains the first Kubernetes reference deployment for Vectis.
It is intentionally a simple single-cell manifest that proves the core stack can
run under Kubernetes before adding mTLS, cell ingress, worker-control addressing,
or external providers.

Render the manifest:

```sh
go run ./cmd/cli deploy kubernetes render --output artifacts/deploy/kubernetes/vectis.yaml
```

Or:

```sh
make deploy-kubernetes-render
```

For local cluster validation, the repo treats kind as the Kubernetes provider
and keeps the container runtime configurable. The defaults match the existing
Podman-based build posture:

```sh
make k8s-kind-up
make k8s-kind-load-images
make k8s-kind-apply
make k8s-kind-smoke
```

The local contract is:

| Variable | Default | Purpose |
|---|---:|---|
| `K8S_PROVIDER` | `kind` | Dispatch target for generic `k8s-*` aliases. |
| `K8S_CLUSTER` | `vectis` | Local cluster name. |
| `K8S_NAMESPACE` | `vectis` | Namespace rendered into the manifest. |
| `K8S_IMAGE_REGISTRY` | `localhost` | Local image registry/name prefix rendered for kind and used when tagging images. |
| `K8S_IMAGE_TAG` | `dev-local` | Tag used for images loaded into kind. |
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

The default manifest creates:

- a `vectis` namespace;
- Postgres with a PVC;
- Vectis registry, queue, orchestrator, log, artifact, secrets, API, docs,
  cron, reconciler, catalog, worker, and worker-core workloads;
- PVCs for queue persistence, log storage, artifact storage, and encryptedfs
  secret envelopes;
- a single worker pod with `vectis-worker` and `vectis-worker-core` side by side
  over a shared Unix socket.

The first manifest keeps internal gRPC plaintext inside the namespace. Do not use
it as a production security baseline. The next Kubernetes slices should add:

- internal gRPC/mTLS material and SPIFFE bootstrap;
- cell ingress once mTLS is configured;
- Kubernetes-native worker-control publish addresses for fast cancel;
- cluster smoke tests that apply the manifest, trigger a canonical job, stream
  logs, and download an artifact.

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
