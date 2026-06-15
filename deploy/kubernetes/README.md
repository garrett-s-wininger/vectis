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
make images-components
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
