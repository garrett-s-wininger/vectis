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
container runtime with Make variables:

```sh
make k8s-kind-up
make k8s-kind-load-images
make k8s-kind-apply
make k8s-kind-smoke
make k8s-kind-run-smoke
make k8s-kind-run-cancel-smoke
```

The generic `k8s-*` aliases dispatch through `K8S_PROVIDER`, which defaults to
`kind`. The kind lane defaults to Podman because the rest of the reference
container tooling already uses Podman:

| Variable | Default | Purpose |
|---|---:|---|
| `K8S_PROVIDER` | `kind` | Provider used by generic `k8s-*` aliases. |
| `K8S_CLUSTER` | `vectis` | Local cluster name. |
| `K8S_NAMESPACE` | `vectis` | Namespace rendered into the manifest. |
| `K8S_IMAGE_REGISTRY` | `localhost` | Local image registry/name prefix rendered for kind and used when tagging images. |
| `K8S_IMAGE_TAG` | `dev-local` | Local image tag built and loaded into kind. |
| `K8S_API_LOCAL_PORT` | `18080` | Local port used by the canonical smoke API port-forward. |
| `K8S_SMOKE_JOB` | `examples/e2e-canonical.json` | Job submitted by the workload smoke harness. |
| `K8S_SMOKE_CLI_IMAGE` | `localhost/vectis-cli:dev-local` | CLI image used by the smoke harness to seed the encryptedfs secret. |
| `K8S_SMOKE_SEED_SECRET` | `true` | Whether the smoke harness seeds `encryptedfs://team/smoke-token`. |
| `K8S_CANCEL_API_LOCAL_PORT` | `18082` | Local port used by the worker-control cancel smoke API port-forward. |
| `K8S_CANCEL_SMOKE_JOB` | `examples/e2e-kubernetes-cancel.json` | Long-running job submitted by the worker-control cancel smoke harness. |
| `CONTAINER_CMD` | `podman` | Runtime command used to build and save images. |
| `IMAGE_REGISTRY` | unset | General image-build prefix; the kind target sets it from `K8S_IMAGE_REGISTRY`. |
| `KIND_PROVIDER` | `podman` | Provider passed to kind as `KIND_EXPERIMENTAL_PROVIDER`; set `auto` for kind autodetection. |
| `KUBECTL` | `kubectl` | Kubernetes client command. |
| `KUBECONFIG` | unset | Optional kubeconfig path for isolated local clusters. |

Docker or Docker-compatible Colima profiles can use the same targets:

```sh
make k8s-kind-validate CONTAINER_CMD=docker KIND_PROVIDER=docker
```

The workload smoke is a Go harness under the Make target:

```sh
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis
go run ./deploy/kubernetes/smoke --context kind-vectis --namespace vectis --cancel-only
```

The first manifest is a single-cell deployment. It includes Postgres, registry,
queue, orchestrator, log, artifact, secrets, API, docs, cron, reconciler,
catalog, and a worker pod that runs `vectis-worker` beside
`vectis-worker-core` over a shared Unix socket. The worker pod also runs a
local `vectis-spiffe` sidecar so secret resolution uses per-execution SVIDs over
internal gRPC mTLS. The worker registers its pod IP and worker-control port with
the registry so the API can issue fast cancel requests inside the cluster.

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

The Kubernetes deployment lane now validates the core workload path and
worker-control cancellation. The next useful checks are:

1. Scale the worker deployment and verify parallel task throughput.
2. Delete a worker pod during a running task and verify recovery behavior.
3. Expose cell ingress once the mTLS edge contract is ready.

For the current security posture and service dependencies, see
[Security](../../concepts/security.md) and
[Failure Domains](../../concepts/failure-domains.md).
