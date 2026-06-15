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

The first manifest is a single-cell deployment. It includes Postgres, registry,
queue, orchestrator, log, artifact, secrets, API, docs, cron, reconciler,
catalog, and a worker pod that runs `vectis-worker` beside
`vectis-worker-core` over a shared Unix socket.

## Current Scope

The manifest is meant for local or staging validation. It deliberately does not
claim production security posture yet:

- internal gRPC is plaintext inside the namespace;
- `vectis-cell-ingress` is not exposed until the Kubernetes mTLS/SPIFFE lane is
  added;
- worker registry registration is disabled because Kubernetes worker pods need a
  real publish-address strategy for worker-control fast cancel;
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

The Kubernetes deployment lane should grow in this order:

1. Apply the rendered manifest to a local cluster and wait for all core workloads.
2. Trigger the canonical job and verify run status, logs, artifact upload, and
   artifact download.
3. Scale the worker deployment and verify parallel task throughput.
4. Delete a worker pod during a running task and verify recovery behavior.
5. Add internal mTLS/SPIFFE and then expose cell ingress.

For the current security posture and service dependencies, see
[Security](../../concepts/security.md) and
[Failure Domains](../../concepts/failure-domains.md).
