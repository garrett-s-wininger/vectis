# Internal gRPC Service Reference

Vectis uses protobuf/gRPC for internal service-to-service contracts. These surfaces are not browser-facing APIs; keep them on private networks or behind internal mTLS.

The source of truth is `api/proto/`. Generated Go in `api/gen/go/` is committed for consumers, but it is generated output and should not be edited by hand. Run `mage proto` after changing protobuf definitions.

For the public HTTP API, see [API Reference](../../using/api-reference.md) and [OpenAPI Specification](../../using/openapi-specification.md). For network trust and service identity policy, see [Internal Service Trust](../../concepts/internal-service-trust.md).

## Service Inventory

| Service | Owner | Proto | RPCs | Default listener | Main callers |
| --- | --- | --- | --- | --- | --- |
| `QueueService` | `vectis-queue` | `api/proto/queue.proto` | `Enqueue`, `Dequeue`, `TryDequeue`, `Ack`, `ListDeadLetter`, `RequeueDeadLetter` | `queue.port` (`8081`) | API, cron, reconciler, cell ingress, workers |
| `RegistryService` | `vectis-registry` | `api/proto/registry.proto` | `Register`, `GetAddress`, `ListRegistrations`, `Gossip`, `GetSnapshot` | `registry.port` (`8082`) | Services that register or resolve queue, orchestrator, log, artifact, and worker-control addresses |
| `LogService` | `vectis-log` | `api/proto/log.proto` | `StreamLogs`, `SendLogBatch`, `GetLogs` | `log.grpc.port` (`8083`) | Workers and log-forwarders write logs; API reads logs for SSE clients |
| `ArtifactService` | `vectis-artifact` | `api/proto/artifact.proto` | `UploadBlob`, `StatBlob`, `ReadBlob` | `artifact.grpc.port` (`8086`) | Workers upload artifacts; API reads blobs for authenticated downloads |
| `OrchestratorService` | `vectis-orchestrator` | `api/proto/orchestrator.proto` | `LoadRun`, `ListPending`, `ClaimExecution`, `RenewExecutionLease`, `CompleteExecution`, `GetRunTaskCompletion`, `GetRunTaskSnapshot`, `ExecutionStream` | `orchestrator.port` (`8087`) | Workers claiming, leasing, completing, and reducing task executions |
| `SecretsService` | `vectis-secrets` | `api/proto/secrets.proto` | `ResolveSecrets` | `secrets.port` (`8090`) | Workers resolving task-scoped job secret files |
| `WorkerControlService` | `vectis-worker` | `api/proto/worker_control.proto` | `CancelRun` | `worker.control.port` (`9084`) or `worker.control.port_min` to `worker.control.port_max` | API cancellation path through worker-control discovery |
| `WorkerCoreService` | `vectis-worker-core` | `api/proto/worker_core.proto` | `DescribeCore`, `ExecuteTask`, `CancelTask`, `WarmCheckoutCache` | `worker.core.socket` Unix socket | Worker execution manager |
| `WorkerCoreShellService` | `vectis-worker` shell callback listener | `api/proto/worker_core.proto` | `StreamLogs`, `PublishArtifact` | `worker.core.shell_socket` Unix socket | Worker-core callbacks for durable logs and artifact publication |

`vectis-cell-ingress` is a private HTTP ingress, not a protobuf service. It accepts cell-local execution submissions on `cell_ingress.port` (`8085`) and uses the queue internally.

## RPC Semantics

### `QueueService`

`QueueService` buffers execution deliveries. Producers call `Enqueue`; workers call `Dequeue` or `TryDequeue` with advertised execution capabilities and then `Ack` a delivery after they can prove they accepted it. Dead-letter RPCs expose failed deliveries for inspection and repair.

The database remains the durable source of truth for run and task state. Queue delivery is allowed to be duplicated during retry and repair; execution claims fence duplicate execution.

### `RegistryService`

`RegistryService` is internal service discovery. Queue, log, artifact, orchestrator, and worker-control endpoints can register themselves, and clients can resolve the address for a service kind when a pinned address is not configured.

Registry discovery is convenient, but it becomes a trust dependency. Operators can avoid that dependency for many paths by configuring pinned addresses.

### `LogService`

`LogService.StreamLogs` ingests worker log chunks, while `SendLogBatch` accepts already-batched encoded records from forwarders. `LogService.GetLogs` streams stored chunks back to the API, which converts them into public SSE log streams. Clients should prefer API/RBAC-mediated log access over dialing the log service directly.

### `ArtifactService`

`ArtifactService.UploadBlob` streams artifact bytes to the content-addressed artifact service. `StatBlob` and `ReadBlob` let the API verify and serve the exact blob referenced by SQL artifact manifests. Artifact names are API metadata; blob lookup is by service-managed digest metadata.

### `OrchestratorService`

`OrchestratorService` owns hot run and task choreography. Workers load pending execution state, claim execution leases, renew active leases, complete executions, ask for run task completion summaries during fan-in and repair, and use `ExecutionStream` to keep repeated worker/orchestrator round trips on one bidi stream. API read routing can use `GetRunTaskSnapshot` for hot task status before falling back to SQL.

### `SecretsService`

`SecretsService.ResolveSecrets` is the worker-to-broker secret resolution path. The request includes `run_id`, `execution_id`, `execution_claim_token`, and selected job secret references. The broker authorizes the caller using mTLS peer SPIFFE identity plus active execution state before returning file material.

See [Secrets Reference](../../using/secrets-reference.md) for the full file delivery and authorization contract.

### `WorkerControlService`

`WorkerControlService.CancelRun` is the worker-owned cancellation control path. The API records cancellation intent in SQL and then routes a cancel request to the worker that owns the run when worker-control registration/discovery is available.

### `WorkerCoreService`

`WorkerCoreService` is the local worker-to-worker-core contract. The worker asks the core to describe supported execution isolation, execute a task, cancel a task, and warm persistent checkout cache remotes. The default transport is a Unix-domain socket rather than a network listener.

### `WorkerCoreShellService`

`WorkerCoreShellService` is the local callback contract exposed by the worker for worker-core. It lets worker-core stream logs and publish artifacts back through worker-owned durable paths. This keeps log/artifact durability and run state updates under the worker process.

## Discovery And Pinning

| Path | Registry-capable default | Pinning config |
| --- | --- | --- |
| Queue | `queue.register_with_registry=true` | `queue.advertise_address`; producer-specific queue addresses such as cell ingress or cron queue config |
| Orchestrator | `orchestrator.register_with_registry=true` | `orchestrator.advertise_address`; `discovery.orchestrator.address` or worker orchestrator address config |
| Log gRPC | `log.grpc.register_with_registry=true` | `log.grpc.advertise_address`; API/worker log address config |
| Artifact gRPC | `artifact.grpc.register_with_registry=true` | `artifact.grpc.advertise_address`; API/worker artifact resolver config |
| Worker control | `worker.register_with_registry=true` | `worker.control.mode`, `worker.control.port`, `worker.control.port_min`, `worker.control.port_max` |

When registry discovery is disabled or not desired, configure callers with fixed addresses and keep the registry out of that path. In split global/cell deployments, cell ingress endpoints are configured as HTTP URLs rather than registry records.

## TLS And Service Identity

Internal gRPC TLS is controlled by `grpc_tls.*`. Local development defaults to `grpc_tls.insecure` set to `true`; shared or production-like deployments should use TLS or mTLS.

When `grpc_tls.insecure=false`, internal gRPC listeners use `grpc_tls.cert_file` and `grpc_tls.key_file`; mTLS listeners also use `grpc_tls.client_ca_file` so peer certificates can be verified. Clients use `grpc_tls.ca_file`, and client-certificate paths use `grpc_tls.client_cert_file` and `grpc_tls.client_key_file` unless a workload SVID supplies the certificate.

Service identity allowlists are exact SPIFFE URI allowlists:

| Role | Config key |
| --- | --- |
| Registry | `service_identity.registry_allowed_client_identities` |
| Queue | `service_identity.queue_allowed_client_identities` |
| Log | `service_identity.log_allowed_client_identities` |
| Artifact | `service_identity.artifact_allowed_client_identities` |
| Orchestrator | `service_identity.orchestrator_allowed_client_identities` |
| Worker control | `service_identity.worker_control_allowed_client_identities` |
| Secrets | `service_identity.secrets_allowed_client_identities` |

`vectis-secrets` also performs per-execution authorization inside `ResolveSecrets`; keep `service_identity.secrets_allowed_client_identities` empty unless adding a deliberate static second gate for known caller identities.

## Compatibility

Protobuf compatibility follows the project compatibility contract:

| Compatible additive change | Breaking without release-note coordination |
| --- | --- |
| Add a field with a new tag number. | Reuse a tag number for a different meaning. |
| Add an enum value when old receivers tolerate it. | Change an existing field type or meaning. |
| Add a new RPC or service without changing existing behavior. | Remove or rename an existing RPC, service, field, or enum value without a coordinated migration. |
| Reserve removed tags and names when removal is unavoidable. | Remove a field without reserving its tag and name. |

Run `mage proto` when protobuf definitions change, and commit the generated `api/gen/go/` updates with the proto change.

## Related Documentation

| Need | Document |
| --- | --- |
| Public HTTP API | [API Reference](../../using/api-reference.md) |
| HTTP OpenAPI document | [OpenAPI Specification](../../using/openapi-specification.md) |
| Internal trust boundaries | [Internal Service Trust](../../concepts/internal-service-trust.md) |
| Config defaults and keys | [Configuration Key Reference](./configuration-key-reference.md) |
| Log streaming contract | [SSE And Streaming Reference](../../using/streaming-reference.md) |
| Artifact behavior | [Artifacts](../../using/artifacts.md) |
| Secret resolution | [Secrets Reference](../../using/secrets-reference.md) |
