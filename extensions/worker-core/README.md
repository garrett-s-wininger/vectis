# Worker Core Extensions

Worker core extensions implement the supported execution-provider boundary.
They should depend on the public SDK and protocol surface:

- `sdk/workercore`
- `sdk/workercore/conformance` for tests
- generated protobuf types from `api/gen/go`
- raw protobuf definitions under `api/proto/` for non-Go consumers

They should not depend on `internal/workercore`, `internal/job`,
`internal/dal`, or other Vectis control-plane internals. The worker owns queue
claims, execution leases, secret/SPIFFE gates, action locks, log/artifact
callbacks, cancellation intent, and final task state. A worker core owns only the
provider-specific execution of the claimed task session it receives.

The Kubernetes implementation in `kubernetes/` is the first standard extension.
The smaller `examples/worker-core-external` program remains a teaching example
for the SDK shape.
