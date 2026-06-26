# Vectis Extensions

This directory contains standard extension implementations that are maintained
with Vectis but are not part of the core control plane.

Core Vectis owns durable workflow state, leases, policy gates, identity checks,
task finalization, audit, and service-to-service contracts. Extensions plug into
explicit contracts so integrations can be reused without becoming hidden core
dependencies.

| Directory | Contract | Purpose |
| --- | --- | --- |
| `actions/` | Action registry descriptors and future action runtime packages | Workflow vocabulary integrations such as review/report/deploy actions. |
| `secrets/` | Future public secrets-provider SDK or provider protocol | Secret-store integrations that run behind the Vectis broker authorization path. |
| `worker-core/` | `sdk/workercore` plus `api/proto/worker_core.proto` | Execution backends that run claimed Vectis tasks somewhere other than the default host core. |

Use `examples/` for minimal teaching samples. Use `extensions/` for reusable
implementations that should be built, tested, documented, and packaged as
first-class Vectis extension artifacts.

The boundary is enforced by tests in this directory. Extension Go packages may
import public Vectis SDK and generated API packages, but they must not import
`vectis/internal/...`, `cmd/...`, or other core implementation packages.
