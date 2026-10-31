# Development Environment

Use this page to set up a local machine for working on Vectis itself. The goal is a predictable build, a useful local SQLite test lane, and a fast feedback loop on both Unix-like systems and Windows.

## Preflight Doctor

Run the doctor script for your platform before the first build and whenever the local toolchain changes:

```bash
scripts/dev-doctor.sh
```

```powershell
.\scripts\dev-doctor.ps1
```

The doctor checks Go, Git, Mage, protobuf tooling, SQLite/CGO readiness, optional frontend tooling, optional container/formal tools, and Windows symlink support.

On Unix-like systems, this installs the standard local toolchain and writes `.tools/env.sh`:

```bash
scripts/dev-doctor.sh --install --yes
. .tools/env.sh
```

`./.tools/bin/mage` uses the Go installed under `.tools/go` without a PATH change. Source `.tools/env.sh` when you want `go`, `mage`, and `protoc` as bare commands in the current shell.

Use `--install-go-tools` on Unix or `-InstallGoTools` on Windows when you only need Mage and protobuf Go plugins installed through `go install`. On Unix, add `--install-frontend` when you also want repo-local Node.js for docs/UI work.

## Unix

The normal Unix setup is:

```bash
scripts/dev-doctor.sh --install --yes
. .tools/env.sh
mage testQuick
```

Docs and browser UI work need Node.js `20.19+` and npm. To install repo-local Node.js for that lane:

```bash
scripts/dev-doctor.sh --install-frontend
. .tools/env.sh
mage buildFrontend
```

Local SQLite tests need CGO and a C compiler. On common Linux distributions the doctor installer pulls in GCC and libc development headers. macOS developers should have Xcode Command Line Tools available.

Most Unix developers can use the system default temp directory. If your `/tmp` is remote, encrypted, very slow, or mounted with restrictions that affect tests, create a fast local temp directory and opt in for test commands:

```bash
mkdir -p "$HOME/.cache/vectis/tmp"
export VECTIS_TEST_TEMPDIR="$HOME/.cache/vectis/tmp"
mage testQuick
```

## Windows

Use PowerShell for the supported Windows development lane:

```powershell
.\scripts\dev-doctor.ps1
```

For local SQLite tests, Go's CGO path needs a GCC-compatible C compiler. Use MinGW/UCRT GCC from MSYS2 or LLVM `clang` in GCC-compatible mode. MSVC `cl.exe` and `clang-cl` are not supported by this SQLite CGO lane. After installing a compiler, open a new PowerShell session or refresh `PATH` before rerunning the doctor.

Enable Windows Developer Mode, or run from an elevated shell, so checkout-cache tests can create directory symlinks. Without symlink permission those tests skip the symlink-dependent path, which is useful for a partial check but not the full Windows development signal.

For best test performance, use a Dev Drive or another fast local filesystem for the repository, Go caches, and optional Vectis test temp directory. Example:

```powershell
go env -w GOCACHE=D:\Caches\go-build
go env -w GOMODCACHE=D:\Caches\go-mod
New-Item -ItemType Directory -Force D:\Caches\tmp | Out-Null
$env:VECTIS_TEST_TEMPDIR = 'D:\Caches\tmp'
mage testQuick
```

`VECTIS_TEST_TEMPDIR` is opt-in and cross-platform. When set, Mage passes it to Go tests as `GOTMPDIR`, `TEMP`, `TMP`, and `TMPDIR`, which keeps Go scratch files and `os.TempDir()` users on the chosen filesystem. When unset, tests use the normal OS temp location. Set it to `0`, `off`, or `false` to disable the override in a shell that inherited it.

To persist the override for future PowerShell sessions:

```powershell
[Environment]::SetEnvironmentVariable('VECTIS_TEST_TEMPDIR', 'D:\Caches\tmp', 'User')
```

## Build Lanes

Use the lane that matches the work:

```bash
mage build         # local Go lane: backend services, local supervisor, workers, SCM services, CLI
mage buildDocs     # docs assets + vectis-docs
mage buildUI       # browser UI assets + vectis-ui
mage buildFrontend # docs + UI assets and serving binaries
mage buildFull     # local Go lane plus docs/UI assets and binaries
mage buildContainer # nosqlite container-profile binaries, no local npm
```

`mage build` does not require Node.js or npm. The frontend targets require Node.js `20.19+` and npm. Container image targets build the web assets they package inside the Containerfile, so the host needs a container engine rather than a local frontend toolchain.

## Test Loop

Use the smallest test command that gives useful feedback:

```bash
mage testQuick
mage testFault
mage testProperty
mage test
```

On Windows, `mage testQuick` defaults to a longer timeout because filesystem and process creation are slower than on Unix. Keeping `GOCACHE`, `GOMODCACHE`, and optionally `VECTIS_TEST_TEMPDIR` on a fast local filesystem gives the best feedback loop.

### CI Scope And Dependency Checks

Routine pull requests use the existing quick, vulnerability, and docs jobs.
`mage auditDependencies` audits both JavaScript lockfiles without installing
packages and fails on high or critical advisories. Lower-severity findings are
still printed for triage. `mage vulncheck` remains the Go reachability check.

UI code and API-contract changes additionally run `mage testUI` with two workers
and a UI build in the existing quick job. Docs accessibility runs when website,
docs-serving, security middleware, or workflow files change. Dependency install
caches are reused, superseded runs are cancelled, and job timeouts cap runaway
checks. These path filters reduce ordinary pull-request runner usage.

For heavier validation, manually run the CI workflow with `deep_checks=true`.
That optional job runs Postgres integration with missing prerequisites treated
as errors, followed by focused worker/orchestrator/DAL race tests. It is not a
scheduled or per-pull-request job. The same checks remain runnable locally:

```bash
mage auditDependencies
mage testUI
VECTIS_REQUIRE_POSTGRES_TESTS=true mage testPostgresIntegration
go test -race -short -count=1 -timeout=120s ./cmd/worker ./internal/orchestrator ./internal/dal
```

Docs dependencies keep official Docusaurus packages on one patch release. The
Tinypool and serialization overrides address transitive advisories; validate
upgrades with `DOCUSAURUS_SSG_WORKER_THREAD_COUNT=2 npm run build:workers` in `website` so the
worker-pool override is exercised. The lockfile audit is separate from the UI
and docs builds: neither a successful build nor an audit establishes the other.

### Deterministic Failure And Recovery Tests

`mage testFault` runs a focused regression suite without containers. Failures are
injected at named storage or RPC boundaries, leases are advanced explicitly where
supported, and tests assert the resulting durable and in-memory state. Timeouts
bound a hung test; elapsed time does not trigger the new RPC failure scenarios.

| Failure boundary | Coverage | Required result |
| --- | --- | --- |
| Queue persistence fails during enqueue, delivery, or acknowledgment | `internal/queue/server_fault_test.go` | Failed writes do not advance in-memory state; restart preserves recoverable deliveries. |
| Queue state is lost or restored behind SQL | `internal/reconciler/reconciler_restore_test.go` | Frozen SQL payloads restore dispatch; an active durable claim suppresses redispatch. |
| Worker lease expires or orchestrator state disappears | Worker restart and orchestrator recovery tests in `cmd/worker/main_test.go` | Recovery respects claim fencing and reconstructs the needed state. |
| Completion takes effect but its response is lost | `TestWorkerFault_DurableCompletionAcrossRPCFailures` | Retrying returns the accepted terminal snapshot; one task execution and one durable final fact remain. |
| SQL commits before acknowledgment request or response is lost | The same RPC fault matrix | A fresh worker delivers the durable receipt; graph reclamation and receipt deletion complete without changing the run outcome. |
| Orchestrator restarts after SQL commits, before receipt recovery | The same RPC fault matrix | Receipts for absent graphs are consumed safely; delayed deliveries do not rerun the task. |
| Final snapshot transaction fails | `TestTerminalPersistenceFailureDoesNotReleaseState` | Run state rolls back and the graph stays available until persistence succeeds. |
| A fan-out completion takes effect but its response is lost | `TestWorkerFault_LostFanoutCompletionResponse` | Accepted completion is replayed over either RPC transport; activated children reach the queue without rerunning the parent. |
| An old final snapshot arrives after requeue | `TestRunsRepository_TerminalSnapshotFencesRetryGeneration` and its Postgres counterpart | A snapshot from an older root attempt is rejected before writes; the current claim and final facts remain intact. |
| A delayed delivery targets a terminal run | `TestWorkerFault_TerminalRunCannotExecuteDelayedDelivery` | Every terminal status, including `aborted` and `abandoned`, skips execution and graph loading. |
| A late orphan report arrives after completion | `TestWorkerFault_LateOrphanPreservesTerminalResult` and Postgres retirement contracts | Terminal SQL outcomes remain unchanged. |
| A snapshot batch changes active identity or contains a later invalid entry | `internal/orchestrator/snapshot_atomicity_test.go` | Active claims retain their identity; a rejected batch leaves the graph unchanged. |
| A choreography response stalls while another caller waits | `TestWorkerFault_StalledStreamCancellationReleasesGate` | Cancellation releases the gate, resets the stream, and permits reconnection; production exchanges have a timeout. |
| A late root admission is rejected after graph reclamation | Admission fault and retirement tests | A durable claim-fenced discard receipt releases the graph, including after worker replacement. |
| Acknowledgment is duplicated or belongs to an old generation | `internal/orchestrator/retention_test.go` | Reclamation is idempotent and cannot remove a newer graph. |
| Log forwarding acknowledgment is lost | `internal/logforwarder/forwarder_fault_test.go` | Durable spool remains available for retry. |

The RPC matrix covers successful, failed, and cancelled task outcomes over both
the execution stream and unary RPCs: 24 scenarios. It uses real worker execution
and finalization code, an in-memory gRPC transport, and migrated SQLite. Faults
occur synchronously at the selected boundary; worker replacement starts with no
in-memory finalization result. The tests also run in `mage testQuick`.

To repeat this matrix under the race detector:

```bash
go test -race -count=10 -timeout=120s ./cmd/worker -run '^TestWorkerFault_DurableCompletionAcrossRPCFailures$'
```

This suite proves the listed recovery transitions. Postgres behavior remains in
`mage testPostgresIntegration`; operating-system process termination, full
backup/restore, deployment topology, and capacity require their corresponding
integration or deployment tests. The execution-count assertion applies to these
tested RPC failures, not arbitrary external side effects interrupted by a crash.

For narrow work, prefer package-level tests:

```bash
go test ./internal/api/...
```
