# Tests

**Commands** are defined in Mage (`../mage_*.go`):

| Target | Scope | Notes |
|--------|-------|-------|
| `test` | All packages | No timeout, no race |
| `testQuick` | `internal/...` `cmd/...` `api/...` `sdk/...` `examples/...` `tools/...` | `-count=1`; timeout defaults to `60s` on Unix and `600s` on Windows |
| `testIntegration` | Packages with `//go:build integration` | Requires Postgres (see `VECTIS_DATABASE_DSN`) |
| `testE2E` | Packages with `//go:build e2e` | Starts live binaries/stacks such as the Podman reference deployment |
| `vmValidate` | Prepared VM static validation | Runs Packer script regression tests and `packer validate`; does not boot guests |
| `vmStatus` | Prepared VM inventory | Read-only VM lane status; does not start stopped guests |
| `vmDoctor` | Prepared VM health check | Starts stopped prepared guests long enough to verify markers and tooling, then stops any VM it started |
| `vmCheck` | Prepared VM health check | Umbrella target for `vmDoctor`; individual VM check targets select one lane |
| `testPostgresIntegration` | `tests/integration/postgres` | Starts `postgres:18-alpine` with testcontainers |
| `testWindowsCompile` | All packages | Cross-compiles the `nosqlite` Windows package graph with a no-op test executor |
| `testWindowsSQLiteCompile` | All packages | Compile-checks the Windows CGO/SQLite package graph with a no-op test executor; needs native Windows CGO with a GCC-compatible compiler or a cross C compiler such as Zig |
| `testRace` | All packages | `-race` flag |
| `fuzzAPIAuth` | API auth fuzz targets | `FUZZTIME` (default 30s) |

`VECTIS_TEST_TEMPDIR` is an opt-in cross-platform test scratch override. When set, Mage passes it to Go tests as `GOTMPDIR`, `TEMP`, `TMP`, and `TMPDIR`; when unset, tests use the normal OS temp location. This is useful on Windows Dev Drives and on Unix hosts where `/tmp` is slow or restricted.

## Style

- **Standard `testing` only** — no testify/ginkgo. This avoids external test framework dependency and keeps test output uniform.
- **Table-driven tests** where there are multiple input/output cases.
- **`t.Helper()`** on helper functions.
- **`t.Parallel()`** when safe (not sharing state).
- **Test files** live next to the code they test (`xxx_test.go` in the same package).
- **Windows symlinks:** checkout-cache tests exercise directory symlinks when Windows Developer Mode or an elevated shell grants symlink creation; otherwise they skip the symlink-dependent cases.

## Integration tests

Integration packages need `//go:build integration` at the top of every file and live under [`integration/`](integration/). Package names under `integration/` are the source of truth for what integration tests exist.

Uses [`../internal/testutil/grpctest/`](../internal/testutil/grpctest/) for gRPC server setup. Example: [`integration/queue/server_test.go`](integration/queue/server_test.go). The `grpctest.SetupGRPCServer` function handles listening on an ephemeral port and returning a `*grpc.ClientConn`.

**Postgres lane:** `tests/integration/postgres` uses [`internal/testutil/pgtest`](../internal/testutil/pgtest/) to start `postgres:18-alpine` with testcontainers, apply embedded migrations, and skip cleanly when no local container runtime is available. Set `VECTIS_REQUIRE_POSTGRES_TESTS=true` to fail instead of skip. Keep the image pre-pulled on dogfood hosts when runs must avoid network access.

## E2E tests

E2E packages need `//go:build e2e` at the top of every file and live under [`e2e/`](e2e/). They exercise live binaries or full local stacks and are intentionally outside the normal integration lane.

The local e2e lane expects the host `bin/vectis-local` and `bin/vectis-cli` binaries to already exist. It starts `vectis-local --http-tls=off --docs=false` with internal gRPC TLS/SPIFFE enabled, seeds the local encryptedfs secret store, creates and triggers [`../examples/e2e-canonical.json`](../examples/e2e-canonical.json), waits for the run to report `running` and then `succeeded`, and verifies task completion, run logs, action-registry retry behavior, secret delivery, artifact manifests, and artifact download. Stop any existing local Vectis stack first; the smoke uses the default local service ports.

The Podman e2e lane expects the host `bin/vectis-cli` binary and local Podman images to already exist. A typical prep loop is:

```sh
mage build
mage imagesComponents
podman pull docker.io/library/alpine:3.21
podman pull docker.io/library/postgres:18-alpine
podman pull docker.io/prom/prometheus:v3.11.0-distroless
podman pull docker.io/opensearchproject/opensearch:2.19.1
podman pull docker.io/fluent/fluent-bit:5.0.4
podman pull cr.jaegertracing.io/jaegertracing/jaeger:2.17.0
podman pull docker.io/opensearchproject/opensearch-dashboards:2.19.1
podman pull docker.io/grafana/grafana:13.0.0-23943897787
VECTIS_E2E_PODMAN_RESET=true mage testE2E
```

Useful e2e controls:

| Variable | Meaning |
|---|---|
| `VECTIS_E2E_CLI` | Override the host CLI binary path; defaults to `bin/vectis-cli`. |
| `VECTIS_E2E_LOCAL` | Override the host local supervisor binary path; defaults to `bin/vectis-local`. |
| `VECTIS_E2E_REQUIRE=true` | Fail instead of skip when prerequisites are missing. |
| `VECTIS_E2E_KEEP_LOCAL=true` | Leave `vectis-local` running after the local e2e for debugging. |
| `VECTIS_E2E_ENVELOPE=true` | Run the opt-in local deployment envelope test in addition to the smoke; it drives concurrent canonical E2E runs through `vectis-local`. |
| `VECTIS_E2E_ENVELOPE_RUNS` | Number of canonical E2E runs in the local envelope test (default `20`). |
| `VECTIS_E2E_ENVELOPE_TRIGGER_CLIENTS` | Concurrent trigger clients for the local envelope test (default `4`). |
| `VECTIS_E2E_ENVELOPE_STATUS_READERS` | Concurrent `/api/v1/runs/{id}` readers during the local envelope test (default `4`). |
| `VECTIS_E2E_ENVELOPE_STATUS_READS_PER_RUN` | Status reads enqueued per triggered run during the local envelope test (default `2`). |
| `VECTIS_E2E_ENVELOPE_LOG_REPLAY_RUNS` | Number of terminal runs to replay logs for during the local envelope test (default `5`). |
| `VECTIS_E2E_ENVELOPE_ARTIFACT_DOWNLOAD_RUNS` | Number of terminal runs to download both canonical artifacts for during the local envelope test (default `5`). |
| `VECTIS_E2E_ENVELOPE_MIN_RUNS_PER_SECOND` | Optional failing lower bound for local envelope throughput; unset or `0` records only. |
| `VECTIS_E2E_PODMAN_RESET=true` | Allow the Podman e2e to remove and recreate the fixed `vectis` pod/volumes. |
| `VECTIS_E2E_KEEP_PODMAN=true` | Leave the Podman stack up after the test for debugging. |
| `VECTIS_E2E_ALLOW_IMAGE_PULL=true` | Skip local image preflight and let Podman pull missing `IfNotPresent` images. |
| `VECTIS_E2E_DEPLOY_LINUX_PROVIDER` | Linux deploy VM provider; defaults to `auto` (currently Lima). |
| `VECTIS_E2E_DEPLOY_LINUX_PROVIDER_PATH` | Override the VM provider command path, such as `limactl`. |
| `VECTIS_E2E_DEPLOY_LINUX_INSTANCE` | Override the prepared Linux deploy smoke VM instance name. |
| `VECTIS_E2E_DEPLOY_LINUX_TIMEOUT` | Timeout for the Linux deploy VM smoke; defaults to `10m`. |
| `VECTIS_E2E_KEEP_DEPLOY_LINUX=true` | Leave Linux deploy smoke artifacts and the VM running after the test for debugging. |
| `VECTIS_E2E_PACKAGE_CLI_DEB` | Path to a built `vectis-cli` DEB for package e2e testing. |
| `VECTIS_E2E_PACKAGE_CLI_RPM` | Path to a built `vectis-cli` RPM for package e2e testing. |
| `VECTIS_E2E_PACKAGE_SERVICES_DEB` | Whitespace- or path-list-separated paths for built `vectis-common`, service, and `vectis-services` DEBs. |
| `VECTIS_E2E_PACKAGE_SERVICES_RPM` | Whitespace- or path-list-separated paths for built `vectis-common`, service, and `vectis-services` RPMs. |
| `VECTIS_E2E_PACKAGE_LOCAL_DEB` | Path to a native Linux CGO `vectis-local` DEB for package e2e testing. |
| `VECTIS_E2E_PACKAGE_LOCAL_RPM` | Path to a native Linux CGO `vectis-local` RPM for package e2e testing. |
| `VECTIS_E2E_PACKAGE_LINUX_PROVIDER` | Linux package VM provider; defaults to `auto` (currently Lima). |
| `VECTIS_E2E_PACKAGE_LINUX_PROVIDER_PATH` | Override the VM provider command path, such as `limactl`. |
| `VECTIS_E2E_PACKAGE_LINUX_INSTANCE` | Override the prepared Linux DEB package smoke VM instance name. |
| `VECTIS_E2E_PACKAGE_RPM_LINUX_INSTANCE` | Override the prepared Linux RPM package smoke VM instance name. |
| `VECTIS_E2E_PACKAGE_LINUX_TIMEOUT` | Timeout for the Linux package VM smoke; defaults to `10m`. |
| `VECTIS_E2E_KEEP_PACKAGE_LINUX=true` | Leave the Linux package smoke VM running after the test for debugging. |

Prepared VM controls used by deploy and package lanes:

| Variable | Meaning |
|---|---|
| `PACKER_VM_PREP_VERSION` | Shared prepared VM marker version checked by `mage vmCheck` and VM e2e harnesses; defaults to `2`. |
| `VM_PROVIDER` | Provider used by `mage vmStatus`, `mage vmDoctor`, and `mage vmCheck`; defaults to `auto` (currently Lima). |
| `VM_DOCTOR_TIMEOUT` | Overall timeout for `mage vmStatus`, `mage vmDoctor`, and `mage vmCheck`; defaults to `10m`. |
| `PACKER_DEPLOY_SMOKE_INSTANCE` | Prepared Linux deploy smoke VM; defaults to `vectis-deploy-smoke`. |
| `PACKER_DEPLOY_SMOKE_TEMPLATE` | Lima template used for the prepared Linux deploy smoke VM; defaults to `ubuntu-lts`. |
| `PACKER_PACKAGE_BUILDER_INSTANCE` | Prepared Linux package builder instance; defaults to `vectis-package-builder`. |
| `PACKER_PACKAGE_BUILDER_TEMPLATE` | Lima template used by `mage vmPackageBuilderPrepare`; defaults to `ubuntu-lts`. |
| `PACKER_PACKAGE_BUILDER_GO_VERSION` | Go version installed into the prepared package builder; defaults to the root `go.mod` directive. |
| `PACKER_PACKAGE_BUILDER_GO_SHA256` | Optional SHA-256 for the downloaded Go archive. |
| `PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT` | Guest-side parent directory for writable package build workspaces. |
| `PACKER_PACKAGE_BUILDER_CACHE_ROOT` | Guest-side parent directory for persistent Go build and module caches. |
| `PACKER_PACKAGE_DEB_SMOKE_INSTANCE` | Prepared DEB package smoke VM; defaults to `vectis-package-smoke`. |
| `PACKER_PACKAGE_DEB_SMOKE_TEMPLATE` | Lima template used for the prepared DEB package smoke VM; defaults to `ubuntu-lts`. |
| `PACKER_PACKAGE_RPM_SMOKE_INSTANCE` | Prepared RPM package smoke VM; defaults to `vectis-package-rpm-smoke`. |
| `PACKER_PACKAGE_RPM_SMOKE_TEMPLATE` | Lima template used for the prepared RPM package smoke VM; defaults to `fedora`. |

Local package build controls used by `mage packageLocal` before the e2e install
lane runs:

| Variable | Meaning |
|---|---|
| `PACKAGE_LOCAL_VM_PROVIDER` | Build VM provider for non-Linux hosts; defaults to `auto` (currently Lima). |
| `PACKAGE_LOCAL_VM_PROVIDER_PATH` | Override the VM provider command path, such as `limactl`. |
| `PACKAGE_LOCAL_VM_INSTANCE` | Override the local package build VM instance name; defaults to the prepared builder. |
| `PACKAGE_LOCAL_VM_TIMEOUT` | Timeout for local package builds through the VM; defaults to `30m`. |
| `PACKAGE_LOCAL_VM_WORKSPACE_ROOT` | Guest-side parent directory for writable local package build workspaces; defaults to `/var/tmp/vectis-package-local-workspaces`. |
| `PACKAGE_LOCAL_VM_CACHE_ROOT` | Guest-side parent directory for persistent Go build and module caches; defaults to `/var/tmp/vectis-package-local-cache`. |
| `PACKAGE_LOCAL_VM_GO` | Go executable to use inside the local package build VM; defaults to `go`. |
| `PACKAGE_LOCAL_VM_KEEP=1` | Leave the local package build VM running after the package build. |
| `PACKAGE_LOCAL_ALLOW_CROSS_CGO=1` | Force the native build path on a non-Linux host with a working Linux CGO cross-toolchain. |

## Mocks

Import `"vectis/internal/interfaces/mocks"`. Record-and-verify + `XxxErr` injection pattern; details in [`internal/AGENTS.md`](../internal/AGENTS.md). Mocks are hand-written (no mockgen), which keeps the mock package dependency-free and makes mock behaviour explicit in review.

## Golden files and `testdata/`

- **Golden files** live in `testdata/` directories adjacent to the test file (e.g. `internal/api/testdata/`).
- Use `flag.UpdateGoldens()` pattern: when a flag like `-update` is passed, tests rewrite golden files instead of comparing. This avoids manual copy-paste during development.
- **Naming:** `testdata/<testname>.golden` for expected output.

## Coverage

No hard coverage bar. Focus on:
- Non-obvious branching and edge cases.
- Error paths (SQL errors, network failures, auth rejections).
- Interface contract compliance (mocks prove the interface compiles, integration tests prove it works).

Unit tests should be fast and deterministic. Integration tests may be slower; keep external dependencies limited to the services they explicitly start, such as the Postgres testcontainer lane.

## Fuzz testing

Fuzz targets are registered with `func FuzzXxx(f *testing.F)`. Locate them with `rg '^func Fuzz' internal/`. Add new fuzz targets for:
- Input parsing (token formats, config values).
- Authz decision paths.
- SQL rebind logic.

## Flaky tests

If a test flakes:
1. Add `t.Parallel()` only when the test genuinely shares no state — otherwise remove it.
2. For timing-dependent tests, use polling with deadlines, such as helpers in `internal/testutil`, rather than fixed sleeps.
3. If a flake can't be fixed, skip with `t.Skip("flaky — https://github.com/garrett-s-wininger/vectis/issues/NNN")`.
