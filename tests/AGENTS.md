# Tests

**Commands** are defined in the root [`Makefile`](../Makefile):

| Target | Scope | Notes |
|--------|-------|-------|
| `test` | All packages | No timeout, no race |
| `test-quick` | `internal/...` `cmd/...` `api/...` | `-count=1 -timeout=60s` — fast feedback |
| `test-integration` | Packages with `//go:build integration` | Requires Postgres (see `VECTIS_DATABASE_DSN`) |
| `test-race` | All packages | `-race` flag |
| `fuzz-api-auth` | API auth fuzz targets | `FUZZTIME` (default 30s) |

## Style

- **Standard `testing` only** — no testify/ginkgo. This avoids external test framework dependency and keeps test output uniform.
- **Table-driven tests** where there are multiple input/output cases.
- **`t.Helper()`** on helper functions.
- **`t.Parallel()`** when safe (not sharing state).
- **Test files** live next to the code they test (`xxx_test.go` in the same package).

## Integration tests

Integration packages need `//go:build integration` at the top of every file and live under [`integration/`](integration/). Package names under `integration/` are the source of truth for what integration tests exist.

Uses [`../internal/testutil/grpctest/`](../internal/testutil/grpctest/) for gRPC server setup. Example: [`integration/queue/server_test.go`](integration/queue/server_test.go). The `grpctest.SetupGRPCServer` function handles listening on an ephemeral port and returning a `*grpc.ClientConn`.

**Prerequisites:** a running Postgres instance reachable at `VECTIS_DATABASE_DSN` (defaults to `postgres://vectis:vectis@127.0.0.1:15432/vectis?sslmode=require`).

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

Unit tests should be fast (sub-millisecond). Integration tests may be slower but should not depend on network resources beyond the Postgres instance.

## Fuzz testing

Fuzz targets are registered with `func FuzzXxx(f *testing.F)`. Locate them with `rg '^func Fuzz' internal/`. Add new fuzz targets for:
- Input parsing (token formats, config values).
- Authz decision paths.
- SQL rebind logic.

## Flaky tests

If a test flakes:
1. Add `t.Parallel()` only when the test genuinely shares no state — otherwise remove it.
2. For timing-dependent tests, use `testutil.Poll()` or `require.Eventually`-style retry loops.
3. If a flake can't be fixed, skip with `t.Skip("flaky — https://github.com/anomalyco/vectis/issues/NNN")`.
