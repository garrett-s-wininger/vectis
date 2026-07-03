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

For narrow work, prefer package-level tests:

```bash
go test ./internal/api/...
```
