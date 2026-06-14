# Linux Packages

This directory contains the packaging source of truth for installable Linux
packages.

The first package split is:

| Package | Contents |
| --- | --- |
| `vectis-cli` | `/usr/bin/vectis-cli`; no systemd units or service configuration |
| `vectis-local` | `/usr/bin/vectis-local` wrapper plus private SQLite-capable local-stack binaries under `/usr/lib/vectis-local/bin` |
| `vectis-common` | shared target, migration unit, sysusers/tmpfiles, and common env examples |
| `vectis-<service>` | one service binary, its systemd unit, and its env example |
| `vectis-services` | metadata-only convenience package depending on the standard standalone service set |

Build the production Linux package set with:

```sh
make package-linux
```

Build either production side of the split with:

```sh
make package-cli
make package-services
```

Build the local single-host package with:

```sh
make package-local
```

By default the production package targets build DEB and RPM packages for
`linux/amd64` and `linux/arm64`. Override `PACKAGE_ARCHES` with Go architecture
names to change that build matrix, for example:

```sh
make package-cli PACKAGE_ARCHES=arm64
```

The local package target uses `PACKAGE_LOCAL_ARCHES`, which defaults to
`PACKAGE_ARCH`, because SQLite-enabled CGO builds normally need a native Linux C
toolchain. `make package-local` gates on the host platform: Linux hosts run the
native CGO package target directly, while macOS and other non-Linux hosts run
that native target inside the configured platform VM provider. Lima is the
default provider.

On macOS, prepare the package builder before the first local package build:

```sh
make vm-package-builder-prepare
make vm-package-builder-check
```

Then install-test it in the Linux VM lane with:

```sh
make test-e2e-package-cli-deb
make test-e2e-package-cli-rpm
make test-e2e-package-services-deb
make test-e2e-package-services-rpm
make test-e2e-package-local-deb
make test-e2e-package-local-rpm
make test-e2e-package-local
```

The e2e package targets use `PACKAGE_ARCH`, which defaults to the local Go
architecture, so the package under test matches the VM architecture.

Production CLI and service packages are built with `CGO_ENABLED=0
-tags=nosqlite`, matching the container build posture. The `vectis-local`
package is intentionally different: it is a native Linux CGO build with SQLite
enabled. The public local package target uses a VM automatically on non-Linux
hosts. Set `PACKAGE_LOCAL_ALLOW_CROSS_CGO=1` only when you intentionally want to
run the native target on a non-Linux host with a working Linux C cross-toolchain.

Useful VM knobs for local package builds:

```sh
make package-local \
  PACKAGE_LOCAL_VM_PROVIDER=lima \
  PACKAGE_LOCAL_VM_INSTANCE=vectis-package-builder
```

The dispatcher copies the worktree into a writable guest workspace under
`PACKAGE_LOCAL_VM_WORKSPACE_ROOT` and copies `PACKAGE_OUT` back after the build,
so it does not require a writable host mount in Lima. Go build and module caches
live under `PACKAGE_LOCAL_VM_CACHE_ROOT`, which defaults to `/var/tmp` so repeat
VM builds can reuse downloads across boots. The build VM must have Go, `make`,
and a C compiler installed. The default path is a Packer-prepared builder named
`vectis-package-builder`; run `make vm-package-builder-prepare` to create or
refresh it. Direct Linux-builder entrypoints are available as
`make package-local-native-deb`, `make package-local-native-rpm`, and
`make package-local-native`.

The TOML package manifest is shared across DEB and RPM so package metadata and
file inventory are not duplicated across formats. Service packages use
`service = "api"` style declarations to expand the binary, unit, and env example
from `deploy/linux/services.toml` artifacts. Packages install units into
`/usr/lib/systemd/system`, while live `/etc/vectis/*.env` configuration remains
operator/config-management owned. The `vectis-local` package does not install
systemd units or live `/etc/vectis` config; it is run directly as
`vectis-local`.

Packages intentionally do not include DEB maintainer scripts, RPM scriptlets, or
`systemctl` calls. Installation only places files on disk; operators or
configuration management should run
`systemd-sysusers /usr/lib/sysusers.d/vectis.conf`,
`systemd-tmpfiles --create /usr/lib/tmpfiles.d/vectis.conf`,
`systemctl daemon-reload`, and then enable the desired `vectis-*.service` units
before starting `vectis.target` when the host configuration is ready.
