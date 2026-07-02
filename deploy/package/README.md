# Linux Packages

This directory contains the packaging source of truth for installable Linux
packages.

The first package split is:

| Package | Contents |
| --- | --- |
| `vectis-cli` | `/usr/bin/vectis-cli`; no systemd units or service configuration |
| `vectis-local` | `/usr/bin/vectis-local` wrapper plus private SQLite-capable local-stack binaries under `/usr/lib/vectis-local/bin` |
| `vectis-common` | shared target, migration unit, reference retention cleanup timer, sysusers/tmpfiles, and common env examples |
| `vectis-<service>` | one service binary, its systemd unit, and its env example |
| `vectis-services` | metadata-only convenience package depending on the standard standalone service set |

The standard standalone service set includes API, artifact, catalog, cell
ingress, cron, docs, log, log-forwarder, orchestrator, queue, reconciler,
registry, secrets, SPIFFE authority, worker, and worker-core packages. The
common package also ships the timer-driven retention cleanup workflow that uses
`vectis-cli`; operators enable it only after supplying backup evidence,
expected topology, and API credentials.

Build the production Linux package set with:

```sh
mage packageLinux
```

Build either production side of the split with:

```sh
mage packageCLI
mage packageServices
```

Build the local single-host package with:

```sh
mage packageLocal
```

By default the production package targets build DEB and RPM packages for
`linux/amd64` and `linux/arm64`. Override `PACKAGE_ARCHES` with Go architecture
names to change that build matrix, for example:

```sh
PACKAGE_ARCHES=arm64 mage packageCLI
```

The local package target uses `PACKAGE_LOCAL_ARCHES`, which defaults to
`PACKAGE_ARCH`, because SQLite-enabled CGO builds normally need a native Linux C
toolchain. `mage packageLocal` gates on the host platform: Linux hosts run the
native CGO package target directly, while macOS and other non-Linux hosts run
that native target inside the configured platform VM provider. Lima is the
default provider.

On macOS, prepare the package builder before the first local package build:

```sh
mage vmPackageBuilderPrepare
mage vmPackageBuilderCheck
```

To inspect the prepared VM lanes without starting stopped guests, use:

```sh
mage vmStatus
```

To start stopped prepared guests long enough to verify markers and expected
guest tools, use:

```sh
mage vmDoctor
```

To prepare and check every VM-backed deploy/package lane in one pass, use:

```sh
mage vmValidate
mage vmPrepare
mage vmCheck
```

`mage vmCheck` delegates to `mage vmDoctor`, while the individual VM check
targets call the same Go checker with a single lane selected.

Then install-test it in the Linux VM lane with:

```sh
mage vmPackageSmokePrepare
mage vmPackageSmokeCheck
mage testE2EPackageCLIDeb
mage testE2EPackageCLIRPM
mage testE2EPackageServicesDeb
mage testE2EPackageServicesRPM
mage testE2EPackageLocalDeb
mage testE2EPackageLocalRPM
mage testE2EPackageLocal
```

For the full VM-backed deployment/package smoke lane, use:

```sh
mage vmValidate
mage vmPrepare
mage vmCheck
mage testE2EVM
```

The e2e package targets use `PACKAGE_ARCH`, which defaults to the local Go
architecture, so the package under test matches the VM architecture. The e2e
package harness expects prepared smoke VMs; it does not create raw VMs from
templates.

Production CLI and service packages are built with `CGO_ENABLED=0
-tags=nosqlite`, matching the container build posture. The `vectis-local`
package is intentionally different: it is a native Linux CGO build with SQLite
enabled. The public local package target uses a VM automatically on non-Linux
hosts. Set `PACKAGE_LOCAL_ALLOW_CROSS_CGO=1` only when you intentionally want to
run the native target on a non-Linux host with a working Linux C cross-toolchain.

Useful VM knobs for local package builds:

```sh
PACKAGE_LOCAL_VM_PROVIDER=lima \
  PACKAGE_LOCAL_VM_INSTANCE=vectis-package-builder \
  mage packageLocal
```

The dispatcher copies the worktree into a writable guest workspace under
`PACKAGE_LOCAL_VM_WORKSPACE_ROOT` and copies `PACKAGE_OUT` back after the build,
so it does not require a writable host mount in Lima. Go build and module caches
live under `PACKAGE_LOCAL_VM_CACHE_ROOT`, which defaults to `/var/tmp` so repeat
VM builds can reuse downloads across boots. The build VM must have Go, Mage,
and a C compiler installed. The default path is a Packer-prepared builder named
`vectis-package-builder`; run `mage vmPackageBuilderPrepare` to create or
refresh it. Prepared VMs write `/etc/vectis-vm-prep/*-prep-version` markers;
rerun the matching prepare target when a check reports a stale marker. Direct
Linux-builder entrypoints are available as
`mage packageLocalNativeDeb`, `mage packageLocalNativeRPM`, and
`mage packageLocalNative`.

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
