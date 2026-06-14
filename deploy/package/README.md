# Linux Packages

This directory contains the packaging source of truth for installable Linux
packages.

The first package split is:

| Package | Contents |
| --- | --- |
| `vectis-cli` | `/usr/bin/vectis-cli`; no systemd units or service configuration |
| `vectis-services` | standalone service-stack binaries, systemd units, sysusers/tmpfiles, and rendered env examples under `/usr/share/doc/vectis-services/examples` |

Build all Linux packages with:

```sh
make package-linux
```

Build either side of the split with:

```sh
make package-cli
make package-services
```

By default this builds DEB and RPM packages for `linux/amd64` and `linux/arm64`.
Override `PACKAGE_ARCHES` with Go architecture names to change the build matrix,
for example:

```sh
make package-cli PACKAGE_ARCHES=arm64
```

Then install-test it in the Linux VM lane with:

```sh
make test-e2e-package-cli-deb
make test-e2e-package-cli-rpm
make test-e2e-package-services-deb
make test-e2e-package-services-rpm
```

The e2e package targets use `PACKAGE_ARCH`, which defaults to the local Go
architecture, so the package under test matches the VM architecture.

Production packages are built with `CGO_ENABLED=0 -tags=nosqlite`, matching the
container build posture. SQLite remains available for local source builds unless
the `nosqlite` tag is set.

The TOML package manifest is shared across DEB and RPM so package metadata and
file inventory are not duplicated across formats. The `vectis-services` package
reuses rendered artifacts from `deploy/linux/services.toml`; packages install
units into `/usr/lib/systemd/system`, while live `/etc/vectis/*.env`
configuration remains operator/config-management owned.

Packages intentionally do not include DEB maintainer scripts, RPM scriptlets, or
`systemctl` calls. Installation only places files on disk; operators or
configuration management should run
`systemd-sysusers /usr/lib/sysusers.d/vectis.conf`,
`systemd-tmpfiles --create /usr/lib/tmpfiles.d/vectis.conf`,
`systemctl daemon-reload`, and then enable/start `vectis.target` when the host
configuration is ready.
