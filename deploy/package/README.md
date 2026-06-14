# Linux Packages

This directory contains the packaging source of truth for installable Linux
packages. The initial package surface is intentionally small: `vectis-cli`
installs only `/usr/bin/vectis-cli` and does not install systemd units,
sysusers/tmpfiles entries, or service configuration.

Build the first package with:

```sh
make package-cli
```

Then install-test it in the Linux VM lane with:

```sh
make test-e2e-package-cli-deb
make test-e2e-package-cli-rpm
```

Production packages are built with `CGO_ENABLED=0 -tags=nosqlite`, matching the
container build posture. SQLite remains available for local source builds unless
the `nosqlite` tag is set.

The TOML package manifest is shared across DEB and RPM so later service packages
reuse the same package metadata and file inventory rather than duplicating
package definitions.
