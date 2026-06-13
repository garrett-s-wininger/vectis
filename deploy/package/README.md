# Linux Packages

This directory contains the packaging source of truth for installable Linux
packages. The initial package surface is intentionally small: `vectis-cli`
installs only `/usr/bin/vectis-cli` and does not install systemd units,
sysusers/tmpfiles entries, or service configuration.

Build the first package with:

```sh
make package-cli-deb
```

Then install-test it in the Linux VM lane with:

```sh
make test-e2e-package-cli-deb
```

Production packages are built with `CGO_ENABLED=0 -tags=nosqlite`, matching the
container build posture. SQLite remains available for local source builds unless
the `nosqlite` tag is set.

RPM output intentionally remains disabled until there is an RPM-family VM lane
that can install, run, remove, and verify the package lifecycle. The TOML package
manifest is shared so the RPM path can reuse the same package metadata and file
inventory rather than duplicating package definitions.
