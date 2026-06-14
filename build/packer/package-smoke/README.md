# Package Smoke VM Prep

This Packer template prepares the Linux VMs used by package installation e2e
tests. The e2e harness expects these instances to exist; it does not create raw
VMs from templates.

Prepare and check both package smoke profiles with:

```sh
make vm-package-smoke-prepare
make vm-package-smoke-check
```

The shared VM umbrella targets include both package smoke profiles:

```sh
make vm-prepare
make vm-check
```

Or run one side of the package matrix:

```sh
make vm-package-smoke-deb-prepare
make vm-package-smoke-deb-check
make vm-package-smoke-rpm-prepare
make vm-package-smoke-rpm-check
```

Defaults:

| Profile | Instance | Template |
| --- | --- | --- |
| DEB | `vectis-package-smoke` | `ubuntu-lts` |
| RPM | `vectis-package-rpm-smoke` | `fedora` |
