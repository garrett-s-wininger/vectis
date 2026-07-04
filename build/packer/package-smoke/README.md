# Package Smoke VM Prep

This Packer template prepares the Linux VMs used by package installation e2e
tests. The e2e harness expects these instances to exist; it does not create raw
VMs from templates.

Prepare and check both package smoke profiles with:

```sh
mage vmPackageSmokePrepare
mage vmPackageSmokeCheck
```

The shared VM umbrella targets include both package smoke profiles:

```sh
mage vmPrepare
mage vmCheck
```

Or run one side of the package matrix:

```sh
mage vmPackageSmokeDebPrepare
mage vmPackageSmokeDebCheck
mage vmPackageSmokeRPMPrepare
mage vmPackageSmokeRPMCheck
```

The check targets use `mage vmDoctor` with the package-smoke lanes so status,
marker, and guest tooling checks
stay behind the shared VM provider path.

Defaults:

| Profile | Instance | Template |
| --- | --- | --- |
| DEB | `vectis-package-smoke` | `ubuntu-lts` |
| RPM | `vectis-package-rpm-smoke` | `fedora` |
