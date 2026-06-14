# Package Builder Image Prep

This Packer template prepares the Linux builder used by `make package-local`
when the developer host is not Linux. The first provider-backed implementation
uses Lima on macOS and provisions a named builder instance instead of doing
package-build-time setup.

Prepare and smoke-check the builder with:

```sh
make vm-package-builder-prepare
make vm-package-builder-check
```

The builder installs:

- the exact Go toolchain version declared by the root `go.mod`
- `make`
- a C compiler and build essentials for SQLite-enabled CGO builds
- writable guest workspace and persistent Go cache directories

Useful overrides:

```sh
make vm-package-builder-prepare \
  PACKER_PACKAGE_BUILDER_INSTANCE=vectis-package-builder \
  PACKER_PACKAGE_BUILDER_TEMPLATE=ubuntu-lts \
  PACKER_PACKAGE_BUILDER_CPUS=4 \
  PACKER_PACKAGE_BUILDER_MEMORY=4 \
  PACKER_PACKAGE_BUILDER_DISK=60
```

`PACKER_PACKAGE_BUILDER_GO_SHA256` may be set to verify the downloaded Go
archive. Prepare the builder before running local package builds on non-Linux
hosts.
