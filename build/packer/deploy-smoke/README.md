# Deploy Smoke VM Prep

This Packer template prepares the Linux VM used by the systemd deploy e2e test.
The e2e harness expects this instance to exist; it does not create raw VMs from
templates.

Prepare and check the deploy smoke VM with:

```sh
make vm-deploy-smoke-prepare
make vm-deploy-smoke-check
```

The shared VM umbrella targets include this deploy smoke VM:

```sh
make vm-prepare
make vm-check
```

Defaults:

| Instance | Template |
| --- | --- |
| `vectis-deploy-smoke` | `ubuntu-lts` |
