# Deploy Smoke VM Prep

This Packer template prepares the Linux VM used by the systemd deploy e2e test.
The e2e harness expects this instance to exist; it does not create raw VMs from
templates.

Prepare and check the deploy smoke VM with:

```sh
mage vmDeploySmokePrepare
mage vmDeploySmokeCheck
```

The shared VM umbrella targets include this deploy smoke VM:

```sh
mage vmPrepare
mage vmCheck
```

The check target uses `mage vmDoctor` with the deploy-smoke lane so status, marker, and
guest tooling checks stay behind the shared VM provider path.

Defaults:

| Instance | Template |
| --- | --- |
| `vectis-deploy-smoke` | `ubuntu-lts` |
