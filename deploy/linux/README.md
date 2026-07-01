# Linux Service Artifacts

This directory contains the first production-install artifact set for Linux
hosts. The checked-in source of truth is `services.toml`; systemd services and
timers, environment examples, sysusers, and tmpfiles entries are rendered from
that manifest. The render and validation path works on macOS without booting a
VM. The Lima smoke path adds a real Linux systemd check when developers want the
heavier cross-platform lane.

## What Is Included

| Path | Purpose |
| --- | --- |
| `services.toml` | Linux service inventory, systemd defaults, and env examples |
| `artifacts.go` | Renderer for systemd units/timers, env examples, sysusers, and tmpfiles |
| `vm_smoke.go` | provider-neutral Linux systemd smoke flow over `internal/platform` VMs |
| `cmd/render` | small renderer entrypoint retained for package/build integrations |

Rendering also produces `install/manifest.json` and `install/manifest.tsv`.
Those files are the install contract for package scripts, config management, and
the VM smoke harness: source artifact, destination path, mode, owner, group, and
artifact kind.

The standalone units are Postgres-first. Set `VECTIS_DATABASE_DRIVER=pgx` and a
real PostgreSQL DSN after copying the rendered `env/vectis.env.example` to
`/etc/vectis/vectis.env`.
The rendered artifact contract describes the standalone multi-service stack,
including API, artifact, catalog, cell ingress, cron, docs, log, log-forwarder,
orchestrator, queue, reconciler, registry, Gerrit stream bridge, SCM poller,
secrets, SPIFFE authority, worker, and worker-core units. It also includes a static
`vectis-retention-scheduled-cleanup.service` plus
`vectis-retention-scheduled-cleanup.timer` for the retained backup/audit/hold
evidence cleanup workflow. The service also emits
`/var/lib/vectis/ops/retention-evidence.prom` for Prometheus textfile scraping;
operators still own the backup manifest, expected topology, API token, textfile
collector wiring, and timer enablement.
The `vectis-local` DEB/RPM package is intentionally separate from this
TOML-driven service inventory and does not install systemd units.

## Ownership Boundary

Vectis owns the artifact contract: which units exist, which binaries they run,
which env files they reference, baseline process hardening, system user/directory
shape, target attachment, and the migration one-shot ordering for DB-backed
services.

systemd owns runtime lifecycle once the artifacts are installed: process start
and stop, restart policy, dependency ordering, journald integration, and
directory creation through `StateDirectory`, `RuntimeDirectory`, and related
settings.

Config management owns host-specific reality: installing packages or binaries,
writing `/etc/vectis/*.env`, placing secrets and TLS material, choosing Postgres
DSNs, assigning cells, configuring registry/queue/log addresses, opening
firewalls, enabling units, and deciding which standalone services run on a host.

The rendered `env/*.example` files are examples, not production configuration.
They are generated so Ansible, future packages, and manual installs share the
same documented shape without Vectis becoming the config manager.

## Local Validation

Run the no-VM artifact checks with:

```sh
mage deployArtifactsTest
```

The test lane renders the manifest, parses every generated unit and env example,
and validates service inventory, target attachment, environment file wiring,
migration ordering, and baseline hardening settings.

Render the installable files with:

```sh
go run ./cmd/cli deploy linux render --output artifacts/deploy/linux
DEPLOY_LINUX_OUT=artifacts/deploy/linux mage deployArtifactsRender
```

## Linux VM Validation On macOS

When a supported backend is available, macOS developers can run the rendered
artifacts through real Linux systemd without leaving the workstation. The
platform layer selects the backend automatically; Lima is the first provider.
This is an e2e test harness, not a user-facing deploy command. Prepare the
guest once with Packer, then run the e2e harness against that prepared VM:

```sh
mage vmValidate
mage vmDeploySmokePrepare
mage vmDeploySmokeCheck
mage testE2EDeployLinux
```

`mage vmPrepare` and `mage vmCheck` run this lane along with the package
builder and package install smoke VMs. The check targets use `mage vmDoctor`
with lane selection, so the marker/tooling checks live in one Go implementation.

`mage vmStatus` reports whether the prepared deploy/package VMs exist and
whether they are currently running without starting them. `mage vmDoctor`
starts stopped prepared VMs long enough to verify their marker files and guest
tooling, then stops any VM it started.

For the full prepared-VM lane across deploy and package smoke profiles, use:

```sh
mage vmValidate
mage vmPrepare
mage vmCheck
mage testE2EVM
```

By default, `mage vmDeploySmokePrepare` prepares a Lima instance named
`vectis-deploy-smoke` from the `ubuntu-lts` template, installs the systemd
tooling needed by the harness, and writes
`/etc/vectis-vm-prep/deploy-smoke-profile`.
The e2e harness expects that prepared instance to exist; it does not create raw
VMs from templates. During the test it starts the VM, renders the Linux
artifacts into a temporary local directory, copies them into the guest, installs
them under `/etc/systemd/system`, `/etc/vectis`, `/usr/lib/sysusers.d`, and
`/usr/lib/tmpfiles.d`, creates temporary Vectis stub binaries, and runs
`systemd-analyze verify`, `systemd-sysusers`, `systemd-tmpfiles`, and
`systemctl daemon-reload`. The file installation step is driven by the rendered
`install/manifest.tsv`.

The prepared guest also carries
`/etc/vectis-vm-prep/deploy-smoke-prep-version` so stale VMs fail early with a
prepare-target hint.

The harness installs marker-bearing stubs under `/usr/bin`, so cleanup can
remove smoke files without claiming ownership of existing host binaries. It
enables the rendered standalone service units, and starts `vectis.target`,
letting individual units enforce their own `Wants`/`After`/`Requires` ordering
while the target aggregates the enabled standalone stack. On success, the e2e
harness removes the smoke artifacts from the guest and deletes the temporary
local render directory.

The e2e target stops the deploy VM after the test unless
`VECTIS_E2E_KEEP_DEPLOY_LINUX=true` is set.

Set `VECTIS_E2E_DEPLOY_LINUX_PROVIDER=lima` when you want to force a specific
backend. Host operations run through `internal/platform` so additional backends
can reuse the same render/install/verify/cleanup flow. Use
`PACKER_DEPLOY_SMOKE_INSTANCE` and `VECTIS_E2E_DEPLOY_LINUX_INSTANCE` together
when you want to prepare and test a non-default instance name.
The current Packer prepare scripts are Lima-backed. Linux KVM and Windows
Hyper-V support should extend the prepare/provider layer while keeping the
guest provisioning scripts shared.

## Manual Install Sketch

Package scripts or config management should eventually own this, but the intended shape is:

1. Render the artifacts with `vectis-cli deploy linux render`.
2. Install `vectis-*` binaries into `/usr/bin`.
3. Install rendered `systemd/*.service`, `systemd/*.timer`, and `systemd/*.target` into the system unit dir.
4. Install rendered `sysusers.d/vectis.conf` and `tmpfiles.d/vectis.conf`.
5. Copy rendered `env/*.example` to `/etc/vectis/*.env` and adjust secrets, DSNs, TLS,
   ports, and auth settings.
6. Run `systemctl daemon-reload`.
7. Enable the desired service units, such as `systemctl enable vectis-api.service`, and any desired timers, such as `systemctl enable --now vectis-retention-scheduled-cleanup.timer`.
8. Start the enabled standalone stack with `systemctl start vectis.target`.
