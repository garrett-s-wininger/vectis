# Linux Service Artifacts

This directory contains the first production-install artifact set for Linux
hosts. The checked-in source of truth is `services.toml`; systemd units,
environment examples, sysusers, and tmpfiles entries are rendered from that
manifest. The render and validation path works on macOS without booting a VM.
The Lima smoke path adds a real Linux systemd check when developers want the
heavier cross-platform lane.

## What Is Included

| Path | Purpose |
| --- | --- |
| `services.toml` | Linux service inventory, systemd defaults, and env examples |
| `artifacts.go` | Renderer for systemd units, env examples, sysusers, and tmpfiles |
| `vm_smoke.go` | provider-neutral Linux systemd smoke flow over `internal/platform` VMs |
| `cmd/render` | small renderer entrypoint retained for package/build integrations |

Rendering also produces `install/manifest.json` and `install/manifest.tsv`.
Those files are the install contract for package scripts, config management, and
the VM smoke harness: source artifact, destination path, mode, owner, group, and
artifact kind.

The standalone units are Postgres-first. Set `VECTIS_DATABASE_DRIVER=pgx` and a
real PostgreSQL DSN after copying the rendered `env/vectis.env.example` to
`/etc/vectis/vectis.env`.
The rendered artifact contract describes the standalone multi-service stack.
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
make deploy-artifacts-test
```

The test lane renders the manifest, parses every generated unit and env example,
and validates service inventory, target attachment, environment file wiring,
migration ordering, and baseline hardening settings.

Render the installable files with:

```sh
go run ./cmd/cli deploy linux render --output artifacts/deploy/linux
make deploy-artifacts-render DEPLOY_LINUX_OUT=artifacts/deploy/linux
```

## Linux VM Validation On macOS

When a supported backend is available, macOS developers can run the rendered
artifacts through real Linux systemd without leaving the workstation. The
platform layer selects the backend automatically; Lima is the first provider.
This is an e2e test harness, not a user-facing deploy command:

```sh
make test-e2e-deploy-linux
```

The e2e harness creates or starts a Lima instance named `vectis-deploy-smoke` from the
`ubuntu-lts` template, renders the Linux artifacts into a temporary local
directory, copies them into the guest, installs them under `/etc/systemd/system`,
`/etc/vectis`, `/usr/lib/sysusers.d`, and `/usr/lib/tmpfiles.d`, creates
temporary Vectis stub binaries, and runs `systemd-analyze verify`,
`systemd-sysusers`, `systemd-tmpfiles`, and `systemctl daemon-reload`. The file
installation step is driven by the rendered `install/manifest.tsv`.

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
can reuse the same render/install/verify/cleanup flow.

## Manual Install Sketch

Package scripts or config management should eventually own this, but the intended shape is:

1. Render the artifacts with `vectis-cli deploy linux render`.
2. Install `vectis-*` binaries into `/usr/bin`.
3. Install rendered `systemd/*.service` and `systemd/*.target` into the system unit dir.
4. Install rendered `sysusers.d/vectis.conf` and `tmpfiles.d/vectis.conf`.
5. Copy rendered `env/*.example` to `/etc/vectis/*.env` and adjust secrets, DSNs, TLS,
   ports, and auth settings.
6. Run `systemctl daemon-reload`.
7. Enable the desired service units, such as `systemctl enable vectis-api.service`.
8. Start the enabled standalone stack with `systemctl start vectis.target`.
