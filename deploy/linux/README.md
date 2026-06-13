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
| `lima.go` | Lima defaults and current macOS-to-Linux command wrappers |
| `cmd/render` | small renderer entrypoint retained for package/build integrations |

Rendering also produces `install/manifest.json` and `install/manifest.tsv`.
Those files are the install contract for package scripts, config management, and
the VM smoke harness: source artifact, destination path, mode, owner, group, and
artifact kind.

The standalone units are Postgres-first. Set `VECTIS_DATABASE_DRIVER=pgx` and a
real PostgreSQL DSN after copying the rendered `env/vectis.env.example` to
`/etc/vectis/vectis.env`.
`vectis-local.service` is the one-box path; it runs `vectis-local`, which manages
its local SQLite databases and child services itself.

## Ownership Boundary

Vectis owns the artifact contract: which units exist, which binaries they run,
which env files they reference, baseline process hardening, system user/directory
shape, target membership, and the migration one-shot ordering for DB-backed
services.

systemd owns runtime lifecycle once the artifacts are installed: process start
and stop, restart policy, dependency ordering, journald integration, and
directory creation through `StateDirectory`, `RuntimeDirectory`, and related
settings.

Config management owns host-specific reality: installing packages or binaries,
writing `/etc/vectis/*.env`, placing secrets and TLS material, choosing Postgres
DSNs, assigning cells, configuring registry/queue/log addresses, opening
firewalls, enabling units, and deciding whether a host runs the standalone stack
or the one-box `vectis-local.service`.

The rendered `env/*.example` files are examples, not production configuration.
They are generated so Ansible, future packages, and manual installs share the
same documented shape without Vectis becoming the config manager.

## Local Validation

Run the no-VM artifact checks with:

```sh
make deploy-artifacts-test
```

The test lane renders the manifest, parses every generated unit and env example,
and validates service inventory, target membership, environment file wiring,
migration ordering, and baseline hardening settings.

Render the installable files with:

```sh
go run ./cmd/cli deploy linux render --output artifacts/deploy/linux
make deploy-artifacts-render DEPLOY_LINUX_OUT=artifacts/deploy/linux
```

## Linux VM Validation On macOS

When Lima is installed, macOS developers can run the rendered artifacts through
real Linux systemd without leaving the workstation:

```sh
go run ./cmd/cli deploy linux lima verify
make deploy-linux-lima-verify
```

This creates or starts a Lima instance named `vectis-deploy-smoke` from the
`ubuntu-lts` template, renders the Linux artifacts into a temporary local
directory, copies them into the guest, installs them under `/etc/systemd/system`,
`/etc/vectis`, `/usr/lib/sysusers.d`, and `/usr/lib/tmpfiles.d`, creates
temporary Vectis stub binaries, and runs `systemd-analyze verify`,
`systemd-sysusers`, `systemd-tmpfiles`, and `systemctl daemon-reload`. The file
installation step is driven by the rendered `install/manifest.tsv`.

On success, the verify command removes the smoke artifacts from the guest and
deletes the temporary local render directory. For debugging, pass
`--keep-artifacts` and optionally `--artifacts <dir>`.

The Lima lane verifies that the generated files are accepted by a real Linux
systemd host. It does not yet prove a full Vectis process stack with real
binaries and Postgres. That is the next smoke profile.

The CLI still exposes this as `deploy linux lima ...`, but the host VM operations
run through `internal/platform` so additional local VM backends can reuse the
same render/install/verify/cleanup flow.

Useful cleanup commands for interrupted runs or `--keep-artifacts` sessions:

```sh
go run ./cmd/cli deploy linux lima clean
go run ./cmd/cli deploy linux lima down
go run ./cmd/cli deploy linux lima delete
make deploy-linux-lima-clean
make deploy-linux-lima-down
make deploy-linux-lima-delete
```

## Manual Install Sketch

Package scripts or config management should eventually own this, but the intended shape is:

1. Render the artifacts with `vectis-cli deploy linux render`.
2. Install `vectis-*` binaries into `/usr/bin`.
3. Install rendered `systemd/*.service` and `systemd/*.target` into the system unit dir.
4. Install rendered `sysusers.d/vectis.conf` and `tmpfiles.d/vectis.conf`.
5. Copy rendered `env/*.example` to `/etc/vectis/*.env` and adjust secrets, DSNs, TLS,
   ports, and auth settings.
6. Run `systemctl daemon-reload`.
7. Start the standalone stack with `systemctl start vectis.target`, or start the
   one-box stack with `systemctl start vectis-local.service`.

Do not enable both `vectis.target` and `vectis-local.service` on the same host
unless you have deliberately changed ports and paths to avoid overlap.
