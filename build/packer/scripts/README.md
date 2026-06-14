# Shared Packer Scripts

These helpers are sourced by the per-lane Packer `shell-local` entrypoints.

- `lima-common.sh` owns the host-side Lima lifecycle: validate inputs, create
  the named VM when missing, start it, stream guest provisioning into it, and
  optionally stop it afterward.
- `guest-common.sh` is prepended to each guest provisioning script and provides
  small helpers for package installation, command checks, and VM prep markers.

Lane-specific scripts under `deploy-smoke/scripts`, `package-builder/scripts`,
and `package-smoke/scripts` should stay focused on their profile-specific guest
requirements.
