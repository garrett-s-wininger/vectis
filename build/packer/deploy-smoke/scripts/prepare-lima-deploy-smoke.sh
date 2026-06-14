#!/bin/sh
set -eu

limactl_bin=${VECTIS_PACKER_LIMA_BIN:-limactl}
instance=${VECTIS_PACKER_LIMA_INSTANCE:-}
template=${VECTIS_PACKER_LIMA_TEMPLATE:-}
cpus=${VECTIS_PACKER_CPUS:-2}
memory=${VECTIS_PACKER_MEMORY:-2}
disk=${VECTIS_PACKER_DISK:-30}
stop_after=${VECTIS_PACKER_STOP_AFTER_PREPARE:-true}

if [ -z "$instance" ]; then
	echo "VECTIS_PACKER_LIMA_INSTANCE is required" >&2
	exit 1
fi

if [ -z "$template" ]; then
	echo "VECTIS_PACKER_LIMA_TEMPLATE is required" >&2
	exit 1
fi

if ! command -v "$limactl_bin" >/dev/null 2>&1; then
	echo "limactl is required to prepare the Lima deploy smoke VM" >&2
	exit 1
fi

if "$limactl_bin" list "$instance" >/dev/null 2>&1; then
	echo "using existing Lima deploy smoke VM $instance"
else
	echo "creating Lima deploy smoke VM $instance from template:$template"
	"$limactl_bin" --tty=false create \
		--name="$instance" \
		--cpus="$cpus" \
		--memory="$memory" \
		--disk="$disk" \
		--mount-none \
		"template:$template"
fi

"$limactl_bin" --tty=false start "$instance"

"$limactl_bin" --tty=false shell "$instance" -- sh -s <<'GUEST'
set -eu

if ! command -v sudo >/dev/null 2>&1; then
	echo "sudo is required inside the deploy smoke VM" >&2
	exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
	echo "deploy smoke profile requires an apt-based guest" >&2
	exit 1
fi

sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates systemd

command -v systemctl >/dev/null
command -v systemd-analyze >/dev/null
command -v systemd-sysusers >/dev/null
command -v systemd-tmpfiles >/dev/null

sudo install -d -m 0755 /etc/vectis
printf '%s\n' systemd | sudo tee /etc/vectis/deploy-smoke-profile >/dev/null
GUEST

case "$stop_after" in
	1|t|T|true|TRUE|y|Y|yes|YES|on|ON)
		"$limactl_bin" --tty=false stop "$instance"
		;;
esac
