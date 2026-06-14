#!/bin/sh
set -eu

limactl_bin=${VECTIS_PACKER_LIMA_BIN:-limactl}
instance=${VECTIS_PACKER_LIMA_INSTANCE:-}
template=${VECTIS_PACKER_LIMA_TEMPLATE:-}
profile=${VECTIS_PACKER_PACKAGE_PROFILE:-}
prep_version=${VECTIS_PACKER_PREP_VERSION:-1}
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

case "$profile" in
	deb|rpm)
		;;
	*)
		echo "VECTIS_PACKER_PACKAGE_PROFILE must be deb or rpm" >&2
		exit 1
		;;
esac

if ! command -v "$limactl_bin" >/dev/null 2>&1; then
	echo "limactl is required to prepare the Lima package smoke VM" >&2
	exit 1
fi

if "$limactl_bin" list "$instance" >/dev/null 2>&1; then
	echo "using existing Lima instance $instance"
else
	echo "creating Lima package smoke VM $instance from template:$template"
	"$limactl_bin" --tty=false create \
		--name="$instance" \
		--cpus="$cpus" \
		--memory="$memory" \
		--disk="$disk" \
		--mount-none \
		"template:$template"
fi

"$limactl_bin" --tty=false start "$instance"

"$limactl_bin" --tty=false shell "$instance" -- sh -s -- "$profile" "$prep_version" <<'GUEST'
set -eu

profile=$1
prep_version=$2

if ! command -v sudo >/dev/null 2>&1; then
	echo "sudo is required inside the package smoke VM" >&2
	exit 1
fi

case "$profile" in
	deb)
		if ! command -v apt-get >/dev/null 2>&1; then
			echo "deb package smoke profile requires an apt-based guest" >&2
			exit 1
		fi

		sudo apt-get update
		sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates dpkg systemd
		command -v dpkg >/dev/null
		command -v dpkg-deb >/dev/null
		;;
	rpm)
		if command -v dnf >/dev/null 2>&1; then
			sudo dnf -y install ca-certificates rpm systemd
		elif command -v microdnf >/dev/null 2>&1; then
			sudo microdnf -y install ca-certificates rpm systemd
		else
			echo "rpm package smoke profile requires dnf or microdnf" >&2
			exit 1
		fi

		command -v rpm >/dev/null
		;;
esac

command -v systemctl >/dev/null
command -v systemd-sysusers >/dev/null
command -v systemd-tmpfiles >/dev/null

sudo install -d -m 0755 /etc/vectis-vm-prep
printf '%s\n' "$profile" | sudo tee /etc/vectis-vm-prep/package-smoke-profile >/dev/null
printf '%s\n' "$prep_version" | sudo tee /etc/vectis-vm-prep/package-smoke-prep-version >/dev/null
GUEST

case "$stop_after" in
	1|t|T|true|TRUE|y|Y|yes|YES|on|ON)
		"$limactl_bin" --tty=false stop "$instance"
		;;
esac
