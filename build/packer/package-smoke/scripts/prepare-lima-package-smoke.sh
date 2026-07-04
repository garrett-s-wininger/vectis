#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
common_dir=$(CDPATH= cd "$script_dir/../../scripts" && pwd)

. "$common_dir/lima-common.sh"

limactl_bin=${VECTIS_PACKER_LIMA_BIN:-limactl}
instance=${VECTIS_PACKER_LIMA_INSTANCE:-}
template=${VECTIS_PACKER_LIMA_TEMPLATE:-}
profile=${VECTIS_PACKER_PACKAGE_PROFILE:-}
prep_version=${VECTIS_PACKER_PREP_VERSION:-2}
cpus=${VECTIS_PACKER_CPUS:-2}
memory=${VECTIS_PACKER_MEMORY:-2}
disk=${VECTIS_PACKER_DISK:-30}
stop_after=${VECTIS_PACKER_STOP_AFTER_PREPARE:-true}
guest_common="$common_dir/guest-common.sh"

case "$profile" in
	deb|rpm)
		;;
	*)
		echo "VECTIS_PACKER_PACKAGE_PROFILE must be deb or rpm" >&2
		exit 1
		;;
esac

prepare_lima_vm "Lima package smoke VM" "$script_dir/guest-package-smoke.sh" "$profile" "$prep_version"
