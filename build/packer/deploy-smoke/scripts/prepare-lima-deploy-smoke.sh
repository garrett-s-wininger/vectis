#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
common_dir=$(CDPATH= cd "$script_dir/../../scripts" && pwd)

. "$common_dir/lima-common.sh"

limactl_bin=${VECTIS_PACKER_LIMA_BIN:-limactl}
instance=${VECTIS_PACKER_LIMA_INSTANCE:-}
template=${VECTIS_PACKER_LIMA_TEMPLATE:-}
prep_version=${VECTIS_PACKER_PREP_VERSION:-1}
cpus=${VECTIS_PACKER_CPUS:-2}
memory=${VECTIS_PACKER_MEMORY:-2}
disk=${VECTIS_PACKER_DISK:-30}
stop_after=${VECTIS_PACKER_STOP_AFTER_PREPARE:-true}
guest_common="$common_dir/guest-common.sh"

prepare_lima_vm "Lima deploy smoke VM" "$script_dir/guest-deploy-smoke.sh" "$prep_version"
