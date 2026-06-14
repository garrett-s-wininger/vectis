#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
common_dir=$(CDPATH= cd "$script_dir/../../scripts" && pwd)

. "$common_dir/lima-common.sh"

limactl_bin=${VECTIS_PACKER_LIMA_BIN:-limactl}
instance=${VECTIS_PACKER_LIMA_INSTANCE:-vectis-package-builder}
template=${VECTIS_PACKER_LIMA_TEMPLATE:-ubuntu-lts}
go_version=${VECTIS_PACKER_GO_VERSION:-}
go_sha256=${VECTIS_PACKER_GO_SHA256:-}
prep_version=${VECTIS_PACKER_PREP_VERSION:-1}
cpus=${VECTIS_PACKER_CPUS:-4}
memory=${VECTIS_PACKER_MEMORY:-4}
disk=${VECTIS_PACKER_DISK:-60}
stop_after=${VECTIS_PACKER_STOP_AFTER_PREPARE:-true}
workspace_root=${VECTIS_PACKER_WORKSPACE_ROOT:-/var/tmp/vectis-package-local-workspaces}
cache_root=${VECTIS_PACKER_CACHE_ROOT:-/var/tmp/vectis-package-local-cache}
guest_common="$common_dir/guest-common.sh"

require_non_empty VECTIS_PACKER_GO_VERSION "$go_version"

prepare_lima_vm "Lima package builder" "$script_dir/guest-package-builder.sh" \
	"$go_version" "$go_sha256" "$cache_root" "$workspace_root" "$prep_version"
