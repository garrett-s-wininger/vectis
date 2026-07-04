#!/bin/sh
set -eu

require_non_empty() {
	name=$1
	value=$2

	if [ -z "$value" ]; then
		echo "$name is required" >&2
		exit 1
	fi
}

prepare_lima_vm() {
	label=$1
	guest_script=$2
	shift 2

	limactl_bin=${limactl_bin:-limactl}
	cpus=${cpus:-2}
	memory=${memory:-2}
	disk=${disk:-30}
	stop_after=${stop_after:-true}

	require_non_empty VECTIS_PACKER_LIMA_INSTANCE "${instance:-}"
	require_non_empty VECTIS_PACKER_LIMA_TEMPLATE "${template:-}"

	if [ -z "${guest_common:-}" ]; then
		echo "guest_common is required" >&2
		exit 1
	fi

	if [ ! -r "$guest_common" ]; then
		echo "guest common script is not readable: $guest_common" >&2
		exit 1
	fi

	if [ ! -r "$guest_script" ]; then
		echo "guest script is not readable: $guest_script" >&2
		exit 1
	fi

	if ! command -v "$limactl_bin" >/dev/null 2>&1; then
		echo "limactl is required to prepare $label" >&2
		exit 1
	fi

	if "$limactl_bin" list "$instance" >/dev/null 2>&1; then
		echo "using existing Lima instance $instance"
	else
		echo "creating $label $instance from template:$template"
		"$limactl_bin" --tty=false create \
			--name="$instance" \
			--cpus="$cpus" \
			--memory="$memory" \
			--disk="$disk" \
			--mount-none \
			"template:$template"
	fi

	"$limactl_bin" --tty=false start "$instance"

	{
		cat "$guest_common"
		printf '\n'
		cat "$guest_script"
	} | "$limactl_bin" --tty=false shell "$instance" -- sh -s -- "$@"

	case "$stop_after" in
	1 | t | T | true | TRUE | y | Y | yes | YES | on | ON)
		"$limactl_bin" --tty=false stop "$instance"
		;;
	esac
}
