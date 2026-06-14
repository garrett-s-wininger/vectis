#!/bin/sh
set -eu

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

if [ -z "$go_version" ]; then
	echo "VECTIS_PACKER_GO_VERSION is required" >&2
	exit 1
fi

if ! command -v "$limactl_bin" >/dev/null 2>&1; then
	echo "limactl is required to prepare the Lima package builder" >&2
	exit 1
fi

if "$limactl_bin" list "$instance" >/dev/null 2>&1; then
	echo "using existing Lima instance $instance"
else
	echo "creating Lima package builder $instance from template:$template"
	"$limactl_bin" --tty=false create \
		--name="$instance" \
		--cpus="$cpus" \
		--memory="$memory" \
		--disk="$disk" \
		--mount-none \
		"template:$template"
fi

"$limactl_bin" --tty=false start "$instance"

"$limactl_bin" --tty=false shell "$instance" -- sh -s -- "$go_version" "$go_sha256" "$cache_root" "$workspace_root" "$prep_version" <<'GUEST'
set -eu

go_version=$1
go_sha256=$2
cache_root=$3
workspace_root=$4
prep_version=$5

case "$(uname -m)" in
	x86_64)
		go_arch=amd64
		;;
	aarch64|arm64)
		go_arch=arm64
		;;
	*)
		echo "unsupported package builder architecture: $(uname -m)" >&2
		exit 1
		;;
esac

if ! command -v sudo >/dev/null 2>&1; then
	echo "sudo is required inside the package builder" >&2
	exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
	echo "package builder preparation currently supports apt-based Linux guests" >&2
	exit 1
fi

export DEBIAN_FRONTEND=noninteractive
sudo apt-get update
sudo apt-get install -y --no-install-recommends ca-certificates curl tar xz-utils build-essential make git

install_go=1
if [ -x "/usr/local/go-${go_version}/bin/go" ] && [ "$(/usr/local/go-${go_version}/bin/go env GOVERSION)" = "go${go_version}" ]; then
	install_go=0
fi

if [ "$install_go" = "1" ]; then
	tmpdir=$(mktemp -d)
	trap 'rm -rf "$tmpdir"' EXIT
	go_archive="$tmpdir/go.tgz"
	go_url="https://go.dev/dl/go${go_version}.linux-${go_arch}.tar.gz"

	curl -fsSL "$go_url" -o "$go_archive"
	if [ -n "$go_sha256" ]; then
		echo "$go_sha256  $go_archive" | sha256sum -c -
	fi

	sudo rm -rf "/usr/local/go-${go_version}"
	sudo mkdir -p "/usr/local/go-${go_version}"
	sudo tar -C "/usr/local/go-${go_version}" --strip-components=1 -xzf "$go_archive"
fi

sudo ln -sfn "/usr/local/go-${go_version}" /usr/local/go
sudo install -d -m 1777 "$cache_root" "$cache_root/go-build" "$cache_root/gomod" "$workspace_root"
sudo install -d -m 0755 /etc/vectis-vm-prep
printf '%s\n' 'export PATH=/usr/local/go/bin:$PATH' | sudo tee /etc/profile.d/vectis-package-builder.sh >/dev/null
printf '%s\n' "$prep_version" | sudo tee /etc/vectis-vm-prep/package-builder-prep-version >/dev/null

PATH=/usr/local/go/bin:$PATH
export PATH

if [ "$(go env GOVERSION)" != "go${go_version}" ]; then
	echo "prepared builder has $(go env GOVERSION), want go${go_version}" >&2
	exit 1
fi

make --version >/dev/null
if command -v cc >/dev/null 2>&1; then
	cc --version >/dev/null
else
	echo "prepared builder is missing cc" >&2
	exit 1
fi
GUEST

case "$stop_after" in
	1|t|T|true|TRUE|y|Y|yes|YES|on|ON)
		"$limactl_bin" --tty=false stop "$instance"
		;;
esac
