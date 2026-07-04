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

require_sudo "the package builder"
install_apt_packages "package builder preparation currently supports apt-based Linux guests" ca-certificates curl tar xz-utils build-essential git

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
printf '%s\n' 'export PATH=/usr/local/go/bin:/usr/local/bin:$PATH' | sudo tee /etc/profile.d/vectis-package-builder.sh >/dev/null

PATH=/usr/local/go/bin:/usr/local/bin:$PATH
export PATH

if [ "$(go env GOVERSION)" != "go${go_version}" ]; then
	echo "prepared builder has $(go env GOVERSION), want go${go_version}" >&2
	exit 1
fi

mage_tmpdir=$(mktemp -d)
trap 'rm -rf "$mage_tmpdir" ${tmpdir:-}' EXIT
GOBIN="$mage_tmpdir" go install github.com/magefile/mage@v1.17.2
sudo install -m 0755 "$mage_tmpdir/mage" /usr/local/bin/mage

mage --version >/dev/null
if command -v cc >/dev/null 2>&1; then
	cc --version >/dev/null
else
	echo "prepared builder is missing cc" >&2
	exit 1
fi

write_prep_marker /etc/vectis-vm-prep/package-builder-prep-version "$prep_version"
