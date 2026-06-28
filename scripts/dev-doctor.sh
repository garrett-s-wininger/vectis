#!/bin/sh
set -u

PROTOC_GEN_GO_VERSION="v1.36.11"
PROTOC_GEN_GO_GRPC_VERSION="v1.6.1"
MAGE_VERSION="v1.17.2"
PROTOC_VERSION="${VECTIS_PROTOC_VERSION:-34.1}"
MIN_PROTOC_VERSION="25.0"
MIN_NODE_MAJOR="20"

INSTALL_REQUIRED=0
INSTALL_GO_TOOLS=0
INSTALL_OPTIONAL=0
NO_SQLITE=0
YES=0

usage() {
	cat <<'EOF'
Usage: scripts/dev-doctor.sh [--install] [--install-go-tools] [--install-optional] [--yes] [--no-sqlite]

Checks the local Vectis development toolchain with friendly install guidance.

Options:
  --install           Install the required Unix development toolchain.
  --install-go-tools  Install Mage, protoc-gen-go, and protoc-gen-go-grpc with go install.
  --install-optional  Best-effort install for optional lanes such as containers and formal verification.
  --yes, -y           Run supported package managers in non-interactive mode.
  --no-sqlite         Skip CGO/C compiler checks for nosqlite development lanes.
  -h, --help          Show this help.
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--install)
			INSTALL_REQUIRED=1
			INSTALL_GO_TOOLS=1
			;;
		--install-go-tools)
			INSTALL_GO_TOOLS=1
			;;
		--install-optional)
			INSTALL_OPTIONAL=1
			;;
		--yes|-y)
			YES=1
			;;
		--no-sqlite)
			NO_SQLITE=1
			;;
		-h|--help)
			usage
			exit 0
			;;
		*)
			echo "unknown argument: $1" >&2
			usage >&2
			exit 2
			;;
	esac
	shift
done

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
GO_MOD="$ROOT_DIR/go.mod"
MIN_GO=$(awk '/^go / { print $2; exit }' "$GO_MOD")
TOOL_PREFIX=${VECTIS_TOOL_PREFIX:-"$ROOT_DIR/.tools"}

if [ -d "$TOOL_PREFIX/bin" ]; then
	PATH="$TOOL_PREFIX/bin:$PATH"
fi

if [ -d "$TOOL_PREFIX/protoc/bin" ]; then
	PATH="$TOOL_PREFIX/protoc/bin:$PATH"
fi

if [ -d "$TOOL_PREFIX/go/bin" ]; then
	PATH="$TOOL_PREFIX/go/bin:$PATH"
fi

if [ -d "$TOOL_PREFIX/node/bin" ]; then
	PATH="$TOOL_PREFIX/node/bin:$PATH"
fi

export PATH

failures=0
warnings=0

section() {
	printf '\n%s\n' "$1"
}

pass() {
	printf '  [ok] %s\n' "$1"
}

warn() {
	warnings=$((warnings + 1))
	printf '  [warn] %s\n' "$1"
}

fail() {
	failures=$((failures + 1))
	printf '  [missing] %s\n' "$1"
}

note() {
	printf '          %s\n' "$1"
}

has_command() {
	command -v "$1" >/dev/null 2>&1
}

run_cmd() {
	printf '  [run] %s\n' "$*"
	"$@"
}

sudo_cmd() {
	if [ "$(id -u)" -eq 0 ]; then
		"$@"
	elif has_command sudo; then
		sudo "$@"
	else
		fail "cannot run privileged command because sudo is not available: $*"
		return 1
	fi
}

normalize_version() {
	printf '%s' "$1" | sed 's/^[^0-9]*//' | sed 's/[^0-9.].*$//'
}

version_ge() {
	awk -v got="$1" -v want="$2" '
		BEGIN {
			split(got, g, ".")
			split(want, w, ".")
			for (i = 1; i <= 3; i++) {
				gi = g[i] + 0
				wi = w[i] + 0
				if (gi > wi) exit 0
				if (gi < wi) exit 1
			}
			exit 0
		}'
}

go_tool_path() {
	tool="$1"
	if [ -x "$TOOL_PREFIX/bin/$tool" ]; then
		printf '%s\n' "$TOOL_PREFIX/bin/$tool"
		return 0
	fi

	if has_command "$tool"; then
		command -v "$tool"
		return 0
	fi

	if has_command go; then
		gopath=$(go env GOPATH 2>/dev/null)
		if [ -n "$gopath" ] && [ -x "$gopath/bin/$tool" ]; then
			printf '%s\n' "$gopath/bin/$tool"
			return 0
		fi
	fi

	return 1
}

download_file() {
	url="$1"
	dest="$2"

	if has_command curl; then
		curl -fsSL "$url" -o "$dest"
	elif has_command wget; then
		wget -q "$url" -O "$dest"
	else
		return 127
	fi
}

download_stdout() {
	url="$1"

	if has_command curl; then
		curl -fsSL "$url"
	elif has_command wget; then
		wget -q "$url" -O -
	else
		return 127
	fi
}

host_os() {
	case "$(uname -s)" in
		Linux) printf 'linux' ;;
		Darwin) printf 'darwin' ;;
		*) printf 'unsupported' ;;
	esac
}

host_arch_go() {
	case "$(uname -m)" in
		x86_64|amd64) printf 'amd64' ;;
		arm64|aarch64) printf 'arm64' ;;
		*) printf 'unsupported' ;;
	esac
}

host_arch_node() {
	case "$(uname -m)" in
		x86_64|amd64) printf 'x64' ;;
		arm64|aarch64) printf 'arm64' ;;
		*) printf 'unsupported' ;;
	esac
}

host_os_protoc() {
	case "$(uname -s)" in
		Linux) printf 'linux' ;;
		Darwin) printf 'osx' ;;
		*) printf 'unsupported' ;;
	esac
}

host_arch_protoc() {
	case "$(uname -m)" in
		x86_64|amd64) printf 'x86_64' ;;
		arm64|aarch64) printf 'aarch_64' ;;
		*) printf 'unsupported' ;;
	esac
}

install_go_tool() {
	module="$1"
	if ! has_command go; then
		fail "cannot install $module because go is not on PATH"
		return
	fi

	mkdir -p "$TOOL_PREFIX/bin"
	printf '  [run] go install %s\n' "$module"

	if GOBIN="$TOOL_PREFIX/bin" go install "$module"; then
		pass "installed $module to $TOOL_PREFIX/bin"
	else
		fail "go install failed for $module"
		note "Check network access and GOPATH/GOBIN permissions, then rerun this script."
	fi
}

detect_package_manager() {
	for manager in apt-get dnf yum apk pacman zypper brew; do
		if has_command "$manager"; then
			printf '%s\n' "$manager"
			return 0
		fi
	done
	return 1
}

install_package_prereqs() {
	section "Installing Required Packages"

	manager=$(detect_package_manager || true)
	if [ -z "$manager" ]; then
		fail "no supported Unix package manager found"
		note "Supported installers: apt-get, dnf, yum, apk, pacman, zypper, brew."
		return
	fi

	pass "using package manager: $manager"

	case "$manager" in
	apt-get)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		run_cmd sudo_cmd apt-get update || return

		# shellcheck disable=SC2086
		run_cmd sudo_cmd apt-get install $yes_arg ca-certificates curl git make protobuf-compiler build-essential tar gzip xz-utils unzip || return
		pass "installed apt packages for required development toolchain"
		;;

	dnf)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd dnf install $yes_arg ca-certificates curl git make protobuf-compiler gcc gcc-c++ glibc-devel tar gzip xz unzip || return
		pass "installed dnf packages for required development toolchain"
		;;

	yum)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd yum install $yes_arg ca-certificates curl git make protobuf-compiler gcc gcc-c++ glibc-devel tar gzip xz unzip || return
		pass "installed yum packages for required development toolchain"
		;;

	apk)
		run_cmd sudo_cmd apk add ca-certificates curl git make protobuf build-base tar gzip xz unzip || return
		pass "installed apk packages for required development toolchain"
		;;

	pacman)
		yes_args=""
		if [ "$YES" -eq 1 ]; then
			yes_args="--noconfirm"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd pacman -Sy --needed $yes_args ca-certificates curl git make protobuf base-devel tar gzip xz unzip || return
		pass "installed pacman packages for required development toolchain"
		;;

	zypper)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-n"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd zypper install $yes_arg ca-certificates curl git make protobuf-devel gcc gcc-c++ tar gzip xz unzip || return
		pass "installed zypper packages for required development toolchain"
		;;

	brew)
		run_cmd brew install git make protobuf curl xz || return
		pass "installed Homebrew packages for required development toolchain"

		if ! xcode-select -p >/dev/null 2>&1; then
			warn "Xcode Command Line Tools are not installed"
			note "Run: xcode-select --install"
		fi
		;;
	esac
}

install_local_go() {
	if has_command go; then
		go_version_raw=$(go env GOVERSION 2>/dev/null)
		go_version=$(normalize_version "$go_version_raw")
		if version_ge "$go_version" "$MIN_GO"; then
			pass "go $go_version_raw already satisfies $MIN_GO+"
			return
		fi
	fi

	os=$(host_os)
	arch=$(host_arch_go)
	if [ "$os" = "unsupported" ] || [ "$arch" = "unsupported" ]; then
		fail "cannot install Go automatically on $(uname -s)/$(uname -m)"
		note "Install Go $MIN_GO+ from https://go.dev/doc/install."
		return
	fi

	url="https://go.dev/dl/go${MIN_GO}.${os}-${arch}.tar.gz"
	tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/vectis-go.XXXXXX")
	archive="$tmpdir/go.tar.gz"
	printf '  [download] %s\n' "$url"

	if ! download_file "$url" "$archive"; then
		fail "failed to download Go $MIN_GO"
		note "Install Go $MIN_GO+ from https://go.dev/doc/install, or check network access and rerun."
		rm -rf "$tmpdir"
		return
	fi

	mkdir -p "$TOOL_PREFIX"
	rm -rf "$TOOL_PREFIX/go"

	if tar -C "$TOOL_PREFIX" -xzf "$archive"; then
		pass "installed Go $MIN_GO to $TOOL_PREFIX/go"
	else
		fail "failed to extract Go archive"
	fi

	rm -rf "$tmpdir"
	PATH="$TOOL_PREFIX/go/bin:$PATH"
	export PATH
}

install_local_node() {
	if has_command node; then
		node_raw=$(node --version 2>/dev/null)
		node_major=$(printf '%s' "$node_raw" | sed 's/^v//' | sed 's/\..*$//')

		if [ "$node_major" -ge "$MIN_NODE_MAJOR" ] 2>/dev/null; then
			pass "node $node_raw already satisfies $MIN_NODE_MAJOR+"
			return
		fi
	fi

	os=$(host_os)
	arch=$(host_arch_node)

	if [ "$os" = "unsupported" ] || [ "$arch" = "unsupported" ]; then
		fail "cannot install Node.js automatically on $(uname -s)/$(uname -m)"
		note "Install Node.js $MIN_NODE_MAJOR+ from https://nodejs.org/."
		return
	fi

	index_url="https://nodejs.org/dist/index.tab"
	version=$(download_stdout "$index_url" | awk -v major="v${MIN_NODE_MAJOR}." 'NR > 1 && index($1, major) == 1 { print $1; exit }')

	if [ -z "$version" ]; then
		fail "could not find a Node.js ${MIN_NODE_MAJOR}.x release in $index_url"
		note "Install Node.js $MIN_NODE_MAJOR+ from https://nodejs.org/."
		return
	fi

	archive="node-${version}-${os}-${arch}.tar.xz"
	tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/vectis-node.XXXXXX")
	archive_path="$tmpdir/node.tar.xz"
	url="https://nodejs.org/dist/${version}/${archive}"
	printf '  [download] %s\n' "$url"

	if ! download_file "$url" "$archive_path"; then
		fail "failed to download Node.js $archive"
		note "Install Node.js $MIN_NODE_MAJOR+ from https://nodejs.org/, or check network access and rerun."
		rm -rf "$tmpdir"
		return
	fi

	mkdir -p "$TOOL_PREFIX"
	rm -rf "$TOOL_PREFIX/node"

	if tar -C "$tmpdir" -xJf "$archive_path"; then
		mv "$tmpdir/${archive%.tar.xz}" "$TOOL_PREFIX/node"
		pass "installed Node.js to $TOOL_PREFIX/node"
	else
		fail "failed to extract Node.js archive"
	fi

	rm -rf "$tmpdir"
	PATH="$TOOL_PREFIX/node/bin:$PATH"
	export PATH
}

install_local_protoc() {
	if has_command protoc; then
		protoc_raw=$(protoc --version 2>/dev/null)
		protoc_version=$(normalize_version "$protoc_raw")

		if version_ge "$protoc_version" "$MIN_PROTOC_VERSION"; then
			pass "protoc $protoc_raw already satisfies $MIN_PROTOC_VERSION+"
			return
		fi
	fi

	if ! has_command unzip; then
		fail "cannot install protoc automatically because unzip is not on PATH"
		note "Install unzip, or install protoc $MIN_PROTOC_VERSION+ from https://protobuf.dev/installation/."
		return
	fi

	os=$(host_os_protoc)
	arch=$(host_arch_protoc)

	if [ "$os" = "unsupported" ] || [ "$arch" = "unsupported" ]; then
		fail "cannot install protoc automatically on $(uname -s)/$(uname -m)"
		note "Install protoc $MIN_PROTOC_VERSION+ from https://protobuf.dev/installation/."
		return
	fi

	archive="protoc-${PROTOC_VERSION}-${os}-${arch}.zip"
	url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${archive}"
	tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/vectis-protoc.XXXXXX")
	archive_path="$tmpdir/protoc.zip"
	printf '  [download] %s\n' "$url"

	if ! download_file "$url" "$archive_path"; then
		fail "failed to download protoc $PROTOC_VERSION"
		note "Install protoc $MIN_PROTOC_VERSION+ from https://protobuf.dev/installation/, or check network access and rerun."
		rm -rf "$tmpdir"
		return
	fi

	if unzip -q "$archive_path" -d "$tmpdir/protoc"; then
		mkdir -p "$TOOL_PREFIX"
		rm -rf "$TOOL_PREFIX/protoc"
		mv "$tmpdir/protoc" "$TOOL_PREFIX/protoc"
		pass "installed protoc $PROTOC_VERSION to $TOOL_PREFIX/protoc"
	else
		fail "failed to extract protoc archive"
	fi

	rm -rf "$tmpdir"
	PATH="$TOOL_PREFIX/protoc/bin:$PATH"
	export PATH
}

write_env_file() {
	mkdir -p "$TOOL_PREFIX"
	cat > "$TOOL_PREFIX/env.sh" <<EOF
# Source this file to use the Vectis repo-local toolchain installed by scripts/dev-doctor.sh.
export PATH="$TOOL_PREFIX/bin:$TOOL_PREFIX/protoc/bin:$TOOL_PREFIX/go/bin:$TOOL_PREFIX/node/bin:\$PATH"
export MAGE="$TOOL_PREFIX/bin/mage"
export PROTOC_GEN_GO="$TOOL_PREFIX/bin/protoc-gen-go"
export PROTOC_GEN_GO_GRPC="$TOOL_PREFIX/bin/protoc-gen-go-grpc"
EOF
	pass "wrote $TOOL_PREFIX/env.sh"
	note "Run: . \"$TOOL_PREFIX/env.sh\""
}

install_optional_packages() {
	section "Installing Optional Packages"

	manager=$(detect_package_manager || true)
	if [ -z "$manager" ]; then
		warn "no supported package manager found for optional installs"
		return
	fi

	pass "using package manager: $manager"

	case "$manager" in
	apt-get)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd apt-get install $yes_arg podman openjdk-21-jdk || warn "optional apt install failed"
		note "Packer is not installed from default apt repositories. See https://developer.hashicorp.com/packer/install."
		;;

	dnf)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd dnf install $yes_arg podman packer java-21-openjdk-devel || warn "optional dnf install failed"
		;;

	yum)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-y"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd yum install $yes_arg podman java-21-openjdk-devel || warn "optional yum install failed"
		note "Packer is not installed from default yum repositories. See https://developer.hashicorp.com/packer/install."
		;;

	apk)
		run_cmd sudo_cmd apk add podman openjdk21 || warn "optional apk install failed"
		note "Packer is not installed from default apk repositories. See https://developer.hashicorp.com/packer/install."
		;;

	pacman)
		yes_args=""
		if [ "$YES" -eq 1 ]; then
			yes_args="--noconfirm"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd pacman -Sy --needed $yes_args podman packer jdk21-openjdk || warn "optional pacman install failed"
		;;

	zypper)
		yes_arg=""
		if [ "$YES" -eq 1 ]; then
			yes_arg="-n"
		fi

		# shellcheck disable=SC2086
		run_cmd sudo_cmd zypper install $yes_arg podman java-21-openjdk-devel || warn "optional zypper install failed"
		note "Packer is not installed from default zypper repositories. See https://developer.hashicorp.com/packer/install."
		;;

	brew)
		run_cmd brew install podman packer openjdk || warn "optional Homebrew install failed"
		;;
	esac
}

check_required_command() {
	name="$1"
	purpose="$2"
	guidance="$3"

	if has_command "$name"; then
		path=$(command -v "$name")
		pass "$name found at $path ($purpose)"
	else
		fail "$name not found ($purpose)"
		note "$guidance"
	fi
}

check_optional_command() {
	name="$1"
	purpose="$2"
	guidance="$3"

	if has_command "$name"; then
		path=$(command -v "$name")
		pass "$name found at $path ($purpose)"
	else
		warn "$name not found ($purpose)"
		note "$guidance"
	fi
}

if [ "$INSTALL_REQUIRED" -eq 1 ]; then
	install_package_prereqs
	install_local_go
	install_local_node
	install_local_protoc
	write_env_file
fi

if [ "$INSTALL_OPTIONAL" -eq 1 ]; then
	install_optional_packages
fi

if [ "$INSTALL_GO_TOOLS" -eq 1 ]; then
	section "Installing Go Tools"
	install_go_tool "github.com/magefile/mage@$MAGE_VERSION"
	install_go_tool "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
	install_go_tool "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC_VERSION"
fi

section "Required Tools"

if has_command go; then
	go_version_raw=$(go env GOVERSION 2>/dev/null)
	go_version=$(normalize_version "$go_version_raw")

	if version_ge "$go_version" "$MIN_GO"; then
		pass "go $go_version_raw satisfies go.mod requirement $MIN_GO+"
	else
		fail "go $go_version_raw is older than go.mod requirement $MIN_GO"
		note "Install Go $MIN_GO+ from https://go.dev/doc/install."
		note "Or run: scripts/dev-doctor.sh --install --yes"
	fi
else
	fail "go not found"
	note "Install Go $MIN_GO+ from https://go.dev/doc/install."
	note "Or run: scripts/dev-doctor.sh --install --yes"
fi

check_required_command "git" "source control and ci-quick worktree checks" \
	"Install Git from https://git-scm.com/downloads, or run scripts/dev-doctor.sh --install --yes."

check_required_command "make" "current Unix build/test entrypoint until Mage owns portable workflows" \
	"Install Make through your OS tools, or run scripts/dev-doctor.sh --install --yes."

if path=$(go_tool_path "mage"); then
	version=$("$path" --version 2>/dev/null | sed -n '1p' || true)
	pass "mage found at $path ${version:+($version)}"
else
	fail "mage not found"
	note "Run: scripts/dev-doctor.sh --install-go-tools"
	note "Or: go install github.com/magefile/mage@$MAGE_VERSION"
fi

if has_command node; then
	node_raw=$(node --version 2>/dev/null)
	node_major=$(printf '%s' "$node_raw" | sed 's/^v//' | sed 's/\..*$//')

	if [ "$node_major" -ge "$MIN_NODE_MAJOR" ] 2>/dev/null; then
		pass "node $node_raw satisfies docs build requirement $MIN_NODE_MAJOR+"
	else
		fail "node $node_raw is older than docs build requirement $MIN_NODE_MAJOR+"
		note "Install Node.js $MIN_NODE_MAJOR+ from https://nodejs.org/."
		note "Or run: scripts/dev-doctor.sh --install --yes"
	fi
else
	fail "node not found (default build embeds the docs site)"
	note "Install Node.js $MIN_NODE_MAJOR+ from https://nodejs.org/."
	note "Or run: scripts/dev-doctor.sh --install --yes"
	note "Use SKIP_WEB_BUILD=1 only when intentionally skipping vectis-docs assets."
fi

check_required_command "npm" "website dependency installation and docs build" \
	"npm ships with Node.js. Install Node.js $MIN_NODE_MAJOR+ or run scripts/dev-doctor.sh --install --yes."

if has_command protoc; then
	protoc_raw=$(protoc --version 2>/dev/null)
	protoc_version=$(normalize_version "$protoc_raw")

	if version_ge "$protoc_version" "$MIN_PROTOC_VERSION"; then
		pass "protoc $protoc_raw satisfies protobuf edition 2023 requirement $MIN_PROTOC_VERSION+"
	else
		fail "protoc $protoc_raw is older than required $MIN_PROTOC_VERSION+"
		note "api/proto uses edition = \"2023\"; older distro protoc packages parse it as proto2."
		note "Install protoc $MIN_PROTOC_VERSION+ from https://protobuf.dev/installation/, or run scripts/dev-doctor.sh --install --yes."
	fi
else
	fail "protoc not found (protobuf regeneration with make proto)"
	note "Install protoc $MIN_PROTOC_VERSION+ from https://protobuf.dev/installation/, or run scripts/dev-doctor.sh --install --yes."
fi

if path=$(go_tool_path "protoc-gen-go"); then
	version=$("$path" --version 2>/dev/null || true)
	pass "protoc-gen-go found at $path ${version:+($version)}"
else
	fail "protoc-gen-go not found"
	note "Run: scripts/dev-doctor.sh --install-go-tools"
	note "Or: go install google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
fi

if path=$(go_tool_path "protoc-gen-go-grpc"); then
	version=$("$path" --version 2>/dev/null || true)
	pass "protoc-gen-go-grpc found at $path ${version:+($version)}"
else
	fail "protoc-gen-go-grpc not found"
	note "Run: scripts/dev-doctor.sh --install-go-tools"
	note "Or: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC_VERSION"
fi

if [ "$NO_SQLITE" -eq 0 ]; then
	section "SQLite / CGO"
	if has_command go; then
		cgo_enabled=$(go env CGO_ENABLED 2>/dev/null)
		if [ "$cgo_enabled" = "1" ]; then
			pass "CGO_ENABLED=1"
		else
			fail "CGO_ENABLED=$cgo_enabled, but local SQLite development needs CGO_ENABLED=1"
			note "Unset CGO_ENABLED or set CGO_ENABLED=1. Use --no-sqlite for nosqlite build lanes."
		fi

		cc=$(go env CC 2>/dev/null)
		if [ -n "$cc" ] && has_command "$cc"; then
			pass "C compiler found via go env CC=$cc"
		elif has_command cc; then
			pass "C compiler found at $(command -v cc)"
		elif has_command clang; then
			pass "C compiler found at $(command -v clang)"
		elif has_command gcc; then
			pass "C compiler found at $(command -v gcc)"
		else
			fail "no C compiler found for CGO SQLite builds"
			note "Install a C toolchain, or run scripts/dev-doctor.sh --install --yes."
		fi
	fi
else
	section "SQLite / CGO"
	warn "skipping CGO/C compiler checks because --no-sqlite was supplied"
fi

section "Optional Lanes"
check_optional_command "podman" "container image targets" \
	"Install Podman from https://podman.io/docs/installation/ or try scripts/dev-doctor.sh --install-optional --yes."
check_optional_command "packer" "VM/package e2e preparation" \
	"Install Packer from https://developer.hashicorp.com/packer/install or try scripts/dev-doctor.sh --install-optional --yes."
check_optional_command "java" "formal-verification TLA+ target" \
	"Install a JDK and set TLA_TOOLS_JAR if you need make formal-verification."

tla_jar=${TLA_TOOLS_JAR:-/opt/tla+/tla2tools.jar}
if [ -f "$tla_jar" ]; then
	pass "TLA+ tools jar found at $tla_jar"
else
	warn "TLA+ tools jar not found at $tla_jar"
	note "Set TLA_TOOLS_JAR=/path/to/tla2tools.jar when using formal-verification targets."
fi

section "Summary"
if [ "$failures" -gt 0 ]; then
	printf '  %s required check(s) failed; %s warning(s).\n' "$failures" "$warnings"
	printf '  Fix the missing required tools above, then rerun scripts/dev-doctor.sh.\n'
	exit 1
fi

printf '  All required checks passed; %s warning(s).\n' "$warnings"
