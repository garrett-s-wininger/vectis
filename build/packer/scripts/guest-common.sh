set -eu

require_command() {
	command_name=$1
	message=$2

	if ! command -v "$command_name" >/dev/null 2>&1; then
		echo "$message" >&2
		exit 1
	fi
}

require_commands() {
	for command_name in "$@"; do
		command -v "$command_name" >/dev/null
	done
}

require_sudo() {
	label=$1
	require_command sudo "sudo is required inside $label"
}

install_apt_packages() {
	message=$1
	shift

	require_command apt-get "$message"
	sudo apt-get update
	sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@"
}

install_rpm_packages() {
	message=$1
	shift

	if command -v dnf >/dev/null 2>&1; then
		sudo dnf -y install "$@"
	elif command -v microdnf >/dev/null 2>&1; then
		sudo microdnf -y install "$@"
	else
		echo "$message" >&2
		exit 1
	fi
}

write_prep_marker() {
	path=$1
	value=$2

	sudo install -d -m 0755 /etc/vectis-vm-prep
	printf '%s\n' "$value" | sudo tee "$path" >/dev/null
}
