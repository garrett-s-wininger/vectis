#!/bin/sh
set -eu

IMAGE=${VECTIS_DOCTOR_IMAGE:-ubuntu:24.04}
ENGINE=${VECTIS_CONTAINER_ENGINE:-}
TARGETS=${VECTIS_SMOKE_TARGETS:-"scripts/dev-doctor.sh && SKIP_WEB_BUILD=1 mage buildContainer && mage proto && mage testQuick"}

usage() {
	cat <<'EOF'
Usage: scripts/dev-doctor-container.sh [--engine podman|docker] [--image IMAGE] [--targets COMMANDS]

Starts from a clean container image, runs scripts/dev-doctor.sh --install --yes,
sources the installed repo-local toolchain, then runs the target chain.

Defaults:
  image   ubuntu:24.04
  targets scripts/dev-doctor.sh && SKIP_WEB_BUILD=1 mage buildContainer && mage proto && mage testQuick

Environment:
  VECTIS_CONTAINER_ENGINE  Container engine override.
  VECTIS_DOCTOR_IMAGE      Base image override.
  VECTIS_SMOKE_TARGETS     Target chain override.
EOF
}

while [ "$#" -gt 0 ]; do
	case "$1" in
		--engine)
			ENGINE=${2:-}
			shift
			;;
		--image)
			IMAGE=${2:-}
			shift
			;;
		--targets)
			TARGETS=${2:-}
			shift
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

if [ -z "$ENGINE" ]; then
	if command -v podman >/dev/null 2>&1; then
		ENGINE=podman
	elif command -v docker >/dev/null 2>&1; then
		ENGINE=docker
	else
		echo "podman or docker is required to run the container smoke test." >&2
		echo "Install Podman from https://podman.io/docs/installation/ or Docker from https://docs.docker.com/get-docker/." >&2
		exit 1
	fi
fi

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
ROOT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

case "$ENGINE" in
	podman|docker)
		;;
	*)
		echo "unsupported container engine: $ENGINE" >&2
		echo "expected podman or docker" >&2
		exit 2
		;;
esac

echo "Using $ENGINE with image $IMAGE"
echo "Smoke targets: $TARGETS"

"$ENGINE" run --rm \
	-e "VECTIS_SMOKE_TARGETS=$TARGETS" \
	-v "$ROOT_DIR:/workspace:ro" \
	"$IMAGE" \
	sh -lc '
		set -eu
		rm -rf /tmp/vectis-smoke
		mkdir -p /tmp/vectis-smoke
		cp -a /workspace/. /tmp/vectis-smoke/
		cd /tmp/vectis-smoke
		rm -rf .tools
		scripts/dev-doctor.sh --install --yes
		. .tools/env.sh
		eval "$VECTIS_SMOKE_TARGETS"
	'
