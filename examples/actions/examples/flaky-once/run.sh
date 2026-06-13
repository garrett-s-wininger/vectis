#!/bin/sh
set -eu

marker="${VECTIS_WORKSPACE}/.vectis-flaky-action-attempts"
attempt=1
if [ -f "$marker" ]; then
	previous="$(cat "$marker")"
	attempt=$((previous + 1))
fi

printf '%s\n' "$attempt" > "$marker"
target="${VECTIS_INPUT_SUCCEED_ON:-2}"

echo "canonical-registry-retry-attempt=${attempt}"
if [ "$attempt" -lt "$target" ]; then
	echo "canonical-registry-retry-failing"
	exit 1
fi

mkdir -p "${VECTIS_WORKSPACE}/reports"
{
	printf 'canonical-registry-retry-attempt=%s\n' "$attempt"
	printf '%s\n' "canonical-registry-retry-succeeded"
} > "${VECTIS_WORKSPACE}/reports/registry-retry.txt"

echo "canonical-registry-retry-succeeded"
