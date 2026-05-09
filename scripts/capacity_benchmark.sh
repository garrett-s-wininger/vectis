#!/usr/bin/env bash
set -euo pipefail

BENCHTIME="${VECTIS_CAPACITY_BENCHTIME:-2s}"
COUNT="${VECTIS_CAPACITY_COUNT:-1}"
QUEUE_BENCH="${VECTIS_CAPACITY_QUEUE_BENCH:-BenchmarkQueue_(EnqueueDequeue_RoundTrip|ConcurrentEnqueueDequeue|SustainedLoad|RingLatency)}"
GO="${GO:-go}"

print_heading() {
  printf '\n%s\n' "$1"
  printf '%*s\n' "${#1}" '' | tr ' ' '='
}

print_heading "Vectis capacity benchmark"
printf 'Benchmark duration : %s\n' "${BENCHTIME}"
printf 'Repetitions        : %s\n' "${COUNT}"
printf 'Benchmark pattern  : %s\n' "${QUEUE_BENCH}"

set +e
BENCH_OUTPUT="$("${GO}" test ./internal/queue \
  -run '^$' \
  -bench "${QUEUE_BENCH}" \
  -benchtime "${BENCHTIME}" \
  -count "${COUNT}" \
  -benchmem 2>&1)"
BENCH_STATUS=$?
set -e

print_heading "Environment"
printf '%s\n' "${BENCH_OUTPUT}" | awk '
  /^goos:/ || /^goarch:/ || /^pkg:/ || /^cpu:/ {
    key=$1
    sub(/:$/, "", key)
    value=$0
    sub(/^[^:]+:[[:space:]]*/, "", value)
    printf "%-8s %s\n", key ":", value
  }
'

if [ "${BENCH_STATUS}" -ne 0 ]; then
  print_heading "Benchmark failed"
  printf '%s\n' "${BENCH_OUTPUT}"
  exit "${BENCH_STATUS}"
fi

print_heading "Benchmark summary"
printf '%s\n' "${BENCH_OUTPUT}" | awk '
  /^Benchmark/ {
    name=$1
    sub(/-[0-9]+$/, "", name)
    iterations=$2
    printf "- %s\n", name
    printf "  iterations: %s\n", iterations
    for (i = 3; i <= NF; i += 2) {
      if ((i + 1) <= NF) {
        printf "  %-18s %s\n", $(i + 1) ":", $i
      }
    }
  }
'

print_heading "Raw Go benchmark output"
printf '%s\n' "${BENCH_OUTPUT}"

cat <<'EOF'

Next manual checks
==================
- Trigger bursts: submit stored-job triggers in batches and record accepted latency plus queued-run age.
- Worker scale: vary worker count and watch claim failures, queue depth, and terminal run latency.
- Log readers: open concurrent /api/v1/runs/{id}/logs clients and watch active stream and replay metrics.
- Cron scale: seed schedules, advance time, and record schedule-to-run latency.
EOF
