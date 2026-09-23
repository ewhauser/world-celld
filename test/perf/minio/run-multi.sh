#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
compose_file="$script_dir/compose.yaml"

if docker compose version >/dev/null 2>&1; then
  compose=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  compose=(docker-compose)
else
  echo "error: Docker Compose is required" >&2
  exit 1
fi

export PERF_FAILOVER=1
export PERF_RESULTS_DIR="${PERF_RESULTS_DIR:-$repo_root/.perf-results}"
mkdir -p "$PERF_RESULTS_DIR"
rm -f "$PERF_RESULTS_DIR/failover-ready" "$PERF_RESULTS_DIR/failover-done"

cleanup() {
  "${compose[@]}" -f "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
"${compose[@]}" -f "$compose_file" up --build --wait --wait-timeout 180 celld
"${compose[@]}" -f "$compose_file" up --no-deps --exit-code-from perf perf >"$PERF_RESULTS_DIR/failover-run.log" 2>&1 &
perf_pid=$!

ready=0
for ((attempt=0; attempt<120; attempt++)); do
  if [[ -f "$PERF_RESULTS_DIR/failover-ready" ]]; then
    ready=1
    break
  fi
  if ! kill -0 "$perf_pid" 2>/dev/null; then
    cat "$PERF_RESULTS_DIR/failover-run.log" >&2
    wait "$perf_pid" || true
    echo "error: failover workload exited before reaching the fault barrier" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  cat "$PERF_RESULTS_DIR/failover-run.log" >&2
  echo "error: failover workload did not reach the fault barrier" >&2
  exit 1
fi

"${compose[@]}" -f "$compose_file" up --no-deps --wait --wait-timeout 180 celld-peer
"${compose[@]}" -f "$compose_file" kill -s SIGKILL celld
touch "$PERF_RESULTS_DIR/failover-done"

if ! wait "$perf_pid"; then
  cat "$PERF_RESULTS_DIR/failover-run.log" >&2
  exit 1
fi
cat "$PERF_RESULTS_DIR/failover-run.log"
