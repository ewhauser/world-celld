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

export PERF_RESULTS_DIR="${PERF_RESULTS_DIR:-$repo_root/.perf-results}"
mkdir -p "$PERF_RESULTS_DIR"

cleanup() {
  "${compose[@]}" -f "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
"${compose[@]}" -f "$compose_file" up --build --wait --wait-timeout 180 celld
"${compose[@]}" -f "$compose_file" up --no-deps --exit-code-from perf perf
