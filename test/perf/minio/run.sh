#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
compose_file="$script_dir/compose.yaml"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: Docker with the Compose plugin is required" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "error: the Docker Compose plugin is required" >&2
  exit 1
fi

export PERF_RESULTS_DIR="${PERF_RESULTS_DIR:-$repo_root/.perf-results}"
mkdir -p "$PERF_RESULTS_DIR"

cleanup() {
  docker compose -f "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cleanup
docker compose -f "$compose_file" up --build --wait --wait-timeout 180 celld
docker compose -f "$compose_file" up --no-deps --exit-code-from perf perf
