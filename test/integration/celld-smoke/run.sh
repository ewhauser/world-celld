#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd "$script_dir/../../.." && pwd)
download_root="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/world-celld-smoke-downloads"
runtime_root=$(mktemp -d "${TMPDIR:-/tmp}/world-celld-smoke-runtime.XXXXXX")

cleanup() {
  rm -rf "$runtime_root"
}
trap cleanup EXIT INT TERM

download_verified() {
  local url=$1
  local expected_sha256=$2
  local destination=$3

  if [[ -f "$destination" ]] &&
    [[ $(shasum -a 256 "$destination" | awk '{print $1}') == "$expected_sha256" ]]; then
    chmod 755 "$destination"
    return
  fi

  local temporary
  temporary=$(mktemp "$download_root/.download.XXXXXX")
  curl --fail --location --silent --show-error --retry 3 --retry-delay 1 \
    --proto '=https' --proto-redir '=https' --tlsv1.2 \
    --output "$temporary" "$url"

  local actual_sha256
  actual_sha256=$(shasum -a 256 "$temporary" | awk '{print $1}')
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    rm -f "$temporary"
    echo "error: checksum mismatch for $url" >&2
    echo "expected $expected_sha256, got $actual_sha256" >&2
    exit 1
  fi

  chmod 755 "$temporary"
  mv "$temporary" "$destination"
}

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    celld_asset=celld-x86_64-unknown-linux-gnu.gz
    celld_sha256=8f1e18072c234ab75459d4da104c13cebc9b29a3e4a4772086bf985829bea8aa
    minio_platform=linux-amd64
    minio_sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
    mc_sha256=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891
    ;;
  Darwin-arm64)
    celld_asset=celld-aarch64-apple-darwin.gz
    celld_sha256=bf6f0c06c4f815eecddf61ae40340935a0dbf76be3d643bf75fdd92bfac425cd
    minio_platform=darwin-arm64
    minio_sha256=7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4
    mc_sha256=a877fd0c183409da9f20f9d6e1811987298bbbca1aa03428eebdffba79fb9445
    ;;
  *)
    echo "error: the real-celld smoke supports Linux x86-64 and macOS arm64" >&2
    exit 1
    ;;
esac

mkdir -p "$download_root"

celld_archive="$download_root/v0.6.0-$celld_asset"
minio_binary="$download_root/minio-${minio_platform}-RELEASE.2025-09-07T16-13-09Z"
mc_binary="$download_root/mc-${minio_platform}-RELEASE.2025-08-13T08-35-41Z"

download_verified \
  "https://github.com/denoland/celld/releases/download/v0.6.0/$celld_asset" \
  "$celld_sha256" \
  "$celld_archive"
download_verified \
  "https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.$minio_platform.RELEASE.2025-09-07T16-13-09Z" \
  "$minio_sha256" \
  "$minio_binary"
download_verified \
  "https://github.com/minio/mc/releases/download/RELEASE.2025-08-13T08-35-41Z/mc.$minio_platform.RELEASE.2025-08-13T08-35-41Z" \
  "$mc_sha256" \
  "$mc_binary"

celld_binary="$runtime_root/celld"
gzip -dc "$celld_archive" > "$celld_binary"
chmod 755 "$celld_binary"

cd "$repo_root"
pnpm build
pnpm --dir examples/demo-app build

# Build matching test-only workflows with versioned Run methods under both
# runtimes. The shipped demo source is left untouched. Pin the older release
# commit rather than relying on a moving branch or tag.
baseline_app=${CELLD_SMOKE_BASELINE_APP:-}
stable_app=${CELLD_SMOKE_STABLE_APP:-}
fetch_baseline_app=${CELLD_SMOKE_FETCH_BASELINE_APP:-}
if [[ -z "$baseline_app" || -z "$stable_app" || -z "$fetch_baseline_app" ]]; then
  baseline_ref=0aacc8bc25ceef44249eb2491baf54b96f136e73
  if ! git cat-file -e "$baseline_ref^{commit}" 2>/dev/null; then
    git fetch --depth=1 origin "$baseline_ref"
  fi
  baseline_root="$runtime_root/baseline"
  stable_root="$runtime_root/stable"
  fetch_root="$runtime_root/baseline-fetch"
  mkdir -p "$baseline_root"
  mkdir -p "$stable_root"
  mkdir -p "$fetch_root"
  git archive "$baseline_ref" | tar -x -C "$baseline_root"
  git archive HEAD | tar -x -C "$stable_root"
  git archive "$baseline_ref" | tar -x -C "$fetch_root"
  for fixture_root in "$baseline_root" "$stable_root"; do
    cp test/fixtures/upgrade-order.ts "$fixture_root/examples/demo-app/workflows/order.ts"
    (
      cd "$fixture_root"
      pnpm install --frozen-lockfile
      pnpm build
      pnpm --dir examples/demo-app build
    )
  done
  cp test/fixtures/upgrade-fetch-order.ts "$fetch_root/examples/demo-app/workflows/order.ts"
  (
    cd "$fetch_root"
    pnpm install --frozen-lockfile
    pnpm build
    pnpm --dir examples/demo-app build
  )
  baseline_app="$baseline_root/examples/demo-app"
  stable_app="$stable_root/examples/demo-app"
  fetch_baseline_app="$fetch_root/examples/demo-app"
fi

CELLD_SMOKE_CELLD_BIN="$celld_binary" \
CELLD_SMOKE_MINIO_BIN="$minio_binary" \
CELLD_SMOKE_MC_BIN="$mc_binary" \
CELLD_SMOKE_TEMP_ROOT="$runtime_root/harness" \
CELLD_SMOKE_BASELINE_APP="$baseline_app" \
CELLD_SMOKE_STABLE_APP="$stable_app" \
CELLD_SMOKE_FETCH_BASELINE_APP="$fetch_baseline_app" \
  pnpm vitest run --config vitest.celld-smoke.config.ts "$@"
