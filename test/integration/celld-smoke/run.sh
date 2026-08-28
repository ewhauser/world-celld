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
    celld_sha256=0488628597154725db2f61f85434fb381e1b2535d1e9f097c6d20727cd337973
    minio_platform=linux-amd64
    minio_sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
    mc_sha256=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891
    ;;
  Darwin-arm64)
    celld_asset=celld-aarch64-apple-darwin.gz
    celld_sha256=83311694b4b0797f3e12eaa581107de8b0d16b7c47d0e4edc1316445a0319bbe
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

celld_archive="$download_root/$celld_asset"
minio_binary="$download_root/minio-${minio_platform}-RELEASE.2025-09-07T16-13-09Z"
mc_binary="$download_root/mc-${minio_platform}-RELEASE.2025-08-13T08-35-41Z"

download_verified \
  "https://github.com/denoland/celld/releases/download/v0.4.0/$celld_asset" \
  "$celld_sha256" \
  "$celld_archive"
download_verified \
  "https://dl.min.io/server/minio/release/$minio_platform/archive/minio.RELEASE.2025-09-07T16-13-09Z" \
  "$minio_sha256" \
  "$minio_binary"
download_verified \
  "https://dl.min.io/client/mc/release/$minio_platform/archive/mc.RELEASE.2025-08-13T08-35-41Z" \
  "$mc_sha256" \
  "$mc_binary"

celld_binary="$runtime_root/celld"
gzip -dc "$celld_archive" > "$celld_binary"
chmod 755 "$celld_binary"

cd "$repo_root"
pnpm build

CELLD_SMOKE_CELLD_BIN="$celld_binary" \
CELLD_SMOKE_MINIO_BIN="$minio_binary" \
CELLD_SMOKE_MC_BIN="$mc_binary" \
CELLD_SMOKE_TEMP_ROOT="$runtime_root/harness" \
  pnpm vitest run --config vitest.celld-smoke.config.ts
