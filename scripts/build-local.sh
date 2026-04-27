#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TAG="${TAG:-latest}"
PLATFORM="${PLATFORM:-linux/amd64}"
OUTPUT_DIR="${OUTPUT_DIR:-$ROOT_DIR/build}"

export GITHUB_TOKEN="${GITHUB_TOKEN:?GITHUB_TOKEN is required}"

scripts/verify-release-images.sh

docker buildx create --use --name watt-builder 2>/dev/null || docker buildx use watt-builder

echo "building wattswarm (platform=${PLATFORM}, tag=${TAG})..."

docker buildx build \
  --platform "${PLATFORM}" \
  --secret id=github_token,env=GITHUB_TOKEN \
  --label "org.opencontainers.image.version=${TAG}" \
  -t "wattswarm:${TAG}" \
  --load \
  .

mkdir -p "$OUTPUT_DIR"
docker save "wattswarm:${TAG}" -o "${OUTPUT_DIR}/wattswarm-${TAG}.tar"

echo "done: ${OUTPUT_DIR}/wattswarm-${TAG}.tar"
