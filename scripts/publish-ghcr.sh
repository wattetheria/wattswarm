#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -z "${RELEASE:-}" ]; then
  echo "RELEASE is required, for example: RELEASE=1.0.7 scripts/publish-ghcr.sh" >&2
  exit 1
fi

scripts/verify-release-images.sh

docker buildx create --use --name watt-builder 2>/dev/null || docker buildx use watt-builder

secret_args=()
if [ -n "${GITHUB_TOKEN:-}" ]; then
  secret_args+=(--secret id=github_token,env=GITHUB_TOKEN)
fi

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  "${secret_args[@]}" \
  --label "org.opencontainers.image.version=${RELEASE}" \
  -t "ghcr.io/wattetheria/wattswarm-kernel:${RELEASE}" \
  -t "ghcr.io/wattetheria/wattswarm-runtime:${RELEASE}" \
  -t "ghcr.io/wattetheria/wattswarm-worker:${RELEASE}" \
  --push \
  .
