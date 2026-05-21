#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DOCKERFILE="$ROOT_DIR/Dockerfile"
COMPOSE_FILE="$ROOT_DIR/docker-compose.yml"
ENTRYPOINT="$ROOT_DIR/scripts/docker-kernel-entrypoint.sh"

require_file() {
  local path="$1"
  if [ ! -f "$path" ]; then
    echo "missing required release file: $path" >&2
    exit 1
  fi
}

require_text() {
  local path="$1"
  local pattern="$2"
  local description="$3"
  if ! grep -Fq -- "$pattern" "$path"; then
    echo "release image check failed: $description" >&2
    echo "expected to find: $pattern" >&2
    exit 1
  fi
}

require_file "$DOCKERFILE"
require_file "$COMPOSE_FILE"
require_file "$ENTRYPOINT"

require_text "$DOCKERFILE" "cargo chef cook --release" "Docker builds must keep dependency caching"
require_text "$DOCKERFILE" "--mount=type=secret,id=github_token" "Docker build must consume the GitHub token secret for private git dependencies"
require_text "$DOCKERFILE" "CARGO_NET_GIT_FETCH_WITH_CLI=true" "Docker build must route Cargo git fetches through git CLI auth"
require_text "$DOCKERFILE" "cargo build --release -p wattswarm --bin wattswarm --bin wattswarm-runtime" "Docker build must produce both release binaries"
require_text "$DOCKERFILE" "/app/target/release/wattswarm" "final image must include the wattswarm CLI/kernel binary"
require_text "$DOCKERFILE" "/app/target/release/wattswarm-runtime" "final image must include the runtime binary"
require_text "$DOCKERFILE" "/app/scripts/docker-kernel-entrypoint.sh" "final image must include the kernel entrypoint"
require_text "$DOCKERFILE" "ca-certificates curl" "final image must include curl for runtime healthchecks"

require_text "$ENTRYPOINT" 'exec /app/target/release/wattswarm' "kernel entrypoint must launch the release wattswarm binary"
require_text "$ENTRYPOINT" '--state-dir "${STATE_DIR}"' "kernel entrypoint must pass the state directory"
require_text "$ENTRYPOINT" '--store "${STORE_NAME}"' "kernel entrypoint must pass the configured store"

require_text "$COMPOSE_FILE" "wattswarm_state_data:/var/lib/wattswarm" "compose must persist Wattswarm state"
require_text "$COMPOSE_FILE" "WATTSWARM_STATE_DIR: /var/lib/wattswarm" "compose must point services at persisted state"
require_text "$COMPOSE_FILE" "WATTSWARM_STORE_NAME: wattswarm.state" "compose must keep the expected store name"
require_text "$COMPOSE_FILE" "github_token:" "compose must declare the GitHub token build secret"
require_text "$COMPOSE_FILE" "environment: GITHUB_TOKEN" "compose must source the build secret from GITHUB_TOKEN"
require_text "$COMPOSE_FILE" 'entrypoint: ["/app/target/release/wattswarm-runtime"]' "runtime service must override the shared image entrypoint"
require_text "$COMPOSE_FILE" 'entrypoint: ["/app/target/release/wattswarm"]' "worker service must override the shared image entrypoint"

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  GITHUB_TOKEN="${GITHUB_TOKEN:-release-image-check-placeholder}" docker compose -f "$COMPOSE_FILE" config >/dev/null
fi

echo "release image artifacts verified"
