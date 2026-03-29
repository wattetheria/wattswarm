#!/bin/sh
set -eu

STATE_DIR="${WATTSWARM_STATE_DIR:-/var/lib/wattswarm}"
STORE_NAME="${WATTSWARM_STORE_NAME:-wattswarm.state}"
PG_URL="${WATTSWARM_PG_URL:-postgres://postgres:postgres@postgres:5432/wattswarm}"
UI_LISTEN="${WATTSWARM_UI_LISTEN:-0.0.0.0:7788}"
BOOTSTRAP_EXECUTOR_NAME="${WATTSWARM_BOOTSTRAP_EXECUTOR_NAME:-rt}"
BOOTSTRAP_EXECUTOR_URL="${WATTSWARM_BOOTSTRAP_EXECUTOR_URL:-http://runtime:8787}"

/app/target/release/wattswarm --state-dir "${STATE_DIR}" --store "${STORE_NAME}" run --pg-url "${PG_URL}" init
if [ -n "${BOOTSTRAP_EXECUTOR_NAME}" ] && [ -n "${BOOTSTRAP_EXECUTOR_URL}" ]; then
  /app/target/release/wattswarm --state-dir "${STATE_DIR}" --store "${STORE_NAME}" executors add "${BOOTSTRAP_EXECUTOR_NAME}" "${BOOTSTRAP_EXECUTOR_URL}"
fi
exec /app/target/release/wattswarm --state-dir "${STATE_DIR}" --store "${STORE_NAME}" ui --listen "${UI_LISTEN}"
