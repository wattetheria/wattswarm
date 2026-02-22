#!/bin/sh
set -eu

cd /app

cargo run --bin wattswarm -- \
  --state-dir "${WATTSWARM_STATE_DIR}" \
  --store "${WATTSWARM_STORE_NAME}" \
  run --pg-url "${WATTSWARM_PG_URL}" \
  init

cargo run --bin wattswarm -- \
  --state-dir "${WATTSWARM_STATE_DIR}" \
  --store "${WATTSWARM_STORE_NAME}" \
  executors add "${WATTSWARM_BOOTSTRAP_EXECUTOR_NAME}" "${WATTSWARM_BOOTSTRAP_EXECUTOR_URL}"

exec cargo watch \
  --poll \
  --watch Cargo.toml \
  --watch Cargo.lock \
  --watch apps \
  --watch crates \
  --watch ui \
  --watch scripts \
  --ignore target \
  --shell "cargo run --bin wattswarm -- --state-dir \"${WATTSWARM_STATE_DIR}\" --store \"${WATTSWARM_STORE_NAME}\" ui --listen \"${WATTSWARM_UI_LISTEN}\""
