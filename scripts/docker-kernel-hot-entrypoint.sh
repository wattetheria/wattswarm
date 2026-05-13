#!/bin/sh
set -eu

cd /app

env -u RUST_RECURSION_COUNT cargo build --bin wattswarm

env -u RUST_RECURSION_COUNT target/debug/wattswarm \
  --state-dir "${WATTSWARM_STATE_DIR}" \
  --store "${WATTSWARM_STORE_NAME}" \
  run --pg-url "${WATTSWARM_PG_URL}" \
  init

env -u RUST_RECURSION_COUNT target/debug/wattswarm \
  --state-dir "${WATTSWARM_STATE_DIR}" \
  --store "${WATTSWARM_STORE_NAME}" \
  executors add "${WATTSWARM_BOOTSTRAP_EXECUTOR_NAME}" "${WATTSWARM_BOOTSTRAP_EXECUTOR_URL}"

exec env -u RUST_RECURSION_COUNT cargo watch \
  --poll \
  --watch Cargo.toml \
  --watch Cargo.lock \
  --watch apps \
  --watch crates \
  --watch ui \
  --watch scripts \
  --ignore target \
  --shell "env -u RUST_RECURSION_COUNT cargo build --bin wattswarm && env -u RUST_RECURSION_COUNT target/debug/wattswarm --state-dir \"${WATTSWARM_STATE_DIR}\" --store \"${WATTSWARM_STORE_NAME}\" ui --listen \"${WATTSWARM_UI_LISTEN}\""
