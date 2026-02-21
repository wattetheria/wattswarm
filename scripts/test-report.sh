#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-target/test-report}"
mkdir -p "$OUT_DIR"

if ! cargo nextest --version >/dev/null 2>&1; then
  echo "Installing cargo-nextest..."
  cargo install cargo-nextest --locked
fi

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  echo "Installing cargo-llvm-cov..."
  cargo install cargo-llvm-cov --locked
fi

echo "Running tests with JUnit report..."
cargo nextest run --workspace --junit-file "$OUT_DIR/junit.xml"

echo "Generating LCOV + HTML coverage report..."
cargo llvm-cov --workspace --lcov --output-path "$OUT_DIR/lcov.info"
cargo llvm-cov --workspace --html --output-dir "$OUT_DIR/html"

echo "Generating coverage summary..."
cargo llvm-cov --workspace --summary-only > "$OUT_DIR/coverage-summary.txt"

echo "Reports generated:"
echo "- $OUT_DIR/junit.xml"
echo "- $OUT_DIR/lcov.info"
echo "- $OUT_DIR/html/index.html"
echo "- $OUT_DIR/coverage-summary.txt"
