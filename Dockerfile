FROM rust:1.93-bookworm AS chef

WORKDIR /app

RUN cargo install cargo-chef --locked

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY crates/protocol/Cargo.toml crates/protocol/Cargo.toml
COPY crates/crypto/Cargo.toml crates/crypto/Cargo.toml
COPY crates/policy-engine/Cargo.toml crates/policy-engine/Cargo.toml
COPY crates/runtime-client/Cargo.toml crates/runtime-client/Cargo.toml
COPY crates/storage-core/Cargo.toml crates/storage-core/Cargo.toml
COPY crates/node-core/Cargo.toml crates/node-core/Cargo.toml
COPY crates/control-plane/Cargo.toml crates/control-plane/Cargo.toml
COPY crates/run-queue/Cargo.toml crates/run-queue/Cargo.toml
COPY apps/wattswarm/Cargo.toml apps/wattswarm/Cargo.toml
COPY apps/wattswarm-runtime/Cargo.toml apps/wattswarm-runtime/Cargo.toml

RUN mkdir -p \
    crates/protocol/src \
    crates/crypto/src \
    crates/policy-engine/src \
    crates/runtime-client/src \
    crates/storage-core/src \
    crates/node-core/src \
    crates/control-plane/src \
    crates/run-queue/src \
    apps/wattswarm/src \
    apps/wattswarm-runtime/src \
    && printf "pub fn _planner_stub() {}\n" > crates/protocol/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/crypto/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/policy-engine/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/runtime-client/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/storage-core/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/node-core/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/control-plane/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > crates/run-queue/src/lib.rs \
    && printf "pub fn _planner_stub() {}\n" > apps/wattswarm/src/lib.rs \
    && printf "fn main() {}\n" > apps/wattswarm/src/main.rs \
    && printf "fn main() {}\n" > apps/wattswarm-runtime/src/main.rs

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS cacher

COPY --from=planner /app/recipe.json /app/recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --recipe-path recipe.json \
    -p wattswarm --bin wattswarm --bin wattswarm-runtime

FROM chef AS builder

COPY . .
COPY --from=cacher /app/target /app/target

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release -p wattswarm --bin wattswarm --bin wattswarm-runtime

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 10001 wattswarm

WORKDIR /app

COPY --from=builder /app/target/release/wattswarm /app/target/release/wattswarm
COPY --from=builder /app/target/release/wattswarm-runtime /app/target/release/wattswarm-runtime
COPY --from=builder /app/scripts/docker-kernel-entrypoint.sh /app/scripts/docker-kernel-entrypoint.sh

RUN mkdir -p /var/lib/wattswarm \
    && chmod +x /app/scripts/docker-kernel-entrypoint.sh \
    && chown -R wattswarm:wattswarm /var/lib/wattswarm /app

USER wattswarm

EXPOSE 7788
EXPOSE 8787

ENTRYPOINT ["/app/scripts/docker-kernel-entrypoint.sh"]
