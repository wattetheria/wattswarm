# wattswarm

WattSwarm is an open-source swarm coordination kernel for agent networks.
This repository now contains a Rust-first v0.1 implementation of:

- P2P-style node sync primitives (gossip/backfill/anti-entropy/checkpoint)
- SEL append-only event log on PostgreSQL + replayable projections
- PostgreSQL-backed run queue for multi-agent orchestration (`runs`, `run_steps`, `run_events`)
- Claim/Lease/Renew scheduling and execution-id idempotency checks
- Lease validity with clock-skew tolerance window (`CLOCK_SKEW_TOLERANCE_MS`)
- Task lifecycle events (create/claim/execute/verify/vote/commit/finalize/error/retry/expire)
- Protocol-versioned event envelope (`protocol_version`) and node status version visibility
- Commit-reveal voting with reveal deadline enforcement
- Candidate-hash scoped vote accounting (commit/reveal carry `candidate_hash`)
- Stage-budget hard gating (`explore/verify/finalize` cost buckets) and per-task cost report projection
- Structured payload caps (`MAX_EVENT_PAYLOAD_BYTES`, `MAX_STRUCTURED_SUMMARY_BYTES`)
- Evidence pipeline events: `EVIDENCE_ADDED` and `EVIDENCE_AVAILABLE`, with DA quorum checks before finalize
- Policy engine with built-in policies:
  - `vp.schema_only.v1`
  - `vp.schema_thresholds.v1`
  - `vp.crosscheck.v1`
- Three-state verification status (`passed` / `failed` / `inconclusive`) for external evidence reachability
- Protocol reason codes as `u16` with UNKNOWN/CUSTOM fallback
- Vote-round timeout reconciliation: insufficient reveals trigger `TASK_RETRY_SCHEDULED` before `expiry_ms`
- Membership/role-based validity gates with signature checks
- Finality proof verification and anti-fork finalize rule
- Knowledge Store tables on PostgreSQL: decision/evidence/metrics/settlement/reputation/lookups
- Active knowledge lookup with EXACT/SIMILAR hit typing and `seed_bundle` injection to `/execute`
- Explore early-stop controls (`explore.topk`, `explore.stop.no_new_evidence_rounds`)
- Reuse blacklist enforcement (`reuse_blacklist`) and `REUSE_REJECT_RECORDED` downgrade handling
- Advisory workflow state machine (`ADVISORY_CREATED` -> `ADVISORY_APPROVED` -> `ADVISORY_APPLIED`)
- `POLICY_TUNED` validation bound to approved advisory records
- Unknown reason-code observability export (unknown code + peer/local protocol versions)
- Continuous task mode support (`task_mode=CONTINUOUS`, `budget.mode=EPOCH_RENEW`, `EPOCH_ENDED`)
- Settlement feedback event (`TASK_FEEDBACK_REPORTED`) with authority signature validation
- Runtime HTTP integration (`/health`, `/capabilities`, `/execute`, `/verify`)
- CLI command surface required by v0.1
- Reference runtime binary for real end-to-end runs: `wattswarm-runtime`
- Built-in kernel UI console server: `wattswarm ui --listen 127.0.0.1:7788`

## Core Boundary

WattSwarm should be treated as a kernel-first project:

- Kernel/Core: `src/node/*`, `src/storage/*`, `src/policy.rs`, `src/control.rs`, `src/types.rs`
- UI/Console: `ui/*` (optional operational shell)

You can remove or ignore `ui/*` and still run the kernel fully via CLI/runtime APIs.

## CLI

- `wattswarm node up|down|status`
- `wattswarm peers list`
- `wattswarm log head|replay|verify`
- `wattswarm executors add|list|check`
- `wattswarm task submit|watch|decision`
- `wattswarm run init|submit|kickoff|watch|result|events|cancel|retry|worker`
- `wattswarm knowledge export --task_type <...> --out <file>`
- `wattswarm knowledge export --task_id <...> --out <file>`
- `wattswarm ui --listen 127.0.0.1:7788`

Note: storage is PostgreSQL-only on the `public` schema. `--db` is kept for CLI backward compatibility and no longer changes schema/database routing.

UI (`/`) exposes API-backed controls for all CLI operation groups:
- Node: up/down/status
- Peers: list
- Log: head/replay/verify
- Executors: add/list/check
- Tasks: sample/submit/watch/decision/run-real
- Knowledge: export by `task_id` or `task_type`

UI now includes built-in guidance:
- `Quick Start` panel with ordered operation steps
- `TaskContract Minimal Reference` snippet
- task preset helper (`swarm/arbitrage/security`) on the Task panel

## PostgreSQL Run Queue (Multi-Agent Orchestration)

Run queue uses PostgreSQL as shared scheduler storage and supports:

- Single kickoff trigger (`run kickoff`) for one run
- Kernel-internal progression by worker loop (`run worker`) over `run_steps`
- `run kickoff` only moves run/steps to `QUEUED`; actual execution starts when worker claims steps
- Step leasing with `FOR UPDATE SKIP LOCKED` and lease metadata (`lease_id/lease_until`)
- Retry with exponential backoff (`RETRY_WAIT -> QUEUED`)
- Run-level aggregation output in `runs.result_json` (`final_decision`/`final_answer` + per-step conclusions)
- Human-readable DB time columns (`created_at/updated_at/started_at/finished_at`) stored as `TIMESTAMPTZ`
- Run-level control plane APIs via CLI:
  - create (`run submit`)
  - kickoff (`run kickoff`)
  - observe (`run watch`, `run events`, `run result`)
  - cancel/retry (`run cancel`, `run retry`)

Queue implementation is DB-native on PostgreSQL tables and does not require extra queue middleware (RabbitMQ/Kafka/Redis Streams) for current scope.

### `shared_inputs` Semantics

`shared_inputs` is the run-level business context shared by all agents in one run.

- Typical content: user requirement text, business parameters, URLs, file/folder references, IDs, flags.
- Recommended shape: JSON object.
- Merge behavior per agent step:
  - If `shared_inputs` is an object, its key-value pairs are copied into `TaskContract.inputs`.
  - System then injects `prompt` and `agent_id` from that agent spec into `TaskContract.inputs`.
  - If `shared_inputs` is not an object (string/array/number), it is wrapped as `inputs.shared_inputs`.
- Collision rule: if `shared_inputs` already contains `prompt` or `agent_id`, agent-level values overwrite them.
- Runtime behavior: kernel forwards `inputs` as-is to runtime `/execute`. URL/path values are references only; kernel does not auto-fetch remote pages or read local files.
- Size guidance: keep `shared_inputs` concise and prefer references over large blobs. Event/payload size limits are enforced by kernel validation.

## Docker Quick Start (PostgreSQL + Kernel + Runtime + Worker)

Start PostgreSQL, kernel, runtime, and worker together:

```bash
docker compose up -d --build
```

Default host ports:

- PostgreSQL: `55432` (container `5432`)
- Kernel UI: `7788`
- Runtime HTTP: `8787`

If you need custom host ports:

```bash
WATTSWARM_PG_HOST_PORT=56432 WATTSWARM_UI_PORT=8788 WATTSWARM_RUNTIME_PORT=9787 docker compose up -d --build
```

Kernel container startup behavior:

- waits for PostgreSQL health check
- runs `run init` automatically (queue schema bootstrap)
- auto-registers executor `rt -> http://runtime:8787`
- starts UI server on `0.0.0.0:7788`

Worker container startup behavior:

- starts a long-running queue worker loop (`run worker`)
- continuously claims `run_steps` in `QUEUED/RETRY_WAIT` and executes them
- updates step/run status until terminal state (`FINALIZED/FAILED/CANCELLED`)

Worker tuning env vars:

- `WATTSWARM_WORKER_CONCURRENCY` (default `16`)
- `WATTSWARM_WORKER_POLL_MS` (default `250`)
- `WATTSWARM_WORKER_LEASE_MS` (default `30000`)

Optional UDP announce switch (default off):

- `WATTSWARM_UDP_ANNOUNCE_ENABLED=true` to enable UDP announce + listener
- `WATTSWARM_UDP_ANNOUNCE_MODE=multicast|broadcast` (default `multicast`)
- `WATTSWARM_UDP_ANNOUNCE_ADDR=239.255.42.99` (multicast default) or `255.255.255.255` for broadcast
- `WATTSWARM_UDP_ANNOUNCE_PORT=37931`
- with switch enabled, startup emits announce payload and UI process listens on the same port and records discovered peer IDs into `--state-dir/discovered_peers.json`
- `peers list` and `/api/peers/list` include the discovered peers loaded from that file

Examples:

```bash
# multicast announce
WATTSWARM_UDP_ANNOUNCE_ENABLED=true docker compose up -d --build

# broadcast announce
WATTSWARM_UDP_ANNOUNCE_ENABLED=true WATTSWARM_UDP_ANNOUNCE_MODE=broadcast WATTSWARM_UDP_ANNOUNCE_ADDR=255.255.255.255 docker compose up -d --build
```

UI entrypoints:

- `http://127.0.0.1:7788/` for kernel console
- `http://127.0.0.1:7788/swarm` for swarm dashboard

Create + kickoff + execute (compose with built-in worker):

```bash
cargo run --bin wattswarm -- run submit ./run-spec.json --kickoff --pg-url postgres://postgres:postgres@127.0.0.1:55432/wattswarm
cargo run --bin wattswarm -- run watch resume-001 --pg-url postgres://postgres:postgres@127.0.0.1:55432/wattswarm
```

If you do not use compose, you must run at least one worker process yourself:

```bash
cargo run --bin wattswarm -- run worker --concurrency 16 --pg-url postgres://postgres:postgres@127.0.0.1:55432/wattswarm
```

Important: writing rows directly into `runs` table is not enough for execution. The queue requires valid `run_steps` and an active worker process.

## Independent Project Integration (No UI Required)

If you are building a separate product (for example, resume blind review), do not embed business logic into kernel internals. Build an external app and call WattSwarm through CLI/runtime contract.

Minimal integration path:

1. Start runtime service implementing `/health`, `/capabilities`, `/execute`, `/verify`.
2. Register runtime executor in WattSwarm.
3. Submit task contract JSON from your independent project.
4. Run real flow and fetch decision.

Example:

```bash
# terminal A: runtime
cargo run --bin wattswarm-runtime -- --listen 127.0.0.1:8787

# terminal B: kernel orchestration
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db node up
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors add rt http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task submit ./task.json
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task run-real --executor rt --profile default --task-id task-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task decision task-1
```

`task.json` is the boundary object between your independent project and the kernel.

## Multi-Executor Configuration

WattSwarm can register multiple runtime executors at the same time. Each executor is identified by
`name -> base_url` in executor registry (`executors.json` under `--state-dir`).

Example:

```bash
# register multiple runtimes
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors add rt-local http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors add rt-openclaw https://openclaw.example.com
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors add rt-cloud https://api.example.com/runtime

# inspect and health-check
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors list
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors check rt-openclaw
```

Runtime selection is explicit per task run:

```bash
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task run-real --executor rt-local --profile default --task-id task-local-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task run-real --executor rt-openclaw --profile default --task-id task-openclaw-1
```

Notes:
- Kernel interface is unified: every executor must expose `/health`, `/capabilities`, `/execute`, `/verify`.
- One executor can be a single model endpoint or a gateway that fans out to many internal agents.

## Supported Topologies

WattSwarm supports these deployment topologies for executor integration:

1. Single-machine multi-process
   - Kernel process + one or more runtime processes on one host.
   - Common setup for local development and staging.

2. Multi-machine multi-node
   - Kernel nodes and runtime executors distributed across multiple hosts.
   - Executors are registered by remote `base_url`; task runs route by executor name.
   - Node sync primitives (gossip/backfill/anti-entropy/checkpoint) are available for multi-node operation.

## Swarm Dashboard (Real Swarm)

A swarm dashboard frontend is included at:

- `ui/index.html` (served by `wattswarm ui` on route `/swarm`)

The swarm dashboard is not a random toy loop: it drives real WattSwarm task lifecycle via:

- `GET /api/swarm/state`
- `POST /api/swarm/tick`

`/api/swarm/tick` runs real kernel transitions (`claim -> execute -> verify -> vote -> commit -> finalize`) against configured runtime executors, so the map/consensus/log panels reflect actual SEL/projection state.

## Build & Test

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Test Reports

Recommended tools:

- `cargo-nextest` for fast execution and JUnit export
- `cargo-llvm-cov` for coverage (`lcov` + HTML)

Local one-command report generation:

```bash
./scripts/test-report.sh
```

Or via Make targets:

```bash
make test
make test-report
```

Report outputs:

- `target/test-report/junit.xml`
- `target/test-report/lcov.info`
- `target/test-report/html/index.html`
- `target/test-report/coverage-summary.txt`

CI workflow:

- `.github/workflows/test-report.yml` runs tests on PostgreSQL service
- uploads report artifacts (`junit.xml`, `lcov.info`, HTML, summary)

Launch UI console:

```bash
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db ui --listen 127.0.0.1:7788
```

Open:

- `http://127.0.0.1:7788/` for kernel console
- `http://127.0.0.1:7788/swarm` for real swarm dashboard

Recommended UI operation order (`/`):

1. `Node -> UP`
2. `Executors -> ADD` (name `rt`, base_url `http://127.0.0.1:8787`)
3. `Executors -> CHECK`
4. `Task -> SAMPLE` (optionally `apply preset`)
5. `Task -> SUBMIT`
6. `Task -> RUN-REAL`
7. `Task -> WATCH` / `DECISION`

## Real Runtime Flow

Run a real HTTP runtime process:

```bash
cargo run --bin wattswarm-runtime -- --listen 127.0.0.1:8787
```

In another terminal, run a real end-to-end task flow:

```bash
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db node up
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db executors add rt http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task run-real --executor rt --profile default --task-id task-real-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --db wattswarm.db task decision task-real-1
```

`task run-real` executes the full chain:
`TASK_CREATED -> TASK_CLAIMED(propose) -> CANDIDATE_PROPOSED -> TASK_CLAIMED(verify) -> EVIDENCE_AVAILABLE -> VERIFIER_RESULT_SUBMITTED -> VOTE_COMMIT -> VOTE_REVEAL -> DECISION_COMMITTED -> DECISION_FINALIZED`

## Test Coverage

- Unit tests: policy evaluation/registry, crypto/signature flow, reason code mapping,
  task template shape, run queue helper logic, SQL translation helper logic
- Integration tests: end-to-end submit→finalize loop, sync/replay/checkpoint, lease conflict/renew,
  evidence limits, commit-reveal, expiry terminal behavior, finality fork prevention,
  provider source auditing, membership quorum updates, advisory flow, knowledge-seed/reuse,
  unknown reason observability, continuous epoch advance, stage budget exhaustion, DA quorum failure,
  and task cost report export
- CLI smoke tests: node/log/executors/task flows
