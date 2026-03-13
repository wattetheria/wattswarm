# wattswarm

WattSwarm is an open-source swarm coordination kernel for agent networks.
This repository now contains a Rust-first v0.1 implementation of:

- P2P-style node sync primitives (gossip/backfill/anti-entropy/checkpoint)
- SEL append-only event log on PostgreSQL + replayable projections
- Node-local artifact storage layout for evidence/checkpoints/snapshots/event batches
- PostgreSQL-backed run queue for multi-agent orchestration (`runs`, `run_steps`, `run_events`)
- Run queue aggregation policy supports optional quorum (`aggregation.quorum`), tie resolver chain (`aggregation.tie_policy`), and independent null resolver chain (`aggregation.null_policy`)
- Default tie resolver is deterministic `STOCHASTIC` for tie-only paths; null paths use independent default `null_policy` chain
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
- Seed bundle guardrails with strict size caps (bundle/hit payload/reason summary/constraints) and automatic fallback to `EXPLORE` when oversized
- Explore early-stop controls (`explore.topk`, `explore.stop.no_new_evidence_rounds`)
- Reuse budget gating for DECIDE path (`reuse_max_attempts`, `reuse_verify_time_ms`, `reuse_verify_cost_units`)
- Reuse blacklist enforcement (`reuse_blacklist`) and `REUSE_REJECT_RECORDED` downgrade handling
- Automatic `REUSE_REJECT_RECORDED` emission during timeout reconciliation when reject quorum is reached for a reused candidate
- Decision memory persistence now includes `quorum_result_json` and `reason_details` snapshots for finalize/error/expiry auditing
- Knowledge lookup persistence now includes `exact_hit_count` and `similar_hit_count` per lookup row
- Runtime metrics now compute true p95 latency from samples and track `reuse_hit_rate_exact`, `reuse_hit_rate_similar`, `reuse_candidate_accept_rate`
- Advisory workflow state machine (`ADVISORY_CREATED` -> `ADVISORY_APPROVED` -> `ADVISORY_APPLIED`)
- `POLICY_TUNED` validation bound to approved advisory records
- Unknown reason-code observability export (unknown code + peer/local protocol versions)
- Continuous task mode support (`task_mode=CONTINUOUS`, `budget.mode=EPOCH_RENEW`, `EPOCH_ENDED`)
- Settlement feedback event (`TASK_FEEDBACK_REPORTED`) with authority signature validation
- Implicit settlement execution for no-BAD windows, applied to stability reputation with deterministic fixed-point delta
- Runtime HTTP integration (`/health`, `/capabilities`, `/execute`, `/verify`)
- CLI command surface required by v0.1
- Reference runtime binary for real end-to-end runs: `wattswarm-runtime`
- Built-in kernel UI console server: `wattswarm ui --listen 127.0.0.1:7788`

## Core Boundary

WattSwarm should be treated as a kernel-first project:

- Kernel/Core: `src/node/*`, `src/storage/*`, `src/policy.rs`, `src/control.rs`, `src/types.rs`
- Network layer: `crates/network-p2p`
- Artifact/object storage: `crates/artifact-store`
- Network-to-kernel bridge: `crates/control-plane/src/network_bridge.rs`
- UI/Console: `ui/*` (optional operational shell)

You can remove or ignore `ui/*` and still run the kernel fully via CLI/runtime APIs.

## Node Storage Split

WattSwarm now treats node-local storage as layered storage:

- PostgreSQL remains the node-local source of truth for:
  - SEL event log indexing
  - replayable projections
  - leases/votes/finality/task state
  - knowledge, reputation, runtime metrics, settlement
  - local run queue and dashboard queries
- Local filesystem artifacts are the intended home for:
  - evidence blobs and exported evidence bundles
  - checkpoint manifests and snapshot files
  - event batch archives for backfill/bootstrap

This split is local to one node. Nodes do not synchronize PostgreSQL databases with each other.
Inter-node sync remains protocol-driven: signed events, checkpoint metadata, summaries, and artifact references are exchanged over the network, then re-applied into each node's local store.

## Network And Org Context

WattSwarm now separates:

- `network`: the outer boundary (`local` or shared network)
- `org`: the swarm/emergence container inside that network
- `node`: the runtime host that stores local replicas and executes agents

Registry tables now live in PostgreSQL:

- `network_registry`
- `network_params`
- `node_registry`
- `node_network_membership`
- `org_registry`

Core swarm projection tables now carry `org_id`. The org then resolves back to its network through `org_registry.network_id`.

Local mode keeps backward-compatible behavior by auto-bootstrapping:

- `network_id = local:<node_id>`
- default org id = `local:<node_id>:bootstrap`

So local runs still behave like a single self-contained swarm, but the storage model is now ready for multiple org contexts without mixing decision memory, reputation, or settlement data.

Node identity reminder:

- `node_id` is derived from the node Ed25519 public key
- `node_registry.node_id` and `node_registry.public_key` are unique
- if the same `node_seed.hex` is copied into multiple nodes, registry bootstrap now raises a clear conflict error instead of silently accepting a duplicate identity

## Network Layer

The workspace now includes dedicated crates and a control-plane bridge for node-local storage plus live inter-node sync:

- `crates/network-p2p`
  - owns scope-aware topic naming (`global` / `region.*` / `local.*`)
  - defines network envelopes for events, rule broadcasts, checkpoint announcements, and summaries
  - defines typed backfill request/response messages
  - builds the minimal libp2p behaviour set used by WattSwarm networking:
    - gossipsub
    - identify
    - request-response (CBOR backfill)
    - mDNS
  - exposes a pollable network runtime for listen/dial/publish/backfill flows
- `crates/control-plane/src/network_bridge.rs`
  - bridges libp2p gossip events into `node-core::ingest_remote`
  - serves scope-aware backfill requests from the local event log
  - applies backfill responses into the local node projection pipeline
  - auto-requests backfill for every configured scope when a peer connection is established
  - runs periodic anti-entropy backfill on connected peers so missed messages and post-partition gaps are re-requested without requiring a reconnect
  - auto-publishes locally authored events into `global / region / local` topics based on task scope hints
  - auto-publishes knowledge summaries on finalized decisions and reputation summaries on verifier updates
  - propagates append-only revoke/penalty events so malicious event effects can be rolled back without deleting the event log
  - suppresses summary fanout while local publish backlog is high, keeping event catch-up ahead of summary traffic
  - can dial peer listen addresses discovered through LAN state (`discovered_peers.json`)
- `crates/artifact-store`
  - owns the node-local filesystem layout for evidence, checkpoints, snapshots, and event batches
  - provides read/write helpers for byte and JSON payloads

Current coverage in tests:

- unit tests for artifact storage paths/IO
- unit tests for network topic/backfill/runtime primitives
- unit tests for bridge ingest and backfill helpers
- integration tests for two-node global gossip sync and request-response backfill sync
- integration tests for region/local scoped sync and summary import
- integration tests for event revoke rollback and summary revoke cleanup across nodes
- integration tests for periodic anti-entropy catch-up and reconnect recovery after a partition-like disconnect

Runtime toggles:

- `WATTSWARM_P2P_ENABLED=true` by default
- set `WATTSWARM_P2P_ENABLED=false` to run WattSwarm in local-only mode
- `WATTSWARM_P2P_MDNS=true` by default
- `WATTSWARM_P2P_PORT=4001` by default
- `WATTSWARM_P2P_LISTEN_ADDRS` can override the default listen multiaddr list
- `WATTSWARM_P2P_REGION_IDS=sol-1,sol-2` subscribes the node to those region scopes
- `WATTSWARM_P2P_LOCAL_IDS=lab-a` subscribes the node to matching local scopes

Task scope hints:

- default task scope is `global`
- set `inputs.swarm_scope = "region:<id>"` or `inputs.swarm_scope = "local:<id>"` to route a task into region/local sync
- object form is also accepted: `{"kind":"region","id":"sol-1"}`
- `task_type` prefixes `region:<id>:` and `local:<id>:` are supported as a fallback

Malicious data rollback model:

- event log stays append-only; bad data is invalidated with `EventRevoked`, `SummaryRevoked`, and `NodePenalized`
- projection rebuild skips revoked events, so rollback does not require deleting history
- imported knowledge/reputation summaries are stored separately from local projections and can be revoked or quarantined by source node

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

Note: storage is PostgreSQL-only. Default schema is `public`; override with `WATTSWARM_PG_SCHEMA=<schema>`. `--store` is a logical local store identifier and does not change PostgreSQL routing.

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
- Aggregation policy behavior:
  - optional `aggregation.quorum` threshold
  - tie path and null path are handled independently
  - tie path uses `aggregation.tie_policy.chain` (`REEXPLORE`, `CONFIDENCE_WEIGHTED`, `REPUTATION_WEIGHTED`, `STOCHASTIC`)
  - null path uses `aggregation.null_policy.chain` (`REEXPLORE`, `FINALIZE_NULL`)
  - tie `REEXPLORE`: emits `RUN_TIE_REEXPLORE_TRIGGERED`, injects `_aggregation_signal.type=NEED_MORE_EVIDENCE`, and requeues terminal steps
  - null `REEXPLORE`: emits `RUN_NULL_REEXPLORE_TRIGGERED`, injects `_aggregation_signal` (`NEED_DECISION` / `NEED_STEP_RECOVERY` / `NEED_MORE_EVIDENCE`) and requeues terminal steps
  - `CONFIDENCE_WEIGHTED`: sums per-value confidence and chooses unique max
  - `REPUTATION_WEIGHTED`: sums per-value `agent_reputation_units` and chooses unique max
  - `STOCHASTIC`: deterministic pseudo-random tie break from hashed seed
  - default tie behavior uses `tie_policy.enabled_on=["TIE"]` and `tie_policy.chain=["STOCHASTIC"]`
  - default null behavior uses `null_policy.enabled_on=["EMPTY","QUORUM_NULL"]` and `null_policy.chain=["REEXPLORE","FINALIZE_NULL"]`
  - result payload now includes `aggregation.resolution_paths` and `aggregation.null_resolution` for monitoring
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

## Docker Quick Start (PostgreSQL + Kernel + Runtime + Worker, Hot Reload Enabled)

Start PostgreSQL, kernel, runtime, and worker together:

```bash
docker compose up -d --build
```

Default host ports:

- PostgreSQL: `55432` (container `5432`)
- Kernel UI: `7788`
- Kernel P2P TCP: `4001`
- Runtime HTTP: `8787`
- UDP announce: `37931/udp`

If you need custom host ports:

```bash
WATTSWARM_PG_HOST_PORT=56432 WATTSWARM_UI_PORT=8788 WATTSWARM_P2P_HOST_PORT=5001 WATTSWARM_RUNTIME_PORT=9787 docker compose up -d --build
```

Kernel container startup behavior:

- waits for PostgreSQL health check
- runs `run init` automatically (queue schema bootstrap)
- auto-registers executor `rt -> http://runtime:8787`
- starts UI server on `0.0.0.0:7788`
- starts the background libp2p network bridge by default

Worker container startup behavior:

- starts a long-running queue worker loop (`run worker`)
- continuously claims `run_steps` in `QUEUED/RETRY_WAIT` and executes them
- updates step/run status until terminal state (`FINALIZED/FAILED/CANCELLED`)

Hot reload behavior in current compose:

- `kernel`, `runtime`, `worker` use `cargo watch` in containers.
- local source is bind-mounted (`.:/app`), so Rust code changes trigger auto rebuild + process restart.
- no manual `docker compose restart` is needed for normal source edits.

Worker tuning env vars:

- `WATTSWARM_WORKER_CONCURRENCY` (default `16`)
- `WATTSWARM_WORKER_POLL_MS` (default `250`)
- `WATTSWARM_WORKER_LEASE_MS` (default `30000`)

P2P env vars:

- `WATTSWARM_P2P_ENABLED=true` by default
- `WATTSWARM_P2P_MDNS=true` by default
- `WATTSWARM_P2P_PORT=4001`
- `WATTSWARM_P2P_LISTEN_ADDRS` optional comma-separated multiaddr override
- `WATTSWARM_P2P_REGION_IDS` optional comma-separated region scope subscription list
- `WATTSWARM_P2P_LOCAL_IDS` optional comma-separated local scope subscription list

Optional UDP announce switch (default off):

- `WATTSWARM_UDP_ANNOUNCE_ENABLED=true` to enable UDP announce + listener
- `WATTSWARM_UDP_ANNOUNCE_MODE=multicast|broadcast` (default `multicast`)
- `WATTSWARM_UDP_ANNOUNCE_ADDR=239.255.42.99` (multicast default) or `255.255.255.255` for broadcast
- `WATTSWARM_UDP_ANNOUNCE_PORT=37931`
- with switch enabled, startup emits announce payload and UI process listens on the same port and records discovered peer IDs into `--state-dir/discovered_peers.json`
- when a peer advertise includes a libp2p listen address, that address is also recorded and used by the background network bridge for automatic LAN dialing
- `peers list` and `/api/peers/list` include the discovered peers loaded from that file

Examples:

```bash
# multicast announce
WATTSWARM_UDP_ANNOUNCE_ENABLED=true docker compose up -d --build

# broadcast announce
WATTSWARM_UDP_ANNOUNCE_ENABLED=true WATTSWARM_UDP_ANNOUNCE_MODE=broadcast WATTSWARM_UDP_ANNOUNCE_ADDR=255.255.255.255 docker compose up -d --build

# local-only mode
WATTSWARM_P2P_ENABLED=false docker compose up -d --build
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
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state node up
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors add rt http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task submit ./task.json
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task run-real --executor rt --profile default --task-id task-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task decision task-1
```

`task.json` is the boundary object between your independent project and the kernel.

## Multi-Executor Configuration

WattSwarm can register multiple runtime executors at the same time. Each executor is identified by
`name -> base_url` in executor registry (`executors.json` under `--state-dir`).

Example:

```bash
# register multiple runtimes
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors add rt-local http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors add rt-openclaw https://openclaw.example.com
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors add rt-cloud https://api.example.com/runtime

# inspect and health-check
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors list
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors check rt-openclaw
```

Runtime selection is explicit per task run:

```bash
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task run-real --executor rt-local --profile default --task-id task-local-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task run-real --executor rt-openclaw --profile default --task-id task-openclaw-1
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
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state ui --listen 127.0.0.1:7788
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
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state node up
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state executors add rt http://127.0.0.1:8787
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task run-real --executor rt --profile default --task-id task-real-1
cargo run --bin wattswarm -- --state-dir ./.ws-dev --store wattswarm.state task decision task-real-1
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
