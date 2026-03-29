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
  - owns scope-aware topic naming (`global` / `region.*` / `node.*`)
  - defines network envelopes for events, rule broadcasts, checkpoint announcements, and summaries
  - defines typed backfill request/response messages
  - builds the minimal libp2p behaviour set used by WattSwarm networking:
    - gossipsub
    - identify
    - Kademlia
    - relay client
    - AutoNAT
    - DCUtR
    - request-response (CBOR backfill)
    - mDNS
  - exposes a pollable network runtime for listen/dial/publish/backfill flows
- `crates/control-plane/src/network_bridge.rs`
  - bridges libp2p gossip events into `node-core::ingest_remote`
  - serves scope-aware backfill requests from the local event log
  - applies backfill responses into the local node projection pipeline
  - auto-requests backfill for every configured scope when a peer connection is established
  - runs periodic anti-entropy backfill on connected peers so missed messages and post-partition gaps are re-requested without requiring a reconnect
  - auto-publishes locally authored events into `global / region / node` topics based on task scope hints
  - auto-publishes knowledge summaries on finalized decisions and reputation summaries on verifier updates
  - propagates append-only revoke/penalty events so malicious event effects can be rolled back without deleting the event log
  - suppresses summary fanout while local publish backlog is high, keeping event catch-up ahead of summary traffic
  - can dial peer listen addresses discovered through the local PostgreSQL peer cache (`discovered_peers_local`)
- `crates/artifact-store`
  - owns the node-local filesystem layout for evidence, checkpoints, snapshots, and event batches
  - provides read/write helpers for byte and JSON payloads

Current coverage in tests:

- unit tests for artifact storage paths/IO
- unit tests for network topic/backfill/runtime primitives
- unit tests for bridge ingest and backfill helpers
- integration tests for two-node global gossip sync and request-response backfill sync
- integration tests for region/node scoped sync and summary import
- integration tests for event revoke rollback and summary revoke cleanup across nodes
- integration tests for periodic anti-entropy catch-up and reconnect recovery after a partition-like disconnect

Runtime toggles:

- `WATTSWARM_P2P_ENABLED=true` by default
- set `WATTSWARM_P2P_ENABLED=false` to run WattSwarm in local-only mode
- `WATTSWARM_P2P_MDNS=true` by default
- `WATTSWARM_P2P_PORT=4001` by default
- `WATTSWARM_P2P_LISTEN_ADDRS` can override the default listen multiaddr list
- `WATTSWARM_P2P_BOOTSTRAP_PEERS=/ip4/203.0.113.10/tcp/4001/p2p/<peer-id>` seeds WAN bootstrap dialing and Kademlia discovery
- `WATTSWARM_P2P_REGION_IDS=sol-1,sol-2` subscribes the node to those region scopes
- `WATTSWARM_P2P_NODE_IDS=lab-a` subscribes the node to matching node scopes
- `WATTSWARM_P2P_LOCAL_IDS=lab-a` is still accepted as a legacy alias for node scopes

Network-mode bootstrap behavior:

- `WATTSWARM_NODE_MODE=network` tells the node to join an existing shared network instead of creating `local:` or `lan:` topology.
- a joining node must know at least one bootstrap peer multiaddr; in product UI this is saved as `bootstrap_peers` inside `startup_config.json`
- if local PostgreSQL is missing `network_registry / org_registry / network_params`, the node now attempts an automatic bootstrap sync:
  - derives one or more bootstrap HTTP endpoints from configured peers (or explicit `WATTSWARM_NETWORK_BOOTSTRAP_HTTP_URLS`)
  - fetches the remote signed `NetworkBootstrapBundle`
  - verifies `network_id`, `genesis_node_id`, `params_hash`, and signature
  - imports the bundle into the local PostgreSQL store before opening the node
- normal joining nodes do not re-sign network params locally; they import the genesis-signed bundle and verify it

### Current Network Propagation Architecture

WattSwarm's current decentralized networking model is:

- topic-based publish/subscribe
- gossip-style peer-to-peer dissemination
- bootstrap-peer and Kademlia-assisted peer discovery
- relay-assisted WAN reachability with DCUtR upgrade attempts
- AutoNAT reachability probing against known peers
- inbound dedupe and per-peer/topic traffic guards
- topic message dissemination for scoped agent chat traffic
- persisted topic history with cursor/backfill recovery
- request/response backfill for missed data
- eventual consistency by replaying events into each node's local store

This means nodes do not replicate PostgreSQL state directly. Instead, they exchange selected network messages, then each node re-applies those messages into its own local event log and projections.

```mermaid
flowchart TD
    A["Node joins network"] --> B["Peer discovery<br/>mDNS / UDP announce / bootstrap peers / Kademlia"]
    B --> C["P2P connections established<br/>direct, or via relay when needed"]
    C --> D["Node subscribes to configured scopes"]
    D --> E["Local topic set is derived<br/>namespace + scope + kind"]
    C --> C1["AutoNAT probes reachability<br/>DCUtR attempts direct upgrade on relayed links"]

    F["Local task/event/summary is produced"] --> G["Network bridge resolves scope"]
    G --> H["Publish to topic over gossip"]
    H --> I["Connected peers that subscribed to that topic receive it"]
    I --> J["Event: ingest into local event log"]
    I --> K["Summary: import supported summary payloads"]
    J --> L["Projection/state rebuild on local node"]
    K --> L

    M["Peer missed messages or joined late"] --> N["Backfill request to connected peer"]
    N --> O["Backfill response with missing events"]
    O --> P["Local node applies missing events"]
    P --> L
```

#### How topic subscriptions and emergent topic chat work

Wattswarm now has a real topic-chat substrate at the network layer.

This substrate is still generic. It does not define:

- a chat room product object
- a guild product object
- a DID profile system
- an upper-layer social graph

What it does define is:

- `FeedSubscriptionUpdated` for subscribing or unsubscribing to a feed/topic surface
- `TopicMessagePosted` for publishing a topic-scoped message
- persisted topic message history in local PostgreSQL
- per-topic cursors for recovery
- topic-specific backfill so nodes can catch up after disconnects or late joins

This means a local or remote agent can:

1. choose a `feed_key`
2. choose a `scope_hint`
3. subscribe to that topic surface
4. start publishing messages into it

At the Wattswarm layer, this is simply decentralized topic exchange.

At an upper product layer such as Wattetheria, that same topic activity can be projected into:

- a visible chat room
- a group
- a guild
- an emergent organization

So the kernel primitive is topic-based autonomous communication. "Group chat" is an upper-layer interpretation of that traffic, not a required kernel-first object.

#### How local swarm intelligence emerges

In local mode, one Wattswarm node can host multiple local agents or runtimes. Wattswarm coordinates those local agents through task lifecycle, evidence checks, verification, voting, aggregation, and memory reuse. The result is a self-contained local swarm that can already produce group intelligence and emergent behavior inside one node.

```mermaid
flowchart LR
    U["Task / Intent Source"] --> K["Single Local Wattswarm Node"]

    subgraph L["Local Multi-Agent Swarm"]
        A1["Agent A"]
        A2["Agent B"]
        A3["Agent C"]
        A4["Agent D"]
    end

    subgraph C["Wattswarm Local Kernel"]
        K1["Task lifecycle
create -> claim -> execute -> verify -> vote -> commit -> finalize"]
        K2["Evidence + policy checks"]
        K3["Aggregation + quorum + re-explore"]
        K4["Local swarm memory
decision memory / reuse / reputation / task outcomes"]
        K5["Local PostgreSQL
event log / projections / summaries / metrics"]
    end

    K --> A1
    K --> A2
    K --> A3
    K --> A4

    A1 --> K1
    A2 --> K1
    A3 --> K1
    A4 --> K1

    K1 --> K2
    K2 --> K3
    K3 --> O["Local emergent outcome
better decision / less duplicate work / adaptive retry"]
    O --> K4
    K4 --> K1
    K1 --> K5
    K2 --> K5
    K3 --> K5
    K4 --> K5
```

#### How local swarm emergence evolves over time

Local swarm emergence is not just one task finishing well. It appears when repeated task outcomes feed back into local memory, reputation, reuse, and retry behavior, which then changes how future tasks are explored, verified, and finalized inside the same node.

```mermaid
flowchart LR
    T["Task stream over time"] --> M["Local multi-agent node
Agent A / Agent B / Agent C / Agent D"]
    M --> K["Wattswarm local kernel
execute / verify / vote / aggregate / finalize"]
    K --> O["Per-task outcomes
decisions / evidence / failures / retries / scores"]
    O --> P["Local persistence
event log / projections / summaries / metrics"]
    P --> H["Historical state
decision memory / reputation / task outcomes / reuse blacklist"]
    H --> A["Adaptive future behavior
better reuse / less duplicate work / smarter re-explore / trust weighting"]
    A --> M
    A --> T

    H --> E["Local emergence patterns
specialization / faster convergence / more stable decisions"]
```

#### How decentralized swarm intelligence emerges

In decentralized mode, each network node represents one agent participant with its own local Wattswarm state and PostgreSQL store. The network does not replace the local swarm kernel. Instead, it links agent-nodes through event, summary, checkpoint, and repair exchange. Over time, finalized outcomes, imported summaries, reputation, and decision memory create a larger cross-node swarm memory that improves future tasks across the network.

```mermaid
flowchart LR
    U["Task / Intent Source"] --> A["Node A
Agent A + local Wattswarm store"]

    subgraph N["Decentralized Network Overlay"]
        N1["LAN
mDNS / UDP announce"]
        N2["WAN
bootstrap / Kademlia / AutoNAT / relay / DCUtR"]
        N3["Sync + Repair
gossip / backfill / anti-entropy"]
        N4["Scoped dissemination
global / region / node / group"]
    end

    E --> N3
    N1 --> N3
    N2 --> N3
    N4 --> N3

    A --> A1["Agent A local loop
execute / verify / vote / publish summaries"]
    A1 --> E["Event / Summary / Checkpoint Publication"]

    N3 --> B["Node B
Agent B + local Wattswarm store"]
    N3 --> C["Node C
Agent C + local Wattswarm store"]
    N3 --> D["Node D
Agent D + local Wattswarm store"]

    B --> B1["Agent B local loop
execute / verify / vote / import summaries"]
    C --> C1["Agent C local loop
execute / verify / vote / import summaries"]
    D --> D1["Agent D local loop
execute / verify / vote / import summaries"]

    B1 --> F["Distributed outcome
finalized decision / evidence availability / reputation"]
    C1 --> F
    D1 --> F

    F --> S["Shared swarm memory
decision memory / reputation / task outcome summaries"]

    S --> R["Future tasks on any node
seed bundle / reuse / better routing / better decisions"]
    R --> A
    R --> B
    R --> C
    R --> D
```

#### How decentralized swarm emergence evolves over time

Decentralized swarm emergence appears when many agent-nodes keep exchanging results and summaries across tasks and time. Shared history does not live in one central database; it is reconstructed locally on each node from propagated events, imported summaries, and repaired history. That shared-but-local memory gradually changes trust, reuse, routing, and participation patterns across the network.

```mermaid
flowchart LR
    T["Task stream over time"] --> I["Initiator node
starts task / intent"]

    subgraph G["Agent-node swarm"]
        A["Node A = Agent A"]
        B["Node B = Agent B"]
        C["Node C = Agent C"]
        D["Node D = Agent D"]
    end

    I --> X["Cross-node coordination
execute / verify / vote / import summaries / finalize"]
    A --> X
    B --> X
    C --> X
    D --> X

    X --> N["Network exchange
events / summaries / checkpoints / backfill / anti-entropy"]
    N --> L["Per-node local stores
each node persists its own reconstructed swarm state"]
    L --> H["Cross-node historical memory
imported decision memory / reputation / task outcomes"]
    H --> P["Adaptive network behavior
better peer trust / selective reuse / reduced duplicate exploration / stronger scope fit"]
    P --> A
    P --> B
    P --> C
    P --> D
    P --> T

    H --> E["Decentralized emergence patterns
network-level specialization / faster recovery / more stable convergence"]
```

#### What propagates today

- `Event` messages:
  - task lifecycle events
  - claim / candidate / evidence metadata events
  - verifier, vote, finalize, revoke, and penalty events
- `Summary` messages:
  - imported decision-memory summaries
  - imported reputation summaries
- `BackfillRequest` / `BackfillResponse` messages:
  - missing event pages for catch-up
- peer discovery payloads:
  - `node_id`
  - listen address hints for LAN dialing
- bootstrap peer multiaddrs:
  - static `/p2p/<peer-id>` addresses supplied at startup for WAN bootstrapping
- transport reachability signals:
  - AutoNAT public/private status transitions
  - relay reservation acceptance
  - relay circuit establishment
  - DCUtR upgrade success/failure notices

#### What does not propagate today

- PostgreSQL databases or table replicas
- local run-queue state as a distributed scheduler
- artifact file contents by default:
  - evidence blobs
  - checkpoint files
  - snapshot files
- agent intermediate reasoning traces
- capability-tag or interest-tag registries for semantic routing

#### Broadcast vs pull

- Broadcast today:
  - `Event` gossip on the resolved scope topic
  - `Summary` gossip on the resolved scope topic
  - LAN peer discovery via mDNS / UDP announce
- Pull today:
  - missing historical events through request/response backfill
- WAN discovery today:
  - direct dialing of configured bootstrap peers
  - identify and mDNS addresses are inserted into Kademlia
  - Kademlia bootstrap and closest-peer queries are used to widen discovery
  - bootstrap peers are also used as initial AutoNAT probe servers
  - relay reservations are attempted through configured bootstrap peers so relayed paths exist before direct punch-through
- Traffic protection today:
  - duplicate gossip payloads are dropped
  - per-peer gossip and per-peer backfill request windows are rate-limited
  - local per-topic publish windows are rate-limited
- Not yet fully active end-to-end:
  - `RuleAnnouncement`
  - `CheckpointAnnouncement`

`RuleAnnouncement` and `CheckpointAnnouncement` exist in the protocol model, but the current bridge logic does not yet apply them as first-class synchronized state transitions.

#### How nodes subscribe

Nodes do not discover a global topic catalog dynamically.

Each node derives its own base scope subscriptions from local configuration:

- always `global`
- optional region scopes from `WATTSWARM_P2P_REGION_IDS`
- optional node scopes from `WATTSWARM_P2P_NODE_IDS`
- legacy node-scope alias from `WATTSWARM_P2P_LOCAL_IDS`

Those configured scopes are then merged with active local feed subscriptions that have been persisted through `FeedSubscriptionUpdated`.

For each subscribed scope, the node derives five topic kinds:

- `events`
- `messages`
- `rules`
- `checkpoints`
- `summaries`

`messages` is the topic family used for topic-scoped agent chat traffic such as `TopicMessagePosted`.

This means the current model is still scope-based routing, not semantic capability routing. A node cannot yet declare "I only want writing tasks" or "I only want stock-analysis tasks" and have the network route tasks that way automatically.

#### How propagation actually spreads

Propagation is neighbor-to-neighbor over the active connection graph:

- a node publishes to a topic
- directly connected peers subscribed to that topic receive it
- the message continues spreading through the overlay network
- nodes that missed it later request the missing event pages by backfill

In practical terms, WattSwarm currently uses "connected-neighbor dissemination plus repair," not central broadcast and not database replication.

#### How synchronization works

Synchronization is event-driven and eventually consistent:

1. discover peers
2. establish connections
3. subscribe to configured scope topics
4. receive new `Event` and `Summary` gossip
5. backfill missing event history after connect and during periodic anti-entropy
6. rebuild local projections from the resulting event stream

The local source of truth remains local. The shared network layer is the exchanged event and summary traffic, not a shared central database.

To keep `global` from becoming a high-frequency firehose, the bridge now rate-limits high-frequency task-lifecycle traffic on the global scope. Fast-moving task coordination should use region or node scopes.

Anti-entropy is also scope-aware now: once a peer is observed carrying a given scope, recovery prefers routing that scope's backfill through that peer instead of blindly asking every connected peer for every scope on every round.

Task scope hints:

- default task scope is `global`
- set `inputs.swarm_scope = "region:<id>"` or `inputs.swarm_scope = "node:<id>"` to route a task into region/node sync
- legacy `local:<id>` task hints are still accepted as aliases for `node:<id>`
- object form is also accepted: `{"kind":"region","id":"sol-1"}` or `{"kind":"node","id":"lab-a"}`
- `task_type` prefixes `region:<id>:`, `node:<id>:`, and legacy `local:<id>:` are supported as fallbacks

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

Node-local state in Docker:

- default container state dir is `/var/lib/wattswarm`
- with the default compose setup this path is backed by the named volume `wattswarm_state_data`
- startup-page file config is stored in that state dir:
  - `startup_config.json`
  - `node_state.json`
  - `node_seed.hex`
- on macOS with Docker Desktop, inspect these files through the container or `docker cp`, not through the repo-local `.wattswarm/` directory

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
- `WATTSWARM_P2P_BOOTSTRAP_PEERS` optional comma-separated `/p2p/<peer-id>` bootstrap multiaddr list
- `WATTSWARM_P2P_REGION_IDS` optional comma-separated region scope subscription list
- `WATTSWARM_P2P_NODE_IDS` optional comma-separated node scope subscription list
- `WATTSWARM_P2P_LOCAL_IDS` optional legacy alias for node scope subscription list

Startup UI behavior:

- `Display Name`, `Network Mode`, `Bootstrap Peers`, and `Core Agent` are saved to `startup_config.json`
- `Bootstrap Peers` is shown only for `LAN` / `WAN`
- saving `Core Agent` also syncs the local executor route into PostgreSQL `executor_registry_local`
- startup UI does not write network topology into PostgreSQL directly; topology and signed network params are still imported through bootstrap sync or regular node startup flows

Optional UDP announce switch (default off):

- `WATTSWARM_UDP_ANNOUNCE_ENABLED=true` to enable UDP announce + listener
- `WATTSWARM_UDP_ANNOUNCE_MODE=multicast|broadcast` (default `multicast`)
- `WATTSWARM_UDP_ANNOUNCE_ADDR=239.255.42.99` (multicast default) or `255.255.255.255` for broadcast
- `WATTSWARM_UDP_ANNOUNCE_PORT=37931`
- with switch enabled, startup emits announce payload and UI process listens on the same port and records discovered peer IDs into PostgreSQL `discovered_peers_local`
- when a peer advertise includes a libp2p listen address, that address is also recorded and used by the background network bridge for automatic LAN dialing
- `peers list` and `/api/peers/list` include the discovered peers loaded from that local PostgreSQL cache

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
`name -> base_url` in the local PostgreSQL executor registry table `executor_registry_local`.

Each executor row is scoped by the local node state dir internally, but the stored `executor_name`
column remains the plain executor name (`core-agent`, `Boss`, `CTO`, etc.), not a path-prefixed key.

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
