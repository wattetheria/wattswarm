# third_party

## iroh-0.97.0

Unmodified `iroh` 0.97.0 from crates.io plus one backport, wired in through
`[patch.crates-io]` in the workspace `Cargo.toml`.

**Backport:** [n0-computer/iroh#4463](https://github.com/n0-computer/iroh/pull/4463)
(released in iroh 1.1.0), applied as `iroh-0.97-backport-4463.patch` to
`src/net_report/reportgen.rs`.

**Why:** net_report's HTTPS latency probe and captive-portal check resolve the relay
host locally and pin reqwest to those addresses with `resolve_to_addrs`. reqwest then
connects to the address directly and skips an HTTP(S) proxy's CONNECT tunnel. On hosts
whose only egress is a proxy and that have no usable local DNS (for example sandboxed
cloud agents), no relay latency is ever measured, so no home relay is selected and the
node publishes empty `relay_urls`: peers can never dial it. With a proxy configured in
the environment, the patched probes skip the local lookup and let the proxy resolve and
tunnel by hostname; without one, behaviour is unchanged.

**Remove** this directory and the `[patch.crates-io]` entry when upgrading iroh to 1.1.0
or later.

## iroh-gossip-0.97.0

`iroh-gossip` 0.97.0 from crates.io plus two fixes, wired in through `[patch.crates-io]`
in the workspace `Cargo.toml`. The crate's own CI and tooling files (`.github`, `.cargo`,
`.config`, `.gitignore`, `Cargo.lock`) are left out.

**Fix 1:** `iroh-gossip-0.97-close-unused-connections.patch`, applied to `src/net.rs`,
`src/net/util.rs`, `src/proto/hyparview.rs` and `src/proto/state.rs`.

**Why:** iroh-gossip 0.97 never closes a gossip connection. Every neighbor that is
replaced and every short exchange with a non-neighbor (join, shuffle, shuffle reply)
leaves a QUIC connection open, about 70 KB each, and iroh's keep-alive stops it from
timing out. A node in a busy swarm opens several connections per second, so memory grows
until the process is killed (`wattswarm-kernel` reached 900 MB in about ten minutes).
Upstream describes and fixes the same bug in
[n0-computer/iroh-gossip#178](https://github.com/n0-computer/iroh-gossip/pull/178)
(released in 0.102), but that release needs iroh 1.x.

The patch does four things:

- The send loop ends when its sender is dropped. `Some(msg) = send_rx.recv()` in the
  `select!` only disabled that branch, so the loop kept waiting for the connection to close.
- Once our side stopped sending, the receive loop closes the connection after the remote
  sent nothing for 5 s.
- The actor drops a peer that no topic uses and that it did not send to for 5 s. The
  protocol state only asks to disconnect peers it received a message from.
- When the connection the actor sends on closes, the actor forgets the peer, so the next
  message dials again instead of going to the closed connection.

`net::test::unused_connections_are_closed` in the patched crate reproduces the leak, and
`gossip_closes_unused_connections` in `crates/network-transport-iroh` covers it from the
workspace.

**Fix 2:** `iroh-gossip-0.97-drop-queue-after-failed-dial.patch`, applied on top of fix 1
to `src/net.rs`.

**Why:** messages to a peer that is not connected yet wait in a per-peer queue while the
peer is dialed. When the dial failed, the queue stayed: the protocol state only asks to
disconnect peers it received a message from. A non-empty queue starts no new dial, so
every later message to that peer was added to it and never sent. `wattswarm-kernel` kept
about 930,000 queued join messages (104 MB) after two hours with an estimated 44 unreachable
contacts, and it never dialed those contacts again.

The patch drops the peer, and with it the queue, when its dial fails. The next message to
the peer dials again.

`net::test::failed_dial_is_retried_by_the_next_message` in the patched crate reproduces
it, and `gossip_dials_again_after_failed_dial` in `crates/network-transport-iroh` covers
it from the workspace.

**Remove** this directory and the `[patch.crates-io]` entry when upgrading iroh-gossip to
0.102 or later. Check first that the release has fix 2: it is not known to be upstream.
