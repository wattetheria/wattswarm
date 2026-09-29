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
