use crate::constants::LOCAL_PROTOCOL_VERSION;
use crate::control::add_discovered_peer;
use serde_json::json;
use std::env;
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::path::PathBuf;
use std::thread;

const ENV_ENABLED: &str = "WATTSWARM_UDP_ANNOUNCE_ENABLED";
const ENV_MODE: &str = "WATTSWARM_UDP_ANNOUNCE_MODE";
const ENV_ADDR: &str = "WATTSWARM_UDP_ANNOUNCE_ADDR";
const ENV_PORT: &str = "WATTSWARM_UDP_ANNOUNCE_PORT";

const DEFAULT_MULTICAST_ADDR: Ipv4Addr = Ipv4Addr::new(239, 255, 42, 99);
const DEFAULT_BROADCAST_ADDR: Ipv4Addr = Ipv4Addr::new(255, 255, 255, 255);
const DEFAULT_PORT: u16 = 37931;
const ANNOUNCE_KIND: &str = "wattswarm_udp_announce_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpAnnounceMode {
    Broadcast,
    Multicast,
}

impl UdpAnnounceMode {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "broadcast" => Some(Self::Broadcast),
            "multicast" => Some(Self::Multicast),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Broadcast => "broadcast",
            Self::Multicast => "multicast",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct UdpAnnounceConfig {
    enabled: bool,
    mode: UdpAnnounceMode,
    addr: Ipv4Addr,
    port: u16,
}

impl UdpAnnounceConfig {
    fn from_env() -> Self {
        let enabled = parse_bool_env(ENV_ENABLED).unwrap_or(false);
        let mode = env::var(ENV_MODE)
            .ok()
            .and_then(|v| UdpAnnounceMode::parse(&v))
            .unwrap_or(UdpAnnounceMode::Multicast);

        let default_addr = match mode {
            UdpAnnounceMode::Broadcast => DEFAULT_BROADCAST_ADDR,
            UdpAnnounceMode::Multicast => DEFAULT_MULTICAST_ADDR,
        };
        let addr = env::var(ENV_ADDR)
            .ok()
            .and_then(|v| v.parse::<Ipv4Addr>().ok())
            .unwrap_or(default_addr);
        let port = env::var(ENV_PORT)
            .ok()
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(DEFAULT_PORT);

        Self {
            enabled,
            mode,
            addr,
            port,
        }
    }
}

fn parse_bool_env(key: &str) -> Option<bool> {
    env::var(key)
        .ok()
        .and_then(|raw| match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

pub fn announce_startup(component: &str, listen_addr: Option<&str>, node_id: Option<&str>) {
    let cfg = UdpAnnounceConfig::from_env();
    if !cfg.enabled {
        return;
    }

    let payload = json!({
        "kind": ANNOUNCE_KIND,
        "protocol_version": LOCAL_PROTOCOL_VERSION,
        "node_id": node_id,
        "component": component,
        "listen_addr": listen_addr,
        "mode": cfg.mode.as_str(),
        "ts_ms": chrono::Utc::now().timestamp_millis(),
        "hostname": env::var("HOSTNAME").ok(),
    });

    if let Err(err) = send_payload(cfg, &payload.to_string()) {
        eprintln!("udp announce failed: {err}");
    }
}

fn send_payload(cfg: UdpAnnounceConfig, payload: &str) -> std::io::Result<()> {
    let socket = UdpSocket::bind(("0.0.0.0", 0))?;
    if cfg.mode == UdpAnnounceMode::Broadcast {
        socket.set_broadcast(true)?;
    }
    let target = SocketAddrV4::new(cfg.addr, cfg.port);
    let _ = socket.send_to(payload.as_bytes(), target)?;
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct AnnouncePacket {
    kind: String,
    node_id: Option<String>,
}

pub fn maybe_start_listener(state_dir: PathBuf, self_node_id: String) {
    let cfg = UdpAnnounceConfig::from_env();
    if !cfg.enabled {
        return;
    }

    let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, cfg.port);
    let socket = match UdpSocket::bind(bind) {
        Ok(sock) => sock,
        Err(err) => {
            eprintln!("udp announce listener bind failed: {err}");
            return;
        }
    };

    if cfg.mode == UdpAnnounceMode::Multicast
        && let Err(err) = socket.join_multicast_v4(&cfg.addr, &Ipv4Addr::UNSPECIFIED)
    {
        eprintln!("udp announce join multicast failed: {err}");
        return;
    }

    thread::spawn(move || {
        loop {
            let mut buf = [0_u8; 4096];
            let (n, _) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("udp announce recv failed: {err}");
                    continue;
                }
            };
            let raw = match std::str::from_utf8(&buf[..n]) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let packet = match serde_json::from_str::<AnnouncePacket>(raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if packet.kind != ANNOUNCE_KIND {
                continue;
            }
            let Some(peer_id) = packet.node_id else {
                continue;
            };
            if peer_id == self_node_id {
                continue;
            }
            let _ = add_discovered_peer(&state_dir, &peer_id);
        }
    });
}
