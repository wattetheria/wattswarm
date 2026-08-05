use crate::node::Node;
use crate::types::{ArtifactRef, EventPayload};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;
use std::sync::Arc;
use std::time::Duration;

pub const ENV_NETWORK_BACKEND: &str = "WATTSWARM_NETWORK_BACKEND";
pub const ENV_NETWORK_SERVICE_ENABLED: &str = "WATTSWARM_NETWORK_SERVICE_ENABLED";
pub const ENV_NODE_MAINTENANCE_ENABLED: &str = "WATTSWARM_NODE_MAINTENANCE_ENABLED";
pub const ENV_CLIENT_SERVER_URL: &str = "WATTSWARM_CLIENT_SERVER_URL";
pub(crate) const ENV_P2P_ENABLED: &str = "WATTSWARM_P2P_ENABLED";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NetworkBackend {
    #[default]
    P2p,
    ClientServer,
}

impl NetworkBackend {
    pub fn from_env() -> Result<Self> {
        match env::var(ENV_NETWORK_BACKEND)
            .unwrap_or_else(|_| "p2p".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "p2p" => Ok(Self::P2p),
            "client_server" | "client-server" => Ok(Self::ClientServer),
            value => bail!("unsupported Wattswarm network backend: {value}"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::P2p => "p2p",
            Self::ClientServer => "client_server",
        }
    }
}

pub fn network_service_enabled_from_env(backend: NetworkBackend) -> bool {
    if let Some(enabled) = parse_optional_bool_env(ENV_NETWORK_SERVICE_ENABLED) {
        return enabled;
    }
    match backend {
        NetworkBackend::P2p => parse_optional_bool_env(ENV_P2P_ENABLED).unwrap_or(true),
        NetworkBackend::ClientServer => true,
    }
}

pub fn node_maintenance_enabled_from_env(maintenance_required: bool) -> bool {
    parse_optional_bool_env(ENV_NODE_MAINTENANCE_ENABLED).unwrap_or(maintenance_required)
}

pub fn node_maintenance_explicitly_enabled() -> bool {
    parse_optional_bool_env(ENV_NODE_MAINTENANCE_ENABLED) == Some(true)
}

fn parse_optional_bool_env(key: &str) -> Option<bool> {
    env::var(key)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PublishProgress {
    pub scanned: u64,
    pub published: u64,
    pub backpressured: bool,
}

pub trait OutboundPublisher: Send {
    fn publish_pending(&mut self, node: &Node) -> Result<PublishProgress>;
}

pub trait InboundDeliveryClient: Send {
    fn receive_pending(&mut self, node: &mut Node) -> Result<u64>;
}

pub trait NetworkCommandDispatcher: Send {
    fn dispatch(
        &mut self,
        command: &crate::storage::storage::ClaimedPendingNetworkCommandRow,
    ) -> Result<CommandDisposition>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandDisposition {
    Complete,
    AwaitRemoteAck { retry_at: u64 },
    Retry { retry_at: u64, error: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataPlaneRoute {
    IrohDirect,
    HttpsObjectStore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedContent {
    pub bytes: Vec<u8>,
    pub route: DataPlaneRoute,
}

#[derive(Debug)]
pub struct ContentFetchError {
    pub route: DataPlaneRoute,
    pub source: anyhow::Error,
}

impl std::fmt::Display for ContentFetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "content fetch via {:?} failed: {}",
            self.route, self.source
        )
    }
}

impl std::error::Error for ContentFetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

pub trait ContentFetcher: Send + Sync {
    fn fetch(
        &self,
        artifact: &ArtifactRef,
    ) -> std::result::Result<FetchedContent, ContentFetchError>;
}

pub fn event_artifact_refs(payload: &EventPayload) -> Vec<&ArtifactRef> {
    match payload {
        EventPayload::CandidateProposed(payload) => std::iter::once(&payload.candidate.output_ref)
            .chain(payload.candidate.evidence_refs.iter())
            .collect(),
        EventPayload::EvidenceAdded(payload) => payload.evidence_refs.iter().collect(),
        EventPayload::TaskAnnounced(payload) => payload.detail_ref.iter().collect(),
        EventPayload::TopicMessagePosted(payload) => vec![&payload.content_ref],
        _ => Vec::new(),
    }
}

pub struct HttpObjectStoreContentFetcher {
    base_url: Option<String>,
    http: reqwest::blocking::Client,
    max_attempts: usize,
    bearer_token: std::sync::RwLock<Option<String>>,
}

impl HttpObjectStoreContentFetcher {
    pub fn new(base_url: Option<String>, timeout: Duration, max_attempts: usize) -> Result<Self> {
        if max_attempts == 0 {
            bail!("ContentFetcher max attempts must be positive");
        }
        Ok(Self {
            base_url: base_url.map(|value| value.trim_end_matches('/').to_owned()),
            http: reqwest::blocking::Client::builder()
                .timeout(timeout)
                .build()?,
            max_attempts,
            bearer_token: std::sync::RwLock::new(None),
        })
    }

    pub fn set_bearer_token(&self, token: Option<String>) {
        *self
            .bearer_token
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = token;
    }

    fn object_url(&self, artifact: &ArtifactRef) -> Result<String> {
        if artifact.uri.starts_with("https://") || artifact.uri.starts_with("http://") {
            return Ok(artifact.uri.clone());
        }
        self.base_url
            .as_ref()
            .map(|base| format!("{base}/v1/objects/{}", artifact.digest))
            .ok_or_else(|| anyhow::anyhow!("content_unavailable: no CS Object Store URL"))
    }
}

impl ContentFetcher for HttpObjectStoreContentFetcher {
    fn fetch(
        &self,
        artifact: &ArtifactRef,
    ) -> std::result::Result<FetchedContent, ContentFetchError> {
        let result = (|| {
            let url = self.object_url(artifact)?;
            let mut last_error = None;
            for attempt in 1..=self.max_attempts {
                let mut request = self.http.get(&url);
                if let Some(token) = self
                    .bearer_token
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .as_deref()
                {
                    request = request.bearer_auth(token);
                }
                match request.send() {
                    Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                        bail!("content_unavailable: object was not found")
                    }
                    Ok(response) if response.status().is_success() => {
                        let bytes = response.bytes()?.to_vec();
                        validate_fetched_content(artifact, &bytes)?;
                        return Ok(FetchedContent {
                            bytes,
                            route: DataPlaneRoute::HttpsObjectStore,
                        });
                    }
                    Ok(response) => {
                        last_error = Some(anyhow::anyhow!(
                            "Object Store returned {}",
                            response.status()
                        ));
                    }
                    Err(error) => last_error = Some(error.into()),
                }
                if attempt < self.max_attempts {
                    std::thread::sleep(Duration::from_millis(25 * attempt as u64));
                }
            }
            Err(last_error.unwrap_or_else(|| anyhow::anyhow!("content fetch failed")))
        })();
        result.map_err(|source| ContentFetchError {
            route: DataPlaneRoute::HttpsObjectStore,
            source,
        })
    }
}

type RawContentFetch = dyn Fn(&ArtifactRef) -> Result<Vec<u8>> + Send + Sync + 'static;

pub struct IrohContentFetcher {
    fetch_raw: Arc<RawContentFetch>,
    timeout: Duration,
    max_attempts: usize,
}

impl IrohContentFetcher {
    pub fn new(
        fetch_raw: impl Fn(&ArtifactRef) -> Result<Vec<u8>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            fetch_raw: Arc::new(fetch_raw),
            timeout: Duration::from_secs(5),
            max_attempts: 3,
        }
    }

    pub fn with_policy(
        fetch_raw: impl Fn(&ArtifactRef) -> Result<Vec<u8>> + Send + Sync + 'static,
        timeout: Duration,
        max_attempts: usize,
    ) -> Result<Self> {
        if max_attempts == 0 {
            bail!("ContentFetcher max attempts must be positive");
        }
        Ok(Self {
            fetch_raw: Arc::new(fetch_raw),
            timeout,
            max_attempts,
        })
    }
}

impl ContentFetcher for IrohContentFetcher {
    fn fetch(
        &self,
        artifact: &ArtifactRef,
    ) -> std::result::Result<FetchedContent, ContentFetchError> {
        let mut last_error = None;
        for attempt in 1..=self.max_attempts {
            let fetch_raw = Arc::clone(&self.fetch_raw);
            let artifact_for_fetch = artifact.clone();
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            std::thread::spawn(move || {
                let _ = sender.send(fetch_raw(&artifact_for_fetch));
            });
            match receiver.recv_timeout(self.timeout) {
                Ok(Ok(bytes)) => match validate_fetched_content(artifact, &bytes) {
                    Ok(()) => {
                        return Ok(FetchedContent {
                            bytes,
                            route: DataPlaneRoute::IrohDirect,
                        });
                    }
                    Err(error) => last_error = Some(error),
                },
                Ok(Err(error)) => last_error = Some(error),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    last_error = Some(anyhow::anyhow!("content fetch timed out"));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    last_error = Some(anyhow::anyhow!("content fetch worker disconnected"));
                }
            }
            if attempt < self.max_attempts {
                std::thread::sleep(Duration::from_millis(25 * attempt as u64));
            }
        }
        Err(ContentFetchError {
            route: DataPlaneRoute::IrohDirect,
            source: last_error.unwrap_or_else(|| anyhow::anyhow!("content fetch failed")),
        })
    }
}

fn validate_fetched_content(artifact: &ArtifactRef, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 != artifact.size_bytes {
        bail!("content size does not match ArtifactRef");
    }
    let digest = hex::encode(Sha256::digest(bytes));
    let expected_digest = artifact
        .digest
        .strip_prefix("sha256:")
        .unwrap_or(&artifact.digest);
    if digest != expected_digest {
        bail!("content digest does not match ArtifactRef");
    }
    Ok(())
}

pub struct NetworkServiceCoordinator<P, I, D> {
    pub publisher: P,
    pub inbound: I,
    pub command_dispatcher: D,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn backend_defaults_to_p2p_and_new_enable_flag_wins() {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        unsafe {
            env::remove_var(ENV_NETWORK_BACKEND);
            env::set_var(ENV_P2P_ENABLED, "false");
            env::set_var(ENV_NETWORK_SERVICE_ENABLED, "true");
        }
        assert_eq!(NetworkBackend::from_env().unwrap(), NetworkBackend::P2p);
        assert!(network_service_enabled_from_env(NetworkBackend::P2p));
        unsafe {
            env::remove_var(ENV_NETWORK_SERVICE_ENABLED);
            env::remove_var(ENV_P2P_ENABLED);
        }
    }

    fn artifact(bytes: &[u8], uri: String) -> ArtifactRef {
        ArtifactRef {
            uri,
            digest: format!("sha256:{}", hex::encode(Sha256::digest(bytes))),
            size_bytes: bytes.len() as u64,
            mime: "application/octet-stream".to_owned(),
            created_at: 1,
            producer: "content-fetcher-contract".to_owned(),
        }
    }

    fn serve_http(responses: Vec<(Duration, u16, Vec<u8>)>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for (delay, status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 1024];
                let _ = stream.read(&mut request);
                std::thread::sleep(delay);
                let reason = if status == 200 { "OK" } else { "Error" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(&body);
            }
        });
        format!("http://{address}/object")
    }

    #[test]
    fn iroh_content_fetcher_validates_digest_size_retry_and_timeout() {
        let bytes = b"validated-content".to_vec();
        let reference = artifact(&bytes, "artifact://reference/1".to_owned());
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_fetch = Arc::clone(&attempts);
        let bytes_for_fetch = bytes.clone();
        let fetcher = IrohContentFetcher::with_policy(
            move |_| {
                if attempts_for_fetch.fetch_add(1, Ordering::SeqCst) == 0 {
                    bail!("temporary unavailable");
                }
                Ok(bytes_for_fetch.clone())
            },
            Duration::from_millis(100),
            2,
        )
        .unwrap();
        assert_eq!(fetcher.fetch(&reference).unwrap().bytes, bytes);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);

        let bad_size = ArtifactRef {
            size_bytes: reference.size_bytes + 1,
            ..reference.clone()
        };
        assert!(
            IrohContentFetcher::new(|_| Ok(b"validated-content".to_vec()))
                .fetch(&bad_size)
                .unwrap_err()
                .to_string()
                .contains("size")
        );

        let bad_digest = ArtifactRef {
            digest: "sha256:00".to_owned(),
            ..reference.clone()
        };
        assert!(
            IrohContentFetcher::new(|_| Ok(b"validated-content".to_vec()))
                .fetch(&bad_digest)
                .unwrap_err()
                .to_string()
                .contains("digest")
        );

        let timeout = IrohContentFetcher::with_policy(
            |_| {
                std::thread::sleep(Duration::from_millis(100));
                Ok(Vec::new())
            },
            Duration::from_millis(5),
            1,
        )
        .unwrap();
        assert!(
            timeout
                .fetch(&reference)
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
    }

    #[test]
    fn http_content_fetcher_validates_success_retry_not_found_and_timeout() {
        let bytes = b"object-store-content".to_vec();
        let retry_url = serve_http(vec![
            (Duration::ZERO, 503, Vec::new()),
            (Duration::ZERO, 200, bytes.clone()),
        ]);
        let reference = artifact(&bytes, retry_url);
        let fetcher =
            HttpObjectStoreContentFetcher::new(None, Duration::from_millis(100), 2).unwrap();
        assert_eq!(fetcher.fetch(&reference).unwrap().bytes, bytes);

        let missing_url = serve_http(vec![(Duration::ZERO, 404, Vec::new())]);
        let missing = artifact(b"missing", missing_url);
        assert!(
            fetcher
                .fetch(&missing)
                .unwrap_err()
                .to_string()
                .contains("content_unavailable")
        );

        let timeout_url = serve_http(vec![(Duration::from_millis(100), 200, b"slow".to_vec())]);
        let slow = artifact(b"slow", timeout_url);
        let timeout =
            HttpObjectStoreContentFetcher::new(None, Duration::from_millis(5), 1).unwrap();
        assert!(timeout.fetch(&slow).is_err());
    }
}
