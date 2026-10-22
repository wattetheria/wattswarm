use thiserror::Error;

pub const CONTROL_BUSY_ERROR_PREFIX: &str = "control_busy";
pub const CONTROL_BUSY_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("{0}")]
    Message(String),
}
