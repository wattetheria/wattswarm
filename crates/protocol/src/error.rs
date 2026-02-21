use thiserror::Error;

#[derive(Debug, Error)]
pub enum SwarmError {
    #[error("invalid event: {0}")]
    InvalidEvent(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("storage error: {0}")]
    Storage(String),
}
