use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("{0}")]
    Message(String),
}
