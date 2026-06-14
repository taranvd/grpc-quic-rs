use std::io;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoreError {
    #[error("h3 stream error: {0}")]
    H3Stream(String),

    #[error("h3 connection error: {0}")]
    H3Connection(String),

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("QUIC transport error: {0}")]
    Transport(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("invalid response: {0}")]
    InvalidResponse(String),
}

impl From<h3::error::StreamError> for CoreError {
    fn from(e: h3::error::StreamError) -> Self {
        CoreError::H3Stream(e.to_string())
    }
}

impl From<h3::error::ConnectionError> for CoreError {
    fn from(e: h3::error::ConnectionError) -> Self {
        CoreError::H3Connection(e.to_string())
    }
}
