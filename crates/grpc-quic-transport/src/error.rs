//! Transport error types.

use thiserror::Error;

/// Errors produced by the QUIC transport layer.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransportError {
    /// Failed to bind or configure the QUIC endpoint.
    ///
    /// Typically an OS-level socket bind error.
    #[error("endpoint bind error: {0}")]
    EndpointBind(String),

    /// A QUIC connection-level error.
    #[error("connection error: {0}")]
    Connection(#[from] quinn::ConnectionError),

    /// Failed to open or accept a QUIC stream.
    #[error("stream error: {0}")]
    Stream(String),

    /// TLS configuration error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// I/O error on an underlying socket.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
