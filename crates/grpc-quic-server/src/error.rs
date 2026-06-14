//! Server-side error types.

use thiserror::Error;

/// Errors produced by the gRPC-QUIC server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServerError {
    /// Failed to bind the QUIC endpoint.
    #[error("transport error: {0}")]
    Transport(#[from] grpc_quic_transport::TransportError),

    /// The server received a malformed request.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// A stream-level I/O error occurred.
    #[error("stream I/O error: {0}")]
    StreamIo(#[from] std::io::Error),

    /// Graceful shutdown was requested.
    #[error("server shutting down")]
    Shutdown,
}
