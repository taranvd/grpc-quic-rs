//! Client-side error types.

use thiserror::Error;

/// Errors produced by the gRPC-QUIC client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ClientError {
    /// Failed to establish the underlying QUIC connection.
    #[error("transport error: {0}")]
    Transport(#[from] grpc_quic_transport::TransportError),

    /// All retry attempts were exhausted.
    #[error("retries exhausted after {attempts} attempts: {last_error}")]
    RetriesExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// The error from the last attempt.
        last_error: String,
    },

    /// The channel has been shut down.
    #[error("channel closed")]
    Closed,

    /// I/O error while reading/writing the QUIC stream.
    #[error("stream I/O error: {0}")]
    StreamIo(#[from] std::io::Error),

    /// The response from the server was malformed.
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// Failed to build the HTTP/3 request.
    #[error("request build error: {0}")]
    RequestBuild(String),
}
