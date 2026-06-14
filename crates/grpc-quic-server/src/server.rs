//! QuicServer — builder and main serve loop.

use std::net::SocketAddr;

use grpc_quic_transport::TlsConfig;
use tracing::info;

use crate::error::ServerError;

/// Builder for [`QuicServer`].
#[derive(Debug, Default)]
pub struct QuicServerBuilder {
    tls: Option<TlsConfig>,
    max_concurrent_streams: Option<u32>,
}

impl QuicServerBuilder {
    /// Set the TLS configuration (required for production; test helpers available).
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Limit the number of concurrent streams per connection.
    pub fn max_concurrent_streams(mut self, limit: u32) -> Self {
        self.max_concurrent_streams = Some(limit);
        self
    }

    /// Bind and return a [`QuicServer`] ready to serve.
    pub fn build(self) -> QuicServer {
        QuicServer {
            tls: self.tls,
            max_concurrent_streams: self.max_concurrent_streams.unwrap_or(256),
        }
    }
}

/// A QUIC server that delegates incoming gRPC requests to a tonic service.
///
/// ```text
/// QuicServer
///   └── quinn::Endpoint  (accepts QUIC connections)
///         └── per connection: accept bi-streams
///               └── each bi-stream: read path + gRPC bytes → tonic handler
/// ```
#[derive(Debug)]
pub struct QuicServer {
    // Phase 3: fields used in acceptor loop
    #[allow(dead_code)]
    tls: Option<TlsConfig>,
    #[allow(dead_code)]
    max_concurrent_streams: u32,
}

impl QuicServer {
    /// Return a builder to configure the server.
    pub fn builder() -> QuicServerBuilder {
        QuicServerBuilder::default()
    }

    /// Bind to `addr` and serve requests until a shutdown signal is received.
    ///
    /// Full implementation arrives in Phase 3.
    pub async fn serve(self, addr: SocketAddr) -> Result<(), ServerError> {
        info!(%addr, max_concurrent_streams = self.max_concurrent_streams, "QuicServer starting");
        // Phase 3: bind endpoint, accept connections, dispatch streams.
        Ok(())
    }
}
