//! QUIC endpoint — the entry point for creating and accepting connections.
//!
//! Phase 2 implements the full TLS + quinn setup.
//! Phase 1 exposes the public API surface with placeholder bodies.

use std::net::SocketAddr;

use crate::{error::TransportError, tls::TlsConfig, QuicConnection};

/// A QUIC endpoint that can act as a server (accept connections)
/// or as a client (initiate connections).
///
/// Wraps [`quinn::Endpoint`] and owns the TLS configuration.
///
/// ## Server usage
/// ```rust,no_run
/// # use grpc_quic_transport::{QuicEndpoint, TlsConfig};
/// # use std::net::SocketAddr;
/// # fn server_tls() -> TlsConfig { todo!() }
/// let endpoint = QuicEndpoint::server("0.0.0.0:50051".parse().unwrap(), server_tls());
/// ```
///
/// ## Client usage
/// ```rust,no_run
/// # use grpc_quic_transport::{QuicEndpoint, TlsConfig};
/// # fn client_tls() -> TlsConfig { todo!() }
/// let endpoint = QuicEndpoint::client(client_tls());
/// ```
#[derive(Debug)]
pub struct QuicEndpoint {
    inner: quinn::Endpoint,
}

impl QuicEndpoint {
    /// Bind a **server-side** endpoint on `addr` using the provided TLS config.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] if the socket cannot be bound or TLS is invalid.
    ///
    /// # Note
    ///
    /// Full implementation in Phase 2 (rustls + quinn server config setup).
    pub fn server(addr: SocketAddr, tls: TlsConfig) -> Result<Self, TransportError> {
        let server_config = tls.server_config()?;
        let quic_server_config = quinn::crypto::rustls::QuicServerConfig::try_from(server_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let quinn_server_config =
            quinn::ServerConfig::with_crypto(std::sync::Arc::new(quic_server_config));

        let endpoint = quinn::Endpoint::server(quinn_server_config, addr)?;
        Ok(Self { inner: endpoint })
    }

    /// Create a **client-side** endpoint bound to an ephemeral local address.
    ///
    /// # Errors
    ///
    /// Returns [`TransportError`] if the socket cannot be created.
    ///
    /// # Note
    ///
    /// Full implementation in Phase 2 (rustls + quinn client config setup).
    pub fn client(tls: TlsConfig) -> Result<Self, TransportError> {
        let client_config = tls.client_config()?;
        let quic_client_config = quinn::crypto::rustls::QuicClientConfig::try_from(client_config)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        let quinn_client_config = quinn::ClientConfig::new(std::sync::Arc::new(quic_client_config));

        let bind_addr = "0.0.0.0:0".parse().unwrap();
        let mut endpoint = quinn::Endpoint::client(bind_addr)?;
        endpoint.set_default_client_config(quinn_client_config);
        Ok(Self { inner: endpoint })
    }

    /// Accept the next incoming QUIC connection.
    ///
    /// Returns `None` when the endpoint has been closed.
    #[tracing::instrument(skip(self))]
    pub async fn accept(&self) -> Option<Result<QuicConnection, TransportError>> {
        let connecting = self.inner.accept().await?;
        Some(
            connecting
                .await
                .map(QuicConnection::new)
                .map_err(TransportError::Connection),
        )
    }

    /// Initiate a connection to a remote server.
    ///
    /// `server_name` is used for TLS SNI.
    #[tracing::instrument(skip(self))]
    pub async fn connect(
        &self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<QuicConnection, TransportError> {
        let conn = self
            .inner
            .connect(addr, server_name)
            .map_err(|e| TransportError::Stream(e.to_string()))?
            .await
            .map_err(TransportError::Connection)?;
        Ok(QuicConnection::new(conn))
    }

    /// Get the local `SocketAddr` this endpoint is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        self.inner.local_addr().map_err(TransportError::Io)
    }

    /// Close the endpoint gracefully.
    pub fn close(&self, error_code: u32, reason: &[u8]) {
        self.inner
            .close(quinn::VarInt::from_u32(error_code), reason);
    }
}
