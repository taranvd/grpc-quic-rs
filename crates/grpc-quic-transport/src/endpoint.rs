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
        // Phase 2: build quinn::ServerConfig from TLS, bind endpoint.
        let _ = (addr, tls);
        Err(TransportError::EndpointBind(
            "Phase 2 not yet implemented".into(),
        ))
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
        // Phase 2: build quinn::ClientConfig from TLS, bind ephemeral socket.
        let _ = tls;
        Err(TransportError::EndpointBind(
            "Phase 2 not yet implemented".into(),
        ))
    }

    /// Accept the next incoming QUIC connection.
    ///
    /// Returns `None` when the endpoint has been closed.
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
    pub async fn connect(
        &self,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<QuicConnection, TransportError> {
        let conn = self
            .inner
            .connect(addr, server_name)
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .await
            .map_err(TransportError::Connection)?;
        Ok(QuicConnection::new(conn))
    }

    /// Close the endpoint gracefully.
    pub fn close(&self, error_code: u32, reason: &[u8]) {
        self.inner
            .close(quinn::VarInt::from_u32(error_code), reason);
    }
}
