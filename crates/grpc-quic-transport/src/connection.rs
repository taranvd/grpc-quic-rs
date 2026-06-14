//! A single QUIC connection with bi-directional stream support.
//!
//! A [`QuicConnection`] wraps a [`quinn::Connection`] and exposes methods for
//! opening and accepting bi-directional streams. One stream = one gRPC call.

use crate::error::TransportError;

/// A handle to an established QUIC connection.
///
/// Connections are cheap to clone — the underlying connection is reference-counted.
#[derive(Clone, Debug)]
pub struct QuicConnection {
    inner: quinn::Connection,
}

impl QuicConnection {
    /// Wrap an established [`quinn::Connection`].
    pub(crate) fn new(inner: quinn::Connection) -> Self {
        Self { inner }
    }

    /// Open a new outbound bi-directional stream.
    ///
    /// Each call yields an independent pair of `(SendStream, RecvStream)`.
    /// In grpc-quic this maps to **one RPC call**.
    #[tracing::instrument(skip(self))]
    pub async fn open_bi(&self) -> Result<(quinn::SendStream, quinn::RecvStream), TransportError> {
        self.inner
            .open_bi()
            .await
            .map_err(|e| TransportError::Stream(e.to_string()))
    }

    /// Accept the next inbound bi-directional stream from the remote peer.
    ///
    /// Returns `None` when the connection is closed.
    #[tracing::instrument(skip(self))]
    pub async fn accept_bi(
        &self,
    ) -> Option<Result<(quinn::SendStream, quinn::RecvStream), TransportError>> {
        match self.inner.accept_bi().await {
            Ok(pair) => Some(Ok(pair)),
            Err(quinn::ConnectionError::LocallyClosed) => None,
            Err(e) => Some(Err(TransportError::Connection(e))),
        }
    }

    /// Remote address of this connection.
    pub fn remote_address(&self) -> std::net::SocketAddr {
        self.inner.remote_address()
    }

    /// Return a stable connection identifier (useful for logging / metrics).
    pub fn stable_id(&self) -> usize {
        self.inner.stable_id()
    }

    /// Close the connection with an application-level error code.
    pub fn close(&self, error_code: u32, reason: &[u8]) {
        self.inner
            .close(quinn::VarInt::from_u32(error_code), reason);
    }

    /// Returns `true` if the connection has been closed (locally or remotely).
    pub fn is_closed(&self) -> bool {
        self.inner.close_reason().is_some()
    }

    /// Access the underlying [`quinn::Connection`].
    pub fn get_ref(&self) -> &quinn::Connection {
        &self.inner
    }
}
