//! QuicChannel — tower::Service adapter that bridges tonic to QUIC streams.
//!
//! ## Forwarding principle
//!
//! grpc-quic **MUST NOT** interpret, modify, or re-encode gRPC payloads.
//! It only forwards bytes between tonic and QUIC streams.
//!
//! ```text
//! tonic (encodes gRPC payload as Bytes)
//!     ↓  [opaque Bytes — untouched by grpc-quic]
//! QuicChannel  →  open QUIC bi-stream  →  write path header + Bytes
//!     ↑  [opaque Bytes — untouched by grpc-quic]
//! tonic (decodes gRPC response from Bytes)
//! ```

use std::{
    net::SocketAddr,
    task::{Context, Poll},
};

use bytes::Bytes;
use tower::Service;
use tracing::trace;

use grpc_quic_transport::TlsConfig;

use crate::{error::ClientError, pool::ConnectionPool, retry::RetryPolicy};

/// Builder for [`QuicChannel`].
#[derive(Debug, Default)]
pub struct QuicChannelBuilder {
    retry: RetryPolicy,
    server_name: Option<String>,
    tls: Option<TlsConfig>,
}

impl QuicChannelBuilder {
    /// Override the retry policy.
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    /// Set the TLS server name (SNI). Defaults to the IP string of the address.
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Override the TLS configuration (required for mTLS or custom CAs).
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Parse `addr` and build a [`QuicChannel`] ready for use with tonic.
    ///
    /// No network activity happens here; connections are established lazily.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if `addr` cannot be parsed.
    pub async fn connect(self, addr: impl Into<String>) -> Result<QuicChannel, ClientError> {
        let addr_str = addr.into();
        let remote: SocketAddr = addr_str
            .parse()
            .map_err(|_| ClientError::InvalidResponse(format!("invalid address: {addr_str}")))?;

        let server_name = self
            .server_name
            .unwrap_or_else(|| remote.ip().to_string());

        Ok(QuicChannel {
            remote,
            server_name,
            tls: self.tls,
            retry: self.retry,
            pool: ConnectionPool::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Request / response body newtype
// ---------------------------------------------------------------------------

/// Opaque response body — wraps a [`Bytes`] payload without any interpretation.
///
/// grpc-quic never inspects the contents; it is passed directly to tonic.
#[derive(Debug)]
pub struct QuicResponseBody {
    data: Option<Bytes>,
}

impl QuicResponseBody {
    #[allow(dead_code)] // used in Phase 4
    pub(crate) fn new(data: Bytes) -> Self {
        Self { data: Some(data) }
    }
}

impl http_body::Body for QuicResponseBody {
    type Data = Bytes;
    type Error = ClientError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(
            self.data
                .take()
                .map(|b| Ok(http_body::Frame::data(b))),
        )
    }
}

// ---------------------------------------------------------------------------
// QuicChannel
// ---------------------------------------------------------------------------

/// A tonic-compatible channel that routes gRPC calls over QUIC streams.
///
/// Pass this directly to any tonic-generated `*Client::new(channel)`.
///
/// ```rust,no_run
/// use grpc_quic_client::QuicChannel;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let channel = QuicChannel::builder()
///     .connect("127.0.0.1:50051")
///     .await?;
/// // let client = MyServiceClient::new(channel);
/// # Ok(())
/// # }
/// ```
///
/// ## Wire envelope per QUIC bi-directional stream
///
/// ```text
/// ┌─ Request envelope (client → server) ──────────────────────┐
/// │  [u16 BE: path_len][path_bytes][gRPC payload from tonic…]  │
/// └────────────────────────────────────────────────────────────┘
/// ┌─ Response envelope (server → client) ─────────────────────┐
/// │  [gRPC response payload from tonic…][u32 BE: grpc_status]  │
/// └────────────────────────────────────────────────────────────┘
/// ```
///
/// The `path_len` / `path_bytes` header is **routing metadata only** — it is
/// the HTTP path tonic already put on the request (e.g. `/pkg.Svc/Method`).
/// The gRPC payload bytes are **forwarded verbatim** and never interpreted.
#[derive(Clone, Debug)]
pub struct QuicChannel {
    remote: SocketAddr,
    server_name: String,
    tls: Option<TlsConfig>,
    #[allow(dead_code)] // used in Phase 4
    retry: RetryPolicy,
    pool: ConnectionPool,
}

impl QuicChannel {
    /// Return a builder to configure and create a channel.
    pub fn builder() -> QuicChannelBuilder {
        QuicChannelBuilder::default()
    }
}

// ---------------------------------------------------------------------------
// tower::Service impl — full I/O implementation arrives in Phase 4.
// ---------------------------------------------------------------------------

impl Service<http::Request<tonic::body::BoxBody>> for QuicChannel {
    type Response = http::Response<QuicResponseBody>;
    type Error = ClientError;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready — stream is opened per-call in `call`.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let remote = self.remote;
        let server_name = self.server_name.clone();
        let _tls = self.tls.clone();
        let pool = self.pool.clone();

        trace!(
            remote = %remote,
            path = %req.uri().path(),
            "dispatching gRPC call over QUIC"
        );

        Box::pin(async move {
            // Phase 4: open QUIC bi-stream, write path header + body bytes
            // (verbatim from tonic), read response bytes, return to tonic.
            let _ = (pool, server_name);
            Err(ClientError::Closed) // placeholder until Phase 4
        })
    }
}
