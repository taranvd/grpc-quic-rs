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

use bytes::{Bytes, BytesMut};
use tower::Service;
use tracing::trace;
use http_body::{Body, Frame};
use std::pin::Pin;
use tokio::io::AsyncRead;
use quinn::RecvStream;

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
pub struct QuicResponseBody {
    recv: RecvStream,
    buf: BytesMut,
    eof: bool,
}

impl std::fmt::Debug for QuicResponseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicResponseBody")
            .field("buf", &self.buf)
            .field("eof", &self.eof)
            .finish()
    }
}

impl QuicResponseBody {
    pub(crate) fn new(recv: RecvStream) -> Self {
        Self {
            recv,
            buf: BytesMut::new(),
            eof: false,
        }
    }
}

impl http_body::Body for QuicResponseBody {
    type Data = Bytes;
    type Error = ClientError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;

        // 1. Read all available data from the stream into `buf`
        if !this.eof {
            let mut temp_buf = [0u8; 8192];
            loop {
                let mut read_buf = tokio::io::ReadBuf::new(&mut temp_buf);
                match Pin::new(&mut this.recv).poll_read(cx, &mut read_buf) {
                    Poll::Ready(Ok(())) => {
                        let filled = read_buf.filled();
                        if filled.is_empty() {
                            this.eof = true;
                            break;
                        } else {
                            this.buf.extend_from_slice(filled);
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(ClientError::StreamIo(e))));
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }

        // 2. Parse frames from `buf`
        loop {
            let n = this.buf.len();

            // Check if we have the trailers block (only when eof is true and it's the only thing left)
            if this.eof && n >= 6 {
                let status = u32::from_be_bytes([this.buf[0], this.buf[1], this.buf[2], this.buf[3]]);
                let msg_len = u16::from_be_bytes([this.buf[4], this.buf[5]]) as usize;
                if 6 + msg_len == n {
                    let msg_bytes = this.buf[6..6+msg_len].to_vec();
                    let msg = String::from_utf8(msg_bytes)
                        .unwrap_or_else(|_| "invalid utf-8 message".to_string());
                    this.buf.clear();

                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", http::HeaderValue::from_str(&status.to_string()).unwrap());
                    if !msg.is_empty() {
                        trailers.insert("grpc-message", http::HeaderValue::from_str(&msg).unwrap());
                    }
                    return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
                }
            }

            // Otherwise, try to parse a gRPC frame
            if n >= 5 {
                let len = u32::from_be_bytes([this.buf[1], this.buf[2], this.buf[3], this.buf[4]]) as usize;
                let total_len = 5 + len;

                if n >= total_len {
                    let data = this.buf.split_to(total_len).freeze();
                    return Poll::Ready(Some(Ok(Frame::data(data))));
                } else if this.eof {
                    return Poll::Ready(Some(Err(ClientError::InvalidResponse(
                        "truncated gRPC frame".into()
                    ))));
                } else {
                    return Poll::Pending;
                }
            }

            if this.eof {
                if n > 0 {
                    return Poll::Ready(Some(Err(ClientError::InvalidResponse(
                        format!("trailing garbage bytes at end of stream: {n} bytes")
                    ))));
                }
                return Poll::Ready(None);
            }

            return Poll::Pending;
        }
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
        let tls = self.tls.clone();
        let pool = self.pool.clone();

        trace!(
            remote = %remote,
            path = %req.uri().path(),
            "dispatching gRPC call over QUIC"
        );

        Box::pin(async move {
            // 1. Get connection from pool
            let tls_config = tls.unwrap_or_else(TlsConfig::client_default);
            let conn = pool.get_or_connect(remote, move |addr| async move {
                let endpoint = grpc_quic_transport::QuicEndpoint::client(tls_config)?;
                let conn = endpoint.connect(addr, &server_name).await?;
                Ok(conn)
            }).await?;

            // 2. Open bidirectional stream
            let (mut send, recv) = conn.open_bi().await
                .map_err(ClientError::Transport)?;

            // 3. Write request header: path length (2 bytes BE) + path bytes
            let path = req.uri().path();
            let path_len = path.len();
            if path_len > u16::MAX as usize {
                return Err(ClientError::InvalidResponse(format!("request path too long: {path_len}")));
            }
            
            let mut header_buf = Vec::with_capacity(2 + path_len);
            header_buf.extend_from_slice(&(path_len as u16).to_be_bytes());
            header_buf.extend_from_slice(path.as_bytes());
            
            send.write_all(&header_buf).await
                .map_err(|e| ClientError::StreamIo(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

            // 4. Stream request body frames (verbatim from tonic)
            let mut body = req.into_body();
            while let Some(frame_res) = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
                let frame = frame_res.map_err(|e| ClientError::StreamIo(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))?;
                
                if let Ok(data) = frame.into_data() {
                    use bytes::Buf;
                    let mut data = data;
                    while data.has_remaining() {
                        let chunk = data.chunk();
                        send.write_all(chunk).await
                            .map_err(|e| ClientError::StreamIo(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
                        let len = chunk.len();
                        data.advance(len);
                    }
                }
            }

            // 5. Close writing side of the stream
            send.finish()
                .map_err(|e| ClientError::StreamIo(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

            // 6. Wrap recv stream into response body
            let resp_body = QuicResponseBody::new(recv);
            Ok(http::Response::new(resp_body))
        })
    }
}
