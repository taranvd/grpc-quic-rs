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

use grpc_quic_discovery::Resolver;
use grpc_quic_metrics::{record_stream, record_request, record_bytes_sent, record_bytes_received, record_reconnect};
use grpc_quic_transport::TlsConfig;

use crate::{error::ClientError, pool::ConnectionPool, retry::RetryPolicy};

/// Builder for [`QuicChannel`].
#[derive(Debug, Default)]
pub struct QuicChannelBuilder {
    retry: RetryPolicy,
    server_name: Option<String>,
    tls: Option<TlsConfig>,
    resolver: Option<Box<dyn Resolver>>,
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

    /// Set a service resolver for discovery-based addressing.
    ///
    /// When configured, `connect()` first tries to parse the input as a raw
    /// socket address. If that fails, it falls back to resolving via this
    /// resolver. If the resolved list is empty, an error is returned.
    pub fn resolver(mut self, resolver: impl Resolver) -> Self {
        self.resolver = Some(Box::new(resolver));
        self
    }

    /// Parse `addr` and build a [`QuicChannel`] ready for use with tonic.
    ///
    /// No network activity happens here; connections are established lazily.
    ///
    /// If a [`resolver`](Self::resolver) has been configured, `addr` is first
    /// tried as a raw socket address; if that fails, the resolver is consulted.
    /// The first resolved address is used.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError`] if `addr` cannot be parsed and no resolver
    /// is configured, or if the resolver returns an empty list.
    #[tracing::instrument(skip(self, addr))]
    pub async fn connect(self, addr: impl Into<String>) -> Result<QuicChannel, ClientError> {
        let addr_str = addr.into();

        let remote = if let Ok(addr) = addr_str.parse::<SocketAddr>() {
            addr
        } else if let Some(ref resolver) = self.resolver {
            let mut addrs = resolver.resolve(&addr_str);
            if addrs.is_empty() {
                return Err(ClientError::InvalidResponse(format!(
                    "resolver returned no addresses for: {addr_str}"
                )));
            }
            addrs.remove(0)
        } else {
            return Err(ClientError::InvalidResponse(format!(
                "invalid address and no resolver configured: {addr_str}"
            )));
        };

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

/// Collect the entire body into a single `Bytes` buffer.
async fn body_to_bytes(mut body: tonic::body::BoxBody) -> Result<Bytes, ClientError> {
    let mut buf = BytesMut::new();
    while let Some(frame_res) = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame_res.map_err(|e| ClientError::StreamIo(std::io::Error::other(e.to_string())))?;
        if let Ok(data) = frame.into_data() {
            buf.extend_from_slice(&data);
        }
    }
    Ok(buf.freeze())
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
                            record_bytes_received("client", filled.len() as u64);
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

        Poll::Pending
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
        let retry = self.retry.clone();

        trace!(
            remote = %remote,
            path = %req.uri().path(),
            "dispatching gRPC call over QUIC"
        );

        Box::pin(async move {
            let path = req.uri().path().to_owned();
            let span = tracing::info_span!(
                "grpc_quic.call",
                remote = %remote,
                path = %path,
            );
            let _guard = span.enter();

            // Reconstruct the body since we may need multiple attempts
            let (_, body) = req.into_parts();
            // Drop the span guard before any .await to keep future Send
            drop(_guard);
            let body_bytes = body_to_bytes(body).await?;
            let _guard = span.enter();

            // Check path length once
            if path.len() > u16::MAX as usize {
                return Err(ClientError::InvalidResponse(format!("request path too long: {}", path.len())));
            }

            let mut last_error = None;

            for attempt in 0..retry.max_attempts {
                if attempt > 0 {
                    record_reconnect();
                    let backoff = retry.backoff_for(attempt - 1);
                    trace!(attempt, backoff = ?backoff, "retrying gRPC call");
                    tokio::time::sleep(backoff).await;
                }

                let tls_config = tls.clone().unwrap_or_else(TlsConfig::client_default);
                let conn = match pool.get_or_connect(remote, |addr| {
                    let tls_config = tls_config.clone();
                    let server_name = server_name.clone();
                    async move {
                        let endpoint = grpc_quic_transport::QuicEndpoint::client(tls_config)?;
                        let conn = endpoint.connect(addr, &server_name).await?;
                        Ok(conn)
                    }
                }).await {
                    Ok(c) => c,
                    Err(e) => {
                        last_error = Some(e);
                        pool.remove(&remote).await;
                        continue;
                    }
                };

                // 2. Open bidirectional stream
                let (mut send, recv) = match conn.open_bi().await {
                    Ok(pair) => {
                        record_stream("client");
                        pair
                    }
                    Err(e) => {
                        last_error = Some(ClientError::Transport(e));
                        pool.remove(&remote).await;
                        continue;
                    }
                };

                record_request("client", &path);

                // 3. Write request header + body
                let mut header_buf = Vec::with_capacity(2 + path.len());
                header_buf.extend_from_slice(&(path.len() as u16).to_be_bytes());
                header_buf.extend_from_slice(path.as_bytes());

                if let Err(e) = send.write_all(&header_buf).await {
                    let io_err = ClientError::StreamIo(std::io::Error::other(e.to_string()));
                    last_error = Some(io_err);
                    continue;
                }
                record_bytes_sent("client", (2 + path.len()) as u64);

                if let Err(e) = send.write_all(&body_bytes).await {
                    let io_err = ClientError::StreamIo(std::io::Error::other(e.to_string()));
                    last_error = Some(io_err);
                    continue;
                }
                record_bytes_sent("client", body_bytes.len() as u64);

                // 4. Close writing side
                if let Err(e) = send.finish() {
                    let io_err = ClientError::StreamIo(std::io::Error::other(e.to_string()));
                    last_error = Some(io_err);
                    continue;
                }

                // 5. Build response
                let resp = http::Response::new(QuicResponseBody::new(recv));
                return Ok(resp);
            }

            Err(last_error.unwrap_or_else(|| ClientError::RetriesExhausted {
                attempts: retry.max_attempts,
                last_error: "no error captured".into(),
            }))
        })
    }
}
