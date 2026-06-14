use std::{
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use http_body::Body;
use std::pin::Pin;
use tokio::sync::Semaphore;
use tower::Service;
use tracing::trace;

use grpc_quic_discovery::Resolver;
use grpc_quic_metrics::{record_bytes_sent, record_reconnect, record_request, record_stream};
use grpc_quic_transport::TlsConfig;

use crate::{error::ClientError, pool::ConnectionPool, retry::RetryPolicy};

const DEFAULT_CONCURRENCY_LIMIT: usize = 256;

#[derive(Debug)]
pub struct QuicChannelBuilder {
    retry: RetryPolicy,
    server_name: Option<String>,
    tls: Option<TlsConfig>,
    resolver: Option<Box<dyn Resolver>>,
    concurrency_limit: usize,
}

impl Default for QuicChannelBuilder {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            server_name: None,
            tls: None,
            resolver: None,
            concurrency_limit: DEFAULT_CONCURRENCY_LIMIT,
        }
    }
}

impl QuicChannelBuilder {
    pub fn retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }

    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn concurrency_limit(mut self, limit: usize) -> Self {
        self.concurrency_limit = limit;
        self
    }

    pub fn resolver(mut self, resolver: impl Resolver) -> Self {
        self.resolver = Some(Box::new(resolver));
        self
    }

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
        let server_name = self.server_name.unwrap_or_else(|| remote.ip().to_string());
        Ok(QuicChannel {
            remote,
            server_name,
            tls: self.tls,
            retry: self.retry,
            pool: ConnectionPool::new(),
            concurrency_limit: Arc::new(Semaphore::new(self.concurrency_limit)),
        })
    }
}

async fn buffer_body(mut body: tonic::body::BoxBody) -> Result<Bytes, ClientError> {
    use bytes::BytesMut;
    let mut buf = BytesMut::new();
    while let Some(frame_res) =
        futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
    {
        let frame =
            frame_res.map_err(|e| ClientError::StreamIo(std::io::Error::other(e.to_string())))?;
        if let Ok(data) = frame.into_data() {
            buf.extend_from_slice(&data);
        }
    }
    Ok(buf.freeze())
}

#[derive(Clone, Debug)]
pub struct QuicChannel {
    remote: SocketAddr,
    server_name: String,
    tls: Option<TlsConfig>,
    retry: RetryPolicy,
    pool: ConnectionPool,
    concurrency_limit: Arc<Semaphore>,
}

impl QuicChannel {
    pub fn builder() -> QuicChannelBuilder {
        QuicChannelBuilder::default()
    }
}

impl Service<http::Request<tonic::body::BoxBody>> for QuicChannel {
    type Response = http::Response<grpc_quic_core::body::ClientRecvBody>;
    type Error = ClientError;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let remote = self.remote;
        let server_name = self.server_name.clone();
        let tls = self.tls.clone();
        let pool = self.pool.clone();
        let retry = self.retry.clone();
        let concurrency_limit = self.concurrency_limit.clone();

        trace!(remote = %remote, path = %req.uri().path(), "dispatching gRPC call over HTTP/3");

        Box::pin(async move {
            let _permit = concurrency_limit
                .acquire_owned()
                .await
                .map_err(|_| ClientError::Closed)?;

            let path = req.uri().path().to_owned();
            let authority = req
                .uri()
                .authority()
                .map(|a| a.to_string())
                .unwrap_or_else(|| server_name.clone());
            let (_, body) = req.into_parts();

            let body_bytes = buffer_body(body).await?;

            let mut last_error = None;

            for attempt in 0..retry.max_attempts {
                if attempt > 0 {
                    record_reconnect();
                    let backoff = retry.backoff_for(attempt - 1);
                    trace!(attempt, backoff = ?backoff, "retrying gRPC call");
                    tokio::time::sleep(backoff).await;
                }

                let tls_config = tls.clone().unwrap_or_else(TlsConfig::client_default);
                let entry = match pool
                    .get_or_connect(remote, |addr| {
                        let tls_config = tls_config.clone();
                        let server_name = server_name.clone();
                        async move {
                            let endpoint = grpc_quic_transport::QuicEndpoint::client(tls_config)?;
                            let conn = endpoint.connect(addr, &server_name).await?;
                            Ok(conn)
                        }
                    })
                    .await
                {
                    Ok(e) => e,
                    Err(e) => {
                        last_error = Some(e);
                        pool.remove(&remote).await;
                        continue;
                    }
                };

                record_request("client", &path);

                let uri = format!("https://{}{}", authority, path);
                let h3_req = http::Request::builder()
                    .method(http::Method::POST)
                    .uri(&uri)
                    .header("content-type", "application/grpc")
                    .header("te", "trailers")
                    .body(())
                    .map_err(|e| ClientError::InvalidResponse(e.to_string()))?;

                let mut stream = match entry.h3.send_request(h3_req).await {
                    Ok(s) => {
                        record_stream("client");
                        s
                    }
                    Err(e) => {
                        last_error =
                            Some(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                        pool.remove(&remote).await;
                        continue;
                    }
                };

                if !body_bytes.is_empty() {
                    if let Err(e) = stream.send_data(body_bytes.clone()).await {
                        last_error =
                            Some(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                        continue;
                    }
                    record_bytes_sent("client", body_bytes.len() as u64);
                }

                if let Err(e) = stream.finish().await {
                    last_error = Some(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                    continue;
                }

                let resp = match stream.recv_response().await {
                    Ok(r) => r,
                    Err(e) => {
                        last_error =
                            Some(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                        continue;
                    }
                };

                let (_send, recv) = stream.split();
                let body = grpc_quic_core::body::ClientRecvBody::new(recv);

                let mut response = http::Response::new(body);
                *response.status_mut() = resp.status();
                *response.headers_mut() = resp.headers().clone();
                return Ok(response);
            }

            Err(last_error.unwrap_or_else(|| ClientError::RetriesExhausted {
                attempts: retry.max_attempts,
                last_error: "no error captured".into(),
            }))
        })
    }
}
