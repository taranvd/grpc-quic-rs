use std::{
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
};

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
                return Err(ClientError::RequestBuild(format!(
                    "resolver returned no addresses for: {addr_str}"
                )));
            }
            addrs.remove(0)
        } else {
            return Err(ClientError::RequestBuild(format!(
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
            let mut timeout_duration = None;
            if let Some(timeout_val) = req.headers().get("grpc-timeout") {
                if let Ok(timeout_str) = timeout_val.to_str() {
                    timeout_duration = parse_grpc_timeout(timeout_str);
                }
            }
            
            let deadline = timeout_duration.map(|d| tokio::time::Instant::now() + d);

            let rpc_future = async {
                let _permit = concurrency_limit
                    .acquire_owned()
                    .await
                    .map_err(|_| ClientError::Closed)?;

                let (parts, body) = req.into_parts();
                let path = parts.uri.path().to_owned();
                let server_name_for_auth = server_name.clone();
                let authority = parts
                    .uri
                    .authority()
                    .map(|a| a.to_string())
                    .unwrap_or(server_name_for_auth);
                let original_headers = parts.headers;
                let method = parts.method;
                let uri = format!("https://{}{}", authority, path);

                let mut last_error = None;

                for attempt in 0..retry.max_attempts {
                    if attempt > 0 {
                        record_reconnect();
                        let backoff = retry.backoff_for(attempt - 1);
                        trace!(attempt, backoff = ?backoff, "retrying gRPC call");
                        tokio::time::sleep(backoff).await;
                    }

                    let tls_config = tls.clone().unwrap_or_else(TlsConfig::client_default);
                    let server_name = server_name.clone();
                    let entry = match pool
                        .get_or_connect(remote, move |addr| {
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
                            pool.invalidate(&remote).await;
                            continue;
                        }
                    };

                    record_request("client", &path);

                    let mut h3_req = http::Request::builder()
                        .method(method.clone())
                        .uri(&uri)
                        .body(())
                        .map_err(|e| ClientError::RequestBuild(e.to_string()))?;

                    *h3_req.headers_mut() = original_headers.clone();
                    h3_req.headers_mut().insert("content-type", "application/grpc".parse().unwrap());
                    h3_req.headers_mut().insert("te", "trailers".parse().unwrap());

                    let stream = match entry.h3.send_request(h3_req).await {
                        Ok(s) => {
                            record_stream("client");
                            s
                        }
                        Err(e) => {
                            last_error =
                                Some(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                            pool.invalidate(&remote).await;
                            continue; // Can retry connection because body is not consumed yet
                        }
                    };

                    let (mut send, mut recv) = stream.split();
                    let (err_tx, err_rx) = tokio::sync::oneshot::channel();
                    
                    let forward_task = tokio::spawn(async move {
                        tokio::pin!(body);
                        loop {
                            let frame = futures::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
                            match frame {
                                Some(Ok(frame)) => match frame.into_data() {
                                    Ok(data) => {
                                        let len = data.len() as u64;
                                        if let Err(e) = send.send_data(data).await {
                                            let _ = err_tx.send(grpc_quic_core::error::CoreError::H3Stream(e.to_string()));
                                            break;
                                        }
                                        record_bytes_sent("client", len);
                                    }
                                    Err(frame) => {
                                        if let Ok(trailers) = frame.into_trailers() {
                                            if let Err(e) = send.send_trailers(trailers).await {
                                                let _ = err_tx.send(grpc_quic_core::error::CoreError::H3Stream(e.to_string()));
                                            }
                                            return; // trailers close the stream
                                        }
                                    }
                                },
                                Some(Err(e)) => {
                                    let _ = err_tx.send(grpc_quic_core::error::CoreError::H3Stream(e.to_string()));
                                    break;
                                }
                                None => {
                                    if let Err(e) = send.finish().await {
                                        let _ = err_tx.send(grpc_quic_core::error::CoreError::H3Stream(e.to_string()));
                                    }
                                    break;
                                }
                            }
                        }
                    });

                    let resp = match recv.recv_response().await {
                        Ok(r) => r,
                        Err(e) => {
                            forward_task.abort();
                            return Err(ClientError::StreamIo(std::io::Error::other(e.to_string())));
                        }
                    };

                    let body = grpc_quic_core::body::ClientRecvBody::new(recv, Some(err_rx), Some(forward_task.abort_handle()), deadline);

                    let mut response = http::Response::new(body);
                    *response.status_mut() = resp.status();
                    *response.headers_mut() = resp.headers().clone();
                    return Ok(response);
                }

                Err(last_error.unwrap_or_else(|| ClientError::RetriesExhausted {
                    attempts: retry.max_attempts,
                    last_error: "no error captured".into(),
                }))
            };

            if let Some(dl) = deadline {
                match tokio::time::timeout_at(dl, rpc_future).await {
                    Ok(res) => res,
                    Err(_) => Err(ClientError::RequestBuild("DeadlineExceeded".into())),
                }
            } else {
                rpc_future.await
            }
        })
    }
}

fn parse_grpc_timeout(timeout_str: &str) -> Option<std::time::Duration> {
    if timeout_str.is_empty() {
        return None;
    }
    let (val_str, unit) = timeout_str.split_at(timeout_str.len() - 1);
    let val: u64 = val_str.parse().ok()?;
    match unit {
        "H" => Some(std::time::Duration::from_secs(val * 3600)),
        "M" => Some(std::time::Duration::from_secs(val * 60)),
        "S" => Some(std::time::Duration::from_secs(val)),
        "m" => Some(std::time::Duration::from_millis(val)),
        "u" => Some(std::time::Duration::from_micros(val)),
        "n" => Some(std::time::Duration::from_nanos(val)),
        _ => None,
    }
}
