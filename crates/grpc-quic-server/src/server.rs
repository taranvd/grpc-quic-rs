//! QuicServer — builder and main serve loop.

use grpc_quic_metrics::record_connection;
use grpc_quic_transport::{QuicConnection, QuicEndpoint, TlsConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::acceptor::handle_request;
use crate::error::ServerError;

/// Builder for [`QuicServer`].
#[derive(Debug)]
pub struct QuicServerBuilder {
    tls: Option<TlsConfig>,
    max_concurrent_streams: Option<u32>,
    graceful_timeout: std::time::Duration,
}

impl Default for QuicServerBuilder {
    fn default() -> Self {
        Self {
            tls: None,
            max_concurrent_streams: None,
            graceful_timeout: std::time::Duration::from_secs(30),
        }
    }
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

    /// Set the timeout for graceful shutdown to drain existing streams (default 30s).
    pub fn graceful_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.graceful_timeout = timeout;
        self
    }

    /// Return a configured [`QuicServer`]. The actual socket bind happens in
    /// [`serve`](QuicServer::serve) or [`serve_with_incoming`](QuicServer::serve_with_incoming).
    pub fn build(self) -> QuicServer {
        QuicServer {
            tls: self.tls,
            max_concurrent_streams: self.max_concurrent_streams.unwrap_or(256),
            graceful_timeout: self.graceful_timeout,
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
///
/// ```ignore
/// // Build and start the server:
/// let server = QuicServer::builder()
///     .tls(tls_config)
///     .build();
///
/// // Pass any tonic-generated Router or service_fn:
/// server.serve(addr, MyServiceServer::new(my_service)).await?;
/// ```
#[derive(Debug)]
pub struct QuicServer {
    pub(crate) tls: Option<TlsConfig>,
    pub(crate) max_concurrent_streams: u32,
    pub(crate) graceful_timeout: std::time::Duration,
}

impl QuicServer {
    /// Return a builder to configure the server.
    pub fn builder() -> QuicServerBuilder {
        QuicServerBuilder::default()
    }

    /// Bind to `addr` and serve requests until a shutdown signal is received.
    pub async fn serve<S>(self, addr: SocketAddr, service: S) -> Result<(), ServerError>
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        self.serve_with_shutdown(addr, service, std::future::pending())
            .await
    }

    /// Bind to `addr` and serve requests until the `signal` future completes.
    pub async fn serve_with_shutdown<S, F>(
        self,
        addr: SocketAddr,
        service: S,
        signal: F,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let tls = self.tls.clone().ok_or_else(|| {
            ServerError::Transport(grpc_quic_transport::TransportError::Tls(
                "TLS config is required".into(),
            ))
        })?;

        let endpoint = grpc_quic_transport::QuicEndpoint::server(addr, tls)?;
        self.serve_with_incoming_shutdown(endpoint, service, signal)
            .await
    }

    /// Serve requests over an already-bound `QuicEndpoint`.
    pub async fn serve_with_incoming<S>(
        self,
        endpoint: QuicEndpoint,
        service: S,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        self.serve_with_incoming_shutdown(endpoint, service, std::future::pending())
            .await
    }

    /// Serve requests over an already-bound `QuicEndpoint` until the `signal` future completes.
    #[tracing::instrument(skip(self, endpoint, service, signal))]
    pub async fn serve_with_incoming_shutdown<S, F>(
        self,
        endpoint: QuicEndpoint,
        service: S,
        signal: F,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<tonic::body::BoxBody>,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        info!(
            local_addr = ?endpoint.local_addr(),
            max_concurrent_streams = self.max_concurrent_streams,
            "QuicServer listening"
        );

        let mut signal = Box::pin(signal);

        // Global semaphore that bounds the total number of concurrent stream
        // handler tasks across all connections.  When exhausted, new streams
        // are dropped (try_acquire_owned fails), providing backpressure.
        let stream_limit = (self.max_concurrent_streams as usize).max(64) * 4;
        let stream_semaphore = Arc::new(Semaphore::new(stream_limit));

        let mut join_set = tokio::task::JoinSet::new();
        let (cancel_tx, _) = tokio::sync::broadcast::channel::<()>(1);

        loop {
            tokio::select! {
                _ = &mut signal => {
                    info!("shutdown signal received, rejecting new connections");
                    endpoint.reject_new_connections();
                    let _ = cancel_tx.send(());
                    break;
                }
                conn_res = endpoint.accept() => {
                    let conn_res = match conn_res {
                        Some(res) => res,
                        None => break,
                    };
                    let conn = match conn_res {
                        Ok(c) => {
                            record_connection("server");
                            c
                        }
                        Err(e) => {
                            error!(error = %e, "failed to accept connection");
                            continue;
                        }
                    };

                    let service = service.clone();
                    let sem = stream_semaphore.clone();
                    let cancel_rx = cancel_tx.subscribe();
                    join_set.spawn(async move {
                        if let Err(e) = handle_connection(conn, service, sem, cancel_rx).await {
                            error!(error = %e, "connection handling error");
                        }
                    });
                }
            }
        }

        // Wait for all in-flight connections to complete with a 30s timeout
        let wait_for_connections = async {
            while let Some(result) = join_set.join_next().await {
                if let Err(e) = result {
                    error!("connection task failed: {e}");
                }
            }
        };

        if tokio::time::timeout(self.graceful_timeout, wait_for_connections).await.is_err() {
            error!("graceful shutdown timed out, closing endpoint forcefully");
            endpoint.close(0, b"shutdown timeout");
        }

        Ok(())
    }
}

#[tracing::instrument(skip(conn, service, semaphore, cancel_rx))]
async fn handle_connection<S>(
    conn: QuicConnection,
    service: S,
    semaphore: Arc<Semaphore>,
    mut cancel_rx: tokio::sync::broadcast::Receiver<()>,
) -> Result<(), ServerError>
where
    S: tower::Service<
            http::Request<tonic::body::BoxBody>,
            Response = http::Response<tonic::body::BoxBody>,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    use grpc_quic_core::server::build_server_conn;

    let mut h3_conn = match build_server_conn(conn.get_ref().clone()).await {
        Ok(c) => c,
        Err(e) => {
            error!("failed to build h3 server connection: {e}");
            return Ok(());
        }
    };

    let mut request_join_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            _ = cancel_rx.recv() => {
                let _ = h3_conn.shutdown(0).await;
                // As requested: do not poll accept() anymore, but we must
                // keep h3_conn alive so that active streams aren't abruptly destroyed.
                break;
            }
            accept_res = h3_conn.accept() => {
                let resolver = match accept_res {
                    Ok(Some(r)) => r,
                    Ok(None) => break,
                    Err(e) => {
                        error!("h3 accept error: {e}");
                        break;
                    }
                };

                let (req, stream) = match resolver.resolve_request().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        error!("resolve request error: {e}");
                        continue;
                    }
                };

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        error!("server overloaded — dropping request");
                        continue;
                    }
                };

                let service = service.clone();
                request_join_set.spawn(async move {
                    let _permit = permit;
                    if let Err(e) = handle_request(req, stream, service).await {
                        error!(error = %e, "request handling error");
                    }
                });
            }
        }
    }

    // Wait for all active requests on this connection to finish
    while let Some(res) = request_join_set.join_next().await {
        if let Err(e) = res {
            error!("request task failed: {e}");
        }
    }

    drop(h3_conn);

    Ok(())
}
