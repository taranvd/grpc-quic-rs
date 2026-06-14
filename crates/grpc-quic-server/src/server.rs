//! QuicServer — builder and main serve loop.

use grpc_quic_metrics::record_connection;
use grpc_quic_transport::{QuicConnection, QuicEndpoint, TlsConfig};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info};

use crate::acceptor::handle_stream;
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

    /// Return a configured [`QuicServer`]. The actual socket bind happens in
    /// [`serve`](QuicServer::serve) or [`serve_with_incoming`](QuicServer::serve_with_incoming).
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
}

impl QuicServer {
    /// Return a builder to configure the server.
    pub fn builder() -> QuicServerBuilder {
        QuicServerBuilder::default()
    }

    /// Bind to `addr` and serve requests until a shutdown signal is received.
    pub async fn serve<S, B>(self, addr: SocketAddr, service: S) -> Result<(), ServerError>
    where
        S: tower::Service<http::Request<tonic::body::BoxBody>, Response = http::Response<B>>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        self.serve_with_shutdown(addr, service, std::future::pending())
            .await
    }

    /// Bind to `addr` and serve requests until the `signal` future completes.
    pub async fn serve_with_shutdown<S, B, F>(
        self,
        addr: SocketAddr,
        service: S,
        signal: F,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<http::Request<tonic::body::BoxBody>, Response = http::Response<B>>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
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
    pub async fn serve_with_incoming<S, B>(
        self,
        endpoint: QuicEndpoint,
        service: S,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<http::Request<tonic::body::BoxBody>, Response = http::Response<B>>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    {
        self.serve_with_incoming_shutdown(endpoint, service, std::future::pending())
            .await
    }

    /// Serve requests over an already-bound `QuicEndpoint` until the `signal` future completes.
    #[tracing::instrument(skip(self, endpoint, service, signal))]
    pub async fn serve_with_incoming_shutdown<S, B, F>(
        self,
        endpoint: QuicEndpoint,
        service: S,
        signal: F,
    ) -> Result<(), ServerError>
    where
        S: tower::Service<http::Request<tonic::body::BoxBody>, Response = http::Response<B>>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        B: http_body::Body + Send + 'static,
        B::Data: Send,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
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

        loop {
            tokio::select! {
                _ = &mut signal => {
                    info!("shutdown signal received, closing server");
                    endpoint.close(0, b"shutdown");
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
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(conn, service, sem).await {
                            error!(error = %e, "connection handling error");
                        }
                    });
                }
            }
        }

        Ok(())
    }
}

#[tracing::instrument(skip(conn, service, semaphore))]
async fn handle_connection<S, B>(
    conn: QuicConnection,
    service: S,
    semaphore: Arc<Semaphore>,
) -> Result<(), ServerError>
where
    S: tower::Service<http::Request<tonic::body::BoxBody>, Response = http::Response<B>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    loop {
        let stream_res = match conn.accept_bi().await {
            Some(res) => res,
            None => break,
        };
        let (send, recv) = stream_res?;

        // Try to acquire a permit — if all slots are busy, drop the stream.
        let permit = match semaphore.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                error!("server overloaded — dropping stream");
                continue;
            }
        };

        let service = service.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = handle_stream(send, recv, service).await {
                error!(error = %e, "stream handling error");
            }
        });
    }
    Ok(())
}
