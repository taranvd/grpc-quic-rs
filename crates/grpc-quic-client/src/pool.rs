//! Connection pool for QUIC connections.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;
use tracing::debug;

use grpc_quic_metrics::record_connection;
use grpc_quic_transport::QuicConnection;

use crate::error::ClientError;

/// A pool of QUIC connections keyed by remote [`SocketAddr`].
///
/// The pool returns an existing live connection if one is available, otherwise
/// it establishes a new one. Connections are validated lazily on use.
#[derive(Clone, Debug)]
pub struct ConnectionPool {
    inner: Arc<Mutex<HashMap<SocketAddr, QuicConnection>>>,
}

impl ConnectionPool {
    /// Create a new empty pool.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Return a connection to `addr`, creating one via `connect_fn` if needed.
    pub async fn get_or_connect<F, Fut>(
        &self,
        addr: SocketAddr,
        connect_fn: F,
    ) -> Result<QuicConnection, ClientError>
    where
        F: FnOnce(SocketAddr) -> Fut,
        Fut: std::future::Future<Output = Result<QuicConnection, ClientError>>,
    {
        let mut map = self.inner.lock().await;
        if let Some(conn) = map.get(&addr) {
            debug!(remote = %addr, "reusing existing QUIC connection");
            return Ok(conn.clone());
        }
        let conn = connect_fn(addr).await?;
        record_connection("client");
        debug!(remote = %addr, "established new QUIC connection");
        map.insert(addr, conn.clone());
        Ok(conn)
    }

    /// Remove a connection from the pool (called after a connection error).
    pub async fn remove(&self, addr: &SocketAddr) {
        let mut map = self.inner.lock().await;
        map.remove(addr);
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
