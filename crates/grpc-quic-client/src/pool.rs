use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;
use tracing::debug;

use grpc_quic_core::client::H3ClientSession;
use grpc_quic_metrics::record_connection;
use grpc_quic_transport::QuicConnection;

use crate::error::ClientError;

#[derive(Clone, Debug)]
pub struct PoolEntry {
    pub quic: QuicConnection,
    pub h3: H3ClientSession,
}

#[derive(Clone, Debug)]
pub struct ConnectionPool {
    inner: Arc<Mutex<HashMap<SocketAddr, PoolEntry>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_or_connect<F, Fut>(
        &self,
        addr: SocketAddr,
        connect_fn: F,
    ) -> Result<PoolEntry, ClientError>
    where
        F: FnOnce(SocketAddr) -> Fut,
        Fut: std::future::Future<Output = Result<QuicConnection, ClientError>>,
    {
        let mut map = self.inner.lock().await;
        if let Some(entry) = map.get(&addr) {
            if !entry.quic.is_closed() {
                debug!(remote = %addr, "reusing existing QUIC connection + h3 session");
                return Ok(entry.clone());
            }
            debug!(remote = %addr, "cached connection is closed, removing");
            map.remove(&addr);
        }
        let quic = connect_fn(addr).await?;
        let h3 = H3ClientSession::new(quic.get_ref().clone())
            .await
            .map_err(|e| {
                ClientError::StreamIo(std::io::Error::other(e.to_string()))
            })?;
        record_connection("client");
        debug!(remote = %addr, "established new QUIC connection + h3 session");
        let entry = PoolEntry { quic: quic.clone(), h3 };
        map.insert(addr, entry.clone());
        Ok(entry)
    }

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
