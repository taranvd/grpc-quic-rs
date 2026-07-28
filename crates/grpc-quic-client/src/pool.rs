use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{oneshot, Mutex};
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

enum Slot {
    Connecting(Vec<oneshot::Sender<Result<PoolEntry, ClientError>>>),
    Ready(PoolEntry),
}

#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<Mutex<HashMap<SocketAddr, Slot>>>,
}

impl std::fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPool").finish()
    }
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
        F: FnOnce(SocketAddr) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<QuicConnection, ClientError>> + Send + 'static,
    {
        let rx = {
            let mut map = self.inner.lock().await;
            if let Some(slot) = map.get_mut(&addr) {
                match slot {
                    Slot::Ready(entry) => {
                        if !entry.quic.is_closed() {
                            debug!(remote = %addr, "reusing existing QUIC connection + h3 session");
                            return Ok(entry.clone());
                        }
                        debug!(remote = %addr, "cached connection is closed, will reconnect");
                    }
                    Slot::Connecting(waiters) => {
                        let (tx, rx) = oneshot::channel();
                        waiters.push(tx);
                        drop(map);
                        return rx.await.unwrap_or_else(|_| {
                            Err(ClientError::StreamIo(std::io::Error::other(
                                "connection task cancelled",
                            )))
                        });
                    }
                }
            }

            let (tx, rx) = oneshot::channel();
            map.insert(addr, Slot::Connecting(vec![tx]));

            let pool_inner = self.inner.clone();
            tokio::spawn(async move {
                let quic_res = connect_fn(addr).await;
                let res = match quic_res {
                    Ok(quic) => match H3ClientSession::new(quic.get_ref().clone()).await {
                        Ok(h3) => {
                            record_connection("client");
                            debug!(remote = %addr, "established new QUIC connection + h3 session");
                            Ok(PoolEntry { quic, h3 })
                        }
                        Err(e) => Err(ClientError::StreamIo(std::io::Error::other(e.to_string()))),
                    },
                    Err(e) => Err(e),
                };

                let mut map = pool_inner.lock().await;
                if let Some(Slot::Connecting(waiters)) = map.remove(&addr) {
                    if let Ok(entry) = &res {
                        map.insert(addr, Slot::Ready(entry.clone()));
                    }
                    for tx in waiters {
                        let send_res = match &res {
                            Ok(entry) => Ok(entry.clone()),
                            Err(e) => {
                                Err(ClientError::StreamIo(std::io::Error::other(e.to_string())))
                            }
                        };
                        let _ = tx.send(send_res);
                    }
                }
            });
            rx
        };

        rx.await.unwrap_or_else(|_| {
            Err(ClientError::StreamIo(std::io::Error::other(
                "connection task cancelled",
            )))
        })
    }

    pub async fn invalidate(&self, addr: &SocketAddr) {
        let mut map = self.inner.lock().await;
        if let Some(Slot::Ready(entry)) = map.get(addr) {
            if entry.quic.is_closed() {
                map.remove(addr);
            }
        }
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}
