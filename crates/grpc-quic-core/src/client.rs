use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::future;
use tokio::sync::Mutex;

use crate::error::CoreError;

pub type H3ClientConnection = h3::client::Connection<h3_quinn::Connection, Bytes>;
pub type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Build a client connection and spawn the driver in the background.
/// Returns the `SendRequest` handle.
pub async fn build_client_conn(conn: quinn::Connection) -> Result<H3SendRequest, CoreError> {
    let (mut h3_conn, send_req) = h3::client::builder()
        .build(h3_quinn::Connection::new(conn))
        .await
        .map_err(|e| CoreError::H3Connection(e.to_string()))?;

    // The driver must be polled in the background to process incoming frames
    // (QPACK, SETTINGS, GOAWAY, server pushes, etc.).
    tokio::spawn(async move {
        let err = future::poll_fn(|cx| h3_conn.poll_close(cx)).await;
        if !err.is_h3_no_error() {
            tracing::error!(error = %err, "h3 client driver error");
        }
    });

    Ok(send_req)
}

#[derive(Clone)]
pub struct H3ClientSession {
    send_req: Arc<Mutex<H3SendRequest>>,
}

impl fmt::Debug for H3ClientSession {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("H3ClientSession").finish()
    }
}

impl H3ClientSession {
    pub async fn new(conn: quinn::Connection) -> Result<Self, CoreError> {
        let send_req = build_client_conn(conn).await?;
        Ok(Self {
            send_req: Arc::new(Mutex::new(send_req)),
        })
    }

    pub async fn send_request(
        &self,
        req: http::Request<()>,
    ) -> Result<h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>, CoreError> {
        let mut guard = self.send_req.lock().await;
        let stream = guard.send_request(req).await.map_err(CoreError::from)?;
        Ok(stream)
    }
}
