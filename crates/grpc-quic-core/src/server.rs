use bytes::Bytes;
use h3::server::builder as h3_server_builder;
use h3::server::Connection as H3Connection;
use h3_quinn::Connection as H3QuinnConnection;

use crate::error::CoreError;

pub type H3ServerConn = H3Connection<H3QuinnConnection, Bytes>;

pub async fn build_server_conn(conn: quinn::Connection) -> Result<H3ServerConn, CoreError> {
    let h3_conn = h3_server_builder()
        .build(H3QuinnConnection::new(conn))
        .await
        .map_err(|e| CoreError::H3Connection(e.to_string()))?;
    Ok(h3_conn)
}
