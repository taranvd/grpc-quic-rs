//! Stream acceptor — reads a QUIC bi-stream, reconstructs the gRPC request,
//! and dispatches it to the tonic service handler.

use crate::error::ServerError;
use bytes::Bytes;
use grpc_quic_metrics::{record_bytes_received, record_bytes_sent, record_request, record_stream};
use http_body::{Body, Frame};
use quinn::{RecvStream, SendStream};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

/// Request body that reads raw bytes from a QUIC receive stream.
pub struct QuicRequestBody {
    recv: RecvStream,
}

impl QuicRequestBody {
    /// Create a new request body.
    pub fn new(recv: RecvStream) -> Self {
        Self { recv }
    }
}

impl Body for QuicRequestBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut buf = vec![0u8; 8192];
        let mut read_buf = tokio::io::ReadBuf::new(&mut buf);

        match Pin::new(&mut self.recv).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let filled = read_buf.filled();
                if filled.is_empty() {
                    Poll::Ready(None)
                } else {
                    let len = filled.len() as u64;
                    record_bytes_received("server", len);
                    let bytes = Bytes::copy_from_slice(filled);
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Handle a single bi-directional stream.
#[tracing::instrument(skip(send, recv, service))]
pub async fn handle_stream<S, B>(
    mut send: SendStream,
    mut recv: RecvStream,
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
    // 1. Read path length (2 bytes BE)
    let mut path_len_buf = [0u8; 2];
    recv.read_exact(&mut path_len_buf)
        .await
        .map_err(|e| ServerError::InvalidRequest(format!("failed to read path length: {e}")))?;
    let path_len = u16::from_be_bytes(path_len_buf) as usize;

    // 2. Read path (UTF-8 bytes)
    let mut path_bytes = vec![0u8; path_len];
    recv.read_exact(&mut path_bytes)
        .await
        .map_err(|e| ServerError::InvalidRequest(format!("failed to read path: {e}")))?;
    let path = String::from_utf8(path_bytes)
        .map_err(|e| ServerError::InvalidRequest(format!("path is not UTF-8: {e}")))?;

    record_stream("server");
    record_request("server", &path);

    // 3. Build request
    let request_body = QuicRequestBody::new(recv);
    let box_body = tonic::body::boxed(request_body);
    let mut request = http::Request::new(box_body);
    *request.uri_mut() = path
        .parse()
        .map_err(|e| ServerError::InvalidRequest(format!("invalid URI: {e}")))?;
    *request.method_mut() = http::Method::POST;
    request.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::header::HeaderValue::from_static("application/grpc"),
    );

    // 4. Dispatch to tonic service
    let mut service = service;
    futures::future::poll_fn(|cx| service.poll_ready(cx))
        .await
        .map_err(|e| ServerError::InvalidRequest(format!("service not ready: {:?}", e.into())))?;
    let response = service
        .call(request)
        .await
        .map_err(|e| ServerError::InvalidRequest(format!("service call failed: {:?}", e.into())))?;

    // 5. Stream response frames
    let body = response.into_body();
    tokio::pin!(body);
    let mut has_trailers = false;

    while let Some(frame_res) = futures::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = frame_res
            .map_err(|e| ServerError::StreamIo(std::io::Error::other(e.into().to_string())))?;

        match frame.into_data() {
            Ok(mut data) => {
                use bytes::Buf;
                while data.has_remaining() {
                    let chunk = data.chunk();
                    send.write_all(chunk)
                        .await
                        .map_err(|e| ServerError::StreamIo(std::io::Error::other(e.to_string())))?;
                    let len = chunk.len() as u64;
                    record_bytes_sent("server", len);
                    data.advance(len as usize);
                }
            }
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    has_trailers = true;
                    let (status, message) = parse_trailers(&trailers);
                    write_trailers(&mut send, status, &message).await?;
                    break;
                }
            }
        }
    }

    if !has_trailers {
        write_trailers(&mut send, 0, "").await?;
    }

    send.finish()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    Ok(())
}

fn parse_trailers(headers: &http::HeaderMap) -> (u32, String) {
    let status = headers
        .get("grpc-status")
        .and_then(|val| val.to_str().ok())
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);

    let message = headers
        .get("grpc-message")
        .and_then(|val| val.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();

    (status, message)
}

async fn write_trailers(
    send: &mut SendStream,
    status: u32,
    message: &str,
) -> Result<(), ServerError> {
    let msg_bytes = message.as_bytes();
    let msg_len = msg_bytes.len();
    if msg_len > u16::MAX as usize {
        return Err(ServerError::StreamIo(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "grpc-message too long",
        )));
    }

    let mut buf = Vec::with_capacity(4 + 2 + msg_len);
    buf.extend_from_slice(&status.to_be_bytes());
    buf.extend_from_slice(&(msg_len as u16).to_be_bytes());
    buf.extend_from_slice(msg_bytes);

    send.write_all(&buf)
        .await
        .map_err(|e| ServerError::StreamIo(std::io::Error::other(e.to_string())))?;
    Ok(())
}
