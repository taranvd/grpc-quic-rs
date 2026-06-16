use crate::error::ServerError;
use bytes::Bytes;
use grpc_quic_core::body::ServerRecvBody;
use grpc_quic_metrics::{record_bytes_sent, record_request, record_stream};
use http_body::Body;
use tracing::error;

pub async fn handle_request<S>(
    req: http::Request<()>,
    stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
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
    let path = req.uri().path().to_owned();
    record_request("server", &path);
    record_stream("server");

    let (mut send, recv) = stream.split();
    let recv_body = ServerRecvBody::new(recv);
    let box_body = tonic::body::boxed(recv_body);

    let mut request = http::Request::new(box_body);
    *request.method_mut() = req.method().clone();
    *request.uri_mut() = req.uri().clone();
    *request.headers_mut() = req.headers().clone();

    let mut service = service;
    let response = match service.call(request).await {
        Ok(r) => r,
        Err(e) => {
            error!("service call failed: {:?}", e.into());
            if let Ok(resp) = http::Response::builder().status(500).body(()) {
                if let Err(e) = send.send_response(resp).await {
                    error!("failed to send 500 response: {e}");
                }
            }
            return Ok(());
        }
    };

    let (parts, body) = response.into_parts();
    let resp = http::Response::from_parts(parts, ());
    if let Err(e) = send.send_response(resp).await {
        error!("failed to send response headers: {e}");
        return Ok(());
    }

    tokio::pin!(body);
    let mut data_frame_received = false;
    loop {
        let frame = futures::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await;
        match frame {
            Some(Ok(frame)) => match frame.into_data() {
                Ok(data) => {
                    data_frame_received = true;
                    let len = data.len() as u64;
                    if let Err(e) = send.send_data(data).await {
                        error!("failed to send data: {e}");
                        break;
                    }
                    record_bytes_sent("server", len);
                }
                Err(frame) => {
                    if let Ok(trailers) = frame.into_trailers() {
                        if let Err(e) = send.send_trailers(trailers).await {
                            error!("failed to send trailers: {e}");
                        }
                        return Ok(());
                    }
                }
            },
            Some(Err(e)) => {
                error!("response body error: {e}");
                break;
            }
            None => {
                if data_frame_received {
                    if let Err(e) = send.finish().await {
                        error!("failed to finish stream: {e}");
                    }
                }
                return Ok(());
            }
        }
    }

    if let Err(e) = send.finish().await {
        error!("failed to finish stream: {e}");
    }
    Ok(())
}
