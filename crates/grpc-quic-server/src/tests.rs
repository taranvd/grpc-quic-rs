use bytes::Bytes;
use http::{Request, Response};
use http_body::Body;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::sync::oneshot;
use tower::Service;

use crate::QuicServer;
use grpc_quic_client::QuicChannel;
use grpc_quic_core::body::ClientRecvBody;
use grpc_quic_core::client::H3ClientSession;
use grpc_quic_transport::{QuicEndpoint, TlsConfig};

fn make_tls_configs() -> (TlsConfig, TlsConfig) {
    let server = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"]).unwrap();
    let client = TlsConfig::client_insecure();
    (server, client)
}

struct TestBody {
    data: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
}

impl http_body::Body for TestBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            return std::task::Poll::Ready(Some(Ok(http_body::Frame::data(data))));
        }
        if let Some(trailers) = self.trailers.take() {
            return std::task::Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        std::task::Poll::Ready(None)
    }
}

#[tokio::test]
async fn test_server_serve_and_dispatch() {
    let (server_tls, client_tls) = make_tls_configs();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let service = tower::service_fn(|req: Request<tonic::body::BoxBody>| async move {
        assert_eq!(req.uri().path(), "/helloworld.Greeter/SayHello");
        assert_eq!(
            req.headers().get("content-type").unwrap(),
            "application/grpc"
        );

        let mut body = req.into_body();
        let frame = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        let data = frame.into_data().unwrap();
        assert_eq!(data.as_ref(), b"hello grpc-quic");

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));

        let body = TestBody {
            data: Some(Bytes::from_static(b"response bytes")),
            trailers: Some(trailers),
        };

        Ok::<_, std::convert::Infallible>(Response::new(tonic::body::boxed(body)))
    });

    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = QuicServer::builder().tls(server_tls).build();

    let endpoint = QuicEndpoint::server(server_addr, server.tls.clone().unwrap()).unwrap();
    let bound_addr = endpoint.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let signal = async move {
            shutdown_rx.await.ok();
        };
        server
            .serve_with_incoming_shutdown(endpoint, service, signal)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let quic_conn = {
        let client_endpoint = QuicEndpoint::client(client_tls).unwrap();
        client_endpoint
            .connect(bound_addr, "localhost")
            .await
            .unwrap()
    };

    let h3_session = H3ClientSession::new(quic_conn.get_ref().clone())
        .await
        .unwrap();

    let req = http::Request::builder()
        .method("POST")
        .uri(format!(
            "https://localhost:{}/helloworld.Greeter/SayHello",
            bound_addr.port()
        ))
        .header("content-type", "application/grpc")
        .body(())
        .unwrap();

    let mut stream = h3_session.send_request(req).await.unwrap();

    stream
        .send_data(Bytes::from_static(b"hello grpc-quic"))
        .await
        .unwrap();
    stream.finish().await.unwrap();

    let resp = stream.recv_response().await.unwrap();
    assert_eq!(resp.status(), 200);

    let (_send_resp, recv_resp) = stream.split();
    let mut resp_body = ClientRecvBody::new(recv_resp);

    let frame = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let data = frame.into_data().unwrap();
    assert_eq!(&data[..], b"response bytes");

    let frame = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_channel_end_to_end() {
    let (server_tls, client_tls) = make_tls_configs();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let service = tower::service_fn(|req: Request<tonic::body::BoxBody>| async move {
        assert_eq!(req.uri().path(), "/helloworld.Greeter/SayHello");

        let mut body = req.into_body();
        let frame = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        let data = frame.into_data().unwrap();
        assert_eq!(&data[..], b"hello channel");

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));

        let body = TestBody {
            data: Some(Bytes::from_static(b"channel response")),
            trailers: Some(trailers),
        };

        Ok::<_, std::convert::Infallible>(Response::new(tonic::body::boxed(body)))
    });

    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = QuicServer::builder().tls(server_tls).build();

    let endpoint = QuicEndpoint::server(server_addr, server.tls.clone().unwrap()).unwrap();
    let bound_addr = endpoint.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let signal = async move {
            shutdown_rx.await.ok();
        };
        server
            .serve_with_incoming_shutdown(endpoint, service, signal)
            .await
            .unwrap();
    });

    let mut channel = QuicChannel::builder()
        .tls(client_tls)
        .connect(bound_addr.to_string())
        .await
        .unwrap();

    let body = TestBody {
        data: Some(Bytes::from_static(b"hello channel")),
        trailers: None,
    };
    let mut request = Request::new(tonic::body::boxed(body));
    *request.uri_mut() = "/helloworld.Greeter/SayHello".parse().unwrap();

    let response = channel.call(request).await.unwrap();

    let mut resp_body = response.into_body();

    let frame = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let data = frame.into_data().unwrap();
    assert_eq!(&data[..], b"channel response");

    let frame = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_large_payload() {
    let (server_tls, client_tls) = make_tls_configs();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let service = tower::service_fn(|req: Request<tonic::body::BoxBody>| async move {
        let mut body = req.into_body();
        let mut received = Vec::new();
        loop {
            let frame = futures::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            match frame {
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        received.extend_from_slice(&data);
                    }
                }
                Some(Err(_)) => break,
                None => break,
            }
        }

        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));

        let body = TestBody {
            data: Some(Bytes::from(received)),
            trailers: Some(trailers),
        };

        Ok::<_, std::convert::Infallible>(Response::new(tonic::body::boxed(body)))
    });

    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = QuicServer::builder().tls(server_tls).build();

    let endpoint = QuicEndpoint::server(server_addr, server.tls.clone().unwrap()).unwrap();
    let bound_addr = endpoint.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let signal = async move {
            shutdown_rx.await.ok();
        };
        server
            .serve_with_incoming_shutdown(endpoint, service, signal)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut channel = QuicChannel::builder()
        .tls(client_tls)
        .connect(bound_addr.to_string())
        .await
        .unwrap();

    let payload = vec![0xABu8; 512 * 1024]; // 512 KB
    let body = TestBody {
        data: Some(Bytes::from(payload.clone())),
        trailers: None,
    };
    let mut request = Request::new(tonic::body::boxed(body));
    *request.uri_mut() = "/helloworld.Greeter/SayHello".parse().unwrap();

    let response = channel.call(request).await.unwrap();

    let mut resp_body = response.into_body();
    let mut result = Vec::new();
    loop {
        let frame = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx)).await;
        match frame {
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    result.extend_from_slice(&data);
                }
            }
            Some(Err(_)) => break,
            None => break,
        }
    }

    assert_eq!(result.len(), payload.len());
    assert_eq!(result, payload);

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
