use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tower::service_fn;
use http::{Request, Response};
use bytes::Bytes;
use http_body::Body;

use grpc_quic_transport::{QuicEndpoint, TlsConfig};
use crate::QuicServer;

fn make_tls_configs() -> (TlsConfig, TlsConfig) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();
    
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.key_pair.serialize_der();
    
    let server_cert = rustls::pki_types::CertificateDer::from(cert_der);
    let server_key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(key_der));
    
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    
    let mut server_crypto = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![server_cert.clone()], server_key)
        .unwrap();
    server_crypto.alpn_protocols = vec![b"grpc-quic".to_vec()];
    server_crypto.max_early_data_size = u32::MAX;
    
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(server_cert).unwrap();
    
    let mut client_crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"grpc-quic".to_vec()];
    
    (TlsConfig::server(server_crypto), TlsConfig::client(client_crypto))
}

struct TestResponseBody {
    data: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
}

impl http_body::Body for TestResponseBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
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
    
    // Create the mock service
    let service = service_fn(|req: Request<tonic::body::BoxBody>| async move {
        assert_eq!(req.uri().path(), "/helloworld.Greeter/SayHello");
        assert_eq!(req.headers().get("content-type").unwrap(), "application/grpc");
        
        // Read request body to verify it
        let mut body = req.into_body();
        let frame_res = futures::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
        let frame = frame_res.unwrap().unwrap();
        let data = frame.into_data().unwrap();
        assert_eq!(data.as_ref(), b"hello grpc-quic");
        
        // Respond with data + trailers
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        trailers.insert("grpc-message", http::HeaderValue::from_static("Success"));
        
        let body = TestResponseBody {
            data: Some(Bytes::from_static(b"response bytes")),
            trailers: Some(trailers),
        };
        
        Ok::<_, std::convert::Infallible>(Response::new(body))
    });
    
    // Start server in background
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = QuicServer::builder()
        .tls(server_tls)
        .build();
        
    // Bind server endpoint so we can get its local port
    let endpoint = QuicEndpoint::server(server_addr, server.tls.clone().unwrap()).unwrap();
    let bound_addr = endpoint.local_addr().unwrap();
    
    // Re-use connection/accept loop by spawning the serve_with_incoming_shutdown with a signal
    let server_handle = tokio::spawn(async move {
        let signal = async move {
            shutdown_rx.await.ok();
        };
        server.serve_with_incoming_shutdown(endpoint, service, signal).await.unwrap();
    });
    
    // Connect client
    let client_endpoint = QuicEndpoint::client(client_tls).unwrap();
    let conn = client_endpoint.connect(bound_addr, "localhost").await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    
    // Write request envelope
    let path = "/helloworld.Greeter/SayHello";
    let path_len = path.len() as u16;
    let payload = b"hello grpc-quic";
    
    let mut req_buf = Vec::new();
    req_buf.extend_from_slice(&path_len.to_be_bytes());
    req_buf.extend_from_slice(path.as_bytes());
    req_buf.extend_from_slice(payload);
    
    send.write_all(&req_buf).await.unwrap();
    send.finish().unwrap();
    
    // Read response
    // Response envelope is: [response data][u32 grpc_status BE][u16 msg_len BE][msg_bytes]
    // In our case, the response is exactly "response bytes" followed by the status block
    let mut resp_buf = vec![0u8; 14];
    recv.read_exact(&mut resp_buf).await.unwrap();
    assert_eq!(&resp_buf, b"response bytes");
    
    // Read status (4 bytes)
    let mut status_buf = [0u8; 4];
    recv.read_exact(&mut status_buf).await.unwrap();
    let status = u32::from_be_bytes(status_buf);
    assert_eq!(status, 0);
    
    // Read msg_len (2 bytes)
    let mut msg_len_buf = [0u8; 2];
    recv.read_exact(&mut msg_len_buf).await.unwrap();
    let msg_len = u16::from_be_bytes(msg_len_buf) as usize;
    assert_eq!(msg_len, 7);
    
    // Read msg_bytes
    let mut msg_buf = vec![0u8; msg_len];
    recv.read_exact(&mut msg_buf).await.unwrap();
    assert_eq!(&msg_buf, b"Success");
    
    // Clean shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}

#[tokio::test]
async fn test_channel_end_to_end() {
    let (server_tls, client_tls) = make_tls_configs();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    
    // Create the mock service
    let service = service_fn(|req: Request<tonic::body::BoxBody>| async move {
        assert_eq!(req.uri().path(), "/helloworld.Greeter/SayHello");
        
        // Read request body
        let mut body = req.into_body();
        let frame_res = futures::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)).await;
        let frame = frame_res.unwrap().unwrap();
        let data = frame.into_data().unwrap();
        assert_eq!(data.len(), 5 + 13);
        assert_eq!(&data[5..], b"hello channel");
        
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
        trailers.insert("grpc-message", http::HeaderValue::from_static("Success"));
        
        let payload = b"channel response";
        let mut resp_bytes = vec![0u8; 5];
        resp_bytes[4] = payload.len() as u8;
        resp_bytes.extend_from_slice(payload);
        
        let body = TestResponseBody {
            data: Some(Bytes::from(resp_bytes)),
            trailers: Some(trailers),
        };
        
        Ok::<_, std::convert::Infallible>(Response::new(body))
    });
    
    // Start server in background
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = QuicServer::builder()
        .tls(server_tls)
        .build();
        
    let endpoint = QuicEndpoint::server(server_addr, server.tls.clone().unwrap()).unwrap();
    let bound_addr = endpoint.local_addr().unwrap();
    
    let server_handle = tokio::spawn(async move {
        let signal = async move {
            shutdown_rx.await.ok();
        };
        server.serve_with_incoming_shutdown(endpoint, service, signal).await.unwrap();
    });
    
    // Create QuicChannel
    use grpc_quic_client::QuicChannel;
    use tower::Service;
    use std::pin::Pin;
    
    let mut channel = QuicChannel::builder()
        .tls(client_tls)
        .connect(bound_addr.to_string())
        .await
        .unwrap();
        
    // Build http::Request
    let payload = b"hello channel";
    let mut req_bytes = vec![0u8; 5];
    req_bytes[4] = payload.len() as u8;
    req_bytes.extend_from_slice(payload);
    
    let req_body = tonic::body::boxed(TestResponseBody {
        data: Some(Bytes::from(req_bytes)),
        trailers: None,
    });
    let mut request = http::Request::new(req_body);
    *request.uri_mut() = "/helloworld.Greeter/SayHello".parse().unwrap();
    
    // Call channel
    let response = channel.call(request).await.unwrap();
    
    // Parse response body
    let mut resp_body = response.into_body();
    
    // Read data frame
    let frame_res = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx)).await;
    let frame = frame_res.unwrap().unwrap();
    let data = frame.into_data().unwrap();
    assert_eq!(data.len(), 5 + 16);
    assert_eq!(&data[5..], b"channel response");
    
    // Read trailers frame
    let frame_res = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx)).await;
    let frame = frame_res.unwrap().unwrap();
    let trailers = frame.into_trailers().unwrap();
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");
    assert_eq!(trailers.get("grpc-message").unwrap(), "Success");
    
    // Read EOF
    let frame_res = futures::future::poll_fn(|cx| Pin::new(&mut resp_body).poll_frame(cx)).await;
    assert!(frame_res.is_none());
    
    // Clean shutdown
    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap();
}
