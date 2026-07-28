use grpc_quic::transport::TlsConfig;
use grpc_quic_examples::pb::streaming_service_client::StreamingServiceClient;
use grpc_quic_examples::pb::streaming_service_server::{StreamingService, StreamingServiceServer};
use grpc_quic_examples::pb::{HelloRequest, HelloResponse};
use std::net::SocketAddr;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct MyStreamingService;

#[tonic::async_trait]
impl StreamingService for MyStreamingService {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        let auth = request.metadata().get("authorization");
        if auth.is_some() && auth.unwrap() == "Bearer token" {
            // Check custom metadata
            let custom = request.metadata().get("custom-id");
            if custom.is_some() && custom.unwrap() == "12345" {
                let req = request.into_inner();
                return Ok(Response::new(HelloResponse {
                    message: format!("Hello, {}! (Unary)", req.name),
                }));
            }
        }
        
        let req = request.into_inner();
        
        if req.name == "early_error" {
            return Err(Status::unauthenticated("Early Auth Error"));
        }
        
        if req.name == "custom_trailers" {
            let mut response = Response::new(HelloResponse {
                message: format!("Hello, {}! (Unary)", req.name),
            });
            response.metadata_mut().insert("custom-trailer", "value123".parse().unwrap());
            response.metadata_mut().insert_bin("trace-bin", tonic::metadata::MetadataValue::from_bytes(b"\x00\x01\x02"));
            return Ok(response);
        }

        Ok(Response::new(HelloResponse {
            message: format!("Hello, {}! (Unary)", req.name),
        }))
    }

    async fn lots_of_requests(
        &self,
        request: Request<tonic::Streaming<HelloRequest>>,
    ) -> Result<Response<HelloResponse>, Status> {
        let mut stream = request.into_inner();
        let mut names = Vec::new();
        while let Some(req) = stream.next().await {
            let req = req?;
            names.push(req.name);
        }
        Ok(Response::new(HelloResponse {
            message: format!(
                "Hello to all of you: {}! (Client Streaming)",
                names.join(", ")
            ),
        }))
    }

    type LotsOfRepliesStream = Pin<Box<dyn Stream<Item = Result<HelloResponse, Status>> + Send>>;

    async fn lots_of_replies(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<Self::LotsOfRepliesStream>, Status> {
        let req = request.into_inner();
        let name = req.name;

        let output_stream = async_stream::try_stream! {
            if name.starts_with("sleep") {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            for i in 1..=5 {
                yield HelloResponse {
                    message: format!("Reply #{} for {} (Server Streaming)", i, name),
                };
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }

    type BidiHelloStream = Pin<Box<dyn Stream<Item = Result<HelloResponse, Status>> + Send>>;

    async fn bidi_hello(
        &self,
        request: Request<tonic::Streaming<HelloRequest>>,
    ) -> Result<Response<Self::BidiHelloStream>, Status> {
        if let Some(auth) = request.metadata().get("authorization") {
            if auth == "early_error" {
                return Err(Status::unauthenticated("Early Auth Error"));
            }
        }
    
        let mut in_stream = request.into_inner();

        let output_stream = async_stream::try_stream! {
            while let Some(req) = in_stream.next().await {
                let req = req?;
                if req.name == "sleep_1s" {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                yield HelloResponse {
                    message: format!("Hello, {}! (Bidi Streaming)", req.name),
                };
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }
}

async fn start_server() -> SocketAddr {
    let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"]).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = MyStreamingService;
    let server = grpc_quic::server::QuicServer::builder().tls(tls.clone()).build();
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    tokio::spawn(async move {
        let endpoint = grpc_quic::transport::QuicEndpoint::server(addr, tls.clone()).unwrap();
        let bound_addr = endpoint.local_addr().unwrap();
        tx.send(bound_addr).unwrap();
        server.serve_with_incoming_shutdown(endpoint, StreamingServiceServer::new(service), async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }).await.unwrap();
    });
    
    rx.await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unary_metadata() {
    let addr = start_server().await;
    let tls = TlsConfig::client_insecure();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(tls)
        .connect(addr.to_string())
        .await
        .unwrap();

    let mut client = StreamingServiceClient::new(channel);
    
    let mut req = Request::new(HelloRequest {
        name: "Test".into(),
    });
    req.metadata_mut().insert("authorization", "Bearer token".parse().unwrap());
    req.metadata_mut().insert("custom-id", "12345".parse().unwrap());

    let res = client.say_hello(req).await.unwrap();
    assert_eq!(res.into_inner().message, "Hello, Test! (Unary)");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_client_streaming() {
    let addr = start_server().await;
    let tls = TlsConfig::client_insecure();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(tls)
        .connect(addr.to_string())
        .await
        .unwrap();

    let mut client = StreamingServiceClient::new(channel);
    
    let stream = async_stream::stream! {
        yield HelloRequest { name: "A".into() };
        yield HelloRequest { name: "B".into() };
    };

    let res = client.lots_of_requests(Request::new(stream)).await.unwrap();
    assert_eq!(res.into_inner().message, "Hello to all of you: A, B! (Client Streaming)");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bidi_streaming() {
    let addr = start_server().await;
    let tls = TlsConfig::client_insecure();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(tls)
        .connect(addr.to_string())
        .await
        .unwrap();

    let mut client = StreamingServiceClient::new(channel);
    
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    tx.send(HelloRequest { name: "1".into() }).await.unwrap();
    
    let stream = async_stream::stream! {
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    };

    let mut response_stream = client.bidi_hello(Request::new(stream)).await.unwrap().into_inner();
    
    // Server should respond to "1" without waiting for EOF
    let res1 = response_stream.next().await.unwrap().unwrap();
    assert_eq!(res1.message, "Hello, 1! (Bidi Streaming)");
    
    tx.send(HelloRequest { name: "2".into() }).await.unwrap();
    let res2 = response_stream.next().await.unwrap().unwrap();
    assert_eq!(res2.message, "Hello, 2! (Bidi Streaming)");
    
    drop(tx);
    assert!(response_stream.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_rpc() {
    let addr = start_server().await;
    let tls = TlsConfig::client_insecure();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(tls)
        .connect(addr.to_string())
        .await
        .unwrap();

    let client = StreamingServiceClient::new(channel);
    
    let mut join_set = tokio::task::JoinSet::new();
    for i in 0..100 {
        let mut client = client.clone();
        join_set.spawn(async move {
            let req = Request::new(HelloRequest { name: format!("User{}", i) });
            let res = client.say_hello(req).await.unwrap();
            assert_eq!(res.into_inner().message, format!("Hello, User{}! (Unary)", i));
        });
    }
    
    while let Some(res) = join_set.join_next().await {
        res.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_early_response_cancels_request_sender() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    struct DropGuard(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let stream = async_stream::stream! {
        let _guard = DropGuard(Some(tx));
        // Yield one item, then block forever
        yield HelloRequest { name: "test".into() };
        std::future::pending::<()>().await;
    };
    
    let mut req = Request::new(stream);
    req.metadata_mut().insert("authorization", "early_error".parse().unwrap());
    
    let res = client.bidi_hello(req).await;
    let err = res.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(err.message(), "Early Auth Error");
    
    // Check that the request stream task was aborted and the guard dropped
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx).await.expect("DropGuard was not dropped!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_response_trailers_and_binary_metadata() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    let req = Request::new(HelloRequest { name: "custom_trailers".into() });
    let res = client.say_hello(req).await.unwrap();
    
    let metadata = res.metadata();
    assert_eq!(metadata.get("custom-trailer").unwrap(), "value123");
    
    let bin = metadata.get_bin("trace-bin").unwrap();
    assert_eq!(bin.to_bytes().unwrap().as_ref(), b"\x00\x01\x02");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_deadline_exceeded() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    let mut req = Request::new(HelloRequest { name: "sleep_1s".into() });
    req.set_timeout(std::time::Duration::from_millis(50));
    
    let mut response = client.lots_of_replies(req).await.unwrap().into_inner();
    let err = response.next().await.unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connect_failure_wakes_all_waiters() {
    // Port 0 without a server listening -> connect will fail
    let addr = "127.0.0.1:40000";
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .retry_policy(grpc_quic::client::RetryPolicy { max_attempts: 1, ..Default::default() })
        .connect(addr.to_string())
        .await
        .unwrap();
    let client = StreamingServiceClient::new(channel);
    
    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let mut c = client.clone();
        join_set.spawn(async move {
            c.say_hello(Request::new(HelloRequest { name: "A".into() })).await
        });
    }
    
    while let Some(res) = join_set.join_next().await {
        let err = res.unwrap().unwrap_err();
        // Tonic translates our internal errors into tonic::Status with some code (likely Unknown or Unavailable)
        let msg = err.message().to_lowercase();
        assert!(msg.contains("refused") || msg.contains("closed") || msg.contains("timed out") || msg.contains("timeout"), "unexpected error: {}", msg);
    }
}


#[tokio::test(flavor = "multi_thread")]
async fn test_deadline_aborts_request_sender() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    struct DropGuard(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let stream = async_stream::stream! {
        let _guard = DropGuard(Some(tx));
        yield HelloRequest { name: "sleep_1s".into() };
        // Wait forever
        std::future::pending::<()>().await;
    };
    
    let mut req = Request::new(stream);
    req.set_timeout(std::time::Duration::from_millis(50));
    
    let res = client.bidi_hello(req).await;
    // Bidi stream will return the response stream, and we wait on the stream to get deadline exceeded.
    let mut response = res.unwrap().into_inner();
    let err = response.next().await.unwrap().unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    
    // Check that the request stream task was aborted and the guard dropped
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx).await.expect("DropGuard was not dropped!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_drop_response_cancels_body_forwarder() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    struct DropGuard(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DropGuard {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel();
    let stream = async_stream::stream! {
        let _guard = DropGuard(Some(tx));
        yield HelloRequest { name: "test".into() };
        // Wait forever
        std::future::pending::<()>().await;
    };
    
    let req = Request::new(stream);
    let response = client.bidi_hello(req).await.unwrap().into_inner();
    
    // Drop the response inner body
    drop(response);
    
    // Check that the request stream task was aborted and the guard dropped
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), rx).await.expect("DropGuard was not dropped!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_connect_owner_cancellation_does_not_deadlock() {
    let addr = start_server().await;
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(addr.to_string())
        .await
        .unwrap();
    let client = StreamingServiceClient::new(channel);
    
    let mut join_set = tokio::task::JoinSet::new();
    for i in 0..10 {
        let mut c = client.clone();
        join_set.spawn(async move {
            let req = Request::new(HelloRequest { name: format!("User{}", i) });
            if i == 0 {
                // Cancel the first caller extremely quickly to simulate caller cancellation
                let _ = tokio::time::timeout(std::time::Duration::from_nanos(1), c.say_hello(req)).await;
                Ok::<(), tonic::Status>(())
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let res = c.say_hello(req).await.unwrap();
                assert_eq!(res.into_inner().message, format!("Hello, User{}! (Unary)", i));
                Ok::<(), tonic::Status>(())
            }
        });
    }
    
    while let Some(res) = join_set.join_next().await {
        res.unwrap().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_rejects_new_rpc_on_existing_connection() {
    let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"]).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = MyStreamingService;
    let server = grpc_quic::server::QuicServer::builder().tls(tls.clone()).build();
    let (tx_addr, rx_addr) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    
    let tls_for_server1 = tls.clone();
    tokio::spawn(async move {
        let endpoint = grpc_quic::transport::QuicEndpoint::server(addr, tls_for_server1.clone()).unwrap();
        let bound_addr = endpoint.local_addr().unwrap();
        tx_addr.send(bound_addr).unwrap();
        
        let signal = async move {
            let _ = shutdown_rx.await;
        };
        server.serve_with_incoming_shutdown(endpoint, StreamingServiceServer::new(service), signal).await.unwrap();
    });
    
    let server_addr = rx_addr.await.unwrap();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(server_addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    // 1. Make an active RPC
    let (tx_msg, mut rx_msg) = tokio::sync::mpsc::channel(2);
    let stream = async_stream::stream! {
        while let Some(msg) = rx_msg.recv().await {
            yield msg;
        }
    };
    
    let req = Request::new(stream);
    let mut active_response = client.bidi_hello(req).await.unwrap().into_inner();
    
    // Prove it works
    tx_msg.send(HelloRequest { name: "1".into() }).await.unwrap();
    let res1 = active_response.next().await.unwrap().unwrap();
    assert_eq!(res1.message, "Hello, 1! (Bidi Streaming)");
    
    // 2. Trigger Shutdown
    shutdown_tx.send(()).unwrap();
    // Give the server time to process shutdown
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    
    // 3. Make a NEW RPC. It should fail!
    let req2 = Request::new(HelloRequest { name: "2".into() });
    let result = client.say_hello(req2).await;
    assert!(result.is_err(), "New RPC should be rejected during graceful shutdown");
    
    // 4. Prove active RPC is STILL working!
    tx_msg.send(HelloRequest { name: "3".into() }).await.unwrap();
    let res3 = active_response.next().await.unwrap().unwrap();
    assert_eq!(res3.message, "Hello, 3! (Bidi Streaming)");
    
    drop(tx_msg);
    assert!(active_response.next().await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reconnect_after_closed_connection() {
    let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"]).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = MyStreamingService;
    let server = grpc_quic::server::QuicServer::builder()
        .tls(tls.clone())
        .graceful_timeout(std::time::Duration::from_millis(10))
        .build();
    let (tx_addr, rx_addr) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    
    let tls_for_server1 = tls.clone();
    tokio::spawn(async move {
        let endpoint = grpc_quic::transport::QuicEndpoint::server(addr, tls_for_server1.clone()).unwrap();
        let bound_addr = endpoint.local_addr().unwrap();
        tx_addr.send(bound_addr).unwrap();
        
        let signal = async move {
            let _ = shutdown_rx.await;
        };
        server.serve_with_incoming_shutdown(endpoint, StreamingServiceServer::new(service), signal).await.unwrap();
    });
    
    let server_addr = rx_addr.await.unwrap();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(server_addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel.clone());
    
    // 1. First RPC
    let req = Request::new(HelloRequest { name: "1".into() });
    let res = client.say_hello(req).await.unwrap();
    assert_eq!(res.into_inner().message, "Hello, 1! (Unary)");
    
    // 2. Shut down the server, causing it to close the connection
    shutdown_tx.send(()).unwrap();
    // Wait for the server task to finish and close the endpoint
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Start a NEW server on the exact same port!
    let server2 = grpc_quic::server::QuicServer::builder().tls(tls.clone()).build();
    tokio::spawn(async move {
        // Retry a few times in case OS hasn't released UDP port yet
        let mut endpoint = None;
        for _ in 0..5 {
            if let Ok(ep) = grpc_quic::transport::QuicEndpoint::server(server_addr, tls.clone()) {
                endpoint = Some(ep);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let endpoint = endpoint.expect("Failed to bind new server to same port");
        server2.serve_with_incoming_shutdown(endpoint, StreamingServiceServer::new(MyStreamingService), std::future::pending()).await.unwrap();
    });
    // Wait for server to bind
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    
    // 3. New RPC
    // The client pool still has the old closed connection cached!
    // But since it's closed, `send_request` error will drop it and reconnect!
    let req = Request::new(HelloRequest { name: "2".into() });
    let mut client = StreamingServiceClient::new(channel);
    let res = client.say_hello(req).await.unwrap();
    assert_eq!(res.into_inner().message, "Hello, 2! (Unary)");
}


#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_force_closes_after_grace_timeout() {
    let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"]).unwrap();
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let service = MyStreamingService;
    // VERY SHORT GRACE PERIOD (50ms)
    let server = grpc_quic::server::QuicServer::builder()
        .tls(tls.clone())
        .graceful_timeout(std::time::Duration::from_millis(50))
        .build();
    
    let (tx_addr, rx_addr) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    
    tokio::spawn(async move {
        let endpoint = grpc_quic::transport::QuicEndpoint::server(addr, tls.clone()).unwrap();
        let bound_addr = endpoint.local_addr().unwrap();
        tx_addr.send(bound_addr).unwrap();
        
        let signal = async move {
            let _ = shutdown_rx.await;
        };
        server.serve_with_incoming_shutdown(endpoint, StreamingServiceServer::new(service), signal).await.unwrap();
    });
    
    let server_addr = rx_addr.await.unwrap();
    let channel = grpc_quic::client::QuicChannel::builder()
        .tls(TlsConfig::client_insecure())
        .connect(server_addr.to_string())
        .await
        .unwrap();
    let mut client = StreamingServiceClient::new(channel);
    
    let req = Request::new(HelloRequest { name: "sleep_1s".into() });
    
    let mut response = client.lots_of_replies(req).await.unwrap().into_inner();
    
    // Shut down immediately after starting request!
    shutdown_tx.send(()).unwrap();
    
    // Server has 50ms grace period, but the stream takes 500ms to yield the first response!
    // So the server will hit the graceful timeout and forcefully close the connection!
    let err = response.next().await.unwrap().unwrap_err();
    
    println!("Forced closed error: {:?}", err);
    assert_eq!(err.code(), tonic::Code::Unknown);
}
