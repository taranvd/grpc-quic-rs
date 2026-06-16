use grpc_quic::client::QuicChannel;
use grpc_quic::transport::TlsConfig;
use tokio_stream::StreamExt;

pub mod pb {
    tonic::include_proto!("streaming");
}

use pb::streaming_service_client::StreamingServiceClient;
use pb::HelloRequest;

/// When running against a server with a proper CA-signed certificate, use
/// `TlsConfig::client_default()` which validates against webpki roots.
/// For the development example (self-signed cert), we use an insecure client
/// that accepts any certificate.
fn client_tls() -> TlsConfig {
    TlsConfig::client_insecure()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let tls = client_tls();
    let addr = "127.0.0.1:50051";

    println!("Connecting to gRPC-over-QUIC server at {}...", addr);
    let channel = QuicChannel::builder().tls(tls).connect(addr).await?;

    let mut client = StreamingServiceClient::new(channel);

    // 1. Unary
    println!("\n--- 1. Unary Call ---");
    let response = client
        .say_hello(HelloRequest {
            name: "Alice".to_string(),
        })
        .await?;
    println!("Response: {}", response.into_inner().message);

    // 2. Client Streaming
    println!("\n--- 2. Client Streaming Call ---");
    let requests = vec![
        HelloRequest {
            name: "Bob".to_string(),
        },
        HelloRequest {
            name: "Charlie".to_string(),
        },
        HelloRequest {
            name: "David".to_string(),
        },
    ];
    let request_stream = tokio_stream::iter(requests);
    let response = client.lots_of_requests(request_stream).await?;
    println!("Response: {}", response.into_inner().message);

    // 3. Server Streaming
    println!("\n--- 3. Server Streaming Call ---");
    let mut response_stream = client
        .lots_of_replies(HelloRequest {
            name: "Eve".to_string(),
        })
        .await?
        .into_inner();
    while let Some(res) = response_stream.next().await {
        let res = res?;
        println!("Received reply: {}", res.message);
    }

    // 4. Bidirectional Streaming
    println!("\n--- 4. Bidirectional Streaming Call ---");
    let bidi_requests = vec![
        HelloRequest {
            name: "Frank".to_string(),
        },
        HelloRequest {
            name: "Grace".to_string(),
        },
        HelloRequest {
            name: "Heidi".to_string(),
        },
    ];
    let request_stream = tokio_stream::iter(bidi_requests);
    let mut response_stream = client.bidi_hello(request_stream).await?.into_inner();
    while let Some(res) = response_stream.next().await {
        let res = res?;
        println!("Received bidi reply: {}", res.message);
    }

    Ok(())
}
