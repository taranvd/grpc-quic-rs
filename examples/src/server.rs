use grpc_quic::transport::TlsConfig;
use std::net::SocketAddr;
use std::pin::Pin;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

pub mod pb {
    tonic::include_proto!("streaming");
}

use pb::streaming_service_server::{StreamingService, StreamingServiceServer};
use pb::{HelloRequest, HelloResponse};

#[derive(Debug, Default)]
pub struct MyStreamingService;

#[tonic::async_trait]
impl StreamingService for MyStreamingService {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        let req = request.into_inner();
        println!("Received Unary Request from: {}", req.name);
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
        println!("Received Client Streaming Request...");
        while let Some(req) = stream.next().await {
            let req = req?;
            println!("  Client Stream message: {}", req.name);
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
        println!("Received Server Streaming Request from: {}", name);

        let output_stream = async_stream::try_stream! {
            for i in 1..=5 {
                yield HelloResponse {
                    message: format!("Reply #{} for {} (Server Streaming)", i, name),
                };
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }

    type BidiHelloStream = Pin<Box<dyn Stream<Item = Result<HelloResponse, Status>> + Send>>;

    async fn bidi_hello(
        &self,
        request: Request<tonic::Streaming<HelloRequest>>,
    ) -> Result<Response<Self::BidiHelloStream>, Status> {
        let mut in_stream = request.into_inner();
        println!("Received Bidirectional Streaming Request...");

        let output_stream = async_stream::try_stream! {
            while let Some(req) = in_stream.next().await {
                let req = req?;
                println!("  Bidi Stream message: {}", req.name);
                yield HelloResponse {
                    message: format!("Hello, {}! (Bidi Streaming)", req.name),
                };
            }
        };

        Ok(Response::new(Box::pin(output_stream)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"])?;
    let addr: SocketAddr = "127.0.0.1:50051".parse()?;

    let service = MyStreamingService;
    let server = grpc_quic::server::QuicServer::builder().tls(tls).build();

    println!("Starting gRPC-over-QUIC server on {}", addr);

    server
        .serve(addr, StreamingServiceServer::new(service))
        .await?;

    Ok(())
}
