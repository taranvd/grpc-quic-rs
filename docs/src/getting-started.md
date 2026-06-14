# Getting Started

## Prerequisites

- Rust 1.75+
- `protoc` (Protocol Buffers compiler) — for building examples

## Add to your project

```toml
[dependencies]
grpc-quic = { git = "https://github.com/taranvd/grpc-quic-rs" }
```

Or use individual crates:

```toml
[dependencies]
grpc-quic-client = { git = "https://github.com/taranvd/grpc-quic-rs" }
grpc-quic-server = { git = "https://github.com/taranvd/grpc-quic-rs" }
```

## Server Example

```rust,ignore
use grpc_quic::server::QuicServer;
use grpc_quic::transport::TlsConfig;

let tls = load_tls_config(); // see examples/ for self-signed cert generation

QuicServer::builder()
    .tls(tls)
    .build()
    .serve("0.0.0.0:50051".parse()?, MyServiceServer::new(my_service))
    .await?;
```

## Client Example

```rust,ignore
use grpc_quic::client::QuicChannel;

let channel = QuicChannel::builder()
    .connect("127.0.0.1:50051")
    .await?;

let mut client = MyServiceClient::new(channel);
let response = client.my_method(request).await?;
```

## Running the Examples

```bash
# Terminal 1: start the server
cargo run -p grpc-quic-examples --bin streaming-server

# Terminal 2: run the client
cargo run -p grpc-quic-examples --bin streaming-client
```

The server generates a self-signed `cert.der` that the client reads automatically.
