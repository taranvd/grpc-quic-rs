# grpc-quic-rs

> **gRPC over HTTP/3 for tonic** — enables standards-compliant gRPC transport over HTTP/3 (h3) and QUIC while preserving full gRPC semantics and API compatibility..

[![Rust](https://img.shields.io/badge/rust-1.75+-orange.svg)](https://www.rust-lang.org)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![Docs.rs](https://img.shields.io/badge/docs.rs-grpc_quic-success)](https://docs.rs/grpc-quic)

---

## Motivation

Standard gRPC runs over HTTP/2 over TCP. While HTTP/2 solves head-of-line
blocking at the application layer, TCP still suffers from HOL blocking at the
transport layer. A single lost packet stalls all multiplexed streams.

**QUIC** (RFC 9000) eliminates TCP HOL blocking by giving each stream
independent loss recovery. Combined with TLS 1.3 built into the handshake,
QUIC offers:

- Lower connection establishment latency (0-RTT resumption)
- No transport-level HOL blocking across streams
- Connection migration (survives IP changes, e.g. mobile roaming)
- Built-in encryption — no separate TLS layer

`grpc-quic-rs` gives tonic services all of this with **zero changes to your
protobuf definitions or service implementations**.

---

## Architecture

```mermaid
flowchart TB
    subgraph Application
        S[tonic Service]
        P[protobuf codec]
    end

    subgraph grpc-quic-rs
        C[grpc-quic-client<br/>QuicChannel]
        V[grpc-quic-server<br/>QuicServer]
        K[grpc-quic-core<br/>h3 transport + body]
        T[grpc-quic-transport<br/>QUIC primitives]
        M[grpc-quic-metrics<br/>Prometheus + tracing]
        D[grpc-quic-discovery<br/>Resolver trait]
    end

    subgraph Network
        H[HTTP/3 · h3 + h3-quinn]
        Q[QUIC · quinn · UDP<br/>TLS 1.3 via rustls]
    end

    S --> C
    S --> V
    C --> K
    V --> K
    K --> T
    T --> Q
    C -.-> M
    V -.-> M
    C -.-> D
```

### Key design principle

> **grpc-quic-rs does NOT modify gRPC semantics.**
> It replaces HTTP/2/TCP with HTTP/3/QUIC (h3 + h3-quinn).
> All gRPC payload bytes are forwarded verbatim — never interpreted or re-encoded.

### Crate structure

| Crate | Role |
|---|---|
| `grpc-quic` | Public façade — re-exports everything |
| `grpc-quic-transport` | Raw QUIC primitives (quinn + rustls). No tonic dependency. |
| `grpc-quic-core` | HTTP/3 + gRPC core — h3 connection builders, body adapters, error types |
| `grpc-quic-client` | `QuicChannel` — tonic-compatible `tower::Service` |
| `grpc-quic-server` | `QuicServer` — accepts QUIC connections, delegates to tonic Router |
| `grpc-quic-metrics` | Prometheus counters + tracing spans |
| `grpc-quic-discovery` | `Resolver` trait + `StaticResolver` |

---

## Quick start

> **Cargo.toml:**
> ```toml
> [dependencies]
> grpc-quic = { version = "0.1", features = ["full"] }
> ```

### Server (development — self-signed cert)

```rust
use grpc_quic::{server::QuicServer, transport::TlsConfig};

let tls = TlsConfig::server_self_signed(vec!["localhost", "127.0.0.1"])?;

QuicServer::builder()
    .tls(tls)
    .build()
    .serve("0.0.0.0:50051".parse()?, MyServiceServer::new(service))
    .await?;
```

### Server (production — PEM files)

```rust
let tls = TlsConfig::server_from_pem("cert.pem", "key.pem")?;
```

### Client (development — accepts any cert)

```rust
use grpc_quic::{client::QuicChannel, transport::TlsConfig};

let channel = QuicChannel::builder()
    .tls(TlsConfig::client_insecure())
    .connect("127.0.0.1:50051")
    .await?;

let mut client = MyServiceClient::new(channel);
let response = client.say_hello(Request::new(HelloRequest { name: "world".into() })).await?;
```

### Client (production — webpki roots)

```rust
let channel = QuicChannel::builder()
    .tls(TlsConfig::client_default())
    .connect("api.example.com:50051")
    .await?;
```

---

## Streaming support

All four gRPC streaming modes are supported via HTTP/3 data frames + trailers:

| Mode | Transport |
|---|---|
| Unary | HTTP/3 request/response with trailers (grpc-status, grpc-message) |
| Client Streaming | HTTP/3 request stream, single response |
| Server Streaming | Single request, HTTP/3 response stream |
| Bidirectional | Full-duplex HTTP/3 stream |

---

## Roadmap

- [x] **Phase 1** — Workspace scaffold, CI, justfile
- [x] **Phase 2** — QUIC transport: endpoints, connections, TLS
- [x] **Phase 3** — Server: QUIC acceptor → tonic Router dispatch
- [x] **Phase 4** — Client: QuicChannel + ConnectionPool + RetryPolicy
- [x] **Phase 5** — All streaming modes + examples
- [x] **Phase 6** — Prometheus metrics + tracing spans
- [x] **Phase 7** — Service discovery (Resolver trait + StaticResolver)
- [x] **Phase 8** — mdbook documentation + rustdoc
- [x] **Phase 9** — Criterion benchmarks (QUIC vs TCP/tonic baseline)
- [x] **Phase 10** — Rewrite to HTTP/3 (h3 + h3-quinn), remove custom wire format

---

## Development

```bash
# Install just (task runner)
cargo install just
# Install mdbook for docs
cargo install mdbook mdbook-mermaid

just build       # cargo build
just test        # cargo test
just check       # cargo check
just fmt         # cargo fmt
just lint        # cargo clippy -D warnings
just ci          # full CI pipeline locally
just docs-serve  # read the mdbook documentation
just doc         # open rustdoc API docs
```

---

## License

Licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.
