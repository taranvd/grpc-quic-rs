//! # grpc-quic
//!
//! A custom QUIC transport implementation for [tonic](https://docs.rs/tonic) gRPC.
//!
//! ## Overview
//!
//! `grpc-quic` replaces the default HTTP/TCP transport with QUIC streams
//! while preserving full gRPC semantics, protobuf encoding, and tonic service
//! abstractions. All RPC data is transmitted as opaque gRPC-encoded bytes over
//! QUIC streams secured by TLS 1.3.
//!
//! ```text
//! tonic  (RPC layer: service traits + protobuf codec)
//!         ↓
//! grpc-quic  (tower::Service transport adapter)
//!         ↓
//! quinn  (QUIC bi-directional streams, 1 stream = 1 RPC call)
//!         ↓
//! UDP + TLS 1.3 (rustls)
//! ```
//!
//! ## Key design principles
//!
//! - `grpc-quic` does **not** change gRPC semantics.
//! - `grpc-quic` **only** replaces the transport layer (TCP → QUIC).
//! - tonic / prost remain fully intact as the RPC layer.
//!
//! ## Quick start
//!
//! See the `examples/` directory for unary, client-streaming, server-streaming,
//! and bidirectional-streaming demos.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub use grpc_quic_client as client;
pub use grpc_quic_server as server;
pub use grpc_quic_transport as transport;

#[cfg(feature = "metrics")]
pub use grpc_quic_metrics as metrics;

#[cfg(feature = "discovery")]
pub use grpc_quic_discovery as discovery;
