//! # grpc-quic-transport
//!
//! Raw QUIC transport primitives for the `grpc-quic` ecosystem.
//!
//! This crate provides:
//! - [`QuicEndpoint`] — a QUIC endpoint that can accept or initiate connections
//! - [`QuicConnection`] — a single QUIC connection with bi-directional stream support
//! - [`TlsConfig`] — TLS 1.3 configuration for both client and server
//!
//! ## Design constraints
//!
//! This crate intentionally has **no dependency on tonic, prost, h2, or h3**.
//! It only deals with raw bytes over QUIC streams. All gRPC semantics live in
//! the higher-level `grpc-quic-client` and `grpc-quic-server` crates.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod connection;
pub mod endpoint;
pub mod error;
pub mod tls;

pub use connection::QuicConnection;
pub use endpoint::QuicEndpoint;
pub use error::TransportError;
pub use tls::TlsConfig;
