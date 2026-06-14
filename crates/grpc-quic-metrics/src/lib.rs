//! # grpc-quic-metrics
//!
//! Prometheus metrics and tracing spans for the `grpc-quic` ecosystem.
//!
//! ## Metrics exposed
//!
//! | Name | Type | Description |
//! |------|------|-------------|
//! | `grpc_quic_connections_total` | Counter | Total QUIC connections established |
//! | `grpc_quic_streams_total` | Counter | Total QUIC streams opened |
//! | `grpc_quic_requests_total` | Counter | Total gRPC requests dispatched |
//! | `grpc_quic_reconnects_total` | Counter | Total reconnect attempts |
//! | `grpc_quic_bytes_sent` | Counter | Total bytes written to QUIC streams |
//! | `grpc_quic_bytes_received` | Counter | Total bytes read from QUIC streams |
//!
//! Full implementation arrives in Phase 6.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod prometheus;
pub mod tracing;
