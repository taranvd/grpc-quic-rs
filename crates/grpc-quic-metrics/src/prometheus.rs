//! Prometheus metrics stubs — implementation arrives in Phase 6.

/// Prometheus metric names used across the `grpc-quic` ecosystem.
pub mod names {
    pub const CONNECTIONS_TOTAL: &str = "grpc_quic_connections_total";
    pub const STREAMS_TOTAL: &str = "grpc_quic_streams_total";
    pub const REQUESTS_TOTAL: &str = "grpc_quic_requests_total";
    pub const RECONNECTS_TOTAL: &str = "grpc_quic_reconnects_total";
    pub const BYTES_SENT: &str = "grpc_quic_bytes_sent";
    pub const BYTES_RECEIVED: &str = "grpc_quic_bytes_received";
}
