//! Tracing span helpers — implementation arrives in Phase 6.

/// Span names used across the `grpc-quic` ecosystem.
pub mod spans {
    pub const CONNECT: &str = "grpc_quic.connect";
    pub const ACCEPT: &str = "grpc_quic.accept";
    pub const SEND: &str = "grpc_quic.send";
    pub const RECV: &str = "grpc_quic.recv";
    pub const RECONNECT: &str = "grpc_quic.reconnect";
}
