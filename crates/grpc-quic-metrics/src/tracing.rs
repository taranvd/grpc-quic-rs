//! Tracing span helpers for the `grpc-quic` ecosystem.

/// Span names used across the `grpc-quic` ecosystem.
pub mod spans {
    pub const CONNECT: &str = "grpc_quic.connect";
    pub const ACCEPT: &str = "grpc_quic.accept";
    pub const SEND: &str = "grpc_quic.send";
    pub const RECV: &str = "grpc_quic.recv";
    pub const RECONNECT: &str = "grpc_quic.reconnect";
}

/// Create a span for a QUIC connection establishment.
#[macro_export]
macro_rules! connect_span {
    ($remote:expr) => {
        tracing::info_span!($crate::tracing::spans::CONNECT, remote = %$remote)
    };
}

/// Create a span for accepting an incoming QUIC connection.
#[macro_export]
macro_rules! accept_span {
    () => {
        tracing::info_span!($crate::tracing::spans::ACCEPT)
    };
}

/// Create a span for sending data over a QUIC stream.
#[macro_export]
macro_rules! send_span {
    ($len:expr) => {
        tracing::debug_span!($crate::tracing::spans::SEND, len = $len)
    };
}

/// Create a span for receiving data over a QUIC stream.
#[macro_export]
macro_rules! recv_span {
    ($len:expr) => {
        tracing::debug_span!($crate::tracing::spans::RECV, len = $len)
    };
}

/// Create a span for a reconnect attempt.
#[macro_export]
macro_rules! reconnect_span {
    ($attempt:expr) => {
        tracing::info_span!($crate::tracing::spans::RECONNECT, attempt = $attempt)
    };
}
