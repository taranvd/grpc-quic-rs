//! Prometheus metrics for the grpc-quic ecosystem.

use prometheus::{register_int_counter_vec, IntCounterVec};
use std::sync::OnceLock;

/// Holds the registered Prometheus metrics.
pub struct Metrics {
    pub connections_total: IntCounterVec,
    pub streams_total: IntCounterVec,
    pub requests_total: IntCounterVec,
    pub reconnects_total: IntCounterVec,
    pub bytes_sent: IntCounterVec,
    pub bytes_received: IntCounterVec,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

/// Retrieve or initialize the global metrics collector.
pub fn get_metrics() -> &'static Metrics {
    METRICS.get_or_init(|| Metrics {
        connections_total: register_int_counter_vec!(
            "grpc_quic_connections_total",
            "Total QUIC connections established",
            &["role"]
        )
        .unwrap(),
        streams_total: register_int_counter_vec!(
            "grpc_quic_streams_total",
            "Total QUIC streams opened",
            &["role"]
        )
        .unwrap(),
        requests_total: register_int_counter_vec!(
            "grpc_quic_requests_total",
            "Total gRPC requests dispatched",
            &["role", "path"]
        )
        .unwrap(),
        reconnects_total: register_int_counter_vec!(
            "grpc_quic_reconnects_total",
            "Total reconnect attempts",
            &[]
        )
        .unwrap(),
        bytes_sent: register_int_counter_vec!(
            "grpc_quic_bytes_sent",
            "Total bytes written to QUIC streams",
            &["role"]
        )
        .unwrap(),
        bytes_received: register_int_counter_vec!(
            "grpc_quic_bytes_received",
            "Total bytes read from QUIC streams",
            &["role"]
        )
        .unwrap(),
    })
}

/// Record a connection establishment.
pub fn record_connection(role: &str) {
    get_metrics()
        .connections_total
        .with_label_values(&[role])
        .inc();
}

/// Record a stream creation.
pub fn record_stream(role: &str) {
    get_metrics().streams_total.with_label_values(&[role]).inc();
}

/// Record a gRPC request dispatch.
pub fn record_request(role: &str, path: &str) {
    get_metrics()
        .requests_total
        .with_label_values(&[role, path])
        .inc();
}

/// Record a connection reconnect attempt.
pub fn record_reconnect() {
    get_metrics().reconnects_total.with_label_values(&[]).inc();
}

/// Record bytes sent over QUIC streams.
pub fn record_bytes_sent(role: &str, bytes: u64) {
    get_metrics()
        .bytes_sent
        .with_label_values(&[role])
        .inc_by(bytes);
}

/// Record bytes received from QUIC streams.
pub fn record_bytes_received(role: &str, bytes: u64) {
    get_metrics()
        .bytes_received
        .with_label_values(&[role])
        .inc_by(bytes);
}
