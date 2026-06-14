//! Stream acceptor — reads a QUIC bi-stream, reconstructs the gRPC request,
//! and dispatches it to the tonic service handler.
//!
//! Full implementation arrives in Phase 3.

/// Accepts bi-directional QUIC streams from an established connection and
/// routes each stream to the appropriate tonic service handler.
///
/// One stream = one gRPC call (unary or streaming).
pub struct StreamAcceptor;

impl StreamAcceptor {
    /// Create a new acceptor.
    pub fn new() -> Self {
        Self
    }
}

impl Default for StreamAcceptor {
    fn default() -> Self {
        Self::new()
    }
}
