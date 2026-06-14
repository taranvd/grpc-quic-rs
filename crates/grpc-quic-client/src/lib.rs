//! # grpc-quic-client
//!
//! gRPC client over QUIC transport.
//!
//! Provides [`QuicChannel`] — a [`tower::Service`] implementation that bridges
//! tonic-generated client stubs to QUIC streams via [`quinn`].
//!
//! ## Usage
//!
//! ```rust,no_run
//! use grpc_quic_client::QuicChannel;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let channel = QuicChannel::builder()
//!     .connect("127.0.0.1:50051")
//!     .await?;
//!
//! // Pass `channel` directly to any tonic-generated *Client::new(channel)
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod channel;
pub mod error;
pub mod pool;
pub mod retry;

pub use channel::{QuicChannel, QuicChannelBuilder};
pub use error::ClientError;
pub use retry::RetryPolicy;
