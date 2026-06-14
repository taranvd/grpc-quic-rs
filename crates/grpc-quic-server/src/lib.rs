//! # grpc-quic-server
//!
//! gRPC server over QUIC transport.
//!
//! Provides [`QuicServer`] which accepts QUIC connections, reads raw gRPC bytes
//! from each bi-directional stream, and delegates requests to a tonic [`Router`].
//!
//! ## Usage (Phase 3)
//!
//! ```rust,no_run
//! use grpc_quic_server::QuicServer;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! QuicServer::builder()
//!     .build()
//!     .serve("127.0.0.1:50051".parse()?)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`Router`]: tonic::transport::server::Router

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod acceptor;
pub mod error;
pub mod server;

pub use error::ServerError;
pub use server::{QuicServer, QuicServerBuilder};
