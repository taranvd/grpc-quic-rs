//! # grpc-quic-discovery
//!
//! Service discovery abstractions for the `grpc-quic` ecosystem.
//!
//! Provides the [`Resolver`] trait and a built-in [`StaticResolver`]
//! for static address lists. Future implementations may target
//! consul, etcd, or Kubernetes endpoints.
//!
//! Full implementation arrives in Phase 7.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod resolver;

pub use resolver::{Resolver, StaticResolver};
