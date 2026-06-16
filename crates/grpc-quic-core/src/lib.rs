#![forbid(unsafe_code)]
#![warn(rust_2018_idioms)]

pub mod body;
pub mod client;
pub mod error;
pub mod server;

pub use error::CoreError;
