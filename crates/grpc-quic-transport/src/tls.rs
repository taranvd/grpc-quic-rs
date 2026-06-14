//! TLS 1.3 configuration for QUIC endpoints.
//!
//! Wraps `rustls` server/client configuration and exposes a builder-style API
//! for both server-side and client-side TLS, including mutual TLS (mTLS).

use std::sync::Arc;

use rustls::{ClientConfig, ServerConfig};

use crate::error::TransportError;

/// Unified TLS configuration for QUIC endpoints.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    inner: TlsConfigInner,
}

#[derive(Clone, Debug)]
enum TlsConfigInner {
    Server(Arc<ServerConfig>),
    Client(Arc<ClientConfig>),
}

impl TlsConfig {
    /// Wrap an already-built [`rustls::ServerConfig`].
    pub fn server(config: ServerConfig) -> Self {
        Self {
            inner: TlsConfigInner::Server(Arc::new(config)),
        }
    }

    /// Wrap an already-built [`rustls::ClientConfig`].
    pub fn client(config: ClientConfig) -> Self {
        Self {
            inner: TlsConfigInner::Client(Arc::new(config)),
        }
    }

    /// Return the inner server config, or an error if this is a client config.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, TransportError> {
        match &self.inner {
            TlsConfigInner::Server(c) => Ok(Arc::clone(c)),
            TlsConfigInner::Client(_) => {
                Err(TransportError::Tls("expected server config".into()))
            }
        }
    }

    /// Return the inner client config, or an error if this is a server config.
    pub fn client_config(&self) -> Result<Arc<ClientConfig>, TransportError> {
        match &self.inner {
            TlsConfigInner::Client(c) => Ok(Arc::clone(c)),
            TlsConfigInner::Server(_) => {
                Err(TransportError::Tls("expected client config".into()))
            }
        }
    }
}
