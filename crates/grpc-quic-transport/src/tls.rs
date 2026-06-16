//! TLS 1.3 configuration for QUIC endpoints.
//!
//! Wraps `rustls` server/client configuration and provides convenience helpers
//! for common TLS setups (self-signed certs for development, PEM loading for
//! production, insecure client for testing, etc.).

use std::path::Path;
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
    ///
    /// Make sure `alpn_protocols` includes `b"h3"`.
    pub fn server(config: ServerConfig) -> Self {
        Self {
            inner: TlsConfigInner::Server(Arc::new(config)),
        }
    }

    /// Wrap an already-built [`rustls::ClientConfig`].
    ///
    /// Make sure `alpn_protocols` includes `b"h3"`.
    pub fn client(config: ClientConfig) -> Self {
        Self {
            inner: TlsConfigInner::Client(Arc::new(config)),
        }
    }

    /// Load server TLS from PEM-encoded certificate chain and private key files.
    ///
    /// `cert_path` — PEM file with the server certificate (optionally followed by
    /// intermediate CA certificates).
    ///
    /// `key_path` — PEM file with the private key (PKCS#8, RSA, or ECDSA).
    ///
    /// ALPN is automatically set to `b"h3"`.
    pub fn server_from_pem(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, TransportError> {
        let certs = {
            let file = std::fs::File::open(cert_path.as_ref())
                .map_err(|e| TransportError::Tls(format!("cannot open cert file: {e}")))?;
            let mut reader = std::io::BufReader::new(file);
            rustls_pemfile::certs(&mut reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| TransportError::Tls(format!("cannot parse certs: {e}")))?
        };

        if certs.is_empty() {
            return Err(TransportError::Tls(
                "no certificates found in PEM file".into(),
            ));
        }

        let key = {
            let file = std::fs::File::open(key_path.as_ref())
                .map_err(|e| TransportError::Tls(format!("cannot open key file: {e}")))?;
            let mut reader = std::io::BufReader::new(file);
            rustls_pemfile::private_key(&mut reader)
                .map_err(|e| TransportError::Tls(format!("cannot parse private key: {e}")))?
                .ok_or_else(|| TransportError::Tls("no private key found in PEM file".into()))?
        };

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        config.alpn_protocols = vec![b"h3".to_vec()];
        config.max_early_data_size = u32::MAX;

        Ok(Self::server(config))
    }

    /// Generate a self-signed certificate and create a server TLS config.
    ///
    /// `hostnames` — DNS names / IPs to include in the certificate SAN
    /// (e.g. `vec!["localhost", "127.0.0.1"]`).
    ///
    /// This is intended for development and testing only — not for production.
    #[cfg(feature = "dev-certs")]
    pub fn server_self_signed(
        hostnames: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, TransportError> {
        let hostnames: Vec<String> = hostnames.into_iter().map(Into::into).collect();
        let cert = rcgen::generate_simple_self_signed(hostnames)
            .map_err(|e| TransportError::Tls(e.to_string()))?;

        let cert_der = rustls::pki_types::CertificateDer::from(cert.cert.der().to_vec());
        let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()),
        );

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| TransportError::Tls(e.to_string()))?
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        config.alpn_protocols = vec![b"h3".to_vec()];
        config.max_early_data_size = u32::MAX;

        Ok(Self::server(config))
    }

    /// Create a default client TLS configuration.
    ///
    /// Loads the standard Mozilla root CA certificates via `webpki-roots` and
    /// sets ALPN to `b"h3"`.
    pub fn client_default() -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut client_crypto = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        client_crypto.alpn_protocols = vec![b"h3".to_vec()];

        Self::client(client_crypto)
    }

    /// Create an insecure client TLS configuration (development only).
    ///
    /// **WARNING:** This accepts any server certificate, including self-signed
    /// and invalid ones. Never use in production.
    ///
    /// Useful for testing against a server with a self-signed certificate.
    pub fn client_insecure() -> Self {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client_crypto = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        client_crypto.alpn_protocols = vec![b"h3".to_vec()];

        Self::client(client_crypto)
    }

    /// Return the inner server config, or an error if this is a client config.
    pub fn server_config(&self) -> Result<Arc<ServerConfig>, TransportError> {
        match &self.inner {
            TlsConfigInner::Server(c) => Ok(Arc::clone(c)),
            TlsConfigInner::Client(_) => Err(TransportError::Tls("expected server config".into())),
        }
    }

    /// Return the inner client config, or an error if this is a server config.
    pub fn client_config(&self) -> Result<Arc<ClientConfig>, TransportError> {
        match &self.inner {
            TlsConfigInner::Client(c) => Ok(Arc::clone(c)),
            TlsConfigInner::Server(_) => Err(TransportError::Tls("expected client config".into())),
        }
    }
}

// ── Insecure certificate verifier (development only) ──────────────────────

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};

#[derive(Debug)]
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }

    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
}
