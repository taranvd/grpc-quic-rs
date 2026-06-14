use std::net::SocketAddr;
use std::sync::Arc;

use crate::{QuicEndpoint, TlsConfig};

fn make_tls_configs() -> (TlsConfig, TlsConfig) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.key_pair.serialize_der();

    let server_cert = rustls::pki_types::CertificateDer::from(cert_der);
    let server_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let mut server_crypto = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![server_cert.clone()], server_key)
        .unwrap();
    server_crypto.alpn_protocols = vec![b"h3".to_vec()];
    server_crypto.max_early_data_size = u32::MAX;

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(server_cert).unwrap();

    let mut client_crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"h3".to_vec()];

    (
        TlsConfig::server(server_crypto),
        TlsConfig::client(client_crypto),
    )
}

#[tokio::test]
async fn test_quic_endpoint_and_streams() {
    let (server_tls, client_tls) = make_tls_configs();

    // Bind server endpoint on ephemeral port
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server_endpoint = QuicEndpoint::server(server_addr, server_tls).unwrap();
    let bound_addr = server_endpoint.local_addr().unwrap();

    // Create client endpoint
    let client_endpoint = QuicEndpoint::client(client_tls).unwrap();

    // Spawn server accept task
    let server_handle = tokio::spawn(async move {
        let conn_res = server_endpoint.accept().await.unwrap();
        let conn = conn_res.unwrap();

        let stream_res = conn.accept_bi().await.unwrap();
        let (mut send, mut recv) = stream_res.unwrap();

        // Read client request
        let mut buf = vec![0u8; 12];
        recv.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello server");

        // Write server response
        send.write_all(b"hello client").await.unwrap();
        send.finish().unwrap();

        conn
    });

    // Client connects to server
    let conn = client_endpoint
        .connect(bound_addr, "localhost")
        .await
        .unwrap();
    assert_eq!(conn.remote_address(), bound_addr);
    assert!(conn.stable_id() > 0);

    // Client opens bidirectional stream
    let (mut send, mut recv) = conn.open_bi().await.unwrap();

    // Write client request
    send.write_all(b"hello server").await.unwrap();
    send.finish().unwrap();

    // Read server response
    let mut buf = vec![0u8; 12];
    recv.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello client");

    let _server_conn = server_handle.await.unwrap();
}
