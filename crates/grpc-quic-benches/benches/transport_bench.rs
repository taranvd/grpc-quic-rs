//! # Transport benchmarks: QUIC vs TCP baseline
//!
//! Measures raw request-response round-trip latency across both transports
//! using identical echo services with a simple length-prefixed wire format.
//!
//! - **QUIC**: [`QuicEndpoint`] + [`QuicConnection`] (mandatory TLS 1.3).
//! - **TCP**: tokio [`TcpStream`] (plain, no TLS).
//!
//! Wire format (both):
//! ```text
//! Request:  [u16 BE payload_len][payload_bytes]
//! Response: [u16 BE payload_len][payload_bytes]
//! ```
//!
//! ## Design
//!
//! Both servers are lean echo handlers — no gRPC, no tonic, no protobuf.
//! This gives a pure transport-vs-transport comparison.
//!
//! ## Caveat
//!
//! QUIC includes mandatory TLS 1.3 handshake overhead on each **new**
//! connection. This benchmark reuses a single QUIC connection (streams
//! are cheap and independent), so the TLS cost is amortised.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;

use grpc_quic::transport::{QuicConnection, QuicEndpoint, TlsConfig};

/// Payload sizes in bytes for the latency sweep.
const PAYLOAD_SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];

// ── TLS helpers (same as grpc-quic-server tests) ─────────────────────────────

fn make_tls_configs() -> (TlsConfig, TlsConfig) {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    let cert = rcgen::generate_simple_self_signed(subject_alt_names).unwrap();

    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.key_pair.serialize_der();

    let server_cert = rustls::pki_types::CertificateDer::from(cert_der.clone());
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
    server_crypto.alpn_protocols = vec![b"grpc-quic".to_vec()];
    server_crypto.max_early_data_size = u32::MAX;

    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(server_cert).unwrap();

    let mut client_crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"grpc-quic".to_vec()];

    (
        TlsConfig::server(server_crypto),
        TlsConfig::client(client_crypto),
    )
}

// ── Server setup ─────────────────────────────────────────────────────────────

struct BenchServers {
    quic_addr: SocketAddr,
    tcp_addr: SocketAddr,
    quic_shutdown: tokio::sync::oneshot::Sender<()>,
    tcp_shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Start both echo servers on ephemeral ports.
fn setup_servers(rt: &Runtime, server_tls: TlsConfig) -> BenchServers {
    let (quic_shutdown_tx, mut quic_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (tcp_shutdown_tx, mut tcp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let (quic_addr, tcp_addr) = rt.block_on(async {
        // ── QUIC server ───────────────────────────────────────────────────
        let quic_ep = QuicEndpoint::server("127.0.0.1:0".parse().unwrap(), server_tls).unwrap();
        let qaddr = quic_ep.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut quic_shutdown_rx => break,
                    conn = quic_ep.accept() => {
                        match conn {
                            Some(Ok(c)) => {
                                tokio::spawn(handle_quic_connection(c));
                            }
                            _ => break,
                        }
                    }
                }
            }
        });

        // ── TCP server ───────────────────────────────────────────────────
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taddr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut tcp_shutdown_rx => break,
                    conn = listener.accept() => {
                        match conn {
                            Ok((mut s, _)) => {
                                tokio::spawn(async move {
                                    echo_on_tcp(&mut s).await;
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        (qaddr, taddr)
    });

    BenchServers {
        quic_addr,
        tcp_addr,
        quic_shutdown: quic_shutdown_tx,
        tcp_shutdown: tcp_shutdown_tx,
    }
}

/// QUIC connection handler: accepts streams and echoes them.
async fn handle_quic_connection(conn: QuicConnection) {
    while let Some(Ok((send, recv))) = conn.accept_bi().await {
        tokio::spawn(echo_on_quic_stream(send, recv));
    }
}

/// Echo on a QUIC bi-stream: `[u16 len][payload]` → `[u16 len][payload]`.
async fn echo_on_quic_stream(mut send: quinn::SendStream, mut recv: quinn::RecvStream) {
    let mut len_buf = [0u8; 2];
    if recv.read_exact(&mut len_buf).await.is_err() {
        return;
    }
    let payload_len = u16::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; payload_len];
    if recv.read_exact(&mut payload).await.is_err() {
        return;
    }
    let _ = send.write_all(&len_buf).await;
    let _ = send.write_all(&payload).await;
    let _ = send.finish();
}

/// Echo on a TCP stream — loops to keep the connection alive for
/// multiple requests (similar to how QUIC reuses one connection
/// for many bi-streams).
async fn echo_on_tcp(stream: &mut tokio::net::TcpStream) {
    loop {
        let mut len_buf = [0u8; 2];
        if stream.read_exact(&mut len_buf).await.is_err() {
            return;
        }
        let payload_len = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; payload_len];
        if stream.read_exact(&mut payload).await.is_err() {
            return;
        }
        if stream.write_all(&len_buf).await.is_err() {
            return;
        }
        if stream.write_all(&payload).await.is_err() {
            return;
        }
    }
}

// ── Benchmark ────────────────────────────────────────────────────────────────

fn bench_transport(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);

    // Brief pause for servers to start accepting
    std::thread::sleep(Duration::from_millis(200));

    // ── Pre-open one QUIC connection and one TCP connection ───────────────
    let quic_conn = rt.block_on(async {
        // QUIC: create client endpoint and connect
        let client_ep = QuicEndpoint::client(client_tls).unwrap();
        client_ep
            .connect(servers.quic_addr, "localhost")
            .await
            .unwrap()
    });

    // ── QUIC latency sweep ────────────────────────────────────────────────
    {
        let mut group = c.benchmark_group("quic_stream");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                let conn = quic_conn.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let (mut send, mut recv) = conn.open_bi().await.unwrap();
                        let payload = vec![0u8; size];
                        let len_be = (size as u16).to_be_bytes();
                        send.write_all(&len_be).await.unwrap();
                        send.write_all(&payload).await.unwrap();
                        send.finish().unwrap();

                        let mut resp_len = [0u8; 2];
                        recv.read_exact(&mut resp_len).await.unwrap();
                        let resp_len = u16::from_be_bytes(resp_len) as usize;
                        let mut resp = vec![0u8; resp_len];
                        recv.read_exact(&mut resp).await.unwrap();
                        black_box(resp);
                    });
                });
            });
        }
        group.finish();
    }

    // ── TCP latency sweep (persistent connection) ─────────────────────────
    {
        let mut tcp_stream = rt.block_on(async {
            tokio::net::TcpStream::connect(servers.tcp_addr)
                .await
                .unwrap()
        });

        let mut group = c.benchmark_group("tcp_stream");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                b.iter(|| {
                    rt.block_on(async {
                        let payload = vec![0u8; size];
                        let len_be = (size as u16).to_be_bytes();
                        tcp_stream.write_all(&len_be).await.unwrap();
                        tcp_stream.write_all(&payload).await.unwrap();

                        let mut resp_len = [0u8; 2];
                        tcp_stream.read_exact(&mut resp_len).await.unwrap();
                        let resp_len = u16::from_be_bytes(resp_len) as usize;
                        let mut resp = vec![0u8; resp_len];
                        tcp_stream.read_exact(&mut resp).await.unwrap();
                        black_box(resp);
                    });
                });
            });
        }
        group.finish();
    }

    // ── Clean shutdown ────────────────────────────────────────────────────
    servers.quic_shutdown.send(()).ok();
    servers.tcp_shutdown.send(()).ok();
    std::thread::sleep(Duration::from_millis(200));
}

criterion_group!(benches, bench_transport);
criterion_main!(benches);
