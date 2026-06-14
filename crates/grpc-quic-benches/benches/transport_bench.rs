//! # Transport benchmarks: gRPC over QUIC vs gRPC over TCP (tonic baseline)
//!
//! Full-stack unary RPC latency — tonic generated client → transport
//! → tonic generated service — over both QUIC and TCP.
//!
//! - **QUIC**: `BenchServiceClient<QuicChannel>` → [`QuicServer`] → [`BenchServiceServer`]
//! - **TCP**:  `BenchServiceClient<Channel>` → [`tonic::transport::Server`] → [`BenchServiceServer`]
//!
//! Both paths use identical protobuf messages and identical service logic.
//! The only difference is the transport layer (QUIC vs TCP).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use grpc_quic::client::QuicChannel;
use grpc_quic::server::QuicServer;
use grpc_quic::transport::{QuicEndpoint, TlsConfig};

pub mod pb {
    tonic::include_proto!("bench");
}

use pb::bench_service_client::BenchServiceClient;
use pb::bench_service_server::{BenchService, BenchServiceServer};
use pb::Payload;

/// Payload body sizes in bytes (protobuf overhead is negligible).
const PAYLOAD_SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];

// ── Echo service ─────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct EchoService;

#[tonic::async_trait]
impl BenchService for EchoService {
    async fn unary(
        &self,
        request: tonic::Request<Payload>,
    ) -> Result<tonic::Response<Payload>, tonic::Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}

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

fn setup_servers(rt: &Runtime, server_tls: TlsConfig) -> BenchServers {
    let (quic_shutdown_tx, quic_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (tcp_shutdown_tx, tcp_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let (quic_addr, tcp_addr) = rt.block_on(async {
        // ── QUIC server ───────────────────────────────────────────────────
        let quic_ep = QuicEndpoint::server("127.0.0.1:0".parse().unwrap(), server_tls).unwrap();
        let qaddr = quic_ep.local_addr().unwrap();
        let quic_svc = BenchServiceServer::new(EchoService);

        tokio::spawn(async move {
            QuicServer::builder()
                .build()
                .serve_with_incoming_shutdown(quic_ep, quic_svc, async {
                    quic_shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        // ── TCP/tonic server ──────────────────────────────────────────────
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let taddr = listener.local_addr().unwrap();
        let tcp_svc = BenchServiceServer::new(EchoService);

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(tcp_svc)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
                        .unwrap(),
                    async {
                        tcp_shutdown_rx.await.ok();
                    },
                )
                .await
                .ok();
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

// ── Benchmark ────────────────────────────────────────────────────────────────

fn bench_transport(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);

    // Give servers time to bind and start accepting
    std::thread::sleep(Duration::from_millis(200));

    // ── Clients ───────────────────────────────────────────────────────────
    let quic_channel = rt.block_on(async {
        QuicChannel::builder()
            .tls(client_tls)
            .connect(servers.quic_addr.to_string())
            .await
            .unwrap()
    });

    let tcp_channel = rt.block_on(async {
        tonic::transport::Endpoint::new(format!("http://{}", servers.tcp_addr))
            .unwrap()
            .connect()
            .await
            .unwrap()
    });

    let mut quic_client = BenchServiceClient::new(quic_channel);
    let mut tcp_client = BenchServiceClient::new(tcp_channel);

    // Warmup to establish connections
    rt.block_on(async {
        let w = Payload {
            body: vec![0u8; 64],
        };
        let _ = quic_client.unary(tonic::Request::new(w.clone())).await;
        let _ = tcp_client.unary(tonic::Request::new(w)).await;
    });

    // ── QUIC unary latency ────────────────────────────────────────────────
    {
        let mut group = c.benchmark_group("quic_unary");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                let mut client = quic_client.clone();
                let payload = Payload {
                    body: vec![0u8; size],
                };
                b.iter(|| {
                    rt.block_on(async {
                        let req = tonic::Request::new(payload.clone());
                        let resp = client.unary(req).await.unwrap();
                        black_box(resp.into_inner());
                    });
                });
            });
        }
        group.finish();
    }

    // ── TCP unary latency ─────────────────────────────────────────────────
    {
        let mut group = c.benchmark_group("tcp_unary");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        for &size in PAYLOAD_SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
                let mut client = tcp_client.clone();
                let payload = Payload {
                    body: vec![0u8; size],
                };
                b.iter(|| {
                    rt.block_on(async {
                        let req = tonic::Request::new(payload.clone());
                        let resp = client.unary(req).await.unwrap();
                        black_box(resp.into_inner());
                    });
                });
            });
        }
        group.finish();
    }

    // ── Cleanup ───────────────────────────────────────────────────────────
    servers.quic_shutdown.send(()).ok();
    servers.tcp_shutdown.send(()).ok();
    std::thread::sleep(Duration::from_millis(200));
}

criterion_group!(benches, bench_transport);
criterion_main!(benches);
