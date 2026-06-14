//! # Loss-simulation benchmark: QUIC vs TCP under packet loss
//!
//! Demonstrates TCP HOL-blocking: one lost packet blocks ALL multiplexed
//! HTTP/2 streams because TCP reassembles in-order.  QUIC recovers
//! independently per-stream, so only the affected stream stalls.
//!
//! **Linux only** — requires `tc` (netem) for packet loss injection.
//!
//! ## Usage
//!
//! ```bash
//! cargo bench --features loss-sim --bench loss_bench
//! ```
//!
//! Results show single-request and concurrent-request latency under 5 %
//! packet loss.  Concurrent TCP should exhibit **higher tail latency**
//! and a wider spread than QUIC, directly illustrating HOL blocking.

#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use futures::future::join_all;
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

/// Fixed payload for loss bench — large enough to span multiple QUIC/TCP
/// segments, small enough to keep latency reasonable.
const PAYLOAD_SIZE: usize = 4096;

/// Concurrency levels tested under loss.
const CONCURRENCY: &[usize] = &[1, 4, 8, 16];

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

// ── TLS helpers ──────────────────────────────────────────────────────────────

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

// ── tc (netem) guard ─────────────────────────────────────────────────────────

/// Applies `tc` packet loss on `lo` and restores it on drop.
struct TcGuard {
    iface: String,
}

impl TcGuard {
    fn new(iface: &str, loss_percent: u32) -> Self {
        let guard = Self {
            iface: iface.to_string(),
        };
        let out = std::process::Command::new("tc")
            .args([
                "qdisc",
                "add",
                "dev",
                iface,
                "root",
                "netem",
                "loss",
                &loss_percent.to_string(),
            ])
            .output();
        match out {
            Ok(o) if o.status.success() => {
                eprintln!(
                    "[tc] {}% loss on {} (OK)",
                    loss_percent, iface
                );
            }
            Ok(o) => eprintln!(
                "[tc] add warning: {}",
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(e) => eprintln!("[tc] binary not found: {e}"),
        }
        guard
    }
}

impl Drop for TcGuard {
    fn drop(&mut self) {
        let out = std::process::Command::new("tc")
            .args(["qdisc", "del", "dev", &self.iface, "root"])
            .output();
        match out {
            Ok(o) if o.status.success() => {
                eprintln!("[tc] restored {} (OK)", self.iface);
            }
            Ok(o) => eprintln!(
                "[tc] restore warning: {}",
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(e) => eprintln!("[tc] binary not found: {e}"),
        }
    }
}

// ── Benchmark ────────────────────────────────────────────────────────────────

fn bench_loss(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);

    // Give servers time to bind
    std::thread::sleep(Duration::from_millis(200));

    // Clients
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

    let quic_client = BenchServiceClient::new(quic_channel);
    let tcp_client = BenchServiceClient::new(tcp_channel);

    // Warmup (no loss)
    rt.block_on(async {
        let w = Payload {
            body: vec![0u8; PAYLOAD_SIZE],
        };
        let _ = quic_client
            .clone()
            .unary(tonic::Request::new(w.clone()))
            .await;
        let _ = tcp_client
            .clone()
            .unary(tonic::Request::new(w))
            .await;
    });

    // ── Enable packet loss ──────────────────────────────────────────────
    let _tc = TcGuard::new("lo", 5);

    // Small delay for tc to take effect
    std::thread::sleep(Duration::from_millis(100));

    let payload = Payload {
        body: vec![0u8; PAYLOAD_SIZE],
    };

    // ── QUIC under loss ─────────────────────────────────────────────────
    {
        let mut group = c.benchmark_group("quic_loss");
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(30);
        for &conc in CONCURRENCY {
            group.throughput(Throughput::Bytes((PAYLOAD_SIZE * conc) as u64));
            group.bench_with_input(
                BenchmarkId::new("concurrent", conc),
                &conc,
                |b, &conc| {
                    let client = quic_client.clone();
                    let payload = payload.clone();
                    b.iter(|| {
                        rt.block_on(async {
                            let futures: Vec<_> = (0..conc)
                                .map(|_| {
                                    let mut c = client.clone();
                                    let p = payload.clone();
                                    async move {
                                        let req = tonic::Request::new(p);
                                        c.unary(req).await.unwrap()
                                    }
                                })
                                .collect();
                            let results = join_all(futures).await;
                            black_box(results);
                        });
                    });
                },
            );
        }
        group.finish();
    }

    // ── TCP under loss ──────────────────────────────────────────────────
    {
        let mut group = c.benchmark_group("tcp_loss");
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(30);
        for &conc in CONCURRENCY {
            group.throughput(Throughput::Bytes((PAYLOAD_SIZE * conc) as u64));
            group.bench_with_input(
                BenchmarkId::new("concurrent", conc),
                &conc,
                |b, &conc| {
                    let client = tcp_client.clone();
                    let payload = payload.clone();
                    b.iter(|| {
                        rt.block_on(async {
                            let futures: Vec<_> = (0..conc)
                                .map(|_| {
                                    let mut c = client.clone();
                                    let p = payload.clone();
                                    async move {
                                        let req = tonic::Request::new(p);
                                        c.unary(req).await.unwrap()
                                    }
                                })
                                .collect();
                            let results = join_all(futures).await;
                            black_box(results);
                        });
                    });
                },
            );
        }
        group.finish();
    }

    // tc guard drops here, restoring `lo`

    // ── Cleanup ─────────────────────────────────────────────────────────
    servers.quic_shutdown.send(()).ok();
    servers.tcp_shutdown.send(()).ok();
    std::thread::sleep(Duration::from_millis(200));
}

#[cfg(target_os = "linux")]
criterion_group!(benches, bench_loss);

#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("loss_bench requires Linux (tc/netem). Skipping.");
}
