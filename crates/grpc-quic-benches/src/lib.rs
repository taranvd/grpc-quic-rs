//! Shared infrastructure for gRPC-QUIC benchmarks.
//!
//! Provides TLS setup, server lifecycle, histogram-based latency recording,
//! JSON report generation, and a `netem` wrapper.

use std::cell::RefCell;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::runtime::Runtime;

use grpc_quic::client::QuicChannel;
use grpc_quic::server::QuicServer;
use grpc_quic::transport::{QuicEndpoint, TlsConfig};

// ── Proto-generated types ──────────────────────────────────────────────────

pub mod pb {
    tonic::include_proto!("bench");
}

use pb::bench_service_server::{BenchService, BenchServiceServer};
use pb::Payload;

// ── Echo service ───────────────────────────────────────────────────────────

#[derive(Clone, Default)]
pub struct EchoService;

#[tonic::async_trait]
impl BenchService for EchoService {
    async fn unary(
        &self,
        request: tonic::Request<Payload>,
    ) -> Result<tonic::Response<Payload>, tonic::Status> {
        Ok(tonic::Response::new(request.into_inner()))
    }
}

// ─── Default payload sizes & concurrency levels ────────────────────────────

pub const PAYLOAD_SIZES: &[usize] = &[64, 256, 1024, 4096, 16384];
pub const QUICK_SIZES: &[usize] = &[64, 256, 1024, 4096];
pub const CONCURRENCY: &[usize] = &[1, 4, 8, 16];
pub const LOSS_PERCENTS: &[u32] = &[0, 1, 5];

/// Payload sizes for the current run — excludes 16384 in quick mode.
pub fn bench_sizes() -> &'static [usize] {
    if is_quick() {
        QUICK_SIZES
    } else {
        PAYLOAD_SIZES
    }
}

// ── TLS helpers ────────────────────────────────────────────────────────────

pub fn make_tls_configs() -> (TlsConfig, TlsConfig) {
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

// ── Server lifecycle ───────────────────────────────────────────────────────

pub struct BenchServers {
    pub quic_addr: SocketAddr,
    pub tcp_addr: SocketAddr,
    pub quic_shutdown: tokio::sync::oneshot::Sender<()>,
    pub tcp_shutdown: tokio::sync::oneshot::Sender<()>,
}

pub fn setup_servers(rt: &Runtime, server_tls: TlsConfig) -> BenchServers {
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

pub fn shutdown_servers(servers: BenchServers) {
    servers.quic_shutdown.send(()).ok();
    servers.tcp_shutdown.send(()).ok();
    std::thread::sleep(Duration::from_millis(200));
}

// ── QUIC + TCP client builders ─────────────────────────────────────────────

pub fn make_quic_client(
    rt: &Runtime,
    addr: SocketAddr,
    tls: TlsConfig,
) -> pb::bench_service_client::BenchServiceClient<QuicChannel> {
    rt.block_on(async {
        let channel = QuicChannel::builder()
            .tls(tls)
            .concurrency_limit(512)
            .connect(addr.to_string())
            .await
            .unwrap();
        pb::bench_service_client::BenchServiceClient::new(channel)
    })
}

pub fn make_tcp_client(
    rt: &Runtime,
    addr: SocketAddr,
) -> pb::bench_service_client::BenchServiceClient<tonic::transport::Channel> {
    rt.block_on(async {
        let channel = tonic::transport::Endpoint::new(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        pb::bench_service_client::BenchServiceClient::new(channel)
    })
}

// ── Histogram-based latency recording ──────────────────────────────────────

thread_local! {
    static LATENCY_HIST: RefCell<Histogram<u64>> =
        RefCell::new(Histogram::<u64>::new(3).unwrap());
}

/// Record a single RPC latency observation.
///
/// Call immediately after a successful RPC returns.  Uses a thread-local
/// `Histogram` so no heap allocation or locking in the hot path.
pub fn record_latency(start: Instant) {
    let elapsed = start.elapsed().as_micros() as u64;
    LATENCY_HIST.with(|h| {
        h.borrow_mut().record(elapsed).ok();
    });
}

/// Drain the thread-local histogram and return it, replacing it with
/// a fresh empty histogram.
///
/// Call after a benchmark group finishes to extract the distribution.
pub fn take_histogram() -> Histogram<u64> {
    LATENCY_HIST.with(|h| {
        let mut hist = h.borrow_mut();
        let old = hist.clone();
        *hist = Histogram::<u64>::new(3).unwrap();
        old
    })
}

// ── JSON report types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct LatencySummary {
    /// 50th percentile in milliseconds
    pub p50_ms: f64,
    /// 95th percentile in milliseconds
    pub p95_ms: f64,
    /// 99th percentile in milliseconds
    pub p99_ms: f64,
    /// Arithmetic mean in milliseconds
    pub mean_ms: f64,
    /// Minimum observed value in milliseconds
    pub min_ms: f64,
    /// Maximum observed value in milliseconds
    pub max_ms: f64,
    /// Total number of samples
    pub samples: u64,
}

impl LatencySummary {
    pub fn from_histogram(h: &Histogram<u64>) -> Self {
        Self {
            p50_ms: h.value_at_percentile(50.0) as f64 / 1000.0,
            p95_ms: h.value_at_percentile(95.0) as f64 / 1000.0,
            p99_ms: h.value_at_percentile(99.0) as f64 / 1000.0,
            mean_ms: h.mean() / 1000.0,
            min_ms: h.min() as f64 / 1000.0,
            max_ms: h.max() as f64 / 1000.0,
            samples: h.len(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchResult {
    pub protocol: String,
    pub scenario: String,
    pub concurrency: usize,
    pub loss_percent: u32,
    pub payload_bytes: usize,
    pub latency: LatencySummary,
    /// Throughput in MiB/s computed from mean latency and payload size
    pub throughput_mib_s: f64,
    /// Git commit SHA of the build (from GITHUB_SHA env or git rev-parse)
    pub commit_sha: String,
    /// ISO 8601 timestamp when the benchmark was run
    pub timestamp: String,
}

impl BenchResult {
    /// Build a result from a histogram, metadata, and raw throughput.
    pub fn new(
        protocol: &str,
        scenario: &str,
        concurrency: usize,
        loss_percent: u32,
        payload_bytes: usize,
        hist: &Histogram<u64>,
    ) -> Self {
        let latency = LatencySummary::from_histogram(hist);
        // throughput = (payload_bytes * concurrency) / mean_latency_secs / 1024^2
        let mean_secs = latency.mean_ms / 1000.0;
        let throughput = if mean_secs > 0.0 {
            (payload_bytes as f64 * concurrency as f64) / mean_secs / (1024.0 * 1024.0)
        } else {
            0.0
        };
        Self {
            protocol: protocol.to_string(),
            scenario: scenario.to_string(),
            concurrency,
            loss_percent,
            payload_bytes,
            latency,
            throughput_mib_s: throughput,
            commit_sha: current_commit_sha(),
            timestamp: current_timestamp(),
        }
    }

    /// Save a list of results as a JSON report.
    pub fn save_json(reports: &[BenchResult], path: impl AsRef<Path>) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(reports).unwrap();
        std::fs::create_dir_all(path.as_ref().parent().unwrap_or(Path::new(".")))?;
        std::fs::write(path.as_ref(), &json)
    }

    /// Save and panic on failure (for use in benchmark harness where errors
    /// would otherwise be silently swallowed).
    pub fn save_json_or_panic(reports: &[BenchResult], path: impl AsRef<Path>) {
        let path = path.as_ref().to_path_buf();
        eprintln!(
            "[bench] saving {} results to {} (cwd: {})",
            reports.len(),
            path.display(),
            std::env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
        );
        if let Err(e) = Self::save_json(reports, &path) {
            panic!("failed to write bench report to {}: {e}", path.display());
        }
    }
}

/// Returns the current commit SHA from `GITHUB_SHA` env or `git rev-parse`.
pub fn current_commit_sha() -> String {
    if let Ok(sha) = std::env::var("GITHUB_SHA") {
        return sha;
    }
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                String::from_utf8(o.stdout).ok()
            } else {
                None
            }
        })
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// Returns an ISO 8601 timestamp string for the current time.
pub fn current_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

// ── Payload builder ────────────────────────────────────────────────────────

pub fn make_payload(size: usize) -> Payload {
    Payload {
        body: vec![0u8; size],
    }
}

// ── Mode flags ──────────────────────────────────────────────────────────────

/// Returns `true` when the `--deterministic` CLI flag (or `DETERMINISTIC=1`)
/// is set.
pub fn is_deterministic() -> bool {
    std::env::var("DETERMINISTIC").as_deref() == Ok("1")
        || std::env::args().any(|a| a == "--deterministic")
}

/// Returns `true` when `QUICK=1` env is set (CI quick-smoke mode).
/// Excludes large payloads (16384) to keep CI fast.
pub fn is_quick() -> bool {
    std::env::var("QUICK").as_deref() == Ok("1")
}
