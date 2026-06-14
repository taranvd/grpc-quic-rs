//! Loss-simulation benchmark: QUIC vs TCP under controlled packet loss.
//!
//! Demonstrates TCP HOL-blocking under NETEM packet loss.  Requires
//! `scripts/netem.sh` (Linux + `tc`) to be run before execution.
//! On non-Linux systems the benchmark is a no-op.
//!
//! Loss percentage is read from the `LOSS_PERCENT` env variable (default 5).
//! JSON report goes to `bench-output/`.

#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::future::join_all;
use tokio::runtime::Runtime;

use grpc_quic_benches::{
    make_payload, make_quic_client, make_tcp_client, make_tls_configs, record_latency,
    setup_servers, shutdown_servers, take_histogram, BenchResult, CONCURRENCY, LOSS_PERCENTS,
};

/// Fixed payload that spans multiple segments.
const PAYLOAD_SIZE: usize = 4096;

fn bench_loss(c: &mut Criterion) {
    let loss = std::env::var("LOSS_PERCENT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(LOSS_PERCENTS[2]);

    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);
    std::thread::sleep(Duration::from_millis(200));

    let quic_client = make_quic_client(&rt, servers.quic_addr, client_tls);
    let tcp_client = make_tcp_client(&rt, servers.tcp_addr);

    rt.block_on(async {
        let w = make_payload(PAYLOAD_SIZE);
        let _ = quic_client
            .clone()
            .unary(tonic::Request::new(w.clone()))
            .await;
        let _ = tcp_client.clone().unary(tonic::Request::new(w)).await;
    });

    // ── QUIC under loss ─────────────────────────────────────────────────
    {
        let mut reports = Vec::new();
        let mut group = c.benchmark_group("quic_loss");
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(30);

        for &conc in CONCURRENCY {
            group.throughput(Throughput::Bytes((PAYLOAD_SIZE * conc) as u64));
            group.bench_with_input(BenchmarkId::new("concurrent", conc), &conc, |b, &conc| {
                let client = quic_client.clone();
                let payload = make_payload(PAYLOAD_SIZE);
                b.iter(|| {
                    rt.block_on(async {
                        let futures: Vec<_> = (0..conc)
                            .map(|_| {
                                let mut c = client.clone();
                                let p = payload.clone();
                                async move {
                                    let start = Instant::now();
                                    let resp = c.unary(tonic::Request::new(p)).await.unwrap();
                                    record_latency(start);
                                    resp
                                }
                            })
                            .collect();
                        let results = join_all(futures).await;
                        black_box(results);
                    });
                });
            });

            let hist = take_histogram();
            reports.push(BenchResult::new(
                "quic",
                "loss_sim",
                conc,
                loss,
                PAYLOAD_SIZE,
                &hist,
            ));
        }
        group.finish();
        let _ = BenchResult::save_json(&reports, "bench-output/quic_loss.json");
    }

    // ── TCP under loss ──────────────────────────────────────────────────
    {
        let mut reports = Vec::new();
        let mut group = c.benchmark_group("tcp_loss");
        group.measurement_time(Duration::from_secs(15));
        group.sample_size(30);

        for &conc in CONCURRENCY {
            group.throughput(Throughput::Bytes((PAYLOAD_SIZE * conc) as u64));
            group.bench_with_input(BenchmarkId::new("concurrent", conc), &conc, |b, &conc| {
                let client = tcp_client.clone();
                let payload = make_payload(PAYLOAD_SIZE);
                b.iter(|| {
                    rt.block_on(async {
                        let futures: Vec<_> = (0..conc)
                            .map(|_| {
                                let mut c = client.clone();
                                let p = payload.clone();
                                async move {
                                    let start = Instant::now();
                                    let resp = c.unary(tonic::Request::new(p)).await.unwrap();
                                    record_latency(start);
                                    resp
                                }
                            })
                            .collect();
                        let results = join_all(futures).await;
                        black_box(results);
                    });
                });
            });

            let hist = take_histogram();
            reports.push(BenchResult::new(
                "tcp",
                "loss_sim",
                conc,
                loss,
                PAYLOAD_SIZE,
                &hist,
            ));
        }
        group.finish();
        let _ = BenchResult::save_json(&reports, "bench-output/tcp_loss.json");
    }

    shutdown_servers(servers);
}

#[cfg(target_os = "linux")]
criterion_group!(benches, bench_loss);

#[cfg(target_os = "linux")]
criterion_main!(benches);

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("loss_sim requires Linux (tc/netem). Skipping.");
}
