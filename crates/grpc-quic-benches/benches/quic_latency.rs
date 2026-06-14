//! QUIC unary RPC latency at various payload sizes.

use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use grpc_quic_benches::{
    make_payload, make_quic_client, make_tls_configs, record_latency, setup_servers,
    shutdown_servers, take_histogram, BenchResult, PAYLOAD_SIZES,
};

fn bench_quic_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);
    std::thread::sleep(Duration::from_millis(200));

    let quic_client = make_quic_client(&rt, servers.quic_addr, client_tls);

    rt.block_on(async {
        let _ = quic_client
            .clone()
            .unary(tonic::Request::new(make_payload(64)))
            .await;
    });

    let mut reports = Vec::new();

    for &size in PAYLOAD_SIZES {
        let mut group = c.benchmark_group("quic_latency");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        group.throughput(Throughput::Bytes(size as u64));

        let mut client = quic_client.clone();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let p = make_payload(size);
                    let start = Instant::now();
                    let resp = client.unary(tonic::Request::new(p)).await.unwrap();
                    record_latency(start);
                    black_box(resp.into_inner());
                });
            });
        });
        group.finish();

        let hist = take_histogram();
        reports.push(BenchResult::new("quic", "latency", 1, 0, size, &hist));
    }

    shutdown_servers(servers);
    let _ = BenchResult::save_json(&reports, "bench-output/quic_latency.json");
}

criterion_group!(benches, bench_quic_latency);
criterion_main!(benches);
