//! TCP (tonic baseline) unary RPC latency.

use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tokio::runtime::Runtime;

use grpc_quic_benches::{
    make_payload, make_tcp_client, make_tls_configs, record_latency, setup_servers,
    shutdown_servers, take_histogram, BenchResult, PAYLOAD_SIZES,
};

fn bench_tcp_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, _client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);
    std::thread::sleep(Duration::from_millis(200));

    let tcp_client = make_tcp_client(&rt, servers.tcp_addr);

    rt.block_on(async {
        let _ = tcp_client
            .clone()
            .unary(tonic::Request::new(make_payload(64)))
            .await;
    });

    let mut reports = Vec::new();

    for &size in PAYLOAD_SIZES {
        let mut group = c.benchmark_group("tcp_latency");
        group.measurement_time(Duration::from_secs(10));
        group.sample_size(50);
        group.throughput(Throughput::Bytes(size as u64));

        let mut client = tcp_client.clone();
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
        reports.push(BenchResult::new("tcp", "latency", 1, 0, size, &hist));
    }

    shutdown_servers(servers);
    let _ = BenchResult::save_json(&reports, "target/bench-results/tcp_latency.json");
}

criterion_group!(benches, bench_tcp_latency);
criterion_main!(benches);
