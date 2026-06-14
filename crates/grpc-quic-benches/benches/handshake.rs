//! Connection handshake latency: QUIC vs TCP.

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use grpc_quic::client::QuicChannel;
use grpc_quic_benches::{make_tls_configs, setup_servers, shutdown_servers};

fn bench_handshake(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (server_tls, client_tls) = make_tls_configs();
    let servers = setup_servers(&rt, server_tls);
    std::thread::sleep(Duration::from_millis(200));

    let tcp_addr = servers.tcp_addr;
    let quic_addr = servers.quic_addr;

    let mut qg = c.benchmark_group("quic_handshake");
    qg.measurement_time(Duration::from_secs(10));
    qg.sample_size(50);
    qg.bench_function("connect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let ch = QuicChannel::builder()
                    .tls(client_tls.clone())
                    .connect(quic_addr.to_string())
                    .await
                    .unwrap();
                black_box(ch);
            });
        });
    });
    qg.finish();

    let mut tg = c.benchmark_group("tcp_handshake");
    tg.measurement_time(Duration::from_secs(10));
    tg.sample_size(50);
    tg.bench_function("connect", |b| {
        b.iter(|| {
            rt.block_on(async {
                let ch = tonic::transport::Endpoint::new(format!("http://{tcp_addr}"))
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                black_box(ch);
            });
        });
    });
    tg.finish();

    shutdown_servers(servers);
}

criterion_group!(benches, bench_handshake);
criterion_main!(benches);
