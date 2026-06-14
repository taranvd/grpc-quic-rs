# Roadmap

- [x] **Phase 1** — Workspace scaffold, CI, justfile
- [x] **Phase 2** — QUIC transport: endpoints, connections, TLS
- [x] **Phase 3** — Server: QUIC acceptor → tonic Router dispatch
- [x] **Phase 4** — Client: QuicChannel + ConnectionPool + RetryPolicy
- [x] **Phase 5** — All streaming modes + examples
- [x] **Phase 6** — Prometheus metrics + tracing spans
- [x] **Phase 7** — Service discovery (Resolver trait + StaticResolver)
- [x] **Phase 8** — Documentation (this book, rustdoc, README)
- [x] **Phase 9** — Criterion benchmarks (QUIC vs TCP/tonic baseline)
- [x] **Phase 10** — Rewrite to HTTP/3 (h3 + h3-quinn), remove custom wire format

## Future Ideas

- DNS-based resolver implementation
- Connection migration support
- Load balancing across multiple server addresses
- QUIC 0-RTT for faster reconnection
- Integration with OpenTelemetry tracing
