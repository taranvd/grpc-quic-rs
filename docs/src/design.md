# Design Decisions

## Why QUIC instead of TCP?

TCP HOL blocking is inherent — a single lost packet delays all multiplexed
streams behind it. QUIC gives every stream independent loss recovery, so one
slow stream cannot stall others.

## Why a custom transport instead of using quinn directly?

`quinn` provides raw QUIC streams but no gRPC integration. `grpc-quic-rs`
bridges the gap by implementing HTTP/3 (via `h3` + `h3-quinn`) and exposing
`tower::Service`, which is the interface tonic expects. This lets existing
tonic services switch to QUIC with zero code changes.

## Why HTTP/3 instead of a custom envelope?

The initial version of `grpc-quic-rs` used a custom wire format. After
several rounds of development, the project was rewritten to use **real HTTP/3**
(h3 v0.0.8 + h3-quinn v0.0.10) for these reasons:

1. **Standards compliance**: gRPC is defined to run over HTTP/2 or HTTP/3.
   A custom envelope means every new team member must learn a bespoke protocol.
2. **h3 handles framing**: pseudo-headers (`:method`, `:path`, `:authority`),
   data frames, and trailers map exactly to gRPC's needs — no need to reinvent.
3. **Ecosystem interop**: tools and middleware that understand HTTP/3 can
   inspect or proxy gRPC-quic traffic.
4. **Trailers built-in**: h3 has first-class support for trailers, which
   carry `grpc-status` and `grpc-message`. No manual trailer framing needed.

The `h3` and `h3-quinn` crates from the hyperium ecosystem provide a thin,
async-friendly HTTP/3 layer on top of quinn — no QPACK or server push complexity
is exposed when we don't need it.

## Connection Pooling

QUIC connections are expensive to establish (TLS 1.3 handshake). The
`ConnectionPool` caches connections keyed by remote address and reuses them
across RPC calls. Health checks via `is_closed()` detect dead connections
before use.

## Retry Logic

The `RetryPolicy` implements exponential backoff for transient failures
(connection drops, stream errors). Non-retryable errors (invalid responses,
protocol violations) are returned immediately.

## Security

- **TLS 1.3** mandatory — QUIC does not support unencrypted connections
- **mTLS** supported via `TlsConfig` wrapping custom `rustls::ServerConfig`/`rustls::ClientConfig`
- Server Name Indication (SNI) is configurable through `QuicChannelBuilder::server_name()`
