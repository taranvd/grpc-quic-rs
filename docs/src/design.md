# Design Decisions

## Why QUIC instead of TCP?

TCP HOL blocking is inherent — a single lost packet delays all multiplexed
streams behind it. QUIC gives every stream independent loss recovery, so one
slow stream cannot stall others.

## Why a custom transport instead of using quinn directly?

`quinn` provides raw QUIC streams but no gRPC integration. `grpc-quic-rs`
bridges the gap by implementing `tower::Service`, which is the interface tonic
expects. This lets existing tonic services switch to QUIC with zero code
changes.

## Wire format: why a custom envelope instead of HTTP/3?

HTTP/3 would add significant complexity (QPACK, server push, etc.) for no
benefit in the gRPC use case. gRPC already handles framing at the HTTP/2 layer.
We only need:

1. Path routing (the gRPC service/method name)
2. Opaque byte forwarding

A simple `[u16 path_len][path_bytes][payload]` header is sufficient.

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
