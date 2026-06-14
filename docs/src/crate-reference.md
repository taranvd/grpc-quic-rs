# Crate Reference

## `grpc-quic-core`

HTTP/3 + gRPC core layer — h3 connection builders, body adapters, error types.

**Key types:**

| Type | Description |
|---|---|
| `H3ClientSession` | Cloneable HTTP/3 client session (Arc\<Mutex\<SendRequest>>) with background `poll_close` driver |
| `ServerRecvBody` | `http_body::Body<Data=Bytes>` wrapping an h3 server `RecvStream` |
| `ClientRecvBody` | `http_body::Body<Data=Bytes>` wrapping an h3 client `RecvStream` |
| `CoreError` | Error type bridging h3 stream/connection errors |

## `grpc-quic-transport`

Low-level QUIC primitives wrapping `quinn` and `rustls`. No tonic dependency.

**Key types:**

| Type | Description |
|---|---|
| `QuicEndpoint` | Server/client endpoint for accepting/initiating QUIC connections |
| `QuicConnection` | Established QUIC connection, can open/accept bi-directional streams |
| `TlsConfig` | Unified TLS configuration (server + client sides, mTLS support) |
| `TransportError` | Error enum: endpoint, connection, stream, TLS, or I/O errors |

## `grpc-quic-client`

`QuicChannel` — a `tower::Service` adapter that makes HTTP/3-over-QUIC streams
look like an HTTP transport to tonic.

**Key types:**

| Type | Description |
|---|---|
| `QuicChannel` | Cloneable channel implementing `tower::Service<http::Request<BoxBody>>` |
| `QuicChannelBuilder` | Builder for `QuicChannel` with retry, TLS, SNI, and resolver options |
| `ConnectionPool` | Reuses QUIC connections across RPC calls per remote address |
| `RetryPolicy` | Exponential backoff configuration for transient failures |
| `ClientError` | Error enum: transport, retries exhausted, closed, I/O, invalid response |

## `grpc-quic-server`

`QuicServer` — accepts incoming QUIC connections and dispatches streams to a
tonic service.

**Key types:**

| Type | Description |
|---|---|
| `QuicServer` | Server with graceful shutdown via `serve_with_shutdown()` |
| `QuicServerBuilder` | Builder for `QuicServer` with TLS and stream limits |
| `ServerRecvBody` | `http_body::Body<Data=Bytes>` bridging h3 `RecvStream` to tonic |
| `ServerError` | Error enum: transport, invalid request, I/O, shutdown |

## `grpc-quic-metrics`

Prometheus counters and tracing span helpers for observability.

**Metrics:**

| Counter | Labels | Description |
|---|---|---|
| `grpc_quic_connections_total` | `role` (client/server) | Total connections established |
| `grpc_quic_streams_total` | `role` | Total streams opened |
| `grpc_quic_requests_total` | `role`, `path` | Total gRPC requests dispatched |
| `grpc_quic_reconnects_total` | — | Total reconnection attempts |
| `grpc_quic_bytes_sent` | `role` | Total bytes written to streams |
| `grpc_quic_bytes_received` | `role` | Total bytes read from streams |

## `grpc-quic-discovery`

Service discovery abstraction.

**Key types:**

| Type | Description |
|---|---|
| `Resolver` trait | Resolve service name → `Vec<SocketAddr>` |
| `StaticResolver` | Static list of `(name, addr)` pairs |

## `grpc-quic` (facade)

Re-exports all sub-crates under feature flags:

```toml
[dependencies]
grpc-quic = { features = ["full"] }
```
