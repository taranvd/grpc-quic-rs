# grpc-quic-rs Development Guide for AI Agents

This guide provides comprehensive instructions for AI agents working on the grpc-quic-rs codebase. It covers the architecture, development workflows, and critical guidelines for effective contributions.

## Project Overview

`grpc-quic-rs` is a library that enables standards-compliant gRPC transport over HTTP/3 (h3) and QUIC for `tonic`. It preserves full gRPC semantics and API compatibility while eliminating TCP Head-of-Line blocking.

## Architecture Overview

### Core Components

1. **`grpc-quic`**: Public façade that re-exports everything.
2. **`grpc-quic-transport`**: Raw QUIC primitives (`quinn` + `rustls`). No `tonic` dependency.
3. **`grpc-quic-core`**: HTTP/3 + gRPC core — `h3` connection builders, body adapters, error types.
4. **`grpc-quic-client`**: `QuicChannel` — `tonic`-compatible `tower::Service`.
5. **`grpc-quic-server`**: `QuicServer` — accepts QUIC connections, delegates to `tonic` Router.
6. **`grpc-quic-metrics`**: Prometheus counters + tracing spans.
7. **`grpc-quic-discovery`**: `Resolver` trait + `StaticResolver`.

### Key Design Principles

- **Zero Semantics Changes**: The library does not modify gRPC semantics. All payload bytes are forwarded verbatim.
- **Drop-in Replacement**: Designed to work seamlessly with existing `tonic` services and clients without changing protobuf definitions.
- **Robustness**: Relies on `tower::Service`, `tokio` for async runtime, and `quinn`/`h3` for the underlying transport.

## Development Workflow

### Code Style and Standards

1. **Formatting**: Always format code before submitting.
   ```bash
   just fmt
   ```

2. **Linting**: Run clippy to catch common mistakes.
   ```bash
   just lint
   ```

3. **Testing**: Run the full test suite.
   ```bash
   just test
   ```

### Common Contribution Types

Based on the architecture, here are typical contribution patterns:

#### 1. Small Bug Fixes (1-10 lines)
Example: Fixing error propagation in connection pooling
```rust
// Changed a single line to correctly propagate the underlying error
- Err(ClientError::Closed)
+ Err(ClientError::Transport(e))
```

#### 2. Adding Comprehensive Tests
Example: End-to-end streaming tests
```rust
#[tokio::test(flavor = "multi_thread")]
async fn test_bidirectional_streaming() {
    // Create test server and client
    let server = QuicServer::builder().tls(test_tls()).build();
    let channel = QuicChannel::builder().connect("127.0.0.1:50051").await.unwrap();
    // Verify streams don't block each other
}
```

#### 3. Making Components Generic
Example: Custom DNS resolver implementation
```rust
// Before: Hardcoded to IP resolution
- pub struct StaticResolver { ip: IpAddr }

// After: Generic over any async resolution
+ pub trait Resolver: Send + Sync {
+     fn resolve(&self, name: &str) -> Vec<SocketAddr>;
+ }
```

#### 4. Observability Enhancements
Example: Adding spans to new streams
```rust
// Add tracing context
+ #[tracing::instrument(skip(self, req))]
  pub async fn call(&mut self, req: Request<BoxBody>) -> Result<Response<Body>, Status> {
```

### Testing Guidelines

1. **Unit Tests**: Test individual adapters (e.g., `grpc-quic-core/body.rs`).
2. **Integration Tests**: Verify the full Client-Server lifecycle using a local `quinn` endpoint.
3. **Benchmarks**: Run `just bench` (Criterion) to compare performance against the standard TCP `tonic` baseline.

Example test structure:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_body_conversion() {
        // Arrange
        let input = vec![1, 2, 3];
        
        // Act
        let result = convert_to_h3_data(input).await;
        
        // Assert
        assert!(result.is_ok());
    }
}
```

### Performance Considerations

1. **Avoid Allocations in Hot Paths**: Use `Bytes` and zero-copy abstractions for gRPC payloads. Avoid `String` formatting in loop paths.
2. **Concurrency Limits**: Respect `tokio::sync::Semaphore` bounds to provide backpressure and avoid out-of-memory scenarios on the server.
3. **Async/Await**: Use `tokio` for I/O-bound operations and avoid blocking the executor.

### Common Pitfalls

1. **Don't Block Async Tasks**: Do not use `std::thread::sleep` or perform heavy synchronous IO in async functions.
2. **Handle Stream Errors Properly**: QUIC streams can be dropped at any time; ensure errors are gracefully mapped to gRPC `Status` codes.

### What to Avoid

1. **Modifying gRPC Wire Format**: Never alter the bytes of a gRPC payload.
2. **Ignoring CI failures**: `just ci` must pass completely.
3. **Large, sweeping changes**: Keep PRs focused.

### CI Requirements

Before submitting changes, ensure:

1. **Format Check**: `just fmt` introduces no diffs.
2. **Clippy**: `just lint` shows no warnings.
3. **Tests Pass**: `just test` passes successfully.

### Opening PRs

#### Titles
Use Conventional Commits with an optional scope:
`<type>(<scope>): <short description>`

**Types**: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `chore`
**Scope** (optional): `client`, `server`, `core`, `transport`, `metrics`

Examples:
- `fix(client): correctly handle GOAWAY frames`
- `perf(core): zero-copy trait bounds for HTTP/3 trailers`
- `feat(metrics): add open connection gauge`

#### Descriptions
Keep it short. Say what changed and why.
**Do:**
- Write 1–3 sentences summarizing the change
- Explain _why_ if the diff doesn't make it obvious

**Template:**
```text
Closes #<issue>

<what changed, 1-3 sentences>

<why, if not obvious from the diff>
```

### Debugging Tips

1. **Logging**: Use `tracing` crate with appropriate levels. Check `RUST_LOG=trace`.
   ```rust
   tracing::trace!(target: "grpc_quic::transport", ?frame, "Received QUIC frame");
   ```
2. **Metrics**: Verify `prometheus` counters for dropped packets or retries.

### Finding Where to Contribute

1. **Check Issues**: Look for `good-first-issue` labels.
2. **Review TODOs**: Search for `TODO` comments in the codebase.
3. **Benchmarks**: Add missing performance comparisons against `tonic` defaults.

### When to Comment

Write comments that remain valuable after the PR is merged.

##### ✅ DO: Add Value
**Explain WHY and non-obvious behavior:**
```rust
// We limit concurrency to prevent OOM on massive simultaneous requests.
// 256 is chosen to match default HTTP/2 max concurrent streams.
let semaphore = Arc::new(Semaphore::new(256));
```

##### ❌ DON'T: Describe Changes
```rust
// ❌ BAD - States the obvious
// Return the error
return Err(e);

// ❌ BAD - PR-specific context
// Fixed issue #42 where connection pool leaked
```

### Rust Style Guides

#### Type Ordering in Files

When defining structs, traits, and functions in a file, follow this ordering convention: primary type (matching the file name) comes first, followed by public auxiliary types, then private types and helpers.

```rust
use ...;

/// The primary type of this file (matches filename).
pub struct QuicChannel { ... }

impl QuicChannel { ... }

// Followed by public auxiliary types
pub struct QuicChannelBuilder { ... }

// Followed by private helper functions
async fn buffer_body() { ... }
```

### Example Contribution Workflow

1. **Create a branch**: `git checkout -b fix-retry-logic`
2. **Find code**: `rg "RetryPolicy" --type rust`
3. **Make the fix** in `crates/grpc-quic-client/src/retry.rs`.
4. **Add a test**.
5. **Run checks**: `just ci`
6. **Commit**: `git commit -m "fix(client): cap max retry backoff to 10s"`

## Quick Reference

### Essential Commands

```bash
# Full local CI check
just ci

# Format code
just fmt

# Run lints
just lint

# Run tests
just test

# Check compilation
just check
```
