# grpc-quic-rs

**Custom QUIC transport for tonic gRPC** — replaces HTTP/TCP with QUIC streams
while preserving full gRPC semantics.

## Motivation

Standard gRPC runs over HTTP/2 over TCP. While HTTP/2 solves head-of-line
blocking at the application layer, TCP still suffers from HOL blocking at the
transport layer. A single lost packet stalls all multiplexed streams.

**QUIC** (RFC 9000) eliminates TCP HOL blocking by giving each stream
independent loss recovery. Combined with TLS 1.3 built into the handshake,
QUIC offers:

- **Lower latency**: 0-RTT resumption for repeat connections
- **No HOL blocking**: independent stream loss recovery
- **Connection migration**: survives IP changes (e.g. mobile roaming)
- **Built-in encryption**: no separate TLS layer

`grpc-quic-rs` gives tonic services all of this with **zero changes** to your
protobuf definitions or service implementations.

## Design Principle

> grpc-quic-rs does NOT modify gRPC semantics.
> It only replaces the transport layer (TCP → QUIC).
> All gRPC payload bytes are forwarded verbatim — never interpreted or re-encoded.

## Project Status

All core functionality is implemented and tested. See the [Roadmap](roadmap.md) for details.
