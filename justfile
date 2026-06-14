# grpc-quic justfile
# Run `just --list` to see all available commands.

# Default: show available recipes
default:
    @just --list

# ── Build ──────────────────────────────────────────────────────────────────────

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

# ── Test ───────────────────────────────────────────────────────────────────────

test:
    cargo test --workspace

test-verbose:
    cargo test --workspace -- --nocapture

# ── Check & Lint ───────────────────────────────────────────────────────────────

check:
    cargo check --workspace --all-targets

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# ── Docs ───────────────────────────────────────────────────────────────────────

doc:
    cargo doc --workspace --no-deps --open

# ── Benchmarks ─────────────────────────────────────────────────────────────────

bench:
    cargo bench --workspace

# ── CI (mirrors GitHub Actions pipeline) ───────────────────────────────────────

ci: fmt-check lint check test

# ── Utilities ──────────────────────────────────────────────────────────────────

clean:
    cargo clean

# Generate self-signed TLS certs for local testing
gen-certs:
    cargo run --example gen-certs 2>/dev/null || echo "Run after Phase 2 examples are added"
