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

# Build API documentation with rustdoc
doc:
    cargo doc --workspace --no-deps --open

# Build API docs to a separate dir (used by mdbook)
docs-api:
    cargo doc --workspace --no-deps --document-private-items --target-dir target/api-docs

# Check that mdbook + mermaid are installed
check-docs:
    @which mdbook > /dev/null || (echo "Install mdbook: cargo install mdbook" && exit 1)
    @which mdbook-mermaid > /dev/null || (echo "Install mdbook-mermaid: cargo install mdbook-mermaid" && exit 1)

# Build the mdbook documentation
docs-build: check-docs
    mdbook-mermaid install docs/ 2>/dev/null || true
    mdbook build docs

# Serve the mdbook documentation locally
docs-serve: check-docs
    mdbook-mermaid install docs/ 2>/dev/null || true
    mdbook serve docs --open

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
