# Verification Records

## Historical pre-workspace record: Rust 1.85.0

The following completed successfully in the original single-package artifact environment on July
15, 2026. It is retained as historical evidence and does not describe the current workspace gate:

```bash
cargo fmt --all -- --check
cargo check --locked --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
./scripts/verify.sh
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
cargo build --release --locked
python3 scripts/smoke_mcp.py ./target/release/market-squawk
```

Results:

- Rust compilation succeeded for all targets and features.
- Clippy completed with warnings denied.
- All 24 unit and integration tests passed.
- The offline 101-event mock ingestion smoke test passed.
- MCP initialization and tool discovery passed against debug and optimized release binaries.
- A local synthetic WebSocket server exercised Coinbase subscription, snapshot and heartbeat receipt, raw journaling, decoding, event publication, and clean cancellation.
- The optimized Linux x86-64 binary built successfully and is approximately 3.3 MB after symbol stripping.
- Rust source parsing, TOML parsing, Bash syntax validation, and Python bytecode compilation also passed.
- Repository scans found no production `unwrap`, `expect`, `panic!`, `todo!`, or unsafe block.

## External-network boundary

The sandbox could not contact the public Coinbase endpoint, so an external live capture was not claimed. The adapter was verified end to end against a local WebSocket server using Coinbase-format messages. A normal local machine with outbound network access should run this final source check:

```bash
market-squawk capture --products BTC-USD --seconds 10
market-squawk replay --source coinbase-exchange
```

## Reproducibility

`Cargo.lock` is committed. Local and CI verification use `--locked` so dependency resolution cannot silently drift during a build.

## Current Rust 1.97 workspace gate

The repository now pins Rust 1.97.1 and runs one fail-fast local/CI entry point. Rust 1.97.0 is
explicitly ineligible for release or performance evidence because of its critical LLVM
miscompilation:

```bash
./scripts/verify.sh
```

That entry point checks the brand allowlist, Python policy-helper tests, workspace inheritance,
reviewed duplicate-dependency inventory, workspace formatting, strict all-target/all-feature
Clippy, the locked all-target/all-feature workspace suite, an explicit locked all-feature workspace
doctest pass, a release build, rustdoc with warnings denied, CLI help, the deterministic 101-event
offline mock, and a timeout-bounded local stdio MCP interaction. A policy-helper regression test
pins the exact `cargo test --doc --workspace --all-features --locked` command so document examples
cannot silently fall out of the gate. `cargo doc` remains a separate warning-denied documentation
build; it is not treated as a substitute for running doctests.

The historical 24-test count above applies only to the pre-workspace artifact. Current harness and
doctest counts are read from the fresh release-gate transcript and will be recorded in the
commit-specific Stage 1 verification record after the Quarter 1 correction review. This living
overview intentionally does not copy a count across commits whose test corpus changed.
