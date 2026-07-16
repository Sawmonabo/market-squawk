# Verification Record

## Verified with Rust 1.85.0

The following completed successfully in the artifact environment on July 15, 2026:

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
