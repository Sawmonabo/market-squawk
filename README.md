# Market Squawk

**Turn market noise into market state.**

A local-first market platform with independent live-execution and research-data planes. They share
invariant-preserving financial, identity, time, quality, and provenance contracts without requiring
historical datasets to originate from or reproduce the live feed.

## Status

`v0.1.0` is a runnable local foundation, not a production brokerage system.

Implemented production foundation contracts:

- Rust 1.97 virtual workspace with invariant-preserving shared domain contracts
- Distinct fair-value, market-depth, data-quality, stream-integrity, and capture-integrity types
- Typed internal/external instrument identities, scaled financial values, provenance, and separate
  canonical live/research observation families
- Opaque current-source/live authority, deterministic single-writer sharding, transactional books,
  and bounded immutable snapshots
- Independent live and research boundaries; research storage and extraction adapters remain later
  implementation stages and do not depend on captured live journals

Runnable diagnostic compatibility capabilities:

- Public Coinbase Exchange WebSocket reader with single-venue, partial coverage
- Level 2 price-level snapshots and incremental updates
- Heartbeat sequence tracking separated from order-book freshness
- Match/trade capture
- Append-only length-prefixed raw journal with CRC32 integrity checks and a single-writer OS lock
- Exact journal validation plus optional Coinbase diagnostic reconstruction
- Fixed-point decimal prices and quantities
- In-memory order books
- Midprice, spread, spread basis points, microprice, and book imbalance
- Latched feed-quality state machine requiring a fresh snapshot after hard failures
- Diagnostic pre-trade calculation
- Optional diagnostic paper-only momentum simulation
- Local diagnostic stdio MCP server with strict schemas and per-process tool-call limiting
- Deterministic mock source for offline verification

These compatibility capabilities are authority-free. Their app-local `QualityState::Valid` is not
canonical `DataQuality::DirectVerified`, cannot enter the production live runtime, and can never
authorize a production order. All bot/fill behavior described below is paper simulation only.

Permanently excluded: Market Squawk will not implement identity/account rotation to evade limits,
browser/TLS fingerprint concealment, CAPTCHA or anti-bot bypass, blocking-evasion proxy rotation,
distributed quota evasion, stealth scraping, or any other access-control circumvention.

Not yet implemented in the current foundation:

- Live order submission
- Credentialed exchange or brokerage access
- Full consolidated equities, options, futures, or fixed-income feeds
- Distributed deployment
- OpenTelemetry infrastructure
- Arbitrary MCP SQL, shell, network, or filesystem access

## Why Rust

The live path needs predictable memory use, native execution, safe concurrency, fixed-point
financial values, and a single local binary. Python remains the intended research and model-training
consumer through journal exports and, in later stages, Parquet or an Arrow/PyO3 interface. Python is
not placed between a live event and a paper-bot decision.

## Quick start

Prerequisites:

- Rust 1.97.0 (pinned by `rust-toolchain.toml`)
- Internet access only for dependency installation and live Coinbase capture

```bash
cargo build --workspace --all-features --release --locked

# Create local state
./target/release/market-squawk init

# Fully offline deterministic smoke run
./target/release/market-squawk mock --events 100

# Diagnostic capture of Coinbase Exchange single-venue, partial-coverage data for 30 seconds
./target/release/market-squawk capture \
  --products BTC-USD,ETH-USD \
  --seconds 30

# Optionally validate a capture journal and reconstruct ending state for diagnostics
./target/release/market-squawk replay --source coinbase-exchange
```

All local data defaults to `.market-squawk/`. Override it with `--data-dir` or `MARKET_SQUAWK_DATA_DIR`.

## Local MCP server

Offline mode is useful for verifying protocol integration without opening a market-data connection:

```bash
market-squawk mcp --offline
```

Diagnostic live-display mode starts the Coinbase Exchange compatibility reader and MCP server in
the same process. It does not create `DirectVerified` authority:

```bash
market-squawk mcp --products BTC-USD,ETH-USD
```

Generic MCP client configuration:

```json
{
  "mcpServers": {
    "market-squawk": {
      "command": "/absolute/path/to/market-squawk",
      "args": [
        "--data-dir",
        "/absolute/path/to/market-data",
        "mcp",
        "--products",
        "BTC-USD,ETH-USD"
      ]
    }
  }
}
```

The server writes protocol responses only to stdout. Operational logs go to stderr. Local stdio access inherits the operating-system identity of the process that launches it, and tool calls are schema-validated and rate-limited.

### MCP tools

| Tool | Access | Purpose |
|---|---|---|
| `Market.GetSnapshot` | Read | Authority-free diagnostic snapshot from Coinbase Exchange single-venue partial coverage |
| `Market.GetQuality` | Read | App-local diagnostic `QualityState`, not canonical `DataQuality` |
| `Bot.GetStatus` | Read | Diagnostic paper-only account, fills, positions, and calculation state |
| `Journal.GetSummary` | Read | Validate and summarize the configured journal |
| `Risk.TriggerKillSwitch` | Restricted mutation | Irreversibly stop paper order approval for the current process |

The MCP server does not accept arbitrary paths, SQL, shell commands, remote code, or unchecked order requests.

## Independent data planes

```text
                         shared domain contracts
            instruments · time · money · quality · provenance
                              │
              ┌───────────────┴────────────────┐
              │                                │
       live execution plane             research data plane
       direct source adapters           extraction adapters
              │                                │
       bounded live state               Arrow/Parquet datasets
              │                                │
       strategy and risk                point-in-time analytics
              └───────────────┬────────────────┘
                              │
                    local CLI, catalog, MCP
```

The planes may reuse pure mathematical kernels, but neither pipeline is a transport or persistence
requirement for the other. Historical sources may differ from live sources. Journal replay is
optional diagnostic tooling for integrity investigation and decoder reprocessing, not the research
architecture or a completion dependency.

The research plane is currently represented by shared contracts. Arrow, Parquet, DataFusion,
point-in-time datasets, and working extraction adapters are subsequent implementation stages and are
not claimed as current capabilities.

## Diagnostic compatibility data path

This runnable path exists for local capture, display, and paper simulation. It is not the
production current-authority plane and never produces `DirectVerified` data.

```text
Coinbase Exchange WebSocket (single venue, partial coverage)
        │
        ▼
raw JSON frame ──► acknowledged bounded journal queue ─► CRC-framed journal
        │
        ▼
decoder
        │
        ▼
canonical market event
        │
        ▼
order book ─► incremental features ─► optional paper bot
        │                                  │
        └────────► quality state           ▼
                                       diagnostic calculation
                                           │
                                           ▼
                                      paper fill only
```

No database, LLM, MCP request, notebook, or filesystem query is in the event-to-decision path. Journal writes use a bounded asynchronous queue. The source waits for an in-process writer acknowledgement before publishing the decoded event. If the writer disappears or the queue cannot accept data, capture fails rather than silently discarding raw market data. Durability flushes occur at explicit checkpoints and shutdown so the hot path does not fsync every message.

## Diagnostic data-integrity model

The engine distinguishes source capture from market truth. It guarantees that locally accepted journal records can be checksummed, replayed, and traced to the raw source frame. It does not claim that any external venue or free provider is globally complete or infallible.

The compatibility engine's app-local `QualityState` values include:

- `INITIALIZING`
- `VALID`
- `STALE`
- `GAP_DETECTED`
- `CHECKSUM_FAILED`
- `DIVERGENT`
- `QUARANTINED`

Diagnostic paper intents are rejected unless the compatibility book is app-locally `VALID`,
recently updated by a snapshot or delta, within calculation limits, and the diagnostic kill switch
is inactive. `VALID` never means canonical `DirectVerified` and grants no production order
authority. Heartbeats are tracked separately and never make a stale book fresh.

## Journal format

Each `.msj` file starts with `MSJ1`, followed by records:

```text
u32 little-endian payload length
u32 little-endian payload CRC
UTF-8 JSON RawEnvelope payload
```

Readers retain bounded compatibility with legacy `MEJ1/.mej` journals, but writers never create or
append that format. If a source has both formats, replay and offline MCP fail closed until the user
selects `--journal-format current` or `--journal-format legacy`; initialization never creates an
empty current journal that would shadow a sole legacy file.

The raw envelope preserves:

- Event ID
- Source
- Connection ID
- Source sequence when supplied
- Exchange timestamp when supplied
- Local receive timestamp
- Exact raw payload bytes

A future format version may add segmented files, stronger cryptographic segment manifests, compression, and Arrow/Parquet compaction. Existing versions remain independently readable.

## Diagnostic paper bot

The optional bot exists to exercise the authority-free compatibility path without risking capital:

```bash
market-squawk capture --products BTC-USD --paper-bot
```

It is intentionally simple and not an investment recommendation. It generates fixed-size momentum
intents after a warm-up window. Every intent passes through a diagnostic calculation before a
paper-only simulated fill is recorded. It has no broker connection or production execution
authority.

## Local verification

```bash
./scripts/verify.sh
```

This runs the brand, Python-helper, workspace-boundary, and exact duplicate-dependency gates;
workspace-wide formatting, strict Clippy, tests, release build, and rustdoc; then CLI, offline mock,
and timeout-bounded local MCP smoke tests. All Cargo operations that consume dependencies use the
committed lockfile.

To exercise MCP after building:

```bash
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
```

## Repository boundaries

```text
apps/
└── market-squawk/                 CLI, current live application, MCP, journal, and compatibility tests
crates/
└── market-squawk-domain/          shared financial, identity, quality, provenance, and event contracts
adapters/                           added atomically with the first working production adapter crate
scripts/                            deterministic local/CI policy and smoke gates
docs/                               architecture, plans, research, and verification evidence
```

## Zero-cost boundary

The project has no mandatory paid software, API, cloud, telemetry, or hosted database. Existing hardware, storage, electricity, and internet access are outside the software-cost claim. External market providers retain their own coverage, availability, licensing, and rate constraints.

The architecture removes avoidable vendor dependence through adapters, local persistence, caching,
explicit coverage, source health, and fail-closed degradation. It does not attempt to evade
legitimate provider restrictions.

## License

Market Squawk is available under your choice of the
[Apache License 2.0](LICENSE-APACHE) or the [MIT License](LICENSE-MIT)
(`Apache-2.0 OR MIT`). Contributions are accepted under those same terms unless explicitly stated
otherwise.

## Roadmap

1. Segmented journals with SHA-256 manifests and crash recovery indexes
2. Kraken Level 2 adapter with checksum validation
3. Arrow record batches and Parquet compaction
4. Apache DataFusion point-in-time query layer
5. Corporate actions and total-return series
6. SEC EDGAR, FRED/ALFRED, Treasury, BLS, and portfolio import adapters
7. Optional cross-adapter capture replay for diagnostics and decoder validation
8. Strategy plugin ABI and versioned model bundles
9. ONNX inference outside the source-reader threads
10. Latency histograms and queue diagnostics without OTEL deployment
11. Broker-specific paper adapters, followed only later by explicitly authorized live execution

## Primary references

- Coinbase Exchange WebSocket channels: `https://docs.cdp.coinbase.com/exchange/websocket-feed/channels`
- Kraken WebSocket v2 book checksum guide: `https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2`
- MCP stdio transport specification: `https://modelcontextprotocol.io/specification/2025-11-25/basic/transports`
- Rust installation: `https://rustup.rs/`

## Financial-use warning

This project is research infrastructure. Free market data may be incomplete, delayed, venue-specific, revised, or unavailable. Validate data rights, source quality, execution assumptions, fees, slippage, liquidity, and risk controls before relying on any result. No software can guarantee investment outcomes or universal market-data accuracy.
