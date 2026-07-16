# Market Engine

A local-first Rust engine for live market-data capture, loss-aware order-book processing, deterministic replay, incremental financial features, paper-only bot evaluation, and Model Context Protocol access.

## Status

`v0.1.0` is a runnable local foundation, not a production brokerage system.

Implemented now:

- Public Coinbase Exchange WebSocket adapter
- Level 2 snapshots and updates
- Heartbeat sequence tracking separated from order-book freshness
- Match/trade capture
- Append-only length-prefixed raw journal with CRC32 integrity checks and a single-writer OS lock
- Exact journal validation plus Coinbase state reconstruction through replay
- Fixed-point decimal prices and quantities
- In-memory order books
- Midprice, spread, spread basis points, microprice, and book imbalance
- Latched feed-quality state machine requiring a fresh snapshot after hard failures
- Deterministic pre-trade risk checks
- Optional paper-only momentum bot
- Local stdio MCP server with strict schemas and per-process tool-call limiting
- Deterministic mock source for offline verification

Deliberately not implemented in v0.1:

- Live order submission
- Credentialed exchange or brokerage access
- Quota bypassing, stealth scraping, identity rotation, or access-control evasion
- Full consolidated equities, options, futures, or fixed-income feeds
- Distributed deployment
- OpenTelemetry infrastructure
- Arbitrary MCP SQL, shell, network, or filesystem access

## Why Rust

The live path needs predictable memory use, native execution, safe concurrency, fixed-point financial values, and a single local binary. Python remains the intended research and model-training consumer through journal exports, Parquet in later versions, or a future Arrow/PyO3 interface. Python is not placed between a live event and a paper-bot decision.

## Quick start

Prerequisites:

- Rust 1.85 or newer
- Internet access only for dependency installation and live Coinbase capture

```bash
cargo build --release --locked

# Create local state
./target/release/market-engine init

# Fully offline deterministic smoke run
./target/release/market-engine mock --events 100

# Capture public BTC-USD and ETH-USD data for 30 seconds
./target/release/market-engine capture \
  --products BTC-USD,ETH-USD \
  --seconds 30

# Validate the journal and rebuild the ending market state
./target/release/market-engine replay --source coinbase-exchange
```

All local data defaults to `.market-engine/`. Override it with `--data-dir` or `MARKET_ENGINE_DATA_DIR`.

## Local MCP server

Offline mode is useful for verifying protocol integration without opening a market-data connection:

```bash
market-engine mcp --offline
```

Live mode starts the Coinbase source and MCP server in the same process:

```bash
market-engine mcp --products BTC-USD,ETH-USD
```

Generic MCP client configuration:

```json
{
  "mcpServers": {
    "market-engine": {
      "command": "/absolute/path/to/market-engine",
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
| `Market.GetSnapshot` | Read | Latest local books and incremental features |
| `Market.GetQuality` | Read | Quality states, timestamps, sequences, and gaps |
| `Bot.GetStatus` | Read | Paper account, fills, positions, and risk state |
| `Journal.GetSummary` | Read | Validate and summarize the configured journal |
| `Risk.TriggerKillSwitch` | Restricted mutation | Irreversibly stop paper order approval for the current process |

The MCP server does not accept arbitrary paths, SQL, shell commands, remote code, or unchecked order requests.

## Live data path

```text
Coinbase WebSocket
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
                                       risk kernel
                                           │
                                           ▼
                                      paper fill only
```

No database, LLM, MCP request, notebook, or filesystem query is in the event-to-decision path. Journal writes use a bounded asynchronous queue. The source waits for an in-process writer acknowledgement before publishing the decoded event. If the writer disappears or the queue cannot accept data, capture fails rather than silently discarding raw market data. Durability flushes occur at explicit checkpoints and shutdown so the hot path does not fsync every message.

## Data integrity model

The engine distinguishes source capture from market truth. It guarantees that locally accepted journal records can be checksummed, replayed, and traced to the raw source frame. It does not claim that any external venue or free provider is globally complete or infallible.

Quality states include:

- `INITIALIZING`
- `VALID`
- `STALE`
- `GAP_DETECTED`
- `CHECKSUM_FAILED`
- `DIVERGENT`
- `QUARANTINED`

Paper orders are rejected unless the relevant book is `VALID`, recently updated by a snapshot or delta, within notional and position limits, and the kill switch is inactive. Heartbeats are tracked separately and never make a stale book fresh.

## Journal format

Each `.mej` file starts with `MEJ1`, followed by records:

```text
u32 little-endian payload length
u32 little-endian payload CRC
UTF-8 JSON RawEnvelope payload
```

The raw envelope preserves:

- Event ID
- Source
- Connection ID
- Source sequence when supplied
- Exchange timestamp when supplied
- Local receive timestamp
- Exact raw payload bytes

A future format version may add segmented files, stronger cryptographic segment manifests, compression, and Arrow/Parquet compaction. Existing versions remain independently readable.

## Paper bot

The optional bot exists to exercise the complete live path without risking capital:

```bash
market-engine capture --products BTC-USD --paper-bot
```

It is intentionally simple and not an investment recommendation. It generates fixed-size momentum intents after a warm-up window. Every intent passes through the deterministic risk kernel before a paper fill is recorded.

## Local verification

```bash
./scripts/verify.sh
```

This runs locked dependency verification, formatting checks, Clippy with warnings denied, 24 tests, and an offline mock smoke run.

To exercise MCP after building:

```bash
python3 scripts/smoke_mcp.py ./target/debug/market-engine
```

## Repository boundaries

```text
src/
├── bot.rs             paper intent and fill state
├── config.rs          local paths and runtime configuration
├── domain.rs          canonical raw and market events
├── engine.rs          deterministic event coordinator
├── features.rs        incremental order-book features
├── journal.rs         CRC-framed immutable raw capture
├── mcp.rs             bounded local stdio MCP interface
├── order_book.rs      single-writer local book state
├── quality.rs         fail-closed data-quality state
├── replay.rs          journal validation and summaries
├── risk.rs            deterministic order-intent checks
└── source/
    ├── coinbase.rs    public Coinbase Exchange adapter
    └── mock.rs        deterministic offline source
```

## Zero-cost boundary

The project has no mandatory paid software, API, cloud, telemetry, or hosted database. Existing hardware, storage, electricity, and internet access are outside the software-cost claim. External market providers retain their own coverage, availability, licensing, and rate constraints.

The architecture removes avoidable vendor dependence through adapters, immutable local capture, exact replay, and fail-closed degradation. It does not attempt to evade legitimate provider restrictions.

## Roadmap

1. Segmented journals with SHA-256 manifests and crash recovery indexes
2. Kraken Level 2 adapter with checksum validation
3. Arrow record batches and Parquet compaction
4. Apache DataFusion point-in-time query layer
5. Corporate actions and total-return series
6. SEC EDGAR, FRED/ALFRED, Treasury, BLS, and portfolio import adapters
7. Generalized historical replay through the same event pipeline
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
