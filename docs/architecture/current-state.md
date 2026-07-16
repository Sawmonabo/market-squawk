# Market Squawk Current State

## Document control

- Audit date: 2026-07-16
- Repository: `market-squawk`
- Branch observed: `main`
- Audited commit: `c568ef0`
- Working tree: modified by an in-progress `market-engine` to `market-squawk` rename
- Evidence: repository inspection, fresh local verification, and the persisted
  [deep-research report](../research/2026-07-15-market-squawk/final-report.md)

This document describes software that exists in the repository. Roadmap text, interfaces without a
working consumer, mocks, and synthetic fixtures are not counted as production capability.

## Executive assessment

The repository contains a runnable single-package Rust v0.1 live-market prototype. It implements a
public Coinbase Exchange WebSocket source, CRC-framed local raw capture, a single in-memory
price-level book per product, five top-of-book features, a basic feed-health state machine, a
hardcoded momentum paper bot, elementary pre-trade checks, journal replay, five local MCP tools,
and a small CLI.

It is not yet the specified Market Squawk platform. The research plane, required source adapters,
canonical instrument identity, execution-quality evidence, sharded single-writer runtime,
realistic paper execution, portfolio accounting, modeling, fair-value analysis, full MCP surface,
release audits, fuzzing, and performance evidence are absent. Two current behaviors are unsafe
against the target specification:

1. A raw journal append is acknowledged by the writer before the decoded event is published, so
   event processing waits on persistence work.
2. A snapshot can set operational quality to `Valid` and permit a paper action without proving the
   complete `DirectVerified` evidence contract.

## Repository and package shape

The root `Cargo.toml` defines one package, one library, and one binary. There is no virtual
workspace and no `apps/`, `crates/`, or `adapters/` boundary.

```text
market-squawk/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── src/
│   ├── bot.rs
│   ├── config.rs
│   ├── domain.rs
│   ├── engine.rs
│   ├── features.rs
│   ├── journal.rs
│   ├── lib.rs
│   ├── main.rs
│   ├── mcp.rs
│   ├── order_book.rs
│   ├── quality.rs
│   ├── replay.rs
│   ├── risk.rs
│   └── source/
│       ├── coinbase.rs
│       ├── mock.rs
│       └── mod.rs
├── tests/
│   ├── coinbase_decode.rs
│   ├── coinbase_source.rs
│   ├── engine.rs
│   ├── journal.rs
│   ├── order_book.rs
│   ├── quality.rs
│   ├── replay.rs
│   └── risk.rs
└── scripts/
    ├── smoke_mcp.py
    └── verify.sh
```

The Rust source and deterministic tests are small enough to migrate deliberately. No existing
source file exceeds 500 lines; `src/mcp.rs` is the largest at 441 lines.

## Toolchain, metadata, and lints

### Implemented

- Rust Edition 2024 is declared.
- `rustfmt` uses a 100-character width.
- `Cargo.lock` exists and locked builds work.
- `unsafe_code = "forbid"` is configured for the package.
- The release profile enables optimization, thin LTO, one codegen unit, abort-on-panic, and symbol
  stripping.

### Mismatches

- `rust-toolchain.toml` pins Rust 1.85.0.
- `Cargo.toml` declares `rust-version = "1.85"`.
- The required version is Rust 1.97.0, released 2026-07-09.
- The locally installed default `stable` toolchain is Rust 1.95.0; Rust 1.97.0 is not installed in
  the audited environment.
- There is no `[workspace]` table or explicit `resolver = "3"`.
- Package metadata is not inherited from `[workspace.package]`.
- Clippy `all` is only `warn`, not `deny`.
- The required `cargo`, `unwrap_used`, `expect_used`, `panic`, `todo`, and `unimplemented` policies
  are absent.
- Library modules use `anyhow` rather than typed `thiserror` errors.
- Public APIs do not consistently document units, invariants, time semantics, or errors.

## Dependency state

The package currently depends on Tokio, Serde, Tokio-Tungstenite, Chrono, Rust Decimal, Clap,
Tracing, UUID, CRC32, `parking_lot`, `fs2`, `futures-util`, `async-trait`, and `anyhow`.

Missing target dependencies include Arrow, Parquet, DataFusion, SQLite integration, Reqwest,
`thiserror`, cancellation-token support, Proptest, Criterion, fuzzing infrastructure, native or
ONNX-compatible inference, secret storage, encryption support, and dependency/advisory/license
policy tooling.

The audit does not infer target versions merely from research. Exact releases, features, MSRVs,
transitive licenses, and native artifacts require a locked Rust 1.97 build spike.

## Domain model

`src/domain.rs` exposes `RawEnvelope`, `Side`, `PriceLevel`, `BookChange`, and `MarketEvent`.
`MarketEvent` currently contains book snapshots, book deltas, trades, heartbeats, and source status.
Prices and quantities are parsed as `rust_decimal::Decimal`, avoiding binary floating point.

Source names and products are unvalidated `String` fields, public fields expose invariants, and
price/quantity values do not carry an instrument tick or lot scale. The repository has no:

- `InstrumentId`, `VenueId`, `SourceId`, or provider-independent identity
- Identifier registry for ticker, CUSIP, ISIN, SEDOL, FIGI, OCC, futures, crypto, or chain address
- Symbol history, merger, delisting, contract roll, or corporate-action identity policy
- `SequenceNumber`, `PriceTicks`, `QuantityLots`, or `BasisPoints`
- Currency type or currency-aware `Money`
- Explicit rounding policy or checked live-path scaled-integer representation
- Schema-versioned canonical record envelope
- Complete provenance, source reference, or payload hash on canonical events
- Research observation hierarchy or research time/revision semantics

## Classification state

`src/quality.rs` defines operational states: `Initializing`, `Valid`, `Stale`, `GapDetected`,
`ChecksumFailed`, `Divergent`, and `Quarantined`. These are useful stream-integrity states, but they
do not implement the required `DataQuality` taxonomy.

There is no `FairValueHierarchy`, `MarketDepth`, `ExecutionEligibility`, source coverage type, or
qualification-evidence record. `QualityState::Valid` currently serves as the paper-risk tradability
gate, which is broader than `DirectVerified` and therefore unsafe for the target contract.

## Live source framework

The current source trait is:

```rust
#[async_trait]
pub trait MarketSource: Send {
    async fn run(
        self: Box<Self>,
        journal: JournalSink,
        events: mpsc::Sender<MarketEvent>,
        cancel: watch::Receiver<bool>,
    ) -> anyhow::Result<()>;
}
```

It combines raw capture, decoded publication, and cancellation in one live-only contract. It has no
source metadata provider, coverage declaration, capability declaration, typed source error, raw
sink abstraction, discovery contract, or extraction contract.

### Coinbase adapter

The working adapter connects to Coinbase Exchange, subscribes to `level2`, `heartbeat`, and
`matches`, reconnects with capped exponential backoff, journals exact text frames, parses decimals,
and emits snapshot, delta, heartbeat, trade, subscription, error, and connection-status events. It
also supports an alternate URL for local integration testing.

It does not:

- Maintain Coinbase full-channel sequence replay.
- Establish update-level sequence continuity for Level 2 messages.
- Validate a Level 2 checksum; the chosen official channel documents none.
- Validate an internal instrument and venue mapping.
- Enforce instrument tick and lot precision from status metadata.
- Record source coverage as single-venue/non-consolidated.
- Carry connection generation in canonical events.
- Qualify the stream as `DirectVerified` with complete evidence.
- Apply a typed endpoint allowlist, proxy policy, redirect policy, or timeout policy.

### Other sources

The only other source is deterministic `MockSource`. Its behavior is appropriate for tests and
offline smoke runs, but it is currently compiled into the production library and exposed as a CLI
source through `market-squawk mock`. That representation is incorrect under the target contract;
the implementation must move behind test support or an explicitly diagnostic generator that cannot
enter production source registration or execution qualification. Kraken and all required extraction
adapters are missing.

## Raw capture and replay

Implemented capture behavior includes `MSJ1` magic, length/CRC32/JSON records, a 64 MiB record cap,
full validation before append, an OS single-writer lock, truncation/corruption detection, a bounded
writer queue, and durable flush at checkpoints and shutdown. Coinbase replay uses the current
decoder and engine.

`JournalSink::append` sends an append command and awaits a one-shot acknowledgement. The Coinbase
reader awaits that function before decoding and publishing the event. Processing therefore depends
on the journal task consuming the queue and performing serialization and buffered file writes. It
does not call `sync_data` per event, but it still violates persistence independence.

The committed predecessor used `MEJ1` and `.mej`; the working tree changes those to `MSJ1` and
`.msj`. There is no dual-reader compatibility or explicit migration, so existing user data can
become unreadable after the rename.

## Live engine and concurrency

`Engine` owns every product in one `HashMap`. A single Tokio task receives all events through a
bounded channel and obtains a write lock on `Arc<RwLock<Engine>>` for every event. MCP reads through
the same lock.

Implemented integrity behavior includes delta-before-snapshot quarantine, disconnect invalidation,
crossed-book quarantine, heartbeat/book-freshness separation, and fresh-snapshot recovery.

Missing or incorrect behavior includes:

- No stable `(venue_id, instrument_id)` shard function or versioned hash
- No per-shard state owner
- No queue overflow event or financial integrity policy
- `send().await` pauses readers instead of invalidating an overloaded stream
- No connection-generation, sequence, checksum, precision, trading-status, or coverage gate
- No bounded immutable snapshot publication
- MCP contention on the global engine lock
- No measured memory bound under sustained bursts

## Order books and online features

The current price-level book supports snapshot replacement, absolute-size updates, delete-on-zero,
best bid/ask, level counts, and crossed-book detection. It lacks configurable depth, order-level
state, venue checksum views, message-atomic validation, and property tests.

The current feature set is midpoint, spread, spread basis points, microprice, and top-level
imbalance. Order-flow imbalance, depth-weighted price, aggressor imbalance, rolling VWAP, volume
velocity, momentum, returns, volatility, cross-venue divergence, liquidity, and slippage estimates
are missing.

## Strategy, risk, and paper execution

`MomentumBot` keeps a 20-observation midpoint history. A five-basis-point move produces a fixed
quantity limit intent at the midpoint. `RiskKernel` checks kill switch, operational feed state,
book age, positive values, notional, and absolute position. `PaperAccount::fill` immediately fills
the entire quantity at the intent price.

Missing or unsafe elements include:

- No canonical `Strategy` or `ExecutionAdapter` trait
- Incomplete order intent: no model, order type, time in force, expiration, slippage, required
  quality, or structured reasons
- No unforgeable `ApprovedOrder` boundary
- No account/instrument eligibility, exposure, leverage, capital, loss, drawdown, order-rate, or
  duplicate controls
- Risk gates `QualityState::Valid`, not `DirectVerified`
- No order-state machine, latency, spreads, queue position, slippage, fees, partial fills, rejects,
  cancellation, balances, reconciliation, or idempotency key
- No durable audit record for each risk decision

## Research and analytical plane

The research plane is missing. There is no SQLite catalog, Arrow batch, Parquet dataset writer,
manifest, schema registry, compaction, DataFusion session, point-in-time filter, research
observation model, dataset builder, or lineage store.

SEC, FRED/ALFRED, BLS, Treasury, CSV, JSON/NDJSON, Parquet, and portfolio adapters are absent. No
provider budget, SEC identity, FRED key handling, BLS chunking, Treasury pagination, cache,
bulk-reconciliation path, or separately gated network tests exist.

## Analytics, modeling, portfolio, and valuation

Apart from five online features, the batch analytics, feature registry, point-in-time dataset,
label/leakage controls, model bundles, inference backends, backtester, and predictions are missing.

`PaperAccount` is not a portfolio system. Accounts, source imports, holdings, transactions, cash
flows, cost basis, lots, income, performance, exposures, attribution, rebalancing, reconciliation,
and scenarios are missing.

Fair-value types, evidence, methods, classification rules, ruleset versions, overrides, approvals,
and explanations are also missing.

## MCP and CLI

The hand-written stdio server implements initialization, ping, tool listing/calling, and five tools:
`Market.GetSnapshot`, `Market.GetQuality`, `Bot.GetStatus`, `Journal.GetSummary`, and
`Risk.TriggerKillSwitch`. It keeps protocol output on stdout, bounds request lines, rejects unknown
arguments, and applies a per-process call limit.

It lacks complete lifecycle enforcement, progress/cancellation, deadlines, per-tool result/time/
instrument bounds, audit persistence, controlled artifacts, most required domains, and a shared
application-service layer. Snapshot output can grow with every observed product.

The CLI implements `init`, `mock`, `capture`, `mcp`, and `replay`. The remaining required command
groups are missing, and commands call concrete engine/source code rather than complete shared
services.

## Configuration, privacy, and operations

Implemented elements are local storage, a few CLI/environment overrides, human or JSON tracing to
stderr, no telemetry beacon, no cloud dependency, and no live credentials.

Missing elements include the local configuration file, full precedence model, typed source config,
endpoint allowlists, secret redaction tests, OS keyring/encrypted fallback, provider budgets,
controlled artifacts, persistent source health/cursors, structured audit, SQLite lifecycle,
service supervision, dependency/advisory/license policy, SBOM, and build provenance.

The project correctly documents that quota evasion, identity rotation, anti-bot bypass, stealth
scraping, and access-control circumvention are non-goals.

## Tests and verification

Fresh audit results:

```text
cargo fmt --all -- --check                           PASS
cargo clippy --locked --all-targets --all-features   PASS
cargo test --locked --all-targets --all-features     PASS (24 tests)
cargo build --release --locked                       PASS
gitleaks git --redact                                PASS
./scripts/verify.sh                                  FAIL (missing scripts/check_brand.py)
```

The deterministic suite covers Coinbase decoding and a local WebSocket session, journal integrity,
book updates, operational quality, replay, engine recovery, and basic risk. External network tests
are not in the default suite.

Missing verification includes Rust 1.97 workspace commands, property tests, fuzz targets,
benchmarks, sustained-burst/memory tests, point-in-time and revision tests, model/portfolio/
valuation/full-MCP tests, dependency and license audits, working-tree credential scanning,
generated-artifact checks, SBOM, and provenance validation.

## Working-tree ownership and migration risk

The working tree contains an uncommitted product rename created before this audit. Those changes
belong to the user and must be preserved. Before workspace migration:

1. Review the rename diff and add the missing brand checker.
2. Add dual journal read compatibility.
3. Verify current behavior.
4. Commit the rename as a focused baseline, or obtain explicit direction to carry it uncommitted.
5. Create an isolated worktree for Stage 1 execution.

## Conclusion

The prototype supplies tested behavior and fixtures, but its types and concurrency must not become
the implicit contracts for the full product. The safe migration preserves working Coinbase,
journal, book, CLI, and MCP behavior while replacing primitive identity, operational-only quality,
synchronous capture acknowledgement, the global engine lock, and direct paper filling with the
target domain, qualification, sharding, risk, and execution boundaries.
