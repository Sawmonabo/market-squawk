# Market Squawk

**Turn market noise into market state.**

A local-first market platform with independent live-execution and research-data planes. They share
invariant-preserving financial, identity, time, quality, and provenance contracts without requiring
historical datasets to originate from or reproduce the live feed.

## Status

`v0.1.0` is a runnable diagnostic foundation. It is not the usable complete Market Squawk release
and it is not a production brokerage system. The linked
[historical state audit](docs/architecture/current-state.md) records its own rejected audit anchor;
it is not an exact-head inventory. The dated
[release baseline](docs/verification/usable-release-baseline.md) is also historical audit evidence.
The sections below are the current product inventory and user-facing truth. All mandatory remaining
work is bound by the single canonical
[usable complete-release implementation plan](docs/superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md).

## Runnable now

- Rust 1.97.1 workspace with invariant-preserving domain, identity, time, financial, quality,
  provenance, source-authority, sharding, transactional-book, and bounded-snapshot foundations.
- Public Coinbase Exchange WebSocket diagnostic capture with explicit single-venue, partial
  coverage. This is not a production `DirectVerified` adapter.
- A separate bounded Coinbase Exchange WebSocket v1 production-source crate with strict endpoint
  policy, capture-before-decode, exact decimal lexemes, bounded subscriptions and frames,
  cancellation, pinned protocol fixtures, source metadata, explicit single-venue partial coverage,
  durable generation/revision authority, and authoritative application composition into the
  instrument-owned live runtime. Its current qualification ceiling remains intentionally
  `DirectUnverified`, so it remains execution-ineligible.
- A bounded Kraken WebSocket v2 production-source crate for price-level books and trades, with
  capture-before-decode, official CRC32 book validation, bounded control traffic, cancellation,
  source metadata, exact registry-session/budget binding, one-use connection generations, and
  fresh-snapshot recovery after quarantine. Its current qualification ceiling is intentionally
  `DirectUnverified` because Kraken does not supply the venue sequence required by the current
  execution policy. The application authority layer therefore never invokes strategy, approval,
  dispatch, or paper execution for Kraken observations. An independent canonical risk probe also
  proves both `SourceQuality` and unsupported-settlement rejection before any paper cash,
  availability, position, or account-risk state can change.
- Level 2 price-level snapshots and updates, heartbeat tracking separated from market freshness,
  match/trade capture, fixed-point prices and quantities, and in-memory order books.
- MSJ1 append-only journal writing, CRC32 validation, a single-writer OS lock, bounded legacy read
  compatibility, and optional Coinbase diagnostic reconstruction.
- A durable local SQLite control catalog with versioned migrations, rights-bound records, immutable
  authority history, backup/restore recovery, and tamper-evident catalog state.
- Versioned Arrow analytical interchange plus authority-bound immutable Parquet publication,
  manifests, lineage, compaction, recovery, and bounded read-only DataFusion queries with
  point-in-time availability filtering. Other required extraction adapters and the complete
  point-in-time dataset builder remain release-blocking below.
- A production local-file extraction vertical for CSV/TSV, JSON/NDJSON, entity-safe XML,
  formula- and external-link-constrained Excel, allowlisted read-only SQLite exports, OFX/QFX, and
  Parquet. User-authorized capability roots, bounded parsing and decompression, revocable source
  authority, precision-preserving research time, immutable representation evidence, and the
  analytical ingestion service are composed end to end.
- Authority-free midpoint, spread, spread-basis-point, microprice, imbalance, feed-quality,
  pre-trade calculation, and paper-only momentum diagnostics.
- Immutable typed order intents plus fixed-capacity, nonblocking account risk coordination with
  exact cash, position, exposure, leverage, capital, loss, drawdown, rate, idempotency, freshness,
  eligibility, price, slippage, and expiry checks; private approval minting; one-time dispatch;
  price-bound reconciliation; and terminal, bounded audit evidence. No public unchecked order
  submission or approval-minting surface exists.
- A deterministic, bounded realistic paper-execution engine with configurable fees, latency,
  slippage, partial fills, rejections, cancellations, balances, positions, reservations,
  reconciliation, versioned recovery checkpoints, and fail-closed shutdown. The `paper-bot`
  command composes the production Coinbase source, instrument-owned live runtime, canonical risk,
  one-time dispatcher, and paper worker under one lifecycle. Coinbase remains `DirectUnverified`,
  and the command installs a no-intent strategy, so this runnable ownership path cannot authorize or
  place an order.
- A hardened five-tool local stdio MCP surface with typed schemas, bounded admission, deadlines,
  cancellation, result limits, controlled artifacts, durable audit, and bounded worker shutdown.
  Audit, artifacts, capture, and configured journal reads derive from the same prepared local
  capability graph. This is not the complete typed MCP product.
- A deterministic mock source for offline diagnostic verification. It is never represented as a
  production source.

The app-local Coinbase reader remains a compatibility path. Its app-local `QualityState::Valid` is
not canonical `DataQuality::DirectVerified` and cannot authorize an order. The MCP command uses the
sole hardened application MCP composition over that authority-free diagnostic state; there is no
second legacy MCP server or unchecked application-local handler. The separately composed production
Coinbase source can enter the production live runtime only at its declared `DirectUnverified`
ceiling; Coinbase and Kraken therefore remain execution-ineligible. All fills remain local paper
simulation; no broker adapter or live order authority is enabled.

## Required but missing

Every row below is currently `Missing`. A row becomes `Runnable` only when its real producer,
terminal consumer, focused verification, immutable evidence, and exact commit exist together.

| State | Mandatory capability | Current blocker | Closing task |
| --- | --- | --- | --- |
| `Missing` | Coinbase direct-source qualification | The bounded source-to-live-to-risk-to-dispatch-to-paper ownership path is runnable, but Coinbase remains capped at `DirectUnverified`; it cannot satisfy the `DirectVerified` execution gate, and the CLI adds a no-intent strategy | Task 2 |
| `Missing` | Kraken direct-source qualification | The production transport, decoder, checksum, exact-generation session lifecycle, fresh-snapshot recovery, and canonical risk/no-paper-mutation terminal proof exist; Kraken WebSocket v2 supplies no venue sequence satisfying the current `DirectVerified` execution predicate | Task 20 |
| `Missing` | SEC filings/XBRL/Company Facts | No lawful, revision-preserving SEC vertical | Task 8 |
| `Missing` | FRED/ALFRED | No vintage-aware production macro adapter | Task 9 |
| `Missing` | BLS | No quota-honest production macro adapter | Task 9 |
| `Missing` | US Treasury | No schema-tracked production Treasury adapter | Task 9 |
| `Missing` | portfolio import | No raw-preserving holdings/transactions reconciliation vertical | Task 10 |
| `Missing` | point-in-time datasets | No availability-aware joins or leakage-checked builder | Task 11 |
| `Missing` | Rust financial analytics | No complete tested batch-analytics implementation | Task 12 |
| `Missing` | feature registry | Versioned metadata, exact live kernels, and compatibility checks exist, but no production live-route or batch-dataset consumer closes the capability | Task 12 |
| `Missing` | Python data/financial/training product | No tracked product package or Rust-parity training boundary | Task 14 |
| `Missing` | complete model bundle | No fully hashed artifact/schema/metadata bundle | Task 13 |
| `Missing` | native Rust inference | No production local inference backend | Task 13 |
| `Missing` | constrained ONNX inference | No validated, bounded, fail-closed ONNX-compatible backend | Task 15 |
| `Missing` | research backtesting | No point-in-time research-dataset backtester | Task 17 |
| `Missing` | portfolio accounting/analytics | No lots, gains, performance, exposure, attribution, risk, or scenarios | Task 16 |
| `Missing` | strategies and comprehensive risk | Bounded account/risk coordination, actor-owned authority consumption, private approval, one-time dispatch, price-bound reconciliation, and terminal audit exist; a production order-producing strategy and its controlled user-facing configuration do not | Task 2 |
| `Missing` | realistic paper execution | The bounded engine implements lifecycle, fees, latency, slippage, partial fills, rejection, cancellation, accounting, checkpoint recovery, and reconciliation; no execution-eligible source/strategy currently drives orders through the user-facing production composition | Task 2 |
| `Missing` | ASC 820/IFRS 13 fair value | No ruleset, evidence, override, approval, or classification service | Task 18 |
| `Missing` | complete local CLI | No complete command hierarchy over shared application services | Task 19 |
| `Missing` | complete typed local MCP | No complete bounded tool domains over shared application services | Task 19 |
| `Missing` | release security/fuzz/performance gate | No exact-head release evidence or final integrated demonstration | Task 20 |

Production-hardened Coinbase and Kraken source crates are tracked under `adapters/`; their
execution-qualification verticals remain release-blocking above. The checkout has no tracked
`python/` product package. Python files under `scripts/` are repository-verification and
protocol-smoke utilities, not financial-analytics or model-training product code.

## Release blocked until implemented

Market Squawk is not a usable complete release until every mandatory capability above is a working,
bounded producer-to-consumer vertical slice; runs together locally through the CLI and complete typed
MCP where applicable; and passes the clean, unchanged exact-head release gate. Traits, schemas,
empty crates, mocks, synthetic sources, diagnostic compatibility paths, plans, and focused lane tests
do not count as implemented production capabilities.

Permanently excluded: identity or account rotation to evade limits, browser or TLS fingerprint
concealment, CAPTCHA or anti-bot bypass, blocking-evasion proxy rotation, distributed quota evasion,
stealth scraping, arbitrary MCP shell/filesystem/network/SQL authority, risk bypass, credential
access, audit deletion, and other access-control circumvention.

Only paid or licensed provider adapters, explicitly authorized live-money execution adapters, replay
beyond diagnostic and decoder-validation needs, and a possible observability adapter beyond required
local structured tracing are optional after the usable complete release. Distributed deployment,
commercial consolidated-feed coverage, and OpenTelemetry infrastructure are not release blockers.

## Why Rust

The live path needs predictable memory use, native execution, safe concurrency, fixed-point
financial values, and a single local binary. The required Python research, financial-analytics, and
model-training product is currently missing and release-blocking. Its specified boundary consumes
point-in-time Arrow/Parquet data and pure Rust analytical kernels outside the live path; Python is
never placed between a live event and an automated decision.

## Diagnostic foundation quick start

These commands demonstrate only the current authority-free diagnostic foundation. They do not
provide production execution quality, research datasets, model training or inference, portfolio
accounting, fair-value analysis, or complete MCP coverage.

Prerequisites:

- Rust 1.97.1 (pinned by `rust-toolchain.toml`)
- Cargo Deny 0.20.2
- Cargo Audit 0.22.2
- Gitleaks 8.30.1
- Internet access only for dependency installation and live Coinbase capture

Install the pinned Rust security tools locally with:

```bash
cargo install cargo-deny --version 0.20.2 --locked
cargo install cargo-audit --version 0.22.2 --locked
```

Install Gitleaks 8.30.1 from its
[upstream release](https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1), place the binary on
`PATH`, and verify the downloaded archive against the release checksum file. The current archive
SHA-256 values used by supported local/CI hosts are:

| Archive | SHA-256 |
| --- | --- |
| `gitleaks_8.30.1_darwin_arm64.tar.gz` | `b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5` |
| `gitleaks_8.30.1_darwin_x64.tar.gz` | `dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709` |
| `gitleaks_8.30.1_linux_x64.tar.gz` | `551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb` |

**Diagnostic only — build and compatibility commands:**

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

## Hardened diagnostic MCP surface

This is the hardened five-tool diagnostic stdio MCP surface, not the complete typed MCP product.
It is the application's sole MCP server path and enforces typed schemas, bounded requests and
results, cancellation and deadlines, durable local audit, controlled artifacts, and bounded worker
lifecycle. Audit, artifact, capture, and configured-journal authority share one prepared local path
capability graph; journal tools accept no caller-supplied filesystem path. Source, Research,
Fundamental, Macro, Portfolio, Analysis, Model, FairValue, Bot, and Execution coverage remains
release-blocking.

Offline mode is useful for verifying protocol integration without opening a market-data connection:

**Diagnostic only — offline five-tool MCP:**

```bash
market-squawk mcp --offline
```

Diagnostic live-display mode starts the Coinbase Exchange compatibility reader and the sole hardened
MCP composition in the same process. It does not create `DirectVerified` authority:

**Diagnostic only — single-venue partial-coverage display plus five-tool MCP:**

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

The server writes protocol responses only to stdout. Operational logs go to stderr. Local stdio
access inherits the operating-system identity of the process that launches it. Tool calls are
schema-validated, rate-limited, deadline- and cancellation-aware, result-bounded, and durably
audited before accepted mutations are reported complete.

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

The research plane currently has a working local SQLite catalog, versioned Arrow interchange,
authority-bound immutable Parquet publication and compaction, manifests, lineage, recovery, and
bounded read-only DataFusion queries. Production extraction/provider verticals and the complete
point-in-time dataset builder remain mandatory missing capabilities bound to the complete-release
plan; the release cannot pass without their real producers and terminal consumers.

## Diagnostic compatibility data path

This runnable path exists for local capture, display, and paper simulation. It is not the
production current-authority plane and never produces `DirectVerified` data.

```text
Coinbase Exchange WebSocket (single venue, partial coverage)
        │
        ▼
raw JSON frame ──► bounded capture-admission queue ──► asynchronous CRC-framed journal writer
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

No database, LLM, MCP request, notebook, or filesystem query is in the event-to-decision path.
Before decode, the source synchronously attempts nonblocking admission of the exact raw frame to a
count- and byte-bounded in-process queue. A returned diagnostic capture receipt means the frame
passed diagnostic capture identity, generation, and integrity admission checks and entered that
queue; it is never current live or source-registry authority and is not a writer or durability
acknowledgement.
Saturation, a stopped or closed writer, authority failure, or a generation change fails publication
closed. The dedicated writer appends and flushes asynchronously outside the event-to-decision path.
Writer, storage, or shutdown-deadline failure marks capture incomplete and prevents execution-quality
qualification; explicit flush checkpoints and shutdown avoid an fsync on every frame.

## Diagnostic data-integrity model

The engine distinguishes source capture from market truth. Frames successfully appended to the local
journal can be checksummed, replayed, and traced to their raw source bytes. Queue admission alone is
not a durability claim. Capture health records an incomplete generation when asynchronous
persistence cannot complete. No external venue or free provider is assumed globally complete or
infallible.

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

Optional MSJ journal-format evolution may add segmentation, cryptographic segment manifests, or
compression while retaining independent compatibility. That diagnostic-journal evolution is
separate from the already-runnable research Parquet compaction service and from the still-mandatory
provider-ingestion and point-in-time dataset-construction verticals.

## Paper execution modes

The legacy optional flag exercises the authority-free compatibility path without risking capital:

```bash
market-squawk capture --products BTC-USD --paper-bot
```

It is intentionally simple and not an investment recommendation. It generates fixed-size momentum
intents after a warm-up window. Every intent passes through a diagnostic calculation before a
paper-only simulated fill is recorded. It has no broker connection or production execution
authority.

The production-owned paper service is a separate command:

```bash
market-squawk paper-bot \
  --seconds 30 \
  --initial-cash 100000 \
  --fee-basis-points 100
```

It starts and shuts down the sealed Coinbase-to-live-to-risk-to-dispatch-to-paper graph. The graph
uses the realistic paper engine and canonical risk contracts, but it intentionally produces no
orders: Coinbase is still `DirectUnverified`, and the CLI installs a no-intent strategy as a second
fail-closed barrier. This command demonstrates production ownership and lifecycle behavior; it does
not demonstrate an execution-qualified strategy.

## Local verification

```bash
./scripts/verify.sh
```

This runs focused Python tests for repository-input hygiene, immutable CI action references, and the
MCP smoke harness; workspace-boundary and generated-artifact gates; Cargo Deny, Cargo Audit, and
Gitleaks tree/history scans; workspace-wide formatting, strict Clippy, tests, release build, and
rustdoc; then CLI, offline mock, and timeout-bounded local MCP smoke tests. All Cargo operations that
consume dependencies use the committed lockfile.

To exercise MCP after building:

```bash
python3 scripts/smoke_mcp.py ./target/debug/market-squawk
```

## Repository boundaries

```text
apps/
└── market-squawk/                 CLI, current live application, MCP, journal, and compatibility tests
crates/
├── market-squawk-analytics/       exact live feature kernels and versioned feature metadata
├── market-squawk-data/            SQLite catalog, Arrow, Parquet, DataFusion, and lineage
├── market-squawk-domain/          shared financial, identity, quality, provenance, and event contracts
├── market-squawk-execution/       typed intents and bounded pre-authority account/risk coordination
├── market-squawk-live/            production authority, sharding, books, and bounded snapshots
├── market-squawk-mcp/             bounded local stdio MCP protocol and lifecycle foundation
├── market-squawk-platform/        local paths, lifecycle, capture, persistence, and operations
├── market-squawk-services/        shared application-service contracts
└── market-squawk-sources/         source contracts, registry, budgets, health, and supervision
adapters/
├── market-squawk-adapter-coinbase/ bounded Coinbase Exchange v1 source and protocol fixtures
├── market-squawk-adapter-kraken/   bounded Kraken v2 transport, decoder, checksum, and session source
└── market-squawk-adapter-paper/    bounded realistic paper execution, accounting, audit, and recovery
scripts/                            deterministic local/CI policy and smoke gates
docs/                               architecture, plans, research, and verification evidence
```

The release baseline records current tracked adapter and Python-package state; required
implementations are listed under **Required but missing**.

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

## Release-blocking implementation map

The [usable complete-release implementation plan](docs/superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md)
is the executable delivery contract. Stages and Waves describe dependency/parallel ownership inside
exactly four production-weighted review quarters:

1. **Quarter 1 of 4 — Stages 0–1 / Waves 0–1B:** close the live/capture prerequisite; refresh truth,
   dependencies, rights, and ownership; then complete production Coinbase/risk/paper execution,
   SQLite, Arrow/Parquet/DataFusion, bounded MCP protocol, and Kraken.
2. **Quarter 2 of 4 — Stage 2 / Waves 2–3:** implement file, SEC, macro, and portfolio adapters;
   compose research ingestion, point-in-time datasets, corporate actions, and batch analytics.
3. **Quarter 3 of 4 — Stage 3 / Waves 4A–4B:** implement model bundles, native and ONNX inference,
   the Python product, portfolio accounting, backtesting, and fair-value analysis.
4. **Quarter 4 of 4 — Stages 4–5 / Waves 5–6:** complete shared services, CLI, and typed MCP domains;
   then run integrated demonstrations, provider evidence, fuzzing, measured performance, security,
   supply-chain gates, grouped review, publication, and cleanup.

Every item is mandatory unless the product contract is explicitly changed. Each quarter ends at one
clean exact commit with grouped independent review and remediation of every substantiated severity.
No per-task review rounds or fifth delivery quarter are part of the plan. Stage, Wave, and percentage
describe progress; none authorizes a partial-release stop.

## Primary references

- Coinbase Exchange WebSocket channels: `https://docs.cdp.coinbase.com/exchange/websocket-feed/channels`
- Kraken WebSocket v2 book checksum guide: `https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2`
- MCP stdio transport specification: `https://modelcontextprotocol.io/specification/2025-11-25/basic/transports`
- Rust installation: `https://rustup.rs/`

## Financial-use warning

This project is research infrastructure. Free market data may be incomplete, delayed, venue-specific, revised, or unavailable. Validate data rights, source quality, execution assumptions, fees, slippage, liquidity, and risk controls before relying on any result. No software can guarantee investment outcomes or universal market-data accuracy.
