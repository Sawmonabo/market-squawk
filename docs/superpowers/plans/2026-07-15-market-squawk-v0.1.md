# Market Squawk v0.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a runnable local Rust market-data engine with public live capture, immutable journaling, deterministic order-book features, paper-only risk enforcement, replay, and bounded stdio MCP tools.

**Architecture:** A source adapter journals raw frames before decoded events enter a single-writer engine. The engine owns books, quality, online features, paper strategy state, and deterministic risk. MCP reads shared state but never participates in the live event-to-decision path.

**Tech Stack:** Rust 1.85, Tokio, tokio-tungstenite, Serde, rust_decimal, Clap, tracing, CRC-framed local files, JSON-RPC/MCP over stdio.

## Global Constraints

- No mandatory paid API, service, cloud, or software license.
- Local execution is the first-release target.
- No OpenTelemetry deployment in v0.1.
- No live order submission in v0.1.
- No arbitrary MCP SQL, shell, filesystem, remote-code, or unchecked order tools.
- All financial prices and quantities use fixed-point decimal values.
- Market-data loss or uncertainty fails closed.

---

### Task 1: Domain and order-book core

**Files:** `src/domain.rs`, `src/order_book.rs`, `src/features.rs`, `tests/order_book.rs`

**Produces:** Canonical `MarketEvent`, `OrderBook`, `TopOfBook`, and `OnlineFeatures` interfaces.

- [x] Write order-book and feature tests first.
- [x] Define fixed-point canonical events.
- [x] Implement snapshot and delta application.
- [x] Implement midprice, spread, microprice, and imbalance.
- [x] Run `cargo test --test order_book` and observe green output.

### Task 2: Immutable raw journal

**Files:** `src/journal.rs`, `src/replay.rs`, `tests/journal.rs`

**Produces:** `JournalWriter`, bounded `JournalSink`, `JournalReader`, and `summarize_journal`.

- [x] Write round-trip and corruption tests first.
- [x] Implement versioned length/CRC framing.
- [x] Implement bounded asynchronous writer commands.
- [x] Implement checksum-validating reader and replay summary.
- [x] Run `cargo test --test journal` and observe green output.

### Task 3: Source adapters

**Files:** `src/source/mod.rs`, `src/source/coinbase.rs`, `src/source/mock.rs`, `tests/coinbase_decode.rs`

**Produces:** `MarketSource`, public Coinbase adapter, and deterministic mock source.

- [x] Write decoder tests first.
- [x] Implement Coinbase Level 2, heartbeat, and match decoding.
- [x] Journal exact incoming text frames.
- [x] Implement offline mock capture.
- [x] Run `cargo test --test coinbase_decode` and observe green output.

### Task 4: Engine, paper bot, and risk

**Files:** `src/engine.rs`, `src/bot.rs`, `src/risk.rs`, `src/quality.rs`, `tests/risk.rs`

**Produces:** `Engine`, quality state, optional `MomentumBot`, `PaperAccount`, and `RiskKernel`.

- [x] Write fail-closed risk tests first.
- [x] Implement quality and staleness state.
- [x] Implement paper intents and fills.
- [x] Enforce quality, age, notional, position, and kill-switch checks.
- [x] Run `cargo test --test risk` and observe green output.

### Task 5: MCP and CLI

**Files:** `src/mcp.rs`, `src/main.rs`, `scripts/smoke_mcp.py`

**Produces:** `market-squawk` commands and local stdio MCP protocol support.

- [x] Write MCP initialization test first.
- [x] Implement bounded tools and typed schemas.
- [x] Implement init, mock, capture, replay, and MCP commands.
- [x] Keep logs on stderr and MCP JSON-RPC on stdout.
- [x] Run the MCP smoke test and observe successful initialization and tool discovery.

### Task 6: Documentation and verification

**Files:** `README.md`, `scripts/verify.sh`, `.github/workflows/ci.yml`

**Produces:** Reproducible local operating and validation instructions.

- [x] Document capabilities, constraints, journal format, MCP setup, and roadmap.
- [x] Add local verification script.
- [x] Add CI for formatting, Clippy, tests, and mock smoke execution.
- [x] Run `./scripts/verify.sh` and observe all checks pass.

## Verification completion

A Rust 1.85 toolchain was assembled from the environment's trusted package mirror. Formatting, compilation, Clippy with warnings denied, all tests, the offline mock smoke test, the local MCP smoke test, and the optimized release build completed successfully. The external Coinbase endpoint was not contacted from the sandbox; the complete WebSocket adapter path was instead exercised against a local synthetic exchange server.
