# Local Market Engine Design

## Goal

Build a zero-mandatory-license-cost, local-first Rust platform that consumes live and historical financial data, journals source-faithful raw events, maintains validated market state, supports deterministic modeling and replay, and exposes bounded analysis through MCP.

## First-release boundary

Version 0.1 is a single local process. It captures a public Coinbase Level 2 feed, validates and journals raw frames, maintains order books and online features, runs an optional paper-only strategy behind a fail-closed risk kernel, and exposes local stdio MCP tools. It has no deployment stack, OTEL collector, live brokerage execution, arbitrary MCP execution, or access-control bypass functionality.

## Architecture

The live path is Rust end to end. A source adapter receives raw messages, sends exact frames to a bounded journal writer, decodes canonical market events, and forwards them to a single-writer engine. The engine updates books, features, quality, strategy state, and paper risk. MCP observes or controls the engine outside the per-event hot path.

## Correctness rules

- Prices, quantities, and monetary limits use fixed-point decimal values.
- A failed journal enqueue stops capture instead of discarding data.
- An operating-system lock prevents concurrent writers from corrupting one journal.
- Crossed books are quarantined.
- Stale or invalid feeds cannot approve paper intents.
- The kill switch is irreversible for the current process.
- MCP cannot execute arbitrary shell, SQL, network, or filesystem operations.
- Raw events remain replayable independently of future decoder versions.
- External data is source-attributed and never labeled universally correct.

## Extensibility

Source adapters implement one async contract. Canonical events isolate downstream state from source schemas. Journal framing is versioned. Analytical storage, DataFusion, Parquet, additional sources, model bundles, and authorized execution can be added without moving MCP or Python into the live decision path.
