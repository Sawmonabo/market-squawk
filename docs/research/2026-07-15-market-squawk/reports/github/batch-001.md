# GitHub Batch 001 Deep Dive

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
  - [Adoption Posture](#adoption-posture)
  - [Live Trading Contracts](#live-trading-contracts)
  - [Research Data Contracts](#research-data-contracts)
  - [Risk, Audit, and Operational Controls](#risk-audit-and-operational-controls)
  - [Performance and Release Hardening](#performance-and-release-hardening)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews only the four assigned GitHub repositories as of **2026-07-15**:
`nautechsystems/nautilus_trader`, `barter-rs/barter-rs`, `apache/arrow-rs`, and
`apache/datafusion`. Repository statistics were captured with the GitHub API, and implementation
claims were checked against repository pages and commit-pinned source files rather than search
snippets. GitHub timestamps are UTC; therefore activity late on July 15 in New York can appear as
July 16 UTC.

The decision context is Market Squawk's hardened self-hosted design: independent live and research
planes; strict execution-data qualification; deterministic single-writer live state; bounded
queues; non-bypassable risk; Arrow/Parquet/DataFusion research storage; and no analytical, Python,
MCP, database, or arbitrary I/O in the event-to-action path.

Throughout this report:

- **Confirmed** means the linked repository, file, release, or GitHub metadata directly supports
  the statement.
- **Inference** means an adoption recommendation or Market Squawk fit assessment derived from the
  confirmed evidence.
- A repository's own performance or production-readiness description is not treated as independent
  proof that it satisfies Market Squawk's acceptance criteria.

## Sources Reviewed

| ID | Repository | Owner | Observed stars / forks | License | Primary language | Freshness and maintenance signal | Market Squawk relevance |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `github-001` | [`nautechsystems/nautilus_trader`](https://github.com/nautechsystems/nautilus_trader) | Nautech Systems | 24,718 / 3,171 | LGPL-3.0 | Rust | `v1.230.0` released 2026-06-29; reviewed `develop` commit [`c7d60c1`](https://github.com/nautechsystems/nautilus_trader/commit/c7d60c1d6e64d72076f8cd2a652d199263679223) dated 2026-07-15; extensive crates, adapters, tests, benches, fuzz targets, and security policy | Broad comparative architecture for live adapters, domain types, books, risk, portfolio, execution, simulation, and hardening |
| `github-002` | [`barter-rs/barter-rs`](https://github.com/barter-rs/barter-rs) | barter-rs | 2,202 / 355 | MIT | Rust | `barter-v0.12.5` released 2026-05-09; reviewed `main` commit [`33e5618`](https://github.com/barter-rs/barter-rs/commit/33e56188e2095781331f85aa3d7f88e251eec65a) dated 2026-05-09; modular workspace, examples, CI, tests, and a backtest benchmark | Smaller Rust reference for source/execution traits, reconnecting streams, engine state, strategy/risk hooks, audit replicas, and mock execution |
| `github-003` | [`apache/arrow-rs`](https://github.com/apache/arrow-rs) | Apache Software Foundation | 3,527 / 1,213 | Apache-2.0 | Rust | `59.1.0` released 2026-07-07; reviewed `main` commit [`ee30b61`](https://github.com/apache/arrow-rs/commit/ee30b61b00df8a590c4c45c490fbecc0962cfba5) dated 2026-07-15; official Arrow and Parquet Rust implementation | Direct dependency family for Arrow record batches, schemas, decimal analytical values, Parquet, and columnar interchange |
| `github-004` | [`apache/datafusion`](https://github.com/apache/datafusion) | Apache Software Foundation | 8,982 / 2,229 | Apache-2.0 | Rust | Reviewed `main` commit [`18121a6`](https://github.com/apache/datafusion/commit/18121a68433ac19763787e9763ef3f50508befd5) dated 2026-07-16 UTC; 14,000+ commits, examples, benchmarks, active roadmap, committed lockfile | Direct embedded analytical SQL/DataFrame engine for local research datasets |

**Confirmed.** Counts, licenses, default branches, languages, and push dates above are point-in-time
observations from each repository's GitHub metadata endpoint: [NautilusTrader](https://api.github.com/repos/nautechsystems/nautilus_trader),
[Barter](https://api.github.com/repos/barter-rs/barter-rs),
[Arrow Rust](https://api.github.com/repos/apache/arrow-rs), and
[DataFusion](https://api.github.com/repos/apache/datafusion). Release dates come from the linked
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader/releases/tag/v1.230.0),
[Barter](https://github.com/barter-rs/barter-rs/releases/tag/barter-v0.12.5), and
[Arrow Rust](https://github.com/apache/arrow-rs/releases/tag/59.1.0) release pages.

## Findings

### Adoption Posture

**Confirmed.** Arrow Rust is the official Rust implementation of Apache Arrow and includes the
Apache Parquet Rust implementation; DataFusion is an Apache Rust query engine that uses Arrow as
its in-memory format and provides SQL and DataFrame APIs with built-in CSV, Parquet, JSON, and Avro
support plus extension points for sources, functions, operators, and query languages
([Arrow Rust README](https://github.com/apache/arrow-rs/blob/ee30b61b00df8a590c4c45c490fbecc0962cfba5/README.md),
[DataFusion README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md)).

**Inference.** `arrow`, `parquet`, and DataFusion should be treated as pinned direct dependencies in
Market Squawk's research plane, not as designs to reimplement. The workspace should pin a mutually
compatible version family in `Cargo.lock`, minimize enabled features, wrap them behind
Market Squawk-owned dataset/catalog services, and test decimal, timestamp, schema-evolution,
partition-pruning, cancellation, and memory-limit behavior.

**Confirmed.** NautilusTrader describes a Rust-native engine spanning research, deterministic
simulation, and live execution, while Barter describes a Rust ecosystem for live, paper, and
backtest systems with modular data, instrument, execution, integration, and core-engine crates
([NautilusTrader README](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/README.md),
[Barter README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md)).

**Inference.** NautilusTrader and Barter are best used as comparative contract and test references,
not adopted as Market Squawk's platform core. Market Squawk deliberately does not require live and
historical data parity, and its `DirectVerified`, quality-transition, queue-overflow, risk, and
fair-value rules are stricter and differently factored than either reviewed engine demonstrates.
NautilusTrader's LGPL-3.0 license also requires legal review before copying or linking Rust code;
Barter's MIT license is permissive, but its explicit production disclaimer and thinner hardening
evidence make due diligence more—not less—important.

### Live Trading Contracts

**Confirmed.** NautilusTrader's workspace separates `live`, `data`, `execution`, `risk`,
`portfolio`, `model`, `persistence`, `backtest`, and adapter crates. Its core repository classifies
Coinbase and Kraken as official data/execution adapters maintained by the project
([workspace crates](https://github.com/nautechsystems/nautilus_trader/tree/c7d60c1d6e64d72076f8cd2a652d199263679223/crates),
[adapter policy and inventory](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/ADAPTERS.md)).

**Confirmed.** The reviewed Kraken Spot L3 implementation contains a CRC32 checksum module, a
document-derived checksum fixture, and a resynchronization helper with a five-attempt bounded
exponential backoff. On final resync failure it logs that the book remains cleared until reconnect
or manual resubscribe
([checksum implementation and tests](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs),
[resync helper](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/resync.rs)).

**Confirmed.** That L3 checksum cache retains raw price/size strings for checksum formatting but
also stores `f64` price/size values and sorts/group-levels using floating-point prices. Separately,
the reviewed Kraken L2 state file applies parsed deltas to a shadow book, generates local sequence
values, prunes to configured depth, and logs apply failures; the reviewed file does not itself
perform a checksum comparison
([L3 checksum source](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs),
[L2 book state](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_2.rs)).

**Inference.** NautilusTrader's checksum and resync decomposition is a valuable test/reference
shape, but it cannot be copied as Market Squawk's financial representation contract. Market Squawk
should parse raw decimals into instrument-scaled integers, build checksum inputs from the exact
venue-prescribed representation, compare only with integer/raw-string ordering semantics, and
couple every failure to a typed quality transition to `Quarantined`. The L2 and L3 paths must each
be audited against Kraken's current official rules; upstream “official adapter” status is a
maintenance classification, not proof of `DirectVerified` eligibility.

**Confirmed.** Barter-Data documents normalized real-time WebSocket streams and a reconnecting
stream abstraction. Its current support table lists Coinbase spot public trades only, Kraken spot
public trades plus L1 books, and Binance L1/L2 books. It does not list Coinbase order books or
Kraken L2/L3 books in that table
([Barter-Data README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-data/README.md)).

**Confirmed.** Barter-Integration separates a low-level `RestClient`, request signing and response
parsing from an asynchronous `ExchangeStream`, protocol parser, and stateful transformer, and says
the stream can be built over WebSocket, FIX, or other asynchronous transports
([Barter-Integration README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-integration/README.md)).

**Inference.** Barter's trait decomposition is reusable design evidence for source-specific
decoders and shared transport plumbing, but its current venue coverage cannot fulfill the required
Coinbase book or Kraken checksum adapter. Market Squawk should keep `LiveMarketSource` distinct
from `ExtractionSource` and must place sequence, checksum, freshness, coverage, and quarantine
policy in explicit validation contracts rather than infer quality from successful normalization.

### Research Data Contracts

**Confirmed.** DataFusion is a columnar, streaming, multithreaded, vectorized and partitioned query
engine. Its README describes built-in Parquet, CSV, JSON, and Avro sources, customizable data
sources/functions/operators, SQL and DataFrame APIs, and an API deprecation policy. It also states
that the project commits `Cargo.lock` and updates dependencies regularly
([DataFusion README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md)).

**Confirmed.** Arrow Rust supplies the columnar in-memory and Parquet implementation layer on which
DataFusion depends, and the reviewed repository has a recent numbered release and active main
branch
([Arrow Rust repository](https://github.com/apache/arrow-rs/tree/ee30b61b00df8a590c4c45c490fbecc0962cfba5),
[release 59.1.0](https://github.com/apache/arrow-rs/releases/tag/59.1.0)).

**Inference.** This pair fits Market Squawk's research plane directly: adapters should normalize
into versioned Arrow schemas; durable writes should produce manifest-tracked Parquet files; and
DataFusion should operate through bounded application services that apply instrument/time limits,
point-in-time predicates, revision rules, cancellation, and memory governance. Neither library
should define Market Squawk's provenance or bitemporal semantics—those belong to domain schemas and
dataset services above them.

**Inference.** Arrow and DataFusion must remain absent from the live event-to-action dependency
graph, even if individual kernels are fast. Their allocation, planning, file access, and query
execution models belong to asynchronous persistence/research services. Pure mathematical kernels
may be shared only when their inputs and ownership do not bring analytical runtime state into a
shard.

### Risk, Audit, and Operational Controls

**Confirmed.** Barter's core documentation exposes plug-in `Strategy` and `RiskManager` components,
a trading-enabled/disabled state, commands such as close/cancel, centralized indexed engine state,
and an audit stream intended for non-hot-path state replicas or persistence. Its paper example
starts with trading disabled and later enables it explicitly
([Barter core README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter/README.md),
[audit-replica example](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter/examples/engine_sync_with_audit_replica_engine_state.rs)).

**Confirmed.** Barter-Execution exposes a normalized `ExecutionClient` and a `MockExchange` /
`MockExecutionClient` for paper and backtest use, but its README does not specify calibrated
latency, fee schedules, slippage, queue position, partial fills, balances, or rejection models
([Barter-Execution README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-execution/README.md)).

**Inference.** Barter's audit replica is a sound reference for removing monitoring and persistence
from the hot path, and its disabled-by-default switch is a useful control. Its pluggable
`RiskManager`, however, is not evidence of the Market Squawk requirement that no strategy, model,
adapter, CLI, or MCP route can bypass risk. Market Squawk needs one application-owned approval
boundary that accepts only typed intents, binds the approved order to the evaluated intent and
market snapshot, enforces expiry/duplicate/rate/exposure/loss rules, and is the only constructor
path into an execution adapter.

**Confirmed.** NautilusTrader publishes a detailed security policy covering protected/signed
changes, immutable release tags, pinned lockfiles, cargo-vet/audit/deny and OSV scanning, Gitleaks,
Zizmor, CodeQL, selected adapter/signing fuzz targets, SLSA provenance, checksum manifests, SBOMs,
signed images, OIDC publishing, post-publish verification, and supported-version policy. It states
that only the latest release is supported
([NautilusTrader security policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md)).

**Inference.** NautilusTrader's release-security documentation is the strongest hardening reference
in this batch. Market Squawk can adopt the pattern—locked and audited dependencies, pinned CI
actions, credential scanning, fuzzing, SBOM/license checks, checksummed artifacts, and documented
vulnerability response—without adopting optional containers, cloud publishing, or telemetry.

### Performance and Release Hardening

**Confirmed.** NautilusTrader distinguishes Criterion wall-clock measurements from `iai`
instruction-count regression signals; has crate-level hot-path benches, scenario/stress tests, and
a nightly performance workflow; requires reported numbers to include hardware, toolchain, and build
profile; and explicitly says numbers from a developer laptop should not be quoted as authoritative.
Its policy also states that Rust benchmark deltas do not currently fail pull requests
([benchmark policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/BENCHMARKING.md)).

**Inference.** This is a useful measurement discipline, not transferable performance evidence.
Market Squawk must create its own end-to-end event fixture and record decoder throughput,
sequence/checksum time, queue delay, book/features/strategy/risk stages, event-to-decision p50/p95/
p99/max, peak memory, hardware, OS, toolchain, and sustained-burst behavior. Because upstream CI
does not gate Rust benchmark deltas on pull requests, Market Squawk should define explicit
regression thresholds for the small, stable acceptance fixture after baselines are measured.

**Confirmed.** Barter's reviewed CI runs `cargo check`, `cargo test`, `cargo fmt --all -- --check`,
and Clippy with `-D warnings` on stable Rust. The workflow uses mutable major-version action tags,
does not show `--locked`, workspace/all-target/all-feature coverage, security/license audit jobs, or
benchmark gates, and the reviewed repository tree exposed a small test/benchmark footprint relative
to the breadth of the trading claims
([Barter CI](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/.github/workflows/ci.yml),
[repository tree](https://github.com/barter-rs/barter-rs/tree/33e56188e2095781331f85aa3d7f88e251eec65a)).

**Confirmed.** Barter's top-level README states that the software is solely for educational and
research purposes and is not intended, designed, tested, verified, or certified for commercial or
production live trading
([Barter disclaimer](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md#legal-disclaimer-and-limitation-of-liability)).

**Inference.** Barter remains valuable for API comparison, but none of its robustness or
performance adjectives should be cited as production assurance. Any code reuse must be surrounded
by Market Squawk's stricter lint profile, property/fuzz/network-separation tests, dependency and
license audits, pinned actions/toolchain/lockfile, bounded queues, fault injection, deterministic
fixtures, and measured release criteria.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** NautilusTrader is a broad Rust-native research/simulation/live engine with official Coinbase and Kraken data/execution adapters. | NautilusTrader | [README](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/README.md) and [adapter inventory](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/ADAPTERS.md) | High | “Official” describes upstream ownership/support, not Market Squawk data quality. |
| **Confirmed:** NautilusTrader separates major domains into live, data, execution, risk, portfolio, persistence, backtest, model, and adapter crates. | NautilusTrader | [Crates tree](https://github.com/nautechsystems/nautilus_trader/tree/c7d60c1d6e64d72076f8cd2a652d199263679223/crates) | High | Strong project-boundary reference. |
| **Confirmed:** Its Kraken Spot L3 path implements CRC32 checksum construction/tests and bounded resync retry. | NautilusTrader | [Checksum](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs), [resync](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/resync.rs) | High | Direct implementation and tests reviewed. |
| **Confirmed:** The reviewed L3 checksum cache uses raw decimal strings and `f64` price/size; the L2 state file applies/prunes a shadow book without a local checksum check in that file. | NautilusTrader | [L3 checksum](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs), [L2 state](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_2.rs) | High for reviewed files | Not a repository-wide proof that no other L2 validation exists. |
| **Inference:** Reuse the checksum/resync decomposition, but implement Market Squawk's scaled-integer and typed quarantine contract independently. | NautilusTrader + specification | Same source files as previous row | High | Fit recommendation, not upstream fact. |
| **Confirmed:** NautilusTrader has extensive documented benchmark and supply-chain security practices. | NautilusTrader | [Benchmarking](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/BENCHMARKING.md), [Security](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md) | High | Policies are strong evidence of process, not proof of zero defects. |
| **Confirmed:** Barter separates core, instruments, data, execution, and integration crates and exposes Strategy/RiskManager, indexed state, commands, and audit replicas. | Barter | [Workspace README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md), [core README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter/README.md) | High | Good trait/boundary reference. |
| **Confirmed:** Barter's support table lists Coinbase trades only and Kraken trades/L1 books, not the required Coinbase book and Kraken checksum coverage. | Barter | [Barter-Data support table](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-data/README.md#supported-exchange-subscriptions) | High | Directly limits reuse for required adapters. |
| **Confirmed:** Barter provides mock execution but its README does not establish the required realistic fill dimensions. | Barter | [Barter-Execution README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-execution/README.md) | Medium-high | Absence claim scoped to reviewed documentation. |
| **Confirmed:** Barter CI checks, tests, formats, and lints, but the reviewed workflow lacks locked/all-feature security/audit/benchmark gates and uses mutable action tags. | Barter | [CI workflow](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/.github/workflows/ci.yml) | High | Workflow-level observation only. |
| **Confirmed:** Barter explicitly disclaims production and commercial live-trading fitness. | Barter | [Legal disclaimer](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md#legal-disclaimer-and-limitation-of-liability) | High | Important counterweight to README performance language. |
| **Confirmed:** Arrow Rust is the official Apache Rust implementation of Arrow and Parquet. | Arrow Rust | [README](https://github.com/apache/arrow-rs/blob/ee30b61b00df8a590c4c45c490fbecc0962cfba5/README.md), [59.1.0 release](https://github.com/apache/arrow-rs/releases/tag/59.1.0) | High | Direct dependency candidate. |
| **Confirmed:** DataFusion uses Arrow and provides extensible SQL/DataFrame analytics with built-in local analytical formats. | DataFusion | [README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md) | High | Direct research-plane dependency candidate. |
| **Confirmed:** DataFusion documents API evolution and commits a lockfile. | DataFusion | [README API/dependency sections](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md#datafusion-api-evolution-and-deprecation-guidelines) | High | Market Squawk must still pin a compatible crate family. |
| **Inference:** Arrow/DataFusion should be wrapped by bounded, provenance-aware dataset services and excluded from the live dependency graph. | Arrow Rust + DataFusion + specification | [Arrow Rust](https://github.com/apache/arrow-rs), [DataFusion](https://github.com/apache/datafusion) | High | Architecture recommendation derived from capability and hot-path constraints. |

## Source-Specific Notes

### `nautechsystems/nautilus_trader`

**Confirmed.** This is the most complete trading-system implementation in the batch and the one
with the strongest visible benchmark, fuzzing, vulnerability-management, dependency-provenance,
artifact-verification, and release-supply-chain posture
([repository](https://github.com/nautechsystems/nautilus_trader),
[security policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md)).

**Inference.** Highest-value areas for deep implementation study are: financial fixed-precision
types; book invariants; adapter test fixtures; Kraken checksum/resync; matching/risk/execution
boundaries; and benchmark/release-security policy. Do not inherit the shared research/live parity
assumption, Python control plane, optional infrastructure, raw/floating checksum representation, or
LGPL code without an intentional architectural and legal decision.

### `barter-rs/barter-rs`

**Confirmed.** Barter is smaller and permissively licensed, with clear abstractions for normalized
market streams, reconnecting sources, WebSocket/REST transformation, execution clients, mock
exchange, engine state, strategy/risk hooks, and out-of-band audit replication
([repository README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md),
[integration README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-integration/README.md)).

**Inference.** Use it to compare ergonomic trait and indexed-state designs, not as evidence that a
production live path is already solved. The incomplete required venue depth, explicit production
disclaimer, modest test/benchmark surface, and weaker CI/supply-chain controls materially limit
direct adoption.

### `apache/arrow-rs`

**Confirmed.** Arrow Rust is an official, actively released Apache implementation under Apache-2.0,
making it the lowest-risk direct dependency in this batch from an ecosystem and license standpoint
([repository](https://github.com/apache/arrow-rs),
[license](https://github.com/apache/arrow-rs/blob/ee30b61b00df8a590c4c45c490fbecc0962cfba5/LICENSE.txt)).

**Inference.** Define Market Squawk's schemas and decimal/time/provenance rules first, then use
Arrow builders/arrays and Parquet writers as mechanics. Do not let inferred CSV/JSON schemas become
canonical without explicit validation, versioning, precision, null, and time-semantic policy.

### `apache/datafusion`

**Confirmed.** DataFusion is an official, Apache-licensed, actively developed embedded query engine
with the extensibility and local format support needed by the research plane
([repository README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md)).

**Inference.** Expose narrow application queries and CLI read-only SQL rather than unrestricted MCP
SQL. Register only controlled dataset locations; apply query cancellation and resource bounds; add
instrument/time/result caps at service boundaries; and implement point-in-time correctness in
schemas, view builders, and tests rather than assuming generic SQL prevents look-ahead bias.

## Cross-Source Patterns

1. **Confirmed:** all four repositories are Rust-first and active, but their maturity signals are
   not equivalent. Apache's official data projects and NautilusTrader show substantially broader
   maintenance/release evidence than Barter's reviewed tree and CI
   ([Nautilus security](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md),
   [Barter CI](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/.github/workflows/ci.yml),
   [Arrow Rust](https://github.com/apache/arrow-rs),
   [DataFusion](https://github.com/apache/datafusion)).
2. **Confirmed:** the trading repositories both use modular typed components and separate
   networking/data/execution concerns; the analytical repositories compose around Arrow as the
   shared columnar representation
   ([Nautilus crates](https://github.com/nautechsystems/nautilus_trader/tree/c7d60c1d6e64d72076f8cd2a652d199263679223/crates),
   [Barter workspace](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/Cargo.toml),
   [DataFusion README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md)).
3. **Inference:** Market Squawk should share only stable domain types and pure math across planes.
   The live plane should borrow ownership, adapter, risk, and benchmark ideas from the trading
   engines; the research plane should directly embed Arrow/DataFusion. A universal upstream engine
   would weaken the specified isolation.
4. **Inference:** normalized data is not qualified data. Every adapter must preserve source-native
   fields long enough to validate ordering, checksum, precision, coverage, time, and status before
   constructing an execution-eligible canonical event.
5. **Inference:** mock execution is not realistic paper execution until latency, fees, slippage,
   available liquidity, partial fills, cancellation races, rejections, balances, positions, and
   order transitions are specified, tested, and calibrated.
6. **Inference:** upstream benchmark results must never be used to claim Market Squawk acceptance.
   The main reusable artifact is benchmark methodology and fixture design.

## Limitations and Non-Findings

- **Confirmed non-finding:** no reviewed source defines Market Squawk's `FairValueHierarchy`,
  `MarketDepth`, and `DataQuality` separation or an ASC 820/IFRS 13 evidence workflow. This batch
  provides no fair-value classification implementation evidence.
- **Confirmed non-finding:** no reviewed trading source demonstrates the complete Market Squawk
  `DirectVerified` predicate as one auditable contract. NautilusTrader supplies valuable Kraken L3
  checksum/resync components, while Barter's documented Kraken coverage stops at L1; neither
  reviewed evidence establishes the full coverage/timestamp/status/precision/freshness/quarantine
  gate for immediate automated action.
- **Confirmed non-finding:** Barter's public documentation reviewed here does not establish
  realistic paper-fill calibration or the complete risk rule set. Absence statements are limited to
  the reviewed commit and documentation, not a proof about every unpublished or downstream use.
- **Confirmed non-finding:** the reviewed sources do not supply SEC, FRED/ALFRED, BLS, Treasury,
  portfolio-file, fair-value, ONNX, or MCP implementations; those belong to other research batches.
- **Confirmed non-finding:** DataFusion and Arrow provide storage/query mechanics, not bitemporal
  investment semantics, revision policy, corporate-action policy, deduplication identity, or
  leakage checks. No generic query engine can infer those rules from raw files.
- **Limitation:** precise GitHub stars/forks and default-branch heads can change after the access
  date. Counts are preserved only as the 2026-07-15 observations in this report.
- **Limitation:** this review did not execute upstream tests or benchmarks, audit every transitive
  dependency, or independently validate upstream performance/security claims. Repository controls
  are process evidence, not a security certification.
- **Limitation:** NautilusTrader's LGPL implications are flagged, not resolved. Rust linkage and
  derivative-work questions require project-specific legal review.
- **Limitation:** DataFusion does not publish current releases through the GitHub Releases endpoint
  used for this batch; freshness was assessed from current repository activity and roadmap rather
  than an asserted latest release number.

## Source List

All sources were accessed on **2026-07-15**.

### NautilusTrader

1. [Repository and README](https://github.com/nautechsystems/nautilus_trader)
2. [GitHub metadata API](https://api.github.com/repos/nautechsystems/nautilus_trader)
3. [Reviewed commit `c7d60c1`](https://github.com/nautechsystems/nautilus_trader/commit/c7d60c1d6e64d72076f8cd2a652d199263679223)
4. [Release `v1.230.0`](https://github.com/nautechsystems/nautilus_trader/releases/tag/v1.230.0)
5. [Adapters and integrations policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/ADAPTERS.md)
6. [Kraken L3 checksum implementation](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/checksum.rs)
7. [Kraken L3 resynchronization helper](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_3/resync.rs)
8. [Kraken L2 state implementation](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/crates/adapters/kraken/src/websocket/spot_v2/level_2.rs)
9. [Benchmarking policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/BENCHMARKING.md)
10. [Security policy](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/SECURITY.md)
11. [LGPL-3.0 license](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/LICENSE)

### Barter

1. [Repository and top-level README](https://github.com/barter-rs/barter-rs)
2. [GitHub metadata API](https://api.github.com/repos/barter-rs/barter-rs)
3. [Reviewed commit `33e5618`](https://github.com/barter-rs/barter-rs/commit/33e56188e2095781331f85aa3d7f88e251eec65a)
4. [Release `barter-v0.12.5`](https://github.com/barter-rs/barter-rs/releases/tag/barter-v0.12.5)
5. [Barter core README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter/README.md)
6. [Barter-Data README and support table](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-data/README.md)
7. [Barter-Execution README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-execution/README.md)
8. [Barter-Integration README](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/barter-integration/README.md)
9. [Workspace manifest](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/Cargo.toml)
10. [CI workflow](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/.github/workflows/ci.yml)
11. [MIT license](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/LICENSE)

### Apache Arrow Rust

1. [Repository and README](https://github.com/apache/arrow-rs)
2. [GitHub metadata API](https://api.github.com/repos/apache/arrow-rs)
3. [Reviewed commit `ee30b61`](https://github.com/apache/arrow-rs/commit/ee30b61b00df8a590c4c45c490fbecc0962cfba5)
4. [Release `59.1.0`](https://github.com/apache/arrow-rs/releases/tag/59.1.0)
5. [Apache-2.0 license](https://github.com/apache/arrow-rs/blob/ee30b61b00df8a590c4c45c490fbecc0962cfba5/LICENSE.txt)

### Apache DataFusion

1. [Repository and README](https://github.com/apache/datafusion)
2. [GitHub metadata API](https://api.github.com/repos/apache/datafusion)
3. [Reviewed commit `18121a6`](https://github.com/apache/datafusion/commit/18121a68433ac19763787e9763ef3f50508befd5)
4. [Examples](https://github.com/apache/datafusion/tree/18121a68433ac19763787e9763ef3f50508befd5/datafusion-examples)
5. [Benchmarks](https://github.com/apache/datafusion/tree/18121a68433ac19763787e9763ef3f50508befd5/benchmarks)
6. [Apache-2.0 license](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/LICENSE.txt)
