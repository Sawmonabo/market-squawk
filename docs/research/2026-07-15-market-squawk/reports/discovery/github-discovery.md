# GitHub Discovery Report

## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Selection Summary](#selection-summary)
- [Candidate Sources](#candidate-sources)
- [Candidate Notes](#candidate-notes)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps](#coverage-gaps)
- [Source List](#source-list)

## Research Scope

This discovery identifies a deliberately small set of open-source repositories that can inform or
accelerate the evolution of Market Squawk from its current single-crate Rust application into the
specified local-first live-execution and research platform. It is anchored to **2026-07-15** in the
`America/New_York` time zone. Repository statistics were captured through the GitHub API on that
date; GitHub reports activity timestamps in UTC, so a few pushes appear as `2026-07-16` UTC.

Selection favored official ecosystem projects and mature, maintained implementations with clear
licenses, documentation, tests, examples, releases, or visible development activity. Stars were
used as one adoption signal, not as a quality ranking. The ten selected repositories cover:

- Rust-native trading engines, market adapters, risk, order management, and paper/backtest design.
- The required Arrow, Parquet, and DataFusion analytical stack.
- Point-in-time research, dataset construction, model training, and backtesting patterns.
- SEC, FRED, BLS, and US Treasury adapter structure.
- Local ONNX-compatible Rust inference and typed local MCP.
- Mature pricing, scenario, and market-risk abstractions useful to valuation work.

This is source discovery, not a recommendation to import any repository wholesale. Market Squawk's
strict separation of live and research planes, `DirectVerified` qualification, bounded queues,
risk non-bypass, point-in-time semantics, and fair-value evidence model remain product-specific
requirements.

## Search Queries Used

Web search queries:

1. `GitHub Rust low latency trading engine order book live backtesting 2026`
2. `GitHub Apache Arrow Rust Parquet DataFusion embedded SQL 2026`
3. `GitHub point in time quantitative research backtesting model platform 2026`
4. `GitHub SEC FRED BLS Treasury open source financial data adapters 2026`
5. `site:github.com/krakenfx official Kraken SDK WebSocket GitHub`
6. `site:github.com/coinbase official Advanced Trade SDK WebSocket GitHub`
7. `site:github.com barter-rs barter-rs GitHub market data execution Rust`
8. `site:github.com OpenGamma Strata GitHub`

GitHub API, organization, and code searches:

1. `org:krakenfx sdk websocket api cli`
2. `org:coinbase advanced websocket exchange`
3. `repo:krakenfx/kraken-cli checksum`
4. Direct metadata, releases, default-branch activity, license, and repository-tree inspection for
   every selected repository.
5. Provider-tree inspection under `OpenBB-finance/OpenBB/openbb_platform/providers` for `bls`,
   `fred`, `government_us`, and `sec`.

Search-result snippets were used only for discovery. Selection findings below are based on opened
repository pages, repository files, and GitHub API metadata.

## Selection Summary

The best reference architecture is not one monolithic upstream. A more credible reuse strategy is:

- Treat [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) and
  [Barter](https://github.com/barter-rs/barter-rs) as comparative designs for domain modeling,
  adapters, event-driven state, OMS/risk, and realistic simulation.
- Adopt the official [Arrow Rust](https://github.com/apache/arrow-rs) and
  [DataFusion](https://github.com/apache/datafusion) crates in the research plane, with aligned
  pinned versions and no use in the live hot path.
- Use [Qlib](https://github.com/microsoft/qlib) as a research reference for point-in-time data,
  leakage-aware datasets, models, portfolio construction, and backtesting, while keeping Python
  out of live inference.
- Use [OpenBB](https://github.com/OpenBB-finance/OpenBB) only as an adapter-architecture and test
  reference unless its AGPL obligations are intentionally accepted.
- Use the official [MCP Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) and
  [ort](https://github.com/pykeio/ort) behind narrow Market Squawk-owned interfaces.
- Use [Strata](https://github.com/OpenGamma/Strata) for valuation and scenario-calculation design,
  not for fair-value hierarchy classification.
- Inspect the official [Kraken CLI](https://github.com/krakenfx/kraken-cli) for current Rust
  WebSocket, paper execution, credential, typed-output, and local MCP patterns, but independently
  implement and test Kraken checksum qualification.

That composition is an inference from the sources. No selected repository independently satisfies
the complete Market Squawk specification.

## Candidate Sources

| ID | Source | URL | Type | Credibility Signal | Freshness Signal | Priority | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| GH-01 | `nautechsystems/nautilus_trader` | [GitHub](https://github.com/nautechsystems/nautilus_trader) | Rust/Python trading engine | 24,717 stars; 3,170 forks; extensive docs, tests, adapters, benchmarks, security controls; LGPL-3.0 | `v1.230.0` released 2026-06-29; default branch pushed 2026-07-16 UTC | P0 | Strongest broad reference for financial domain types, deterministic event processing, live adapters, order management, risk, simulation, and multi-venue execution. |
| GH-02 | `barter-rs/barter-rs` | [GitHub](https://github.com/barter-rs/barter-rs) | Rust trading framework | 2,202 stars; 355 forks; modular crates, docs, examples, tests; MIT | `barter-v0.12.5` released 2026-05-09; pushed 2026-06-06 | P0 | Directly reusable Rust patterns for bounded event-driven trading, REST/WebSocket integrations, strategy/risk traits, OMS, paper trading, and backtests. |
| GH-03 | `apache/arrow-rs` | [GitHub](https://github.com/apache/arrow-rs) | Official Rust data ecosystem | Apache Software Foundation; 3,527 stars; 1,213 forks; official Arrow and Parquet Rust implementation; Apache-2.0 | `59.1.0` released 2026-07-07; pushed 2026-07-15 UTC | P0 | Required foundation for Arrow record batches, Decimal128 schemas, Parquet persistence, IPC, and columnar interchange. |
| GH-04 | `apache/datafusion` | [GitHub](https://github.com/apache/datafusion) | Official embedded SQL engine | Apache Software Foundation; 8,982 stars; 2,229 forks; 14k+ commits, examples, benchmarks, active roadmap; Apache-2.0 | Main pushed 2026-07-16 UTC; active 2026 Q3-Q4 roadmap | P0 | Required embedded SQL/DataFrame engine over local Arrow, Parquet, CSV, and JSON with extension points for domain-specific tables and functions. |
| GH-05 | `microsoft/qlib` | [GitHub](https://github.com/microsoft/qlib) | Quant research/modeling platform | Microsoft; 46,268 stars; 7,372 forks; tests, docs, papers, point-in-time database, full research workflow; MIT | Main pushed 2026-04-22; `v0.9.7` released 2025-08-15 | P1 | High-value reference for point-in-time data, features, model training/evaluation, portfolio construction, execution simulation, and leakage-aware research workflows. |
| GH-06 | `OpenBB-finance/OpenBB` | [GitHub](https://github.com/OpenBB-finance/OpenBB) | Financial-data provider platform | 70,630 stars; 7,175 forks; extensive provider packages and recorded HTTP tests; AGPL-3.0 | `Open-Data-Platform-v1.0.2` released 2026-04-25; pushed 2026-07-16 UTC | P1 | Directly relevant adapter examples for SEC filings/company facts, FRED, BLS, and US Treasury, plus provider registration and normalized-query patterns. |
| GH-07 | `modelcontextprotocol/rust-sdk` | [GitHub](https://github.com/modelcontextprotocol/rust-sdk) | Official protocol SDK | Official MCP project; 3,636 stars; 562 forks; typed Rust client/server, macros, transports, examples | `rmcp-v2.2.0` released 2026-07-08; pushed 2026-07-15 UTC | P0 | Best implementation base for a typed local stdio MCP server while Market Squawk retains its own authorization, result limits, audit, and artifact controls. |
| GH-08 | `pykeio/ort` | [GitHub](https://github.com/pykeio/ort) | Rust ONNX runtime binding | 2,400 stars; 255 forks; examples, tests, security policy; Apache-2.0 OR MIT | Pushed 2026-07-13; `v2.0.0-rc.12` released 2026-03-05 | P1 | Practical local ONNX-compatible inference backend for Rust, allowing Python training without Python calls in the live path. |
| GH-09 | `OpenGamma/Strata` | [GitHub](https://github.com/OpenGamma/Strata) | Valuation and market-risk library | OpenGamma; 950 stars; 313 forks; production-used pricing/risk modules, tests, docs; Apache-2.0 | `v2.12.73` released and pushed 2026-07-02 | P2 | Mature reference for product, market-data, pricer, scenario, calculation, measure, and reporting boundaries used in valuation and risk analytics. |
| GH-10 | `krakenfx/kraken-cli` | [GitHub](https://github.com/krakenfx/kraken-cli) | Official Rust exchange client/CLI | Official Kraken organization; 669 stars; 89 forks; Rust; tests, signed release guidance, typed JSON/NDJSON, paper and MCP surfaces; MIT | Created 2026-03-06; `v0.3.2` released/pushed 2026-04-20; repo metadata updated 2026-07-14 | P1 | Current official Rust reference for Kraken Spot/Futures REST and WebSockets, public books/trades, paper execution, credentials, error envelopes, and stdio MCP. |

Counts are point-in-time GitHub API observations, not quality scores.

## Candidate Notes

### GH-01 — nautechsystems/nautilus_trader

NautilusTrader describes a Rust-native core spanning research, deterministic simulation, and live
execution, with modular REST/WebSocket adapters and integrations that include both Coinbase and
Kraken. Its repository also exposes financial precision modes, an order-management model,
multi-venue support, benchmarking guidance, fuzz targets, and unusually detailed supply-chain
security practices. These are valuable implementation references for Market Squawk's stages 1, 2,
5, and 7.

Caveats: its research/live parity goal differs from Market Squawk's intentionally independent
pipelines; Python is part of its control plane; Redis is an optional persistence path; and its
LGPL-3.0 license deserves legal review before any Rust code reuse, particularly around linking and
distribution. The project also warns that its v2 line is still stabilizing. Prefer architecture
study and carefully isolated reuse over adopting the whole engine.

### GH-02 — barter-rs/barter-rs

Barter's workspace separates data, execution, instruments, integrations, and the engine. It is a
particularly close Rust reference for Market Squawk's proposed crate boundaries and trait-driven
source/execution contracts. The repository documents low-allocation, data-oriented state,
Tokio-based I/O, strategy and risk-manager components, paper/live/backtest operation, and a
stand-alone order-management system.

Caveats: the project carries an explicit educational/research-only disclaimer and is not certified
for production trading. Its normalized events and integrations must not be assumed to establish
Market Squawk's stricter `DirectVerified` status. Sequence continuity, venue checksum algorithms,
coverage, timestamps, trading status, precision, quarantine, and risk non-bypass still require
Market Squawk-owned validation and tests.

### GH-03 and GH-04 — apache/arrow-rs and apache/datafusion

These are the official implementations of the exact analytical baseline in the specification.
`arrow-rs` includes Arrow and Parquet Rust crates; DataFusion is an extensible Rust query engine
using Arrow in memory and supporting Parquet, CSV, JSON, SQL, DataFrames, streaming/vectorized
execution, custom functions, and custom data sources. They should be treated as a paired dependency
family for research datasets, manifests, point-in-time queries, and local analytics.

Caveats: both projects evolve quickly. Pin compatible versions in `Cargo.lock`, budget compile time
and feature size, test Decimal128 and timestamp semantics explicitly, and keep query contexts,
Parquet writers, and all analytical I/O outside the live event-to-action path.

### GH-05 — microsoft/qlib

Qlib explicitly includes a point-in-time database, data processing, model training, backtesting,
risk modeling, portfolio optimization, and execution within an AI-oriented quant research system.
It is useful for designing feature/dataset registries, train-validation-test boundaries, offline
and online model workflows, and reproducible evaluation rather than as a runtime dependency.

Caveats: Qlib is Python-first and therefore unsuitable for Market Squawk's live hot path. Its README
states that its official dataset is temporarily disabled and warns that its public Yahoo-derived
sample data may be imperfect. Market Squawk must supply its own licensed/public persisted datasets,
availability timestamps, revision history, delisted securities, historical constituents, and
corporate-action policy.

### GH-06 — OpenBB-finance/OpenBB

The repository's provider tree contains distinct `bls`, `fred`, `government_us`, and `sec`
packages. Inspected paths include BLS series/search assets and tests, numerous FRED models and
recorded HTTP fixtures, US Treasury auction and price models, and SEC filings, company facts,
statements, schemas, and XBRL helpers. That makes OpenBB a concentrated reference for four required
Market Squawk research adapters and for provider registration, query models, source-specific
normalization, and deterministic HTTP-fixture testing.

Caveats: all repository files are AGPL-3.0, which is materially different from Market Squawk's
planned `Apache-2.0 OR MIT` licensing. Treat the code as a behavioral and architectural reference
unless the licensing strategy deliberately changes. OpenBB also supports a mixture of public,
credentialed, and commercial sources; the existence of an adapter does not establish zero cost,
legal availability, point-in-time correctness, or sufficient coverage.

### GH-07 — modelcontextprotocol/rust-sdk

The official SDK provides Rust client/server abstractions, typed tool support, macros, JSON schema
integration, and local transports. It is the most credible base for Market Squawk's version-one
stdio server.

Caveats: the repository is young and fast-moving. Its license file records an ongoing transition
from MIT to Apache-2.0, leaving a mixed-license history. Pin a reviewed release, keep Market
Squawk-owned request/result types stable, and implement domain authorization, cancellation, audit,
bounded result sets, controlled artifacts, and execution/risk policy above the SDK. MCP must never
be invoked from the live path.

### GH-08 — pykeio/ort

`ort` wraps ONNX Runtime and also advertises alternative pure-Rust runtime support. It is a credible
way to load model bundles trained in Python and perform local Rust inference through a narrow
`InferenceBackend` implementation.

Caveats: the current 2.0 release is still an RC. Market Squawk must pin the wrapper and native
runtime, make offline/reproducible runtime provisioning explicit, validate model opsets and tensor
schemas at bundle load, measure warmed latency on target hardware, and ensure every load or
inference error produces no automated action.

### GH-09 — OpenGamma/Strata

Strata's modules separate product definitions, market data, pricers, scenario calculations,
measures, loading, and reporting. Those boundaries are useful for Market Squawk's valuation,
scenario, curve, rate, and market-risk design.

Caveats: Strata is Java, not Rust, and it is a quantitative analytics library rather than an ASC 820
or IFRS 13 hierarchy workflow. It does not replace evidence capture, source/venue qualification,
active-market and accessibility tests, ruleset versions, overrides, or approvals. Its modeled
outputs must never be promoted to `DirectVerified` or Level 1 evidence merely because they produce
a price.

### GH-10 — krakenfx/kraken-cli

This official Rust CLI exposes public Spot WebSocket v2 book/trade streams, Futures WebSockets,
authenticated order/account streams, paper trading, typed JSON/NDJSON output, categorized error
envelopes, signed release verification guidance, endpoint overrides, credential controls, and a
local MCP server. Its safeguards around dangerous MCP tools and explicit rate-limit errors are
useful control-plane references.

Caveats: it is only a few months old and is structured as a CLI rather than a stable embeddable
library. A repository code search found Kraken `checksum` fields in WebSocket message examples but
did not establish that the CLI reconstructs and validates the venue book checksum. Market Squawk
must independently implement the documented checksum algorithm, snapshot/delta ordering,
connection generations, resynchronization, precision, freshness, and quarantine rules before
granting `DirectVerified` status. The CLI's paper engine should be treated as a behavioral reference,
not accepted as satisfying realistic fill requirements without its own tests and calibration.

## Excluded Sources

| Source | URL | Reason Excluded |
| --- | --- | --- |
| `QuantConnect/Lean` | [GitHub](https://github.com/QuantConnect/Lean) | Mature Apache-2.0 engine with broad portfolio, risk, brokerage, and backtest value, but C# and substantially overlapping with NautilusTrader, Barter, and Qlib in a ten-source shortlist. Retain as a secondary design reference. |
| `nkaz001/hftbacktest` | [GitHub](https://github.com/nkaz001/hftbacktest) | Strong Rust L2/L3, queue-position, feed/order latency, and market-making simulation reference, but narrower than the selected broad engines and last pushed in December 2025. Revisit during performance/backtest deep dives. |
| `coinbase/coinbase-advanced-py` | [GitHub](https://github.com/coinbase/coinbase-advanced-py) | Official, current Apache-2.0 SDK with WebSocket support, but Python-only and partly redundant with NautilusTrader's Coinbase adapter. Coinbase's official protocol documentation should remain authoritative for a fresh Rust implementation. |
| `ccxt/ccxt` | [GitHub](https://github.com/ccxt/ccxt) | Very broad multi-exchange adapter ecosystem, but not Rust and its universal interface is a poor authority for venue-specific sequence, checksum, coverage, and `DirectVerified` semantics. Useful only as a secondary compatibility reference. |
| `pola-rs/polars` | [GitHub](https://github.com/pola-rs/polars) | Excellent Rust DataFrame engine, but the specification explicitly selects Arrow record batches plus DataFusion. Adding a second query/DataFrame stack would increase compile, schema, and semantic complexity in release one. |
| `delta-io/delta-rs` | [GitHub](https://github.com/delta-io/delta-rs) | Credible native Rust Delta Lake implementation, but transactional lakehouse semantics and object-store integration exceed the initial local Parquet-manifest requirement. Reconsider only when atomic multi-writer datasets become necessary. |

## Coverage Gaps

1. **Execution-quality qualification remains custom work.** No selected repository demonstrates the
   complete `DirectVerified` predicate: source/venue/instrument identity, explicit coverage,
   sequence and snapshot consistency, venue checksum, exchange/receive timestamps, trading status,
   precision, freshness, bounded overflow policy, quarantine, and verified resynchronization.
2. **ASC 820 and IFRS 13 workflow is not covered.** Strata provides pricing and market-risk
   calculations, not fair-value hierarchy evidence, active/accessibility assessment, ruleset
   versioning, overrides, or approvals. A Market Squawk-owned evidence and classification engine is
   required.
3. **Point-in-time fundamentals need stronger evidence.** Qlib has PIT concepts and OpenBB has SEC
   adapters, but the combined bitemporal model (`effective_at`, `published_at`, `available_at`,
   revisions, and `superseded_at`) must be implemented and tested locally.
4. **No free comprehensive security master was found.** Provider IDs, FIGI/CUSIP/ISIN/SEDOL where
   legally available, symbol history, option/futures identity, mergers, delistings, and contract
   rolls will need source-specific public imports and user-maintained mappings with coverage flags.
5. **Portfolio import/reconciliation remains underrepresented.** OFX/QFX, broker CSV variants,
   tax-lot methods, source-record preservation, and supplied-total reconciliation need dedicated
   Market Squawk adapters and fixtures.
6. **Required Coinbase and Kraken semantics must follow primary protocol documentation.** Selected
   repositories are implementation references, not substitutes for current official API schemas,
   sequence/checksum rules, rate policies, and coverage disclosures.
7. **Benchmarks are not portable claims.** Upstream performance claims cannot establish Market
   Squawk's 100,000 events/s or sub-millisecond warmed p99 targets. Those require the specified
   fixture and target-hardware measurements.
8. Source health, caching, failover, and bounded backoff are appropriate.

## Source List

All sources were accessed on **2026-07-15**.

1. [nautechsystems/nautilus_trader repository](https://github.com/nautechsystems/nautilus_trader),
   [latest observed release](https://github.com/nautechsystems/nautilus_trader/releases/tag/v1.230.0),
   and [GitHub metadata API](https://api.github.com/repos/nautechsystems/nautilus_trader).
2. [barter-rs/barter-rs repository](https://github.com/barter-rs/barter-rs),
   [latest observed release](https://github.com/barter-rs/barter-rs/releases/tag/barter-v0.12.5),
   and [GitHub metadata API](https://api.github.com/repos/barter-rs/barter-rs).
3. [apache/arrow-rs repository](https://github.com/apache/arrow-rs),
   [release 59.1.0](https://github.com/apache/arrow-rs/releases/tag/59.1.0), and
   [GitHub metadata API](https://api.github.com/repos/apache/arrow-rs).
4. [apache/datafusion repository](https://github.com/apache/datafusion) and
   [GitHub metadata API](https://api.github.com/repos/apache/datafusion).
5. [microsoft/qlib repository](https://github.com/microsoft/qlib),
   [release v0.9.7](https://github.com/microsoft/qlib/releases/tag/v0.9.7), and
   [GitHub metadata API](https://api.github.com/repos/microsoft/qlib).
6. [OpenBB-finance/OpenBB repository](https://github.com/OpenBB-finance/OpenBB),
   [provider packages](https://github.com/OpenBB-finance/OpenBB/tree/develop/openbb_platform/providers),
   [license](https://github.com/OpenBB-finance/OpenBB/blob/develop/LICENSE), and
   [GitHub metadata API](https://api.github.com/repos/OpenBB-finance/OpenBB).
7. [modelcontextprotocol/rust-sdk repository](https://github.com/modelcontextprotocol/rust-sdk),
   [release rmcp-v2.2.0](https://github.com/modelcontextprotocol/rust-sdk/releases/tag/rmcp-v2.2.0),
   [license transition text](https://github.com/modelcontextprotocol/rust-sdk/blob/main/LICENSE), and
   [GitHub metadata API](https://api.github.com/repos/modelcontextprotocol/rust-sdk).
8. [pykeio/ort repository](https://github.com/pykeio/ort),
   [release v2.0.0-rc.12](https://github.com/pykeio/ort/releases/tag/v2.0.0-rc.12), and
   [GitHub metadata API](https://api.github.com/repos/pykeio/ort).
9. [OpenGamma/Strata repository](https://github.com/OpenGamma/Strata),
   [release v2.12.73](https://github.com/OpenGamma/Strata/releases/tag/v2.12.73), and
   [GitHub metadata API](https://api.github.com/repos/OpenGamma/Strata).
10. [krakenfx/kraken-cli repository](https://github.com/krakenfx/kraken-cli),
    [release v0.3.2](https://github.com/krakenfx/kraken-cli/releases/tag/v0.3.2), and
    [GitHub metadata API](https://api.github.com/repos/krakenfx/kraken-cli).
11. Exclusion evidence: [QuantConnect/Lean](https://github.com/QuantConnect/Lean),
    [nkaz001/hftbacktest](https://github.com/nkaz001/hftbacktest),
    [coinbase/coinbase-advanced-py](https://github.com/coinbase/coinbase-advanced-py),
    [ccxt/ccxt](https://github.com/ccxt/ccxt), [pola-rs/polars](https://github.com/pola-rs/polars),
    and [delta-io/delta-rs](https://github.com/delta-io/delta-rs).
