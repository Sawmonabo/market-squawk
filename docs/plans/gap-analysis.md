# Market Squawk Gap Analysis

## Document control

- Analysis date: 2026-07-16
- Evidence baseline: [`current-state.md`](../architecture/current-state.md)
- Target contract: [`target-state.md`](../architecture/target-state.md)
- Research evidence: [deep-research report](../research/2026-07-15-market-squawk/final-report.md)

## Status definitions

- **Implemented**: working production capability exists and is deterministically tested.
- **Partial**: a useful subset works, but the requirement is not complete.
- **Missing**: no working production implementation exists.
- **Incorrect**: implementation exists but does not satisfy the required contract.
- **Unsafe**: current behavior can violate integrity, execution, security, privacy, access, or
  accounting boundaries.
- **Intentionally deferred**: explicitly optional capability is excluded from the first local
  release while a safe extension boundary is retained.

Interfaces, empty crates, mocks, synthetic feeds, roadmap text, and schemas without a working
producer and consumer do not count as implemented.

## 1. Product and cost boundary

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| P-01 | Local-first platform | Partial | Current capture, journal, CLI, and MCP are local; most product domains are missing. | 1-7 |
| P-02 | Direct live market ingestion | Partial | Coinbase public WebSocket works; required Kraken and broader contracts are missing. | 2 |
| P-03 | Low-latency signals and automated actions | Partial | Five features and a paper bot exist, but qualification, sharding, and realistic execution do not. | 1-2, 5 |
| P-04 | Historical and point-in-time research | Missing | No research storage, observations, revisions, or PIT builder. | 3-4 |
| P-05 | Modeling, prediction, and backtesting | Missing | No registry, bundles, inference, datasets, or backtester. | 4 |
| P-06 | Fundamentals, filings, macro, portfolio, alternative data | Missing | No required extraction adapters or datasets. | 3-4 |
| P-07 | Portfolio analytics and risk | Missing | `PaperAccount` is not a portfolio system. | 4 |
| P-08 | ASC 820 and IFRS 13 analysis | Missing | No valuation domain or evidence rules. | 6 |
| P-09 | Local MCP access | Partial | Five local stdio tools work; lifecycle, service domains, cancellation, audit, and bounds are incomplete. | 6 |
| P-10 | No paid software/API/cloud/external DB/container/telemetry requirement | Implemented | Current v0.1 has none; all later dependencies must preserve this gate. | All |
| P-11 | Optional paid/licensed adapters do not become mandatory | Missing | No adapter metadata/configuration contract yet. | 1-3 |
| P-12 | Provider restrictions handled by adapters, persistence, cache, health, failover, coverage | Partial | Coinbase reconnect exists; budgets, cache, persistent health, failover, and coverage types are absent. | 1-3 |
| P-13 | No access-control or quota evasion | Implemented | Security/README reject it; production schemas and tests must preserve exclusion. | 1, 7 |
| P-14 | Optional paid/licensed feed adapters | Intentionally deferred | They are not required for the complete local release; typed source/authorization/coverage contracts remain available without introducing a paid dependency. | Post-release |

## 2. Data classification and execution eligibility

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| Q-01 | Separate `FairValueHierarchy` | Missing | No fair-value types. | 1, 6 |
| Q-02 | Separate `MarketDepth` | Missing | Depth is implicit price-level behavior. | 1 |
| Q-03 | Exact `DataQuality` taxonomy | Missing | `QualityState` is an operational state, not data quality. | 1 |
| Q-04 | Only `DirectVerified` eligible by default | Unsafe | `QualityState::Valid` permits paper action without the required evidence. | 1 |
| Q-05 | Known source, venue, instrument | Incorrect | Source/product strings exist; internal venue/instrument identity does not. | 1 |
| Q-06 | Direct venue or authorized broker delivery | Partial | Coinbase is direct; authorization and source metadata are not typed. | 1-2 |
| Q-07 | Valid sequence progression | Unsafe | Heartbeat monotonicity is checked, not update-level continuity. | 1-2 |
| Q-08 | Snapshot/update consistency | Partial | Delta-before-snapshot and reconnect recovery exist; connection generation and atomic validation do not. | 1-2 |
| Q-09 | Checksum where supported | Missing | Kraken checksum adapter absent. | 2 |
| Q-10 | Valid exchange and receive timestamps | Partial | Receive time exists and some timestamps parse; complete sanity policy is absent. | 1-2 |
| Q-11 | Freshness within limits | Partial | Book receive-time staleness exists; clock/timestamp and source-specific limits are incomplete. | 1-2 |
| Q-12 | Valid trading status | Missing | Source status is not instrument/venue trading status. | 1-2 |
| Q-13 | Valid price/quantity precision | Partial | Positive Decimal parsing exists; instrument tick/lot exactness does not. | 1-2 |
| Q-14 | Explicit source coverage | Missing | Coinbase single-venue/non-consolidated coverage is not recorded. | 1-2 |
| Q-15 | Non-verified quality restricted to research/display/manual use | Unsafe | Paper bot can act on operational `Valid` Coinbase Level 2 data. | 1 |
| Q-16 | Level 2/3 valuation inputs never promoted to Level 1 or execution quality | Missing | Types and conversion barriers do not exist. | 1, 6 |

## 3. Architecture and hot path

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| A-01 | Shared domain with independent live and research pipelines | Missing | One live-focused monolith exists; research plane absent. | 1, 3 |
| A-02 | Local control plane with CLI, SQLite, MCP | Partial | CLI/MCP exist; SQLite and application services absent. | 1, 3, 6 |
| A-03 | No SQLite/DataFusion/Parquet/Python/MCP/LLM in live path | Partial | Those components are absent; MCP shares the global engine lock. | 1, 3, 6 |
| A-04 | No arbitrary filesystem work in live path | Unsafe | Source awaits journal serialization/write acknowledgement before publication. | 1 |
| A-05 | No unrelated network work in live path | Implemented | Only source WebSocket traffic exists in current event path. | All |
| A-06 | No unbounded queue writes | Implemented | Journal and event queues are bounded. | All |
| A-07 | Persistence and reporting outside event-to-action path | Unsafe | Capture acknowledgement is a prerequisite for the decoded event. | 1 |
| A-08 | CLI and MCP share application services | Missing | Both bind directly to current concrete engine/replay code. | 1, 6 |

## 4. Rust baseline and conventions

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| R-01 | Rust 1.97.0 toolchain | Incorrect | Repository pins 1.85.0; installed stable is 1.95.0. | 1 |
| R-02 | Edition 2024 and resolver 3 | Partial | Edition 2024 exists; no virtual workspace or explicit resolver. | 1 |
| R-03 | Workspace members `apps/*`, `crates/*`, `adapters/*` | Missing | Single package. | 1-6 |
| R-04 | Inherited workspace metadata and lints | Missing | Package-local metadata/lints only. | 1 |
| R-05 | Required strict lint policy | Incorrect | `clippy::all` warns and required deny lints are absent. | 1 |
| R-06 | Committed `Cargo.lock`, stable Rust, stable fallbacks | Implemented | Lockfile exists and stable Rust is used. | All |
| R-07 | Mature baseline libraries | Partial | Core live libraries exist; Reqwest, Arrow, Parquet, DataFusion, SQLite, Thiserror, Proptest, Criterion, fuzzing are missing. | 1-4, 7 |
| R-08 | Rust naming and rustfmt conventions | Partial | Most names/style comply; full workspace checks and API review remain. | 1-7 |
| R-09 | Financial values not interchangeable primitives | Incorrect | Public Decimals and Strings lack scale/identity invariants. | 1 |
| R-10 | Scaled integers in live path | Missing | Decimal is used in the book and bot. | 1 |
| R-11 | Tick/lot definition and checked adapter conversion | Missing | Positive Decimal validation only. | 1-2 |
| R-12 | Decimal/Decimal128 for accounting/analytics | Partial | Decimal is used; Arrow Decimal128 absent. | 3-4 |
| R-13 | Explicit currency, scale, rounding, checked arithmetic | Missing | No currency/scale/rounding types; multiplication is unchecked. | 1, 4-5 |
| R-14 | No float for money/orders/balances/cost basis/fees | Implemented | Current financial path uses Decimal. | All |
| R-15 | Private fields and invariant-preserving constructors | Incorrect | Most public domain and risk structs expose fields. | 1 |
| R-16 | Typed `Result`, no production unwrap/expect/panic | Partial | Production scan is clean; libraries use `anyhow` rather than typed errors. | 1 |
| R-17 | Complete rustdoc contracts | Partial | Crate comments and a few comments exist; public API documentation is incomplete. | 1-7 |

## 5. Source framework and adapters

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| S-01 | Separate metadata/live/extraction contracts | Incorrect | One live-only `MarketSource` combines journal and events. | 1 |
| S-02 | Live source classes extensible to WS/REST/broker/FIX/native | Missing | Only Coinbase WebSocket concrete source. | 1-2 |
| S-03 | Research file/database/filing/macro/portfolio source classes | Missing | No extraction framework. | 3 |
| S-04 | Working Coinbase book and trade adapter | Partial | Level 2, heartbeat, matches work; production qualification/coverage are incomplete. | 2 |
| S-05 | Working Kraken book adapter with checksum | Missing | No adapter. | 2 |
| S-06 | Working SEC filings and Company Facts | Missing | No adapter. | 3 |
| S-07 | Working FRED and ALFRED | Missing | No adapter. | 3 |
| S-08 | Working BLS | Missing | No adapter. | 3 |
| S-09 | Working Treasury | Missing | No adapter. | 3 |
| S-10 | Working CSV, JSON/NDJSON, Parquet | Missing | No file extraction adapters. | 3 |
| S-11 | Working portfolio holdings/transactions | Missing | No portfolio adapter. | 3-4 |
| S-12 | Working paper execution adapter | Incorrect | Immediate in-engine fills do not implement the adapter contract or realism. | 5 |
| S-13 | Synthetic source never represented as production | Incorrect | `MockSource` is compiled into the application and exposed as a `MarketSource` CLI command; move it to test/diagnostic support with no production registration or qualification path. | 1 |
| S-14 | Equity coverage disclosure | Missing | No equity adapter or coverage metadata type. | 1-3 |
| S-15 | Working XML and Excel extraction | Missing | No bounded parser, schema policy, or formula/entity safety. | 3 |
| S-16 | Working SQLite and database-export extraction | Missing | No read-only export adapter or allowlisted schema contract. | 3 |
| S-17 | Working OFX/QFX and broker-export extraction | Missing | No financial-export parser, raw-record preservation, or reconciliation. | 3 |
| S-18 | User-owned/licensed and alternative file datasets | Missing | No generic extraction/provenance path or dataset manifest. | 3 |

## 6. Canonical domain and provenance

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| D-01 | Internal instrument identity independent of symbols | Missing | Product string is the identity. | 1 |
| D-02 | Ticker/venue, CUSIP, ISIN, SEDOL, FIGI | Missing | No identifier registry. | 1, 3 |
| D-03 | OCC, futures, crypto pair/chain, provider IDs | Missing | No typed identities. | 1-3 |
| D-04 | Symbol history, mergers, delistings, rolls, corporate actions | Missing | No lifecycle model. | 3-4 |
| D-05 | Complete canonical live event enum | Partial | Book/trade exists; quote, auction, halt, instrument status, corporate action are missing. | 1-2 |
| D-06 | Separate research observation enum | Missing | No research domain. | 1, 3 |
| D-07 | Canonical live provenance fields | Partial | Source, timestamps, raw envelope exist; internal IDs, schema, quality, coverage, hash/reference are incomplete. | 1 |
| D-08 | Research effective/published/revision/superseded semantics | Missing | No research observations. | 1, 3 |
| D-09 | Look-ahead prevention without live-feed mirroring | Missing | No point-in-time storage/builder. | 3-4 |

## 7. Live processing, concurrency, integrity, and books

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| L-01 | Reader -> decode -> validate -> shard -> state -> strategy -> risk -> execution | Incorrect | Journal acknowledgement precedes decode; no shard or execution adapter. | 1-2, 5 |
| L-02 | Raw capture async; decisions do not wait for persistence | Unsafe | Current source awaits writer acknowledgement. | 1 |
| L-03 | Stable deterministic shard ownership | Missing | One global engine task and lock. | 1 |
| L-04 | Shard owns books/windows/features/strategy/risk | Missing | Global engine owns all products. | 1-2 |
| L-05 | Explicit bounded overflow invalidation/degradation | Incorrect | Bounded `send().await` has no integrity transition. | 1 |
| L-06 | Sequence continuity, duplicates, out-of-order | Partial | Heartbeat non-monotonic detection only. | 1-2 |
| L-07 | Snapshot/delta ordering | Partial | Delta-before-snapshot and reconnect reset exist; generation/sequence replay incomplete. | 1-2 |
| L-08 | Checksums | Missing | No supported-source checksum implementation. | 2 |
| L-09 | Connection generations | Missing | UUID exists in raw envelope but not validated canonical state. | 1-2 |
| L-10 | Timestamp sanity | Partial | Parsing exists; sanity/clock bounds absent. | 1-2 |
| L-11 | Tick/lot precision | Missing | No instrument definition enforcement. | 1-2 |
| L-12 | Book consistency | Partial | Crossed book check exists; negative/atomic/depth/checksum invariants incomplete. | 1-2 |
| L-13 | Trading/venue status | Missing | No typed status gate. | 1-2 |
| L-14 | Market freshness distinct from heartbeat | Implemented | Heartbeats do not refresh book freshness. | All |
| L-15 | Quarantine until resynchronized/revalidated | Partial | Fresh snapshot clears hard invalidity; complete evidence requalification absent. | 1-2 |
| L-16 | Top/price/order depth support | Partial | Price-level depth works; explicit top/order-level contracts absent. | 1-2 |
| L-17 | Snapshot, incremental, delete-zero, configurable depth | Partial | First three work; depth is unbounded/not configured. | 2 |
| L-18 | Best bid/ask, crossed, staleness | Partial | Best/crossed and external quality staleness exist; typed book freshness incomplete. | 1-2 |
| L-19 | Venue-specific checksum rules | Missing | Kraken adapter absent. | 2 |

## 8. Research storage and datasets

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| T-01 | SQLite control metadata | Missing | No database. | 3 |
| T-02 | Arrow in-memory exchange | Missing | No Arrow. | 3 |
| T-03 | Parquet durable analytical data | Missing | No Parquet. | 3 |
| T-04 | DataFusion embedded SQL | Missing | No DataFusion. | 3 |
| T-05 | Required core dataset families | Missing | None of the listed analytical datasets exists. | 3-6 |
| T-06 | Versioned schemas and manifests | Missing | No dataset manager. | 3 |
| T-07 | Idempotency and deduplication | Missing | Journal append is not analytical ingestion. | 3 |
| T-08 | Query-driven partitioning and compaction | Missing | No Parquet datasets. | 3 |
| T-09 | Small-file avoidance | Missing | No dataset writer. | 3 |
| T-10 | PIT filtering and revision preservation | Missing | No bitemporal/vintage store. | 3-4 |
| T-11 | Corporate-action policy | Missing | No corporate-action processing. | 3-4 |
| T-12 | Historical sources may differ from live | Implemented | Current design does not force equivalence; target docs preserve separation. | All |

## 9. Features and analytics

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| F-01 | Spread, midpoint, microprice | Implemented | Current top-of-book kernels work. | All |
| F-02 | Book imbalance | Partial | Top-level imbalance exists; configurable/depth variants absent. | 2 |
| F-03 | Order-flow imbalance and depth-weighted price | Missing | No kernels. | 2 |
| F-04 | Trade aggressor imbalance | Missing | Maker side captured; aggressor feature absent. | 2 |
| F-05 | Rolling VWAP, volume velocity, momentum | Partial | Bot has primitive midpoint momentum; registry-quality kernels absent. | 2, 4 |
| F-06 | Rolling returns and volatility | Missing | No rolling statistical state. | 2, 4 |
| F-07 | Cross-venue divergence | Missing | Only one venue. | 2 |
| F-08 | Liquidity/slippage estimates | Missing | No depth/slippage model. | 2, 5 |
| F-09 | Returns, risk-adjusted performance, drawdown | Missing | No batch analytics. | 4 |
| F-10 | Correlation, beta, alpha, factors | Missing | No batch analytics. | 4 |
| F-11 | VaR and Expected Shortfall | Missing | No portfolio risk library. | 4 |
| F-12 | Fundamental, valuation, FCF, surprise, yield features | Missing | No research inputs/analytics. | 4 |
| F-13 | Portfolio exposure, attribution, scenario/stress | Missing | No portfolio analytics. | 4 |
| F-14 | Shared pure kernels where appropriate | Partial | Current feature code is pure but not a separate analytics boundary. | 1, 4 |

## 10. Modeling, inference, and backtesting

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| M-01 | Complete feature registry metadata | Missing | No registry. | 4 |
| M-02 | Universe selection and historical constituents | Missing | No dataset builder/instrument lifecycle. | 4 |
| M-03 | Delisted instruments and corporate-action treatment | Missing | No PIT universe. | 3-4 |
| M-04 | Point-in-time joins and leakage checks | Missing | No builder. | 4 |
| M-05 | Labels, train/validation/test, missing policies | Missing | No model dataset. | 4 |
| M-06 | Reproducible Parquet outputs | Missing | No Parquet. | 3-4 |
| M-07 | Complete production model bundle | Missing | No registry/bundle. | 4 |
| M-08 | `InferenceBackend` contract | Missing | No inference. | 4 |
| M-09 | Native Rust and ONNX-compatible inference | Missing | No runtime. | 4 |
| M-10 | Python outside live inference | Implemented | Python is absent and current live path is Rust; target preserves boundary. | All |
| M-11 | Inference error produces no action | Missing | No inference/risk integration. | 4-5 |
| M-12 | Backtesting over research datasets | Missing | Replay is not a research backtester. | 4 |

## 11. Strategies, risk, execution, and portfolio

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| E-01 | Typed `Strategy` contract | Missing | Hardcoded bot method only. | 5 |
| E-02 | Complete typed `OrderIntent` | Incorrect | Current intent lacks required fields and private invariants. | 1, 5 |
| E-03 | Separate non-bypassable risk service | Incorrect | In-engine kernel exists; no unforgeable approval/service boundary. | 1, 5 |
| E-04 | Full source/account/instrument/exposure/rate/loss risk | Partial | Quality/age/notional/position/kill checks only. | 5 |
| E-05 | No CLI/MCP/model/adapter risk bypass | Unsafe | Current types do not prevent direct future construction/fill. | 1, 5-6 |
| E-06 | Replaceable execution adapter | Missing | No trait. | 1, 5 |
| E-07 | Realistic paper execution | Incorrect | Immediate full midpoint/limit fill only. | 5 |
| E-08 | Live execution optional and explicit | Implemented | No live execution exists; future boundary remains opt-in. | All |
| E-09 | Accounts, holdings, transactions, cash flows | Missing | Paper state is insufficient. | 4 |
| E-10 | Cost basis, realized/unrealized gains, income | Missing | No portfolio accounting. | 4 |
| E-11 | Allocation and sector/factor/currency/instrument exposure | Missing | No portfolio analytics. | 4 |
| E-12 | Performance, attribution, rebalancing, risk, scenarios | Missing | No portfolio services. | 4 |
| E-13 | Preserve source records and reconcile totals | Missing | No portfolio import. | 3-4 |
| E-14 | Optional authorized live-money execution adapter | Intentionally deferred | The first release requires hardened paper execution and an opt-in adapter contract, not a live broker adapter; no live-money path will be implied or enabled. | Post-release |

## 12. Fair value

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| V-01 | Valuation/input/method/classification/evidence/ruleset storage | Missing | No valuation system. | 6 |
| V-02 | Override and approval audit | Missing | No valuation system. | 6 |
| V-03 | Complete Level 1 candidate evidence | Missing | No rule engine. | 6 |
| V-04 | No silent Level 1 for delayed/stale/proxy/adjusted/modeled/similar | Missing | No typed barrier/rules. | 1, 6 |
| V-05 | Level 2/3 never promoted to execution quality | Missing | No classification/quality conversion barrier. | 1, 6 |

## 13. MCP and CLI

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| C-01 | Local stdio MCP outside live path | Partial | Local stdio exists; shared lock and service isolation are incomplete. | 1, 6 |
| C-02 | Required tool domains | Missing | Only five tools across market/bot/journal/risk. | 6 |
| C-03 | Typed schemas and bounded results | Partial | Input schemas/line bound exist; result/time/instrument bounds incomplete. | 6 |
| C-04 | Cancellation and audit | Missing | No cancellation or audit store. | 6 |
| C-05 | Controlled artifact references | Missing | No artifact service. | 6 |
| C-06 | No arbitrary shell/filesystem/SQL/credentials/remote code/risk bypass | Partial | Current tools avoid these; future policy and service types need tests. | 1, 6 |
| C-07 | Read-only DataFusion SQL through CLI only | Missing | DataFusion/CLI query absent. | 3 |
| C-08 | Complete CLI hierarchy | Missing | Five top-level commands only. | 1, 3-6 |
| C-09 | CLI and MCP reuse services | Missing | No application-service layer. | 1, 6 |

## 14. Project structure, configuration, privacy, and operations

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| O-01 | Target virtual workspace structure | Missing | Single package. | 1-6 |
| O-02 | No empty crates; focused files below 500-700 lines | Implemented | Current files are focused; future plan forbids empty crates. | All |
| O-03 | Config precedence defaults/file/env/CLI | Partial | Defaults/env/CLI exist; config file and full typed merge absent. | 1 |
| O-04 | No telemetry or hidden outbound requests | Implemented | Current binary has no beacon; source connection is explicit. | All |
| O-05 | Local storage and structured human/JSON logs | Implemented | Present. | All |
| O-06 | Secret redaction | Missing | No secret type or tests. | 1, 7 |
| O-07 | OS keyring and encrypted fallback | Missing | No credentials currently. | 3, 5 |
| O-08 | Source/execution endpoint allowlists | Missing | Coinbase `with_url` accepts arbitrary URLs. | 1-2, 5 |
| O-09 | Controlled artifact directory | Missing | No artifact service. | 1, 6 |
| O-10 | Dependency lock, vulnerability, license, credential, artifact checks | Partial | Lockfile and git-history gitleaks pass; other policies/checks absent. | 1, 7 |
| O-11 | No OpenTelemetry v1 dependency | Implemented | No OTEL. | All |
| O-12 | Optional future observability adapter | Intentionally deferred | Version 1 intentionally has local tracing and no OpenTelemetry dependency; a future adapter may be added without changing live-domain contracts. | Post-release |

## 15. Verification and performance

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| X-01 | Financial parsing/normalization tests | Partial | Decimal parsing tests exist; ticks/lots/rounding property tests absent. | 1 |
| X-02 | Instrument resolution tests | Missing | No resolver. | 1, 3 |
| X-03 | Sequence/checksum/book/queue/reconnect/quality tests | Partial | Book/reconnect/basic quality exist; checksum/overflow/generation incomplete. | 1-2 |
| X-04 | PIT/revision/corporate-action tests | Missing | No research plane. | 3-4 |
| X-05 | Feature/model/portfolio/risk/paper/fair-value tests | Partial | Five features/basic risk only. | 2, 4-6 |
| X-06 | MCP schema/result/CLI integration tests | Partial | MCP init/arguments/rate and smoke tests exist; full surface absent. | 6 |
| X-07 | External network tests separate | Implemented | Current live integration uses a local server; public endpoint is not in default suite. | All |
| X-08 | Property tests | Missing | No Proptest. | 1-7 |
| X-09 | Required fuzz targets | Missing | No fuzz workspace. | 2-3, 6-7 |
| X-10 | Required format/clippy/test/release commands | Incorrect | Single-package 1.85 variants pass; workspace 1.97 commands cannot run. | 1 |
| X-11 | Dependency/vulnerability/license/credential/artifact checks | Partial | Gitleaks history scan passes; others absent. | 1, 7 |
| X-12 | Decoder/queue/book/feature/strategy/risk performance measures | Missing | No benchmark harness. | 7 |
| X-13 | Arrow/Parquet/DataFusion performance measures | Missing | Research plane absent. | 7 |
| X-14 | Sustained memory measurement | Missing | No harness. | 7 |
| X-15 | 100k events/s and sub-ms warmed p99 evidence | Missing | No measurements; no claim may be made. | 7 |
| X-16 | Record hardware/OS/toolchain/fixture/count/percentiles/memory | Missing | No benchmark report. | 7 |

## 16. Delivery and definition of done

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| G-01 | Current-state architecture document | Implemented | `docs/architecture/current-state.md`. | Planning |
| G-02 | Target-state architecture document | Implemented | `docs/architecture/target-state.md`. | Planning |
| G-03 | Gap analysis with required classifications | Implemented | This document. | Planning |
| G-04 | Concrete implementation plan | Implemented | Seven-stage controlling plan plus an executable TDD Stage 1 plan are linked from `docs/plans/implementation-plan.md`. | Planning |
| G-05 | Repository runnable after every stage | Partial | Current baseline is runnable; aggregate script is broken. | All |
| G-06 | No mocks/scaffolding counted as production | Implemented | Audit and target explicitly enforce this rule. | All |
| G-07 | Complete local release demonstration | Missing | Only a narrow v0.1 subset exists. | 1-7 |
| G-08 | Replay optional, not core | Implemented | Replay exists but target architecture does not require historical/live equivalence. | All |

## 17. Prohibited evasion additions

| ID | Requested behavior | Status | Disposition |
| --- | --- | --- | --- |
| U-01 | Identity/account rotation to evade limits | Unsafe | Will not implement. Central provider budgets and declared identity replace it. |
| U-02 | Browser/TLS fingerprint spoofing for concealment | Unsafe | Will not implement. Standard verified TLS with explicit proxy policy is required. |
| U-03 | CAPTCHA or anti-bot bypass | Unsafe | Will not implement. Source health degrades and requires authorized/manual access. |
| U-04 | Proxy rotation intended to defeat blocking | Unsafe | Will not implement. Proxies cannot be used for concealment or quota evasion. |
| U-05 | Distributed requests intended to evade quotas | Unsafe | Will not implement. Quotas are aggregate across all local workers and hosts. |

## Critical path

The highest-risk gaps are not the missing feature count; they are contracts that would contaminate
every later feature if left unchanged:

1. Primitive identity and financial values
2. Conflated operational quality and execution eligibility
3. Persistence acknowledgement in the event path
4. Global mutable engine and undefined overflow semantics
5. Forgeable intent-to-fill boundary
6. Missing point-in-time and provenance semantics
7. Missing provider coverage and access policy

Stage 1 closes these cross-cutting contracts before Stage 2 adds live sources or Stage 3 adds
research data. Later stages then implement every required production capability against stable,
testable boundaries.
