# Market Squawk Gap Analysis

## Document control

- Analysis date: 2026-07-16
- Evidence baseline: [`current-state.md`](../architecture/current-state.md)
- Target contract: [`target-state.md`](../architecture/target-state.md)
- Research evidence: [deep-research report](../research/2026-07-15-market-squawk/final-report.md)
- Latest live-runtime evidence:
  [Q2 Task 8 implementation report](../reports/q2-task8-implementation.md)
- Latest independent review:
  [Q2 checkpoint rejection and remediation ledger](../reports/q2-checkpoint-review.md)

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
| P-03 | Low-latency signals and automated actions | Partial | Qualification, current authority, deterministic sharding, and state processing exist, but Q2-R01–Q2-R10 invalidate production-readiness claims until remediation. Task 8 intentionally returns `NoStrategy`; features, risk, and execution remain. | 1-2, 5 |
| P-04 | Historical and point-in-time research | Missing | No research storage, observations, revisions, or PIT builder. | 3-4 |
| P-05 | Modeling, prediction, and backtesting | Missing | No registry, bundles, inference, datasets, or backtester. | 4 |
| P-06 | Fundamentals, filings, macro, portfolio, alternative data | Missing | No required extraction adapters or datasets. | 3-4 |
| P-07 | Portfolio analytics and risk | Missing | `PaperAccount` is not a portfolio system. | 4 |
| P-08 | ASC 820 and IFRS 13 analysis | Missing | No valuation domain or evidence rules. | 6 |
| P-09 | Local MCP access | Partial | Five local stdio tools work; lifecycle, service domains, cancellation, audit, and bounds are incomplete. | 6 |
| P-10 | No paid software/API/cloud/external DB/container/telemetry requirement | Implemented | Current v0.1 has none; all later dependencies must preserve this gate. | All |
| P-11 | Optional paid/licensed adapters do not become mandatory | Implemented | Source authorization, coverage, endpoint, and secret contracts do not impose a paid dependency; concrete optional adapters can remain replaceable. | All |
| P-12 | Provider restrictions handled by adapters, persistence, cache, health, failover, coverage | Unsafe | Typed budgets and health exist, but Q2-R03–Q2-R05 show cooldown revocation, process-wide coordination, and audited account binding are incomplete and can multiply effective quotas. | 1-3 |
| P-13 | No access-control or quota evasion | Unsafe | Deliberate evasion features are absent and prohibited, but Q2-R04–Q2-R05 prevent certifying structural quota aggregation until one process-wide audited provider/account budget is enforced. | 1, 7 |
| P-14 | Optional paid/licensed feed adapters | Intentionally deferred | They are not required for the complete local release; typed source/authorization/coverage contracts remain available without introducing a paid dependency. | Post-release |

## 2. Data classification and execution eligibility

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| Q-01 | Separate `FairValueHierarchy` | Implemented | The domain type is distinct and is not an input to live qualification or authority. | All |
| Q-02 | Separate `MarketDepth` | Implemented | Typed top-of-book, price-level, and order-level classifications are independent of quality and valuation hierarchy. | All |
| Q-03 | Exact `DataQuality` taxonomy | Implemented | The exact closed taxonomy is a private-invariant domain type separate from integrity. | All |
| Q-04 | Only `DirectVerified` eligible by default | Partial | Current production authority can be minted only from complete `DirectVerified` evidence, and Task 8 has no strategy/action path. Task 10 must prove the final risk/dispatch consumer; the old paper calculation is diagnostic-only. | 1, 5 |
| Q-05 | Known source, venue, instrument | Implemented | Typed identities and exact source/venue/instrument route bindings are required by current batches and processors. | All |
| Q-06 | Direct venue or authorized broker delivery | Implemented | Delivery and authorization evidence are typed qualification inputs; absent or inconsistent evidence fails closed. | All |
| Q-07 | Valid sequence progression | Implemented | Transactional generation-owned sequence state rejects duplicates, gaps, regression, and rule transplants where sequence is required. | All |
| Q-08 | Snapshot/update consistency | Implemented | Generation state, snapshot applicability/origin, transactional rollback, and current-generation revalidation are implemented. | All |
| Q-09 | Checksum where supported | Partial | Closed provider checksum profiles and Kraken V2 golden CRC32 validation are implemented; the production Kraken stream adapter remains Task 11. | 2 |
| Q-10 | Valid exchange and receive timestamps | Implemented | Exact timestamps and independent source/market/transport/future-skew/idle policies participate in qualification. | All |
| Q-11 | Freshness within limits | Unsafe | Upper deadlines are derived, but Q2-R02 shows future health can qualify before its claimed observation and poison later health reporting. Trusted-time lower bounds are under remediation. | All |
| Q-12 | Valid trading status | Implemented | Typed generation-bound status is revisioned, snapshotted, and required by qualification. | All |
| Q-13 | Valid price/quantity precision | Implemented | Exact provider lexemes normalize through current tick/lot definitions without rounding. | All |
| Q-14 | Explicit source coverage | Implemented | Typed bounded coverage records include domain, topology, delay, consolidation, membership, and current policy binding. | All |
| Q-15 | Non-verified quality restricted to research/display/manual use | Implemented | Non-verified states cannot obtain production capability; compatibility MCP/replay/paper behavior is explicitly diagnostic. | All |
| Q-16 | Level 2/3 valuation inputs never promoted to Level 1 or execution quality | Implemented | Type separation and absence of hierarchy-to-quality/authority conversions enforce the boundary; the valuation service itself remains Task 6. | All |

## 3. Architecture and hot path

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| A-01 | Shared domain with independent live and research pipelines | Partial | Shared live/research domain contracts and an independent production live crate exist; the research pipeline is not implemented. | 1, 3 |
| A-02 | Local control plane with CLI, SQLite, MCP | Partial | CLI/MCP exist; SQLite and application services absent. | 1, 3, 6 |
| A-03 | No SQLite/DataFusion/Parquet/Python/MCP/LLM in live path | Implemented | The live crate dependency graph and actor path contain none of these; diagnostic MCP cannot access production actor state. | All |
| A-04 | No arbitrary filesystem work in live path | Implemented | Raw capture is a bounded asynchronous side branch; Task 8 performs no filesystem operation. | All |
| A-05 | No unrelated network work in live path | Implemented | Only source WebSocket traffic exists in current event path. | All |
| A-06 | No unbounded queue writes | Implemented | Journal and event queues are bounded. | All |
| A-07 | Persistence and reporting outside event-to-action path | Implemented | Capture admission does not await disk; snapshots/health are separate bounded authority-free outputs. | All |
| A-08 | CLI and MCP share application services | Partial | `LiveRuntimeComposition` is the production app owner, but compatibility CLI/MCP still consume the diagnostic engine until Task 13. | 1, 6 |

## 4. Rust baseline and conventions

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| R-01 | Rust 1.97.0 toolchain | Implemented | `rust-toolchain.toml` pins stable 1.97.0 and workspace MSRV is 1.97. | All |
| R-02 | Edition 2024 and resolver 3 | Implemented | The virtual workspace uses Edition 2024 and resolver 3. | All |
| R-03 | Workspace members `apps/*`, `crates/*`, `adapters/*` | Partial | Working app/domain/platform/sources/live packages use `apps/*` and `crates/*`; `adapters/*` is added atomically with Task 11's first working adapter rather than empty crates. | 1-6 |
| R-04 | Inherited workspace metadata and lints | Implemented | Every current package inherits workspace version, edition, Rust version, license, and lint tables. | All |
| R-05 | Required strict lint policy | Implemented | Workspace lints deny unsafe code, unused results, Clippy all, unwrap, expect, panic, todo, and unimplemented paths. | All |
| R-06 | Committed `Cargo.lock`, stable Rust, stable fallbacks | Implemented | Lockfile exists and stable Rust is used. | All |
| R-07 | Mature baseline libraries | Partial | Tokio, Serde, WebSockets, Thiserror, Proptest, ArcSwap, and core live libraries are in use; research/storage/modeling/benchmark/fuzz dependencies arrive with working consumers. | 1-4, 7 |
| R-08 | Rust naming and rustfmt conventions | Partial | Most names/style comply; full workspace checks and API review remain. | 1-7 |
| R-09 | Financial values not interchangeable primitives | Implemented | Production domain and live state use private typed identities, prices, quantities, sizes, currency, money, and basis points. | All |
| R-10 | Scaled integers in live path | Implemented | Production books and canonical live values use `PriceTicks` and `QuantityLots`; Decimal remains only in diagnostic compatibility code. | All |
| R-11 | Tick/lot definition and checked adapter conversion | Implemented | Exact provider lexemes convert through current instrument definitions without implicit rounding. | All |
| R-12 | Decimal/Decimal128 for accounting/analytics | Partial | Decimal is used; Arrow Decimal128 absent. | 3-4 |
| R-13 | Explicit currency, scale, rounding, checked arithmetic | Implemented | Domain money/currency/tick/lot/rounding types and checked live conversions/arithmetic are implemented. | All |
| R-14 | No float for money/orders/balances/cost basis/fees | Implemented | Current financial path uses Decimal. | All |
| R-15 | Private fields and invariant-preserving constructors | Partial | Production domain/source/live structs preserve invariants; diagnostic compatibility DTOs retain public fields until their Task 13 deletion. | 1 |
| R-16 | Typed `Result`, no production unwrap/expect/panic | Implemented | Library crates expose typed errors and the strict lints pass; `anyhow` remains at the app boundary. | All |
| R-17 | Complete rustdoc contracts | Partial | Crate comments and a few comments exist; public API documentation is incomplete. | 1-7 |

## 5. Source framework and adapters

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| S-01 | Separate metadata/live/extraction contracts | Implemented | `SourceMetadataProvider`, `LiveMarketSource`, and `ExtractionSource` are separate typed contracts. | All |
| S-02 | Live source classes extensible to WS/REST/broker/FIX/native | Partial | Bounded protocol/source metadata and live contracts exist; production adapter implementations remain. | 1-2 |
| S-03 | Research file/database/filing/macro/portfolio source classes | Partial | Bounded discovery/extraction contracts and records exist; working research adapters remain Task 3. | 3 |
| S-04 | Working Coinbase book and trade adapter | Partial | Level 2, heartbeat, matches work; production qualification/coverage are incomplete. | 2 |
| S-05 | Working Kraken book adapter with checksum | Missing | No adapter. | 2 |
| S-06 | Working SEC filings and Company Facts | Missing | No adapter. | 3 |
| S-07 | Working FRED and ALFRED | Missing | No adapter. | 3 |
| S-08 | Working BLS | Missing | No adapter. | 3 |
| S-09 | Working Treasury | Missing | No adapter. | 3 |
| S-10 | Working CSV, JSON/NDJSON, Parquet | Missing | No file extraction adapters. | 3 |
| S-11 | Working portfolio holdings/transactions | Missing | No portfolio adapter. | 3-4 |
| S-12 | Working paper execution adapter | Incorrect | Immediate in-engine fills do not implement the adapter contract or realism. | 5 |
| S-13 | Synthetic source never represented as production | Implemented | `MockSource` is explicitly diagnostic and structurally cannot create a current batch or bind production ingress. | All |
| S-14 | Equity coverage disclosure | Partial | Coverage delay/topology/consolidation/membership contracts exist; no production equity adapter publishes them yet. | 1-3 |
| S-15 | Working XML and Excel extraction | Missing | No bounded parser, schema policy, or formula/entity safety. | 3 |
| S-16 | Working SQLite and database-export extraction | Missing | No read-only export adapter or allowlisted schema contract. | 3 |
| S-17 | Working OFX/QFX and broker-export extraction | Missing | No financial-export parser, raw-record preservation, or reconciliation. | 3 |
| S-18 | User-owned/licensed and alternative file datasets | Missing | No generic extraction/provenance path or dataset manifest. | 3 |

## 6. Canonical domain and provenance

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| D-01 | Internal instrument identity independent of symbols | Implemented | `InstrumentId` is provider-independent and current routes bind it separately from venue/provider symbols. | All |
| D-02 | Ticker/venue, CUSIP, ISIN, SEDOL, FIGI | Implemented | Validated types, evidence records, conflicts, supersession, and an in-memory identity registry are implemented; durable catalog storage remains Task 3. | All |
| D-03 | OCC, futures, crypto pair/chain, provider IDs | Implemented | Validated derivative, digital-asset, chain, and provider identity types are implemented. | All |
| D-04 | Symbol history, mergers, delistings, rolls, corporate actions | Partial | Effective symbol/lifecycle/roll/corporate-action contracts are implemented; durable ingestion and analytical treatment remain. | 3-4 |
| D-05 | Complete canonical live event enum | Implemented | Trade, quote, snapshot, delta, auction, halt, status, and corporate-action variants have typed payloads. | All |
| D-06 | Separate research observation enum | Implemented | Filing, fundamental, macro, position, transaction, corporate-action, and alternative-data variants are separate from live events. | All |
| D-07 | Canonical live provenance fields | Implemented | Schema/source/instrument/venue identifiers, source reference/hash, exact timestamps, quality/evidence binding, and assessment reference are typed. | All |
| D-08 | Research effective/published/revision/superseded semantics | Implemented | The research time/provenance contracts preserve effective, publication, availability, ingestion, revision, and supersession dimensions; storage remains Task 3. | All |
| D-09 | Look-ahead prevention without live-feed mirroring | Missing | No point-in-time storage/builder. | 3-4 |

## 7. Live processing, concurrency, integrity, and books

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| L-01 | Reader -> decode -> validate -> shard -> state -> strategy -> risk -> execution | Partial | Capture/current-batch validation, deterministic shard admission, and transactional state are production contracts. The actor intentionally returns `NoStrategy`; Tasks 9-10 attach features, strategy, risk, and execution at its revalidation seams. | 1-2, 5 |
| L-02 | Raw capture async; decisions do not wait for persistence | Implemented | Task 6 capture admission is bounded and asynchronous; Task 8 consumes receipt-validated current batches and performs no persistence work. | All |
| L-03 | Stable deterministic shard ownership | Implemented | Routing V1 freezes byte encoding and FNV-1a golden vectors; one actor is the sole writer for every route assigned to its shard. | All |
| L-04 | Shard owns books/windows/features/strategy/risk | Partial | Actors own route processors, books, generation state, revisions, snapshots, and the reserved feature/strategy state boundary. Task 9-10 state is not yet attached. | 1-2 |
| L-05 | Explicit bounded overflow invalidation/degradation | Unsafe | Count and invalidation ordering are bounded, but Q2-R01 shows retained-byte admission undercounts nested book allocations, so the configured memory bound is false. | All |
| L-06 | Sequence continuity, duplicates, out-of-order | Implemented | The transactional sequence validator handles progression, duplicates, out-of-order/gap outcomes, generation rollover, and rollback on rejected candidates. | All |
| L-07 | Snapshot/delta ordering | Implemented | Generation-owned state requires initialization snapshots, rejects invalid delta ordering, preserves snapshot-origin identity, and revalidates current generation. | All |
| L-08 | Checksums | Implemented | The closed checksum engine validates provider-declared canonicalization profiles and rejects profile/evidence mismatches; production Kraken adapter integration remains L-19. | All |
| L-09 | Connection generations | Implemented | Current source allocations and route generation registries bind exact typed connection generations; refresh, rollover, transplant, and exit invalidation are tested. | All |
| L-10 | Timestamp sanity | Implemented | Qualification enforces independent market, transport, source, future-skew, idle, wall-deadline, and monotonic capability limits. | All |
| L-11 | Tick/lot precision | Implemented | Adapter-boundary normalization converts exact decimal lexemes through instrument tick/lot definitions with checked representability. | All |
| L-12 | Book consistency | Implemented | Price-level updates are message-atomic with bounded rollback, strict ordering, delete-zero, depth, crossed-book, checksum, and last-good-state invariants. | All |
| L-13 | Trading/venue status | Implemented | Typed generation-bound shared trading status participates in qualification, revisions, snapshot diagnostics, and capability invalidation. | All |
| L-14 | Market freshness distinct from heartbeat | Implemented | Connection/heartbeat health cannot refresh market-price freshness; qualification retains separate limits and exact deadlines. | All |
| L-15 | Quarantine until resynchronized/revalidated | Incorrect | Invalid generations cannot issue action, but Q2-R06 shows a first recoverable rejection can retain incomplete provenance and later terminate the actor during snapshot publication. | All |
| L-16 | Top/price/order depth support | Partial | Top-of-book and configurable bounded price-level depth are implemented. Order-level ownership remains required when a production source supplies it. | 2 |
| L-17 | Snapshot, incremental, delete-zero, configurable depth | Implemented | Transactional snapshots/deltas, delete-zero behavior, checked configured depth, and exact output-depth metadata are tested. | All |
| L-18 | Best bid/ask, crossed, staleness | Implemented | The production book maintains extrema, rejects crossed candidates, and qualification/snapshots preserve typed freshness state. | All |
| L-19 | Venue-specific checksum rules | Partial | Provider checksum profiles and Kraken canonical CRC32 rules are implemented and golden-tested; the production Kraken stream adapter that supplies them is Task 11. | 2 |

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
| O-01 | Target virtual workspace structure | Partial | The virtual workspace has working app/domain/platform/sources/live packages; later production crates/adapters are added with their first consumers. | 1-6 |
| O-02 | No empty crates; focused files below 500-700 lines | Implemented | Current files are focused; future plan forbids empty crates. | All |
| O-03 | Config precedence defaults/file/env/CLI | Implemented | Validated configuration composes defaults, bounded TOML, supplied environment, and CLI overrides in the required order. | All |
| O-04 | No telemetry or hidden outbound requests | Implemented | Current binary has no beacon; source connection is explicit. | All |
| O-05 | Local storage and structured human/JSON logs | Implemented | Present. | All |
| O-06 | Secret redaction | Implemented | Secret references and zeroized values have redacted debug behavior and bounded parsing; concrete keyring/encryption providers remain O-07. | All |
| O-07 | OS keyring and encrypted fallback | Missing | No credentials currently. | 3, 5 |
| O-08 | Source/execution endpoint allowlists | Partial | Typed endpoint/redirect/query policies exist for production adapters; compatibility Coinbase test URL injection is diagnostic and Task 11 must consume the policy. | 1-2, 5 |
| O-09 | Controlled artifact directory | Partial | Confined `ArtifactRoot`/resolved-path primitives are implemented; the shared artifact publication service remains Task 13. | 1, 6 |
| O-10 | Dependency lock, vulnerability, license, credential, artifact checks | Implemented | The committed lock, exact duplicate inventory, Cargo-deny policy, RustSec/cargo-audit scans, generated-artifact checker, and scoped working-tree/history Gitleaks gates pass; final release reruns them at the exact release head. | 1, 7 |
| O-11 | No OpenTelemetry v1 dependency | Implemented | No OTEL. | All |
| O-12 | Optional future observability adapter | Intentionally deferred | Version 1 intentionally has local tracing and no OpenTelemetry dependency; a future adapter may be added without changing live-domain contracts. | Post-release |

## 15. Verification and performance

| ID | Requirement | Status | Evidence and closure | Stage |
| --- | --- | --- | --- | --- |
| X-01 | Financial parsing/normalization tests | Implemented | Exact decimal, ticks/lots, rounding/rejection, overflow, and financial property tests pass. | All |
| X-02 | Instrument resolution tests | Partial | Typed identity records, evidence/conflict/supersession, symbol scope, and in-memory registry tests pass; durable catalog resolution remains Task 3. | 1, 3 |
| X-03 | Sequence/checksum/book/queue/reconnect/quality tests | Partial | Transactional sequence, Kraken checksum, book/property, current-generation overflow, lifecycle, and qualification tests pass; production adapter reconnect/resync remains Task 11. | 1-2 |
| X-04 | PIT/revision/corporate-action tests | Missing | No research plane. | 3-4 |
| X-05 | Feature/model/portfolio/risk/paper/fair-value tests | Partial | Five features/basic risk only. | 2, 4-6 |
| X-06 | MCP schema/result/CLI integration tests | Partial | MCP init/arguments/rate and smoke tests exist; full surface absent. | 6 |
| X-07 | External network tests separate | Implemented | Current live integration uses a local server; public endpoint is not in default suite. | All |
| X-08 | Property tests | Partial | Financial and order-book properties are implemented; later analytical/portfolio/execution properties remain. | 1-7 |
| X-09 | Required fuzz targets | Missing | No fuzz workspace. | 2-3, 6-7 |
| X-10 | Required format/clippy/test/release commands | Partial | Exact Task 8 live/app locked fmt, strict Clippy, tests, and release build pass on Rust 1.97; the grouped full-workspace quarter gate remains. | 1 |
| X-11 | Dependency/vulnerability/license/credential/artifact checks | Implemented | Cargo-deny advisories/bans/licenses/sources, cargo-audit, exact duplicate inventory, generated-artifact, and scoped Gitleaks working-tree/history gates are implemented and pass. | 1, 7 |
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
| G-05 | Repository runnable after every stage | Implemented | Diagnostic CLI compatibility and production live/app packages build and test after Task 8; each remaining checkpoint must preserve this gate. | All |
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

The live foundations that would have contaminated later work are now closed through Task 8:
typed identity/finance, quality separation, asynchronous capture, exact current-source authority,
transactional qualification, deterministic ownership, bounded overflow invalidation, immutable
snapshots, and supervised lifecycle.

The current highest-risk gaps are the next real consumers of those contracts:

1. actor-owned online features without weakening current-state revalidation;
2. typed strategy, capability-consuming risk, and final dispatch with no bypass;
3. production Coinbase/Kraken adapters that bind before opening their feeds;
4. point-in-time research storage, revision preservation, and provider-compliant ingestion;
5. realistic paper execution and portfolio reconciliation; and
6. integrated fuzz, performance, dependency/license, and release evidence.

Tasks 9-14 close the live-quarter items at the existing seams. Research and later product stages
remain mandatory complete-release work; no missing capability is counted as implemented merely
because its domain contract exists.
