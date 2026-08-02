# Official Documentation Discovery Report

## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Candidate Sources](#candidate-sources)
- [Decision Notes by Candidate](#decision-notes-by-candidate)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps](#coverage-gaps)
- [Source List](#source-list)

## Research Scope

This discovery is anchored to **2026-07-15** and selects official or project-owned
documentation needed to make implementation decisions for Market Squawk's local Rust
platform. The decision context is a no-mandatory-cost, self-hosted system with two separate
pipelines: an integrity-gated live path and a point-in-time research path. The inventory is
intentionally consolidated into 15 source families so later deep-dive batches can review the
material without duplicating large documentation trees.

The scope covers:

- Rust 1.97.0, Edition 2024, Cargo resolver 3, and virtual-workspace compatibility.
- Tokio supervision and bounded queues; Serde, Reqwest, and Tokio-Tungstenite boundaries.
- Arrow `RecordBatch`, CSV/JSON ingestion, native Rust Parquet, DataFusion, and SQLite.
- MCP stdio framing, typed tool schemas, protocol negotiation, progress, and cancellation.
- Coinbase and Kraken order-book/trade delivery and book-integrity rules.
- SEC submissions/company facts, FRED/ALFRED vintages, BLS, and U.S. Treasury data.
- A current pure-Rust ONNX-compatible inference option and its support caveats.
- Authoritative ASC 820 and IFRS 13 fair-value material and access/licensing boundaries.

All listed pages were opened and reviewed as actual pages or official PDFs, not relied on as
search-result snippets. Access date for every selected source is **2026-07-15**. Statements
about architectural fit are labeled or phrased as implementation inferences; documented API
facts are linked to their official source.

## Search Queries Used

The following query families were used; individual results were then opened on the official
site and followed to related official pages.

1. `site:doc.rust-lang.org Edition Guide Rust 2024 resolver 3 rust-version 1.97.0 release notes`
2. `site:docs.rs tokio serde reqwest tokio-tungstenite latest official documentation`
3. `site:arrow.apache.org/rust OR site:datafusion.apache.org rust parquet csv json DataFusion official docs`
4. `site:modelcontextprotocol.io specification stdio JSON schema cancellation official`
5. `Coinbase Exchange WebSocket feed channels level2 sequence site:docs.cdp.coinbase.com/exchange`
6. `Kraken Spot WebSocket v2 book checksum guide site:docs.kraken.com/api/docs`
7. `site:sec.gov EDGAR APIs companyfacts submissions data official documentation`
8. `site:fred.stlouisfed.org/docs/api/fred OR site:alfred.stlouisfed.org API official vintages`
9. `site:bls.gov/developers API version 2 official registration limits`
10. `site:fiscaldata.treasury.gov api documentation official` and
    `site:home.treasury.gov Treasury Daily Interest Rate XML Feed`
11. `official Rust ONNX runtime crate documentation` and
    `site:docs.rs tract-onnx official rust inference docs`
12. `site:fasb.org Fair Value Measurement Topic 820 official standard` and
    `site:ifrs.org issued standards IFRS 13 Fair Value Measurement official`
13. `site:sqlite.org wal transactions limits foreign keys strict tables official documentation`

## Candidate Sources

| ID | Source | URL | Type | Credibility Signal | Freshness Signal | Priority | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| D01 | Rust release notes + Edition 2024/Cargo resolver family | [Rust Release Notes](https://doc.rust-lang.org/stable/releases.html) | Official language/toolchain docs | Rust Project documentation | 1.97.0 dated 2026-07-09; stable docs opened 2026-07-15 | P0 | Confirms the exact pinned compiler and the virtual-workspace resolver-3 requirement. |
| D02 | Tokio runtime, bounded mpsc, and shutdown family | [Tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown) | Official project docs/Rustdoc | Tokio project documentation and generated API docs | Current Rustdoc showed Tokio 1.52.3 | P0 | Defines the supervision, cancellation, backpressure, and clean-shutdown primitives used by all async services. |
| D03 | Serde + Reqwest + Tokio-Tungstenite transport family | [Reqwest Rustdoc](https://docs.rs/reqwest/latest/reqwest/) | Project-owned Rustdoc family | Maintainer-published crate docs linked to source repositories | Reqwest 0.13.4; Tokio-Tungstenite 0.30.0 dated 2026-07-11 | P0 | Covers typed provider boundaries, reusable HTTP clients, TLS, and async WebSocket streams. |
| D04 | Apache Arrow Rust + Parquet + file-ingestion family | [Arrow Rust API](https://arrow.apache.org/rust/arrow/index.html) | Official ASF project docs | Apache Arrow-owned API docs and Parquet implementation | Arrow/Parquet docs current; Parquet 59.1.0 | P0 | Directly supports `RecordBatch`, CSV, JSON, IPC, Parquet, decimal schemas, and local analytical exchange. |
| D05 | Apache DataFusion | [DataFusion documentation](https://datafusion.apache.org/) | Official ASF project docs | Apache Software Foundation governance and project docs | Current site opened 2026-07-15 | P0 | Establishes the embedded Rust SQL/DataFrame engine and local file-table integration. |
| D06 | SQLite documentation family | [SQLite documentation](https://www.sqlite.org/docs.html) | Official database docs | SQLite project primary documentation | Current pages include 2025-2026 updates | P0 | Supports the local catalog/control plane and documents WAL, typing, concurrency, integrity, and foreign-key prerequisites. |
| D07 | Model Context Protocol 2025-11-25 specification family | [MCP stdio transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) | Protocol specification | Model Context Protocol authoritative versioned spec | Page marks 2025-11-25 as latest as of access date | P0 | Defines local stdio framing, JSON-RPC lifecycle, typed tool schemas, progress, and cancellation. |
| D08 | Coinbase Exchange WebSocket channels | [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) | Official exchange API docs | Coinbase Developer Platform | Active changelog and current channel docs | P0 | Documents snapshots, deltas, trades, status/precision, heartbeat/sequence evidence, and resynchronization. |
| D09 | Kraken WebSocket v2 book + checksum | [Kraken book checksum v2](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | Official exchange API guide | Kraken Developers | Current v2 guide and redirected canonical URL | P0 | Provides the exact CRC32 algorithm and local-book update rules required for verified qualification. |
| D10 | SEC EDGAR submissions and XBRL APIs | [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Official U.S. government API docs | SEC `.gov` primary documentation | Last reviewed 2025-04-08; API freshness schedule documented | P0 | Covers filings, submissions, company facts, frames, bulk archives, freshness, and equitable-access rules. |
| D11 | FRED and ALFRED API/vintage family | [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html) | Official Federal Reserve Bank API docs | Federal Reserve Bank of St. Louis | Current API pages opened 2026-07-15 | P0 | Supports JSON/XML macro series and explicit real-time/vintage windows for point-in-time research. |
| D12 | BLS Public Data API | [BLS developer documentation](https://www.bls.gov/developers/home.htm) | Official U.S. government API docs | BLS `.gov` primary documentation | FAQ updated 2023-08-30; live service limits documented | P0 | Documents free unregistered access, optional registered access, request shapes, and exact quotas. |
| D13 | U.S. Treasury Fiscal Data + daily-rate feed family | [Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/) | Official U.S. Treasury API/file docs | Treasury `.gov` primary documentation | Current API registry plus 2026 daily-rate pages | P0 | Covers Treasury JSON datasets plus official XML/CSV interest-rate files, pagination, schemas, and change notices. |
| D14 | tract-onnx Rust inference with ONNX authority context | [tract-onnx Rustdoc](https://docs.rs/tract-onnx/latest/tract_onnx/) | Official project Rustdoc plus upstream standard docs | Maintainer-published crate docs; ONNX Runtime identifies Rust as community-maintained | tract-onnx 0.23.4; current as of 2026-07-15 | P1 | Provides a current pure-Rust ONNX-compatible path while making unsupported-operator and non-Microsoft-binding caveats explicit. |
| D15 | ASC 820 and IFRS 13 fair-value authority family | [FASB ASU 2011-04](https://storage.fasb.org/ASU2011-04.pdf) | Accounting standard-setter material | FASB and IFRS Foundation primary material | IFRS landing page marked `Standard 2026 Issued`; FASB current ASU index | P0 | Supplies the hierarchy definitions and preserves the crucial distinction between authoritative Codification content and accessible amendment/support material. |

## Decision Notes by Candidate

### D01 — Rust toolchain and resolver

- **Capability documented:** Rust's official release notes identify 1.97.0 as released on
  2026-07-09. Edition 2024 was stabilized in Rust 1.85.0. The
  [Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)
  says Edition 2024 implies resolver 3, but a virtual workspace must still declare
  `resolver = "3"` explicitly.
- **Integration pattern:** pin `channel = "1.97.0"`; use Edition 2024 and resolver 3 at the
  workspace root; inherit `rust-version = "1.97"` in every package; verify the committed
  lockfile under `--locked`.
- **Limits/prerequisites:** resolver 3 uses Rust-version-aware fallback but does not prove that
  every enabled crate feature compiles on the pinned target. The lockfile and all-feature CI
  remain the compatibility authority for the selected dependency graph.
- **Relevance:** directly confirms that the specification's compiler claim is correct as of the
  requested date.

### D02 — Tokio supervision and bounded concurrency

- **Capability documented:** Tokio documents `CancellationToken`, task tracking, `select!`, and
  clean shutdown. Its [mpsc Rustdoc](https://docs.rs/tokio/latest/tokio/sync/mpsc/) distinguishes
  bounded channels—which provide backpressure—from infinite-capacity unbounded channels.
- **Integration pattern:** one supervisor per source/service, cloned cancellation tokens,
  `TaskTracker` for lifecycle joins, and explicitly sized `mpsc::channel` boundaries for live
  shards and persistence fan-out.
- **Limits/prerequisites:** `sync` features must be enabled. An unbounded queue is inappropriate
  for execution-critical traffic. Overflow policy and quarantine/resync are domain behavior,
  not supplied by Tokio.
- **Relevance:** is the primary concurrency contract for bounded memory and controlled failure.

### D03 — Serialization, HTTP, and WebSockets

- **Capability documented:** [Serde](https://serde.rs/) provides derived and custom typed
  serialization; Reqwest provides asynchronous HTTP, connection pooling, TLS, redirect policy,
  and streamed bodies; [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)
  exposes WebSocket streams as `Stream`/`Sink` and supports Rustls or native TLS connectors.
- **Integration pattern:** reuse a configured Reqwest `Client`; set explicit timeouts, redirects,
  endpoint allowlists, and TLS features; deserialize into source DTOs before `TryFrom` validation;
  drive WebSocket read/write halves under a source supervisor and forward only to bounded queues.
- **Limits/prerequisites:** Reqwest system proxies are enabled by default in current docs, so a
  self-hosted security posture should explicitly decide whether to disable them. None of these
  libraries supplies venue sequence/checksum semantics. Crate feature selection and exact MSRV
  still require a locked build test.
- **Relevance:** establishes the transport boundary while leaving financial validation in domain
  adapters.

### D04 — Arrow, Parquet, CSV, and JSON

- **Capability documented:** Arrow groups typed columns in a `RecordBatch`; the Rust family
  includes CSV and JSON readers/writers and Serde conversion. The official
  [Parquet Rust API](https://arrow.apache.org/rust/parquet/index.html) supports synchronous and
  asynchronous Arrow read/write, file metadata, filters, and row groups.
- **Integration pattern:** validate records into an explicit Arrow schema, exchange
  `RecordBatch` values in memory, then persist partitioned Parquet via `ArrowWriter`; read with
  `ParquetRecordBatchReaderBuilder`. Use `Decimal128` for accounting values and store provenance
  columns with each dataset.
- **Limits/prerequisites:** Parquet partitioning, compaction, idempotency, and point-in-time rules
  are application responsibilities. Schema inference over untrusted CSV/JSON must be bounded and
  validated. Experimental features such as content-defined chunking should not be baseline.
- **Relevance:** is the canonical analytical interchange/persistence implementation family.

### D05 — DataFusion

- **Capability documented:** DataFusion is an embeddable Rust query engine over Arrow with SQL,
  DataFrame, streaming/vectorized execution, and built-in CSV, JSON, Parquet, and Avro support.
- **Integration pattern:** register controlled local datasets/table providers in a
  `SessionContext`; expose bounded application-service queries to CLI/MCP; place all query work
  outside the live event-to-action path.
- **Limits/prerequisites:** configure memory/runtime limits and cancel long queries. The engine's
  extension surface is broad, so unrestricted SQL or arbitrary object-store URLs must not be
  exposed through MCP. Lock a tested release because APIs evolve frequently.
- **Relevance:** directly implements the embedded analytical SQL requirement without an external
  database service.

### D06 — SQLite control plane

- **Capability documented:** SQLite documents transactions, WAL, concurrency, STRICT tables,
  foreign keys, integrity checks, and backup. Foreign-key enforcement is disabled by default and
  must be enabled for every connection; STRICT tables require SQLite 3.37 or later.
- **Integration pattern:** use a local SQLite file for configuration, cursors, registries,
  manifests, and run state; enable foreign keys, set an intentional busy timeout/journal policy,
  and use migrations plus integrity checks.
- **Limits/prerequisites:** SQLite remains a single-writer system, has dynamic typing unless
  STRICT is selected, and must not be queried per live event. WAL improves concurrency but does
  not turn it into a hot-path event store.
- **Relevance:** supports the local transactional catalog while reinforcing the hot-path exclusion.

### D07 — MCP stdio, schemas, and cancellation

- **Capability documented:** the versioned 2025-11-25 spec marks itself latest as of the access
  date. In stdio, a client launches a subprocess; newline-delimited UTF-8 JSON-RPC travels over
  stdin/stdout, and non-protocol logging belongs on stderr. The
  [tool specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
  defines input/output JSON Schemas and structured results. The
  [cancellation specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation)
  defines `notifications/cancelled` for in-progress non-task requests.
- **Integration pattern:** negotiate protocol version and capabilities, validate tool inputs and
  outputs, keep stdout protocol-clean, propagate cancellation to bounded application services,
  paginate lists, and return controlled artifact references for large outputs.
- **Limits/prerequisites:** ordinary cancellation is advisory and race-prone; receivers may be
  unable to cancel completed work. Task-augmented requests use `tasks/cancel` instead. Schema
  validation does not itself supply authorization, result limits, audit, or risk enforcement.
- **Relevance:** provides an exact local MCP v1 contract while remaining outside the live path.

### D08 — Coinbase Exchange feed

- **Capability documented:** Coinbase documents heartbeat sequence/trade identifiers, status and
  precision metadata, trades, `full` order events, a REST-snapshot-plus-queued-update procedure,
  and `level2` snapshots/absolute-size updates. A zero size deletes a level. The level2 channel
  is documented as guaranteeing delivery; the matches-only channel explicitly may drop messages.
- **Integration pattern:** subscribe to status plus a book channel; validate product and precision;
  build from snapshot before applying updates; track connection generation, heartbeat/trade IDs,
  and documented sequences; resnapshot on any integrity uncertainty. Use a distinct trade stream
  and persist raw frames asynchronously.
- **Limits/prerequisites:** channel authentication requirements and integrity fields differ across
  Exchange, Advanced Trade, full, level2, and batch variants. The selected level2 examples do not
  document a checksum. **Inference:** Coinbase data must remain `DirectUnverified` unless the
  chosen concrete channel and tests provide all required sequence/freshness/status evidence; a
  provider claim of delivery alone is insufficient for Market Squawk's `DirectVerified` gate.
- **Relevance:** is the required direct Coinbase implementation authority and exposes a critical
  qualification gap to resolve during adapter design.

### D09 — Kraken book integrity

- **Capability documented:** every WebSocket v2 book update carries a CRC32 checksum calculated
  over the top ten asks and bids. Kraken specifies decimal-preserving parsing, full-message update
  application before checking, delete-on-zero, depth truncation, sort order, string normalization,
  and an exact expected fixture.
- **Integration pattern:** parse price/quantity as strings or decimals, maintain the subscribed
  depth, apply all updates atomically, calculate the top-ten checksum, and quarantine/resubscribe
  immediately on mismatch. Turn Kraken's published fixture into a deterministic test vector.
- **Limits/prerequisites:** checksum verification is optional to the upstream protocol but should
  be mandatory for Market Squawk execution eligibility. It covers only the top ten levels even
  for deeper subscriptions, and levels falling out of scope may not receive zero-quantity updates.
- **Relevance:** supplies a strong, deterministic book-integrity basis for the required adapter.

### D10 — SEC filings and company facts

- **Capability documented:** `data.sec.gov` offers unauthenticated JSON submissions and XBRL
  company-concept/company-facts/frames APIs. APIs update as filings disseminate; nightly bulk ZIPs
  cover submissions and company facts. The API excludes custom-taxonomy and non-entity-wide facts
  from its cross-filing aggregates and does not support browser CORS.
- **Integration pattern:** declare a descriptive User-Agent, use conditional/local caching,
  prefer nightly bulk archives for large backfills, retain accession/filing metadata, and preserve
  raw filing/XBRL evidence for facts not represented in aggregates.
- **Limits/prerequisites:** the [SEC developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
  documents a maximum of 10 requests per second for equitable access. Bulk archives are more
  efficient than per-company crawling. Frames use calendar-alignment heuristics and are not a
  substitute for issuer-specific fiscal-period logic.
- **Relevance:** supports both the required filings adapter and point-in-time fundamental lineage.

### D11 — FRED/ALFRED vintages

- **Capability documented:** FRED's REST API returns XML or JSON and also queries ALFRED. Its
  [real-time-period documentation](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
  explicitly distinguishes what is known today from what was known in a past period; vintage-date
  endpoints identify revisions.
- **Integration pattern:** register series/release metadata, request explicit `realtime_start` and
  `realtime_end` windows, preserve vintages/revisions, cache raw responses, and avoid defaulting to
  today's view when building historical datasets.
- **Limits/prerequisites:** a free registered API key is required for documented calls. The error
  documentation returns HTTP 429 when rate-limited but does not publish a stable numeric quota;
  implement conservative backoff/caching.
- **Relevance:** is the strongest official basis for leakage-resistant macro point-in-time data.

### D12 — BLS

- **Capability documented:** v1 supports unregistered public access; v2 requires free registration
  and adds metadata/calculations and larger ranges. The current FAQ documents v1/v2 daily limits
  of 25/500 queries, series-per-query limits of 25/50, year limits of 10/20, and 50 requests per
  ten seconds for both.
- **Integration pattern:** make v1 a zero-registration baseline, optionally accept a user-owned v2
  key, batch series within documented limits, cache results, and import BLS downloadable text
  files for bulk/history where appropriate.
- **Limits/prerequisites:** v2 registration includes a CAPTCHA and annual renewal. v1 lacks catalog
  metadata and has small daily limits.
- **Relevance:** proves a compliant no-paid baseline while making coverage and freshness explicit.

### D13 — U.S. Treasury APIs and files

- **Capability documented:** Fiscal Data exposes GET-only REST endpoints returning JSON data and
  metadata with field selection, filters, sorting, format, and pagination. The official
  [daily-interest-rate feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
  provides XML yield, bill, long-term, and real-rate series; `all` requests paginate from page zero
  in 300-row pages. Official change notices also link static historical CSV archives.
- **Integration pattern:** select a dataset-specific endpoint, page deterministically, parse fields
  from strings using the endpoint data dictionary, normalize explicit decimal/date types, and
  retain the source file/request plus ingestion timestamp. Use the rate feed for yield-curve
  features and Fiscal Data for broader Treasury datasets.
- **Limits/prerequisites:** Fiscal Data may return HTTP 429 and exposes endpoint-specific schemas;
  its response values, including null representations, require deliberate parsing. Treasury has
  changed feed URLs and fields before, so schema-change tests and source health are mandatory.
- **Relevance:** covers both API and file-based Treasury ingestion without a paid dependency.

### D14 — ONNX-compatible local Rust inference

- **Capability documented:** current `tract-onnx` exposes ONNX parsing/model APIs in pure Rust.
  Microsoft's [ONNX Runtime documentation](https://onnxruntime.ai/docs/get-started/community-projects.html)
  lists Rust under external community projects rather than as a first-party supported binding.
- **Integration pattern:** validate bundle hash, ONNX opset, tensor names/shapes/dtypes, and feature
  schema before loading; warm a typed runnable model off-path; expose inference only through the
  local `InferenceBackend` abstraction; treat any load/run/shape error as no action.
- **Limits/prerequisites:** tract supports a subset of ONNX operators and its Rustdoc coverage was
  about 49% at discovery time. Each model must pass compatibility and numerical-equivalence tests.
  **Inference:** native Rust kernels should remain the default for simple live models, with ONNX as
  an adapter-backed option rather than an assumed universal model runtime.
- **Relevance:** is a current no-cloud/no-Python live-runtime candidate with clearly bounded claims.

### D15 — ASC 820 and IFRS 13 authority

- **Capability documented:** FASB ASU 2011-04 contains the hierarchy language: Level 1 inputs are
  unadjusted quoted prices in active markets for identical assets or liabilities accessible at the
  measurement date; the hierarchy prioritizes inputs, and significant lower-level inputs lower the
  whole measurement. The [IFRS 13 page](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/)
  defines fair value as a measurement-date exit price and points to 2026-issued standard/support
  material. IFRS support also states that third-party prices qualify as Level 1 only when based
  solely on qualifying unadjusted quoted inputs.
- **Integration pattern:** store the measurement, input evidence, principal/accessible market,
  active-market assessment, identical-instrument test, adjustment status, method, hierarchy,
  ruleset version, override, and approval separately from market-depth and execution quality.
- **Limits/prerequisites:** FASB explicitly says the Codification is authoritative and an ASU is
  not; the ASU communicates amendments. Current complete ASC access and IFRS text may involve
  licensing/sign-in. The application may encode documented rules and cite evidence, but must not
  redistribute copyrighted standards or claim automated accounting conclusions replace qualified
  review.
- **Relevance:** directly prevents Level 2/3 valuations from being mislabeled as Level 1 or as
  execution-quality evidence.

## Excluded Sources

| Source | URL | Reason Excluded |
| --- | --- | --- |
| Coinbase Advanced Trade WebSocket docs as primary | https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview | Relevant secondary family, but Exchange `full`/`level2` documentation is more explicit about snapshot replay, sequence evidence, absolute level sizes, and dropped matches. Preserve for a later comparison, not a duplicate candidate. |
| Kraken legacy WebSocket v1/legacy pages | https://docs-legacy.kraken.com/api/docs/websocket-v2/book/ | Superseded by the current canonical v2 guide; legacy URLs create version ambiguity. |
| EDGAR filer submission toolkit | https://api.edgarfiling.sec.gov/ | Designed for authenticated filers to submit/manage filings, not public research extraction from `data.sec.gov`. |
| Old `onnxruntime` Rust crate 0.0.14 | https://docs.rs/onnxruntime/latest/onnxruntime/ | Stale wrapper compared with current tract and `ort` ecosystems; not a credible baseline for Rust 1.97 architecture. |
| ONNX Runtime Rust link as implementation authority | https://onnxruntime.ai/docs/get-started/community-projects.html | Official Microsoft page is valuable to prove Rust is external/community-maintained, but it is not a first-party Rust API contract. Retained as D14 context. |
| IFRS educational examples as requirements | https://www.ifrs.org/supporting-implementation/supporting-materials-by-ifrs-standards/ifrs-13/ | IFRS itself says the educational material does not constitute official IASB requirements. Retained only as support/context under D15. |
| Blogs, SEO explainers, unofficial API wrappers, and documentation mirrors | N/A | Lower authority than available project/provider/government primary documentation; often omit limits, versioning, or licensing constraints. |
## Coverage Gaps

1. **Coinbase executable qualification:** official Exchange docs provide useful integrity rules,
   but channel-specific authentication, sequence visibility, and drop detection must be reconciled
   for the exact endpoint selected. There is no documented Coinbase level2 checksum in the chosen
   family. Default to `DirectUnverified` until end-to-end evidence passes all qualification checks.
2. **Kraken continuity beyond checksum:** the checksum is strong top-ten state evidence, but the
   downstream deep dive should confirm exact connection-generation, timestamp, reconnect, and
   subscription-status semantics for WebSocket v2.
3. **Current crate compatibility matrix:** documentation pages show current versions, not a proof
   that one single Cargo graph of Arrow/Parquet/DataFusion/Reqwest/Tokio/tract compiles with all
   features on Rust 1.97. Lockfile resolution plus an isolated build spike is still required.
4. **ONNX operator coverage:** no official source promises universal tract compatibility. Actual
   model bundles need per-opset load, shape, precision, performance, and equivalence fixtures.
5. **ASC/IFRS redistribution and completeness:** accessible standard-setter pages support the rule
   model, but complete current authoritative text may require licensed access. Legal/license review
   is needed before bundling extracts or extended quotations.
6. **Provider availability and changing quotas:** FRED does not publish a stable numeric limit in
   the reviewed API pages; Treasury schemas and endpoints can change; all adapters need cached
   manifests, conditional requests, health, and explicit coverage metadata.
7. **Source breadth intentionally deferred:** BEA, corporate actions, portfolio broker-export
   dialects, options/futures identity, and live equity consolidation are outside this focused
   official-doc discovery assignment and need their own discovery batches.

## Source List

All sources accessed 2026-07-15.

1. [Rust Release Notes](https://doc.rust-lang.org/stable/releases.html)
2. [Rust 2024 Cargo resolver guidance](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)
3. [Tokio graceful shutdown](https://tokio.rs/tokio/topics/shutdown)
4. [Tokio bounded mpsc](https://docs.rs/tokio/latest/tokio/sync/mpsc/)
5. [Serde](https://serde.rs/)
6. [Reqwest](https://docs.rs/reqwest/latest/reqwest/)
7. [Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)
8. [Apache Arrow Rust](https://arrow.apache.org/rust/arrow/index.html)
9. [Apache Parquet Rust](https://arrow.apache.org/rust/parquet/index.html)
10. [Apache DataFusion](https://datafusion.apache.org/)
11. [SQLite documentation](https://www.sqlite.org/docs.html)
12. [SQLite foreign keys](https://www.sqlite.org/foreignkeys.html)
13. [SQLite STRICT tables](https://www.sqlite.org/stricttables.html)
14. [MCP stdio transports, version 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
15. [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
16. [MCP cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation)
17. [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)
18. [Coinbase WebSocket best practices](https://docs.cdp.coinbase.com/exchange/websocket-feed/best-practices)
19. [Kraken WebSocket v2 book checksum](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)
20. [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
21. [SEC Webmaster/Developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
22. [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)
23. [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
24. [FRED series vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html)
25. [BLS Public Data API](https://www.bls.gov/developers/home.htm)
26. [BLS API FAQ and limits](https://www.bls.gov/developers/api_faqs.htm)
27. [Treasury Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/)
28. [Treasury Daily Interest Rate XML Feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
29. [Treasury XML/CSV developer change notice](https://home.treasury.gov/developer-notice-xml-changes)
30. [tract-onnx Rustdoc](https://docs.rs/tract-onnx/latest/tract_onnx/)
31. [ONNX Runtime community projects](https://onnxruntime.ai/docs/get-started/community-projects.html)
32. [FASB ASU 2011-04, Topic 820](https://storage.fasb.org/ASU2011-04.pdf)
33. [FASB Accounting Standards Updates index](https://fasb.org/standards/accounting-standard-updates)
34. [IFRS 13 Fair Value Measurement](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/)
35. [IFRS 13 supporting material](https://www.ifrs.org/supporting-implementation/supporting-materials-by-ifrs-standards/ifrs-13/)
