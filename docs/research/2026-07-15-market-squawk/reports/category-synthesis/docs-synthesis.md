# Docs Synthesis

## Table of Contents

1. [Category Scope](#category-scope)
2. [Sources Covered](#sources-covered)
3. [High-Confidence Findings](#high-confidence-findings)
4. [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
5. [Conflicts and Disagreements](#conflicts-and-disagreements)
6. [Trends and Patterns](#trends-and-patterns)
7. [Implications for the Research Topic](#implications-for-the-research-topic)
8. [Gaps](#gaps)
9. [Source Matrix](#source-matrix)

## Category Scope

This synthesis consolidates the five official-documentation batch reports and their
**14 distinct source families**, `docs-036` through `docs-049`, for Market Squawk's
local Rust platform as of **2026-07-15**. It covers the pinned toolchain, asynchronous
and transport boundaries, analytical storage, local MCP, direct exchange feeds,
public research sources, and local ONNX inference. It does not add sources or claim
that documentation alone proves an implementation, performance target, or production
readiness.

Statements explicitly described by the cited primary documentation are
**Confirmed**. Market Squawk design conclusions are **Inference**. Duplicate findings
from the batches are merged; materially different provider and protocol contracts
remain separate.

## Sources Covered

| Area | Source families | Coverage |
| --- | --- | --- |
| Toolchain and runtime | `docs-036`–`docs-038` | Rust 1.97.0, Edition 2024/resolver 3, Tokio lifecycle/bounded queues, Serde, Reqwest, and Tokio-Tungstenite |
| Analytical data plane | `docs-039`–`docs-041` | Arrow/Parquet, embedded DataFusion, and SQLite control-plane behavior |
| MCP and direct feeds | `docs-042`–`docs-044` | MCP 2025-11-25 stdio/tools lifecycle, Coinbase Exchange channels, and Kraken v2 book checksum |
| Public research sources | `docs-045`–`docs-048` | SEC EDGAR, FRED/ALFRED, BLS, and U.S. Treasury Fiscal Data/daily rates |
| Local model inference | `docs-049` | `tract-onnx` 0.23.4 plus ONNX Runtime's official support-status context |

The source matrix below retains the primary URLs, authority/version signals, limits,
failure semantics, and non-findings for every family.

## High-Confidence Findings

### 1. The Rust baseline is coherent, but only a locked build establishes compatibility

**Confirmed.** Rust 1.97.0 was released on 2026-07-09. Edition 2024 implies Cargo
resolver 3, but a virtual workspace must explicitly declare `resolver = "3"` because
it cannot infer a package edition. Resolver selection is workspace-global and resolver
3 uses Rust-version-aware dependency fallback. Rust 1.97 also changed default symbol
mangling to v0, which can affect older debugging, profiling, and backtrace tooling
([Rust releases](https://doc.rust-lang.org/stable/releases.html),
[Edition Guide](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)).

**Inference.** Pinning Rust 1.97.0, Edition 2024, resolver 3, inherited `rust-version`,
explicit dependency features, and `Cargo.lock` is the correct baseline. Resolver
selection is not a compatibility certificate: the locked all-feature formatting,
Clippy, tests, and release build on 1.97.0 remain the acceptance evidence. Profiling
and symbolization checks should cover the v0 mangling change.

### 2. Bounded lifecycle and network policy must be application-owned

**Confirmed.** Tokio documents cooperative cancellation, tracked task completion,
bounded `mpsc` backpressure, and close/drain semantics. An unbounded channel has
infinite capacity; dropping a receiver discards unread messages
([Tokio shutdown](https://tokio.rs/tokio/topics/shutdown),
[`mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/)). Serde returns parse/type
failures but ignores unknown fields by default unless configured otherwise
([Serde overview](https://serde.rs/),
[container attributes](https://serde.rs/container-attrs.html)). Reqwest 0.13.4 has no
total, read, or connect deadline by default, follows up to ten redirects, and enables
system proxies by default; its certificate and hostname checks are security-critical
defaults ([Reqwest](https://docs.rs/reqwest/latest/reqwest/),
[`ClientBuilder`](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html)).
Tokio-Tungstenite 0.30.0 supplies asynchronous WebSocket framing and selectable TLS
connectors, not venue integrity
([Tokio-Tungstenite](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)).

**Inference.** Every queue needs a finite capacity and an explicit send/overflow
outcome. Source supervisors need cancellation, tracked completion, deadlines, and
generation-aware reconnect state. Provider DTOs should cross a fallible domain
conversion after deserialization. Reusable HTTP/WebSocket clients need explicit
timeouts, redirect and proxy policy, TLS features, endpoint allowlists, retry budgets,
and body/frame limits. A valid JSON document or WebSocket frame is not
`DirectVerified`; instrument mapping, sequence, snapshot, checksum where supported,
timestamps, freshness, status, and precision must still pass.

### 3. The embedded data stack is exact and local, but not automatically bounded or semantic

**Confirmed.** Arrow batches carry typed schemas and arrays. `Decimal128` is exact
fixed-point storage, while Arrow timestamps without a timezone are wall-clock values
in an unknown timezone rather than UTC
([Arrow](https://arrow.apache.org/rust/arrow/index.html),
[`DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html)). Parquet
exposes row-group, page, compression, dictionary, statistics, bloom-filter, and sorting
controls; those controls do not prescribe a universal file size or partition strategy
([Parquet](https://arrow.apache.org/rust/parquet/index.html),
[writer properties](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html)).
DataFusion is embeddable, but its runtime memory and temporary directory are unset by
default, output parallelism can amplify small files, and unbounded joins can exhaust
memory ([DataFusion configuration](https://datafusion.apache.org/user-guide/configs.html)).

**Confirmed.** SQLite permits one writer at a time. WAL supports concurrent snapshot
readers plus a writer but is same-host, requires checkpointing, may return
`SQLITE_BUSY`, and can grow behind long readers. Foreign keys default off per
connection; STRICT tables have a deliberately small type set; integrity and
foreign-key checks are separate. Durable WAL commits require the appropriate
synchronous policy ([SQLite WAL](https://www.sqlite.org/wal.html),
[isolation](https://www.sqlite.org/isolation.html),
[foreign keys](https://www.sqlite.org/foreignkeys.html),
[STRICT](https://www.sqlite.org/stricttables.html),
[PRAGMAs](https://www.sqlite.org/pragma.html)). The WAL documentation also records a
rare concurrent-reset defect fixed in SQLite 3.51.3 and backported to 3.44.6 and
3.50.7 ([SQLite WAL](https://www.sqlite.org/wal.html)).

**Inference.** Arrow types need separately versioned units, currency, scale, rounding,
provenance, and time semantics. Immutable Parquet files should be validated and made
visible by atomically published manifests; compaction stays outside the live path.
DataFusion contexts require allowlisted catalogs, memory/temp/parallelism/result caps,
cancellation, and timeouts. SQLite should hold configuration, cursors, manifests,
registries, and run state with short transactions, verified WAL/runtime version,
per-connection foreign-key initialization, and both integrity checks. None of SQLite,
DataFusion, Parquet, compaction, or arbitrary Arrow construction belongs in the
socket-to-decision path.

### 4. MCP is a bounded local control plane, never a risk or live-path escape hatch

**Confirmed.** MCP 2025-11-25 stdio uses newline-delimited UTF-8 JSON-RPC; server
`stdout` may contain only protocol messages and logs belong on `stderr`. Initialization
and version/capability negotiation precede operation
([transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
[lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle)).
Tool inputs use JSON Schema and declared structured outputs must conform. The
specification requires input validation, access controls, rate limiting, output
sanitization, timeouts, and audit-oriented logging. Progress and cancellation are
active-request-scoped and race-prone; cancellation is cooperative
([tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools),
[progress](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress),
[cancellation](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation)).

**Inference.** Pin protocol version 2025-11-25 and implement a lifecycle state machine.
Bound frame, string, array, time-range, instrument, row, byte, concurrency, duration,
and artifact sizes. Reserve `stdout`, redact audit data, contain artifacts beneath an
owned directory, propagate cancellation, retain hard deadlines, and discard late
results. MCP calls shared application services and risk enforcement; it never performs
live-path work, unrestricted SQL/filesystem operations, credential access, unchecked
orders, or risk bypass.

### 5. Coinbase and Kraken require different, channel-specific qualification

**Confirmed.** Coinbase `level2` begins with a snapshot and sends absolute level sizes;
zero deletes a level, and Coinbase describes delivery as guaranteed. `level2_batch`
groups updates every 50 ms. The `full` channel documents a race-safe procedure: queue
sequenced WebSocket messages, fetch a REST snapshot, discard messages at or below the
snapshot sequence, replay the remainder, then continue live. Heartbeats expose
sequences and last-trade IDs; the separate matches channel can drop messages. Status
messages include increments and trading restrictions, while auction quotes are
indicative rather than firm
([Coinbase channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)).

**Inference.** Coinbase is direct **single-venue**, not consolidated, coverage.
Qualification is per channel. The sequenced `full` path can become a
`DirectVerified` candidate only after complete snapshot/replay, sequence, connection
generation, timestamp, freshness, trading status, precision, and book checks. The
assigned `level2` page shows neither a checksum nor a sequence in its update examples;
provider language about guaranteed delivery is not an observable continuity proof.
`level2` alone, batched channels, matches-only, and auction-indicative data therefore
remain non-executable by default. Heartbeats prove connection activity, not market
price freshness.

**Confirmed.** Kraken v2 `book` validation applies all changes in one message, deletes
zero quantities, truncates to subscribed depth, and then computes CRC32 over the top
ten ask and bid levels. Decimal/string precision and exact normalization/order are
required; checksum coverage remains top-ten even at deeper subscriptions
([Kraken checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)).

**Inference.** Kraken updates are atomic validation units: publish no intermediate
state, never parse checksum inputs through floating point, and validate every
execution-eligible update. A mismatch quarantines the connection generation and book
until a fresh snapshot and passing checksum. A match proves synchronization under
Kraken's top-ten algorithm, not freshness, instrument mapping, venue status, or deeper
book correctness. Coinbase's sequence recovery and Kraken's checksum procedure must
not be abstracted into a lowest-common-denominator integrity claim.

### 6. Research ingestion requires provider-specific quotas, pagination, and revision identity

**Confirmed.** SEC EDGAR permits scripted access without authentication, currently
limits automated traffic to 10 requests/second, and requests a truthful organization
and administrative-contact user agent. Submissions/company facts support incremental
work; nightly bulk ZIPs are the efficient large-backfill route. Submissions history
may span additional files, Frames selects a last-filed fact best aligned to a requested
calendar period, and SEC availability lag is typical rather than guaranteed
([SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
[SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)).

**Confirmed.** FRED requires a per-user application key. Observations supports up to
100,000 rows per call, offset pagination, explicit observation/real-time bounds, and
multiple vintage output modes. ALFRED real-time periods describe when a value was
known, and vintage dates identify release/revision dates with changed data; the default
real-time interval is today
([FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html),
[real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html),
[ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html),
[API keys](https://fred.stlouisfed.org/docs/api/api_key.html)).

**Confirmed.** BLS v1 is unregistered and permits 25 daily queries, 25 series, and 10
years per query; registered v2 permits 500 daily queries, 50 series, and 20 years.
Both document 50 requests per 10 seconds and HTTP 429 for excess traffic. Responses
may include per-series messages and preliminary footnotes
([BLS getting started](https://www.bls.gov/developers/home.htm),
[v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm),
[FAQ](https://www.bls.gov/developers/api_faqs.htm)).

**Confirmed.** Treasury Fiscal Data REST is one-based, defaults to 100 rows, exposes
counts/types/page links, and may aggregate when field projection removes dimensions.
The separate daily-rate XML `all` feed is zero-based, defaults to 300 rows, and ends on
an empty page. Treasury has changed feed URLs, null/absent behavior, and maturity
fields. Constant Maturity Treasury rates are interpolated from indicative bid-side
inputs rather than transactions, and the curve methodology changed to a
monotone-convex spline on 2021-12-06
([Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/),
[XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
[developer changes](https://home.treasury.gov/developer-notice-xml-changes),
[daily rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve)).

**Inference.** Each adapter needs its own honest identity/secret policy, local request
budget, bounded concurrency, caching, backoff, paginator, schema validation, and
source-health state. Use SEC bulk plus API reconciliation; pin every FRED real-time and
transformation parameter; chunk BLS deterministically and inspect each requested
series; keep separate Treasury REST/XML paginators and guard against projection
aggregation. Immutable raw payload hashes, request parameters, time first observed locally,
times, parser/schema revisions, and manifests make overlapping retrieval idempotent
without erasing amendments or revisions.

**Inference.** Point-in-time records must distinguish effective/observation time,
source/filing time, publication or vintage evidence, local `received_at`/
`available_at`/`ingested_at`, revision identity, and supersession. FRED/ALFRED supplies
date-granularity vintage evidence; SEC timing is not guaranteed; BLS supplies no
complete pre-capture vintage history in the reviewed API; Treasury supplies no exact
publication timestamp or immutability guarantee. Treasury CMT values are official
modeled/indicative research inputs—never `DirectVerified`, actual trades, or ASC 820
Level 1 evidence.

### 7. Local ONNX inference is bundle-qualified and fail-closed

**Confirmed.** `tract-onnx` 0.23.4, published 2026-07-08 under MIT or Apache-2.0,
parses protobuf models into an `InferenceModel`, whose types/shapes may be partial; a
`TypedModel` has determined types and shapes, and tract core supplies plans and tensor
execution. Rustdoc coverage is approximately 48.68%
([`tract-onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/),
[`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html),
[`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html),
[tract core](https://docs.rs/tract-core/latest/tract_core/)). ONNX Runtime documents
Rust under external community projects, not its first-party language APIs
([ONNX Runtime](https://onnxruntime.ai/docs/get-started/community-projects.html)).

**Inference.** Treat tract as a candidate local backend, not an official ONNX Runtime
Rust binding or universal ONNX compatibility guarantee. Each controlled local bundle
must pin the artifact SHA-256, model format/opset, tract version/features, operator
inventory, input/output names/dtypes/shapes, normalization, tolerances, and fallback
behavior. Enforce file/type/size/hash/schema/operator checks before parsing and reject
external references, remote loading, code/plugins, non-finite or malformed outputs.
Import, typing, optimization, representative warm-up, golden-vector comparison, and
latency/memory measurement occur outside the live path before atomic activation.
Choose serialized or per-worker execution only after concurrency and determinism
tests; the documentation specifies no plan-level threading or resource guarantee.
Any load, warm-up, or inference error produces no automated action. Another backend is
a fallback only after independent validation against the same bundle.

## Medium- and Low-Confidence Findings

### Medium confidence

- **Inference.** Manifest-mediated immutable datasets, serialized SQLite writes, and
  separate interactive/background DataFusion contexts are strong fits for the
  documented behavior, but the official sources do not prescribe Market Squawk's
  catalog schema, transaction topology, or compaction protocol.
- **Inference.** Coinbase `full` and checksum-validated Kraken books are credible
  `DirectVerified` candidates after all additional Market Squawk checks. The source
  pages do not themselves certify the adapter implementation, local timestamps,
  precision mapping, book invariants, or source coverage metadata.
- **Inference.** Capturing a provider response's first successful local fetch is a
  defensible lower bound for local availability when the provider exposes no exact
  publication instant, but it cannot reconstruct knowledge before capture.
- **Inference.** Strict DTOs are useful for execution-critical envelopes, yet global
  rejection of unknown fields can turn harmless additive schema changes into outages.
  The strict/forward-compatible boundary remains message-specific.
- **Inference.** A privacy-sensitive default can disable ambient system proxies, but
  the final choice must account for explicit user networking configuration and target
  platform requirements.

### Low confidence or unsupported by the reviewed documentation

- No official source proves that the eventual all-feature dependency graph supports
  Rust 1.97.0; that is a locked-build result, not a documentation fact.
- No source supplies a universal Parquet target size, partition key, compaction
  interval, DataFusion limit, HTTP body limit, WebSocket frame limit, queue capacity,
  retry count, or TLS-backend choice.
- Neither Coinbase's page nor Kraken's checksum guide proves Market Squawk's
  end-to-end event-to-decision latency or throughput.
- The assigned tract pages provide no universal operator/opset support, numerical
  equivalence, concurrency model, warm-up behavior, latency, or memory bound.
- No performance figure in the product specification is supported until measured on
  documented hardware with pinned fixtures and versions.

## Conflicts and Disagreements

There are no direct contradictions among the official sources; there are important
contract differences that a generic abstraction must not erase:

1. **Live integrity:** Coinbase documents sequence-based `full` initialization and no
   assigned `level2` checksum; Kraken documents precision-sensitive CRC32 over an
   atomically updated top-ten book. Neither is a provider-wide quality label
   ([Coinbase](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels),
   [Kraken](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)).
2. **Treasury pagination:** Fiscal Data REST is one-based with metadata/links, while
   XML `all` is zero-based and terminates on an empty page. Reusing one cursor contract
   would skip or duplicate data
   ([REST](https://fiscaldata.treasury.gov/api-documentation/),
   [XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)).
3. **Public access:** SEC needs an honest user agent but no key and publishes a
   request ceiling; FRED requires a key but the reviewed pages publish no numeric
   ceiling; BLS exposes separate registered/unregistered budgets. A shared HTTP client
   does not imply shared quota policy
   ([SEC](https://www.sec.gov/about/webmaster-frequently-asked-questions),
   [FRED](https://fred.stlouisfed.org/docs/api/api_key.html),
   [BLS](https://www.bls.gov/developers/api_faqs.htm)).
4. **Missing values and schemas:** Treasury's newer XML can omit unavailable values,
   while Serde can ignore unknown fields by default. Forward compatibility therefore
   must still distinguish absent, null, invalid, zero, and newly added dimensions
   ([Treasury changes](https://home.treasury.gov/developer-notice-xml-changes),
   [Serde attributes](https://serde.rs/container-attrs.html)).
5. **Exact storage versus domain meaning:** Arrow Decimal128 is exact, but it does not
   encode currency, tick/lot definition, rounding authorization, or accounting policy
   ([Arrow `DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html)).
6. **Embedded versus bounded:** DataFusion is embedded but not bounded by default;
   SQLite is local but still serializes writers and requires WAL/checkpoint policy
   ([DataFusion configuration](https://datafusion.apache.org/user-guide/configs.html),
   [SQLite WAL](https://www.sqlite.org/wal.html)).

## Trends and Patterns

1. **Documented defaults are not product policy.** Queue capacity, proxy behavior,
   redirects, timeouts, schema strictness, query memory, SQLite constraints, and model
   concurrency all require explicit configuration and validation.
2. **Successful decoding is only the first boundary.** JSON, WebSocket, Arrow, XML,
   XBRL, and ONNX parse success is weaker than financial semantics, integrity,
   reproducibility, and authorization.
3. **Version and provenance are part of meaning.** Rust/Cargo versions, Cargo features,
   MCP version, provider schema/methodology eras, data vintages, parser revisions,
   model opsets, and artifact hashes belong in manifests and audit evidence.
4. **Self-hosted does not mean resource-free.** Bounded memory, temporary disk,
   concurrency, result size, request budgets, checkpoints, cancellation, and
   compaction remain necessary without cloud infrastructure.
5. **Live and research planes have different qualification standards.** Direct feed
   data may become execution-eligible only after continuous integrity checks. SEC,
   macro, and Treasury inputs preserve point-in-time knowledge and revisions but never
   enter the immediate event-to-action path.
6. **Failure should revoke capability, not disappear.** Queue overflow, sequence gaps,
   checksum mismatch, stale data, incomplete pagination, partial provider errors,
   schema drift, MCP cancellation, and inference errors need typed degraded,
   quarantined, or no-action outcomes.
7. **Deterministic fixtures and opt-in network tests are complementary.** Default tests
   should cover parsing, boundaries, state transitions, and known provider fixtures;
   external tests should run separately under real credentials and documented limits.

## Implications for the Research Topic

### Architecture and boundaries

**Inference.** The official evidence supports the proposed three-part split: a
deterministic bounded live plane; an Arrow/Parquet/DataFusion research plane with a
SQLite control catalog; and a local CLI/MCP control plane. Share financial/domain
types and pure kernels, but do not let SQLite, DataFusion, Parquet, Python, MCP, model
loading, or unrelated filesystem/network operations enter the hot path.

### Adapter contracts and quality

**Inference.** Keep live and extraction source contracts distinct. Every adapter
publishes source identity, authority, coverage, schema/parser version, request and
payload hashes, timestamps, health, and current quality. Promotion to
`DirectVerified` is an explicit state transition owned by source-specific validators,
never inferred from provider reputation, HTTP success, parse success, or an ASC 820
classification. Overflow or integrity loss revokes eligibility until a complete
resynchronization passes.

### Data and point-in-time research

**Inference.** Canonical datasets should use explicit Arrow schemas and exact
financial representations, but retain raw records and provider-specific identifiers.
Manifests should bind schema, provider request parameters, transformations, data
quality, availability/revision semantics, source and payload hashes, row counts,
partitions, and publication state. Point-in-time joins must filter on defensible
availability plus supersession, not merely observation or filing date. Reconciliation
must preserve conflicting amendments/revisions instead of overwriting them.

### Local MCP and inference

**Inference.** CLI and MCP should call the same typed application services. MCP tools
need negotiated version/capabilities, strict bounded schemas, cancellation, audit, and
artifact references. Model activation should use immutable hash-verified bundles and a
validated native backend, with parsing/warm-up off-path and no action on error. Neither
MCP nor a model is authorized to bypass shared risk evaluation.

### Verification priorities

**Inference.** Highest-value deterministic suites are: financial/decimal and time
conversion properties; bounded-queue overflow and lifecycle shutdown; Coinbase
snapshot/sequence/reconnect cases; Kraken official checksum, precision, and atomicity;
manifest/PIT/revision preservation; provider pagination and partial errors; DataFusion
and MCP resource/result bounds; SQLite connection/checkpoint/integrity behavior; and
model hash/operator/schema/golden/concurrency/hot-swap failure tests. Benchmarks must
record pinned toolchain, dependency features, hardware, fixtures, event counts,
percentiles, throughput, and memory before any performance claim.

## Gaps

- **Implementation evidence:** official documentation does not establish that any
  adapter, book, queue, dataset, risk gate, MCP tool, or inference backend exists or
  passes tests in the repository.
- **Version closure:** the final Arrow, Parquet, DataFusion, SQLite binding/runtime,
  MCP library, and transport feature versions are not selected by this synthesis;
  `latest` documentation pages require lockfile-based release evidence.
- **Coverage closure:** Coinbase evidence is single-venue and channel-specific; no
  consolidated equity coverage or generalized cross-venue qualification follows.
- **Point-in-time closure:** SEC timing is variable, FRED vintages are date-granular,
  BLS lacks complete pre-capture vintages, and Treasury lacks exact publication and
  immutability guarantees in the reviewed pages.
- **Source gaps:** this category contains no official deep dive for the required CSV,
  JSON/NDJSON, Parquet extraction adapter behavior; portfolio imports; paper execution;
  or ASC 820/IFRS 13 classification rules. Parquet storage documentation alone is not
  a working extraction adapter.
- **Security closure:** documentation does not determine a complete credential store,
  encrypted fallback, endpoint allowlist format, dependency/license audit policy,
  parser resource limits, or hostile-file threat model.
- **Operational closure:** universal retry, queue, timeout, file-size, partition,
  compaction, checkpoint, and result limits do not exist; configure and benchmark each
  workload/provider.
- **Inference closure:** tract support, numerical tolerance, concurrency, determinism,
  warm-up, memory, and latency are bundle- and version-specific empirical questions.
- **Performance closure:** no reviewed source validates 100,000 events/second,
  sub-millisecond warmed p99 event-to-decision latency, or bounded sustained-burst
  memory for Market Squawk.

## Source Matrix

All sources were accessed on **2026-07-15**.

| ID | Authority / version signal | High-confidence contribution | Limits and failure caveats | Market Squawk implication |
| --- | --- | --- | --- | --- |
| `docs-036` | Rust Project; Rust 1.97.0 release dated 2026-07-09; Edition 2024 guide ([releases](https://doc.rust-lang.org/stable/releases.html), [resolver 3](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)) | Stable toolchain date, resolver 3 semantics, virtual-workspace rule, compatibility notes | Resolver fallback does not prove the locked graph; v0 mangling affects tooling | Pin toolchain, resolver, features, lockfile; verify all targets on 1.97.0 |
| `docs-037` | Tokio Project; Rustdoc showed Tokio 1.52.3 ([shutdown](https://tokio.rs/tokio/topics/shutdown), [`mpsc`](https://docs.rs/tokio/latest/tokio/sync/mpsc/)) | Cooperative cancellation, task tracking, bounded backpressure, close/drain behavior | Cancellation is cooperative; unbounded capacity is infinite; receiver drop loses unread data | Track lifecycle and define finite queues plus domain overflow/quarantine policy |
| `docs-038` | Serde project, Reqwest 0.13.4, Tokio-Tungstenite 0.30.0 ([Serde](https://serde.rs/), [Reqwest](https://docs.rs/reqwest/latest/reqwest/), [WebSocket](https://docs.rs/tokio-tungstenite/latest/tokio_tungstenite/)) | Typed serialization failures, reusable HTTP, TLS/proxy/timeout controls, async WebSocket transport | Unknown fields may be ignored; Reqwest deadlines are unset and ambient proxies enabled; transport success is not feed integrity | DTO-to-domain validation; explicit networking policy; bounded frames/bodies; venue checks after decode |
| `docs-039` | Apache Arrow Rust / Parquet pages identified 59.1.0 ([Arrow](https://arrow.apache.org/rust/arrow/index.html), [types](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html), [Parquet](https://arrow.apache.org/rust/parquet/index.html)) | Typed batches, exact Decimal128, schema metadata, configurable durable columnar files | Types do not encode currency/provenance/PIT; no universal partition/file settings; coercion may lose information | Canonical versioned schemas, checked decimals/times, immutable files and manifest-driven compaction |
| `docs-040` | Apache DataFusion official docs ([overview](https://datafusion.apache.org/), [configuration](https://datafusion.apache.org/user-guide/configs.html), [SQL API](https://datafusion.apache.org/library-user-guide/using-the-sql-api.html)) | Embedded Arrow SQL and Decimal mappings | Memory/temp limits unset by default; unbounded joins and output parallelism can consume resources/create small files; SQL is not a sandbox | Use controlled contexts, allowlisted catalogs, bounded resources/results, cancellation; CLI read-only SQL only |
| `docs-041` | SQLite official documentation; current WAL defect/fix notes ([WAL](https://www.sqlite.org/wal.html), [isolation](https://www.sqlite.org/isolation.html), [foreign keys](https://www.sqlite.org/foreignkeys.html), [STRICT](https://www.sqlite.org/stricttables.html), [PRAGMAs](https://www.sqlite.org/pragma.html)) | Local transactional catalog, one-writer/WAL snapshot model, constraints and checks | Same-host WAL, `BUSY`, checkpoints and long-reader growth; foreign keys off per connection; integrity checks split; version-specific WAL defect | Short catalog transactions, verified fixed runtime/WAL, connection initialization, backups/checkpoints, both checks; never per-event |
| `docs-042` | Official MCP specification version 2025-11-25 ([transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle), [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)) | Stdio framing, negotiation, JSON Schemas, progress/cancellation, security duties | Cancellation races/cooperation; specification supplies no product-specific row/byte/time/artifact caps or business authorization | Pin version; strict lifecycle; bounded typed tools, audit, timeouts, contained artifacts; no SQL/filesystem/risk escape |
| `docs-043` | Coinbase Exchange official WebSocket channel docs ([channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)) | Snapshot/absolute level2 updates, sequenced `full` recovery, heartbeat/status semantics | Single venue; assigned level2 page has no checksum and update examples show no sequence; matches can drop; batches/auctions have weaker semantics | Qualify per channel; sequenced full is only a candidate after all local checks; heartbeat is not price freshness |
| `docs-044` | Kraken Exchange official WebSocket v2 guide ([checksum](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)) | Exact atomic book-update and top-ten CRC32 procedure with decimal/string precision | CRC is top-ten only; guide does not supply freshness, status, sequence, or resync policy | Validate each eligible update; mismatch quarantines; fresh snapshot plus passing checksum before requalification |
| `docs-045` | U.S. SEC official EDGAR APIs and webmaster FAQ ([APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), [FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)) | Submissions/company facts/frames, bulk ZIP strategy, 10 requests/second and user-agent policy | Lag is typical/not guaranteed; frames select aligned facts; bulk is nightly; histories overlap | Honest global budget, bulk bootstrap plus incremental/reconciliation, immutable accession/fact revisions and local availability |
| `docs-046` | Federal Reserve Bank of St. Louis official FRED/ALFRED docs ([observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html), [keys](https://fred.stlouisfed.org/docs/api/api_key.html)) | Offset/count pagination, explicit real-time intervals, vintages/revisions, per-user application keys | Reviewed pages state no numeric request rate; defaults use today; vintages are date-granular | Redact user keys, pin all request/time semantics, exhaust counts, preserve every vintage/supersession |
| `docs-047` | U.S. Bureau of Labor Statistics official developer docs ([getting started](https://www.bls.gov/developers/home.htm), [v2](https://www.bls.gov/developers/api_signature_v2.htm), [FAQ](https://www.bls.gov/developers/api_faqs.htm)) | Free v1 and registered v2 limits, GET/POST forms, messages and footnotes | Daily/rolling caps; 429; success envelope can contain empty/invalid series; no complete vintage-history endpoint shown | v1 zero-cost baseline, optional user v2 key, preflight budgets/chunking, per-series validation, snapshot revisions |
| `docs-048` | U.S. Treasury official Fiscal Data and Treasury daily-rate pages ([REST](https://fiscaldata.treasury.gov/api-documentation/), [XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed), [changes](https://home.treasury.gov/developer-notice-xml-changes), [rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve)) | REST/XML/CSV contracts, distinct pagination, schema changes, CMT methodology | Projection can aggregate; absent/null changed; exact publication/immutability not guaranteed; CMT is derived indicative data | Separate paginators, field/version fixtures and revision hashes; research-only CMT, never execution or Level 1 evidence |
| `docs-049` | `tract-onnx` 0.23.4 Rustdoc plus official ONNX Runtime community classification ([tract](https://docs.rs/tract-onnx/0.23.4/tract_onnx/), [`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html), [`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html), [ONNX context](https://onnxruntime.ai/docs/get-started/community-projects.html)) | Local protobuf import, inference/typed model boundary; Rust is external community support | Incomplete Rustdoc; no universal operators/opsets, equivalence, threading, warm-up, latency, or memory guarantee | Hash- and schema-validated local bundles, golden/stress tests, off-path warm-up, atomic activation, no action on any error |
