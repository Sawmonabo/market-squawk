# Market Squawk Target State

## Document control

- Target release: first complete local release
- Architecture date: 2026-07-16
- Rust baseline: 1.97.1, Edition 2024, resolver 3
- Cost boundary: no mandatory paid software, API, cloud, database service, container runtime, or
  telemetry infrastructure
- Research basis: [Market Squawk deep research](../research/2026-07-15-market-squawk/final-report.md)
- Latest realized live boundary:
  [Q2 Task 8 implementation report](../reports/q2-task8-implementation.md)

## Architectural principles

1. The live execution plane and research data plane are independent pipelines with shared domain
   types and pure mathematics where useful.
2. Execution eligibility is evidence-derived. Operational connectivity, a computed price, fair-value
   classification, and market depth cannot imply execution quality.
3. Mutable live state has deterministic single-writer ownership.
4. Every queue is bounded and every saturation condition has an explicit financial integrity
   consequence.
5. Persistence completion, analytical storage, Python, MCP, and filesystem queries are outside the
   event-to-action path.
6. Provider constraints are respected through declared identity, centralized budgets, caching,
   bulk endpoints, bounded backoff, circuit breakers, health states, and coverage metadata.
7. Library invariants are encoded in types with private fields, checked constructors, and typed
   errors. `anyhow` is restricted to application boundaries.
8. Point-in-time research preserves reference/effective, publication, availability, observation,
   ingestion, revision, and supersession semantics separately.
9. Strategies and models emit intent; only risk creates an approved order; only an execution
   adapter accepts an approved order.
10. Performance, compliance, and production-readiness claims require local evidence.

## System context

```text
                         SHARED DOMAIN
      identity · time · money · quality · provenance · schemas · errors
                              │
             ┌────────────────┴────────────────┐
             │                                 │
      LIVE EXECUTION PLANE              RESEARCH DATA PLANE
             │                                 │
 protocol-specific sources              extraction sources
             │                                 │
 raw capture side branch                parse and validate
             │                                 │
 decode and qualification               normalized observations
             │                                 │
 bounded stable shards                  Arrow record batches
             │                                 │
 books and online state                 Parquet datasets
             │                                 │
 strategy and inference                 DataFusion and Python
             │                                 │
 non-bypassable risk                    analytics and models
             │                                 │
 paper/authorized execution             research outputs
             └────────────────┬────────────────┘
                              │
                     LOCAL CONTROL PLANE
              application services · CLI · SQLite · MCP
```

## Workspace and dependency boundaries

The root is a virtual workspace:

```toml
[workspace]
resolver = "3"
members = ["apps/*", "crates/*", "adapters/*"]
```

Crates are added only with working contracts or production behavior. The final workspace contains:

```text
apps/market-squawk
crates/market-squawk-domain
crates/market-squawk-platform
crates/market-squawk-sources
crates/market-squawk-live
crates/market-squawk-data
crates/market-squawk-analytics
crates/market-squawk-modeling
crates/market-squawk-portfolio
crates/market-squawk-execution
crates/market-squawk-valuation
crates/market-squawk-mcp
adapters/market-squawk-adapter-coinbase
adapters/market-squawk-adapter-kraken
adapters/market-squawk-adapter-sec
adapters/market-squawk-adapter-fred
adapters/market-squawk-adapter-bls
adapters/market-squawk-adapter-treasury
adapters/market-squawk-adapter-files
adapters/market-squawk-adapter-portfolio
adapters/market-squawk-adapter-paper
```

### Allowed dependency direction

```text
domain
├── sources -> domain
├── analytics -> domain
├── platform -> domain
├── live -> domain, sources, analytics
├── data -> domain, sources, analytics, platform
├── modeling -> domain, analytics, data
├── portfolio -> domain, analytics, data
├── execution -> domain, live, analytics, modeling, portfolio
├── valuation -> domain, analytics, data
└── mcp -> domain + platform application-service traits, never concrete adapters

provider adapters -> domain + sources + platform contracts; normal/build edges to data are forbidden
paper adapter -> domain + execution + platform confinement contracts
app -> all composition roots and concrete service implementations
```

An adapter package may receive an explicit development-only edge to a terminal consumer solely for
an end-to-end integration test. The current exceptions are Kraken to execution/paper and the file
and portfolio adapters to data. These exceptions never authorize a production edge or runtime call
from an adapter into data, live, or another terminal consumer.

The dependency graph must remain acyclic. `domain` performs no network, database, filesystem, MCP,
Python, or model-runtime work. `live` cannot depend on `data`, `platform` catalog code, `mcp`, or
Python. `analytics` contains dependency-light pure kernels; batch orchestration in `data`,
`modeling`, and `portfolio` consumes those kernels rather than making the analytics crate depend on
storage. `platform` defines configuration/lifecycle primitives and typed application-service
contracts but does not depend on concrete live, data, modeling, portfolio, or execution
implementations. The app composition root implements those service contracts, and both MCP and CLI
call the same instances rather than adapters directly.

## Shared domain

### Identity and financial types

All public domain structs use private fields and constructors. Core types include:

```rust
pub struct InstrumentId(Uuid);
pub struct VenueId(String);
pub struct SourceId(String);
pub struct AccountId(Uuid);
pub struct StrategyId(String);
pub struct ModelId(String);
pub struct SequenceNumber(u64);
pub struct PriceTicks(i64);
pub struct QuantityLots(i64);
pub struct BasisPoints(i32);
pub struct SchemaVersion(u32);

pub struct Money {
    amount: Decimal,
    currency: Currency,
}
```

Instrument definitions own tick size, lot size, currency, venue mappings, trading status, and
precision. Provider decimals are converted with `TryFrom` using explicit rounding policy; values
that cannot be represented exactly fail. Live arithmetic uses scaled integers and checked
operations. Analytical/accounting values use Decimal or Arrow Decimal128. Floating point is limited
to an explicit statistical/model boundary.

Validated identifier types cover ticker/venue symbol, CUSIP, ISIN, SEDOL, FIGI, OCC options,
futures contracts, crypto pairs/chain addresses, and provider IDs. Syntax/check-digit validity is
kept distinct from registry resolution. Effective-time identity records preserve symbol history,
mergers, delistings, contract rolls, and corporate actions without changing internal
`InstrumentId` identity.

Futures identities preserve the rendered FIX Latest EP307 tag 200 `MonthYear` forms (`YYYYMM`,
`YYYYMMDD`, or `YYYYMMwN`) without reducing day/week designators to a month. Tag 541 maturity date,
leg-scoped tag 610 and 611 claims, and independently supplied lifecycle dates remain separate.
Lifecycle data includes first/last trade, expiration, notice, delivery, and settlement dates; no
field is synthesized from another. The dated official-source evidence and response-body hashes are
retained in the [Quarter 1 contract decisions](../research/2026-07-16-q1-contract-decisions.md).

Provider identity records bind the namespace/native ID and stable instrument to content evidence,
timestamps, effective interval, and caller/source-supplied revision and predecessor claims. Those
claims are bound to exact content evidence. Authority must be established separately by the
applicable registered source and source-specific adapter verification; these caller/source-supplied
values do not establish it. `ProviderIdentityEvidence` retains zero or more bounded canonical
version-pinned locators (`ProviderIdentityEvidence::MAX_LOCATORS = 64`) as non-substantive retrieval
metadata, never evidence. Content-equivalent reingestion is idempotent at the logical-assertion
layer: it creates no second logical assertion. The registry deterministically coalesces bounded
locator and observation metadata and returns `ObservationCoalesced`; an exact repeat with no new
metadata leaves canonical registry state unchanged. Divergence is quarantined.

`ChainId` validates and preserves the case-sensitive CAIP-2 envelope only. Namespace-specific
qualification is separate: `eip155` references are base-10 chain IDs derived from `eth_chainId`,
while `solana` references are the first 32 characters of the genesis hash. Solana account/mint
addresses remain separately validated 32-byte base58 public keys. Generic CAIP-2 syntax never proves
that a chain exists or that a namespace-specific reference is canonical.

Digest semantics are algorithm-qualified and domain-neutral. `DigestAlgorithm::{Sha256, Blake3}`
is the canonical root type; `PayloadHashAlgorithm` remains a compatibility alias. An
`EvidenceDigest` is constructed with `EvidenceDigest::new(algorithm, bytes)` and equality includes
both fields. Canonical state uses `CanonicalStateDigest::new(evidence_digest,
CanonicalizationRule::new(rule_id, rule_version))`; equality additionally includes the rule ID and
one-based rule version. `LiveEvidenceBinding::payload_digest()` returns the algorithm-qualified
payload digest, while `canonical_state_digest()` and `BookStateBinding::state_digest()` return the
rule-qualified state digest. Neither an algorithm nor a canonicalization rule is inferred from
bytes, field names, or provider defaults.

### Separate classification types

```rust
pub enum FairValueHierarchy {
    Level1,
    Level2,
    Level3,
    Unclassified,
}

pub enum MarketDepth {
    TopOfBook,
    PriceLevel,
    OrderLevel,
}

pub enum DataQuality {
    DirectVerified,
    DirectUnverified,
    OfficialDelayed,
    Aggregated,
    Indicative,
    Modeled,
    Estimated,
    Stale,
    Quarantined,
}
```

Additional operational types remain separate:

```rust
pub enum StreamIntegrityState {
    Initializing,
    Synchronizing,
    Validating,
    Healthy,
    Stale,
    GapDetected,
    ChecksumFailed,
    Divergent,
    Quarantined,
}

pub enum CaptureIntegrityState {
    Disabled,
    Healthy,
    Incomplete,
}

pub enum ExecutionEligibility {
    Eligible,
    Ineligible,
}
```

No implicit or infallible conversion exists among these types.

`ExecutionEligibility` in serialized domain data is an archive/control-plane status, not a bearer
capability. Archive-facing live provenance always returns the unit variant `Ineligible`, even when
it retains a historical `DirectVerified` classification and assessment reference. A
`QualificationAssessment` deliberately exposes no `execution_eligibility` method.

`QualificationAssessment` derives `recorded_quality` and an `EligibilityFailures` bit set from its
bound inputs. Callers supply neither derived field. Its audit-only result is queried with
`assessment_status_at(at) -> AssessmentStatus`; callers can inspect `failures()` or
`has_failure(EligibilityFailure)`. `Satisfied` is still not runtime authority. Custom Serde
deserialization rejects unknown fields, reconstructs through `QualificationAssessmentInput` and
`TryFrom`, and rejects any tampered derived quality, failures, evaluated time, or validity deadline.

### Provenance and time

Every canonical live record includes schema version, source, internal instrument, venue where
applicable, source identifier, connection generation, source timestamp, receive time, explicit
availability time, ingestion time, data quality, source coverage, payload hash/reference, and an
optional durable assessment reference. It does not embed a full `QualificationAssessment`.
Construction and deserialization enforce `received_at <= available_at <= ingested_at` and do not
default a missing wire `available_at`. Serde round trips retain recorded classification/evidence but
can never mint current execution authority.

Concretely, `LiveProvenance` owns a complete `LiveEvidenceBinding` instead of separately writable
source, venue, instrument, generation, channel, event, payload, and canonical-state identity fields.
Its archive `record_state` is derived from the checked construction path or optional wire assessment
reference and is not serialized. `RecordedLiveProvenanceInput` accepts a caller-supplied archival
classification and opaque assessment reference. The recorded path requires the reference, and the
wire rejects `DirectVerified` without one, but provenance does not dereference or prove the external
assessment relationship. It remains an audit assertion that the stateful live plane must revalidate.

Every research observation additionally includes effective/reference time, publication time when
known, availability time when evidenced, local first-observed time, revision identity, and
superseded time. Unknown historical availability remains unknown; ingestion code does not invent it
from a filing acceptance or observation date.

## Source framework

Live and extraction sources use distinct contracts:

```rust
pub trait SourceMetadataProvider {
    fn metadata(&self) -> &SourceMetadata;
}

pub trait LiveMarketSource: SourceMetadataProvider {
    async fn run(
        &mut self,
        frames: &mut RawFrameFactory,
        sink: &mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> Result<(), SourceError>;
}

pub trait MarketDecoder: SourceMetadataProvider {
    fn decode(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<DecodedProviderBatch, DecodeError>;
}

pub trait ExtractionSource: SourceMetadataProvider {
    async fn discover(
        &self,
        request: DiscoveryRequest,
    ) -> Result<Vec<SourceObject>, SourceError>;

    async fn extract(
        &self,
        request: ExtractionRequest,
    ) -> Result<ExtractionBatch, SourceError>;
}
```

These signatures express the asynchronous semantic contract. The heterogeneous runtime registries
use object-safe boxed futures allocated once per source session or extraction request, never once
per market event. `MarketDecoder` is synchronous, bounded, source-specific, and separately testable;
capture occurs before it is invoked. It returns provider-normalized pre-state observations, not
caller-asserted canonical `MarketEvent` values. Numeric lexemes/checked decimals, provider sequence,
snapshot markers, expected checksums, source timestamps, and trading/status fields remain available
until the instrument-owning live shard validates and applies them atomically. Provider validation
loops remain concrete, bounded code.

`SourceMetadata` is immutable/versioned and declares its revision/content hash, authorization mode
and evidence/effective interval, provider, endpoint allowlist, evidenced asset/venue/event/depth
coverage and effective interval, delay, consolidation status, sequence/checksum capabilities,
historical/revision coverage, rate policy, schema version, and data-quality ceiling.
`AuthoritativeSourceRegistry` validates that metadata and emits registered-source/current-session
handles consumed by the live authority issuer; loose metadata/result enums cannot construct the
gate. Metadata also binds the provider decoder revision, exact sequence semantics/rule version,
checksum algorithm/canonicalization/scope/depth/level count, and separate future-skew, transport-age,
source-age, market-age, and connection-idle policies. Capability enums alone never prove that those
validators ran.

`SourceHealthSnapshot` is a serializable audit/control-plane DTO only. Current execution authority
uses an opaque registry/supervisor-owned health lease keyed by registry epoch, source, metadata
revision, session, connection generation, and health epoch. A validated live-scope handle selects
one exact authorization, venue/instrument membership, provider product/channel, event/depth rule,
runtime subscription, effective deadline, delivery mode, and validation profile. Callers cannot
assemble this scope from loose metadata or a self-reported healthy snapshot.

The registry also issues one process-local, one-way capture allocation for each connection
generation. The registry returns exactly one non-`Clone`, non-Serde, non-constructible
`CaptureGenerationCapabilities` bundle. The bundle retains the exact binding/allocation and can be
consumed only into initialization, admission, and degradation capabilities for that same
allocation. Platform capture accepts the whole bundle, never loose capabilities, and owns the
resulting non-`Clone` initializer and admission issuer plus the cloneable degradation-only handle.
A cloneable publisher cannot initialize, promote, or rotate capture health. Successful enqueue
returns Task 5's concrete owned, non-Serde admission receipt bound to the exact generation
allocation, frame ordinal, raw-frame digest, and receive time. The registry consumes that exact
receipt with the decoded provider batch to produce the owned, `Send + 'static` current-batch
envelope admitted to a shard. Same-generation degradation is terminal, and audit/replay values
cannot reconstruct either receipt or current authority.

The cross-crate composition contract lives in `market-squawk-domain`; the dependency remains
`platform -> domain` and never becomes `platform -> sources`. It preserves the concrete
frame/receipt relationship with associated types and static dispatch:

```rust
pub trait RawCaptureFrameView: Clone + Send + Sync + 'static {
    fn source_id(&self) -> &SourceId;
    fn metadata_revision(&self) -> &MetadataRevision;
    fn session_identifier(&self) -> &SourceIdentifier;
    fn connection_generation(&self) -> ConnectionGeneration;
    fn frame_ordinal(&self) -> NonZeroU64;
    fn received_at(&self) -> Timestamp;
    fn payload(&self) -> &[u8];
    fn retained_bytes(&self) -> usize;
}
pub trait CaptureInitializer: Debug + Send + 'static {
    fn mark_healthy(&mut self) -> Result<(), CaptureAuthorityError>;
}
pub trait CaptureAdmission<Frame>: Debug + Send + 'static {
    type Receipt: Debug + Send + 'static;
    fn preflight(&self, frame: &Frame) -> Result<(), CaptureAuthorityError>;
    fn issue_after_enqueue(
        &mut self,
        frame: &Frame,
    ) -> Result<Self::Receipt, CaptureAuthorityError>;
    fn validate_active(&self, frame: &Frame) -> Result<(), CaptureAuthorityError>;
}
pub trait CaptureDegradation: Clone + Debug + Send + Sync + 'static {
    fn mark_incomplete(&self);
}
pub trait CaptureAuthorityBundle: Debug + Send + Sized + 'static {
    type Frame: RawCaptureFrameView;
    type Receipt: Debug + Send + 'static;
    type Initializer: CaptureInitializer;
    type Admission: CaptureAdmission<Self::Frame, Receipt = Self::Receipt>;
    type Degradation: CaptureDegradation;

    fn into_parts(
        self,
    ) -> (Self::Initializer, Self::Admission, Self::Degradation);
}
```

The platform channel is generic over the whole bundle. Publication linearizes as concrete
admission preflight, checked byte reservation, bounded `try_send`, concrete receipt issuance, and
a final active-allocation check. Every full, closed, writer, flush, shutdown, rotation, accounting,
or control-drop failure degrades that exact allocation before it returns. A diagnostic or test
bundle can mint only its own associated receipt type; Task 5's registry accepts only its private,
concrete receipt and consumes its exact allocation/frame binding. Object erasure is intentionally
not used at this compile-time composition boundary.

The registry separately issues one non-`Clone`, non-Serde `RawFrameFactory` to the active adapter
session. It binds the current source/revision/session/generation lease and owns the checked,
nonzero, never-reused frame-ordinal counter. `LiveMarketSource::run` receives this restricted
factory explicitly: adapters can create raw frames but cannot access registry, health, capture, or
execution authority. Session invalidation or ordinal exhaustion makes further frame creation fail
closed.

### Provider access policy

Each provider has one local shared request budget. All workers for the same provider/account share
that budget. The HTTP client uses explicit connect/read/total timeouts, redirect policy, proxy
policy, user agent, TLS roots, response-size bounds, and retry classification. HTTP 429 and provider
block responses trigger `Retry-After` or capped exponential backoff with jitter and a source-health
transition. Adapters never rotate identity, account, fingerprint, proxy, or hosts to defeat limits.

## Live execution plane

### Event flow

```text
socket/protocol reader
        │
        ├── try_send(raw capture) ──► bounded audit writer ──► local segment
        │
        └── source decoder
                 │
        bounded provider-normalized observation
                 │
          try_send(stable shard)
                 │
       exact precision/time/sequence validation
                 │
       atomic snapshot/delta/checksum/book transition
                 │
       canonical event and instrument-owned rolling state
                 │
        online features and optional inference
                 │
              strategy
                 │
          pre-trade risk service
                 │
          execution adapter
```

The source reader never waits for disk completion. A raw-capture enqueue failure synchronously
degrades the exact registry-owned capture allocation before emitting a best-effort integrity event
and prevents that frame from producing an executable action. Shard ingress is bounded by both
message count and checked deep retained bytes. An admission failure synchronously invalidates the
exact source/instrument/generation execution lease before returning; it never attempts to enqueue
quarantine into the full mailbox. No critical message is silently discarded. The decoder never
owns a second order book and cannot pre-assert precision, checksum, sequence, or canonical-state
success. The shard computes the post-apply canonical state digest while retaining the exact
initializing snapshot identity/digest.

### Stable sharding

Shard routing is `stable_hash_v1(venue_id, instrument_id) % shard_count`. V1 hashes ASCII
`MSQKSHARD`, tag `0x01`, the venue UTF-8 byte length as a big-endian `u16`, the venue bytes, and the
instrument UUID's 16 network-order bytes with fixed FNV-1a 64-bit constants. The version, byte
encoding, full hash, and shard indices are explicit and covered by golden/property tests. Each
shard task owns all mutable books, rolling windows, feature state, strategy state, and local risk
state for its instruments. No other task obtains a mutable reference.

Shard-count changes are restart-time configuration changes with an explicit state-rebuild policy;
they do not remap live state dynamically.

Task 8 realizes this boundary with pre-feed exact-generation binding, nonblocking count-and-byte
admission, a checked peak-memory model, one writer per route, complete readiness before ingress,
and invalidation-before-return on every admission failure. The persisted implementation evidence is
the [Q2 Task 8 report](../reports/q2-task8-implementation.md).

### Audit assessment and current execution authority

The domain `QualificationAssessment` is a durable audit explanation. It binds one source, source
metadata revision, authorization and scoped coverage record, venue, instrument, channel/event/depth,
session generation, payload and canonical-state revision, snapshot/sequence/checksum evidence,
source/receive/assessment timing window, freshness, status, precision, and stream/capture integrity.
It derives an `EligibilityFailures` set and `DataQuality`, exposes
`assessment_status_at(at) -> AssessmentStatus`, and has invariant-preserving durable Serde. It
exposes no execution-eligibility method, promotion method, or current capability.
`FairValueHierarchy` is not an assessment input.

The stateful `market-squawk-live` authority issuer is the only component that can mint the opaque,
non-Serde, non-`Clone`, single-use, short-lived `LiveExecutionCapability` accepted by risk. Before
issuance it rebinds the complete assessment to the authoritative current source registry/session and
instrument-owned state. A current `DirectVerified` classification is possible only when those inputs
prove:

- Known authorized source
- Known venue and internal instrument
- Explicit source coverage
- Connection generation
- Snapshot/update synchronization
- Valid sequence progression where supplied/required
- Checksum validation where supported
- Timestamp sanity
- Freshness within configured limits
- Valid trading/instrument/venue status
- Exact tick and lot precision
- Consistent non-crossed book

Issuance consumes an opaque validated decoded-batch proof, current session lease, current-health
lease, validated live-scope handle, and a private applied-observation view from the owning shard.
The issuer owns its clock and derives every assessment time and deadline. Exact provider numeric
evidence is converted against the current instrument tick/lot definition before a canonical event
exists. `AllDeclared` coverage requires a registry-owned universe attestation; partial or otherwise
unproven membership cannot qualify for immediate automated action.

The capability also binds one-way exact-generation and shard-liveness lease allocations, runtime
incarnation, capture allocation, and checked state revision. Overflow, rollover, degradation, and
actor exit publish Release invalidation before returning or exiting. Issuance checks authority
before and after bounded nonce registration; consumption, risk, and dispatch recheck with Acquire
semantics. Already queued commands from an invalidated generation are diagnostic-only. This makes
authority revocation linearizable without a registry lookup, database query, lock, or collection
scan in the live path.

Capability expiry is policy-derived as the earliest freshness, metadata/authorization/coverage, or
maximum-lifetime deadline. Loss of any required evidence changes quality to `Quarantined` or
`Stale`, revokes outstanding capability nonces, clears executable features/signals, and requires
source-specific requalification in a current generation. A domain assessment, archive, immutable
snapshot, replay record, or caller-authored quality variant can never substitute for the capability.

Coinbase Level 2 is `DirectUnverified` unless a selected channel and implementation establish the
complete evidence contract. Kraken WebSocket v2 price-level books can become `DirectVerified` only
after message-atomic application and the documented top-ten CRC32 validation passes.

### Immutable control-plane snapshots

Each shard builds complete bounded immutable snapshots after the action decision at a configured
coalesced cadence, then atomically publishes the latest `Arc` through a crate-private `ArcSwap`.
Tokio `watch` is not the snapshot value store because an outstanding receiver borrow may block its
producer. Optional notifications are separate bounded, coalescing hints. A snapshot aggregator
builds bounded views and a sorted per-shard revision vector for application services; it never
fabricates a single-instant cross-shard `as_of`. CLI and MCP receive bounded DTOs and never obtain
the snapshot cell, leases, issuer, nonce state, mutable shard state, or event-handler access.

Reader retention is bounded by returned shard generation: a single-shard lease consumes one
permit, while an aggregate lease consumes one permit for every retained shard. Slow readers can
therefore exhaust only the explicitly modeled retention allowance; publication remains independent.
Runtime shutdown closes new reads without mutating already retained authority-free DTOs.

### Strategies, inference, and risk

Strategies implement the canonical `Strategy` trait and emit `OrderIntent`. An intent includes
strategy/model identity, instrument, side, order type, quantity, price, time in force, signal and
expiration times, reason codes, maximum slippage, and required data quality.

Only the risk service can construct `ApprovedOrder`. It consumes `LiveExecutionCapability` by value
and revalidates action time, current generation, current source health, the authoritative metadata/
authorization/coverage revision, and all normal exposure, leverage, capital, price/slippage, order
rate, duplicate, loss/drawdown, and expiration checks. An approval's expiry cannot exceed the
consumed evidence. Its constructor is crate-private, its fields are private, and it is neither Serde
nor `Clone`. Every risk decision records evaluated limits and reason codes. Inference errors produce
no action.

`ExecutionDispatcher` atomically consumes an approval ID once, rechecks expiry and authority
revocation immediately before the backend call, and creates the privately constructible
adapter-only `DispatchOrder`. CLI, MCP, models, strategies, archives, and replay cannot construct or
submit capabilities, approvals, or dispatch values directly.

## Paper execution

The baseline execution adapter implements:

- New, accepted, partially filled, filled, cancel-pending, canceled, rejected, and expired states
- Deterministic seeded latency distributions
- Bid/ask and configurable slippage
- Depth-aware partial fills
- Venue/instrument fee schedules
- Rejections and trading-status checks
- Idempotent client keys and duplicate suppression
- Cash, balances, positions, and fees
- Cancel and reconcile
- Durable orders, fills, and reconciliation results outside the live path

Model parameters, calibration source, version, scope, and data quality are explicit. Stylized impact
models are labeled `Modeled`; they do not make simulated fills execution-quality observations.

## Research data plane

### Control and analytical storage

- SQLite stores source configuration, migrations, cursors, registries, manifests, run state,
  application audit, and artifact metadata.
- Arrow `RecordBatch` is the in-memory interchange.
- Parquet stores versioned analytical datasets.
- DataFusion provides embedded read-only analytical SQL through bounded application services and
  the local CLI.

SQLite connections enable foreign keys and use versioned migrations, intentional transaction and
busy-timeout policy, integrity checks, and a documented WAL/checkpoint policy. SQLite is never
queried per live event.

### Dataset contract

Every dataset has:

- Stable dataset identity and schema version
- Arrow schema with application metadata
- Source, quality, coverage, and provenance columns
- Manifest containing partitions, files, row counts, hashes, min/max statistics, and parent inputs
- Idempotency key and deduplication policy
- Partition policy derived from query patterns
- Atomic publish and previous-version retention
- Compaction policy and small-file limits
- Point-in-time and revision semantics
- Corporate-action policy

Readers resolve a manifest version and never infer completeness from a directory listing.

### Required adapters

- Files: CSV, TSV, JSON, NDJSON, Parquet, plus schema and size bounds
- SEC: bulk initialization/reconciliation plus submissions and Company Facts incremental ingestion
- FRED/ALFRED: pinned real-time parameters and vintage preservation
- BLS: deterministic v1/v2 request chunking and preliminary/revision indicators
- Treasury: paginated Fiscal Data plus documented rate files and yield methodology
- Portfolio: holdings, transactions, totals, raw source preservation, and reconciliation

External network tests are opt-in and separate. Default tests use recorded, license-compatible
fixtures. Provider responses are size bounded, hashed, cached, and converted through strict typed
validation before publication.

## Analytics and point-in-time datasets

Pure mathematical kernels are deterministic and independently testable. Live-compatible kernels
avoid allocation where practical and declare warm-up/null behavior. Batch analytics include
returns, volatility, drawdown, correlation, beta/alpha, Sharpe/Sortino, tracking error, information
ratio, VaR, Expected Shortfall, factors, fundamentals, valuation, surprises, yield curves,
portfolio exposure/attribution, and scenarios.

Expected Shortfall defines its quantile convention for discrete weighted observations, including
fractional weighting of the quantile atom. Every risk function states units, annualization,
missing-value, weight, and insufficient-history policies.

The point-in-time dataset builder filters on evidenced availability, preserves historical universe
membership and delisted instruments, applies versioned corporate-action policy, generates labels
after feature cutoffs, records all trial/dataset versions, and includes future-perturbation leakage
tests.

## Modeling and inference

The feature registry records name/version, input schema, parameters, time semantics, warm-up, null
policy, output type, live compatibility, point-in-time compatibility, and implementation revision.

Model bundles contain the artifact and hash, format/version, feature schema, normalizers, training
period, universe, dataset versions, label definition, code revision, validation metrics, decision
thresholds, intended use, limitations, and fallback behavior.

Native Rust inference is the baseline. ONNX-compatible inference is conditional on exact runtime
provenance, local verified native artifacts where required, operator compatibility validation,
trusted size-bounded bundles, warm-up, deterministic threading policy, input/output schema checks,
and fail-closed errors. Python trains and explores models outside the live path.

## Portfolio system

Portfolio imports retain the original source record, normalize accounts/positions/transactions/
cash flows, calculate cost basis with an explicit lot policy, and reconcile positions, cash, and
supplied totals. Performance, allocation, sector/factor/currency exposure, attribution, rebalancing,
risk, and scenarios share typed account/instrument/currency identities.

## Fair-value system

Valuations store method, inputs, classification, reason, evidence, ruleset version, override, and
approval. Deterministic rules default missing evidence to `Unclassified`.

A Level 1 candidate requires an identical instrument, quoted unadjusted price, active and
accessible market, measurement-date relevance, valid source/venue evidence, and adequate freshness.
Delayed, stale, proxy, adjusted, modeled, or similar-instrument values cannot silently qualify.

Fair-value classification never changes `DataQuality` or `ExecutionEligibility`. Level 2/3 inputs
remain usable for analysis without promotion into execution-quality observations.

## Application services, CLI, and MCP

Application services own authorization, cancellation, deadlines, audit, result bounds, and
artifact publication. CLI and MCP are transports over the same services.

The CLI provides the required command tree and read-only bounded DataFusion query support. Existing
v0.1 commands retain tested compatibility aliases during migration.

MCP uses the official protocol contracts over local stdio, enforces lifecycle and negotiated
capabilities, keeps stdout protocol-clean, validates typed schemas, propagates cancellation and
deadlines, limits instruments/time/results, records audits, and writes large results only to the
controlled artifact directory. It exposes no shell, arbitrary paths, unrestricted SQL, credentials,
remote code, unchecked orders, risk bypass, or audit deletion.

## Configuration and secrets

Precedence is safe defaults, local config file, `MARKET_SQUAWK_*` environment variables, then CLI
overrides. Effective configuration is validated and can be printed with secrets redacted.

Credentialed adapters use OS keyring storage where available and an encrypted local fallback.
Secret values never implement `Display`, never enter tracing fields, and never appear in MCP or
artifacts. Source and execution endpoints are allowlisted; unsafe TLS and concealment proxy options
are absent from production configuration.

## Compatibility and migration

- Readers accept committed `MEJ1/.mej` and renamed `MSJ1/.msj` journals.
- Writers emit one documented current format; a new format requires a new magic/version and reader
  compatibility tests.
- Current CLI behaviors remain integration-tested while the hierarchy grows.
- Dataset and SQLite migrations are versioned, forward-applied, and backed up before destructive
  changes.
- Model, feature, ruleset, source schema, and dataset versions are immutable identifiers.
- No migration silently reinterprets data quality, time semantics, currency, scale, or hierarchy.
- The legacy application engine is named `DiagnosticEngine`, its app-local event types are private
  and explicitly prefixed `Diagnostic*`, and it has no conversion into current production batches.
  It is deleted only after Task 11 adapters emit receipt-validated current batches after pre-feed
  binding and Task 13 services consume Task 8 immutable snapshots.

## Verification and release gates

Every stage runs formatting, locked all-feature workspace Clippy/tests, and a release build on Rust
1.97. Stage-specific gates add property tests, external-test separation, fuzz smoke runs, benchmark
checks, dependency advisory/license/source policy, credential and generated-artifact scanning,
SBOM, hashes, and build provenance.

The complete release measures decoder throughput, sequence/checksum validation, queue latency,
book/features/strategy/risk latency, event-to-decision percentiles, Arrow/Parquet/DataFusion
throughput, and peak memory on documented hardware. The 100,000 events/s and sub-millisecond warmed
p99 targets are claims only after those measurements pass.

## Security boundary

Market Squawk does not implement identity/account rotation to evade limits, browser/TLS fingerprint
concealment, CAPTCHA/anti-bot bypass, blocking-evasion proxies, or distributed quota evasion. These
behaviors are prohibited by design review, configuration schemas, tests, and source policy.

## Exact identity evidence invariant

Source-authored identity assertions use neutral exact-evidence contracts. `ExactPayloadEvidence`
always carries an algorithm-qualified content digest. Its optional `VersionPinnedSourceLocator`
retains bounded caller/source-supplied locator and version-pin metadata for retrieval, but does not
independently prove that version immutable and never replaces the digest. `FuturesContractIdentity`
uses `RevisionBoundPayloadEvidence` so its caller/source-supplied `MetadataRevision` is retained with
the exact payload evidence supplied for that revision claim; the binding preserves association but
does not by itself establish revision authority. Authority must be established separately by the
applicable registered source and source-specific adapter verification; these caller/source-supplied
values do not establish it. `ExternalIdentifierRecord` retains the same digest contract for its
supplied assignment claim. A moving `FIX.Latest` or provider URL cannot qualify either assertion by
itself. The strict wire rejects omitted content evidence and unknown fields, including legacy
generic-reference shapes. It preserves the explicit digest algorithm as part of evidence identity;
changing the explicit algorithm while retaining the same bytes produces distinct valid evidence
rather than a deserialization error.

## Target-state completion

The release is complete only when the required live and research adapters, verified qualification,
books, features, research ingestion, point-in-time datasets, model bundles/inference, backtesting,
portfolio analytics, risk-enforced realistic paper execution, fair value, typed local MCP, CLI,
audits, fuzz targets, benchmarks, and release evidence all work locally without a paid, cloud,
OpenTelemetry, or remote-database requirement.
