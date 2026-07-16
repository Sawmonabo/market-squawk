# Market Squawk current state

## Document control

- Audit date: 2026-07-16
- Repository: `market-squawk`
- Reviewed implementation baseline: integrated Q2 Tasks 1–8 commit `581d4fd` (rejected pending the
  Q2-R01–Q2-R15 remediation ledger)
- Evidence: repository inspection, locked local verification,
  [Q2 live-readiness audit](../plans/q2-live-readiness-audit.md),
  [Q2 Task 8 implementation report](../reports/q2-task8-implementation.md),
  [Q2 checkpoint rejection ledger](../reports/q2-checkpoint-review.md), and
  [deep-research report](../research/2026-07-15-market-squawk/final-report.md)

This document describes working software at the stated baseline. Interfaces, synthetic fixtures,
diagnostic compatibility code, and roadmap text are not counted as production capability.

## Executive assessment

Market Squawk is now a Rust 1.97 virtual workspace with four production contract crates, one
application composition crate, asynchronous raw capture, a fail-closed current-source registry, a
transactional live processor, deterministic single-writer shards, exact count-and-byte admission,
bounded immutable snapshots, and supervised runtime lifecycle.

The most important live safety boundary is implemented: only an owned, receipt-validated
`CurrentDecodedProviderBatch` carrying the exact current source allocation can bind to production
ingress. A serialized assessment, replay record, diagnostic event, health snapshot, caller-authored
quality value, or stale generation cannot reconstruct current execution authority. Queue failure,
actor exit, shutdown, and runtime replacement invalidate the relevant one-way authority before
returning or exiting.

This is not the complete local release. Production Coinbase/Kraken adapters, online feature and
strategy integration, comprehensive risk and paper execution, the research plane, analytical
storage, modeling, portfolios, fair-value analysis, complete MCP/CLI services, fuzzing,
benchmarks, audits, and release evidence remain in their assigned implementation tasks. No
performance claim has been made.

## Quarter 2 production-readiness correction

The exact integrated Tasks 5–8 checkpoint `581d4fd` was rejected by both independent reviewers.
The components below exist and their focused tests remain useful evidence, but the following
cross-component properties are not accepted as production-ready until the linked Q2-R findings are
closed and re-reviewed:

- decoded/current-batch byte accounting omits nested book/change allocations (Q2-R01);
- caller-authored future health and budget state can outlive or contradict authoritative current
  conditions (Q2-R02–Q2-R05);
- recoverable first-event rejection can later terminate a shard through incomplete snapshot
  provenance (Q2-R06);
- persistent and transient snapshot memory are undercounted (Q2-R07–Q2-R08);
- snapshot timers can starve control/data queues and public snapshot deserialization bypasses
  invariants (Q2-R09–Q2-R10);
- a blocked capture sink can outlive a timeout after supervision ownership is discarded (Q2-R11);
  and
- failure atomicity, aggregate-reader configuration, Windows CI, and cadence semantics require the
  recorded hardening corrections (Q2-R12–Q2-R15).

Consequently, statements later in this document describing exact byte or peak-memory bounds,
complete current health, route-local rejection, clean capture-worker shutdown, and bounded
snapshot input are descriptions of the pre-review implementation intent, not accepted guarantees.
The [checkpoint ledger](../reports/q2-checkpoint-review.md) is authoritative until a replacement
commit closes each item with code and regression evidence.

## Workspace and toolchain

The root is a virtual Cargo workspace:

```text
market-squawk/
├── apps/
│   └── market-squawk/
├── crates/
│   ├── market-squawk-domain/
│   ├── market-squawk-live/
│   ├── market-squawk-platform/
│   └── market-squawk-sources/
├── docs/
├── scripts/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
└── rustfmt.toml
```

`adapters/*` is intentionally not a workspace glob yet because no production adapter crate exists;
Task 11 adds the directory with the first working adapter rather than creating empty crates.

Implemented baseline controls include:

- Rust 1.97.0 stable, Edition 2024, resolver 3;
- inherited workspace version, license, Rust version, and lint policy;
- `unsafe_code = "forbid"`, denied unused results, and strict Clippy policies for unwrap, expect,
  panic, todo, and unimplemented paths;
- a committed workspace lockfile owned by the integrated root;
- 100-column rustfmt and focused source modules; and
- typed `thiserror` errors in library crates with `anyhow` restricted to the application boundary.

The workspace currently has no Arrow, Parquet, DataFusion, SQLite, ONNX, keyring, encryption,
Criterion, or cargo-fuzz implementation. Those dependencies are added only with their working
consumers in later tasks.

## Shared domain

`market-squawk-domain` contains private, validated identity and financial types, including
`InstrumentId`, `VenueId`, `SourceId`, provider identities, ticker and venue symbols, CUSIP, ISIN,
SEDOL, FIGI, OCC option identity, futures identity, crypto pair and chain addresses,
`SequenceNumber`, `PriceTicks`, `QuantityLots`, `BasisPoints`, tick/lot sizes, currency, money,
rounding policy, and exact decimal conversion.

Instrument definitions and identity records cover provider mappings, effective intervals, symbol
changes, lifecycle transitions, contract rolls, evidence, conflicts, supersession, and rights/
entitlement metadata. The registry contracts are in memory; durable SQLite catalog persistence is
not yet implemented.

The domain keeps these concepts separate:

- `FairValueHierarchy`;
- `MarketDepth`;
- `DataQuality`;
- `StreamIntegrityState` and capture integrity;
- audit-only qualification assessments; and
- process-local execution authority.

Canonical live events cover trades, quotes, snapshots, deltas, auctions, halts, instrument status,
and corporate actions. Research observations cover filing, fundamental, macro, position,
transaction, corporate-action, and alternative-data shapes. Live and research provenance retain
separate availability and time semantics. The research types are contracts only; there is no
working research producer or durable dataset yet.

## Source contracts and current authority

`market-squawk-sources` separates live and extraction source contracts. Source metadata includes
typed capabilities, coverage topology and delay, numeric policy, protocol profile, checksum and
sequence profiles, freshness limits, authorization, and provider identity. Network policy includes
endpoint rules, redirect authorization, bounded requests, retry/backoff, shared provider budgets,
and quota-health outcomes. These are working contract types with deterministic tests, not yet a
complete adapter fleet.

The live decoder boundary produces bounded, source-normalized batches with exact decimal lexemes,
sequence/snapshot/checksum/status evidence, frame identity, and checked retained-byte accounting.
The authoritative registry binds metadata, session generation, health epoch, capture allocation,
frame ordinal, live scope, and current deadlines. It produces nonempty homogeneous route batches
of `CurrentProviderObservation` values while preserving wire order.

`CurrentDecodedProviderBatch` is owned, non-Serde, and process-local. Its exact
`CurrentSourceAuthorityLease` can be obtained before a feed starts, allowing Task 8 to perform the
pre-feed route binding handshake without trusting the first data batch.

The application still includes a working public Coinbase WebSocket reader and synthetic mock feed
for diagnostic capture, replay, and compatibility MCP display. They use the private app-local
diagnostic event model and are not production adapters: they do not produce current batches and
cannot enter the production runtime. The required production Coinbase and Kraken adapters are
Task 11 work.

## Raw capture and local platform

`market-squawk-platform` implements local configuration, confined paths, controlled artifact
roots, MSJ1 journal compatibility, and generic asynchronous capture. The capture queue is bounded
by count and retained bytes; admission success is independent of disk completion. Capture
authority is supplied as one registry-owned bundle, and a concrete admission receipt binds the
exact frame ordinal, digest, receive time, source allocation, and one-way health lease.

Full/closed queues, byte-accounting failure, writer append/flush/shutdown failure, rotation
failure, control drop, and task abortion degrade the exact capture allocation. The writer owns a
degradation capability and can never create or restore source authority. MSJ1 is diagnostic audit
storage and cannot reconstruct a raw current frame, capture receipt, or live capability.

Configuration precedence is implemented as safe defaults, optional TOML file, supplied
`MARKET_SQUAWK_*` environment, then CLI overrides. Local secret references and redacted/zeroized
secret values exist, but OS keyring and authenticated-encrypted fallback providers are not yet
implemented.

## Transactional live processing

`market-squawk-live` normalizes provider decimal lexemes exactly through instrument tick/lot
definitions and rejects rounding, negative values, invalid delete semantics, and range overflow.
Its bounded price-level book implements snapshot initialization, incremental absolute updates,
delete-on-zero, configured depth, strict order, extrema, crossed-book detection, message-atomic
rollback, and last-good-state preservation.

Sequence handling covers supported/unsupported rules, exact progression, duplicates, gaps,
regression, snapshot applicability, and generation reset. The checksum engine has a closed
provider-profile dispatch and a tested Kraken WebSocket v2 CRC32 canonicalization over exact
lexemes and top-N scope. Typed shared trading status participates in qualification and authority
revisions.

Qualification derives `DirectVerified` only from complete exact evidence: current source and
capture allocations, authorized direct delivery, declared coverage, connection generation,
sequence/snapshot/checksum policy, precision, book integrity, status, and independent freshness
limits. Domain assessments remain audit data. Only the process-local processor can mint the
non-Clone, non-Serde `LiveExecutionCapability`, and it binds exact one-way runtime, shard,
generation, status, state-revision, deadline, nonce, and evidence dimensions.

Task 8 currently performs no production strategy or execution. The actor applies and revalidates
current state at the future feature boundary and again at the future strategy/issuer boundary,
then records `NoStrategy` without minting an unused capability.

## Deterministic shard runtime

Routing V1 hashes the explicit `MSQKSHARD` byte domain, version tag, big-endian venue-byte length,
venue UTF-8 bytes, and UUID network bytes with fixed FNV-1a constants. The frozen Coinbase vector
hashes to `0x28edee9cb1852659` and routes to shard 9 of 16. Golden tests lock full hashes and shard
indices across counts, Unicode and delimiter cases, and serialization round trips.

Each actor is the single writer for its configured routes and owns processors, generation
registries, order books, revisions, snapshot construction, and future strategy-local state.
Production ingress has two stages:

1. bounded/cancellable pre-feed registration binds an exact current source lease and route; and
2. `BoundShardIngress::try_publish` performs nonblocking count and byte admission for an exact
   `CurrentDecodedProviderBatch`.

No unbound publish method exists. Private retained-size calculation charges the batch, generation
admission, command, and shared allocation. Count-full, byte-full, overweight, checked-cost,
wrong-route, source-transplant, closed, and stale-authority failures invalidate the exact bound
generation before returning.

Startup validates all capacities/routes and a conservative peak-memory model before allocating.
Every actor publishes an initial immutable `Ready` snapshot before the runtime escapes. Partial
startup is aborted and awaited. Shutdown invalidates authority before draining; a deadline causes
abort-and-await, never detach. Replacement starts a fresh incarnation only after complete former
shutdown.

## Immutable snapshots and control-plane reads

Actors build bounded authority-free snapshots from one committed owner state after the action
decision. DTOs include routing version/count, runtime incarnation, shard identity, lifecycle,
snapshot revision, exact timestamps, stream and status revisions, generation/health information,
book depths, provenance, and dimension-specific completeness.

Crate-private `ArcSwap` cells publish complete generations without reader backpressure. A
single-shard lease charges one reader permit; an aggregate lease charges one per retained shard
generation and returns a sorted per-shard revision vector instead of a fabricated global `as_of`.
Slow readers may exhaust the configured retention budget but cannot block publication. Keyed
snapshot notifications are separate bounded coalescing hints; health events are bounded
best-effort diagnostics and never restore authority.

## Application boundary, CLI, replay, and MCP

`LiveRuntimeComposition` owns the production runtime and exposes checked startup, exact pre-feed
binding, bounded snapshot readers, incarnation and memory metrics, health/notification polling,
clean replacement, and explicit complete shutdown. It does not expose actor senders, snapshot
cells, authority issuers, unbound publication, or event conversion.

The previous public `Engine` is now `DiagnosticEngine`. Its domain module is private and exported
only through explicit `Diagnostic*` aliases. The compatibility Coinbase/mock path, replay, five
existing MCP tools, and historical paper calculation remain runnable but cannot accept a current
batch or mint production authority. CLI/MCP wording identifies this as diagnostic simulation.

The deletion trigger is precise: production Task 11 adapters must emit receipt-validated current
batches after pre-feed binding, and Task 13 services must consume Task 8 immutable snapshots. The
diagnostic engine is then removed rather than promoted.

The CLI still implements only `init`, diagnostic `mock`, diagnostic `capture`, diagnostic/offline
`mcp`, and `replay`. The MCP server remains local stdio with bounded lines, typed input schemas,
unknown-field rejection, and a local rate limiter, but it does not implement the complete required
tool domains, durable audit, controlled large artifacts, or cancellation/deadline service layer.

## Research, analytics, modeling, portfolio, execution, and valuation

These complete-release planes are not yet implemented:

- SQLite catalog and migrations;
- Arrow exchange, Parquet datasets/manifests, compaction, lineage, and DataFusion queries;
- file, SEC, FRED/ALFRED, BLS, Treasury, and portfolio extraction adapters;
- point-in-time joins, vintage/revision storage, corporate-action policy, and dataset builder;
- production online features beyond the quarantined compatibility kernels;
- batch analytics, feature registry, model bundles, native/ONNX inference, and backtesting;
- portfolio accounting, reconciliation, performance, exposure, attribution, and scenarios;
- typed strategies, comprehensive risk, realistic paper execution, and execution adapters; and
- ASC 820/IFRS 13 valuation evidence, classification, override, and approval services.

The domain contracts already prevent fair-value hierarchy, market depth, data quality, and live
execution authority from being substituted for one another. Later implementations consume those
contracts; their existence alone is not counted as the production capability.

## Verification state

The exact combined Task 8 live/application gate passed at the deterministic test head:

```text
cargo fmt --all --check                                                        PASS
cargo clippy -p market-squawk-live -p market-squawk \
  --all-targets --all-features --locked -- -D warnings                        PASS
cargo test -p market-squawk-live -p market-squawk --all-features --locked      PASS
cargo build -p market-squawk-live -p market-squawk \
  --all-features --release --locked                                             PASS
git diff --check                                                               PASS
```

The live suite includes 74 unit tests plus compile-fail authority privacy, price-level book,
property, Kraken checksum, conversion, real-registry overflow, sequence, 15 public sharding/config,
and state-machine integration tests. The focused application suite also covers diagnostic
quarantine and real runtime startup, metrics, bounded reads/notifications/health, shutdown,
replacement, and typed configuration failure.

The root integration owner generates and reviews the one authoritative merged `Cargo.lock` and
runs the full required workspace fmt, strict Clippy, test, and release-build gate at the Q2
checkpoint. This Task 8 worktree does not mark Q2 complete before that integration and review.

There are no final fuzz targets, Criterion benchmarks, sustained-burst harness, dependency/license
audit gate, SBOM, or complete release-hardening report yet. No 100,000 events/s or sub-millisecond
p99 claim is made before Task 14 measures the integrated pipeline on documented hardware.

## Security and access boundary

The application remains local-first with no mandatory paid software/API, cloud, external database,
container runtime, telemetry, or OpenTelemetry dependency. Outbound source connections are explicit;
there is no analytics beacon.

Identity/account rotation to evade limits, TLS/browser fingerprint concealment, CAPTCHA or
anti-bot bypass, proxy rotation intended to defeat blocking, and distributed quota evasion are
classified as unsafe and are absent from production schemas and implementation. Provider access is
designed around declared identity, authorization, shared budgets, bounded retry, caching in the
future research plane, explicit coverage, and fail-closed source health.

## Current conclusion

The cross-cutting live contracts that later features depend on are now real and tested: typed
identity and exact finance, current-source/capture authority, transactional qualification,
deterministic ownership, bounded admission, immutable snapshots, and supervised lifecycle. The
next work should attach features, strategy/risk, and real adapters to these seams without reopening
the authority boundary or routing diagnostic/replay values into production.
