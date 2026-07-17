# Market Squawk current state

<!-- q2-checkpoint-state
candidate-id: q2-integrated-remediation-2026-07-16
audit-anchor: 651a01e120dfe27a598b9475296733d238d870b7
review-target: repository-head
lifecycle: remediation-in-progress
prior-r01-r15: closed-as-framed
active-findings: Q2-I01,Q2-I02,Q2-I03,Q2-I04,Q2-I05,Q2-I06,Q2-I07,Q2-I08,Q2-I09,Q2-I10,Q2-I11,Q2-M01,Q2-M02
-->

## Document control

- Audit date: 2026-07-16
- Repository: `market-squawk`
- Current implementation audit anchor: `651a01e120dfe27a598b9475296733d238d870b7`
- Checkpoint disposition: rejected; integrated remediation and exact-head re-review required
- Evidence: repository inspection, locked local verification,
  [Q2 live-readiness audit](../plans/q2-live-readiness-audit.md),
  [Q2 Task 8 implementation report](../reports/q2-task8-implementation.md),
  [Q2 checkpoint rejection ledger](../reports/q2-checkpoint-review.md),
  [integrated Q2 remediation plan](../superpowers/plans/2026-07-16-q2-integrated-checkpoint-remediation.md),
  and
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

The exact `581d4fd` checkpoint was rejected. Q2-R01–R15 are closed as framed at `651a01e`, and the
complete local gate and audits passed at that clean replacement commit. Three fresh independent
reviewers then rejected `651a01e` for the adjacent defects below; passing the earlier gate does not
override missing production contracts:

- Q2-I01 terminal health-epoch exhaustion;
- Q2-I02 registry-owned canonical provider/account budget identity;
- Q2-I03 restart-durable conservative budget enforcement;
- Q2-I04 registry-sealed raw-frame receipt time;
- Q2-I05 trusted wall high-water and rollback discontinuity;
- Q2-I06 complete snapshot/delta processing peak memory;
- Q2-I07 snapshot reader/publication/generation metadata memory;
- Q2-I08 complete capture frame/session/generation retained memory;
- Q2-I09 bounded application source shutdown;
- Q2-I10 allocation-bounded MCP framing;
- Q2-I11 coherent authoritative checkpoint documents;
- Q2-M01 canonical budget-state serialization; and
- Q2-M02 unambiguous diagnostic CLI/MCP/README terminology.

The current candidate lifecycle is `remediation-in-progress`. The three implementation lanes and
root documentation lane start from plan commit `de101ee`; none of their in-flight behavior is
counted as implemented here. Q2 remains rejected until those commits are integrated, the worktree
is frozen and clean, every local gate/audit passes at exact `HEAD`, and three fresh specialist
re-reviews report no unresolved severity.

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

At the current audit anchor, adapter-authored raw receive time, absence of a registry wall
high-water, non-terminal health-epoch exhaustion, aliasable provider/account quota identity, and
process-restart budget reset remain Q2-I01–Q2-I05 blockers. The types and focused tests are useful,
but the source authority subsystem is not checkpoint-approved until those adjacent contracts are
integrated and re-reviewed.

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
by count and its current declared retained-byte estimate; admission success is independent of disk
completion. Q2-I08 records that capacity-heavy session identities and uniquely retained capture
generations are not yet fully charged, so the byte figure is not accepted as a complete heap bound.
Capture
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

Startup validates all capacities/routes and the currently implemented peak-memory model before
allocating. Q2-I06 and Q2-I07 show that processing overlap and snapshot reader/publication metadata
are missing from that estimate, so it is not yet an accepted complete runtime ceiling.
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
batch or mint production authority. Internal types identify the boundary, but Q2-M02 remains open
because public CLI/MCP wording does not yet identify it consistently.

The deletion trigger is precise: production Task 11 adapters must emit receipt-validated current
batches after pre-feed binding, and Task 13 services must consume Task 8 immutable snapshots. The
diagnostic engine is then removed rather than promoted.

The CLI still implements only `init`, diagnostic `mock`, diagnostic `capture`, diagnostic/offline
`mcp`, and `replay`. The compatibility plane is authority-free, paper-only, and based on Coinbase
Exchange single-venue partial coverage. Its app-local `QualityState::Valid` is not canonical
`DataQuality::DirectVerified` and can never authorize production action. Q2-M02 is active because
some public descriptions do not yet state that boundary consistently.

The MCP server remains local stdio with typed input schemas, unknown-field rejection, and a local
rate limiter. Q2-I10 records that its current 1 MiB check occurs after `read_line` allocation, so
request framing is not memory-bounded until the incremental reader is integrated. Complete tool
domains, durable audit, controlled large artifacts, and service cancellation/deadlines remain
later-stage work. Q2-I09 separately blocks the claim that application source shutdown is bounded.

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

Exact commit `651a01e120dfe27a598b9475296733d238d870b7` passed the complete local verification
wrapper and additional audits from a clean, unchanged worktree:

```text
cargo fmt --all --check                                                        PASS
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings PASS
cargo test --workspace --all-features --locked                                PASS
cargo build --workspace --all-features --release --locked                     PASS
cargo deny check                                                              PASS
cargo audit --deny warnings                                                   PASS
gitleaks dir and git-history scans                                             PASS
git diff --check                                                               PASS
```

That evidence establishes tested behavior at `651a01e`, not Q2 approval. The three-reviewer
checkpoint still rejected the exact commit for Q2-I01–Q2-I11 and Q2-M01–Q2-M02. Focused lane gates
and the future integrated gate are recorded separately; no in-flight result is exact-head evidence.

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

The cross-cutting live foundation is substantial and tested, and Q2-R01–R15 remain closed as
framed. Q2 is nevertheless rejected while the adjacent terminality, identity, persistence, trusted
time, complete memory, process-boundary, and documentation contracts are under remediation. Q3
features, strategy/risk, and real adapters begin only after the new exact head passes re-review;
diagnostic/replay values remain permanently outside production authority.
