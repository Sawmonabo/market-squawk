# Market Squawk Complete Local Release Implementation Plan

<!-- q2-checkpoint-state
candidate-id: q2-integrated-remediation-2026-07-16
audit-anchor: 651a01e120dfe27a598b9475296733d238d870b7
review-target: repository-head
lifecycle: remediation-in-progress
prior-r01-r15: closed-as-framed
active-findings: Q2-I01,Q2-I02,Q2-I03,Q2-I04,Q2-I05,Q2-I06,Q2-I07,Q2-I08,Q2-I09,Q2-I10,Q2-I11,Q2-M01,Q2-M02
-->

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve the current single-package v0.1 into the complete hardened local Market Squawk
release described by the product specification without losing runnable behavior between stages.

**Architecture:** A contract-first virtual Rust workspace separates a deterministic, sharded live
execution plane from an Arrow/Parquet/DataFusion research plane while sharing invariant-preserving
domain types. CLI and local stdio MCP operate through bounded application services outside the
event-to-action path. Automated paper/live action requires the stateful live plane's opaque,
single-use current capability for a `DirectVerified` state; serialized domain assessments and
archives never authorize action.

**Tech Stack:** Rust 1.97.0, Edition 2024, Cargo resolver 3, Tokio, Serde, Reqwest,
Tokio-Tungstenite, Arrow, Parquet, DataFusion, SQLite, Clap, Tracing, Thiserror, Anyhow at app
boundaries, Proptest, Criterion, Cargo-fuzz, Python outside the live path, and conditional
ONNX-compatible local inference.

## Global Constraints

- No paid software, paid API, cloud service, external database service, mandatory container
  runtime, or mandatory telemetry infrastructure.
- No OpenTelemetry infrastructure in version 1.
- No identity/account rotation to evade limits, browser/TLS fingerprint spoofing, CAPTCHA bypass,
  proxy rotation intended to defeat blocking, or distributed quota evasion.
- Rust 1.97.0 stable, Edition 2024, resolver 3, committed `Cargo.lock`.
- Every workspace package inherits workspace package metadata and lint policy.
- No empty crates, unconsumed interfaces, or synthetic sources represented as production.
- No unsafe Rust.
- No production `unwrap`, `expect`, `panic!`, `todo!`, or `unimplemented!`.
- `anyhow` is restricted to application boundaries; libraries expose typed `thiserror` errors.
- Live financial values use scaled integers with checked conversion and arithmetic.
- Accounting and analytical values use Decimal or Arrow Decimal128 with explicit currency/scale.
- Fair-value hierarchy, market depth, data quality, stream integrity, and execution eligibility are
  separate types with no implicit conversion.
- Only the stateful live authority issuer can mint the short-lived capability for current
  `DirectVerified` state that risk requires and consumes by default.
- SQLite, DataFusion, Parquet, Python, MCP, LLMs, and arbitrary filesystem work remain outside the
  event-to-action path.
- All queues are bounded; saturation has an explicit quarantine/degradation policy.
- Strategies, models, adapters, CLI, and MCP cannot bypass risk.
- External network tests are opt-in and separate from the deterministic default suite.
- Performance claims require measurements on documented hardware.
- Existing user changes are preserved and never swept into unrelated commits.

---

## Plan set and execution order

The platform contains independent subsystems. Each stage is executed from a code-level plan written
with `superpowers:writing-plans`, reviewed before execution, and closed before the next stage begins.
The first code-level plan is:

- [`2026-07-16-market-squawk-stage-1-foundation.md`](../superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md)

Subsequent plan artifacts have fixed names and are written after the preceding stage fixes the
actual public APIs they consume:

```text
docs/superpowers/plans/2026-07-16-market-squawk-stage-2-live-plane.md
docs/superpowers/plans/2026-07-16-market-squawk-stage-3-research-plane.md
docs/superpowers/plans/2026-07-16-market-squawk-stage-4-analytics-modeling-portfolio.md
docs/superpowers/plans/2026-07-16-market-squawk-stage-5-risk-paper-execution.md
docs/superpowers/plans/2026-07-16-market-squawk-stage-6-valuation-mcp.md
docs/superpowers/plans/2026-07-16-market-squawk-stage-7-release-hardening.md
```

Writing a later plan is not a scope decision. Every deliverable and exit gate below is mandatory for
the complete local release. The separate plan gate prevents a later implementer from guessing file
paths or signatures that an earlier stage changed.

## Specification traceability

| Specification section | Primary closure stage(s) | Evidence gate |
| --- | --- | --- |
| 1-2 Product and zero-mandatory-cost constraints | 1-7 | Every stage gate plus final local demonstration |
| 3 Data classifications and eligibility | 1, 2, 6 | Type/privacy tests, qualification fixtures, fair-value no-promotion tests |
| 4 Architecture and hot-path exclusions | 1, 3, 7 | Boundary checker, saturation tests, dependency graph, latency harness |
| 5-6 Rust baseline and engineering conventions | 1, then all | Rust 1.97 locked fmt/Clippy/test/build, rustdoc, property tests |
| 7 Source framework and functional adapters | 1-3, 5 | Source contract tests, official/local fixtures, opt-in network smoke tests |
| 8 Canonical domain and provenance | 1, 3, 4 | Constructor properties, identifier resolution, PIT/revision tests |
| 9 Live processing, sharding, integrity, books | 1-2 | Golden sequence/checksum/book/generation/overflow fixtures |
| 10 Research storage and datasets | 3 | Migrations, manifests, idempotency, compaction, PIT tests |
| 11 Features and analytics | 2, 4 | Numeric golden/property tests and live/batch kernel comparisons where shared |
| 12 Modeling, inference, and backtesting | 4 | Bundle/hash/schema/leakage/failure/backtest tests |
| 13 Strategies, risk, and execution | 1, 5 | Approval compile-fail tests, risk matrix, state/reconciliation properties |
| 14 Portfolio system | 3-4 | Raw import, lot/cash/position reconciliation, analytics fixtures |
| 15 Fair-value analysis | 6 | Decision tables, evidence/ruleset/override audit, no-silent-Level-1 tests |
| 16 MCP | 1, 6 | SDK protocol/schema/lifecycle/cancellation/limit/prohibited-surface tests |
| 17 CLI | 1, 3-6 | Command snapshots and end-to-end service integration tests |
| 18 Project structure | 1-6 | Workspace-boundary checker and no-empty-crate audit |
| 19 Configuration, privacy, operations | 1, 3, 5, 7 | Precedence, keyring/encryption, allowlist, redaction, threat-model tests |
| 20 Verification | Every stage, completed in 7 | Required commands, audits, properties, fuzz targets, network separation |
| 21 Performance acceptance | 7 | Documented hardware/fixture/throughput/latency/memory report |
| 22 Delivery order | 0-7 | Stage evidence documents and runnable commits |
| 23 Definition of done | 2-7 | Final all-domain local demonstration and release gate |
| Prohibited evasion additions | Permanently excluded | Schema/config scan, provider-budget tests, threat model |

## Commit and review policy

Each task in a stage plan ends in a focused commit after its tests pass. Before committing:

1. Inspect `git status --short` and `git diff --check`.
2. Stage only paths named by the task.
3. Run the task-focused test.
4. Run the affected crate tests.
5. Preserve user-owned unrelated changes.

Each stage ends with a separate release-gate commit that updates documentation and verification
evidence. No stage is marked complete from a partial test run.

## Q2 Tasks 1-8 implementation progress

The live foundation through Task 8 is integrated. Q2-R01–R15 are closed as framed at `651a01e`,
and that exact clean commit passed the full local verification wrapper and audits. Three fresh
specialist reviewers rejected the same commit for the adjacent Q2-I01–Q2-I11 and Q2-M01–Q2-M02
contracts. The checkpoint lifecycle is `remediation-in-progress`; the green gate is evidence, not
approval.

The controlling design and subagent-driven TDD plan are:

- [integrated Q2 remediation design](../superpowers/specs/2026-07-16-q2-integrated-checkpoint-remediation-design.md)
- [integrated Q2 remediation plan](../superpowers/plans/2026-07-16-q2-integrated-checkpoint-remediation.md)

Three isolated workers own source authority/persistence/capture memory, live processing/snapshot
memory, and app framing/shutdown/terminology. Root owns checkpoint coherence, integration, exact-
head verification, and three-scope re-review. Their in-flight changes are not counted below.

| Active finding | Planned production contract |
| --- | --- |
| Q2-I01 | Terminal health-epoch revocation and replacement-only recovery |
| Q2-I02 | Registry-owned canonical endpoint/authorization budget identity |
| Q2-I03 | Restart-durable conservative provider-budget enforcement |
| Q2-I04 | Registry-sealed paired raw-frame receipt time |
| Q2-I05 | Trusted wall high-water and discontinuity latch |
| Q2-I06 | Closed structural snapshot/delta processing peak |
| Q2-I07 | Complete snapshot reader/publication/generation accounting |
| Q2-I08 | Complete capture frame/session/generation/bundle accounting |
| Q2-I09 | Configured cancellation/deadline/abort-and-await source shutdown |
| Q2-I10 | Allocation-bounded incremental MCP framing |
| Q2-I11 | Machine-checked checkpoint document coherence |
| Q2-M01 | Canonically ordered persistent budget state |
| Q2-M02 | Unambiguous diagnostic/authority-free/partial-coverage wording |

Working Task 8 deliverables are:

- [x] frozen routing V1 byte contract, full hash vectors, and deterministic shard indices;
- [x] checked runtime/route/snapshot configuration and conservative peak-memory ceiling;
- [x] exact pre-feed current-generation binding with no unbound publish surface;
- [x] nonblocking count, byte, and per-message admission with invalidation-before-return;
- [x] deterministic single-writer shard actors with transactional Task 7 processing and explicit
  feature/strategy revalidation seams;
- [x] complete startup readiness, terminal invalidation, bounded shutdown, abort-and-await, and
  clean runtime replacement;
- [x] crate-private `ArcSwap` publication, per-shard retained-reader accounting, sorted aggregate
  revisions, and bounded coalesced notifications; and
- [x] application `LiveRuntimeComposition` plus explicit `DiagnosticEngine` quarantine and deletion
  trigger.

The following live-quarter work remains mandatory and is not claimed by Task 8:

- [ ] Task 9 actor-owned online feature state;
- [ ] Task 10 typed strategy, capability issue/consume, comprehensive risk, and dispatch;
- [ ] Task 11 production Coinbase/Kraken current-batch adapters;
- [ ] Task 12 live fuzz and benchmark harnesses;
- [ ] Task 13 bounded shared CLI/MCP live services and diagnostic-engine deletion; and
- [ ] Task 14 integrated performance, security, and release-hardening evidence.

## Stage 0: Planning and verified rename baseline

**Produces:** Reviewed architecture/gap/plan documents and a clean, compatible Market Squawk rename
baseline before workspace movement.

### Task 0.1: Approve audit and target contracts

**Files:**

- Review: `docs/architecture/current-state.md`
- Review: `docs/architecture/target-state.md`
- Review: `docs/plans/gap-analysis.md`
- Review: `docs/plans/implementation-plan.md`
- Review: `docs/research/2026-07-15-market-squawk/final-report.md`

- [ ] Confirm every gap is assigned to a stage.
- [ ] Confirm the prohibited evasion requests remain `Unsafe` and excluded.
- [ ] Confirm `MEJ1/.mej` and `MSJ1/.msj` backward read compatibility.
- [ ] Confirm current CLI behavior remains compatibility-tested.
- [ ] Commit only reviewed planning/research artifacts.

### Task 0.2: Complete the rename as a focused baseline

**Files:**

- Create: `scripts/check_brand.py`
- Modify: current uncommitted rename paths shown by `git status --short`
- Test: `tests/journal.rs`
- Test: `scripts/smoke_mcp.py`
- Modify: `scripts/verify.sh`

- [ ] Add a deterministic tracked-file brand checker that permits documented legacy journal magic
  only in compatibility code and fixtures.
- [ ] Add `MEJ1` and `MSJ1` reader fixtures before changing journal code.
- [ ] Run the reader tests and observe the legacy fixture fail.
- [ ] Implement dual-format read detection while retaining one documented write format.
- [ ] Run all 24+ baseline tests and the MCP/mock/local-WebSocket smoke tests.
- [ ] Run `./scripts/verify.sh` and require exit code zero.
- [ ] Commit the rename separately from the workspace migration.

## Stage 1: Rust baseline, domain contracts, and project boundaries

**Code-level plan:**
[`2026-07-16-market-squawk-stage-1-foundation.md`](../superpowers/plans/2026-07-16-market-squawk-stage-1-foundation.md)

**Produces:** Rust 1.97 virtual workspace; invariant-preserving domain; source metadata and live/
extraction contracts; non-blocking raw-capture handoff; deterministic shards; immutable snapshots;
unforgeable risk approval; migrated Coinbase, paper, CLI, and MCP v0.1 behavior; green release gate.

### Mandatory deliverables

- [x] Virtual workspace with resolver 3 and inherited metadata/lints.
- [ ] Non-empty initial crates and adapters with an acyclic dependency graph.
- [x] Private validated identity, financial, time, quality, coverage, and provenance types.
- [x] Complete `MarketEvent`/`ResearchObservation` contracts needed by later stages.
- [x] Separate `FairValueHierarchy`, `MarketDepth`, `DataQuality`, `StreamIntegrityState`,
  `CaptureIntegrityState`, audit-only `QualificationAssessment`, and `ExecutionEligibility`.
- [x] Domain assessments expose `AssessmentStatus`/`EligibilityFailures` audit diagnostics but no
  execution-eligibility API; archive provenance is unit `Ineligible` and cannot construct,
  deserialize, clone, or substitute for live current authority.
- [x] Live provenance carries explicit validated `available_at`, a durable assessment reference
  rather than a full assessment, and no serialized execution authority.
- [x] Live provenance owns one complete `LiveEvidenceBinding`; its archive record state is derived,
  while a caller-supplied archival classification and opaque assessment reference are retained
  as audit assertions. Provenance does not dereference or prove
  the external assessment relationship.
- [x] `SourceMetadataProvider`, `LiveMarketSource`, and `ExtractionSource` contracts.
- [x] Explicit endpoint/network policy and provider budget contracts.
- [x] Raw capture no longer waits for writer acknowledgement.
- [x] Platform capture consumes one registry-issued generic authority bundle through domain traits;
  `platform -> sources`, loose capability composition, receipt erasure, and diagnostic-receipt
  promotion are forbidden.
- [x] Capture/shard overflow tests prove fail-closed quarantine behavior.
- [x] Versioned stable shard routing and single-writer ownership.
- [x] Immutable bounded snapshots for application services.
- [x] Stateful live authority issuer and opaque non-Serde/non-`Clone`, single-use, policy-expiring
  `LiveExecutionCapability` bound to authoritative metadata/session/instrument state.
- [ ] `OrderIntent`, capability-consuming risk service, publicly nameable but privately
  constructible `ApprovedOrder`, one-time dispatcher, and `ExecutionAdapter` contract.
- [ ] Current features and paper behavior migrated behind new contracts without being mislabeled
  complete paper execution.
- [x] Existing CLI/MCP compatibility behavior remains runnable and is structurally quarantined from
  the production live runtime; full shared services remain Task 13.
- [ ] Current Coinbase path explicitly capped at `DirectUnverified` until Stage 2 qualification.
- [ ] Required Rust 1.97 locked workspace commands and Stage 1 security checks pass.

### Stage 1 exit gate

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_brand.py
python3 scripts/check_generated_artifacts.py
gitleaks dir --redact --no-banner .
cargo deny check
```

Expected: every command exits zero; tests prove legacy journal reads, type separation, exact
financial conversion, stable sharding, overflow quarantine, snapshot isolation, opaque-capability
binding/expiry/one-time use, and risk/dispatch non-bypassability.

## Stage 2: Live adapters, qualification, books, sharding, and source health

**Produces:** Production Coinbase and Kraken live adapters; complete source health and
qualification; price-level books with venue integrity; all specified online features; cross-venue
comparison; measured live kernels.

### Task 2.1: Write the Stage 2 code-level plan

**Consumes:** Stage 1 domain/source/live/analytics interfaces.

- [ ] Inspect the exact Stage 1 public API and test fixture locations.
- [ ] Write `docs/superpowers/plans/2026-07-16-market-squawk-stage-2-live-plane.md` with code,
  focused commands, expected failures, and commits.
- [ ] Map every Stage 2 deliverable below to a task before implementation.
- [ ] Review the plan for source-specific qualification and no-evasion behavior.

### Mandatory deliverables

- [ ] Coinbase channel-specific decoder and qualification state machines.
- [ ] Coinbase full-channel snapshot/sequence replay where selected by the approved design.
- [ ] Coinbase Level 2 remains `DirectUnverified` when update-level evidence is insufficient.
- [ ] Coinbase trades, status/precision, connection generations, reconnect, and resynchronization.
- [ ] Kraken WebSocket v2 price-level adapter.
- [ ] Kraken message-atomic update application, subscribed-depth truncation, delete-on-zero, exact
  decimal-string checksum view, canonical order, and top-ten CRC32.
- [ ] Kraken mismatch quarantine, reconnect, fresh snapshot, and requalification.
- [ ] Typed source coverage and health snapshots for both venues.
- [ ] Kraken current-authority issuance/revocation integrates the authoritative metadata revision,
  session generation, health, and instrument state; Coinbase Level 2 never receives a capability.
- [ ] Stable multi-shard supervisor with bounded queues and controlled shutdown.
- [ ] Top-of-book and configurable price-level depth; order-level contract where supplied later.
- [ ] Spread, midpoint, microprice, book/depth imbalance, order-flow imbalance, depth-weighted price,
  aggressor imbalance, rolling VWAP, volume velocity, momentum, returns, volatility, cross-venue
  divergence, liquidity, and slippage kernels.
- [ ] Property tests for book ordering, delete-zero, message atomicity, checksum, sequence,
  generations, and requalification.
- [ ] Fuzz targets for Coinbase/Kraken decoders and capture records.
- [ ] Separately gated public endpoint smoke tests.
- [ ] Criterion/live-kernel benchmarks without claiming final event-to-decision performance.

### Stage 2 exit gate

- [ ] Required workspace commands pass.
- [ ] Recorded official checksum and sequence fixtures pass.
- [ ] Queue saturation never produces an executable event.
- [ ] Coinbase and Kraken coverage/quality are visible through bounded services.
- [ ] No synthetic source appears in production source registration.

## Stage 3: Research ingestion, catalog, Arrow, Parquet, and DataFusion

**Produces:** Working local research plane with required file, SEC, macro, Treasury, and portfolio
extraction; SQLite catalog; versioned Arrow/Parquet datasets; bounded DataFusion queries; manifests,
lineage, revisions, and compaction.

### Task 3.1: Write the Stage 3 code-level plan

- [ ] Consume the exact Stage 1 extraction/provenance contracts and Stage 2 instrument/source
  registries.
- [ ] Write `docs/superpowers/plans/2026-07-16-market-squawk-stage-3-research-plane.md`.
- [ ] Include real adapter fixtures, schema definitions, SQL migrations, manifest JSON examples,
  commands, expected failures, and focused commits.

### Mandatory deliverables

- [ ] SQLite migration runner with foreign keys, strict tables, transactions, busy policy,
  integrity checks, backup, and versioned migrations.
- [ ] Source configuration, health, cursors, manifests, registries, run state, audit, and artifacts.
- [ ] Arrow schema registry and canonical Decimal128/time/provenance fields.
- [ ] Atomic Parquet dataset writer and content-addressed manifest publication.
- [ ] Idempotency, deduplication, partition policy, compaction, small-file limit, and lineage.
- [ ] Bounded DataFusion session/service and read-only local CLI query.
- [ ] CSV/TSV with explicit schema, delimiter, encoding, size, and row-error policy.
- [ ] JSON/NDJSON with strict schemas and bounded nesting/record sizes.
- [ ] XML and Excel extraction with entity/macro/formula safety, bounded sheets/cells, explicit
  schemas, and raw-source hashes.
- [ ] SQLite and database-export extraction through read-only snapshots with allowlisted schemas;
  no live-path or MCP arbitrary SQL exposure.
- [ ] OFX/QFX and supported broker-export extraction with raw-record preservation, account/currency
  validation, duplicate detection, and reconciliation.
- [ ] Parquet import with schema/version/provenance validation.
- [ ] SEC declared user agent, shared 10 rps ceiling, bulk initialization/reconciliation,
  submissions, Company Facts, payload hashes, and unknown availability semantics.
- [ ] FRED/ALFRED key handling, explicit real-time parameters, vintages, revisions, pagination,
  caching, and HTTP 429 source health.
- [ ] BLS v1/v2 deterministic chunking, published quotas, preliminary indicators, and honest vintage
  limitations.
- [ ] Treasury Fiscal Data pagination and official rate-file ingestion with schema/change tracking.
- [ ] Portfolio raw-record preservation, holdings/transactions normalization, and supplied-total
  reconciliation inputs.
- [ ] OS keyring secret storage where available plus an authenticated-encrypted local fallback with
  a user-supplied unlock secret that is never stored beside the ciphertext; redaction and rotation
  tests cover both backends.
- [ ] Required core dataset schemas and manifests.
- [ ] PIT filtering and revision-preservation tests.
- [ ] Parser/schema fuzz targets and opt-in external-network tests.

### Stage 3 exit gate

- [ ] Re-running each ingestion fixture produces no duplicate logical observations.
- [ ] Dataset readers resolve manifests rather than directory listings.
- [ ] Compaction preserves rows, hashes/lineage, PIT results, and revisions.
- [ ] SQLite/DataFusion/Parquet are absent from live crate dependencies and runtime hot-path tests.
- [ ] Required workspace/security commands pass.

## Stage 4: Analytics, point-in-time datasets, modeling, backtesting, and portfolios

**Produces:** Complete batch analytics; feature registry; reproducible PIT dataset builder; model
bundles and local inference; research backtesting; portfolio accounting, performance, exposures,
attribution, risk, and scenarios.

### Task 4.1: Write the Stage 4 code-level plan

- [ ] Consume actual Stage 3 dataset schemas/manifests and Stage 2 pure live kernels.
- [ ] Write
  `docs/superpowers/plans/2026-07-16-market-squawk-stage-4-analytics-modeling-portfolio.md`.
- [ ] Include numeric fixtures, property tolerances, time/leakage tests, bundle examples, inference
  failure tests, portfolio reconciliation examples, and commits.

### Mandatory deliverables

- [ ] Versioned feature registry with complete metadata and implementation hashes.
- [ ] Price/total returns, volatility, drawdown, correlation, beta, alpha, Sharpe, Sortino,
  tracking error, information ratio, VaR, coherent discrete Expected Shortfall, and factors.
- [ ] Fundamentals, valuation multiples, FCF, earnings/macro surprises, and yield-curve features.
- [ ] Portfolio exposure, attribution, rebalancing, scenario, and stress analytics.
- [ ] Instrument universe as-of selection, historical constituents, delistings, mergers, rolls, and
  corporate-action policy.
- [ ] PIT joins based on evidenced availability, revision preservation, label cutoffs,
  chronological splits, missing-value policies, and future-perturbation leakage tests.
- [ ] Reproducible versioned Parquet feature/label datasets.
- [ ] Model registry and fully hashed model bundle validation.
- [ ] Native Rust inference backend.
- [ ] Conditional ONNX-compatible backend with verified runtime/artifact provenance, operator and
  schema validation, size bounds, warm-up, threading policy, and fail-closed fallback.
- [ ] Python training/research package outside the live dependency graph.
- [ ] Backtester over research datasets with fees, corporate actions, timing, and no implicit live
  replay requirement.
- [ ] Trial inventory and backtest-selection-overfitting diagnostics.
- [ ] Accounts, positions, transactions, cash flows, cost basis/lots, gains, income, allocation,
  performance, exposures, attribution, rebalancing, and reconciliation.

### Stage 4 exit gate

- [ ] Future data perturbations cannot change earlier PIT features.
- [ ] Delisted/historical-universe fixtures remain represented.
- [ ] Expected Shortfall ties/point masses/weighted quantile atoms pass.
- [ ] Model hash/schema/operator/intended-use mismatches fail closed.
- [ ] Portfolio positions/cash/totals reconcile or produce typed discrepancies.
- [ ] Required workspace/security commands pass.

## Stage 5: Strategies, comprehensive risk, and realistic paper execution

**Produces:** Typed strategies, non-bypassable comprehensive risk, realistic paper adapter,
order-state lifecycle, balances/positions/fills, cancel/reconcile, and durable execution audit.

### Task 5.1: Write the Stage 5 code-level plan

- [ ] Consume the actual Stage 1 approval boundary, Stage 2 live snapshots/features, and Stage 4
  portfolio/model services.
- [ ] Write
  `docs/superpowers/plans/2026-07-16-market-squawk-stage-5-risk-paper-execution.md`.
- [ ] Include order-state transition tables, risk matrices, fill fixtures, latency/slippage models,
  reconciliation examples, bypass compile-fail tests, and commits.

### Mandatory deliverables

- [ ] Canonical `Strategy` implementations consume validated live state and emit complete intents.
- [ ] Risk requires and consumes the live plane's opaque capability, revalidates action time/current
  generation/source health, and checks account/instrument, position, notional, exposure, leverage,
  capital, price, slippage, rate, duplicate, loss/drawdown, and expiration.
- [ ] Every decision has structured reasons, evaluated limits, source/model/strategy identity, and
  durable audit outside the live path.
- [ ] Only risk constructs `ApprovedOrder`; its expiry cannot outlive live evidence, and compile-fail
  tests prove domain assessments/archives cannot satisfy the capability parameter.
- [ ] Adapter dispatch consumes each approval ID exactly once and rechecks expiry/authority
  revocation before constructing an adapter-only dispatch value.
- [ ] Paper order state machine supports acceptance, rejection, partial/full fill, cancel,
  expiration, and reconciliation.
- [ ] Deterministic seeded latency, spread, depth, queue/slippage, impact, fee, and rejection models.
- [ ] Parameter provenance, scope, calibration, version, and `Modeled` quality.
- [ ] Cash, balances, positions, fees, realized/unrealized P&L, and reconciliation against portfolio
  services.
- [ ] Kill switches, duplicate/idempotency handling, restart recovery, and cancel races.
- [ ] Live execution remains disabled unless a separately authorized adapter and configuration are
  supplied; no live adapter is required for the first local release.

### Stage 5 exit gate

- [ ] No automated paper action occurs without a current, consumed live capability for
  `DirectVerified` state by default.
- [ ] No strategy/model/CLI/MCP/archive/replay/adapter can construct a capability, approved order,
  or adapter dispatch value.
- [ ] Quantity, cash, position, fee, and fill reconciliation invariants pass under partial fills and
  cancellations.
- [ ] Required workspace/security commands pass.

## Stage 6: Fair-value analysis and complete typed MCP

**Produces:** ASC 820/IFRS 13 evidence/classification services and the complete bounded local stdio
MCP surface over shared application services.

### Task 6.1: Write the Stage 6 code-level plan

- [ ] Consume the actual Stage 3 evidence/data services, Stage 4 analytics/portfolio/model services,
  and Stage 5 controlled bot/execution services.
- [ ] Write `docs/superpowers/plans/2026-07-16-market-squawk-stage-6-valuation-mcp.md`.
- [ ] Include fair-value decision tables, evidence fixtures, override/approval audit, MCP schemas,
  lifecycle/cancellation tests, result-limit fixtures, artifact references, and commits.

### Mandatory deliverables

- [ ] Valuation, input, method, classification, reason, evidence, ruleset, override, and approval
  storage/services.
- [ ] Deterministic Level 1 evidence rules for identity, quote, active/access market,
  measurement-date relevance, adjustment, source/venue, and freshness.
- [ ] Missing evidence yields `Unclassified`; delayed/stale/proxy/adjusted/modeled/similar values
  cannot silently qualify.
- [ ] Compile-time and runtime separation from market depth, data quality, and execution eligibility.
- [ ] Complete Source, Market, Research, Fundamental, Macro, Portfolio, Analysis, Model, FairValue,
  Bot, and Execution MCP domains.
- [ ] Versioned protocol lifecycle, initialization enforcement, capabilities, progress,
  cancellation/deadlines, typed schemas, bounded instruments/time/results, and audit.
- [ ] Controlled artifact directory and content-hashed references for large results.
- [ ] No shell, arbitrary path, unrestricted SQL, credentials, remote code, unchecked order, risk
  bypass, or audit deletion.
- [ ] CLI command hierarchy uses the same application services and retains compatibility aliases.

### Stage 6 exit gate

- [ ] Fair-value golden/property tests cover classification, no-silent-promotion, ruleset versions,
  override/approval, and type separation.
- [ ] MCP schema snapshots, lifecycle, cancellation races, bounds, artifacts, audit, and prohibited
  surfaces pass.
- [ ] Required workspace/security commands pass.

## Stage 7: Performance, fuzzing, security, and release hardening

**Produces:** Measured performance, complete fuzz suite, sustained-burst evidence, dependency and
license closure, SBOM/provenance, credential/artifact checks, release packaging, and final
definition-of-done demonstration.

### Task 7.1: Write the Stage 7 code-level plan

- [ ] Inventory the actual final workspace, binaries, features, native artifacts, datasets, and fuzz
  targets.
- [ ] Write
  `docs/superpowers/plans/2026-07-16-market-squawk-stage-7-release-hardening.md`.
- [ ] Include exact benchmark hardware protocol, commands, thresholds, fuzz corpora/durations,
  dependency policy, SBOM/provenance commands, release artifacts, and commits.

### Mandatory deliverables

- [ ] Criterion and end-to-end harnesses for decoder, sequence/checksum, queue, book, features,
  inference, strategy, risk, and event-to-decision.
- [ ] Arrow/Parquet ingestion/writing and DataFusion query benchmarks.
- [ ] Sustained burst and peak-memory measurement.
- [ ] Hardware, OS, Rust, fixture, event count, throughput, p50/p95/p99/max, and peak memory record.
- [ ] Demonstrated 100,000 synthetic events/s, warmed sub-ms p99 internal event-to-decision, bounded
  memory, and no analytical/database I/O in the live path—or an explicit failed acceptance report
  with remediation before release.
- [ ] Fuzz targets for live decoders, JSON/CSV, capture records, MCP requests, and schema inference.
- [ ] Dependency advisories, license/source bans, duplicate/native dependency review, and MSRV lock.
- [ ] Working-tree and history credential scans, generated-artifact checks, brand checks, and
  secret-redaction tests.
- [ ] Threat model, abuse cases, endpoint allowlist, secure defaults, and vulnerability response.
- [ ] SBOM, source/build provenance, hashes, reproducible instructions, and signed release process
  where locally available without making hosted infrastructure mandatory.
- [ ] Final CLI/MCP/source/research/portfolio/model/execution/valuation demonstration script.
- [ ] README, SECURITY, CONTRIBUTING, CHANGELOG, operations, schemas, and benchmark reports updated.

### Final release gate

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
cargo deny check
cargo audit --deny warnings
python3 scripts/check_brand.py
python3 scripts/check_generated_artifacts.py
python3 scripts/check_workspace_boundaries.py
gitleaks dir --redact --no-banner .
```

Additionally:

- [ ] Every fuzz target completes its release smoke duration without a crash.
- [ ] Benchmarks meet acceptance on documented hardware.
- [ ] External adapters pass separately gated smoke tests or are marked unavailable with honest
  source health, never falsely verified.
- [ ] The local end-to-end demonstration covers every definition-of-done capability.
- [ ] No paid, cloud, remote-database, container, telemetry, or evasion dependency is required.

## Completion rule

This controlling plan is complete only when all seven stage exit gates pass and the final local
demonstration produces working evidence for every definition-of-done line. A green early stage, a
mock, an interface, a generated schema, or a roadmap entry never substitutes for a later required
capability.
