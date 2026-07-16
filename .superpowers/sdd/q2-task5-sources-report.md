# Q2 Task 5 — Source Contracts and Authority Report

Date: 2026-07-16

## Outcome

Implemented the `market-squawk-sources` crate as the fail-closed boundary for source metadata,
live-frame admission, extraction lineage, registry authority, health qualification, provider budget
coordination, and lawful endpoint policy. The crate is independent of platform/storage concerns and
depends only on the shared domain layer and general-purpose libraries.

The initial TDD red gate was:

```text
cargo test -p market-squawk-sources --test network_policy
error: package ID specification `market-squawk-sources` did not match any packages
```

The final crate has deterministic local tests for authority resurrection/transplant attacks,
capture lifecycle, exact request binding, deep allocation limits, URL normalization attacks,
shared budgets, and typed protocol interpretation.

## Architecture decisions

- `SourceMetadata` is immutable, constructor-validated, versioned, and separates authorization,
  coverage, data quality, protocol semantics, freshness, capabilities, network policy, and budget.
- `FairValueHierarchy`, `MarketDepth`, and `DataQuality` remain separate domain concepts.
- Direct live qualification is registry-owned. Serializable health and metadata are audit data, not
  authority. Current handles, health updates, capture receipts, and current decoded batches are
  non-serializable process-local capabilities.
- Live and extraction source traits are distinct and object-safe. Extraction produces bounded,
  request-bound records; live sources produce bounded exact frames through a once-issued
  `RawFrameFactory` and never receive `CurrentSourceSession`.
- `RawFrameFactory` carries only an exact session binding and checked current-generation frame
  counter. It has no health, capture, budget, or registry API and fails after rollover, end,
  revocation, revision replacement, or ordinal exhaustion.
- Capture wiring is issued once as `CaptureGenerationCapabilities`. The bundle retains the exact
  binding and one-way generation lease and is consumed into least-authority initialization,
  admission, and degradation parts. Admission uses `preflight`, caller-owned bounded enqueue,
  `issue_after_enqueue`, and final `validate_active`; no production API can mint a receipt before
  enqueue.
- Every raw frame has a nonzero generation-local `FrameId`. Capture proof binds allocation,
  frame ID, receive time, and SHA-256 payload digest, preventing receipt reuse even for identical
  payloads and timestamps.
- Provider-normalized observations remain pre-state typed data. They preserve exact decimal
  lexemes, provider evidence, event-specific payloads, and independent semantic interpretation
  rules. They cannot construct canonical executable events before state/integrity validation.
- Semantic rules are independently versioned for aggressor, auction, trading-status/halt, and
  corporate-action families. A rule from one family cannot authorize another. Trading-halt reason
  is mandatory, so canonicalization never invents an unknown reason.
- Sequence, checksum, timestamp, decoder, numeric, snapshot-applicability, provider product, and
  provider channel policies are exact revision-bound contracts. Unsupported sequence/checksum is
  explicit evidence, not absence inferred by a caller.
- The registry persists used metadata revisions and connection-generation high-water marks so
  A→B→A resurrection and generation reuse remain impossible after restart.
- Health qualification uses independent connection, transport, source, and market clocks. A
  heartbeat cannot refresh market freshness. Actor-time validation is lock-free and bound to the
  current health epoch and earliest inclusive expiry.
- Provider budgets are shared by exact provider/account scope and use a monotonic clock,
  concurrency permits, common cooldown, refusal escalation, and bounded backoff.
- Endpoint authorization is structural and fail-closed: exact scheme/host/port/path, bounded query
  rules, no ambient proxy, no automatic redirects, no implicit retries, response bounds, and
  same-origin redirect reauthorization. Raw backslashes, controls/whitespace, literal or encoded
  dot traversal, encoded slashes/backslashes, and double-encoded percent traversal are rejected
  before URL normalization.
- Discovery and extraction request IDs use domain-separated SHA-256 with per-field tags and
  big-endian lengths. Extraction identity binds the complete discovered object, evidence algorithm
  and locator, effective/publication metadata, expected bytes, request limits, and deadline.
- Extraction batch limits count the batch container, cloned request/object allocations, both
  version-pinned evidence locators, record structures, identifiers, and normalized payloads. A
  separate 64 MiB in-memory ceiling applies even when the paged operation allowance is larger.
- Cohesive implementation files are split by coverage/protocol/source metadata, endpoint/budget,
  decoder payload/batch, registry catalog/authority/current batch, and extraction request/record.

## Security boundary

No account or identity rotation, TLS/browser fingerprint spoofing, CAPTCHA bypass, proxy rotation,
or distributed quota evasion was implemented. Those capabilities would violate provider controls
and the product's explicit adapter/caching/health/failover model. The implemented design instead
uses declared coverage, allowlisted endpoints, shared budgets, cooldown, explicit provider errors,
and fail-closed health degradation.

## Verification evidence

All commands completed successfully in the isolated Task 5 worktree:

```text
cargo fmt --all --check

cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
Finished `dev` profile ...

cargo test --workspace --all-features --locked
All workspace unit, integration, property, compile-fail, and doc tests passed.

cargo build --workspace --all-features --release --locked
Finished `release` profile [optimized] ...

cargo clippy -p market-squawk-sources --all-targets --all-features -- -D warnings
Finished `dev` profile ...

cargo test -p market-squawk-sources --all-features
26 tests passed; 0 failed.

git diff --check
No whitespace errors.
```

`Cargo.lock` was intentionally left uncommitted for root-lane integration reconciliation, as
directed. The crate manifest inherits workspace metadata and lints. The private Task 5 brief is
also excluded from the commit.

## Integration notes

- Root will provide the generic domain capture traits and bridge this concrete capability bundle
  to the platform capture implementation while preserving `platform -> domain` and avoiding any
  `platform -> sources` dependency.
- The concrete types needed by that bridge are `RawMarketFrame`, `CaptureAdmissionReceipt`, and
  `CaptureGenerationCapabilities`; the bundle's ownership-consuming `into_parts` yields exact
  initializer/admission/degrader capabilities.
- `LiveMarketSource::run` now receives `&mut RawFrameFactory`, `&mut dyn RawMarketSink`, and a
  cancellation token. Supervision retains `CurrentSourceSession` and all health/registry powers.
