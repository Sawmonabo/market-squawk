# Market Squawk Stage 1 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the Rust 1.97 virtual workspace, invariant-preserving shared contracts, and
safe live/application boundaries while preserving the current Coinbase, journal, paper-bot, CLI,
and local MCP behavior.

**Architecture:** Migrate the current single package by vertical slices, keeping the repository
runnable after every task. Shared types live in a dependency-light domain crate; source contracts,
live state, pure analytics, risk/execution, platform services, adapters, MCP, and the application
depend inward through explicit APIs. The live path uses scaled integers, deterministic single-writer
shards, bounded non-blocking queues, immutable snapshots, and an unforgeable risk approval token.

**Tech Stack:** Rust 1.97.0 stable, Edition 2024, Cargo resolver 3, Tokio, Tokio-util, Serde,
Rust Decimal, UUID, Chrono, Thiserror, Tracing, Clap, Tokio-Tungstenite, Reqwest, Proptest,
Trybuild, Cargo Deny, Gitleaks, and Python 3 verification scripts.

**Controlling documents:**

- [`2026-07-15-current-state-anchor.md`](../../audits/architecture/2026-07-15-current-state-anchor.md)
- [`2026-07-16-target-state-baseline.md`](../../audits/architecture/2026-07-16-target-state-baseline.md)
- [`gap-analysis.md`](../../plans/gap-analysis.md)
- [`implementation-plan.md`](../../plans/implementation-plan.md)

## Non-negotiable implementation rules

- Preserve unrelated user changes; inspect `git status --short` before each commit and stage only
  paths named in the task.
- Write the failing test before production code. Confirm that it fails for the stated reason.
- Never weaken workspace lints to make migration code compile. A local exception requires a comment
  that states the invariant and why the exception is narrower than an architectural change.
- Libraries return typed errors. `anyhow` is allowed only in `apps/market-squawk`.
- No app, MCP, adapter, or strategy can construct an approved order.
- A queue overflow, capture loss, sequence fault, checksum fault, crossed book, or stale state fails
  closed for execution eligibility.
- The Stage 1 Coinbase adapter has an explicit `DirectUnverified` quality ceiling. It cannot become
  `DirectVerified` until Stage 2 adds the channel-specific evidence state machine.
- Keep legacy `MEJ1/.mej` journals readable. Write only the current documented `MSJ1/.msj` format.
- Run `git diff --check` at every task boundary.

## Stage 1 target file map

```text
Cargo.toml
rust-toolchain.toml
rustfmt.toml
deny.toml
apps/market-squawk/{Cargo.toml,src/main.rs}
crates/market-squawk-domain/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-platform/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-sources/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-live/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-analytics/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-execution/{Cargo.toml,src/*.rs,tests/*.rs}
crates/market-squawk-mcp/{Cargo.toml,src/*.rs,tests/*.rs}
adapters/market-squawk-adapter-coinbase/{Cargo.toml,src/*.rs,tests/*.rs}
adapters/market-squawk-adapter-paper/{Cargo.toml,src/*.rs,tests/*.rs}
scripts/check_brand.py
scripts/check_generated_artifacts.py
scripts/check_workspace_boundaries.py
scripts/verify.sh
```

Do not create Stage 2-6 crates as empty placeholders. Create them when their first production
contract and test are implemented.

---

## Task 1: Finish the rename with journal compatibility and deterministic brand checks

**Files:**

- Create: `scripts/check_brand.py`
- Modify: `src/journal.rs`
- Modify: `tests/journal.rs`
- Modify: `scripts/verify.sh`
- Modify: `.github/workflows/ci.yml`
- Verify: every currently modified rename path reported by `git status --short`

- [ ] **Step 1: Freeze the dirty-worktree boundary**

Run:

```bash
git status --short
git diff --stat
git diff --check
```

Record the existing user-owned paths in the task notes. Do not stage research, architecture, or
unrelated code with the rename commit.

- [ ] **Step 2: Add legacy and current journal fixtures first**

Add tests that construct a minimal header explicitly instead of relying on the active writer:

```rust
#[test]
fn reads_legacy_mej1_header() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = fixture_with_magic(*b"MEJ1");
    let records = JournalReader::new(Cursor::new(bytes)).read_all()?;
    assert_eq!(records.len(), 1);
    Ok(())
}

#[test]
fn rejects_unknown_journal_magic() {
    let bytes = fixture_with_magic(*b"XXXX");
    assert!(matches!(
        JournalReader::new(Cursor::new(bytes)).read_all(),
        Err(JournalError::UnsupportedMagic(_))
    ));
}
```

Run: `cargo test --test journal reads_legacy_mej1_header -- --exact`

Expected: FAIL because the current reader accepts only the renamed magic.

- [ ] **Step 3: Implement an explicit format discriminator**

In `src/journal.rs`, keep the fields private and make read compatibility visible:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFormat {
    LegacyMej1,
    MarketSquawkMsj1,
}

impl TryFrom<[u8; 4]> for JournalFormat {
    type Error = JournalError;

    fn try_from(value: [u8; 4]) -> Result<Self, Self::Error> {
        match &value {
            b"MEJ1" => Ok(Self::LegacyMej1),
            b"MSJ1" => Ok(Self::MarketSquawkMsj1),
            _ => Err(JournalError::UnsupportedMagic(value)),
        }
    }
}
```

The writer emits `MSJ1`; neither reader branch guesses based on file extension.

- [ ] **Step 4: Add the brand checker**

Implement `scripts/check_brand.py` using `git ls-files -co --exclude-standard`, bounded text-file
reads, and an allowlist containing only the compatibility constant/test and historical research
citations. It must report `path:line:token` and exit nonzero on an unapproved `market-engine`,
`Market Engine`, `.mej`, or `MEJ1` occurrence.

Test it with a temporary tracked-text fixture under `target/check-brand-fixture/`; do not modify a
real source file just to test failure.

- [ ] **Step 5: Put the check into local and CI verification**

Add `python3 scripts/check_brand.py` before compilation in `scripts/verify.sh` and CI. Preserve the
existing deterministic tests and separately gated network smoke tests.

- [ ] **Step 6: Verify and commit**

```bash
cargo fmt --all --check
cargo test --test journal
python3 scripts/check_brand.py
./scripts/verify.sh
git diff --check
git status --short
git add .gitattributes .gitignore Cargo.lock Cargo.toml README.md CHANGELOG.md \
  CONTRIBUTING.md SECURITY.md docs/verification.md rust-toolchain.toml .rustfmt.toml \
  .github scripts src tests
git commit -m "refactor: complete Market Squawk rename compatibly"
```

Expected: all existing tests plus both journal-format tests pass; brand check is clean. Review the
staged diff before committing so planning/research files are not accidentally included.

---

## Task 2: Establish the Rust 1.97 virtual workspace and enforce package inheritance

**Files:**

- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `rust-toolchain.toml`
- Rename: `.rustfmt.toml` to `rustfmt.toml`
- Create: `apps/market-squawk/Cargo.toml`
- Move: `src/` to `apps/market-squawk/src/`
- Move: `tests/` to `apps/market-squawk/tests/`
- Create: `crates/market-squawk-domain/Cargo.toml`
- Create: `crates/market-squawk-domain/src/lib.rs`
- Create: `crates/market-squawk-domain/src/version.rs`
- Create: `crates/market-squawk-domain/tests/schema_version.rs`
- Create: `scripts/check_workspace_boundaries.py`

- [ ] **Step 1: Write the workspace-metadata failure check**

`scripts/check_workspace_boundaries.py` must parse `cargo metadata --format-version 1 --no-deps`
and fail when a workspace package does not have version `0.1.0`, edition `2024`, rust-version
`1.97`, license `Apache-2.0 OR MIT`, or a `lints.workspace = true` declaration in its manifest.
Also fail if a non-app manifest directly depends on `anyhow`.

Run: `python3 scripts/check_workspace_boundaries.py`

Expected: FAIL because the repository is still a root package rather than the required virtual
workspace, the application is not under `apps/`, and the domain crate does not exist.

- [ ] **Step 2: Replace the root package with a virtual workspace**

Use this root shape, filling dependency versions from the newly resolved lockfile rather than
inventing per-crate versions:

```toml
[workspace]
resolver = "3"
members = ["apps/*", "crates/*", "adapters/*"]
default-members = ["apps/market-squawk"]

[workspace.package]
edition = "2024"
rust-version = "1.97"
version = "0.1.0"
license = "Apache-2.0 OR MIT"

[workspace.lints.rust]
unsafe_code = "forbid"
unreachable_pub = "warn"
unused_must_use = "deny"
missing_debug_implementations = "warn"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }
cargo = { level = "warn", priority = -1 }
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"
todo = "deny"
unimplemented = "deny"
```

Add shared dependencies under `[workspace.dependencies]`; do not duplicate version declarations in
member crates. Set `channel = "1.97.0"`, components `clippy` and `rustfmt`, profile `minimal`.

- [ ] **Step 3: Move the existing package intact into the application member**

Use `git mv src apps/market-squawk/src` and `git mv tests apps/market-squawk/tests`. Convert the old
root package manifest into `apps/market-squawk/Cargo.toml`, inheriting workspace metadata/lints and
using `{ workspace = true }` dependencies. Keep both the library and binary targets temporarily;
later tasks remove the app library only after consumers have migrated. Update scripts that use
explicit paths. Before adding any new architecture, run:

```bash
cargo run -- --help
cargo test -p market-squawk
python3 scripts/smoke_mcp.py
```

Expected: the same commands and baseline tests still work from the virtual-workspace root.

- [ ] **Step 4: Add a tested, non-empty domain crate**

Write `schema_version.rs` first: zero is invalid, version one round-trips through Serde, and an
unsupported future version is preserved for a typed compatibility error rather than silently read
as current. Run it and observe the missing-type failure before adding production code.

The crate must inherit all metadata/lints and expose a real schema version type:

```toml
[package]
name = "market-squawk-domain"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[lints]
workspace = true
```

```rust
//! Shared invariant-preserving Market Squawk domain contracts.

mod version;

pub use version::{SchemaVersion, SchemaVersionError};
```

- [ ] **Step 5: Verify toolchain pinning and inheritance**

```bash
rustup toolchain install 1.97.0 --profile minimal --component clippy,rustfmt
cargo +1.97.0 metadata --format-version 1 --no-deps
python3 scripts/check_workspace_boundaries.py
cargo +1.97.0 fmt --all --check
cargo +1.97.0 clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

Expected: the migrated application and non-empty domain crate are workspace members, CLI/MCP
compatibility tests pass, there is no metadata/lint exception, and the lockfile is refreshed.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml \
  apps/market-squawk crates/market-squawk-domain scripts/check_workspace_boundaries.py
git commit -m "build: establish Rust 1.97 virtual workspace"
```

---

## Task 3: Implement validated identities and scaled financial values

**Files:**

- Create: `crates/market-squawk-domain/src/identity.rs`
- Create: `crates/market-squawk-domain/src/identifiers.rs`
- Create: `crates/market-squawk-domain/src/instrument.rs`
- Create: `crates/market-squawk-domain/src/financial.rs`
- Modify: `crates/market-squawk-domain/src/lib.rs`
- Create: `crates/market-squawk-domain/tests/financial_values.rs`
- Create: `crates/market-squawk-domain/tests/financial_properties.rs`

- [ ] **Step 1: Write boundary and exactness tests**

Cover empty/oversized venue IDs, UUID round trips, ticker/venue-symbol validation, CUSIP/ISIN/SEDOL
check digits, FIGI syntax, OCC option identity, every FIX `MonthYear` form (`YYYYMM`, `YYYYMMDD`,
and `YYYYMMwN`), independent tag 541 maturity dates, leg-local tag 610 and 611 claims,
optional first/last-trade, expiration, notice, delivery, and settlement lifecycle dates, CAIP-2
envelope parsing and namespace-profile qualification, crypto pair/chain-address normalization, zero tick/lot sizes,
negative quantities where forbidden, exact decimal normalization, inexact scale rejection, and
checked overflow:

```rust
#[test]
fn price_rejects_fractional_tick() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let result = PriceTicks::try_from_decimal(Decimal::new(102, 2), tick);
    assert_eq!(result, Err(PriceError::InexactTick));
    Ok(())
}

proptest! {
    #[test]
    fn checked_tick_round_trip(ticks in -1_000_000_i64..1_000_000, scale in 0_u32..8) {
        let tick = TickSize::power_of_ten(scale);
        let price = PriceTicks::new(ticks);
        let result = price.checked_to_decimal(tick)
            .and_then(|value| PriceTicks::try_from_decimal(value, tick));
        prop_assert_eq!(result, Ok(price));
    }
}
```

Run: `cargo test -p market-squawk-domain --test financial_values`

Expected: FAIL because the types do not exist.

- [ ] **Step 2: Implement private-field identity types**

Implement and document `InstrumentId(Uuid)`, bounded non-empty `VenueId`, `SourceId`,
`ProviderInstrumentId`, `SequenceNumber`, `ConnectionGeneration`, and `SourceIdentifier`, plus
validated `Ticker`, `VenueSymbol`, `Cusip`, `Isin`, `Sedol`, `Figi`, `OccOptionIdentity`,
`FuturesContractIdentity`, `CryptoPair`, and `ChainAddress`. Check algorithms and format validation
must cite their authoritative specification in rustdoc/tests; a syntactically valid external ID is
still not proof that an instrument exists. Provide borrowed views, `Display`, Serde, and only
semantically valid conversions. Do not implement `Deref<Target = String>` or public tuple fields.

Model the rendered FIX Latest EP307 maturity claims without collapsing them. Tag 200 is a
structured `MaturityMonthYear` designator that preserves `YYYYMM`, `YYYYMMDD`, or `YYYYMMwN`; tag
541 is a separate optional `LocalMktDate`; tag 610 is the same structured designator scoped to one
leg, and tag 611 is that leg's separate maturity date. First/last trade, expiration, notice,
delivery, settlement, and other lifecycle dates remain optional, source-evidenced fields. Never
synthesize one claim from another or reduce a day/week designator to a month. Retain the canonical
URLs, access instant/timezone, rendered edition, response-body SHA-256, and relevant tag/datatype
section in `docs/research/2026-07-16-q1-contract-decisions.md` because `FIX.Latest` is a moving alias.

Treat `ChainId` as a case-sensitive CAIP-2 envelope, not namespace proof. Add explicit profile
validation before a chain becomes registry-qualified: `eip155` uses the base-10 form of the chain
ID returned by `eth_chainId`; `solana` uses the first 32 characters of the genesis hash returned by
`getGenesisHash`. Validate Solana addresses independently as fixed 32-byte base58 public keys. Do
not describe the chain ID, a mutable RPC endpoint, or an unversioned URL as immutable evidence.

Add `InstrumentDefinition` with private instrument ID, asset class, primary currency, tick/lot rules,
venue mappings, identifiers, and trading status. Symbol history, corporate-action transitions,
mergers/delistings, and contract-roll persistence are Stage 3/4 behaviors, but their effective-time
record contracts are defined here so storage cannot later invent incompatible identity semantics.

Provider-native identity assertions use a versioned `ProviderIdentityRecord`, not an unqualified
string or a field on `VenueMapping`. Bind every record to the provider `SourceId`, stable
`InstrumentId`, content evidence, timestamps, effective interval, and caller/source-supplied revision
and predecessor claims bound to exact content evidence. Authority must be established separately by
the applicable registered source and source-specific adapter verification; these
caller/source-supplied values do not establish it. `ProviderIdentityEvidence` retains zero or more
bounded canonical version-pinned locators (`ProviderIdentityEvidence::MAX_LOCATORS = 64`) as
non-substantive retrieval metadata; a bare URL is not evidence.

Put deterministic ingestion semantics in the provider-identity registry, not in vector equality:
content-equivalent reingestion is idempotent at the logical-assertion layer and creates no second
logical assertion. The registry deterministically coalesces bounded locator and observation metadata
and returns `ObservationCoalesced`; an exact repeat with no new metadata leaves canonical registry
state unchanged. Same-revision disagreement is quarantined; a valid newer revision appends and
supersedes without mutation. Expose typed outcomes so adapters cannot reinterpret this behavior.

- [ ] **Step 3: Implement exact scaled values**

Implement `PriceTicks(i64)`, `QuantityLots(i64)`, `TickSize`, `LotSize`, `BasisPoints(i32)`,
`Currency`, and `Money { amount: Decimal, currency: Currency }`. Provider decimals cross the
boundary with `TryFrom`/named checked constructors. Add `checked_add`, `checked_sub`, and
`checked_mul_quantity`; require an explicit `RoundingPolicy` whenever rounding is allowed.

- [ ] **Step 4: Run properties and lint**

```bash
cargo test -p market-squawk-domain --test financial_values
cargo test -p market-squawk-domain --test financial_properties
cargo clippy -p market-squawk-domain --all-targets --all-features -- -D warnings
cargo doc -p market-squawk-domain --no-deps
git diff --check
```

Expected: all examples and properties pass with no floating-point financial fields.

- [ ] **Step 5: Commit**

```bash
git add crates/market-squawk-domain
git commit -m "feat(domain): add validated identities and scaled values"
```

---

## Task 4: Separate classifications, integrity, audit assessment, time, and provenance

**Files:**

- Create: `crates/market-squawk-domain/src/classification.rs`
- Create: `crates/market-squawk-domain/src/time.rs`
- Create: `crates/market-squawk-domain/src/provenance.rs`
- Create: `crates/market-squawk-domain/src/market.rs`
- Create: `crates/market-squawk-domain/src/research.rs`
- Modify: `crates/market-squawk-domain/src/lib.rs`
- Create: `crates/market-squawk-domain/tests/classification.rs`
- Create: `crates/market-squawk-domain/tests/classification_type_separation.rs`
- Create: `crates/market-squawk-domain/tests/provenance.rs`
- Create: `crates/market-squawk-domain/tests/provenance_boundaries.rs`
- Create: `crates/market-squawk-domain/tests/live_authority_boundary.rs`
- Create: `crates/market-squawk-domain/tests/live_timing_contracts.rs`
- Create: `crates/market-squawk-domain/tests/live_trust_contracts.rs`
- Create: `crates/market-squawk-domain/tests/composite_schema_compatibility.rs`
- Create: `crates/market-squawk-domain/tests/canonical_events.rs`

- [ ] **Step 1: Write separation and audit-assessment tests**

Use compile-time negative assertions to prove hierarchy, depth, quality, integrity, and eligibility
have no `Into`/`TryInto` shortcuts. Separately prove that heartbeat time cannot become market-price
freshness, an audit assessment cannot authorize execution, and deserialized/archival provenance is
always ineligible even when it faithfully records a historical `DirectVerified` classification.
Fair-value hierarchy is not an input to live quality assessment:

```rust
#[test]
fn archival_assessment_is_never_current_execution_authority()
    -> Result<(), Box<dyn std::error::Error>>
{
    let assessment = QualificationAssessment::try_from(valid_assessment_input()?)?;
    assert_eq!(
        assessment.assessment_status_at(assessment.evaluated_at()),
        AssessmentStatus::Satisfied
    );
    assert!(assessment.failures().is_empty());
    let archival = recorded_live_provenance_fixture(&assessment)?;
    assert_eq!(
        archival.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(archival.requires_requalification());
    Ok(())
}
```

Run:

```bash
cargo test -p market-squawk-domain --test classification
cargo test -p market-squawk-domain --test classification_type_separation
cargo test -p market-squawk-domain --test live_trust_contracts
cargo test -p market-squawk-domain --test live_authority_boundary
```

Expected: FAIL because the classification and assessment types are absent.

- [ ] **Step 2: Implement independent enums without conversion shortcuts**

Add the exact `FairValueHierarchy`, `MarketDepth`, and `DataQuality` variants from the product spec,
plus `StreamIntegrityState`, `CaptureIntegrityState::{Disabled, Healthy, Incomplete}`, and the unit
variants `ExecutionEligibility::{Eligible, Ineligible}`. Detailed qualification diagnostics live in
`EligibilityFailures`, not inside archive-facing execution eligibility. Do not implement
conversions among taxonomy/operational types or any ordinal comparison that implies evidentiary
equivalence. Runtime tests must test actual behavior; do not add a tautological runtime assertion
that merely compares one enum variant to another.

- [ ] **Step 3: Implement a bound, audit-only qualification assessment**

`QualificationAssessment` is a durable audit explanation, never a current authorization token. It
contains immutable, mutually consistent evidence bound to one source ID, authoritative source
metadata revision, authorization record, scoped coverage record and effective interval, venue,
instrument, connection generation, provider channel/subscription, event class/depth, payload
reference, canonical-state revision, snapshot state, sequence rule/result, checksum rule/result,
source and receive timestamps, assessed-at time and checked policy window, market freshness,
trading/venue/instrument status, precision result, stream integrity, and capture integrity. Every
component repeats or references the same binding key; construction fails on any transplant,
missing required evidence, impossible time ordering, or inconsistent capability/result pair.

The assessment derives `EligibilityFailures` and the recorded `DataQuality`; callers supply neither.
`assessment_status_at(at) -> AssessmentStatus` returns `Satisfied` only when there are no failures,
`at` is within the inclusive assessment window, and coverage is effective at `at`. The public API
also exposes `failures()` and `has_failure(EligibilityFailure)` for durable diagnostics. These are
useful for audit and replay comparison only. The public domain API exposes no
`execution_eligibility` method, promotion method, `QualifiedCurrent` value, opaque authority, or
execution-eligible constructor. Only Task 7's stateful live issuer may create current execution
authority.

Implement custom `Deserialize` for `QualificationAssessment`. Deny unknown fields, reconstruct via
`QualificationAssessmentInput` and `TryFrom`, recompute the derived quality/failures/evaluation
window, and reject a wire record whose retained `recorded_quality`, `failures`, `evaluated_at`, or
`valid_until` differs from the recomputed value. A durable Serde round trip must preserve the audit
record without manufacturing runtime authority.

Use the neutral `DigestAlgorithm::{Sha256, Blake3}` root type (`PayloadHashAlgorithm` remains only a
compatibility alias). Construct `EvidenceDigest` with an explicit algorithm and bytes. Bind
canonical state with `CanonicalStateDigest` plus a `CanonicalizationRule` containing its rule ID and
one-based `RuleVersion`; algorithm, bytes, rule ID, and version all participate in equality.
`LiveEvidenceBinding::payload_digest()` returns `EvidenceDigest`, while
`canonical_state_digest()` and `BookStateBinding::state_digest()` expose the rule-qualified digest.
Never infer the algorithm or canonicalization rule from a field name, payload source, or byte width.

- [ ] **Step 4: Implement canonical time and provenance**

Use UTC nanosecond timestamps with validated ordering. Add:

```rust
pub struct LiveProvenance {
    schema_version: SchemaVersion,
    binding: LiveEvidenceBinding,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
    recorded_quality: DataQuality,
    recorded_coverage: CoverageStatus,
    payload_reference: PayloadReference,
    assessment_reference: Option<SourceIdentifier>,
    // Not serialized; derived from the checked constructor or wire reference.
    record_state: LiveRecordState,
}
```

The concrete record owns one complete `LiveEvidenceBinding`; it does not duplicate flattened source,
session, metadata, authorization, venue, instrument, generation, channel, event, payload, or
canonical-state identity fields. Public views such as `source_id()`, `instrument_id()`,
`venue_id()`, `source_identifier()`, and `connection_generation()` delegate to that binding.
`record_state()` reports either decoder output or a retained archival-assessment assertion, and the
state is reconstructed rather than accepted as a wire field.

`DecodedLiveProvenanceInput::new` takes `(binding, source_timestamp, received_at, available_at,
ingested_at, recorded_quality, recorded_coverage, payload_reference)` in that order.
`RecordedLiveProvenanceInput::new` takes the same fields followed by `assessment_reference`.
`LiveProvenance::available_at()` returns the required availability instant and
`assessment_reference()` returns only the optional durable reference.

Live provenance retains only a durable assessment reference, not a full
`QualificationAssessment`. It may record a `DirectVerified` historical classification only with
that reference, but its archive-facing execution eligibility is always the unit variant
`Ineligible` and `requires_requalification()` remains true. Enforce
`received_at <= available_at <= ingested_at` in constructors and deserialization, require
`available_at` on the wire with no default, preserve the record for audit/research, and never
manufacture Task 7's capability. Decoder construction rejects a caller-authored
`DirectVerified`. `RecordedLiveProvenanceInput` instead accepts a caller-supplied archival
classification and an opaque assessment reference. The recorded constructor structurally requires
that reference, and deserialization rejects a recorded `DirectVerified` classification when the
reference is absent, but `LiveProvenance` does not dereference or prove the assessment relationship.
The classification remains an audit assertion, never current authority; Task 7 must revalidate the
complete binding and current state independently. `ResearchProvenance` adds
`effective_at`, `published_at`, evidenced/unknown availability, `revision`, and `superseded_at`;
constructors reject impossible time ordering without inventing unavailable timestamps.

Run:

```bash
cargo test -p market-squawk-domain --test provenance
cargo test -p market-squawk-domain --test provenance_boundaries
cargo test -p market-squawk-domain --test composite_schema_compatibility
```

- [ ] **Step 5: Add canonical event families**

Implement the exact family split before adapters/storage depend on it:

```rust
pub enum MarketEvent {
    Trade(TradeEvent),
    Quote(QuoteEvent),
    BookSnapshot(BookSnapshotEvent),
    BookDelta(BookDeltaEvent),
    Auction(AuctionEvent),
    TradingHalt(TradingHaltEvent),
    InstrumentStatus(InstrumentStatusEvent),
    CorporateAction(CorporateActionEvent),
}

pub enum ResearchObservation {
    Filing(FilingObservation),
    Fundamental(FundamentalObservation),
    Macro(MacroObservation),
    PortfolioPosition(PositionObservation),
    Transaction(TransactionObservation),
    CorporateAction(CorporateActionObservation),
    AlternativeData(AlternativeDataObservation),
}
```

Payload structs may contain only fields whose invariants are enforceable now and always include the
appropriate provenance/time contract. Do not combine the two families into a universal event or add
empty marker payloads.

Run: `cargo test -p market-squawk-domain --test canonical_events --locked`

- [ ] **Step 6: Verify and commit**

```bash
cargo test -p market-squawk-domain --all-features --locked
cargo test --doc -p market-squawk-domain --all-features --locked
cargo clippy -p market-squawk-domain --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc -p market-squawk-domain --all-features --no-deps --locked
git diff --check
git add crates/market-squawk-domain
git commit -m "feat(domain): separate quality integrity and provenance"
```

---

## Task 5: Define source contracts, coverage, endpoint policy, and provider budgets

**Files:**

- Create: `crates/market-squawk-sources/Cargo.toml`
- Create: `crates/market-squawk-sources/src/lib.rs`
- Create: `crates/market-squawk-sources/src/metadata.rs`
- Create: `crates/market-squawk-sources/src/registry.rs`
- Create: `crates/market-squawk-sources/src/live.rs`
- Create: `crates/market-squawk-sources/src/decoder.rs`
- Create: `crates/market-squawk-sources/src/extraction.rs`
- Create: `crates/market-squawk-sources/src/policy.rs`
- Create: `crates/market-squawk-sources/src/health.rs`
- Create: `crates/market-squawk-sources/tests/contracts.rs`
- Create: `crates/market-squawk-sources/tests/registry_authority.rs`
- Create: `crates/market-squawk-sources/tests/network_policy.rs`

- [ ] **Step 1: Write metadata and allowlist tests**

Test typed single-venue/partial/delayed/consolidated coverage, declared quality ceiling, endpoint
denial by default, redirect revalidation, and an exhausted budget returning a typed wait decision.

```rust
#[test]
fn redirect_must_remain_allowlisted() -> Result<(), Box<dyn std::error::Error>> {
    let policy = EndpointPolicy::try_new(["wss://advanced-trade-ws.coinbase.com"])?;
    assert!(matches!(
        policy.authorize_redirect("https://attacker.invalid/frame"),
        Err(NetworkPolicyError::EndpointDenied { .. })
    ));
    Ok(())
}
```

Run: `cargo test -p market-squawk-sources --test network_policy`

Expected: FAIL because the source crate does not exist.

- [ ] **Step 2: Implement metadata and health snapshots**

`SourceMetadata` is immutable/versioned and contains source ID, metadata revision and content hash,
source class, typed authorization basis/evidence/effective interval, typed scoped coverage evidence/
effective interval, supported instruments/events, delay semantics, declared quality ceiling,
endpoint allowlist, provider budget policy, provider decoder revision, exact sequence rule/version,
checksum algorithm/canonicalization/scope/depth/level count, and separate connection-idle,
future-skew, transport-age, source-age, and market-age policy. Capability enums declare provider
support but never substitute for a metadata-bound validator profile. `SourceHealthSnapshot`
separately reports connection, session generation, market freshness, integrity, budget/cooldown,
last error class, coverage limitations, and the metadata revision to which health applies; it is a
Serde audit DTO and never current execution authority.

`AuthoritativeSourceRegistry` validates configured metadata/authorization/coverage and issues a
registered-source handle; source supervision binds a current session-generation handle to it. Task
7 obtains its authority gate from those handles, not from a public constructor taking loose
`SourceMetadata`/result enums. The supervisor/registry also owns an opaque current-health lease and
a `ValidatedLiveScope` (or equivalent) binding registry/health epoch, source, revision, session,
generation, authorization, exact venue/instrument membership, product/channel, event/depth rule,
runtime subscription, validation profiles, and effective deadlines. `AllDeclared` instrument
coverage needs registry-owned universe attestation; partial or unproven membership is not execution
quality. Tests prove metadata revisions, effective intervals, scope fields, health, and active
sessions cannot be transplanted or caller-forged.

- [ ] **Step 3: Implement object-safe distinct live/extraction traits**

Configured heterogeneous source registries require object safety. Use one `BoxFuture` allocation
per source session/extraction request rather than `async_trait` rewriting or per-event allocations:

```rust
use futures::future::BoxFuture;

pub trait SourceMetadataProvider {
    fn metadata(&self) -> &SourceMetadata;
}

pub trait LiveMarketSource: SourceMetadataProvider {
    fn run<'a>(
        &'a mut self,
        frames: &'a mut RawFrameFactory,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>>;
}

pub trait MarketDecoder: SourceMetadataProvider {
    fn decode(
        &mut self,
        frame: &RawMarketFrame,
    ) -> Result<DecodedProviderBatch, DecodeError>;
}

pub trait ExtractionSource: SourceMetadataProvider {
    fn discover(&self, request: DiscoveryRequest)
        -> BoxFuture<'_, Result<Vec<SourceObject>, SourceError>>;
    fn extract(&self, request: ExtractionRequest)
        -> BoxFuture<'_, Result<ExtractionBatch, SourceError>>;
}
```

`DecodedProviderBatch` is bounded at construction and contains provider-normalized pre-state
observations plus connection-generation/decoder evidence; it is not an extraction `RecordBatch`
and does not contain caller-asserted canonical `MarketEvent` values. It retains exact provider
numeric lexemes/checked decimals, source timestamps, sequence/snapshot fields, expected checksums,
and status fields until the owning Task 7 shard validates precision, time, sequence, checksum, and
message-atomic state transitions. The authoritative registry validates a batch and returns an
opaque, non-Serde proof that retains the exact current-session lease/allocation identity. Keep raw
live frames, decoded provider batches, canonical live events, and normalized extraction batches
separate. The app composes a configured `LiveMarketSource` and source-specific `MarketDecoder`,
allowing capture before decode without making the adapter depend on platform/live runtime
implementations.

The borrowing validation view is only for synchronous inspection. Actor admission consumes the
decoded frame together with an exact successful raw-capture admission receipt and returns an owned,
`Send + 'static`, non-Serde `CurrentDecodedProviderBatches`. One provider frame may contain multiple
venue/instrument scopes; registry admission groups it into nonempty homogeneous
`CurrentDecodedProviderBatch` values while preserving first-key and per-key wire order. Every
intact `CurrentProviderObservation` retains shared exact frame identity, receive time, payload
digest, decoder rule, current session/health/subscription/capture authority, and compact static
policy. A bare or deserialized batch cannot cross the production shard-ingress boundary.

The registry owns one process-local capture allocation per source connection generation. It issues
exactly one registry-only-constructible, non-`Clone`, non-Serde
`CaptureGenerationCapabilities` bundle that retains the exact binding/allocation and consumes into
the initialization, admission, and degradation capabilities for that allocation. Platform
composition must accept this whole bundle, never loose parts. Capture state is one-way within an
allocation: `Initializing -> Healthy -> Incomplete`; `Incomplete` is terminal and recovery
allocates a new generation. A successful admission receipt is owned, non-Serde, and bound to the
exact session/generation allocation, raw payload digest, receive time, and checked nonzero frame
ordinal. Replay, audit health DTOs, diagnostic platform receipts, and reconstructed values cannot
manufacture it.

The registry also issues one restricted, non-`Clone`, non-Serde `RawFrameFactory` to the active
adapter session. It binds the exact source/revision/session/generation lease and owns the checked
frame-ordinal counter. The adapter receives the factory in `LiveMarketSource::run`, but never the
registry's current-session, health, capture, or execution authority. Session invalidation and
ordinal exhaustion terminally disable further frame creation.

- [ ] **Step 4: Implement bounded, policy-compliant provider budgets**

The rate policy represents published limits, local concurrency, backoff, `Retry-After`, and cooldown.
Exhaustion returns `BudgetDecision::WaitUntil` or `BudgetDecision::Unavailable`.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p market-squawk-sources --all-features
cargo clippy -p market-squawk-sources --all-targets --all-features -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-sources
git commit -m "feat(sources): add metadata policy and source contracts"
```

---

## Task 6: Move local platform concerns and make raw capture asynchronous to decisions

**Files:**

- Create: `crates/market-squawk-platform/Cargo.toml`
- Create: `crates/market-squawk-platform/src/lib.rs`
- Create: `crates/market-squawk-platform/src/config.rs`
- Create: `crates/market-squawk-platform/src/paths.rs`
- Create: `crates/market-squawk-platform/src/journal.rs`
- Create: `crates/market-squawk-platform/src/capture.rs`
- Create: `crates/market-squawk-platform/tests/journal_compatibility.rs`
- Create: `crates/market-squawk-platform/tests/capture_backpressure.rs`
- Create: `crates/market-squawk-platform/tests/config_precedence.rs`
- Create: `crates/market-squawk-platform/tests/path_confinement.rs`
- Remove after migration: `apps/market-squawk/src/config.rs`
- Remove after migration: `apps/market-squawk/src/journal.rs`

- [ ] **Step 1: Write a capture test that forbids writer acknowledgement on publish**

Use a bounded channel of capacity one, deliberately stop the writer, and prove that the second
publish returns immediately with a typed overflow instead of awaiting disk or an acknowledgement:

```rust
#[tokio::test(start_paused = true)]
async fn capture_saturation_fails_closed_without_waiting_for_disk() {
    let (publisher, _writer) = raw_capture_channel(NonZeroUsize::MIN);
    assert_eq!(publisher.try_publish(frame(1)), Ok(()));

    assert_eq!(
        publisher.try_publish(frame(2)),
        Err(CapturePublishError::Saturated)
    );
    assert_eq!(publisher.integrity(), CaptureIntegrityState::Incomplete);
}
```

Run: `cargo test -p market-squawk-platform --test capture_backpressure`

Expected: FAIL because capture is still coupled to the current journal writer.

- [ ] **Step 2: Migrate configuration and local paths**

First write table tests for defaults/file/environment/CLI precedence, invalid combined values,
secret redaction, traversal, existing symlink escape, non-UTF-8 paths where supported, and
read-only directory failures. Run both new tests and observe missing-type failures.

Move current config behavior behind validated `AppConfig`, `LocalPaths`, and `ArtifactRoot` types.
Preserve precedence `safe defaults -> local TOML -> MARKET_SQUAWK_* -> CLI overrides`. Canonicalize
only existing path parents, reject artifact traversal/symlink escapes, create directories outside
the live path, and redact secret-bearing values from `Debug` and tracing fields.

- [ ] **Step 3: Migrate the journal with both read formats**

Move Task 1's reader/writer into the platform crate. Preserve CRC/length/record bounds and both
legacy/current reader tests. The journal writer owns its file and flush cadence on a supervised
background task. Shutdown drains up to a configured deadline and reports incomplete capture rather
than blocking the event-to-action path.

- [ ] **Step 4: Implement a non-blocking capture publisher**

The generic capture authority traits live in `market-squawk-domain`, preserving the documented
`platform -> domain` dependency and forbidding `platform -> sources`. The trait associates one
bounded raw-frame view with one concrete receipt, initializer, admission issuer, and cloneable
degrader. Task 5 implements it for `CaptureGenerationCapabilities`; platform statically dispatches
over the whole bundle. Do not erase the frame/receipt relationship behind `dyn`, accept loose
capabilities, or expose a platform diagnostic receipt as source execution authority.

`RawCapturePublisher::try_publish` uses bounded `mpsc::Sender::try_send`; it performs the concrete
admission preflight, checked byte reservation, bounded enqueue, concrete `issue_after_enqueue`, and
final active-allocation recheck in that order. It returns Task 5's associated owned, non-Serde
admission receipt on success, never a disk acknowledgement. The writer task produces separate
metrics/health and never sends per-frame acknowledgements. On full/closed channel, writer failure,
flush failure, control drop, rotation failure, accounting failure, or shutdown deadline, atomically
degrade the registry-issued exact-generation capture allocation before returning and emit a
best-effort bounded control-plane health event. The associated market stream becomes
execution-ineligible until a new connection generation and capture allocation are established.

The channel constructor and rotation operation consume a whole authority bundle. The cloneable
publisher can admit and degrade only. A separate non-`Clone` control handle owns the bundle's
initializer and generation rotation; no publisher clone, public value key, audit snapshot,
diagnostic journal record, or application callback can promote capture health. Capture readiness
means the supervised capture path is ready and is independent of market/book snapshot
synchronization. Same-generation degradation is terminal. Control transitions use RCU/one-way
state or a bounded wait and cannot spin indefinitely on an in-flight publisher. Tests cover wrong-
bundle transplant, exact receipt/frame binding, rotation races, sliced `Bytes`, permit release, and
every degradation path.

- [ ] **Step 5: Rewire and verify compatibility**

```bash
cargo test -p market-squawk-platform --all-features
cargo test -p market-squawk --all-features
python3 scripts/smoke_mcp.py
cargo clippy -p market-squawk-platform -p market-squawk --all-targets --all-features -- -D warnings
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/market-squawk-platform apps/market-squawk scripts
git commit -m "refactor(platform): decouple capture from the live path"
```

---

## Task 7: Implement scaled books and the stateful current-authority issuer

**Files:**

- Create: `crates/market-squawk-live/Cargo.toml`
- Create: `crates/market-squawk-live/src/lib.rs`
- Create: `crates/market-squawk-live/src/book.rs`
- Create: `crates/market-squawk-live/src/integrity.rs`
- Create: `crates/market-squawk-live/src/qualification.rs`
- Create: `crates/market-squawk-live/src/authority.rs`
- Create: `crates/market-squawk-live/tests/book.rs`
- Create: `crates/market-squawk-live/tests/book_properties.rs`
- Create: `crates/market-squawk-live/tests/qualification.rs`
- Create: `crates/market-squawk-live/tests/authority.rs`
- Create: `crates/market-squawk-live/tests/authority_privacy.rs`
- Create: `crates/market-squawk-live/tests/ui/current_capability_is_opaque.rs`
- Create: `crates/market-squawk-live/tests/ui/current_capability_is_not_serde.rs`
- Create: `crates/market-squawk-live/tests/ui/domain_assessment_is_not_capability.rs`
- Remove after migration: `apps/market-squawk/src/order_book.rs`
- Remove after migration: `apps/market-squawk/src/quality.rs`

- [ ] **Step 1: Write book invariant and fail-closed tests**

Cover snapshot-before-delta, duplicate/out-of-order sequence, generation changes, delete-on-zero,
depth truncation, best bid/ask, crossed book, exact precision, stale data, and unsupported checksum:

```rust
#[test]
fn crossed_book_quarantines_the_generation() -> Result<(), Box<dyn std::error::Error>> {
    let mut state = instrument_state();
    state.apply(snapshot([(100, 10)], [(101, 8)]))?;
    assert!(matches!(
        state.apply(delta_bid(102, 1)),
        Err(BookError::Crossed { .. })
    ));
    assert_eq!(state.integrity(), StreamIntegrityState::Quarantined);
    assert!(matches!(
        authority_issuer().issue(&state, current_source_metadata(), decision_time()),
        Err(AuthorityError::QuarantinedGeneration { .. })
    ));
    Ok(())
}
```

Run: `cargo test -p market-squawk-live --test book`

Expected: FAIL because the live crate does not exist.

- [ ] **Step 2: Implement an instrument-configured scaled book**

Use `BTreeMap<PriceTicks, QuantityLots>` with descending bid/ascending ask iteration and an explicit
`DepthLimit`. Apply a complete provider message atomically: validate generation/sequence/precision,
build a candidate mutation set, apply all changes, truncate, then validate uncrossed state. On any
failure, retain the last non-executable diagnostic state and quarantine the generation.

- [ ] **Step 3: Implement resynchronization state transitions**

Use a transition table tested exhaustively:

```text
Disconnected -> AwaitingSnapshot -> Synchronizing -> Healthy
       any integrity failure -> Quarantined -> AwaitingSnapshot(new generation)
```

A heartbeat updates connection liveness only. Market freshness derives from the newest valid market
event. Requalification requires a new or explicitly reset generation, a valid snapshot, all
supported integrity evidence, valid status/precision, and freshness.

- [ ] **Step 4: Implement the sole stateful current-authority issuer**

`market-squawk-live` owns the only production issuer of `LiveExecutionCapability`. Issuance consumes
the authoritative current `SourceMetadata` revision (including authorization mode/evidence and
scoped coverage/effective interval), current source-health state and session generation, and the
instrument-owned state. It rechecks the complete Task 4 binding rather than trusting caller-authored
result enums: source, metadata revision, authorization, coverage, venue, instrument, channel,
event class/depth, generation, payload/canonical-state revision, snapshot, sequence, checksum,
timestamps/freshness, trading status, precision, stream integrity, and capture integrity must all
refer to the same current state.

The production issuer/current-authority gate is obtained only by binding Task 5's registered-source
and active-session handles to the shard's instrument-owned state; there is no public constructor from
loose metadata, audit assessment, health result enums, or timestamps. Test-only issuers are behind
`cfg(test)`/non-default test support and their outputs are not accepted by production risk wiring.

The issuer additionally binds the exact one-way source-generation execution lease, capture/current-
health allocation, shard-liveness lease, runtime incarnation, and checked instrument-state revision.
Issuance checks those leases before and after bounded nonce registration; a concurrent invalidation
retires the nonce and fails closed. Capability consumption and the later risk/dispatch boundaries
recheck the same allocations with Acquire semantics. Overflow, source rollover, health/capture
degradation, and shard exit publish Release invalidation before returning or exiting. State-revision
overflow quarantines instead of wrapping. This is the linearization contract used by Task 8.

The issuer derives `DataQuality` and a policy-bound `valid_until`; callers supply neither. Expiry is
the earliest applicable deadline from the source event's freshness budget, receive/source time
sanity policy, metadata/authorization/coverage interval, session/state validity, and configured
maximum capability lifetime. A capability is opaque, has private fields and constructors,
implements neither `Serialize`, `Deserialize`, nor `Clone`, carries a single-use nonce, and is
accepted only through the consumption path required by Task 10. Domain
`QualificationAssessment`, archived provenance, snapshots, replayed records, and caller-authored
`DirectVerified` values cannot mint or substitute for it.

The book cannot claim checksum or sequence validation when authoritative metadata says those
capabilities are absent. The live evaluator may record an audit assessment as `DirectVerified` only
when every required field is affirmative and the source ceiling allows it; capability issuance has
the additional current-state checks above. Add a regression test proving Stage 1 Coinbase remains
`DirectUnverified` and cannot receive a capability.

- [ ] **Step 5: Write adversarial capability tests before the issuer passes**

Use Trybuild fixtures to prove a dependent crate cannot construct, deserialize, or clone
`LiveExecutionCapability` and cannot pass `QualificationAssessment` where the capability is
required. Table-drive a transplant test that replaces each binding component in turn: source,
metadata revision, authorization, coverage/effective interval, venue, instrument, channel,
event/depth, session generation, payload reference, canonical-state revision, snapshot/sequence/
checksum evidence, timestamps/window, status, precision, stream integrity, and capture integrity.
Every transplant must fail closed.

Add exact boundary tests for generation rollover; metadata, authorization, coverage, or health
revocation; acceptance at `valid_until` and rejection at `valid_until + 1ns`; validity at enqueue
followed by expiry during queue delay; a capability revoked before consumption; and duplicate
consumption of the same nonce. Ordinary reuse must also be prevented by by-value, non-`Clone`
semantics. These tests must fail before the stateful issuer/nonce registry exists.

- [ ] **Step 6: Add book and authority properties**

Generate valid snapshots/deltas and assert: bids remain strictly descending, asks ascending,
zero-quantity levels are absent, configured depth is bounded, best prices match extrema, and any
crossed result cannot produce current authority. Generate valid authority bindings and mutate one
component at a time; no mutation may be accepted. Do not use `unwrap`/`expect` in the production
implementation.

- [ ] **Step 7: Verify and commit**

```bash
cargo test -p market-squawk-live --all-features
cargo clippy -p market-squawk-live --all-targets --all-features -- -D warnings
cargo test -p market-squawk --test order_book --test quality
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-live apps/market-squawk
git commit -m "feat(live): add scaled books and current authority"
```

---

## Task 8: Add versioned deterministic sharding, bounded mailboxes, and immutable snapshots

**Files:**

- Create: `crates/market-squawk-live/src/sharding.rs`
- Create: `crates/market-squawk-live/src/runtime.rs`
- Create: `crates/market-squawk-live/src/snapshot.rs`
- Create: `crates/market-squawk-live/tests/sharding.rs`
- Create: `crates/market-squawk-live/tests/overflow.rs`
- Create: `crates/market-squawk-live/tests/snapshot_isolation.rs`
- Remove after migration: `apps/market-squawk/src/engine.rs`

- [ ] **Step 1: Lock stable shard vectors before implementation**

Add golden vectors for venue/instrument pairs and shard counts. Vectors must be architecture- and
process-independent. Also test that shard count zero is rejected and delimiter ambiguity cannot
create collisions in the preimage encoding.

```rust
#[test]
fn routing_v1_matches_golden_vector() -> Result<(), Box<dyn std::error::Error>> {
    let key = ShardKey::new(
        venue("coinbase")?,
        instrument("018f0000-0000-7000-8000-000000000001")?,
    );
    assert_eq!(ShardRouter::v1(16)?.route(&key), ShardId::new(9, 16)?);
    Ok(())
}
```

Run: `cargo test -p market-squawk-live --test sharding`

Expected: FAIL because routing is not implemented.

- [ ] **Step 2: Implement and version one explicit hash algorithm**

Encode ASCII `MSQKSHARD`, one version byte `0x01`, a big-endian `u16` venue-byte length, the venue
bytes, and the 16 UUID bytes; then apply the documented fixed FNV-1a 64-bit offset/prime. Expose
`ShardRoutingVersion::V1`; never use `DefaultHasher`, randomized map state, or a dependency's
unspecified hash. Persist the routing version with snapshots/diagnostics. For venue `coinbase` and
UUID `018f0000-0000-7000-8000-000000000001`, the V1 hash is `0x28edee9cb1852659` and routes to
shard 9 of 16; use that exact golden vector in Step 1.

- [ ] **Step 3: Implement single-writer shard tasks with count-and-byte admission**

Each shard task owns its instrument books, rolling state, strategy state, issuer/nonce state, and
local risk counters. The only production ingress accepts Task 5's owned
`CurrentDecodedProviderBatch`; a bare decoded batch or canonical event cannot enter the actor.
Ingress uses bounded Tokio `mpsc` count admission plus an exact byte-permit budget and per-message
byte limit, all with nonblocking `try_*` operations. A private closed command enum computes checked
deep retained bytes for every nested provider observation; callers cannot undercount through a
trait. The byte permit remains owned until command processing or discard completes. Validate zero,
overflow, and Tokio maximum-permit configuration before constructing primitives. No state mutex is
shared between shards. A supervisor owns and joins exactly the configured shard tasks; it never
mutates an instrument directly and exposes ingress only after every shard reports ready.

- [ ] **Step 4: Make saturation, closure, and actor exit synchronously fail closed**

Every exact source/venue/instrument/session generation has a process-local one-way execution lease;
recovery allocates a new lease. Before returning any count-full, byte-full, overweight, checked-cost,
or closed error, `ShardIngress::try_publish` synchronously invalidates the exact bound lease. It does
not attempt to enqueue quarantine into the already-full mailbox, and safety never depends on a
best-effort health event. The actor rechecks the lease before apply and immediately before features,
strategy, and issuance, so already queued commands from an invalidated generation are diagnostic-
only and cannot mutate a new generation or produce action. Closure/actor exit first invalidates a
one-way shard-liveness lease and all affected generation authority. Fixed-size bounded health events
are audit mirrors only; dropped-event counters saturate.

Add deterministic interleaving/model tests for issue/consume/risk/dispatch versus overflow and
actor exit. An operation whose final authority Acquire linearizes before invalidation may finish;
anything beginning after the admission API returns overflow must observe invalidation. Repeated
faults are idempotent, stale nonce reclamation is incremental and bounded, and no producer-thread
path scans a collection.

- [ ] **Step 5: Publish bounded immutable snapshots without reader backpressure**

Build complete `Arc<MarketSnapshot>` values on the owning shard after the action decision and at a
bounded coalesced cadence/event budget, then atomically publish through a crate-private
`ArcSwap<MarketSnapshot>`. Do not use Tokio `watch` as the value store: an outstanding receiver
borrow can hold its internal read lock and block the shard producer. Optional notification is a
separate coalescing bounded `try_send(())` hint; readers always load the latest immutable value.

Apply explicit instrument/depth/result bounds and dimension-specific completeness/truncation
metadata. Include routing version/count, runtime incarnation, shard ID, source/session generation,
state and snapshot revision, health epoch, and observed/evaluated/published times. Per-shard
snapshots are atomic; cross-shard services return a sorted bounded revision vector rather than
fabricating one global `as_of`. External services receive bounded DTOs, never the `ArcSwap`, issuer,
lease, nonce, capability, or mutable state. Held/slow readers cannot block publication, and retained
snapshot memory is bounded by a documented trusted-reader contract.

- [ ] **Step 6: Implement supervised lifecycle and bounded shutdown**

Static routing version/count define one runtime incarnation. A change invalidates all prior ingress
and authority, clears state, reconnects sources, and requires fresh snapshots; live remapping is not
supported. Partial startup cancels and joins already-started shards. Shutdown invalidates authority,
closes ingress, cancels actors, discards queued market commands while releasing every permit,
publishes terminal diagnostics best-effort, and joins all tasks to a deadline. Deadline aborts are
also awaited; no task is silently detached. An actor drop guard invalidates shard liveness on every
normal, error, cancellation, or panic/unwind path.

- [ ] **Step 7: Verify and commit**

```bash
cargo test -p market-squawk-live --all-features
cargo clippy -p market-squawk-live --all-targets --all-features -- -D warnings
cargo test -p market-squawk --test engine
git diff --check
git add crates/market-squawk-live apps/market-squawk
git commit -m "feat(live): add deterministic single-writer shards"
```

> **Tasks 9–12 are historical and superseded:** do not execute them; use the [Q3 production plan](2026-07-16-market-squawk-q3-production-plan.md) governed by the [Q3 production design](../specs/2026-07-16-market-squawk-q3-production-design.md).

## Task 9: Extract pure online feature kernels with explicit warm-up and arithmetic policy

**Files:**

- Create: `crates/market-squawk-analytics/Cargo.toml`
- Create: `crates/market-squawk-analytics/src/lib.rs`
- Create: `crates/market-squawk-analytics/src/online.rs`
- Create: `crates/market-squawk-analytics/src/registry.rs`
- Create: `crates/market-squawk-analytics/tests/online_features.rs`
- Create: `crates/market-squawk-analytics/tests/feature_properties.rs`
- Remove after migration: `apps/market-squawk/src/features.rs`

- [ ] **Step 1: Port current feature expectations as black-box tests**

Cover spread, midpoint, microprice, imbalance, and momentum with exact scaled inputs. Add empty-book,
one-sided-book, zero-depth, warm-up, timestamp regression, and overflow cases. Statistical outputs
may use `f64`, but tests must show the explicit `PriceTicks -> f64` conversion boundary.

Run: `cargo test -p market-squawk-analytics --test online_features`

Expected: FAIL because the analytics crate does not exist.

- [ ] **Step 2: Implement pure kernels and typed output validity**

The kernel API receives immutable book/trade views; it performs no I/O and owns no Tokio primitives.
Return `FeatureValue<T> { value, observed_at, validity }`, where validity distinguishes `Ready`,
`WarmingUp`, `Unavailable`, and `Overflow`. An unavailable/overflowed required feature cannot produce
an automated action.

- [ ] **Step 3: Register Stage 1 feature metadata**

For each migrated feature record name/version, input schema, parameters, time semantics, warm-up,
null policy, output type, live compatibility, PIT compatibility, and implementation revision. Reject
duplicate `(name, version)` with different metadata.

- [ ] **Step 4: Verify and commit**

```bash
cargo test -p market-squawk-analytics --all-features
cargo clippy -p market-squawk-analytics --all-targets --all-features -- -D warnings
cargo test -p market-squawk --all-features
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-analytics apps/market-squawk
git commit -m "refactor(analytics): extract pure online feature kernels"
```

---

## Task 10: Make risk approval unforgeable and execution adapter inputs safe by construction

**Files:**

- Create: `crates/market-squawk-execution/Cargo.toml`
- Create: `crates/market-squawk-execution/src/lib.rs`
- Create: `crates/market-squawk-execution/src/intent.rs`
- Create: `crates/market-squawk-execution/src/risk.rs`
- Create: `crates/market-squawk-execution/src/adapter.rs`
- Create: `crates/market-squawk-execution/tests/risk.rs`
- Create: `crates/market-squawk-execution/tests/approval_privacy.rs`
- Create: `crates/market-squawk-execution/tests/ui/approved_order_is_private.rs`
- Create: `crates/market-squawk-execution/tests/ui/domain_assessment_cannot_authorize.rs`
- Create: `crates/market-squawk-execution/tests/ui/capability_cannot_be_reused.rs`
- Create: `crates/market-squawk-execution/tests/authority_adversarial.rs`
- Create: `crates/market-squawk-execution/tests/dispatch_once.rs`
- Remove after migration: `apps/market-squawk/src/risk.rs`

- [ ] **Step 1: Write risk matrix and compile-fail tests**

Cover quality/freshness, instrument/account eligibility, position/notional/exposure/leverage/capital,
price/slippage, rate/duplicates, loss/drawdown, expiration, checked arithmetic, and multiple combined
rejections. A Trybuild test must prove downstream code cannot construct or deserialize approval:

```rust,compile_fail
use market_squawk_execution::ApprovedOrder;

fn bypass() {
    let _ = ApprovedOrder { /* private and inaccessible */ };
}
```

Run: `cargo test -p market-squawk-execution --test approval_privacy`

Expected: FAIL because the execution crate/API is absent, then pass only when the UI fixture fails
with the expected privacy error.

Additional compile-fail fixtures must prove a domain `QualificationAssessment`, deserialized live
provenance, immutable snapshot, replay record, or caller-authored `DirectVerified` classification
cannot satisfy the `LiveExecutionCapability` parameter; the capability cannot be cloned/reused; and
neither `ApprovedOrder` nor the adapter-only dispatch value can be externally constructed or
deserialized. Runtime adversarial tests cover every binding transplant, capability expiry, stale
queue delay, generation rollover, health revocation, duplicate capability nonce, duplicate approval
ID, and adapter retry.

- [ ] **Step 2: Implement the complete intent contract**

`OrderIntent` contains strategy/model identity, instrument, account, side, order type, quantity,
optional limit/stop price, TIF, signal/expiration timestamps, reason codes, maximum slippage, and
required data quality. Its constructor rejects internally inconsistent order-type fields.

- [ ] **Step 3: Implement deterministic risk evaluation**

`RiskService::evaluate` requires the Task 7 `LiveExecutionCapability` by value in addition to intent,
market/account views, limits, explicit action time, and a mutable reference to Task 7's
non-constructible current-authority gate. Risk invokes the gate's consuming validation and checks
that the capability remains bound to the authoritative metadata revision, authorization, coverage,
current session generation, instrument state, current healthy source state, and action time. It does
not accept domain assessments or archive/snapshot DTOs. All other deterministic risk checks still
run and produce typed reason codes; a consumed capability is never reusable for a retry.

Evaluation returns either `RiskRejection` with all applicable reason codes or a privately
constructible `ApprovedOrder`. Approval records input hashes, risk ruleset version, decision/action
time, bounded price reference, authority/capability evidence ID, current-generation binding, and
expiry. Its expiry is the minimum of intent expiration, the consumed capability's `valid_until`,
source authorization/coverage validity, and the risk policy's own approval lifetime, so approval can
never extend the underlying evidence. It is neither Serde-deserializable, clonable, nor publicly
constructible.

- [ ] **Step 4: Define the adapter gate**

```rust
use futures::future::BoxFuture;

pub trait ExecutionAdapter {
    fn submit(&self, order: DispatchOrder)
        -> BoxFuture<'_, Result<ExecutionReceipt, ExecutionError>>;
    fn cancel(&self, order_id: &OrderId)
        -> BoxFuture<'_, Result<CancelReceipt, ExecutionError>>;
    fn reconcile(&self) -> BoxFuture<'_, Result<ExecutionState, ExecutionError>>;
}
```

`ExecutionDispatcher::submit(ApprovedOrder, current_authority, action_at)` is the sole public path
to the configured adapter. Immediately before dispatch it atomically consumes the approval ID,
rechecks expiry and the live authority binding/revocation state, and privately constructs the
adapter-only `DispatchOrder`. Duplicate/retried IDs fail before the backend is invoked; an uncertain
backend outcome requires reconciliation and a new risk decision rather than replaying the value.
`DispatchOrder` is public only because it appears in the adapter trait; all fields and constructors
are private to the execution crate and it implements neither Serde nor `Clone`.

Use a boxed future per adapter operation for object-safe configured adapters; risk evaluation and
event-to-decision kernels remain allocation-free. CLI and MCP services accept intents or
cancellation requests, never capabilities, `ApprovedOrder`, or `DispatchOrder`.

- [ ] **Step 5: Rewire the current bot through the risk service**

Move strategy-to-intent mapping behind the new contracts. Preserve current conservative behavior,
but delete all direct execution-adapter calls from strategy/app/MCP code. Add an integration test
that enumerates every app submission entry point and proves the path is Task 7 issuer -> consuming
`RiskService::evaluate` -> one-time `ExecutionDispatcher` -> adapter, with the same risk/dispatch
audit records. Test `valid_until + 1ns`, expiration while queued, health or generation change after
risk but before dispatch, duplicate/revoked capability consumption, duplicate approval dispatch,
and failure/retry behavior.

- [ ] **Step 6: Verify and commit**

```bash
cargo test -p market-squawk-execution --all-features
cargo clippy -p market-squawk-execution --all-targets --all-features -- -D warnings
cargo test -p market-squawk --test risk --test engine
python3 scripts/check_workspace_boundaries.py
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-execution apps/market-squawk
git commit -m "feat(execution): enforce unforgeable risk approval"
```

---

## Task 11: Migrate Coinbase behind a production adapter with an honest quality ceiling

**Files:**

- Create: `adapters/market-squawk-adapter-coinbase/Cargo.toml`
- Create: `adapters/market-squawk-adapter-coinbase/src/lib.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/config.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/decoder.rs`
- Create: `adapters/market-squawk-adapter-coinbase/src/source.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/decode.rs`
- Create: `adapters/market-squawk-adapter-coinbase/tests/local_websocket.rs`
- Remove after migration: `apps/market-squawk/src/source/coinbase.rs`
- Remove after migration: `apps/market-squawk/src/source/mod.rs`
- Move test-only behavior: `apps/market-squawk/src/source/mock.rs` to live/adapter test support
- Remove after migration: `apps/market-squawk/tests/coinbase_decode.rs`
- Remove after migration: `apps/market-squawk/tests/coinbase_source.rs`

- [ ] **Step 1: Copy existing decoder fixtures and add adversarial boundaries**

Before moving implementation, port every current JSON fixture. Add unknown-channel/event, malformed
decimal, inexact tick/lot, oversized frame, duplicate field, negative size, timestamp, status, and
wrong-product tests. The decoder returns typed `DecodeError`; malformed input never panics.

Run: `cargo test -p market-squawk-adapter-coinbase --test decode`

Expected: FAIL because the adapter has not been created.

- [ ] **Step 2: Implement typed configuration and metadata**

`CoinbaseConfig` validates the exact public WebSocket endpoint against `EndpointPolicy`, bounded
subscriptions, product IDs, frame size, reconnect policy, and freshness limits. Metadata must state:

```text
source class: direct venue WebSocket
venue: Coinbase
coverage: single venue, subscribed products/channels only
delay: real-time delivery, no completeness guarantee
quality ceiling in Stage 1: DirectUnverified
sequence validation: unsupported by the selected Level 2 contract
checksum validation: unsupported by the selected Level 2 contract
```

No adapter test may expect `DirectVerified`.

- [ ] **Step 3: Convert provider decimals only at the adapter boundary**

Deserialize wire prices/sizes as strings or raw decimal tokens, parse checked decimals, and convert
through the instrument's `TickSize`/`LotSize`. Reject values that cannot be represented exactly.
Preserve provider timestamp and received-at separately in provenance.

- [ ] **Step 4: Implement supervised source behavior**

Use cancellation-aware connect/read/reconnect with bounded exponential backoff plus jitter, redirect
allowlist validation, maximum frame sizes, ping/pong connection health, new connection generations,
and explicit resynchronization. The adapter emits each frame to the source contract's
`RawMarketSink`; the app-owned sink composes Task 6's capture publisher with the bounded shard sink.
The adapter therefore does not depend on platform or concrete live runtime crates. Either capture
or shard saturation quarantines the affected generation.

- [ ] **Step 5: Keep synthetic exchange code test-only**

Move the mock into `#[cfg(test)]`/a non-default `test-support` feature and ensure production source
registration cannot enumerate it. Add a registry test that only Coinbase is available in Stage 1.

- [ ] **Step 6: Verify offline and opt-in network paths**

```bash
cargo test -p market-squawk-adapter-coinbase --all-features
cargo clippy -p market-squawk-adapter-coinbase --all-targets --all-features -- -D warnings
cargo test -p market-squawk --all-features
MARKET_SQUAWK_NETWORK_TESTS=1 cargo test -p market-squawk-adapter-coinbase \
  --test public_endpoint -- --ignored
git diff --check
```

The local WebSocket test is deterministic and part of the default suite; the public endpoint test
is ignored unless explicitly enabled and must tolerate documented source availability failure.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock adapters/market-squawk-adapter-coinbase apps/market-squawk \
  crates/market-squawk-live
git commit -m "refactor(coinbase): migrate direct venue adapter"
```

---

## Task 12: Migrate paper execution without overstating its realism

**Files:**

- Create: `adapters/market-squawk-adapter-paper/Cargo.toml`
- Create: `adapters/market-squawk-adapter-paper/src/lib.rs`
- Create: `adapters/market-squawk-adapter-paper/src/state.rs`
- Create: `adapters/market-squawk-adapter-paper/src/adapter.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/compatibility.rs`
- Create: `adapters/market-squawk-adapter-paper/tests/risk_gate.rs`
- Move behavior from: `apps/market-squawk/src/bot.rs`
- Modify: `crates/market-squawk-execution/src/adapter.rs`

- [ ] **Step 1: Freeze current paper behavior in compatibility tests**

Test submission, balances/positions, rejection, cancellation result, and reconciliation exactly as
the current implementation behaves. Add metadata assertions that immediate/full fill behavior is
`Modeled` and that fees, latency, slippage, and partial fills are not supported in Stage 1.

Run: `cargo test -p market-squawk-adapter-paper --test compatibility`

Expected: FAIL because the adapter does not exist.

- [ ] **Step 2: Implement the adapter behind the one-time dispatcher**

Only `ExecutionDispatcher::submit(ApprovedOrder, current_authority, action_at)` may construct the
`DispatchOrder` accepted by `ExecutionAdapter::submit`. Keep order IDs monotonic and state
transitions explicit. Use checked decimal/integer accounting; reject insufficient cash/position
without mutation. Produce an audit record for submit, reject, cancel, fill, and reconcile.

- [ ] **Step 3: Prove the risk gate at the adapter boundary**

An integration test attempts all public constructors/Serde paths from outside the live/execution
crates and proves none can produce a capability, approval, or dispatch value. The successful path
must obtain current authority from Task 7, pass the opaque capability by value to
`RiskService::evaluate`, then pass the resulting approval once through `ExecutionDispatcher` before
the evidence-derived expiry. Prove duplicate dispatch and backend retry cannot reuse the approval.
Ensure app/MCP code cannot import a test-only authority or approval constructor.

- [ ] **Step 4: Preserve limitations for the Stage 5 implementation**

Expose typed `PaperCapability` values. Do not add dormant configuration fields that imply fees,
latency, slippage, partial fills, or order queues work. Stage 5 replaces this compatibility engine
with the complete required simulator under the same adapter contract.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p market-squawk-adapter-paper --all-features
cargo test -p market-squawk-execution --all-features
cargo test -p market-squawk --all-features
cargo clippy -p market-squawk-adapter-paper --all-targets --all-features -- -D warnings
git diff --check
git add Cargo.toml Cargo.lock adapters/market-squawk-adapter-paper \
  crates/market-squawk-execution apps/market-squawk
git commit -m "refactor(paper): gate compatibility execution through risk"
```

---

## Task 13: Add bounded application services and migrate diagnostic replay off the live path

**Files:**

- Create: `crates/market-squawk-platform/src/services.rs`
- Create: `crates/market-squawk-platform/src/lifecycle.rs`
- Create: `crates/market-squawk-platform/src/audit.rs`
- Create: `crates/market-squawk-platform/tests/service_limits.rs`
- Create: `crates/market-squawk-platform/tests/shutdown.rs`
- Create: `apps/market-squawk/src/diagnostic_replay.rs`
- Move behavior from: `apps/market-squawk/src/replay.rs`
- Modify: `apps/market-squawk/src/lib.rs`

- [ ] **Step 1: Write service-limit tests before moving CLI/MCP**

Test maximum instruments, depth, events, time range, response bytes, concurrent requests, deadline,
and cancellation. Oversize requests must return typed limit errors before enqueueing live work.

```rust
#[tokio::test]
async fn snapshot_request_is_rejected_before_live_enqueue() {
    let services = fixture_services(ServiceLimits { max_instruments: 10, ..limits() });
    let result = services.market().snapshot(request_with_instruments(11)).await;
    assert!(matches!(
        result,
        Err(ServiceError::LimitExceeded { limit: "instruments", .. })
    ));
    assert_eq!(services.test_probe().live_messages(), 0);
}
```

Run: `cargo test -p market-squawk-platform --test service_limits`

Expected: FAIL because the service boundary does not exist.

- [ ] **Step 2: Implement bounded application-service contracts**

Define object-safe typed `SourceService`, `MarketService`, `BotService`, and `ExecutionService`
contracts plus shared request/response DTOs, limits, cancellation, deadline, authorization mode, and
redacted audit metadata. The platform crate depends only on domain-level contracts and lifecycle
primitives; it must not depend on live, analytics, execution, MCP, or adapters. Fake implementations
exercise all limit logic in platform tests. The app composition root implements the contracts over
immutable snapshots, health receivers, risk, and paper execution in Task 14. Neither CLI nor MCP
obtains shard handles, adapter internals, secrets, filesystem handles, or risk approval constructors.

- [ ] **Step 3: Migrate replay as explicitly diagnostic tooling**

Move journal replay into the app's `diagnostic_replay` composition module; it joins the platform
journal reader, selected adapter decoder, and live sink without introducing a reverse dependency.
It is not a dependency of historical research, model training, or live startup. Replayed records
remain archival/diagnostic and are unconditionally execution-ineligible: replay must not obtain the
authoritative current source registry/session handle, instantiate the production authority issuer,
or mint `LiveExecutionCapability`. Simulation and test harnesses use separately typed sinks whose
outputs are not accepted by production `RiskService`; there is no policy flag that promotes replay
into current authority. Preserve current replay tests and add a boundary test proving replay cannot
satisfy the risk capability parameter.

- [ ] **Step 4: Implement deterministic lifecycle and shutdown**

Composition order is config/paths -> audit/capture -> shard runtime -> sources -> services ->
CLI/MCP. Shutdown reverses this order with bounded deadlines and reports incomplete capture or
reconciliation. A failed task cannot be silently detached; supervisors expose degraded/unavailable
health.

- [ ] **Step 5: Verify and commit**

```bash
cargo test -p market-squawk-platform --all-features
cargo test -p market-squawk --test replay --test engine
cargo clippy -p market-squawk-platform --all-targets --all-features -- -D warnings
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-platform apps/market-squawk
git commit -m "feat(platform): add bounded application services"
```

---

## Task 14: Move local stdio MCP and CLI onto the same typed services

**Files:**

- Create: `crates/market-squawk-mcp/Cargo.toml`
- Create: `crates/market-squawk-mcp/src/lib.rs`
- Create: `crates/market-squawk-mcp/src/protocol.rs`
- Create: `crates/market-squawk-mcp/src/server.rs`
- Create: `crates/market-squawk-mcp/src/schema.rs`
- Create: `crates/market-squawk-mcp/tests/protocol.rs`
- Create: `crates/market-squawk-mcp/tests/limits.rs`
- Create: `apps/market-squawk/src/cli.rs`
- Create: `apps/market-squawk/src/application.rs`
- Create: `apps/market-squawk/tests/cli.rs`
- Modify: `apps/market-squawk/src/main.rs`
- Remove after migration: `apps/market-squawk/src/mcp.rs`
- Remove after migration: `apps/market-squawk/src/lib.rs`
- Modify: `scripts/smoke_mcp.py`

- [ ] **Step 1: Port MCP protocol tests and add hostile-input limits**

Preserve initialize/list/call/current-error behavior. Add maximum line/body/depth/string/result
limits, invalid JSON-RPC IDs, duplicate request IDs, cancellation, EOF, broken pipe, output
backpressure, and secret-redaction tests. Test that arbitrary SQL, shell, filesystem, credentials,
audit deletion, remote code, unchecked submission, and risk bypass tools are absent.

Run: `cargo test -p market-squawk-mcp --all-features`

Expected: FAIL because the MCP crate does not exist.

- [ ] **Step 2: Adopt the official Rust MCP SDK and bound its stdio transport**

Pin `rmcp` `=2.2.0` with only the server, macros/schema, and Tokio stdio features validated by the
locked Rust 1.97 build; record its exact resolved packages/licenses. Do not reimplement JSON-RPC
lifecycle, capability negotiation, cancellation, or protocol error mapping. Wrap stdin/stdout with a
Market Squawk transport that enforces maximum frame/body/depth/string and output-queue limits before
SDK dispatch, keeps stdout protocol-clean, and handles EOF/broken pipe as controlled shutdown.
Validate generated SDK schemas against committed snapshots. Route large results to `ArtifactRoot`
using atomic write/rename and return an opaque artifact reference. Never accept arbitrary artifact
paths.

- [ ] **Step 3: Route MCP tools only through services**

Migrate the currently working tools first, under the required domain namespace where compatible.
Schemas include result/time/instrument limits and cancellation. Bot/execution mutation calls remain
paper-only and require the same `ExecutionService` risk evaluation as CLI. Audit every request/result
class without request secrets or full financial payloads.

- [ ] **Step 4: Rebuild the CLI as a thin composition/application boundary**

Write command-parser snapshots first for the entire required hierarchy, precedence overrides,
invalid combinations, JSON/human output selection, and exit-code classes. Observe the missing
subcommand failures before modifying Clap definitions.

Keep the required hierarchy in Clap even where a later-stage command returns a typed
`CapabilityUnavailable` with the planned stage and remediation. Existing functional commands must
not regress. The app crate contains argument parsing, composition, tracing setup, and `anyhow`
context only; it contains no order book, feature, risk, provider decoding, or MCP protocol logic.

- [ ] **Step 5: Remove the temporary app library**

Move all reusable code to its owning crate, delete `apps/market-squawk/src/lib.rs`, and ensure
integration tests import owning crates/adapters. `cargo metadata` must show the app as a binary-only
package.

- [ ] **Step 6: Verify and commit**

```bash
cargo test -p market-squawk-mcp --all-features
cargo test -p market-squawk --all-features
python3 scripts/smoke_mcp.py
cargo run -p market-squawk -- --help
cargo run -p market-squawk -- doctor
cargo clippy -p market-squawk-mcp -p market-squawk --all-targets --all-features -- -D warnings
python3 scripts/check_workspace_boundaries.py
git diff --check
git add Cargo.toml Cargo.lock crates/market-squawk-mcp apps/market-squawk scripts/smoke_mcp.py
git commit -m "refactor(app): share typed services across CLI and MCP"
```

---

## Task 15: Enforce dependency boundaries, artifacts, credentials, licenses, and advisories

**Files:**

- Create: `deny.toml`
- Create: `.gitleaks.toml`
- Create: `scripts/tool-versions.env`
- Create: `scripts/check_generated_artifacts.py`
- Modify: `scripts/check_workspace_boundaries.py`
- Modify: `scripts/verify.sh`
- Modify: `.github/workflows/ci.yml`
- Modify: `.gitignore`
- Modify: `SECURITY.md`

- [ ] **Step 1: Pin the audited local toolchain**

Record these versions, verified from official project releases on 2026-07-16:

```dotenv
CARGO_DENY_VERSION=0.20.2
CARGO_AUDIT_VERSION=0.22.2
CARGO_MACHETE_VERSION=0.9.2
GITLEAKS_VERSION=8.30.1
```

CI installs exact versions with locked dependencies or downloads the matching official Gitleaks
asset and verifies its published checksum. Local verification checks versions and emits an
installation command; it does not silently download or execute remote code.

- [ ] **Step 2: Write boundary violations as script tests**

Extend `check_workspace_boundaries.py` with an explicit allowlist matrix:

```text
domain      -> external value/serde/time/error libraries only
sources     -> domain
live        -> domain, sources
analytics   -> domain
execution   -> domain, live, analytics
platform    -> domain
mcp         -> domain, platform
coinbase    -> domain, sources
paper       -> domain, execution
app         -> all composition dependencies and adapters
```

It must reject reverse edges, adapter-to-adapter edges, library `anyhow`, duplicate direct
dependency versions, git dependencies without an approved pinned revision, wildcard versions,
unknown registries, and any production dependency on test-support features. Test the checker
against temporary manifests under `target/boundary-fixtures`.

- [ ] **Step 3: Add generated/runtime artifact policy**

`check_generated_artifacts.py` uses `git ls-files` and `.gitignore` checks to reject tracked
`target/`, journals, databases, Parquet/Arrow data, model binaries, captures, logs, credentials, and
temporary artifacts outside explicit small deterministic fixture directories. It checks that local
data, artifact, and secret directories are ignored and that fixtures have documented size/hash
limits.

Run: `python3 scripts/check_generated_artifacts.py`

Expected before policy fixes: FAIL on missing ignore/policy entries, not on legitimate research
Markdown/JSON evidence.

- [ ] **Step 4: Configure dependency, advisory, source, and license policy**

Create `deny.toml` with multiple-version warnings, unknown-registry/git-source denial, RustSec
advisory denial, explicit allowed licenses, and high license-confidence thresholds. Every skip or
exception must include package/version, owner, rationale, expiry/review date, and linked issue. Do
not blanket-ignore unmaintained/yanked/vulnerable crates. Run both Cargo Deny and Cargo Audit because
they provide separate policy/configuration failure surfaces.

- [ ] **Step 5: Configure secret detection without concealing findings**

Extend the official Gitleaks rules in `.gitleaks.toml`. Allowlist only demonstrably fake test values
by exact path/line regex with comments. Run both history and working-directory scans with full
redaction; never baseline a real credential into the repository.

- [ ] **Step 6: Make local and CI gates identical**

`scripts/verify.sh` runs the pinned Rust 1.97 commands and all policy tools. CI invokes that script
rather than maintaining a second command list. Network tests, fuzz endurance, and benchmarks remain
separate jobs, but deterministic parser smoke/fuzz seeds remain in the default suite.

- [ ] **Step 7: Verify and commit**

```bash
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check advisories bans licenses sources
cargo audit --deny warnings
cargo machete --with-metadata
gitleaks git --redact --no-banner .
gitleaks dir --redact --no-banner .
./scripts/verify.sh
git diff --check
git add deny.toml .gitleaks.toml .gitignore SECURITY.md .github scripts
git commit -m "build: enforce Stage 1 security and boundary gates"
```

---

## Task 16: Run the Stage 1 release gate and reconcile architecture evidence

**Files:**

- Modify: `docs/architecture/current-state.md`
- Modify: `docs/plans/gap-analysis.md`
- Modify: `docs/plans/implementation-plan.md`
- Create: `docs/verification/stage-1-foundation.md`
- Modify: `docs/verification.md`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Run the complete locked release gate from a clean build directory**

```bash
rustc --version
cargo --version
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
cargo test --doc --workspace --all-features --locked
cargo build --workspace --all-features --release --locked
python3 scripts/check_brand.py
python3 scripts/check_workspace_boundaries.py
python3 scripts/check_generated_artifacts.py
cargo deny check advisories bans licenses sources
cargo audit --deny warnings
cargo machete --with-metadata
gitleaks git --redact --no-banner .
gitleaks dir --redact --no-banner .
python3 scripts/smoke_mcp.py
git diff --check
```

Expected: every command exits zero on Rust 1.97.0. Do not report Stage 1 complete if a check is
skipped, unavailable, or run without `--locked`; record it as a blocker.

- [ ] **Step 2: Run focused safety evidence again**

```bash
cargo test -p market-squawk-domain --test financial_properties
cargo test -p market-squawk-live --test book_properties
cargo test -p market-squawk-live --test overflow
cargo test -p market-squawk-live --test sharding
cargo test -p market-squawk-live --test authority --test authority_privacy
cargo test -p market-squawk-execution --test approval_privacy
cargo test -p market-squawk-execution --test authority_adversarial --test dispatch_once
cargo test -p market-squawk-mcp --test limits
```

Expected: exact conversion, book invariants, saturation quarantine, stable routing, capability
binding/privacy/expiry, risk authority rejection, one-time dispatch, approval privacy, and MCP bounds
are directly evidenced rather than inferred from the aggregate suite.

- [ ] **Step 3: Record reproducible evidence without performance claims**

In `docs/verification/stage-1-foundation.md`, record UTC date, commit, OS, hardware summary, rustc/
Cargo versions, tool versions, exact commands, exit codes, test counts, and known Stage 2+ gaps. Do
not claim 100,000 events/s or sub-millisecond p99; Stage 7 owns measured acceptance evidence.

- [ ] **Step 4: Re-audit every Stage 1 gap**

Update `current-state.md` to the post-stage facts. Change a gap status only with a code/test/evidence
link. Keep realistic paper execution, Kraken, research storage/adapters, complete features, modeling,
portfolio analytics, fair value, full MCP domains, fuzzing, and performance open in their assigned
stages.

- [ ] **Step 5: Review documentation and public API consistency**

Run:

```bash
rg -n 'TBD|TODO|FIXME|implement later|fill in|appropriate error handling|similar to' \
  README.md docs crates adapters apps scripts
cargo doc --workspace --all-features --no-deps
python3 scripts/check_brand.py
```

Expected: no placeholder prose, stale names, undocumented public invariants/errors, or capability
claims beyond tests. Intentional code-review markers must be linked to a tracked issue and are not
allowed in Stage 1-owned code.

- [ ] **Step 6: Commit the evidence gate**

```bash
git status --short
git add README.md CHANGELOG.md docs/architecture/current-state.md docs/plans \
  docs/verification.md docs/verification/stage-1-foundation.md
git diff --cached --check
git commit -m "docs: record verified Stage 1 foundation"
```

---

## Stage 1 completion checklist

- [ ] Repository is a Rust 1.97.0 virtual workspace with resolver 3 and inherited lints/metadata.
- [ ] Every created crate has production behavior and tests; no empty scaffold crate exists.
- [ ] Existing CLI, local MCP, journal, replay diagnostics, Coinbase, and paper behavior still run.
- [ ] Legacy `MEJ1/.mej` journals read; current writes use `MSJ1/.msj`.
- [ ] Financial values, identities, classifications, provenance, and event families have private
  invariant-preserving fields.
- [ ] Fair-value hierarchy, market depth, data quality, integrity, and execution eligibility cannot
  be confused through public conversions.
- [ ] Domain assessments expose audit status/failures but no eligibility API; archival/replay
  provenance is unit `Ineligible`; neither can mint or deserialize live current authority.
- [ ] Capture and shard queues are bounded; overflow is observable and execution-ineligible.
- [ ] Live state has deterministic single-writer ownership and immutable bounded snapshots.
- [ ] Coinbase cannot exceed `DirectUnverified` in Stage 1.
- [ ] The live issuer alone mints opaque, one-use, expiring current capabilities from authoritative
  registry/session/instrument state; transplant, rollover, delay, revocation, and reuse tests pass.
- [ ] Every submission route consumes current authority through the same risk service; approvals
  cannot outlive evidence and dispatch consumes each approval ID once.
- [ ] CLI and MCP use the same bounded services; MCP has no unrestricted dangerous tools.
- [ ] Workspace, dependency, advisory, license, credential, artifact, lint, test, and release gates
  pass with a committed lockfile.
- [ ] Documentation statuses and claims cite current code/test/evidence.

## Plan self-review before execution

- [ ] Compare all Stage 1 mandatory deliverables in `implementation-plan.md` to Tasks 1-16.
- [ ] Scan for placeholders and resolve every one.
- [ ] Verify type names and dependency directions match `target-state.md`.
- [ ] Verify all referenced local paths either exist now or are explicitly created by an earlier
  task.
- [ ] Verify each production change has a prior failing test, a focused verification command, and a
  scoped commit.
- [ ] Verify no step adds hidden network access, risk bypass, fake production sources, mandatory
  cloud/container/telemetry, or performance claims without evidence.

## Official references used by this plan

- Rust 1.97.0 and resolver 3 evidence is persisted in
  [`final-report.md`](../../research/2026-07-15-market-squawk/final-report.md).
- Current adapter/storage/model/provider/accounting references are cataloged in
  [`source-inventory.json`](../../research/2026-07-15-market-squawk/source-inventory.json).
- Pinned security-tool release checks are recorded in
  [`tooling-refresh-2026-07-16.md`](../../research/2026-07-15-market-squawk/tooling-refresh-2026-07-16.md).

## Quarter 1 exact identity evidence correction

Use the neutral `ExactPayloadEvidence` contract whenever an identity assertion promises exact
source evidence. It requires an algorithm-qualified `EvidenceDigest`; its optional
`VersionPinnedSourceLocator` retains bounded caller/source-supplied locator and version-pin metadata
for retrieval without independently proving the pin immutable or replacing the digest. Bind a
futures identity's caller/source-supplied `MetadataRevision` and exact security-definition evidence
atomically in `RevisionBoundPayloadEvidence`; the binding preserves association but does not by
itself establish revision authority. Authority must be established separately by the applicable
registered source and source-specific adapter verification; these caller/source-supplied values do
not establish it. `ExternalIdentifierRecord` retains `ExactPayloadEvidence` for its supplied
assignment claim. The strict wire rejects omitted content evidence and unknown fields, including
generic/bare `PayloadReference` values, moving URLs without a digest, and missing locator versions.
It preserves the explicit digest algorithm as part of evidence identity; changing the explicit
algorithm while retaining the same bytes produces distinct valid evidence rather than a
deserialization error.
