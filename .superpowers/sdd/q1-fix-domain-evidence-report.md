# Q1 Domain Evidence Correction Report

Date: 2026-07-16

Branch: `fix/q1-domain-evidence`

## Result

The domain contracts now preserve the official FIX `MonthYear` maturity forms without collapsing
daily or weekly identities, permit evidenced futures definitions without invented lifecycle dates,
construct research provenance through named input, and retain immutable evidence for provider ID
mappings.

No live-classification implementation, documentation plan, float-based financial representation,
unsafe Rust, or access-limit-evasion behavior was added.

## Official FIX evidence

The following primary FIX Trading Community resources were read on 2026-07-16 and are retained here
because `/FIX.Latest/` is a moving specification endpoint:

- [FIX Latest EP307 FIXML datatypes](https://fiximate.fixtrading.org/en/FIX.Latest/fixml_datatypes.html)
  identified itself as `FIX.Latest_EP307`. Its `MonthYear` entry lists the three formats `YYYYMM`,
  `YYYYMMDD`, and `YYYYMMWW`, with valid values `YYYY=0000-9999`, `MM=01-12`, `DD=01-31`, and
  week codes `w1` through `w5`.
- [MaturityMonthYear(200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html) identified
  itself as `FIX.Latest_EP307` and lists `YYYYMM`, `YYYYMMDD`, and `YYYYMMwN`; it explicitly treats
  the appended date or week as a way to distinguish products in the same month.
- [LegMaturityMonthYear(610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html) identified
  itself as `FIX.Latest_EP307` and delegates its `MonthYear` semantics to tag 200.
- [MaturityDate(541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html) identified itself as
  `FIX.Latest_EP307` and defines the instrument-level maturity date separately.
- [LegMaturityDate(611)](https://fiximate.fixtrading.org/en/FIX.Latest/tag611.html) defines the leg
  maturity date separately from tag 610. On the access date its rendered page header said
  `FIX.Latest_EP302` even though the URL is the official `FIX.Latest` endpoint; this report preserves
  that observed edition discrepancy rather than relabeling it.
- [EP307 extension package](https://fixtrading.org/packages/ep307-seclending-trade-enhancements-phase-2/)
  was the official FIX Trading Community EP307 package page and showed a 2026-07-14 update date.

The Rust parser follows the valid-value list: exact six- or eight-character wire values, four-digit
years including `0000`, months 01-12, appended days 01-31, and lowercase week codes w1-w5.

## Implemented contracts

### Derivative identity

- Replaced lossy `ContractMonth` with `MaturityMonthYear::{Month, Day, Week}`.
- `FromStr`, `TryFrom<&str>`, `Display`, and Serde all use the source FIX string representation.
- Top-level `MaturityMonthYear(200)` and leg `LegMaturityMonthYear(610)` use the same typed value.
- `MaturityDate(541)` remains in lifecycle dates and `LegMaturityDate(611)` is a separate leg field.
- The futures identity owns `source_id`, immutable `source_reference`, optional source timestamp,
  local observation timestamp, and immutable metadata revision. This evidence remains present when
  lifecycle dates are empty.
- Lifecycle dates are empty-permitted, preserve every optional date, and retain relational checks.
- Week-distinct legs are not collapsed merely because their provider security text is the same.
- Custom deserialization routes through checked constructors and denies unknown fields.

### Research provenance

- Added `ResearchProvenanceInput` and `ResearchProvenance::try_new`.
- Removed positional construction and all `too_many_arguments` exceptions.
- Custom Serde checks schema version, then routes through `try_new`, preserving receive/ingestion
  and availability/ingestion invariants.
- Migrated every domain callsite and fixture.

### Provider identity evidence

- Added `ProviderIdentityRecordInput` with immutable payload/source-object evidence, optional source
  timestamp, local observation timestamp, immutable metadata revision, and effective interval.
- Custom Serde denies unknown fields and constructs through the named input.
- Exact record equality is the exact-duplicate-evidence policy.
- Repeated observations and distinct metadata revisions for the same logical mapping are retained.
- One immutable revision cannot claim different effective intervals for the same logical mapping;
  this returns `InstrumentError::ConflictingProviderIdentityInterval`.
- A different revision may correct an interval without losing the earlier evidence record.

## TDD evidence

RED command:

```text
cargo test -p market-squawk-domain \
  --test maturity_month_year \
  --test provider_identity_evidence \
  --test provenance \
  --locked
```

The expected compile failure named the deliberately absent APIs and policies:
`MaturityMonthYear`, `FuturesLegInput`, `ProviderIdentityRecordInput`,
`ResearchProvenanceInput`, futures/provider evidence accessors, and the typed duplicate/conflict
errors.

Focused GREEN result:

```text
maturity_month_year:       4 passed
provider_identity_evidence: 3 passed
provenance:                7 passed
```

The inherited complete domain suite then passed, including 29 financial-value tests, 13 financial
property tests, 7 exact-financial tests, digital-asset identity tests, live authority tests, schema
compatibility tests, and rustdoc compile-fail coverage.

## Verification evidence

All commands completed successfully on 2026-07-16:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS='-D warnings' cargo doc -p market-squawk-domain --all-features --no-deps --locked
./scripts/verify.sh
```

The full verification script additionally passed the repository policy checks, duplicate-dependency
inventory, all-target workspace tests, locked release build, strict workspace rustdoc, CLI help,
101-event offline mock smoke test, and MCP smoke test.
