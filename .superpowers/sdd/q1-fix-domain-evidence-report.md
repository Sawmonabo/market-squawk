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

`/FIX.Latest/` is a moving specification endpoint. A reproducibility correction re-fetched the five
canonical pages at `2026-07-16T05:27:26-04:00` (`America/New_York`, EDT) with
`curl --fail --location --silent --show-error` and SHA-256 hashed each response body:

| Canonical URL | Rendered edition | SHA-256 | Relevant section |
| --- | --- | --- | --- |
| [MaturityMonthYear (200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html) | `FIX.Latest_EP307` | `7721110c47caf818497ac2b23d7ca7f12cd43278fe416ef2e9ddaa9652ba20b5` | Field 200 formats and disambiguation |
| [MaturityDate (541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html) | `FIX.Latest_EP307` | `187ecfeb096a0be4a2a23175f717857960514fb20452328e2df0ba982678adee` | Field 541 `LocalMktDate` |
| [LegMaturityMonthYear (610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html) | `FIX.Latest_EP307` | `b16873961ae1c27be3c3bd914cbbc841cfd503fb371aef5ec291a91d740e3191` | Leg field 610 and tag-200 delegation |
| [LegMaturityDate (611)](https://fiximate.fixtrading.org/en/FIX.Latest/tag611.html) | `FIX.Latest_EP307` | `27b63acc02ddc4437dc454e6bb79dbc54a6d68428a7ecc1acd1653764067e3c9` | Leg field 611, separate from 610 |
| [FIXML datatypes](https://fiximate.fixtrading.org/en/FIX.Latest/fixml_datatypes.html) | `FIX.Latest_EP307` | `7ba910d037e37c57056db1cbdb65cd84b1790326d5666dd5a94f856ffc43a586` | `MonthYear` and `LocalMktDate` rows |

The earlier version of this report recorded `FIX.Latest_EP302` for tag 611 but retained neither an
access timestamp nor a response-body digest. The current page and raw response body unambiguously
render EP307. Because the alias can move and the earlier bytes were not retained, this correction
cannot determine whether the old observation was a transient edition or a reporting error; it
preserves that discrepancy here instead of silently rewriting the evidence history.

The Rust parser follows the valid-value list: exact six- or eight-character wire values, four-digit
years including `0000`, months 01-12, appended days 01-31, and lowercase week codes w1-w5.

## Implemented contracts

### Derivative identity

- Replaced lossy `ContractMonth` with `MaturityMonthYear::{Month, Day, Week}`.
- `FromStr`, `TryFrom<&str>`, `Display`, and Serde all use the source FIX string representation.
- Top-level `MaturityMonthYear(200)` and leg `LegMaturityMonthYear(610)` use the same typed value.
- `MaturityDate(541)` remains in lifecycle dates and `LegMaturityDate(611)` is a separate leg field.
- The futures identity owns `source_id`, `source_reference`, optional source timestamp, local
  observation timestamp, and metadata revision. A source reference is immutable evidence only when
  it names a version-pinned object/record or is paired with a retained content digest; a mutable URL
  is not made immutable by storing it in the type.
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

- Added `ProviderIdentityRecordInput` with payload/source-object evidence, optional source timestamp,
  local observation timestamp, metadata revision, and effective interval. Version-pinned object
  identity or a retained content hash is required before calling that evidence immutable.
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
