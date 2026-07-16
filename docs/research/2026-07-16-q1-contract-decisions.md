# Quarter 1 Contract Decisions

Date: 2026-07-16  
Scope: Stage 1 Tasks 1-4 and their grouped review corrections

## Purpose

This note preserves the external sources and architectural decisions that changed the Quarter 1
domain contracts. It supplements the repository-wide research report without replacing the
specification, target architecture, or implementation plan.

## Futures maturity evidence

Primary FIX Trading Community sources, accessed 2026-07-16:

- [MaturityMonthYear (tag 200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html)
- [MaturityDate (tag 541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html)
- [LegMaturityMonthYear (tag 610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html)
- [Security-list leg component](https://fiximate.fixtrading.org/en/FIX.Latest/cmp2088.html)

The sources represent top-level maturity month/year, a full maturity date, and leg-level maturity
month/year as separate fields. Market Squawk therefore preserves those claims independently:

- `FuturesContractIdentity::contract_month()` is optional and records only an explicit top-level
  month component.
- `FuturesLifecycleDates::maturity_date()` retains an explicitly supplied full date and its source
  evidence.
- Each `FuturesLeg` retains its own explicit contract month.
- No top-level month is synthesized from a full date or from component legs.

This prevents adapters from manufacturing evidence for daily contracts, independently identified
multileg products, or source records that omit tag 200.

## Provider identity qualification

`ProviderInstrumentId` is meaningful only inside a provider namespace. A `VenueMapping` now stores
only the venue and venue symbol. Provider-native identifiers are retained through
`ProviderIdentityRecord`, which also carries `SourceId`, the stable `InstrumentId`, the source
reference, and its effective interval. Identical provider-ID text from two sources remains two
distinct records.

## Live authority boundary

Quarter 1 review demonstrated that a purely public domain evaluator cannot be an execution
authority: any dependent crate can construct public observations and affirmative result enums.
The corrected boundary is deliberately fail-closed:

- Domain qualification values are audit assessments, not execution capabilities.
- Every assessment component is bound to one immutable source/session/metadata/instrument/venue/
  channel/event/payload/canonical-state key and a checked assessment window.
- Snapshot initialization is explicit and independent from the optional availability of a provider
  snapshot sequence.
- Scoped coverage names its venue, provider product, event class, depth, delay/consolidation
  semantics, metadata revision, and effective interval.
- Archival `DirectVerified` remains readable as a recorded classification, but archive-facing
  execution eligibility is always `Ineligible` and the record requires requalification.
- The domain exports no `QualifiedCurrent`, promotion method, or deserializable current authority.

The opaque, non-Serde, non-clonable, time-bounded execution capability belongs to the stateful
`market-squawk-live` evaluator after the authoritative source registry and instrument-owned live
state exist. That evaluator must consume the complete bound assessment, verify the current source
metadata revision/session generation and action time, and issue the only value accepted by the risk
boundary. This dependency ordering prevents a domain audit object from becoming an accidental
authorization token.

## Financial rounding verification

Exact conversions continue to use checked rational arithmetic. The explicitly rounded provider
boundary is now independently property-tested with arbitrary-precision integer rational oracles:
every generated price and quantity case is evaluated under nearest-even, away-from-zero,
toward-zero, floor, and ceiling policies, including signed prices and rounding-induced `i64`
overflow boundaries. The oracle does not call the production exact-decimal helpers.

## Taxonomy separation

Compile-time negative assertions prevent both `FairValueHierarchy` and `MarketDepth` from gaining
`Into` or `TryInto` conversions to `DataQuality` or `ExecutionEligibility`. Runtime qualification
tests separately prove that valuation hierarchy does not change the quality of the underlying
market observation.

