# Quarter 1 Contract Decisions

Date: 2026-07-16

Scope: Stage 1 Tasks 1-4 and their grouped review corrections

## Purpose

This note preserves the external sources and architectural decisions that changed the Quarter 1
domain contracts. It supplements the repository-wide research report without replacing the
specification, target architecture, or implementation plan.

## Futures maturity evidence

Research snapshot/immutable edition identifier: **FIX.Latest_EP307**. FIXimate's
`/en/FIX.Latest/` URLs are the official current-edition aliases; each accessed page identified its
rendered edition as `FIX.Latest_EP307`. Primary FIX Trading Community sources, accessed 2026-07-16:

- [FIX Latest EP307 MaturityMonthYear (tag 200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html)
- [FIX Latest EP307 MaturityDate (tag 541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html)
- [FIX Latest EP307 LegMaturityMonthYear (tag 610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html)
- [FIX Latest EP307 datatype definitions](https://fiximate.fixtrading.org/en/FIX.Latest/fix_datatypes.html)
- [FIX Latest EP307 InstrumentLeg usage](https://fiximate.fixtrading.org/en/FIX.Latest/cmp2088.html)

In EP307, tag 200 uses the `MonthYear` datatype and accepts `YYYYMM`, `YYYYMMDD`, or `YYYYMMwN`
(`w1` through `w5`). It is not merely a year/month pair. Tag 541 is a separate `LocalMktDate`
representing a full maturity date. Tag 610 applies the same `MonthYear` semantics to one component
leg of a multileg instrument. Market Squawk therefore preserves each supplied claim independently:

- `FuturesContractIdentity` retains an optional structured top-level tag 200 designator without
  reducing day/week forms to a month.
- An explicitly supplied tag 541 maturity date remains a separate optional local-market date.
- Each `FuturesLeg` retains its own optional structured tag 610 designator.
- First/last trade, notice, delivery, settlement, and other lifecycle dates are separate optional,
  source-evidenced facts; absence is represented as absence.
- No tag 200/541/610 or lifecycle value is synthesized from another field or from component legs.

This prevents adapters from manufacturing or weakening evidence for daily/weekly contracts,
independently identified multileg products, or source records that omit a maturity field.

## Provider identity qualification

`ProviderInstrumentId` is meaningful only inside a provider namespace. A `VenueMapping` stores only
the venue and venue symbol. Every provider-native assertion is retained as a versioned
`ProviderIdentityRecord` bound to:

- provider `SourceId`/namespace and provider-native identifier;
- stable internal `InstrumentId`;
- immutable `PayloadReference` (content hash or immutable object/record identity, never only a
  mutable URL);
- provider source timestamp when supplied and local first-`observed_at` timestamp;
- authoritative source-metadata revision plus the evidence/reference that established it; and
- asserted effective interval and later supersession evidence.

Identical provider-ID text from different sources is therefore distinct. Reingesting the same
natural key, metadata revision, immutable payload, and normalized assertion is idempotent and may
record a repeated observation without creating a second assertion. The same provider namespace,
native ID, effective-start, and metadata revision with a different payload or normalized mapping is
a conflicting duplicate: quarantine it and preserve both evidence objects rather than choosing by
arrival order. A later authoritative metadata/provider revision creates a new immutable assertion;
after temporal validation it may supersede the prior assertion, which remains queryable. A changed
mapping without a newer evidenced revision is a conflict, not an in-place correction.

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
metadata revision/authorization/coverage, session generation, source health, instrument state, and
action time, and issue the only value accepted by the risk boundary. Its expiry is derived from the
earliest freshness/evidence/policy deadline and it is consumed once. Risk consumes it by value,
revalidates current generation/health/action time, and cannot issue an `ApprovedOrder` whose expiry
extends past the capability. Dispatch separately consumes the approval ID exactly once. This
dependency ordering prevents a domain audit object from becoming an accidental authorization token
and prevents queue delay, retry, or generation rollover from extending stale authority.

## Financial rounding verification

Exact conversions continue to use checked rational arithmetic. The explicitly rounded provider
boundary is now independently property-tested with arbitrary-precision integer rational oracles:
every generated price and quantity case is evaluated under nearest-even, away-from-zero,
toward-zero, floor, and ceiling policies, including signed prices and rounding-induced `i64`
overflow boundaries. The oracle does not call the production exact-decimal helpers.

## Taxonomy separation

Compile-time negative assertions guard the type/API boundary: neither `FairValueHierarchy` nor
`MarketDepth` may gain `Into`/`TryInto` conversions to `DataQuality` or `ExecutionEligibility`, and
fair-value hierarchy is absent from live qualification inputs. Do not add a runtime test that merely
compares unrelated enum variants or feeds hierarchy into a test-only helper; that would restate the
type declarations without testing production behavior. Runtime tests instead exercise real live
policy inputs and prove that only the stateful current-authority issuer can mint the capability
accepted by risk. Valuation tests independently verify fair-value classification rules.
