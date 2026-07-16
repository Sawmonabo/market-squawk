# Quarter 1 Contract Decisions

Date: 2026-07-16

Scope: Stage 1 Tasks 1-4 and their grouped review corrections

## Purpose

This note preserves the external sources and architectural decisions that changed the Quarter 1
domain contracts. It supplements the repository-wide research report without replacing the
specification, target architecture, or implementation plan.

## Futures maturity evidence

FIXimate's `/en/FIX.Latest/` URLs are official moving aliases, not immutable edition URLs. The
following response bodies were freshly fetched with `curl --fail --location --silent --show-error`,
then hashed byte-for-byte. Every page rendered `FIX.Latest_EP307` at the common access instant
`2026-07-16T05:27:26-04:00` (`America/New_York`, EDT).

| Canonical URL | Accessed (`America/New_York`) | Rendered edition | SHA-256 of fetched response body | Bytes | Relevant rendered section |
| --- | --- | --- | --- | ---: | --- |
| [MaturityMonthYear (200)](https://fiximate.fixtrading.org/en/FIX.Latest/tag200.html) | `2026-07-16T05:27:26-04:00` (EDT) | `FIX.Latest_EP307` | `7721110c47caf818497ac2b23d7ca7f12cd43278fe416ef2e9ddaa9652ba20b5` | 2,446 | Field 200; `MonthYear`; three valid wire forms and same-month disambiguation |
| [MaturityDate (541)](https://fiximate.fixtrading.org/en/FIX.Latest/tag541.html) | `2026-07-16T05:27:26-04:00` (EDT) | `FIX.Latest_EP307` | `187ecfeb096a0be4a2a23175f717857960514fb20452328e2df0ba982678adee` | 1,771 | Field 541; `LocalMktDate`; independent instrument maturity date |
| [LegMaturityMonthYear (610)](https://fiximate.fixtrading.org/en/FIX.Latest/tag610.html) | `2026-07-16T05:27:26-04:00` (EDT) | `FIX.Latest_EP307` | `b16873961ae1c27be3c3bd914cbbc841cfd503fb371aef5ec291a91d740e3191` | 1,893 | Field 610; leg-local `MonthYear`; delegates semantics to field 200 |
| [LegMaturityDate (611)](https://fiximate.fixtrading.org/en/FIX.Latest/tag611.html) | `2026-07-16T05:27:26-04:00` (EDT) | `FIX.Latest_EP307` | `27b63acc02ddc4437dc454e6bb79dbc54a6d68428a7ecc1acd1653764067e3c9` | 1,890 | Field 611; leg-local `LocalMktDate`; separate from field 610 |
| [FIXML datatypes](https://fiximate.fixtrading.org/en/FIX.Latest/fixml_datatypes.html) | `2026-07-16T05:27:26-04:00` (EDT) | `FIX.Latest_EP307` | `7ba910d037e37c57056db1cbdb65cd84b1790326d5666dd5a94f856ffc43a586` | 18,744 | `MonthYear` and `LocalMktDate` datatype rows |

These digests identify the exact research inputs without republishing the specification. A future
recheck against the moving aliases may legitimately produce a different digest or rendered edition;
that is a new evidence snapshot, not a reason to rewrite this one.

In EP307, tag 200 uses the `MonthYear` datatype and accepts `YYYYMM`, `YYYYMMDD`, or `YYYYMMwN`
(`w1` through `w5`). It is not merely a year/month pair. Tag 541 is a separate `LocalMktDate`
representing a full maturity date. Tag 610 applies the same `MonthYear` semantics to one component
leg of a multileg instrument. Tag 611 is that leg's separate `LocalMktDate`. Market Squawk therefore
preserves each supplied claim independently:

- `FuturesContractIdentity` retains an optional structured top-level tag 200 designator without
  reducing day/week forms to a month.
- An explicitly supplied tag 541 maturity date remains a separate optional local-market date.
- Each `FuturesLeg` retains its own optional structured tag 610 designator.
- Each `FuturesLeg` retains tag 611 separately from tag 610.
- First/last trade, expiration, notice, delivery, settlement, and other lifecycle dates are
  separate optional, source-evidenced facts; absence is represented as absence.
- No tag 200/541/610/611 or lifecycle value is synthesized from another field or from component
  legs.

This prevents adapters from manufacturing or weakening evidence for daily/weekly contracts,
independently identified multileg products, or source records that omit a maturity field.

## Exact identity payload evidence

Identity assertions that promise exact source evidence do not use generic `PayloadReference`
values. `ExactPayloadEvidence` requires an algorithm-qualified `EvidenceDigest`. An optional
`VersionPinnedSourceLocator` preserves a bounded caller/source-supplied locator and separate version
pin as retrieval metadata; the type does not independently prove that pin immutable, and the pin
never replaces the mandatory digest. A moving `FIX.Latest` or provider URL is therefore
structurally insufficient by itself.

`FuturesContractIdentity` carries `RevisionBoundPayloadEvidence`, which atomically binds its typed
`MetadataRevision` to the exact payload evidence that established it. Authoritative assignment
evidence on `ExternalIdentifierRecord` uses `ExactPayloadEvidence` directly. The strict wire
rejects omitted content evidence and unknown fields, including the former bare
`source_reference` shape and a locator without a version pin. The wire preserves the explicit digest
algorithm as part of evidence identity; changing the explicit algorithm while retaining the same
bytes produces distinct valid evidence rather than a deserialization error.

## Provider identity qualification

`ProviderInstrumentId` is meaningful only inside a provider namespace. A `VenueMapping` stores only
the venue and venue symbol. Every provider-native assertion is retained as a versioned
`ProviderIdentityRecord` bound to:

- provider `SourceId`/namespace and provider-native identifier;
- stable internal `InstrumentId`;
- mandatory algorithm-qualified content evidence, plus an optional provider object/record locator
  carrying a separate explicit version identity; the locator can aid retrieval but can never replace
  the retained digest, so a bare or mutable URL is not representable as evidence;
- provider source timestamp when supplied and local first-`observed_at` timestamp;
- authoritative source-metadata revision plus the evidence/reference that established it; and
- asserted effective interval and later supersession evidence.

Identical provider-ID text from different sources is therefore distinct. Registry ingestion uses a
deterministic natural key and returns a typed outcome. Content-equivalent reingestion is idempotent
at the logical-assertion layer: it creates no second logical assertion. The registry deterministically
coalesces bounded locator and observation metadata and returns `ObservationCoalesced`; an exact
repeat with no new metadata leaves canonical registry state unchanged. The same natural key and
metadata revision with a different payload, interval, or normalized mapping is a conflict: retain
the competing evidence, quarantine the mapping, and never choose by arrival order. A temporally
valid newer authoritative revision appends a new immutable assertion and explicitly supersedes the
prior assertion, which remains queryable. A changed mapping without a newer evidenced revision is a
quarantined conflict, not an in-place correction.

## Chain namespace evidence

CAIP-2 defines a generic, case-sensitive namespace/reference envelope. It does not make every
grammar-valid reference canonical. Market Squawk therefore separates generic `ChainId` parsing from
namespace-profile qualification. The following official Chain Agnostic specification/profile pages
were fetched and hashed at the per-row instants:

| Canonical URL | Accessed (`America/New_York`) | Rendered profile | SHA-256 of fetched response body | Bytes | Relevant section |
| --- | --- | --- | --- | ---: | --- |
| [CAIP-2 blockchain ID specification](https://standards.chainagnostic.org/CAIPs/caip-2) | `2026-07-16T05:38:32-04:00` (EDT) | Final; updated 2021-08-25 | `f2995ed64502408d69e315b8736e5acf96dd7f85a5f3702b1a35b053674347d9` | 12,501 | Case-sensitive envelope grammar and delegation of reference semantics to namespace profiles |
| [EIP155 CAIP-2 profile](https://namespaces.chainagnostic.org/eip155/caip2) | `2026-07-16T05:27:26-04:00` (EDT) | Draft; updated 2022-03-27 | `423487876763c2922736a9d274f87f3660e7ea3350724272913c7fc39b91e05e` | 10,813 | Syntax and resolution: convert `eth_chainId` base-16 result to a base-10 reference |
| [Solana CAIP-2 profile](https://namespaces.chainagnostic.org/solana/caip2) | `2026-07-16T05:27:26-04:00` (EDT) | Draft; updated 2023-03-27 | `5598020d520135b0b1d84ad89833785eb7f425b40620941e02d29b69165a12ad` | 12,080 | Reference definition and resolution: first 32 characters of `getGenesisHash` result |

The semantic decisions are deliberately narrow:

- `ChainId` preserves a CAIP-2 string after envelope validation; it does not assert chain existence.
- EVM chain qualification requires an `eip155` reference in canonical base-10 form. EIP-55 address
  validation is separate from chain-reference validation.
- Solana chain qualification uses the truncated genesis hash; a Solana account or mint address is
  separately validated as a case-sensitive base58 encoding of exactly 32 bytes.
- RPC endpoints, moving documentation aliases, and provider URLs are mutable locators. None is
  described as immutable evidence without a version-pinned object or retained content digest.

## Digest qualification

Payload bytes and canonical state are different evidence domains. The neutral root
`DigestAlgorithm::{Sha256, Blake3}` qualifies every `EvidenceDigest`; the older
`PayloadHashAlgorithm` name remains a compatibility alias rather than a second taxonomy. Payload
reference comparisons include the algorithm and all 32 digest bytes.

Canonical-state evidence additionally retains `CanonicalizationRule { rule, version }` through
`CanonicalStateDigest`. Its equality includes algorithm, bytes, rule identifier, and one-based rule
version. This prevents the same bytes produced by different algorithms or canonicalization
revisions from comparing as the same evidence. `LiveEvidenceBinding` carries both a payload digest
and a rule-qualified canonical-state digest; book and initialized-snapshot bindings retain the
same canonical-state type. Digests authenticate identity/equality claims only within their stated
rules—they do not independently establish source authorization, freshness, coverage, or execution
authority.

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
- `QualificationAssessment` derives `recorded_quality` and `EligibilityFailures`; callers supply
  neither. `assessment_status_at(at)` returns only audit status and never authority.
- Its custom deserializer rebuilds and revalidates all relational/derived fields through the same
  checked construction path; tampered derived quality, failures, evaluation time, or deadline is
  rejected.
- Archival `DirectVerified` remains readable as a recorded classification, but archive-facing
  execution eligibility is always the unit variant `Ineligible` and the record requires
  requalification.
- `LiveProvenance` carries explicit `available_at`, enforces
  `received_at <= available_at <= ingested_at`, and retains only a durable assessment reference,
  never a full assessment or capability.
- `LiveProvenance` owns the complete `LiveEvidenceBinding`; source, venue, instrument, generation,
  channel, event, payload, and canonical-state identity are binding views rather than independently
  writable flattened fields. Its non-wire `record_state` is derived from checked construction.
- `RecordedLiveProvenanceInput` accepts a caller-supplied archival classification plus an opaque
  assessment reference. The recorded path requires a reference and the wire rejects
  `DirectVerified` without one, but provenance does not dereference or prove the assessment
  relationship. That assertion is audit evidence only and must be revalidated by the live plane.
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
