# 0002: Derive Execution Quality from Complete Evidence

Status: Accepted

Decision date: 2026-07-16

## Context

Connectivity, a provider-supplied quality label, an archival record, a market-depth level, or a
calculated price does not prove that a market observation is safe for immediate automated action.
Execution requires current source authority, explicit coverage, valid session and capture
evidence, sequence/snapshot/checksum integrity, timestamps and freshness, status, precision, and
consistent instrument-owned state.

Accounting fair-value hierarchy answers a different question. Level 1, Level 2, and Level 3
classify inputs to a fair-value measurement; they do not describe transport integrity or grant
order authority.

## Decision

Execution quality is derived from complete source and stream evidence. It is never caller-assigned
and is never inferred from fair-value hierarchy or market depth.

`FairValueHierarchy`, `MarketDepth`, `DataQuality`, `StreamIntegrityState`, archival
`ExecutionEligibility`, and the process-local live execution capability remain separate types with
no implicit conversion. `QualificationAssessment` derives recorded quality and failures for audit.
Only the stateful live plane can issue the opaque, current, non-serializable, single-use
`LiveExecutionCapability` after all required evidence passes.

An automated `OrderIntent` must require `DirectVerified`, and central risk must also consume and
revalidate the live capability. A serialized assessment, archived `DirectVerified` value, replay
record, immutable snapshot, heartbeat, fair-value conclusion, or model output is insufficient.

## Consequences

- Missing or invalid evidence yields an ineligible quality, staleness, or quarantine and produces
  no automated action.
- Sequence gaps, checksum failures, capture degradation, queue saturation, authority rollover, and
  actor exit revoke affected execution authority.
- Re-entry requires source-specific resynchronization and complete qualification in a current
  generation.
- Fair-value evidence may use governed direct or indirect inputs without promoting them into
  execution quality.
- Even a valid Level 1 classification carries no execution capability.
- Archive and control-plane records remain useful for explanation without becoming bearer
  credentials.

## Rejected alternatives

- Accepting a caller-supplied `DataQuality::DirectVerified` value.
- Treating connection health or heartbeat receipt as market-price freshness.
- Converting Level 1 fair-value evidence into execution eligibility.
- Treating price-level or order-level depth as proof of source integrity.
- Reconstructing current authority from Serde, replay, a snapshot, or an assessment identifier.
- Allowing risk to approve based on a quality enum without consuming live authority.

## Related architecture

- [Live execution plane](../live-execution-plane.md)
- [Data, time, and provenance](../data-time-and-provenance.md)
- [Security and trust boundaries](../security-and-trust-boundaries.md)
- [ADR 0005: Central risk and execution authority](0005-central-risk-and-execution-authority.md)

## Evidence and sources

- [Independent classification types](../../../crates/market-squawk-domain/src/classification.rs),
  [derived qualification](../../../crates/market-squawk-domain/src/classification/qualification.rs),
  and [archive-safe live provenance](../../../crates/market-squawk-domain/src/provenance/live.rs),
  reviewed at `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Live capability issuance and consumption](../../../crates/market-squawk-live/src/authority.rs) and
  [mandatory risk evaluation](../../../crates/market-squawk-execution/src/risk.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Code-owned fair-value rules](../../../crates/market-squawk-valuation/src/rules.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [FASB ASU 2011-04, Fair Value Measurement (Topic 820)](https://fasb.org/page/document?pdf=ASU2011-04.pdf&title=UPDATE+NO.+2011-04%E2%80%94FAIR+VALUE+MEASUREMENT+%28TOPIC+820%29%3A+AMENDMENTS+TO+ACHIEVE+COMMON+FAIR+VALUE+MEASUREMENT+AND+DISCLOSURE+REQUIREMENTS+IN+U.S.+GAAP+AND+IFRSS),
  reviewed 2026-07-23.
- [IFRS 13 Fair Value Measurement](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/),
  reviewed 2026-07-23.
