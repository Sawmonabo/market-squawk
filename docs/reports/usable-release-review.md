# Usable-release Quarter 4 review

This record defines the grouped final review boundary and records only completed review outcomes.
It is not approval evidence until the exact candidate and every required group are populated.

| Field | Value |
| --- | --- |
| Document type | Release review record |
| Audience | Release owner, reviewers, maintainers, and auditors |
| Status | Prepared; review not started |
| Last substantive review | 2026-07-26 |
| Candidate HEAD/tree | Not frozen |
| Release decision | Blocked |

## Contents

- [Review boundary](#review-boundary)
- [Review groups](#review-groups)
- [Finding and remediation contract](#finding-and-remediation-contract)
- [Current result](#current-result)
- [Related evidence](#related-evidence)

## Review boundary

The review begins only after provider acceptance, candidate freeze, and a complete exact-head
evidence run. Reviewers inspect the same commit, tree, release binary, provider report, signed
Python releases, fuzz report, performance report, demonstration, full-gate log, typed full-gate
receipt, and closed artifact inventory.

Review is read-only. Reviewers do not edit the candidate, create parallel fix branches, rerun broad
gates, or approve a different commit.

## Review groups

| Group | Required scope |
| --- | --- |
| Live and execution | Source/session authority, sequence/checksum/freshness, books/features, model no-action, central risk, one-use dispatch, paper accounting, cancellation, and shutdown |
| Research and provider rights | Discovery/extraction, raw evidence, manifests, Arrow/Parquet/DataFusion, PIT/revisions/corporate actions, provider rights, restart recovery, and Python handoff |
| Financial product | Analytics, model bundles, native/ONNX inference, Python training, backtesting, portfolio accounting/analytics, and fair-value hierarchy/evidence |
| Control plane and security | CLI/MCP schema parity, limits, cancellation, audit, artifacts, configuration/secrets, endpoint confinement, dependency/license/credential evidence, backup/recovery, and operator truth |
| Performance and release truth | Fixture/binary/host binding, timing boundary, RSS/queue bounds, evidence closure, documentation claims, GitHub/CI state, and publication/cleanup readiness |

The release owner unions and deduplicates findings across groups. One defect appears once with the
strongest supported severity and all affected paths.

## Finding and remediation contract

A finding must identify:

- severity and violated release predicate;
- exact file/evidence location;
- concrete failure or unsafe state;
- blast radius;
- smallest complete production correction; and
- verification needed to prove the correction.

Critical and Important findings block the release. A Minor finding blocks only when it demonstrates
a violated correctness, safety, bounded-resource, operability, or release-truth predicate. Cosmetic
preference and already-covered nitpicks are recorded once as nonblocking and do not trigger a
rebuild loop.

Any source remediation creates a new candidate. All exact-head evidence and the complete grouped
review must then be regenerated against that new commit.

## Current result

| Predicate | Result |
| --- | --- |
| Candidate frozen | No |
| Provider evidence complete | No |
| Exact-head evidence set complete | No |
| Review groups complete | 0 of 5 |
| Unresolved blocking findings | Not yet assessed |
| Publication authorized | No |

The release remains blocked. This record makes no performance, provider, security, or completion
claim.

## Related evidence

- [Exact-head gate](../verification/usable-release-gate.md)
- [Demonstration methodology](../verification/usable-release-demonstration.md)
- [Performance methodology](../verification/usable-release-performance.md)
- [Quality attributes](../architecture/quality-attributes.md)
- [Delivery ledger](../plans/delivery-ledger.md)
