# Reputable Sources Batch 002 Deep Dive

## Table of Contents

1. [Batch Scope](#batch-scope)
2. [Sources Reviewed](#sources-reviewed)
3. [Findings](#findings)
4. [Evidence Table](#evidence-table)
5. [Source-Specific Notes](#source-specific-notes)
6. [Cross-Source Patterns](#cross-source-patterns)
7. [Limitations and Non-Findings](#limitations-and-non-findings)
8. [Source List](#source-list)

## Batch Scope

This review covers only the Federal Reserve/OCC/FDIC's April 2026 revised model-risk
guidance and the Basel Committee's October 2018 stress-testing principles. It determines their
current status and applicability, extracts governance and validation practices, and translates
them into bounded tests for Market Squawk. “Confirmed” below means directly supported by an
assigned primary source. “Inference” means a proposed local design control derived from those
principles, not a requirement imposed on Market Squawk.

## Sources Reviewed

1. Federal Reserve, OCC, and FDIC, *Revised Guidance on Model Risk Management*,
   SR 26-2, issued April 17, 2026, including the joint-agency attachment.
2. Basel Committee on Banking Supervision, *Stress testing principles*, issued October 17,
   2018, including the full guideline.

## Findings

### Status and applicability

**Confirmed.** SR 26-2 was issued April 17, 2026 and supersedes SR 11-7 (2011) and
SR 21-8 (2021). It is expected to be most relevant to Federal Reserve-regulated banking
organizations with more than $30 billion in total assets. The attachment says the guidance does
not create enforceable or prescriptive requirements and that non-compliance alone is not a basis
for supervisory criticism. Its model definition excludes simple arithmetic and deterministic
rule-based processes; generative and agentic AI are expressly outside its scope.

**Confirmed.** The BIS landing page marks the 2018 principles “Current”; they replace the May
2009 principles. The document is a set of Guidelines, not Basel Standards. It is primarily
directed to large internationally active banks and authorities, while encouraging proportionate
use by smaller institutions.

**Inference.** Market Squawk is local research/execution software, not a bank, banking
organization, or supervisory authority. These sources therefore provide credible engineering
patterns, not direct legal obligations or a valid basis for claiming regulatory compliance.

### Proportional model governance, validation, change, and limits

**Confirmed.** SR 26-2 frames model risk through inherent risk—assumptions, complexity, input
quality, and data constraints—and materiality, determined by purpose plus exposure. Higher-risk
or more material use warrants greater rigor. It calls for clear purpose and intended use,
out-of-sample/out-of-time and alternative-assumption testing where appropriate, data-quality
assessment, effective challenge by objective experts, and assessment of aggregate dependencies
through common assumptions, data, and methods.

Validation should normally precede first use and cover conceptual soundness, outcomes analysis
(including backtesting or outlier analysis), and ongoing monitoring. Frequency depends in part
on the frequency and scope of changes. Material deviation from performance thresholds can
require adjustment, recalibration, or redevelopment. Urgent use before validation calls for
disclosed limitations, constrained use, and closer monitoring. A use beyond the original purpose
calls for further analysis and control review. Model inventories should provide enough information
to understand individual and aggregate risk. Vendor models remain subject to these principles.

**Inference.** A Market Squawk model bundle should record immutable artifact and data hashes,
model/feature versions, purpose, permitted universe and decisions, input schema and quality,
training/calibration period, assumptions, dependencies, metrics and thresholds, limitations,
fallback behavior, validation evidence and status, reviewer, and change history. Its local risk tier
should scale required tests to use and exposure. An unvalidated, changed, out-of-scope, stale, or
threshold-breaching bundle should be unavailable for automated action; inference failure should
produce no action. These are design choices, not wording mandated by SR 26-2.

### Stress governance, scenarios, aggregation, and use

**Confirmed.** Basel's nine principles require documented objectives and governance; use of
results in risk and business decisions; material-risk coverage and sufficiently severe stresses;
adequate resources; accurate, granular data and robust IT; fit-for-purpose methods; regular
challenge/review; and communication. Roles should cover scenario approval, model development
and validation, reporting, challenge, and result use.

Scenarios should capture material and relevant risks, keep key variables internally consistent,
state a narrative, explain exclusions, and be severe, varied, and plausible. Historical and
hypothetical events—including emerging risks—may be used; reverse stress can expose core
vulnerabilities. Data sources, processing, and aggregation should be consistent and capable of
capturing all material risk. Model linkages and interactions among risk types should be considered,
and overlays or expert judgments should be justified, documented, and challenged. Results should
be reported at relevant aggregation levels with assumptions and limitations, then used where
appropriate to inform risk appetite and limits. Review can include validation, backtesting,
benchmarking, and sensitivity to assumptions.

**Inference.** Each Market Squawk scenario should have an ID/version/hash, objective, as-of
time, horizon, narrative, shocks, severity rationale, scope/exclusions, dependencies/correlations,
aggregation policy, data/model versions, overlays, limitations, approval, and output artifact. Runs
should be reproducible and reconcile instrument-to-portfolio totals. Tests should reject missing
material exposures or inconsistent units/currencies; verify shock propagation and correlated-risk
aggregation; compare baseline with stressed outcomes; exercise reverse-stress thresholds; and
show that overrides, changed inputs, or model changes invalidate prior approval.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| SR 26-2 is dated April 17, 2026 and replaces SR 11-7 and SR 21-8. | [Federal Reserve SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm) | Letter date and supersession notice. | High | Confirmed. |
| SR 26-2 is most relevant above $30 billion and is non-prescriptive supervisory guidance. | [Joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf) | Scope and legal-effect discussion, pp. 1–2. | High | Confirmed; not directly applicable to Market Squawk. |
| Model-risk rigor should scale with inherent risk, purpose, exposure, and materiality. | [Joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf) | Model-risk framework, pp. 3–4. | High | Confirmed. |
| Validation generally precedes use and can constrain unvalidated use. | [Joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf) | Validation timing and interim controls, pp. 6–7. | High | Confirmed. |
| Change scope, performance thresholds, and changed conditions affect revalidation or redevelopment. | [Joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf) | Validation and monitoring, pp. 6–9. | High | Confirmed. |
| A model inventory should support individual and aggregate model-risk understanding. | [Joint-agency attachment](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf) | Model inventory, p. 10. | High | Confirmed. |
| Basel's 2018 principles are current Guidelines replacing the 2009 principles. | [BIS publication page](https://www.bis.org/bcbs/publ/d450.htm) | Status, date, and replacement metadata. | High | Confirmed as of review date. |
| Stress scenarios should cover material risks and be internally consistent, severe, varied, and plausible. | [BCBS stress-testing principles](https://www.bis.org/bcbs/publ/d450.pdf) | Principle 4, p. 7 of the report. | High | Confirmed. |
| Stress infrastructure should preserve granular, accurate data and consistent aggregation of material risk. | [BCBS stress-testing principles](https://www.bis.org/bcbs/publ/d450.pdf) | Principle 6, pp. 8–9 of the report. | High | Confirmed. |
| Models, overlays, scenarios, and results should be documented and credibly challenged. | [BCBS stress-testing principles](https://www.bis.org/bcbs/publ/d450.pdf) | Principles 7–8, pp. 9–10 of the report. | High | Confirmed. |
| Bundle gating and scenario-manifest tests are appropriate local controls. | Both sources | Translation of purpose, inventory, validation, coverage, aggregation, and challenge principles. | Medium | Inference; proposed implementation, not source text. |

## Source-Specific Notes

### Federal Reserve/OCC/FDIC SR 26-2

The revision materially matters to citations: new work should not present SR 11-7 or SR 21-8 as
the current joint-agency model-risk guidance. Its proportionality and non-prescriptive language
also caution against converting every supervisory practice into a universal software requirement.
The exclusion of deterministic rules and generative/agentic AI means Market Squawk must define
its own broader registry categories if it inventories those tools alongside statistical models.

### Basel Committee stress-testing principles

The document is intentionally high level. Its strongest software implications concern traceable
objectives, scenario coverage, coherent data aggregation, fit-for-purpose methods, disclosure of
assumptions/limitations, and recurring challenge. It does not prescribe particular market shocks,
portfolio loss formulas, scenario probabilities, or pass/fail thresholds.

## Cross-Source Patterns

Both sources make purpose and intended use the organizing constraint; scale rigor by materiality
and complexity; require inventories, documentation, monitoring, and challenge; and treat model
limitations as inputs to decisions rather than footnotes. Both also elevate aggregate risk: shared
data or assumptions can impair multiple models, while stress results can be misleading if material
exposures or cross-risk linkages disappear during aggregation. For Market Squawk, this supports
one versioned lineage chain from source data through model/scenario configuration to a bounded
decision artifact, with risk controls remaining authoritative over model output.

## Limitations and Non-Findings

- Neither source directly regulates Market Squawk or certifies a software architecture.
- Neither establishes live-market-data verification, execution eligibility, fair-value hierarchy,
  cybersecurity, or event-path latency requirements.
- Basel gives no universal shock magnitudes, probabilities, aggregation formula, or capital limit.
- SR 26-2 does not cover all analytical software, deterministic rules, or generative/agentic AI.
- This batch did not evaluate statutes, SEC/FINRA rules, accounting standards, or jurisdictional
  applicability. Legal or compliance conclusions require separate qualified analysis.
- No performance, correctness, or regulatory-compliance claim can be made until Market Squawk's
  inferred controls are implemented and tested.

## Source List

1. Federal Reserve, [SR 26-2: Revised Guidance on Model Risk Management](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm), April 17, 2026.
2. Federal Reserve/OCC/FDIC, [Supervisory Guidance on Model Risk Management (attachment)](https://www.federalreserve.gov/supervisionreg/srletters/SR2602a1.pdf), April 17, 2026.
3. Basel Committee on Banking Supervision, [Stress testing principles: publication page](https://www.bis.org/bcbs/publ/d450.htm), October 17, 2018.
4. Basel Committee on Banking Supervision, [Stress testing principles (full guideline)](https://www.bis.org/bcbs/publ/d450.pdf), October 2018.
