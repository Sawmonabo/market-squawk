# Papers Batch 004 Deep Dive

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This report reviews only papers-010, as of **2026-07-15**. It evaluates what the source supports
for Market Squawk's fair-value architecture while keeping six concepts separate: hierarchy
classification, empirical value relevance, valuation-input observability, process/model uncertainty,
data quality, and live execution eligibility. Factual claims are **Confirmed**; product mappings and
tests are **Inferences**.

## Sources Reviewed

| Source | Authors / venue / date | Problem, method, result | Quality assessment |
| --- | --- | --- | --- |
| [*Convergence in Motion: A Review of Fair Value Levels’ Relevance*](https://doi.org/10.1080/17449480.2021.1912370) | Andrei Filip, Ahmad Hammami, Zhongwei Huang, Anne Jeny, Michel Magnan, Rucsandra Moldovan; *Accounting in Europe* 18(3), 275–294; online 2021-04-27 | Meta-analysis of comparable value-relevance studies plus practitioner interviews; synthesizes differences among fair-value hierarchy levels and possible explanations | Peer-reviewed post-IFRS-13 academic synthesis with disclosed funding/context and limitations; not an accounting standard, legal opinion, or classification rules engine |

## Findings

**Confirmed.** The paper studies **value relevance**: the extent to which reported fair-value
amounts are associated with market pricing. Its synthesis finds Level 3 measurements less value
relevant overall than Levels 1 and 2, while reporting improvement over time
([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

**Confirmed.** The authors do not reduce the relevance gap to a single hierarchy label. Their
review and interviews identify asset fundamentals, model risk, and measurement-process complexity
as possible contributors. The interview component is small and Canadian, so it supplies contextual
explanations rather than a broadly representative causal test
([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

**Confirmed.** Value relevance is not the same property as classification correctness, measurement
accuracy, standards compliance, or audit sufficiency. An association with equity prices cannot
prove that an individual valuation used the correct level or method
([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

**Confirmed.** The paper discloses IASB-linked funding/research context and states that its views are
not IASB policy. It therefore cannot replace authoritative, current ASC 820 or IFRS 13 text and
interpretive guidance ([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

**Inference.** Market Squawk should store hierarchy classification as its own typed analytical
result: level, measurement date, identical/similar instrument evidence, market/access evidence,
input observability, method, classification reason, ruleset version, override, approval, and
supporting artifacts. Missing evidence should produce `Unclassified`, not a guessed level.

**Inference.** Observability belongs to the valuation **input** and classification evidence. Model
and process uncertainty belong to the valuation method and governance record. Empirical value
relevance belongs to research output. None should be compressed into a single “confidence” score;
the paper's proposed explanations show why the dimensions can move independently.

**Inference.** `FairValueHierarchy`, `DataQuality`, and `MarketDepth` must remain separate types.
A Level 2 or Level 3 estimate may be analytically useful yet remain `Modeled` or `Estimated`.
Conversely, direct delivery and a valid book do not by themselves determine an accounting hierarchy
classification; that decision requires measurement-specific evidence and authoritative rules.

**Inference.** Execution eligibility is a fourth, explicit decision. Even a Level 1 classification
must not imply `DirectVerified` live data: immediate automation still requires known venue and
instrument, authorized direct delivery, sequence/snapshot/checksum integrity, valid timestamps,
freshness, status, precision, and coverage. A delayed, stale, adjusted, proxy, or modeled valuation
cannot be promoted to execution quality because it produces a plausible price.

**Inference.** Implement the following tests:

1. Compile-time/API tests prevent implicit conversion among hierarchy, depth, quality, and execution
   eligibility.
2. Re-running the same evidence under the same ruleset produces the same classification and
   explanation; changing a ruleset creates a new result rather than overwriting history.
3. Changing `DataQuality` alone cannot change hierarchy; changing hierarchy alone cannot grant
   execution eligibility.
4. A Level 2/3 modeled input always fails a “Level 1 evidence” assertion and a default automated
   execution gate.
5. A Level 1 record that is stale, delayed, adjusted, sequence-invalid, or coverage-ambiguous remains
   ineligible for automated action.
6. Overrides are append-only, preserve the original classification, and require reason, approver,
   timestamp, and ruleset version.
7. Missing identical-instrument, active/accessibility, measurement-date, or adjustment evidence
   yields `Unclassified` until authoritative rules justify otherwise.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** Level 3 is less value relevant overall than Levels 1 and 2 | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Meta-analysis synthesis | High for reported association | Not classification correctness |
| **Confirmed:** Reported Level 3 relevance improves over time | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Time-oriented synthesis | Medium-high | Does not establish why |
| **Confirmed:** Asset fundamentals, model risk, and process complexity are possible contributors | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Review plus practitioner interviews | Medium | Explanatory/contextual, not causal proof |
| **Confirmed:** The paper is not authoritative IASB policy | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Funding/context disclaimer | High | ASC 820/IFRS 13 rules require official sources |
| **Inference:** Classification, observability, method uncertainty, and value relevance need separate fields | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Multiple dimensions may explain relevance differences | High | Architecture mapping |
| **Inference:** Hierarchy cannot determine data quality or execution eligibility | [Filip et al.](https://doi.org/10.1080/17449480.2021.1912370) | Paper concerns valuation relevance, not feed integrity | High | Enforces domain separation |

## Source-Specific Notes

**Inference.** The paper is useful for schema separation and uncertainty disclosure, not for coding
the actual ASC 820/IFRS 13 decision tree. Market Squawk's classification service must cite a
versioned authoritative ruleset; this paper may remain research rationale and validation context.

## Cross-Source Patterns

**Inference.** The strongest implementation lesson is non-substitutability: a hierarchy level does
not answer whether a price is market-relevant, precisely measured, operationally reliable, current,
or safe for execution. Each question needs its own evidence, result type, explanation, and audit
trail.

## Limitations and Non-Findings

- **Confirmed:** The study reports associations and practitioner explanations, not causal proof for
  why one hierarchy level is more or less value relevant.
- **Confirmed:** The interview sample is small and Canadian; generalization is limited.
- **Confirmed:** The source is neither ASC 820 nor IFRS 13 and does not establish current binding
  classification, disclosure, override, approval, or measurement rules.
- **Inference:** It does not validate any Market Squawk valuation, classifier, market-data adapter,
  data-quality transition, or execution gate.
- **Inference:** It provides no basis for treating Level 2/3 values as Level 1 evidence or
  `DirectVerified` data, and no support for quota/access-control evasion.

## Source List

1. Filip, Andrei; Hammami, Ahmad; Huang, Zhongwei; Jeny, Anne; Magnan, Michel; Moldovan,
   Rucsandra. “Convergence in Motion: A Review of Fair Value Levels’ Relevance.” *Accounting in
   Europe* 18(3), 275–294, 2021. [Publisher DOI](https://doi.org/10.1080/17449480.2021.1912370).
   Accessed 2026-07-15.
