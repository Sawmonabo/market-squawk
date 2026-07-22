# Fair-value rules source and decision record

As of: 2026-07-22
Ruleset implemented: `asc820-ifrs13-converged-v1`

## Primary sources

- The [FASB Codification access page](https://fasb.org/page/PageContent?isStaticPage=true&pageId=%2Fstaticpages%2Fcodification-access.html)
  identifies Basic View as free access to the authoritative Codification. FASB also states that the
  Codification, rather than an Accounting Standards Update, is authoritative GAAP.
- [FASB ASU 2011-04](https://storage.fasb.org/ASU2011-04.pdf) publicly records the converged
  Topic 820 text used here: the active-market definition; classification by the lowest significant
  input; Level 1's identical/unadjusted/active/accessible/measurement-date tests; and the rule that
  adjusted Level 1 inputs move to a lower level.
- [FASB ASU 2022-03](https://storage.fasb.org/ASU%202022-03.pdf) clarifies that an
  entity-specific contractual restriction on selling an equity security is not part of the unit of
  account and is not itself a price adjustment. The 2026 FASB proposal concerning investment
  companies and those restrictions was still proposed, not authoritative, on the as-of date and is
  therefore not encoded in v1.
- The [IFRS 13 standard page](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/)
  supplies the current standard status and exit-price objective. The IFRS Foundation's
  [IFRS 13 supporting-material page](https://www.ifrs.org/supporting-implementation/supporting-materials-by-ifrs-standards/ifrs-13/)
  links the Standard and agenda decisions.
- The IFRS Interpretations Committee's
  [third-party consensus-price agenda decision](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html)
  confirms that a third-party price is Level 1 only when the measurement relies solely on an
  unadjusted active-market quote for an identical instrument accessible at the measurement date.

## Encoded decisions

1. V1 evaluates every Level 1 predicate and retains the complete per-input truth table. Absence or
   uncertainty never defaults to `Level1`.
2. The measurement follows its lowest significant input. Observable non-Level-1 inputs classify as
   Level 2; significant unobservable inputs classify as Level 3; incomplete or invalid evidence is
   `Unclassified`.
3. Similar/proxy instruments, inactive or inaccessible markets, adjusted values, delayed or
   aggregated delivery, modeled/estimated values, stale/quarantined evidence, and post-measurement
   observations cannot be silently promoted to Level 1. Observable adjustments may support Level 2;
   significant unobservable adjustments support Level 3.
4. `DataQuality`, `MarketDepth`, and `FairValueHierarchy` remain separate types. Fair-value evidence
   verification is its own analytical contract, and a valuation decision or approval carries no
   execution capability.
5. Overrides do not rewrite source evidence or the rules decision. They create a new content-bound
   decision requiring a separately identified approver, with explicit expiry and immutable
   revocation/audit records.
6. The ruleset version and hash bind these semantics and the quote-age policy. A future standards
   change requires a new code-owned ruleset version and source review; it cannot mutate v1 history.
