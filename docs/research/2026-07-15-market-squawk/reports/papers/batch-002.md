# Papers Batch 002 Deep Dive

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

This report reviews only papers 004–006 assigned in `papers-batch-002`, as of
**2026-07-15**. It focuses on three Market Squawk decisions: how to qualify a calibrated execution
simulator; how to enforce point-in-time data use; and how to prevent future survival from defining a
historical universe. Factual claims are labeled **Confirmed**; design mappings are **Inferences**.

**Confirmed.** Evidence maturity differs materially. Brown et al. is a peer-reviewed 1992 journal
article. The two 2026 papers are arXiv v1 preprints. Fonseca was submitted only nine days before
this cutoff and is marked “Submitted to ACM Transactions on Software Engineering and Methodology”;
its formal and artifact results are therefore **provisional author claims**, not peer-reviewed or
independently replicated findings ([arXiv record](https://arxiv.org/abs/2607.04958)).

## Sources Reviewed

| ID | Source | Method | Quality assessment |
| --- | --- | --- | --- |
| papers-004 | Noble, Rosenbaum, Souilmi, [*Bridging the Reality Gap in Limit Order Book Simulation*](https://arxiv.org/pdf/2603.24137) | Projects a large-tick LOB onto spread/imbalance; estimates conditional events, sizes, and times; adds signed-flow impact feedback | Detailed primary preprint, but limited to four large-tick U.S. stocks, one vendor, and illustrative parameters |
| papers-005 | Fonseca, [*Look-Ahead-Freedom as Temporal Non-Interference*](https://arxiv.org/html/2607.04958) | Defines two-run temporal non-interference and a type/effect checker for value-independent availability | Extremely recent single-author preprint; proprietary validation data unavailable and deployed-code integration remains future work |
| papers-006 | Brown, Goetzmann, Ibbotson, Ross, [*Survivorship Bias in Performance Studies*](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Derives selection-induced persistence and runs 20,000 simulated mutual-fund-manager experiments | Peer-reviewed primary research; durable mechanism, but old and specific to manager-performance studies |

## Findings

### Calibrated simulation

**Confirmed.** Noble et al. retain a queue-reactive simulator but (1) project state onto best-level
volume imbalance and spread, (2) replace exponential event timing with an empirical or fitted
conditional distribution, and (3) add a power-law-decaying signed-trade-flow state that biases later
trades toward mean reversion. They describe the method as “project, estimate, validate, adapt,” not
as a universal parameterization ([Noble et al., §§1–4](https://arxiv.org/pdf/2603.24137)).

**Confirmed.** Calibration uses Databento MBP-10 data from December 2023 through December 2025 for
INTC, VZ, T, and PFE, excluding the first/last 30 minutes. Validation compares event mix, imbalance
before trades, hourly activity, five-minute volatility, and returns. The reported return center fits
better than the tails; the simulator omits some large moves driven by exogenous shocks or persistent
flow ([Noble et al., §§1, 2.7–2.8](https://arxiv.org/pdf/2603.24137)).

**Confirmed.** Inter-event times show a reported mode near 29 microseconds across the four names.
The authors interpret this as consistent with round-trip latency/races, but warn that fast events
also contain unrelated asynchronous traffic. Their race fill rule is an explicit proxy because full
participant-order and race data are proprietary ([Noble et al., §3](https://arxiv.org/pdf/2603.24137)).

**Confirmed.** The impact extension produces concave execution impact and partial reversion in the
reported metaorder simulation. In two strategy cases, including the strategy's own impact reduces
or flattens P&L as inventory/aggression increases. Parameters partly target an illustrative impact
curve; the paper says production calibration needs observed metaorders or an explicit theoretical
target ([Noble et al., §§4–5](https://arxiv.org/pdf/2603.24137)).

**Inference.** Market Squawk should treat a simulator as a versioned model bundle containing source
hashes, instrument/venue coverage, projection/bins, event and size rules, timing/fill/fee/impact
models, calibration and held-out intervals, seeds, code revision, and per-metric validation. Output
is always `Modeled`, never `DirectVerified`. The paper's paid data source must remain replaceable by
user-owned local captures; its parameters cannot be defaults for unsupported venues or asset classes.

### Point-in-time correctness and non-interference

**Confirmed (provisional).** Fonseca distinguishes reference time—what a datum describes—from
availability—when it becomes knowable. A decision at epoch `t` may use only values available by
`t`, even when their reference period is earlier. Look-ahead-freedom is defined by two executions
that agree on the available past but differ arbitrarily in the future: their decision at `t` must be
identical ([Fonseca, §§3 and 6](https://arxiv.org/html/2607.04958)).

**Confirmed (provisional).** The paper claims general freedom is Pi-0-1-complete when availability
may depend on data values, while a value-independent fragment admits a sound checker linear in
pipeline size under bounded availability-expression complexity. Effects conservatively retain the
latest availability of influencing inputs; re-stamping cannot erase a future dependency
([Fonseca, §§4, 7, 8.1](https://arxiv.org/html/2607.04958)).

**Confirmed (provisional).** Point-in-time reads expose only store versions available by the as-of
epoch. Accepted pipelines also require availability-monotone sources and causal windows, joins, and
resampling. A joined row becomes available with its latest component. However, correct source stamps
are trusted, and the guarantee applies only to pipelines faithfully represented in the calculus
([Fonseca, §§4–6 and 9](https://arxiv.org/html/2607.04958)).

**Confirmed (provisional).** The paper reports that an independent dynamic oracle found no witness
against accepted pipelines and that the checker caught planted leaks missed by sampled
differential/tiling detectors. It also reports conservative false positives. The corpus is small and
adversarial, proprietary validation data are not redistributable, and the planned artifact provides
synthetic stand-ins rather than those data ([Fonseca, §8 and Data and Code Availability](https://arxiv.org/html/2607.04958)).

**Inference.** Market Squawk should adopt the semantics without taking a release-critical dependency
on this new checker. Records should retain `effective_at`, `published_at`, `available_at`,
`ingested_at`, revision, and `superseded_at`; strict builds select only versions with
`available_at <= decision_at`. Transformations inherit at least the maximum availability of all
inputs. Unknown/ambiguous availability should fail closed or use an explicit conservative policy.

### Survivorship

**Confirmed.** Brown et al. show that risk dispersion plus selection on survival can create apparent
performance persistence even when generated manager returns are serially uncorrelated. Simply
dropping a fund after disappearance is insufficient if it first had to survive an evaluation
window; freedom from bias would require termination/sample loss to be unrelated to performance
([Brown et al., introduction and §§1–2](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).

**Confirmed.** Their experiment starts with 600 managers, draws four annual returns, removes the
worst performers under 0%, 5%, 10%, or 20% annual cuts, and repeats 20,000 times. The mean
cross-sectional persistence t-value is approximately zero with no cut and 2.046 with a 5% cut; at
that cut, the no-persistence null is rejected in at least half the replications under the studied
test ([Brown et al., §2 and Table 5](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).

**Confirmed.** In the 5–10% examples, average risk-adjusted return changes only about 0.4–0.6
percentage points annually while dependence tests distort strongly. Adjustments depend on the
selection rule, risk dispersion, cross-sectional correlation, and serial behavior; the paper does
not establish a universal correction or claim survivorship explains every anomaly
([Brown et al., §§2–3 and Appendix](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).

**Inference.** Historical universe membership must be versioned independently of instrument
existence, listing, provider coverage, and tradability. Market Squawk should retain dead entities,
identifier/symbol histories, exit reasons, terminal observations/cash proceeds, and corporate-action
transitions. Today's survivors must never be the implicit historical universe; a survivor-only
analysis must be labeled and accompanied by attrition diagnostics.

### Implementation and test implications

The following are **Inferences**:

1. Qualify simulators on held-out event mix, spread/depth, size, timing, volatility, return tails,
   fill/partial-fill rate, impact, and reversion; one aggregate score cannot hide a failed axis.
2. Reject a simulation bundle whose coverage, tick/lot semantics, schema, or calibration artifacts
   do not match the requested run. Never silently borrow another asset's parameters.
3. At each decision epoch, perturb/delete every future-available record and assert identical
   universe, features, labels, predictions, and orders through that epoch. This is a useful bug test,
   not a proof of universal freedom.
4. Plant failures for latest-revision reads, centered windows, negative lags, next-bar fills,
   post-close bars, re-stamped future values, and unbounded MCP/retrieval results.
5. Add a future restatement or delisting while preserving earlier data; all pre-availability outputs
   must remain unchanged. Membership published later cannot affect an earlier decision even if its
   effective date is earlier.
6. Preserve mergers, delistings, liquidations, symbol changes, rolls, and coverage exits as append-only
   lifecycle facts. Missing terminal data must trigger an explicit reconciliation policy, not deletion.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** The LOB model estimates conditional events/sizes/times from projected spread and imbalance | [Noble et al., §§2–3](https://arxiv.org/pdf/2603.24137) | Equations, preprocessing, and estimation procedure | High | Projection loses full-book detail |
| **Confirmed:** Validation is multi-metric and still reveals lighter return tails | [Noble et al., §2.8](https://arxiv.org/pdf/2603.24137) | Empirical/simulated event, volume, volatility, and return comparisons | High | Narrow four-stock sample |
| **Confirmed:** Race fills and impact parameters require calibration/proxies | [Noble et al., §§3–4](https://arxiv.org/pdf/2603.24137) | Proprietary-data limitation and illustrative target disclosed | High | Not universal venue evidence |
| **Inference:** Simulator outputs remain `Modeled` and bundles are coverage/version bound | [Noble et al.](https://arxiv.org/pdf/2603.24137) | Outputs depend on projected stochastic assumptions | High | Market Squawk mapping |
| **Confirmed, provisional:** Admissibility depends on availability, not reference time | [Fonseca, §§3 and 6](https://arxiv.org/html/2607.04958) | Time-indexed calculus and two-run definition | Medium-high | Unreviewed v1 theorem framework |
| **Confirmed, provisional:** Causal joins/windows/vintages preserve availability bounds | [Fonseca, §§4–6](https://arxiv.org/html/2607.04958) | Effect and operational rules | Medium-high | Trusts source stamps |
| **Confirmed:** Fonseca is v1 submitted July 6, 2026, with TOSEM submission only | [arXiv record](https://arxiv.org/abs/2607.04958) | Submission history/comments | High | No acceptance stated |
| **Inference:** Implement schemas/tests now; defer formal-checker dependency | [Fonseca, §9](https://arxiv.org/html/2607.04958) | Deployed-code extraction is future work | High | Maturity-calibrated recommendation |
| **Confirmed:** Survival truncation can manufacture persistence from uncorrelated returns | [Brown et al., §§1–2](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Analytic mechanism and 20,000 simulations | High | Peer-reviewed, fund-specific setup |
| **Confirmed:** Dropping entities after evaluation does not itself remove selection bias | [Brown et al., note 7](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Counterexample conditions on survival through evaluation | High | Direct dataset-builder relevance |
| **Confirmed:** A small mean-return effect can coexist with large dependence-test distortion | [Brown et al., §2](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | 5–10% cutoff results | High | Mean checks are insufficient |
| **Inference:** Historical universes must retain dead entities and time-varying membership | [Brown et al.](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Bias arises from later survival conditioning | High | Software translation |

## Source-Specific Notes

- **Papers-004 — Confirmed:** Best used as a calibration/validation blueprint, not a source of
  portable latency or impact constants. It does not validate crypto, derivatives, small-tick assets,
  auctions, halts, or order-level queue identity ([paper](https://arxiv.org/pdf/2603.24137)).
- **Papers-005 — Inference:** Best used now to sharpen schemas and leak fixtures. Formal
  certification should wait for peer/artifact review, deployed-code mapping, and independent
  reproduction; release 0.1 should not claim it ([paper](https://arxiv.org/html/2607.04958)).
- **Papers-006 — Confirmed:** Supplies a durable sample-selection mechanism, not a modern instrument
  schema or universal numeric adjustment ([paper](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).

## Cross-Source Patterns

**Inference.** All three sources favor immutable, versioned evidence over mutable “latest” state:
simulation parameters need calibration versions; facts need availability-aware vintages; universes
need historical membership and exit facts. Validation must also be multi-axis: average volatility,
detector silence, or average return cannot substitute for tail/fill fidelity, non-interference, or
unbiased rankings.

**Inference.** These capabilities remain outside live qualification. Simulated fills and historical
point-in-time data cannot become `DirectVerified` and cannot bypass risk merely because they produce
a plausible price or decision.

## Limitations and Non-Findings

- **Confirmed:** Papers-004 and -005 are arXiv v1 preprints; papers-005 is especially provisional.
- **Confirmed:** Papers-004 uses one vendor and four large-tick stocks; it does not establish broad
  venue/asset calibration or a free mandatory data source.
- **Confirmed:** Papers-005 trusts correct stamps and faithful calculus representation; it does not
  certify arbitrary Rust, SQL, Python, MCP, or model-weight behavior.
- **Confirmed:** Papers-006 is a manager-performance study and explicitly finds no universal
  survivorship correction or direction under every selection rule.
- **Inference:** This batch establishes no implemented Market Squawk capability and no measured
  throughput, latency, memory, or backtest performance.
- **Inference:** It provides no support for account rotation, fingerprint spoofing, CAPTCHA bypass,
  proxy rotation, or quota/blocking evasion; those behaviors remain out of scope.

## Source List

1. Noble, Patrick; Rosenbaum, Mathieu; Souilmi, Saad. “Bridging the Reality Gap in Limit Order Book
   Simulation.” arXiv:2603.24137v1, 2026. [Record](https://arxiv.org/abs/2603.24137) ·
   [PDF](https://arxiv.org/pdf/2603.24137). Accessed 2026-07-15.
2. Fonseca, Xavier. “Look-Ahead-Freedom as Temporal Non-Interference: A Verifiable Correctness
   Property for Backtesting and Agentic Trading Pipelines.” arXiv:2607.04958v1, 2026.
   [Record](https://arxiv.org/abs/2607.04958) · [Full text](https://arxiv.org/html/2607.04958).
   Accessed 2026-07-15.
3. Brown, Stephen J.; Goetzmann, William; Ibbotson, Roger G.; Ross, Stephen A. “Survivorship Bias
   in Performance Studies.” *Review of Financial Studies* 5(4), 553–580, 1992.
   [PDF](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf).
   Accessed 2026-07-15.
