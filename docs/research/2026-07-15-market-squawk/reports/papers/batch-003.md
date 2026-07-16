# Papers Batch 003 Deep Dive

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

This report reviews only papers 007–009 assigned in `papers-batch-003`, as of
**2026-07-15**. It asks how Market Squawk should represent strategy-selection risk, implement
Expected Shortfall (ES) correctly for discrete scenarios, and use execution-cost theory without
mistaking a stylized optimizer for a realistic paper broker. Factual claims are **Confirmed**;
architecture mappings and proposed tests are **Inferences**.

## Sources Reviewed

| ID | Source | Method and result | Quality / access assessment |
| --- | --- | --- | --- |
| papers-007 | Bailey, Borwein, López de Prado, Zhu, [*The probability of backtest overfitting*](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Proposes a general PBO framework and model-free, nonparametric combinatorially symmetric cross-validation (CSCV); reports reasonable estimates in examples | Peer-reviewed *Journal of Computational Finance* article, DOI 10.21314/JCF.2016.322; assigned publisher page exposes metadata and abstract, not full method text |
| papers-008 | Acerbi, Tasche, [*On the coherence of Expected Shortfall*](https://arxiv.org/pdf/cond-mat/0104295) | Compares ES/TCE/WCE/CVaR definitions and proves a distribution-robust coherent ES, including discontinuous losses | Full open v5 manuscript with *Journal of Banking & Finance* 26 (2002) reference; strongest directly inspectable source in this batch |
| papers-009 | Almgren, Chriss, [*Optimal execution of portfolio transactions*](https://doi.org/10.21314/JOR.2001.041) | Trades expected transaction cost from permanent/temporary impact against volatility uncertainty; constructs a linear-cost efficient frontier | Peer-reviewed *Journal of Risk* 3(2), 5–39; assigned publisher page exposes metadata and abstract only |

## Findings

### Strategy-selection risk and PBO

**Confirmed.** Bailey et al. argue that ordinary hold-out techniques can be unreliable for
investment backtests and propose a general probability-of-backtest-overfitting framework. Their
generic implementations are model-free and nonparametric CSCV procedures; the publisher reports
reasonable PBO estimates with relatively small errors in the paper's examples
([publisher abstract](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting)).

**Inference.** PBO addresses repeated selection, not data correctness. Market Squawk should compute
selection-risk diagnostics only after point-in-time joins, universe controls, fees, and paper-fill
assumptions pass their own checks. A low PBO estimate cannot repair look-ahead, survivorship,
unrealistic fills, or regime mismatch.

**Inference.** Every strategy/model search must persist the complete candidate set—not only the
winner—plus dataset/feature versions, objective metric, parameter grid, split configuration, seeds,
execution assumptions, and all in/out-of-sample scores. A PBO result without the trial population
and deterministic recipe should be rejected as unreproducible. Tests should verify permutation
invariance of candidate order, deterministic recomputation, complete split coverage, and explicit
failure for NaNs, too few observations/candidates, or inconsistent score horizons.

### Coherent ES for discrete scenarios

**Confirmed.** Acerbi and Tasche show that ES variants coincide for continuous distributions but
can differ at discontinuities. Simply averaging all losses “beyond VaR” can lose coherence because
an atom at the quantile may contain more probability than the desired tail. Their ES is the average
of exactly the worst `alpha` probability mass and remains coherent for general distributions
([Acerbi and Tasche, §§1–3](https://arxiv.org/pdf/cond-mat/0104295)).

**Confirmed.** With profit/loss variable `X` and lower-tail probability `alpha`, their definition
includes all outcomes strictly below a valid quantile and only the fraction of probability at that
quantile needed to reach mass `alpha`. Equivalently, ES is the negative integral of lower quantiles
from zero to `alpha`, divided by `alpha`. It is independent of which valid quantile representative
is selected and equals the paper's CVaR definition ([Acerbi and Tasche, Definition 2.6, Proposition
3.2, Corollary 4.3](https://arxiv.org/pdf/cond-mat/0104295)).

**Confirmed.** The resulting measure is monotone, subadditive, positively homogeneous, and
translation invariant. It is continuous in `alpha` and becomes no larger when the lower tail is
widened. The paper's discrete three-state counterexample shows VaR and two tail-conditional
expectation variants can violate subadditivity, while ES retains the exact boundary mass
([Acerbi and Tasche, Proposition 3.1, Corollary 3.3, Example 5.4](https://arxiv.org/pdf/cond-mat/0104295)).

**Confirmed.** For independent samples, the average of the worst `floor(n * alpha)` ordered profits
converges to tail mean under the stated integrability assumptions, whereas a single order-statistic
quantile can fail to converge when the quantile is non-unique
([Acerbi and Tasche, Proposition 4.1](https://arxiv.org/pdf/cond-mat/0104295)).

**Inference.** Market Squawk should expose a loss-positive API with both `confidence` and
`tail_probability = 1 - confidence` recorded to prevent sign/tail inversion. For weighted discrete
loss scenarios, sort worst-first, take every scenario strictly worse than VaR, then take exactly the
fractional boundary weight needed to reach the tail probability. Use checked decimal weights and
amounts; return errors for empty samples, nonpositive/unnormalized weights, nonfinite statistical
inputs, or confidence outside the supported open interval.

**Inference.** Required ES fixtures should cover: a point mass at VaR; tied boundary losses;
`n * tail_probability` both integral and fractional; unequal weights; input-order and duplicate-row
invariance; all-equal losses; and scenarios with fewer than one full observation of tail mass.
Property tests should exercise translation invariance, positive homogeneity, monotonicity, and
subadditivity within arithmetic tolerance. A naive “mean of rows whose loss is greater than or equal
to VaR” implementation must fail the boundary-atom fixture.

### Execution cost/risk and paper-execution limits

**Confirmed.** Almgren and Chriss minimize a combination of volatility uncertainty and transaction
costs from permanent and temporary market impact over time-dependent liquidation strategies. In the
linear cost model, they explicitly construct an efficient frontier: the lowest expected cost for a
given execution uncertainty. The publisher also states that schedules may be selected with
quadratic utility or VaR, yielding liquidity-adjusted VaR
([publisher abstract](https://doi.org/10.21314/JOR.2001.041)).

**Inference.** Market Squawk should use this as an auditable baseline schedule/cost model, not as a
fill engine. A versioned execution-model bundle should record horizon, time grid, target quantity,
volatility, temporary/permanent impact parameters, spread/fee additions, calibration window,
instrument/venue coverage, units, and uncertainty objective. Schedule tests must conserve quantity,
start at the requested inventory, finish at the required terminal inventory, respect lot rounding,
and reconcile every cost component.

**Inference.** The assigned source does not establish queue position, order-book depletion, partial
fills, latency, cancels, rejects, halts, venue fragmentation, state-dependent liquidity, or broker
order transitions. Realistic paper execution must model those independently and feed realized fills
back into positions, balances, fees, and risk. An optimal target schedule never authorizes an order:
each child intent still requires freshness/quality gates and pre-trade risk.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** CSCV is proposed as a model-free, nonparametric PBO framework | [Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Publisher abstract and “Need to know” summary | High | Full assigned text unavailable |
| **Confirmed:** The authors report reasonable PBO estimates in examples | [Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Publisher result summary | High for reported claim | No portable threshold established |
| **Inference:** Complete trial provenance is required for reproducible selection-risk analysis | [Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | PBO evaluates selection from backtested candidates | High | Architecture mapping |
| **Confirmed:** ES definitions diverge for discontinuous distributions | [Acerbi and Tasche, abstract](https://arxiv.org/pdf/cond-mat/0104295) | Paper identifies discontinuities as the definition boundary | High | Central directly proved result |
| **Confirmed:** Coherent ES includes only enough quantile-atom mass to complete the tail | [Acerbi and Tasche, Definition 2.6](https://arxiv.org/pdf/cond-mat/0104295) | Explicit correction term at the quantile | High | Critical discrete edge case |
| **Confirmed:** ES is coherent and equals the paper's CVaR | [Acerbi and Tasche, Proposition 3.1, Corollary 4.3](https://arxiv.org/pdf/cond-mat/0104295) | Coherence properties and equivalence are stated/proved | High | Uses the paper's sign/alpha convention |
| **Confirmed:** VaR/TCE can violate subadditivity in a discrete example | [Acerbi and Tasche, Example 5.4](https://arxiv.org/pdf/cond-mat/0104295) | Three-state counterexample | High | Motivates atom-aware tests |
| **Inference:** Weighted empirical ES needs exact boundary weighting and explicit tail convention | [Acerbi and Tasche](https://arxiv.org/pdf/cond-mat/0104295) | Direct translation of Definition 2.6 | High | Market Squawk API choice |
| **Confirmed:** Almgren–Chriss balances volatility uncertainty with permanent/temporary impact cost | [Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041) | Publisher abstract | High | Linear model is explicitly named |
| **Confirmed:** Linear costs permit an explicit efficient frontier | [Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041) | Publisher abstract | High | No claim of fill realism |
| **Inference:** The model is a schedule/cost baseline, not a paper broker | [Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041) | Abstract establishes only stylized cost/risk optimization | High | Other microstructure behavior needs separate evidence |
| **Inference:** PBO, ES, and execution cost are independent controls | [Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting); [Acerbi and Tasche](https://arxiv.org/pdf/cond-mat/0104295); [Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041) | Each addresses a different failure mode | High | No scalar should replace all three |

## Source-Specific Notes

- **Papers-007 — Inference:** Use PBO as a selection-risk report alongside, never instead of,
  chronological tests, point-in-time controls, and realistic costs. The publisher page does not
  support copying an undisclosed formula or threshold into the implementation.
- **Papers-008 — Confirmed:** This source directly determines the discrete ES convention. “Average
  every observation at or beyond VaR” is not equivalent when the quantile has excess probability
  mass ([paper conclusion](https://arxiv.org/pdf/cond-mat/0104295)).
- **Papers-009 — Inference:** Preserve the transparent expected-cost/uncertainty decomposition, then
  validate a richer paper adapter against orders, books, latency, fees, and state transitions.

## Cross-Source Patterns

**Inference.** The sources expose three orthogonal optimism channels: choosing the luckiest strategy,
understating tail loss through an ambiguous discrete convention, and understating liquidation cost
or uncertainty. Market Squawk should persist all three with input/model versions and should never
collapse them into one “risk-adjusted return” score.

## Limitations and Non-Findings

- **Confirmed:** The assigned PBO and Almgren–Chriss pages expose only abstracts; detailed equations,
  simulations, and author-stated full-text limitations were not available from those assigned URLs.
- **Confirmed:** Acerbi–Tasche does not choose horizon, confidence, scenario generation, dependence
  model, liquidity adjustment, or finite-sample confidence intervals.
- **Inference:** No assigned source establishes a universal PBO cutoff, ES confidence, impact
  coefficient, execution horizon, or production strategy.
- **Inference:** Almgren–Chriss alone does not complete the required realistic paper adapter, and no
  source here supports bypassing risk, live-data quality gates, or order-state reconciliation.
- **Inference:** This batch provides no measured Market Squawk performance and no support for
  identity/account rotation, fingerprint spoofing, CAPTCHA bypass, or quota/blocking evasion.

## Source List

1. Bailey, David H.; Borwein, Jonathan M.; López de Prado, Marcos; Zhu, Qiji Jim. “The probability
   of backtest overfitting.” *Journal of Computational Finance* 20(4), 39–69, 2017; first published
   2016-09-19. [Publisher page](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting).
   Accessed 2026-07-15.
2. Acerbi, Carlo; Tasche, Dirk. “On the coherence of Expected Shortfall.” *Journal of Banking &
   Finance* 26, 1487–1503, 2002; arXiv v5 revised 2002-05-02.
   [Record](https://arxiv.org/abs/cond-mat/0104295) ·
   [Full manuscript](https://arxiv.org/pdf/cond-mat/0104295). Accessed 2026-07-15.
3. Almgren, Robert; Chriss, Neil. “Optimal execution of portfolio transactions.” *Journal of Risk*
   3(2), 5–39, 2001. [Publisher DOI](https://doi.org/10.21314/JOR.2001.041).
   Accessed 2026-07-15.
