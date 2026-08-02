# Academic and Research Papers Discovery Report

## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Selection Summary](#selection-summary)
- [Candidate Sources](#candidate-sources)
- [Candidate Source Notes](#candidate-source-notes)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps](#coverage-gaps)
- [Source List](#source-list)

## Research Scope

This discovery report identifies a focused evidence set for the architecture and correctness of
Market Squawk as of **2026-07-15**. The decision context is not “which paper has the best trading
returns,” but “which research results should shape implementable invariants, validation tests, and
explicit limitations” in a zero-mandatory-cost, self-hosted market-data, research, risk, execution,
and valuation platform.

The selected set covers:

1. deterministic low-latency event processing and single-writer ownership;
2. limit-order-book mechanics and online microstructure features;
3. event-driven simulation, execution response, latency, impact, and slippage;
4. look-ahead bias, survivorship bias, point-in-time availability, and backtest selection bias;
5. Value at Risk (VaR), Expected Shortfall (ES/CVaR), and portfolio optimization; and
6. fair-value hierarchy evidence, model risk, and valuation uncertainty.

Primary paper, publisher, university, conference, and recognized engineering-lab pages were read;
search-result snippets were used only for discovery. Every selected source was accessed on
**2026-07-15**. Recommendations and mappings to Market Squawk below are explicitly presented as
inferences, not as claims made by the papers.

## Search Queries Used

The following web queries were used. Exact-title follow-up queries were used to locate primary or
canonical versions after broader discovery.

- `site:arxiv.org limit order book deterministic event driven simulator paper single writer low latency`
- `site:academic.oup.com order flow imbalance limit order book price impact Cont Kukanov Stoikov`
- `site:arxiv.org backtest overfitting look-ahead bias point-in-time financial data paper`
- `site:papers.ssrn.com fair value accounting valuation uncertainty Level 1 Level 2 Level 3 paper`
- `LMAX Disruptor technical paper single writer ring buffer PDF official`
- `Gould Porter Williams McDonald Fenn Howison Limit order books survey Quantitative Finance 2013 PDF`
- `ABIDES Towards High-Fidelity Multi-Agent Market Simulation arXiv authors publication`
- `Probability of Backtest Overfitting Bailey Borwein Lopez de Prado Zhu Journal Computational Finance paper`
- `Acerbi Tasche On the coherence of expected shortfall 2002 journal arxiv`
- `Rockafellar Uryasev Optimization of conditional value-at-risk paper University PDF 2000`
- `Almgren Chriss Optimal execution of portfolio transactions 2001 PDF university`
- `Convergence in Motion Review Fair Value Levels Relevance Accounting in Europe 2021 authors`
- `Brown Goetzmann Ibbotson Ross Survivorship Bias in Performance Studies Review Financial Studies 1992 PDF Yale`
- `event driven backtesting look ahead bias point in time data academic paper finance`
- `look-ahead freedom temporal non-interference backtesting 2607.04958 artifact author`
- `Bridging the Reality Gap in Limit Order Book Simulation Rosenbaum 2026 arxiv`
- `site:risk.net/journal-of-computational-finance "The probability of backtest overfitting"`
- `site:uryasev.ams.stonybrook.edu "Optimization of Conditional Value-at-Risk" PDF`
- `site:tandfonline.com/doi "Convergence in Motion" fair value`
- `"Optimal Execution of Portfolio Transactions" "Almgren" "Chriss" PDF`
- `"Optimization of Conditional Value-at-Risk" "CVaR1_JOR.pdf"`
- `"The Price Impact of Order Book Events" PDF Cont Kukanov Stoikov`

## Selection Summary

The 10 selected sources form a deliberately mixed evidence base:

- **Systems mechanics:** one first-party, benchmarked exchange-engineering paper. Its design ideas
  are directly relevant, but its performance numbers are not portable to Rust or current hardware.
- **Market microstructure:** one focused peer-reviewed empirical paper provides a practical
  top-of-book order-flow-imbalance kernel together with unusually useful validity caveats.
- **Simulation and execution realism:** one peer-reviewed discrete-event simulator paper, one
  current preprint with calibrated market response, and one foundational execution-cost model.
- **Temporal and statistical correctness:** one foundational survivorship-bias paper, one
  peer-reviewed backtest-overfitting method, and one very recent formal look-ahead-freedom paper.
- **Portfolio tail risk:** one mathematical treatment of coherent ES establishes the critical
  discrete-distribution edge cases.
- **Fair value:** one meta-analysis plus practitioner-interview paper focused on the hierarchy and
  valuation-process uncertainty.

The newest sources are useful for current design direction but are not treated as established
consensus. The foundational sources are retained where they define durable correctness concepts.

## Candidate Sources

| ID | Source | URL | Type | Credibility Signal | Freshness Signal | Priority | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P01 | Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads | [LMAX PDF](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | Recognized engineering-lab technical paper | First-party LMAX design paper with disclosed benchmark topology and hardware | May 2011; old benchmark, durable concurrency mechanics | High | Direct evidence for preallocated bounded rings, single-writer ownership, cache layout, sequencing, and latency measurement caveats |
| P02 | The Price Impact of Order Book Events | [arXiv full paper](https://arxiv.org/abs/1011.6402) | Peer-reviewed journal article / open preprint | Published in *Journal of Financial Econometrics*; empirical NYSE TAQ study of 50 stocks | Journal publication 2014; data are April 2010 | High | Gives a simple incremental OFI feature and depth-conditioned price-impact result that maps to online features and slippage controls |
| P03 | ABIDES: Towards High-Fidelity Multi-Agent Market Simulation | [NSF-hosted conference paper](https://par.nsf.gov/servlets/purl/10185795) | Peer-reviewed ACM conference paper | SIGSIM-PADS 2020; university and J.P. Morgan AI Research authors; open implementation lineage | 2020; mature architecture, validation remains illustrative | High | Supports deterministic discrete-event scheduling, protocol-shaped messages, per-agent latency, and counterfactual market response |
| P04 | Bridging the Reality Gap in Limit Order Book Simulation | [arXiv](https://arxiv.org/abs/2603.24137) | arXiv preprint | Authors include recognized market-microstructure researcher Mathieu Rosenbaum; method and case studies are exposed | Submitted 2026-03-25, v1 | High | Current recipe for calibrating timing, imbalance state, impact decay, execution cost, and strategy-sensitive P&L in interactive simulation |
| P05 | Look-Ahead-Freedom as Temporal Non-Interference: A Verifiable Correctness Property for Backtesting and Agentic Trading Pipelines | [arXiv](https://arxiv.org/abs/2607.04958) | arXiv preprint submitted to ACM TOSEM | Formal calculus, soundness argument, artifact claim, and explicit decidability boundary | Submitted 2026-07-06, v1; nine days old at cutoff | High, provisional | Unusually direct formal basis for separating reference/effective time from availability time and testing point-in-time pipeline operators |
| P06 | Survivorship Bias in Performance Studies | [University-hosted paper](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Peer-reviewed journal article | *Review of Financial Studies* 5(4); foundational finance methodology paper | 1992; old but the selection-bias mechanism is durable | Medium | Supports historical constituents, delisted instruments, and explicit universe membership rather than present-day survivor sets |
| P07 | The probability of backtest overfitting | [Publisher page](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Peer-reviewed journal article | *Journal of Computational Finance* 20(4), DOI and publisher abstract; model-free CSCV/PBO framework | First published 2016-09-19; issue dated April 2017 | High | Provides a statistical control for repeated strategy/model selection that point-in-time data alone does not solve |
| P08 | On the coherence of Expected Shortfall | [arXiv / journal version metadata](https://arxiv.org/abs/cond-mat/0104295) | Peer-reviewed journal article / open preprint | *Journal of Banking & Finance* 26 (2002); formal comparison of ES definitions | Revised 2002-05-02; foundational | High | Defines the discrete-distribution edge cases required for correct historical/scenario ES and explains why naïve “tail mean” variants can fail |
| P09 | Optimal execution of portfolio transactions | [Publisher DOI](https://doi.org/10.21314/JOR.2001.041) | Peer-reviewed journal article | *Journal of Risk* 3(2); canonical Almgren–Chriss execution framework | Published 2001-01-01; foundational, simplified dynamics | High | Provides an auditable cost/risk decomposition and baseline impact model for paper execution, slippage, and liquidation scenarios |
| P10 | Convergence in Motion: A Review of Fair Value Levels’ Relevance | [Publisher DOI](https://doi.org/10.1080/17449480.2021.1912370) | Peer-reviewed meta-analysis and interview study | *Accounting in Europe* 18(3); meta-analysis plus practitioner interviews; IASB-linked research context disclosed | Published online 2021-04-27 | High | Direct evidence that hierarchy level, observability, model risk, process complexity, and information usefulness must remain distinct, explained dimensions |

## Candidate Source Notes

### P01 — Disruptor

- **Authors / source / date:** Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, and
  Andrew Stewart; LMAX technical paper, May 2011.
- **Problem studied:** Reducing contention, allocation, cache misses, and queue overhead in
  multi-stage low-latency event processing.
- **Method:** A preallocated bounded ring buffer, sequence barriers, consumer dependency graphs,
  explicit wait strategies, and single-thread write ownership are compared with Java bounded queues
  across several pipeline topologies.
- **Key result:** The authors report materially higher throughput and much lower latency for their
  test configurations, and explain why keeping mutable data under one writer reduces contention.
  The paper discloses its JVM, operating systems, processors, message count, and “best of three”
  measurement method ([LMAX, pp. 1, 5, 9](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Limitations / caveats:** This is a vendor-authored technical paper rather than peer-reviewed
  systems research. The implementation is Java-specific, the benchmark hardware is from 2011, and
  best-of-three throughput is not a warmed p99 latency methodology. Its numerical claims must not be
  reused as Market Squawk performance evidence.
- **Market Squawk relevance (inference):** Strong design support for bounded shard queues,
  preallocation, stable single-writer instrument ownership, explicit overflow policy, cache-aware
  state, and workload/hardware disclosure. It does not justify implementing the Java Disruptor API
  or using unsafe Rust.

### P02 — The Price Impact of Order Book Events

- **Authors / venue / date:** Rama Cont, Arseniy Kukanov, and Sasha Stoikov; *Journal of Financial
  Econometrics* 12(1), 47–88, journal publication 2014; open version dated 2011/2012.
- **Problem studied:** Whether short-horizon price changes are better explained by trades alone or
  by the combined imbalance of market orders, limit orders, and cancellations at the best quotes.
- **Method:** Empirical regressions over one month (April 2010) of consolidated NYSE TAQ trades and
  top-of-book quotes for 50 randomly selected S&P 500 stocks.
- **Key result:** The paper finds a robust linear relation between order-flow imbalance and
  short-interval price changes, with impact slope inversely related to depth; it reports an average
  in-sample explanatory fit near 65% and a 35–60% range after excluding price-changing events in a
  robustness check ([Cont, Kukanov, and Stoikov](https://arxiv.org/abs/1011.6402)).
- **Limitations / caveats:** The data cover one month, 50 U.S. equities, and Level I/top-of-book
  observations. The authors explicitly note potential tautology because some OFI components cause
  the price change being explained; deeper-book events become a noise term. The result is not a
  universal causal slippage law.
- **Market Squawk relevance (inference):** OFI is an attractive pure incremental kernel for online
  features, but it should be versioned, venue-calibrated, and tested out of sample by asset class.

### P03 — ABIDES

- **Authors / venue / date:** David Byrd, Maria Hybinette, and Tucker Hybinette Balch; ACM
  SIGSIM-PADS 2020, DOI 10.1145/3384441.3395986.
- **Problem studied:** Providing a configurable, high-fidelity, agent-based discrete-event market
  simulation environment for AI and market experiments.
- **Method:** A deterministic discrete-event kernel, exchange and trader agents, message protocols
  modeled after NASDAQ ITCH/OUCH, and configurable pairwise network latency.
- **Key result:** ABIDES demonstrates tens of thousands of interacting agents and shows example
  configurations and a preliminary market-impact experiment ([ACM paper via NSF](https://par.nsf.gov/servlets/purl/10185795)).
- **Limitations / caveats:** The paper's market validation is illustrative and the impact case study
  is described as preliminary. Example background agents are simplified, and each trades one symbol
  on one exchange. High-fidelity architecture does not imply calibrated fidelity for every venue or
  asset.
- **Market Squawk relevance (inference):** Strong evidence for timestamp-ordered event simulation,
  explicit latency, typed exchange messages, seedable determinism, and counterfactual reaction. It
  also reinforces the specification's decision not to confuse historical replay with realistic
  endogenous market response.

### P04 — Bridging the Reality Gap in Limit Order Book Simulation

- **Authors / source / date:** Patrick Noble, Mathieu Rosenbaum, and Saad Souilmi; arXiv q-fin.TR,
  submitted 2026-03-25, v1.
- **Problem studied:** Producing interactive LOB simulations whose execution, costs, timing, and P&L
  respond realistically to an experimental strategy.
- **Method:** Project book state onto spread and volume imbalance, estimate transitions from data,
  calibrate fine-scale event timing, and add a power-law-decay feedback term for signed trade flow.
- **Key result:** Across several stocks and strategy case studies, the authors report concave impact,
  partial post-trade reversion, latency-scale timing structure, and strong sensitivity of simulated
  profitability to execution parameters ([Noble, Rosenbaum, and Souilmi](https://arxiv.org/abs/2603.24137)).
- **Limitations / caveats:** This is a recent, non-peer-reviewed v1 preprint scoped to large-tick
  assets and several stocks. The projected state intentionally discards full-book detail. External
  replication and generalization to crypto, options, futures, and order-level books remain open.
- **Market Squawk relevance (inference):** A useful current validation checklist for paper execution:
  latency distribution, queue/imbalance state, impact during execution, reversion, costs, and
  parameter sensitivity—not just price-path resemblance.

### P05 — Look-Ahead-Freedom as Temporal Non-Interference

- **Author / source / date:** Xavier Fonseca; arXiv cs.CR/cs.LO/cs.PL/cs.SE/q-fin.PM, submitted
  2026-07-06, v1; listed as submitted to *ACM Transactions on Software Engineering and Methodology*.
- **Problem studied:** Giving look-ahead freedom a verifiable semantics rather than relying only on
  construct-specific leakage checks.
- **Method:** A time-indexed pipeline calculus separates a datum's reference time from availability
  time, then applies a type-and-effect discipline and two-run non-interference property.
- **Key result:** The author claims a sound, linear-time check for a value-independent fragment that
  includes windows, resampling, joins, point-in-time and vintage reads; the full problem becomes
  undecidable when availability can depend on data values ([Fonseca, 2026](https://arxiv.org/abs/2607.04958)).
- **Limitations / caveats:** This source was only nine days old at the research cutoff, has one
  author, is not yet peer reviewed, and its artifact results were not independently reproduced here.
  The decidable fragment excludes data-dependent availability behavior.
- **Market Squawk relevance (inference):** Provisional but exceptionally direct support for storing
  `effective_at`, `published_at`, `available_at`, revisions, and supersession separately; for making
  dataset operators availability-monotone; and for property-testing point-in-time joins.

### P06 — Survivorship Bias in Performance Studies

- **Authors / venue / date:** Stephen J. Brown, William Goetzmann, Roger G. Ibbotson, and Stephen A.
  Ross; *The Review of Financial Studies* 5(4), 553–580, 1992.
- **Problem studied:** How selecting only surviving funds changes observed volatility/return
  relationships and can create apparent performance predictability.
- **Method:** Analytical and numerical examples for samples truncated by survival.
- **Key result:** The authors show that survivorship truncation can be strong enough to account for
  apparent predictability in the studied setting ([Brown et al., 1992](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).
- **Limitations / caveats:** The empirical framing is mutual-fund performance in an older market
  period. It does not specify instrument-master schemas or corporate-action treatment.
- **Market Squawk relevance (inference):** Durable justification for historical constituents,
  symbol history, delistings, mergers, contract rolls, and explicit universe-as-of selection.

### P07 — The probability of backtest overfitting

- **Authors / venue / date:** David H. Bailey, Jonathan M. Borwein, Marcos López de Prado, and Qiji
  Jim Zhu; *Journal of Computational Finance* 20(4), 39–69; first published 2016-09-19, issue 2017.
- **Problem studied:** Measuring the probability that the best in-sample strategy underperforms out
  of sample after repeated strategy selection.
- **Method:** A model-free, nonparametric framework with combinatorially symmetric cross-validation
  (CSCV) to estimate probability of backtest overfitting (PBO).
- **Key result:** The authors report reasonable PBO estimates with relatively small error in their
  examples ([publisher abstract](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting)).
- **Limitations / caveats:** PBO measures selection overfitting; it does not repair look-ahead data,
  survivor-only universes, unrealistic fills, omitted fees, or regime mismatch. Its usefulness also
  depends on preserving the set of tried strategies rather than reporting only the winner.
- **Market Squawk relevance (inference):** Record model/strategy trials and dataset versions, and
  make selection-risk metrics a complement to chronological test splits and point-in-time joins.

### P08 — On the coherence of Expected Shortfall

- **Authors / venue / date:** Carlo Acerbi and Dirk Tasche; *Journal of Banking & Finance* 26,
  1487–1503, 2002; arXiv v5 revised 2002-05-02.
- **Problem studied:** Resolving competing definitions of Expected Shortfall, especially when loss
  distributions contain discontinuities.
- **Method:** Mathematical comparison of ES, tail conditional expectation, worst conditional
  expectation, CVaR, and related definitions against coherence properties.
- **Key result:** The authors identify a definition that remains coherent for general distributions
  and can be estimated where usual VaR estimators can fail ([Acerbi and Tasche](https://arxiv.org/abs/cond-mat/0104295)).
- **Limitations / caveats:** This foundational theory does not choose a return horizon, scenario
  model, dependence model, confidence level, or finite-sample uncertainty method.
- **Market Squawk relevance (inference):** Implement ES with explicit quantile conventions and tests
  for discrete empirical scenarios, ties, atoms, monotonicity, and subadditivity; do not implement it
  as an ambiguous average of observations “beyond VaR.”

### P09 — Optimal execution of portfolio transactions

- **Authors / venue / date:** Robert Almgren and Neil Chriss; *The Journal of Risk* 3(2), 5–39,
  published 2001-01-01.
- **Problem studied:** Trading off volatility risk against transaction costs during time-sliced
  liquidation.
- **Method:** A mean-variance framework with permanent and temporary market impact; under a linear
  cost model the authors construct an efficient frontier of execution trajectories.
- **Key result:** The model makes execution cost, uncertainty, schedule, and liquidity-adjusted VaR
  explicit in a tractable baseline ([publisher page](https://doi.org/10.21314/JOR.2001.041)).
- **Limitations / caveats:** Linear impact, exogenous price dynamics, and stylized trading do not
  represent queue priority, venue fragmentation, partial fills, rejects, cancels, fees, latency, or
  state-dependent liquidity. It is a baseline, not a realistic paper broker by itself.
- **Market Squawk relevance (inference):** Use the decomposition and invariants to test slippage and
  liquidation scenarios, then extend paper execution with book state, latency, partial-fill, fee,
  rejection, and order-state models.

### P10 — Convergence in Motion

- **Authors / venue / date:** Andrei Filip, Ahmad Hammami, Zhongwei Huang, Anne Jeny, Michel Magnan,
  and Rucsandra Moldovan; *Accounting in Europe* 18(3), 275–294, published online 2021-04-27.
- **Problem studied:** Synthesizing the value relevance of fair-value hierarchy levels and comparing
  academic evidence with valuation practice after IFRS 13 implementation.
- **Method:** Meta-analysis of comparable empirical studies plus interviews with practitioners at
  financial institutions.
- **Key result:** The authors find lower overall value relevance for Level 3 than Levels 1 and 2,
  with improvement over time, and identify asset fundamentals, model risk, and measurement-process
  complexity as possible contributors to gaps ([Filip et al., 2021](https://doi.org/10.1080/17449480.2021.1912370)).
- **Limitations / caveats:** Market value relevance is not the same as measurement correctness or
  standards compliance. The interview component is small and Canadian; the paper reports IASB
  funding and notes that its views are not IASB policy. It is not a classification rules engine.
- **Market Squawk relevance (inference):** Preserve input observability, model/process uncertainty,
  method, evidence, overrides, approval, and ruleset version separately. Do not reinterpret hierarchy
  levels as a generic data-quality score or execution-eligibility class.

## Excluded Sources

| Source | URL | Reason Excluded |
| --- | --- | --- |
| Limit order books | [Oxford-hosted paper](https://people.maths.ox.ac.uk/~porterm/papers/gould-qf-final.pdf) | High-quality 2013 survey, but removed to keep discovery to 10 focused sources; P02 provides a more directly implementable feature and explicit empirical validity caveats |
| Optimization of Conditional Value-at-Risk | [Publisher DOI](https://doi.org/10.21314/JOR.2000.038) | Foundational and credible, but removed to keep discovery focused; P08 establishes ES correctness, while broader portfolio-optimization depth can be recovered in a later risk-specific pass |
| JAX-LOB: A GPU-Accelerated Limit Order Book Simulator to Unlock Large Scale Reinforcement Learning for Trading | [arXiv](https://arxiv.org/abs/2308.13289) | Credible and useful for batched RL experiments, but GPU throughput and RL environment design are less central than deterministic CPU hot-path ownership and calibrated execution realism for this release |
| DeepLOB: Deep Convolutional Neural Networks for Limit Order Books | [arXiv](https://arxiv.org/abs/1808.03668) | Influential forecasting paper, but model accuracy on a benchmark dataset is less useful to the requested architecture/correctness evidence than OFI, bias controls, and simulator response |
| Uncertainty-Aware Lookahead Factor Models for Quantitative Investing | [ICML / PMLR](https://proceedings.mlr.press/v119/chauhan20a.html) | “Lookahead” here means forecasting future fundamentals, not proving point-in-time freedom; selected sources cover leakage and temporal availability more directly |
| Look-Ahead-Bench: a Standardized Benchmark of Look-ahead Bias in Point-in-Time LLMs for Finance | [arXiv](https://arxiv.org/abs/2601.13770) | Current but LLM-specific; Market Squawk explicitly excludes LLM calls from the live path, and the formal P05 paper has broader data-pipeline applicability |
| Summoning the Oracle to Slay It: Mitigating Look-Ahead Bias in Financial Backtesting with Large Language Models | [arXiv](https://arxiv.org/abs/2605.24564) | Addresses parametric memory in pretrained LLMs, not the core point-in-time joins, source revisions, and event-driven execution pipeline |
| EvoMarket: A High-Fidelity and Scalable Financial Market Simulator | [arXiv](https://arxiv.org/abs/2604.18046) | Very recent and relevant but overlaps ABIDES and P04; evidence is focused on China A-share data and had less time for independent validation at the cutoff |
| Auditors' Role in Level 2 versus Level 3 Fair-Value Classification Judgments | [SSRN](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2119720) | Useful behavioral evidence, but narrower and older than the selected hierarchy meta-analysis; authoritative ASC 820/IFRS 13 rules belong in official-documentation research |
| Information Risk and Fair Values: An Examination of Equity Betas | [SSRN / journal metadata](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1439851) | Strong empirical evidence for information risk, but the selected meta-analysis synthesizes multiple hierarchy studies and better fits a limited 10-source set |
| Generic blogs, SEO explainers, Wikipedia pages, Reddit posts, and documentation mirrors | N/A | Excluded under the source-quality rubric because primary papers, publisher pages, and university copies were available |

## Coverage Gaps

1. **No paper validates Market Squawk's performance target.** The selected systems paper supplies
   design hypotheses, not evidence for 100,000 events/s or sub-millisecond warmed p99 on the target
   Rust implementation. Local Criterion and end-to-end benchmarks with documented hardware remain
   mandatory.
2. **Rust-specific concurrency evidence is incomplete.** P01 is Java-oriented and predates modern
   Rust async/runtime practice. Rust memory-model, Tokio bounded-channel, cache-layout, and allocator
   decisions should be sourced from official documentation and verified locally.
3. **Venue protocol correctness is outside these papers.** Coinbase sequencing, Kraken checksum,
   reconnect, snapshot/delta, and rate-limit behavior require current official venue documentation
   and recorded fixtures, not academic inference.
4. **Cross-asset simulator calibration remains weak.** P02 is U.S. equities, and P04 is large-tick
   stocks. Evidence is still needed for crypto, options, futures, FX, auctions, halts, on-chain data,
   and order-level books.
5. **Point-in-time formalism is very new.** P05 maps directly to the desired schema but is a
   nine-day-old preprint. It should inspire properties and tests, not become an unreviewed dependency
   or be represented as settled literature.
6. **Risk estimation uncertainty is only partially covered.** P08 defines coherent tail risk but
   does not resolve horizon selection, serial dependence, stress-window design, EVT, scenario
   generation, backtesting, or finite-sample uncertainty.
7. **Fair-value compliance requires standards.** P10 supports keeping observability and uncertainty
   explicit, but ASC 820 and IFRS 13 classification, disclosure, active-market, accessibility, and
   measurement-date rules must come from authoritative standards and regulatory guidance.
8. **Paper execution needs empirical calibration.** P04 and P09 provide complementary models, yet
   fees, queue position, adverse selection, cancellation, rejects, and partial-fill rules must be
   calibrated per venue and clearly labeled when estimated.
9. **Source licensing and data rights are not answered by papers.** Adapter-specific official terms,
   public-data licenses, retention rules, and coverage metadata need a separate documentation/legal
   evidence pass.

## Source List

All sources below were accessed on **2026-07-15**.

1. Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, and Andrew Stewart. “Disruptor: High
   performance alternative to bounded queues for exchanging data between concurrent threads.” LMAX,
   May 2011. [Primary PDF](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf).
2. Rama Cont, Arseniy Kukanov, and Sasha Stoikov. “The Price Impact of Order Book Events.”
   *Journal of Financial Econometrics* 12(1), 47–88, 2014. [Open full paper and journal
   metadata](https://arxiv.org/abs/1011.6402).
3. David Byrd, Maria Hybinette, and Tucker Hybinette Balch. “ABIDES: Towards High-Fidelity
   Multi-Agent Market Simulation.” *Proceedings of ACM SIGSIM-PADS '20*, 2020, DOI
   10.1145/3384441.3395986. [NSF-hosted paper](https://par.nsf.gov/servlets/purl/10185795).
4. Patrick Noble, Mathieu Rosenbaum, and Saad Souilmi. “Bridging the Reality Gap in Limit Order Book
   Simulation.” arXiv:2603.24137, submitted 2026-03-25.
   [arXiv](https://arxiv.org/abs/2603.24137).
5. Xavier Fonseca. “Look-Ahead-Freedom as Temporal Non-Interference: A Verifiable Correctness
   Property for Backtesting and Agentic Trading Pipelines.” arXiv:2607.04958, submitted 2026-07-06.
   [arXiv](https://arxiv.org/abs/2607.04958).
6. Stephen J. Brown, William Goetzmann, Roger G. Ibbotson, and Stephen A. Ross. “Survivorship Bias
   in Performance Studies.” *The Review of Financial Studies* 5(4), 553–580, 1992.
   [University-hosted paper](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf).
7. David H. Bailey, Jonathan M. Borwein, Marcos López de Prado, and Qiji Jim Zhu. “The probability
   of backtest overfitting.” *Journal of Computational Finance* 20(4), 39–69, 2017, DOI
   10.21314/JCF.2016.322. [Publisher page](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting).
8. Carlo Acerbi and Dirk Tasche. “On the coherence of Expected Shortfall.” *Journal of Banking &
   Finance* 26, 1487–1503, 2002. [Open paper and journal
   metadata](https://arxiv.org/abs/cond-mat/0104295).
9. Robert Almgren and Neil Chriss. “Optimal execution of portfolio transactions.” *The Journal of
    Risk* 3(2), 5–39, 2001, DOI 10.21314/JOR.2001.041.
    [Publisher page](https://doi.org/10.21314/JOR.2001.041).
10. Andrei Filip, Ahmad Hammami, Zhongwei Huang, Anne Jeny, Michel Magnan, and Rucsandra Moldovan.
    “Convergence in Motion: A Review of Fair Value Levels' Relevance.” *Accounting in Europe* 18(3),
    275–294, 2021, DOI 10.1080/17449480.2021.1912370.
    [Publisher DOI](https://doi.org/10.1080/17449480.2021.1912370).
