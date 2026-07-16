# Papers Synthesis

## Table of Contents

- [Category Scope](#category-scope)
- [Sources Covered](#sources-covered)
- [High-Confidence Findings](#high-confidence-findings)
- [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
- [Conflicts and Disagreements](#conflicts-and-disagreements)
- [Trends and Patterns](#trends-and-patterns)
- [Implications for the Research Topic](#implications-for-the-research-topic)
- [Gaps](#gaps)
- [Source Matrix](#source-matrix)

## Category Scope

This synthesis consolidates four paper batches covering ten sources as of **2026-07-15**. It
examines evidence relevant to Market Squawk's live-processing architecture, online market
features, research simulation, point-in-time datasets, survivorship, backtest-selection risk,
Expected Shortfall (ES), execution modeling, and fair-value domain boundaries. It does not add
sources beyond the batch reports.

“Confirmed” means the cited source directly supports the statement, with confidence adjusted for
source maturity and access. “Inference” means a proposed Market Squawk design or test derived from
that evidence. Numerical paper results remain results for the disclosed samples, not Market Squawk
performance claims or production defaults.

## Sources Covered

The evidence set includes:

- one first-party exchange-engineering paper on bounded rings and sequencing
  ([Thompson et al.](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf));
- three market-microstructure/simulation papers on order-flow imbalance (OFI), agent-based
  simulation, and calibrated queue-reactive simulation
  ([Cont et al.](https://arxiv.org/pdf/1011.6402),
  [Byrd et al.](https://par.nsf.gov/servlets/purl/10185795),
  [Noble et al.](https://arxiv.org/pdf/2603.24137));
- two temporal/sample-selection papers on look-ahead freedom and survivorship
  ([Fonseca](https://arxiv.org/html/2607.04958),
  [Brown et al.](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf));
- three research/risk/execution papers on backtest overfitting, coherent ES, and optimal execution
  ([Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting),
  [Acerbi and Tasche](https://arxiv.org/pdf/cond-mat/0104295),
  [Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041)); and
- one peer-reviewed synthesis of fair-value-level value relevance
  ([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

Evidence quality is uneven. Brown et al., Cont et al., Acerbi–Tasche, ABIDES, Bailey et al.,
Almgren–Chriss, and Filip et al. are peer-reviewed or have peer-reviewed publication lineage.
However, only publisher abstracts were available for Bailey et al. and Almgren–Chriss. Disruptor is
detailed primary engineering evidence but vendor-authored and dated. Noble et al. and Fonseca are
2026 arXiv v1 preprints; Fonseca was submitted nine days before the cutoff and its reported
validation includes unavailable proprietary data.

## High-Confidence Findings

### Ownership and ordering are system invariants

**Confirmed.** Disruptor separates preallocated bounded storage, producer coordination, and
consumer notification; producer and consumer sequences prevent overwrite of unconsumed entries.
Its single-writer case avoids a class of write contention, while wait strategies explicitly trade
CPU consumption for notification latency
([Thompson et al., §§4–4.4](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
ABIDES independently shows the research-plane analogue: a chronological event queue,
non-decreasing virtual time, deterministic same-time ordering, per-agent random streams, and
explicit computation/network delays preserve causal and reproducible simulation
([Byrd et al., §4](https://par.nsf.gov/servlets/purl/10185795)).

**Inference.** Market Squawk should give each live instrument state one authoritative shard/writer,
bound every live queue, make publication and overflow transitions explicit, and benchmark safe Rust
implementations locally. Separately, research simulation should serialize event ordering, clocks,
latency, seeds, and versions so repeated runs reproduce orders, fills, and metrics. These are shared
invariants, not a reason to merge live and research pipelines.

### Market features and simulators are coverage-bound models

**Confirmed.** Cont et al. define OFI as signed best-bid/best-ask price and quantity changes. In one
month of filtered Level I U.S. equity data, their contemporaneous linear model averaged about 65%
fit versus about 32% for trade imbalance, with impact approximately inverse to depth. They also
identify hidden/deeper liquidity, timestamp aggregation, spread filtering, and possible tautology
from price-changing quote events
([Cont et al., §§2–4 and Appendix A](https://arxiv.org/pdf/1011.6402)).

**Confirmed.** ABIDES models price-time priority, partial fills, residual orders, cancellation, and
latency, but calls its impact evidence preliminary
([Byrd et al., §§4–7](https://par.nsf.gov/servlets/purl/10185795)). Noble et al. further show that a
simulator can require empirical event timing, race proxies, signed-flow feedback, per-metric
validation, and explicit impact calibration; their four-stock simulation still understates some
return tails
([Noble et al., §§2–5](https://arxiv.org/pdf/2603.24137)).

**Inference.** OFI should be a versioned, quality-gated feature with explicit venue, depth, units,
window, reset, and calibration semantics—not an execution-quality classifier. Simulator bundles
should bind source hashes, coverage, tick/lot semantics, timing, fill, fee and impact assumptions,
seeds, code revision, calibration intervals, held-out intervals, and validation metrics. Simulated
prices and fills remain `Modeled`, never `DirectVerified`.

### Point-in-time correctness needs both availability and historical population controls

**Confirmed.** Brown et al. demonstrate that conditioning a historical sample on survival can
manufacture apparent performance persistence even when generated returns are serially
uncorrelated. Removing an entity only after it disappears does not remove bias if inclusion first
required survival through the evaluation window
([Brown et al., §§1–3](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).

**Confirmed, provisional.** Fonseca distinguishes the time a value describes from the time it
becomes knowable and formalizes look-ahead freedom as temporal non-interference: changing future-
available data must not change an earlier decision. The proposed rules propagate the latest input
availability through causal transforms and point-in-time reads
([Fonseca, §§3–9](https://arxiv.org/html/2607.04958)).

**Inference.** Records should preserve `effective_at`, `published_at`, `available_at`, `ingested_at`,
revision, and `superseded_at`; strict decisions should use only versions available by the decision
time. Historical universe membership, identifiers, exits, terminal proceeds, delistings, mergers,
and coverage changes should be append-only and time-varying. Tests should perturb future revisions
and later exits and prove earlier universes, features, predictions, and orders are unchanged.

### Risk measures require unambiguous mathematical contracts

**Confirmed.** Acerbi and Tasche prove that ES definitions can diverge at discontinuities. Coherent
ES averages exactly the worst tail probability mass, including only the fraction of a quantile atom
needed to complete that mass. This ES is monotone, subadditive, positively homogeneous, and
translation invariant
([Acerbi and Tasche, §§2–5](https://arxiv.org/pdf/cond-mat/0104295)).

**Inference.** Market Squawk should use a loss-positive ES API that records confidence and tail
probability, weights a boundary atom fractionally, validates weights and domains, and property-tests
the four coherence properties. A naive mean of all observations at or beyond VaR must fail a tied-
boundary fixture.

### Accounting hierarchy, data quality, and execution eligibility are non-substitutable

**Confirmed.** Filip et al. report that Level 3 measurements are less value relevant overall than
Levels 1 and 2, with improvement over time, while identifying asset fundamentals, model risk, and
process complexity as possible contributors. The paper studies association with market pricing; it
does not establish classification correctness, measurement accuracy, or standards compliance and
is not IASB policy
([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

**Inference.** `FairValueHierarchy`, input observability, method uncertainty, `DataQuality`,
`MarketDepth`, and execution eligibility need separate types and evidence. A Level 2/3 modeled value
cannot become Level 1 evidence or execution-quality data. A Level 1 classification also cannot
grant automated-action eligibility without independent feed-integrity, freshness, coverage, and
risk checks.

## Medium- and Low-Confidence Findings

- **Medium-high, provisional:** Fonseca's value-independent checker is reported sound and linear in
  pipeline size under bounded availability-expression complexity. The claim is from an extremely
  recent single-author v1 preprint, trusts input stamps and faithful pipeline representation, reports
  conservative false positives, and lacks redistributable proprietary validation data
  ([Fonseca, §§4–9](https://arxiv.org/html/2607.04958)). Use its semantics and leak fixtures now;
  do not claim formal certification or require the checker for release.
- **Medium-high for reported results, low for transferability:** Noble et al.'s conditional event,
  timing, race, and flow-impact mechanisms improve several disclosed fidelity dimensions, but the
  sample contains four large-tick U.S. stocks from one paid vendor and some impact parameters are
  illustrative. It provides a validation blueprint, not portable venue/asset constants
  ([Noble et al.](https://arxiv.org/pdf/2603.24137)).
- **High for the abstract-level claim, medium for implementation detail:** Bailey et al. propose
  model-free, nonparametric combinatorially symmetric cross-validation for probability of backtest
  overfitting (PBO). The accessible publisher page does not expose full method text or a universal
  threshold
  ([Bailey et al.](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting)).
- **High for the abstract-level model, medium for a concrete implementation:** Almgren–Chriss trades
  expected permanent/temporary impact costs against volatility uncertainty and constructs a linear-
  cost efficient frontier. Abstract-only access supports a transparent schedule/cost baseline, not
  a detailed fill implementation
  ([Almgren and Chriss](https://doi.org/10.21314/JOR.2001.041)).
- **Low as current performance evidence:** Disruptor's reported latency and throughput are 2011
  vendor results from Java 1.6-era systems, with execution variability and no CPU binding in the
  latency run. They cannot substantiate Rust 1.97 or Market Squawk acceptance targets
  ([Thompson et al., §§5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Medium for explanation, high for association:** Filip et al.'s small Canadian interview sample
  offers contextual explanations for differences in value relevance, not causal proof or broadly
  representative rules
  ([Filip et al.](https://doi.org/10.1080/17449480.2021.1912370)).

## Conflicts and Disagreements

The reviewed sources contain no direct empirical contradiction after scope is respected. They do
expose important tensions that the architecture must not erase:

1. **Live concurrency versus simulated time.** Disruptor favors bounded cross-thread publication;
   ABIDES uses a single global discrete-event order. These are not competing universal designs.
   The first addresses live stage exchange; the second addresses causal research simulation.
2. **Strong OFI fit versus predictive/causal restraint.** Cont et al.'s sample shows strong
   contemporaneous fit, but the authors' tautology test and omitted liquidity dimensions reject the
   stronger interpretation that OFI alone predicts returns or authorizes trades.
3. **Endogenous book slippage versus calibration gaps.** ABIDES argues that changing book state can
   generate slippage endogenously. Noble et al. show that timing races, persistent flow, impact, and
   tail behavior still require assumptions and calibration. Dynamic state is necessary evidence of
   causality, not sufficient evidence of empirical realism.
4. **Backtest diagnostics versus data validity.** PBO addresses strategy-selection overfitting;
   temporal non-interference and survivorship address different biases. A favorable PBO estimate
   cannot repair future data, survivor-only universes, implausible fills, or missing costs.
5. **Execution optimization versus execution simulation.** Almgren–Chriss supplies an expected-
   cost/uncertainty schedule. ABIDES and Noble et al. address stateful fills and market response.
   An optimal schedule is neither an exchange emulator nor authorization to submit an order.
6. **Fair-value relevance versus classification.** Filip et al.'s hierarchy-level associations do
   not determine the correct hierarchy for an individual measurement, and neither hierarchy nor
   value relevance determines live-data quality.

The ES paper identifies a true definition-level disagreement in common usage: tail conditional
expectation variants can disagree at a discrete quantile and can lose coherence. Market Squawk
should resolve it explicitly through the exact-tail-mass convention rather than treating the names
ES, TCE, and CVaR as automatically interchangeable
([Acerbi and Tasche](https://arxiv.org/pdf/cond-mat/0104295)).

## Trends and Patterns

Across otherwise different domains, five patterns recur:

1. **Make ordering and time explicit.** Live sequences, virtual event time, observation
   availability, universe membership, and valuation measurement dates are distinct semantics.
2. **Version the complete evidence chain.** Source data, feature definitions, candidate trials,
   calibration, models, scenarios, code, seeds, rulesets, and overrides must be reproducible rather
   than mutable “latest” state.
3. **Bind claims to coverage.** An OFI coefficient, fill rule, impact curve, PBO diagnostic, or
   valuation association is valid only for its method and sample; unsupported transfer needs new
   validation.
4. **Use multi-axis validation.** Mean return, average volatility, one fit statistic, or one risk
   score can conceal tail, timing, fill, selection, aggregation, or classification failures.
5. **Keep decision dimensions separate.** Empirical explanatory power, simulated realism,
   point-in-time admissibility, statistical risk, accounting hierarchy, data quality, and order
   eligibility answer different questions. Model output remains subordinate to explicit risk and
   source-quality controls.

## Implications for the Research Topic

The following are consolidated **Inferences**, not claims that the papers implement Market Squawk:

- **Live plane:** use stable instrument-owned shards, safe single-writer state, bounded queues,
  explicit sequence publication, and fail-closed overflow/quarantine. Test wrap safety, partial
  publication, stable sharding, source discontinuities, bounded memory, and latency under nominal,
  burst, and saturated loads.
- **Feature plane:** implement OFI and similar kernels as pure, incremental, versioned functions
  over validated state. Gaps, crossed books, connection-generation changes, or stale snapshots must
  invalidate affected windows. Never use historical paper coefficients as production defaults.
- **Research data plane:** preserve availability, revision, supersession, historical universe,
  identifier, and exit facts. Point-in-time joins inherit the latest availability of influencing
  inputs. Add planted-leak, future-perturbation, restatement, delisting, and centered-window tests.
- **Model and strategy registry:** persist every trial, not only the selected winner, with dataset,
  feature, parameter, split, seed, cost/fill assumption, and metric versions. Apply PBO-like
  selection diagnostics only after temporal, universe, and execution-validity gates pass.
- **Backtest and paper execution:** separate deterministic event scheduling from empirical fill
  calibration and from execution-cost optimization. Model latency, price-time priority, partial
  fills, residuals, cancellation, fees, rejects, balances, and position reconciliation. Bind every
  bundle to supported coverage and label outputs `Modeled`.
- **Risk:** implement exact weighted discrete ES with explicit sign/tail conventions and coherence
  properties. Treat ES, selection risk, model uncertainty, and execution cost as independent
  controls; no aggregate score may bypass pre-trade risk.
- **Valuation:** store versioned evidence and explanations for hierarchy classification while
  keeping input observability, method/model uncertainty, quality, depth, and execution eligibility
  distinct. Use authoritative ASC 820/IFRS 13 sources—not Filip et al.—for the actual decision tree.

Together these imply an evidence lineage of source record → normalized point-in-time state →
versioned feature/model/scenario → bounded decision → risk verdict → simulated or authorized
execution record. Each transition needs typed inputs, coverage metadata, a deterministic audit
record, and a testable failure policy.

## Gaps

- No paper validates Market Squawk, Rust 1.97, Tokio, safe-Rust memory ordering, current target
  hardware, 100,000 events/s, sub-millisecond warmed p99, or bounded sustained-burst memory.
- No source specifies Coinbase/Kraken sequence or checksum rules, snapshot synchronization,
  reconnect generations, authorized delivery, trading status, or `DirectVerified` qualification.
- The set does not establish Arrow/Parquet/DataFusion/SQLite schemas or performance, SEC and macro
  adapters, portfolio import/reconciliation, model-bundle formats, ONNX inference, MCP schemas and
  limits, credential handling, source licensing, vulnerability controls, or release audits.
- No universal OFI coefficient, PBO cutoff, ES confidence, scenario-generation method, impact
  coefficient, execution horizon, fill parameter, or trading strategy follows from these papers.
- ABIDES and Noble et al. do not fully validate queue position, adverse selection, fees, cancels
  under race, rejects, halts, cross-venue routing, all asset classes, or broker state transitions.
- Fonseca's formal checker remains provisional and cannot certify arbitrary Rust, SQL, Python, MCP,
  or model-weight behavior; correct availability stamps remain an upstream trust assumption.
- Brown et al. provide no universal survivorship correction; Bailey et al. do not show that PBO
  cures other forms of research bias; Acerbi–Tasche do not select a horizon or generate scenarios.
- Filip et al. is not ASC 820, IFRS 13, or authoritative interpretive guidance and cannot establish
  current classification, disclosure, override, approval, or measurement rules.
- No source supports identity/account rotation, fingerprint or TLS spoofing, CAPTCHA bypass, proxy
  rotation, distributed quota evasion, or any other access-control circumvention.

## Source Matrix

| ID | Source | Type / maturity | Strongest supported contribution | Confidence | Principal caveat |
| --- | --- | --- | --- | --- | --- |
| papers-001 | [Thompson et al., *Disruptor*](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | First-party engineering paper, 2011 | Bounded preallocation, explicit sequences, wrap protection, single-writer and wait-strategy tradeoffs | High for design; low for current performance | Vendor-authored Java 1.6 benchmarks on dated hardware |
| papers-002 | [Cont et al., *The Price Impact of Order Book Events*](https://arxiv.org/pdf/1011.6402) | Peer-reviewed empirical paper, 2014 | Top-of-book OFI definition and sample-specific contemporaneous price/depth relationship | High for sample | One month of filtered Level I U.S. equity data; possible tautology; not causal prediction |
| papers-003 | [Byrd et al., *ABIDES*](https://par.nsf.gov/servlets/purl/10185795) | Peer-reviewed conference paper, 2020 | Causal discrete-event ordering, per-agent PRNGs/latency, price-time book and partial fills | High for described architecture | Preliminary impact validation; single-threaded Python and narrow examples |
| papers-004 | [Noble et al., *Bridging the Reality Gap*](https://arxiv.org/pdf/2603.24137) | arXiv v1 preprint, 2026 | Multi-metric calibration pattern for event timing, race, flow persistence, and impact | Medium-high, provisional | Four large-tick stocks, one paid vendor, illustrative parameters and weak tails |
| papers-005 | [Fonseca, *Look-Ahead-Freedom as Temporal Non-Interference*](https://arxiv.org/html/2607.04958) | Very recent single-author arXiv v1, 2026 | Availability/reference-time distinction and two-run non-interference specification | Medium-high, provisional | Not peer reviewed; trusts stamps/representation; proprietary validation unavailable |
| papers-006 | [Brown et al., *Survivorship Bias in Performance Studies*](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf) | Peer-reviewed journal paper, 1992 | Mechanism and simulation showing survival selection can manufacture persistence | High | Manager-performance setting; no universal correction |
| papers-007 | [Bailey et al., *The Probability of Backtest Overfitting*](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Peer-reviewed journal paper, 2016/2017; abstract access | PBO framing and model-free nonparametric CSCV | High for abstract claim; medium for implementation | Full method unavailable at assigned URL; no universal cutoff |
| papers-008 | [Acerbi and Tasche, *On the Coherence of Expected Shortfall*](https://arxiv.org/pdf/cond-mat/0104295) | Full peer-reviewed-lineage manuscript, 2002 | Coherent ES for discontinuous/weighted losses with exact quantile-atom mass | High | Does not choose confidence, horizon, scenarios, or finite-sample uncertainty |
| papers-009 | [Almgren and Chriss, *Optimal Execution of Portfolio Transactions*](https://doi.org/10.21314/JOR.2001.041) | Peer-reviewed journal paper, 2001; abstract access | Expected impact-cost/volatility-uncertainty tradeoff and linear efficient frontier | High for abstract claim; medium for implementation | Stylized schedule model, not a realistic fill engine; full text unavailable |
| papers-010 | [Filip et al., *Convergence in Motion*](https://doi.org/10.1080/17449480.2021.1912370) | Peer-reviewed review/meta-analysis, 2021 | Fair-value-level value-relevance evidence and multidimensional explanatory context | High for reported association; medium for explanations | Not an accounting standard, classification test, causal proof, or execution evidence |
