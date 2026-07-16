# Papers Batch 001 Deep Dive

## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
  - [Low-Latency Ownership and Sequencing](#low-latency-ownership-and-sequencing)
  - [Order-Flow Imbalance as an Online Feature](#order-flow-imbalance-as-an-online-feature)
  - [Deterministic Event-Driven Market Simulation](#deterministic-event-driven-market-simulation)
  - [Implementation and Test Implications](#implementation-and-test-implications)
- [Evidence Table](#evidence-table)
- [Source-Specific Notes](#source-specific-notes)
  - [Papers-001: Disruptor](#papers-001-disruptor)
  - [Papers-002: The Price Impact of Order Book Events](#papers-002-the-price-impact-of-order-book-events)
  - [Papers-003: ABIDES](#papers-003-abides)
- [Cross-Source Patterns](#cross-source-patterns)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This deep dive reviews only the three papers assigned in `papers-batch-001`, as of
**2026-07-15**. The decision context is Market Squawk's production architecture and correctness:
bounded low-latency event processing, validated online order-book features, deterministic research
simulation, realistic paper execution, and tests that prevent timing or state-ordering errors.

The sources answer three different questions:

1. How can cross-thread event exchange reduce write contention and allocation jitter?
2. What top-of-book event statistic has empirical explanatory value for short-horizon price changes?
3. How can a research simulator preserve causal event order, model latency, and allow an agent's
   orders to change subsequent market state?

All factual statements are labeled **Confirmed** and cite a primary source. Design mappings and
recommendations are labeled **Inference**. Numerical paper results are treated as results of the
papers' disclosed experiments, not as Market Squawk performance claims.

## Sources Reviewed

| ID | Source | Authors | Venue / date | Problem and method | Source-quality assessment |
| --- | --- | --- | --- | --- | --- |
| papers-001 | [Disruptor: High performance alternative to bounded queues for exchanging data between concurrent threads](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, Andrew Stewart | LMAX technical paper, May 2011 | Designs and benchmarks a preallocated bounded ring, sequence barriers, wait strategies, and dependency graphs against Java bounded queues | Primary first-party exchange-engineering paper with disclosed test topology and hardware; not peer reviewed, vendor-authored, and technologically dated |
| papers-002 | [The Price Impact of Order Book Events](https://arxiv.org/pdf/1011.6402) | Rama Cont, Arseniy Kukanov, Sasha Stoikov | *Journal of Financial Econometrics* 12(1), 47–88, Winter 2014; open v3 dated April 2011 | Defines top-of-book order-flow imbalance (OFI) and estimates contemporaneous relationships among OFI, mid-price changes, depth, trade imbalance, and volume | Peer-reviewed empirical study with transparent sample, equations, filters, regressions, robustness checks, and important author-stated limitations |
| papers-003 | [ABIDES: Towards High-Fidelity Multi-Agent Market Simulation](https://par.nsf.gov/servlets/purl/10185795) | David Byrd, Maria Hybinette, Tucker Hybinette Balch | ACM SIGSIM-PADS 2020, DOI 10.1145/3384441.3395986 | Builds a single-threaded discrete-event kernel, exchange/order-book model, agent messaging, per-agent randomness, network/computation delays, and illustrative impact experiments | Peer-reviewed conference paper from university and J.P. Morgan AI Research authors; architecture is detailed, but market-fidelity validation is explicitly preliminary |

## Findings

### Low-Latency Ownership and Sequencing

**Confirmed.** The Disruptor paper's central design is a preallocated bounded ring buffer whose
storage, producer coordination, and consumer notification are separated. The paper explicitly
combines this with a rule that data have only one writer, and it uses consumer sequences to prevent
a producer from wrapping and overwriting unconsumed entries ([Thompson et al., §§4–4.3,
pp. 5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).

**Confirmed.** With one producer, the paper uses a simple sequence counter rather than a contended
claim operation; consumers observe committed entries through sequence/cursor publication and memory
barriers. Multiple producers require additional atomic coordination. The paper therefore supports a
narrow claim—single-writer sequencing avoids a class of write-contention costs—not the broader
claim that every system should be lock-free or single-threaded ([Thompson et al., §4.3,
pp. 6–7](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).

**Confirmed.** Wait strategy is an explicit resource tradeoff: blocking on a condition variable
conserves CPU but adds contention, whereas yielding or spinning spends CPU to reduce notification
latency. Consumers may batch all entries through the latest published sequence, which the authors
argue helps a lagging consumer catch up during a burst ([Thompson et al., §§4.2–4.4,
pp. 6–7](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).

**Confirmed.** The paper's benchmark compared several producer/consumer graph topologies with
`ArrayBlockingQueue` on Java 1.6 and 2011-era Intel systems. Throughput used 500 million messages and
the best of three runs; the latency test injected 50 million events below saturation, did not bind
threads to CPUs, and reported mean and tail thresholds. The authors reported 52 ns mean per hop for
their Disruptor configuration versus 32,757 ns for the bounded queue configuration, but also noted
substantial variation across JVM executions ([Thompson et al., §§5–6,
pp. 9–10](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).

**Inference.** Market Squawk should adopt the invariant, not the historical benchmark or Java API:
each live shard should be the sole writer of its mutable instrument state; inter-stage buffers should
be bounded; publication must have an explicit sequence/visibility boundary; and a full buffer must
trigger a defined backpressure, degradation, or quarantine transition. These conclusions follow
from the paper's ownership and wrap-prevention mechanics, but the exact Rust channel/ring
implementation requires current local benchmarks.

**Inference.** Preallocation and stable object layout are reasonable hot-path goals, but the paper
does not justify unsafe Rust, permanent busy-spinning, or removing all asynchronous queues. Wait
strategy should be configuration chosen from measured CPU budget, burst profile, and p99/p99.99
latency on documented target hardware.

### Order-Flow Imbalance as an Online Feature

**Confirmed.** Cont, Kukanov, and Stoikov define each best-quote update's signed contribution from
bid/ask price and queue-size changes, then sum those contributions over an interval to form OFI. A
bid-size increase is positive demand; a bid-size decrease is negative whether caused by a market
sell or a cancellation; ask-side signs are reversed. Mid-price changes are normalized in ticks
([Cont, Kukanov, and Stoikov, §2.1,
pp. 3–4](https://arxiv.org/pdf/1011.6402)).

**Confirmed.** Their stylized derivation assumes equal depth beyond the best quotes and events only
at the best quotes. The authors then explicitly relax this to a statistical relation because real
books have deeper-level events, nonuniform depth, intraday depth variation, and hidden orders; those
effects and rounding enter a noise term ([Cont, Kukanov, and Stoikov, §§2.2–2.3,
pp. 5–6](https://arxiv.org/pdf/1011.6402)).

**Confirmed.** The empirical sample comprises 21 trading days in April 2010 for 50 randomly selected
S&P 500 constituents, using consolidated Level I TAQ quotes and trades. Quote timestamps were rounded
to the nearest second; the main grid used 10-second intervals; regressions were estimated within 273
half-hour subsamples per stock. The paper therefore studies a contemporaneous, aggregated
relationship—not event-by-event prediction or exchange-native order-level data
([Cont, Kukanov, and Stoikov, §3.1 and Tables 1–2,
pp. 7–11](https://arxiv.org/pdf/1011.6402)).

**Confirmed.** In the paper's sample, the linear OFI regression averaged roughly 65% explanatory
fit, versus roughly 32% for trade imbalance alone. The OFI coefficient was almost always significant,
the depth exponent averaged about 0.98, and adding a quadratic OFI term improved average fit only
slightly. These are sample results under the paper's filters, not portable constants
([Cont, Kukanov, and Stoikov, §§3.2 and 4.1,
pp. 8–16](https://arxiv.org/pdf/1011.6402)).

**Confirmed.** The authors flag a possible tautology because OFI contains price-changing quote
events. When those events were removed for a stock subsample, fit fell but remained in a reported
35–60% range. Their appendix also removes the top 5% of each stock's spread observations and uses
heuristics to match trades to quotes, although an alternative direction test gave similar results in
a subsample ([Cont, Kukanov, and Stoikov, §3.2 and Appendix A,
pp. 8, 25–26](https://arxiv.org/pdf/1011.6402)).

**Inference.** OFI is well suited to a small, pure, incremental online-feature kernel, but the
canonical feature must specify market depth (`TopOfBook`), units, tick/lot normalization, interval or
event-time semantics, snapshot/reset behavior, connection generation, and feature version. It should
consume only validated book transitions; sequence gaps or resynchronization should invalidate its
window rather than silently continue.

**Inference.** OFI must not by itself qualify data as `DirectVerified`, prove causal price impact,
or authorize an order. A production strategy would need per-venue/per-instrument calibration,
out-of-sample validation, freshness and quality gates, risk evaluation, and explicit fallback
behavior. The paper supplies a feature hypothesis and contemporaneous evidence, not a universal
execution signal.

### Deterministic Event-Driven Market Simulation

**Confirmed.** ABIDES uses a single-threaded event kernel with monotonically non-decreasing Global
Virtual Time. Events are dequeued chronologically; same-nanosecond events are resolved in event
creation order. Each agent also has a current time and computation delay, and inter-agent messages
receive pairwise network latency plus jitter ([Byrd, Hybinette, and Balch, §§4.1–4.2,
pp. 3–5](https://par.nsf.gov/servlets/purl/10185795)).

**Confirmed.** ABIDES makes stochastic experiments reproducible by deriving an independent PRNG
for each agent from a global seed. A changed agent can consume a different number of random values
without shifting the random stream of every other agent. The kernel exposes identifiers but not
direct agent references, preventing message delivery from bypassing simulated time, network delay,
and computation delay ([Byrd, Hybinette, and Balch, §4.1,
pp. 4–5](https://par.nsf.gov/servlets/purl/10185795)).

**Confirmed.** The exchange order book uses price priority and oldest-order priority within a price,
supports partial executions, carries remaining quantity forward, reports one execution message per
partial fill, accepts residual limit quantity, and cancels by unique order ID. In this version, a
“market order” is simulated as a limit order with an arbitrarily extreme price
([Byrd, Hybinette, and Balch, §§4.3–4.4,
pp. 5–6](https://par.nsf.gov/servlets/purl/10185795)).

**Confirmed.** The authors argue that dynamic book state plus computation/network delay generates
slippage endogenously rather than through a separate slippage formula. Their impact case study uses
100 background agents, one exchange, and one large-order agent; they explicitly call the study
preliminary, and each described background agent trades one symbol on one exchange
([Byrd, Hybinette, and Balch, §§4.4, 6–7,
pp. 6, 8–9](https://par.nsf.gov/servlets/purl/10185795)).

**Confirmed.** ABIDES reports the ability to simulate tens of thousands of agents, but the paper's
case studies validate behavior through illustrative scenarios rather than a comprehensive
multi-venue, multi-asset calibration suite. The implementation described is Python 3.7 and uses a
single-threaded kernel ([Byrd, Hybinette, and Balch, abstract and §5,
pp. 1, 7](https://par.nsf.gov/servlets/purl/10185795)).

**Inference.** Market Squawk's research backtester and paper-execution simulator should make
event-order tie breaking, clock advancement, network delay, computation delay, and PRNG stream
ownership explicit and serializable. Re-running the same configuration, data versions, model
versions, and seed should reproduce orders, fills, balances, and metrics byte-for-byte or by a
documented deterministic equivalence.

**Inference.** ABIDES supports the specification's separation between live and research planes. A
causal, event-driven simulator can share domain types and pure book/risk kernels with live code, but
its global event queue, Python implementation, historical oracle, and agent population do not belong
in the live event-to-action path.

**Inference.** Endogenous book crossing is a stronger paper-fill baseline than “fill every market
order at the last price,” but the ABIDES claim does not establish empirically realistic queue
position, adverse selection, fees, venue rejection rules, or cross-venue routing. Those need
separate models, calibration data, and uncertainty labels.

### Implementation and Test Implications

The following are **Inferences** from the confirmed evidence, not requirements stated verbatim by
the papers.

#### Live queue and shard invariants

- Give each `(venue_id, instrument_id)` state object exactly one owning shard/writer; other stages
  communicate through messages rather than shared mutation.
- Bound every live queue and prevent producer wrap from overwriting an unconsumed sequence.
- Make claim, write, publish, and consume phases observable in tests, with a documented memory
  visibility mechanism supplied by safe Rust primitives.
- Define overflow as a state transition: backpressure, degraded display-only quality, disconnect and
  resynchronize, or quarantine. Never treat it as silent message loss.
- Benchmark spin, yield, and blocking policies separately at idle, nominal load, burst load, and
  saturation; record throughput, p50, p95, p99, p99.99, maximum, CPU, and memory.

#### Suggested live-path tests

1. A wrap-around property test proves that sequence `n + capacity` cannot publish before every
   required consumer has released `n`.
2. A publication test proves consumers never observe a partially written entry.
3. A stable-sharding test proves all events for one venue/instrument reach the same writer and retain
   source order within a connection generation.
4. An overload test proves the configured degradation/quarantine transition occurs at capacity and
   that execution eligibility is withdrawn.
5. A burst benchmark demonstrates bounded memory and reports latency both below and through
   saturation; historical Disruptor numbers are not acceptance criteria.

#### OFI kernel invariants and tests

1. Table-driven cases cover unchanged price/changed size, bid or ask price improvement, quote
   retreat, deletion to zero, and snapshot reset, checking the exact sign and queue quantity used.
2. A property test confirms interval OFI equals the sum of its individual event contributions.
3. An invariance test checks equivalent tick/lot-normalized books produce equivalent normalized OFI
   even when provider decimal strings differ.
4. A discontinuity test confirms sequence gap, connection-generation change, stale snapshot, crossed
   book, or quarantine clears or invalidates the feature window.
5. A research validation test partitions calibration by venue, instrument class, and time, and keeps
   the paper's historical coefficients out of production defaults.

#### Simulator and paper-execution invariants and tests

1. Same inputs, seed, configuration, and versions reproduce the identical ordered event log.
2. Changing one agent's random consumption does not alter other agents' PRNG streams.
3. Global simulated time never decreases; same-time tie breaking is deterministic and documented.
4. Send time plus computation delay plus network delay/jitter determines receipt time; no agent or
   strategy may bypass the event kernel or risk service.
5. Price-time priority, partial-fill quantity conservation, residual order state, cancellation, and
   execution-message accounting are property tested.
6. An A/B simulation changes one controlled parameter while holding seed streams and all other
   configuration fixed; results are labeled simulated and calibrated only where evidence exists.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
| --- | --- | --- | --- | --- |
| **Confirmed:** The Disruptor separates storage, producer coordination, and consumer notification around a preallocated bounded ring | [Thompson et al., §§4.1–4.2](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | Design description states all ring entries are preallocated and the three queue concerns are separated | High | First-party architecture evidence; not an independent performance replication |
| **Confirmed:** Single-writer ownership and sequences eliminate a class of write contention and prevent overwrite of unread ring entries | [Thompson et al., §§4–4.3](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | One thread owns writes; producer and consumer sequences govern claim, publication, visibility, and wrap | High | Exact memory-ordering implementation is language/runtime specific |
| **Confirmed:** Spin/yield/block wait policies trade CPU consumption against latency | [Thompson et al., §4.2](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | Paper describes condition-variable, yielding, and busy-loop alternatives and their contention/CPU implications | High | Does not pick a universal best policy |
| **Confirmed:** The reported Disruptor latency numbers come from a 2011 Java benchmark with no CPU binding and are not current Rust evidence | [Thompson et al., §§5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf) | JVM, OS, CPUs, message counts, best-of-three throughput, below-saturation latency injection, and no binding are disclosed | High | This is a limitation of portability, not a refutation of the design |
| **Confirmed:** OFI sums signed changes to best-bid/best-ask prices and quantities, including orders and cancellations | [Cont et al., §2.1](https://arxiv.org/pdf/1011.6402) | Event contribution equation and interval sum are specified | High | Top-of-book statistic; it does not distinguish all causes of a queue reduction |
| **Confirmed:** The study finds a contemporaneous linear OFI/mid-price relationship with slope inversely related to depth | [Cont et al., §§2.3–3.2](https://arxiv.org/pdf/1011.6402) | Model equations and cross-sectional regressions support the stated relationship in the sample | High for the sample | Not a universal causal or predictive law |
| **Confirmed:** Average fit was about 65% for OFI versus 32% for trade imbalance in the paper's sample | [Cont et al., §4.1](https://arxiv.org/pdf/1011.6402) | Side-by-side regressions compare the two contemporaneous variables | High for reported result | One month of filtered Level I U.S. equity data |
| **Confirmed:** OFI's high fit has a possible tautology, while deeper events, hidden orders, and intraday depth enter omitted/noise components | [Cont et al., §§2.3, 3.2](https://arxiv.org/pdf/1011.6402) | Authors state these limitations and rerun a subsample without price-changing events | High | Fit remains 35–60% in that robustness subsample, as reported by authors |
| **Confirmed:** ABIDES orders events chronologically in a single-threaded kernel and models per-agent computation and pairwise network delay | [Byrd et al., §4.1](https://par.nsf.gov/servlets/purl/10185795) | GVT, current time, computation delay, latency matrix, jitter, and priority-queue dispatch are described | High | Research-simulation architecture, not a live latency benchmark |
| **Confirmed:** Per-agent PRNGs preserve other agents' random streams in controlled A/B changes | [Byrd et al., §4.1](https://par.nsf.gov/servlets/purl/10185795) | Global seed derives independent per-agent generators | High | Reproducibility still depends on the complete configuration and implementation |
| **Confirmed:** ABIDES models price-time priority, partial fills, residual orders, and cancellation | [Byrd et al., §§4.3–4.4](https://par.nsf.gov/servlets/purl/10185795) | Order-book behavior and exchange messages are enumerated | High | Market orders are represented by extreme limit prices in this version |
| **Confirmed:** ABIDES's market-impact evidence is preliminary and narrowly configured | [Byrd et al., §§6–7](https://par.nsf.gov/servlets/purl/10185795) | Authors call the investigation preliminary; described trial uses 100 background agents, one exchange, and one impact agent | High | Does not validate realistic fills for all venues/assets |
| **Inference:** Market Squawk should share deterministic domain/book kernels while keeping live processing and research simulation as independent pipelines | [Thompson et al.](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf); [Byrd et al.](https://par.nsf.gov/servlets/purl/10185795) | Both use explicit ordering and ownership, but for different operating goals | Medium-high | Architecture fit judgment, not a paper claim |
| **Inference:** OFI should be a versioned, quality-gated feature rather than an execution-quality classifier or standalone order trigger | [Cont et al.](https://arxiv.org/pdf/1011.6402) | Evidence is contemporaneous, Level I, filtered, historical, and subject to stated omissions | High | Directly guards against overstating empirical fit |
| **Inference:** Paper fills need latency, queue/order state, partial fills, fees, rejects, and calibration uncertainty | [Byrd et al.](https://par.nsf.gov/servlets/purl/10185795); [Cont et al.](https://arxiv.org/pdf/1011.6402) | ABIDES supports some state/latency mechanisms; OFI documents liquidity dependence and omitted book depth | Medium-high | Fees, rejections, and multi-venue calibration are not established by these papers |

## Source-Specific Notes

### Papers-001: Disruptor

- **Bibliography — Confirmed:** Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, and
  Andrew Stewart; LMAX, May 2011 ([paper title page](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Problem — Confirmed:** Conventional queue-based stage exchange incurred contention,
  allocation, cache-coherence, and repeated enqueue/dequeue overhead in LMAX's measured financial
  pipelines ([§§1–3](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Method — Confirmed:** Preallocated power-of-two ring, single- or multi-producer claim strategy,
  publish cursor, consumer sequences, memory barriers, configurable waits, batching, and dependency
  graphs ([§4](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Key result — Confirmed:** The authors report substantially higher throughput and lower latency
  than their `ArrayBlockingQueue` configurations across several topologies
  ([§§5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Method limits — Confirmed:** The benchmark is Java 1.6 on 2011 CPUs/OSs; throughput is best of
  three runs; JVM results vary; the latency run did not bind threads; the comparator was chosen to
  remain bounded and provide backpressure
  ([§§5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
- **Relevance — Inference:** Retain single-writer, boundedness, explicit publication, wrap safety,
  preallocation, and measurement discipline. Re-benchmark safe Rust implementations rather than
  porting numerical results or assuming a particular queue package.

### Papers-002: The Price Impact of Order Book Events

- **Bibliography — Confirmed:** Rama Cont, Arseniy Kukanov, and Sasha Stoikov; *Journal of Financial
  Econometrics* 12(1), 47–88, Winter 2014; open paper v3 April 2011
  ([arXiv metadata](https://arxiv.org/abs/1011.6402)).
- **Problem — Confirmed:** Determine whether the combined imbalance of trades, limit orders, and
  cancellations explains short-interval price changes more parsimoniously than trade volume or trade
  imbalance alone ([abstract and §1](https://arxiv.org/pdf/1011.6402)).
- **Method — Confirmed:** Define event-level top-of-book contributions and aggregate OFI; regress
  10-second mid-price changes within half-hour windows for 50 randomly selected S&P 500 stocks in
  April 2010; model the impact coefficient against average depth
  ([§§2–3](https://arxiv.org/pdf/1011.6402)).
- **Key result — Confirmed:** A contemporaneous linear OFI model averages about 65% fit, OFI exceeds
  trade imbalance's explanatory power, and impact is approximately inversely related to depth in the
  sample ([Tables 2–4](https://arxiv.org/pdf/1011.6402)).
- **Stated limits — Confirmed:** The authors identify deeper-level activity, nonuniform depth, hidden
  liquidity, intraday variation, rounding, and possible tautology. Data are Level I and filters remove
  extreme spreads ([§§2.3, 3.2, Appendix A](https://arxiv.org/pdf/1011.6402)).
- **Relevance — Inference:** Implement OFI as one transparent online feature with precise time,
  depth, units, provenance, reset, and quality semantics. Validate per asset/venue and never equate
  empirical explanatory fit with executable data quality.

### Papers-003: ABIDES

- **Bibliography — Confirmed:** David Byrd, Maria Hybinette, and Tucker Hybinette Balch; ACM
  SIGSIM-PADS 2020, DOI 10.1145/3384441.3395986
  ([paper title page](https://par.nsf.gov/servlets/purl/10185795)).
- **Problem — Confirmed:** Provide an open, configurable, agent-based discrete-event market
  environment in which experimental agents interact through a structured exchange and can change
  subsequent simulated outcomes ([abstract and §1](https://par.nsf.gov/servlets/purl/10185795)).
- **Method — Confirmed:** Single-threaded chronological event queue, Global Virtual Time, per-agent
  computation clocks and PRNGs, pairwise latency/jitter, message-only interaction, exchange agent,
  price-time-priority order book, and historical data oracle
  ([§4](https://par.nsf.gov/servlets/purl/10185795)).
- **Key result — Confirmed:** The paper demonstrates the architecture with background-agent and
  market-impact case studies and reports scaling to tens of thousands of agents
  ([abstract and §§6–9](https://par.nsf.gov/servlets/purl/10185795)).
- **Stated limits — Confirmed:** The impact investigation is preliminary; described background
  agents trade one symbol on one exchange; examples use simplified agent behavior; and the
  implementation is single-threaded Python 3.7
  ([§§5–7](https://par.nsf.gov/servlets/purl/10185795)).
- **Relevance — Inference:** Apply deterministic ordering, independent stochastic streams, explicit
  compute/network delay, and price-time order-state tests to research backtesting and paper
  execution. Do not put the simulator, Python, or its global queue in the live path.

## Cross-Source Patterns

### Explicit ownership and ordering are correctness mechanisms

**Confirmed.** Disruptor uses producer/consumer sequences and single-writer ownership to control
cross-thread visibility, whereas ABIDES uses one chronological event queue and monotonically
non-decreasing simulation time to prevent agents from affecting the simulated past
([Thompson et al., §4](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf);
[Byrd et al., §4.1](https://par.nsf.gov/servlets/purl/10185795)).

**Inference.** Market Squawk can share a general discipline—one authoritative owner and one explicit
ordering rule per mutable state domain—without forcing live and research systems into one pipeline.
The live system optimizes bounded latency under real arrival order; the research simulator optimizes
causal, reproducible virtual time.

### Latency has different meanings at different layers

**Confirmed.** Disruptor measures internal thread-to-thread publication latency, ABIDES models agent
computation plus network latency/jitter, and the OFI study aggregates observations on 10-second grids
with source timestamps rounded to a second
([Thompson et al., §6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf);
[Byrd et al., §4.1](https://par.nsf.gov/servlets/purl/10185795);
[Cont et al., §3.1](https://arxiv.org/pdf/1011.6402)).

**Inference.** These measures must not be conflated. Market Squawk needs distinct types and metrics
for exchange/source time, receive time, internal queue latency, decision latency, modeled network
latency, modeled computation latency, and feature-window duration.

### Book state is necessary but evidence quality still gates action

**Confirmed.** OFI derives information from validated changes at the best quotes, while ABIDES
generates fills and market impact through a price-time order book whose future state responds to
orders ([Cont et al., §2.1](https://arxiv.org/pdf/1011.6402);
[Byrd et al., §§4.3–4.4](https://par.nsf.gov/servlets/purl/10185795)).

**Inference.** A feature or simulated fill is downstream of book integrity. Sequence continuity,
snapshot/update consistency, source coverage, timestamps, freshness, and trading status must be
validated before a live feature can contribute to executable intent; simulated or modeled data stay
non-executable regardless of numeric plausibility.

### Empirical fit and simulation realism are bounded claims

**Confirmed.** Cont et al. disclose filtered, contemporaneous Level I U.S. equity regressions and a
possible tautology, while the ABIDES authors call their market-impact investigation preliminary
([Cont et al., §§3.1–3.2 and Appendix A](https://arxiv.org/pdf/1011.6402);
[Byrd et al., §7](https://par.nsf.gov/servlets/purl/10185795)).

**Inference.** Market Squawk should attach coverage, calibration universe, period, method version,
and uncertainty to features and simulator parameters. Neither a high historical regression fit nor
an endogenous simulator response is sufficient evidence of live execution quality.

## Limitations and Non-Findings

1. **Confirmed non-finding:** None of the three papers evaluates Rust 1.97, Tokio, safe-Rust memory
   ordering, current consumer CPUs, or Market Squawk's proposed 100,000-events/s and sub-millisecond
   warmed p99 target. No performance claim for Market Squawk follows from this batch.
2. **Confirmed non-finding:** The papers do not specify Coinbase or Kraken sequence/checksum rules,
   reconnect generations, snapshot/delta synchronization, trading status, source authorization, or
   `DirectVerified` qualification. Those require current venue documentation and adapter fixtures.
3. **Confirmed non-finding:** The Disruptor paper does not define Market Squawk's overflow-to-quality
   transition, risk behavior, or quarantine/recovery policy. It establishes bounded sequence and
   wrap mechanics, not the domain response to overload.
4. **Confirmed limitation:** The Disruptor numbers are vendor-authored results on dated Java/JVM and
   hardware configurations, with best-of-three throughput and no CPU binding for the latency run
   ([Thompson et al., §§5–6](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf)).
5. **Confirmed limitation:** The OFI evidence is one month of filtered top-of-book U.S. equity data,
   contemporaneous rather than predictive, and subject to hidden/deeper liquidity and a possible
   price-event tautology ([Cont et al., §§2.3, 3.1–3.2, Appendix A](https://arxiv.org/pdf/1011.6402)).
6. **Confirmed non-finding:** The OFI paper does not establish coefficients, thresholds, or
   profitability for crypto, options, futures, FX, auctions, halts, or order-level depth. Its
   reported coefficients should not become production defaults.
7. **Confirmed limitation:** The ABIDES impact study is explicitly preliminary; its described
   background agents are single-symbol/single-exchange, and the platform version uses
   single-threaded Python ([Byrd et al., §§5–7](https://par.nsf.gov/servlets/purl/10185795)).
8. **Confirmed non-finding:** ABIDES's assertion that dynamic state generates realistic slippage is
   not a comprehensive empirical validation of queue position, adverse selection, exchange fees,
   rejects, cancels under race, cross-venue routing, or balances. Those capabilities remain separate
   acceptance items.
9. **Confirmed non-finding:** None of these papers addresses Arrow, Parquet, DataFusion, SQLite,
   point-in-time fundamentals, portfolio VaR/ES, fair-value hierarchy, MCP schemas, source licensing,
   credentials, or security hardening. This batch should not be cited for those decisions.
10. **Inference:** The evidence is strongest when used to generate invariants and falsifiable tests;
    it is weakest when used to select an exact library, transfer a coefficient, or claim production
    performance without local measurement.

## Source List

All sources were accessed on **2026-07-15**.

1. Martin Thompson, Dave Farley, Michael Barker, Patricia Gee, and Andrew Stewart. “Disruptor: High
   performance alternative to bounded queues for exchanging data between concurrent threads.” LMAX,
   May 2011. [Primary full paper](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf).
2. Rama Cont, Arseniy Kukanov, and Sasha Stoikov. “The Price Impact of Order Book Events.”
   *Journal of Financial Econometrics* 12(1), 47–88, Winter 2014, DOI
   10.1093/jjfinec/nbt003. [Open full paper](https://arxiv.org/pdf/1011.6402) and
   [arXiv metadata](https://arxiv.org/abs/1011.6402).
3. David Byrd, Maria Hybinette, and Tucker Hybinette Balch. “ABIDES: Towards High-Fidelity
   Multi-Agent Market Simulation.” *Proceedings of ACM SIGSIM-PADS '20*, 2020, DOI
   10.1145/3384441.3395986. [NSF-hosted full paper](https://par.nsf.gov/servlets/purl/10185795).
