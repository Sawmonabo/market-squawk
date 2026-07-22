# Market Squawk Complete Local Platform Deep Research Report

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Research Scope and Date](#research-scope-and-date)
3. [Methodology](#methodology)
4. [Source Coverage](#source-coverage)
5. [Key Findings](#key-findings)
6. [GitHub Ecosystem Findings](#github-ecosystem-findings)
7. [Academic and Research Findings](#academic-and-research-findings)
8. [Official Documentation Findings](#official-documentation-findings)
9. [Reputable Source Findings](#reputable-source-findings)
10. [Cross-Source Synthesis](#cross-source-synthesis)
11. [Recommendations or Decision Implications](#recommendations-or-decision-implications)
12. [Risks, Gaps, and Open Questions](#risks-gaps-and-open-questions)
13. [Source Matrix](#source-matrix)
14. [Appendix A: Source Inventory](#appendix-a-source-inventory)
15. [Appendix B: Subagent Report Inventory](#appendix-b-subagent-report-inventory)

## Executive Summary

**Decision: proceed through staged, test-gated implementation; do not claim the complete local
release yet.** The evidence supports Market Squawk's core separation into a deterministic live
plane, a revision-aware research plane, and a local CLI/MCP control plane. Mature local components
exist for asynchronous Rust, exact columnar storage, embedded SQL, SQLite control state, typed stdio
MCP, and local model inference. None supplies Market Squawk's complete financial semantics,
execution-quality predicate, point-in-time lineage, risk boundary, fair-value classification, or
acceptance evidence.

**Confirmed.** The strongest direct dependency candidates are Apache Arrow/Parquet, Apache
DataFusion, and the official Rust MCP SDK; `tract-onnx` is a pure-Rust inference candidate, while
`ort` is a conditional native-runtime option with release-candidate and supply-chain caveats
([Arrow](https://github.com/apache/arrow-rs), [DataFusion](https://github.com/apache/datafusion),
[MCP SDK](https://github.com/modelcontextprotocol/rust-sdk),
[`tract-onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/),
[`ort`](https://github.com/pykeio/ort)). Trading, research, and valuation repositories are useful
architecture/test references, not drop-in assurance.

**Inference.** Execution must remain fail-closed: only source-specific, continuously validated
`DirectVerified` state may create immediate automated action; every strategy/model intent must pass
one non-bypassable risk service; inference failure produces no action. Research records must retain
availability, revisions, historical universes, raw hashes, and provenance. Fair-value hierarchy,
market depth, and data quality remain separate types. MCP, SQL, Python, persistence, model loading,
and arbitrary filesystem/network work stay outside the live path.

## Research Scope and Date

The decision context was the migration from the existing single-crate v0.1 toward the specified
complete zero-mandatory-cost, local-first release. Research was frozen **as of 2026-07-15** and
covered architecture, live Coinbase/Kraken data, research providers, storage, analytics, temporal
correctness, models, backtesting, risk, paper execution, portfolio implications, fair value, MCP,
security, supply chain, and performance evidence. It did not inspect or modify application code and
does not establish current repository implementation status.

## Methodology

The workspace inventory selected **42 source families**: 10 GitHub repositories, 10 academic or
engineering papers, 14 official-documentation families, and 8 accounting/policy/security sources.
Fourteen scoped batch reports were independently synthesized into four category reports, then
semantically deduplicated here. Primary specifications and provider documentation control protocol,
quota, and accounting claims; repositories provide implementation and maintenance evidence; papers
support design and mathematical properties; standards/guidance inform governance within their
stated applicability.

Claims marked **Confirmed** are supported by cited source evidence. **Inference** denotes a Market
Squawk design or adoption decision. Repository popularity, upstream labels, paper coefficients, and
supervisory guidance are not treated as product certification. No new searches or sources were
introduced in this final synthesis.

## Source Coverage

| Category | Families | Primary decision contribution | Principal boundary |
| --- | ---: | --- | --- |
| GitHub | 10 | Dependency candidates, adapter/reference patterns, release practices | License, maturity, and upstream tests do not prove Market Squawk behavior |
| Academic/research | 10 | Bounded ownership, features, simulation, PIT bias, ES, execution, valuation context | Samples/models are coverage-bound; two 2026 papers are provisional |
| Official documentation | 14 | Toolchain, runtime, data stack, MCP, exchange/provider contracts, inference APIs | Documentation proves contracts, not their implementation or performance |
| Reputable authorities | 8 | Fair value, fair access, secure development, provenance, model/stress governance | Accounting judgment and regulatory applicability cannot be automated or overstated |

## Key Findings

1. **Architecture is viable.** **Inference.** Stable single-writer instrument shards and bounded
   queues fit live state; Arrow/Parquet/DataFusion plus SQLite fit research/control state. The planes
   may share invariant-rich types and pure kernels but must not share blocking runtime paths.
2. **Integrity is source-specific.** **Confirmed.** Coinbase documents sequenced `full` snapshot
   replay, but the assigned `level2` examples expose no sequence/checksum; Kraken specifies an
   atomic precision-sensitive CRC32 over top-ten levels
   ([Coinbase](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels),
   [Kraken](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)).
3. **Temporal correctness is multidimensional.** **Inference.** Effective, published, available,
   received, ingested, revised, and superseded times plus historical universe membership are
   required. Observation or filing date alone permits look-ahead and survivorship bias
   ([look-ahead](https://arxiv.org/html/2607.04958),
   [survivorship](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf)).
4. **Risk dimensions are non-substitutable.** **Inference.** Data quality, accounting hierarchy,
   model validation, statistical risk, scenario results, and order eligibility answer different
   questions. No score or classification may bypass risk.
5. **Local-first still needs explicit bounds.** **Confirmed.** Tokio, Reqwest, DataFusion, SQLite,
   and MCP expose mechanisms whose defaults do not define Market Squawk's queue, timeout, memory,
   disk, concurrency, result, or cancellation policy
   ([Tokio](https://docs.rs/tokio/latest/tokio/sync/mpsc/),
   [DataFusion](https://datafusion.apache.org/user-guide/configs.html),
   [MCP](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)).
6. **Performance remains unproven.** No source validates 100,000 events/second, sub-millisecond
   warmed p99 decision latency, or bounded sustained-burst memory for this implementation.

## GitHub Ecosystem Findings

**Confirmed.** Arrow Rust 59.1.0 and active DataFusion provide the specified research mechanics;
the official Rust MCP SDK provides typed stdio/server plumbing. They do not implement canonical
finance schemas, PIT rules, query authorization, audit, or risk
([Arrow release](https://github.com/apache/arrow-rs/releases/tag/59.1.0),
[DataFusion README](https://github.com/apache/datafusion/blob/18121a68433ac19763787e9763ef3f50508befd5/README.md),
[MCP SDK](https://github.com/modelcontextprotocol/rust-sdk/blob/839922d8fd44216024b23ae72d16d1eae8cbf013/README.md)).
The MCP workspace/version evidence conflicts with an older README dependency example, so the exact
package, features, and license transition need lockfile-level verification.

**Confirmed.** NautilusTrader and Barter demonstrate useful domain decomposition, state ownership,
adapter, reconnect, risk-hook, audit, simulation, and testing patterns. NautilusTrader is
LGPL-3.0; Barter is MIT but expressly disclaims production live-trading fitness. Neither proves the
required Coinbase/Kraken execution-quality contract
([Nautilus adapters](https://github.com/nautechsystems/nautilus_trader/blob/c7d60c1d6e64d72076f8cd2a652d199263679223/ADAPTERS.md),
[Barter disclaimer](https://github.com/barter-rs/barter-rs/blob/33e56188e2095781331f85aa3d7f88e251eec65a/README.md#legal-disclaimer-and-limitation-of-liability)).
Kraken CLI is a useful official reference for signing, retries, reconnect, fixtures, and release
artifacts, but it does not maintain a fully validated instrument-owned book and its paper/MCP
behavior is insufficient as Market Squawk policy
([client](https://github.com/krakenfx/kraken-cli/blob/aa32814cea70913a70c9909693a7abd762963e83/src/client.rs)).

**Confirmed.** Qlib supplies PIT/experiment ideas but brings Python, pickle, and optional MLflow;
OpenBB supplies provider/fetch/fixture patterns but is AGPL-3.0 and its inspected FRED transform
drops real-time bounds; Strata supplies mature pricing/scenario patterns but is Java and not a
fair-value hierarchy engine
([Qlib PIT](https://github.com/microsoft/qlib/blob/d5379c520f66a39953bad76234a7019a72796fd0/docs/advanced/PIT.rst),
[OpenBB license](https://github.com/OpenBB-finance/OpenBB/blob/c78488d7d18b9f9f89d2f897e58bcdbbd9ddb690/LICENSE),
[Strata scenarios](https://github.com/OpenGamma/Strata/blob/39c46e342a4a95ac083d66287f038f6ae276692a/modules/calc/src/main/java/com/opengamma/strata/calc/marketdata/ScenarioDefinition.java)).
**Inference.** Use these as offline or architectural references, not embedded platform cores.

## Academic and Research Findings

**Confirmed.** Disruptor supports preallocated bounded rings, explicit sequences, wrap protection,
and single-writer contention reduction; ABIDES supports deterministic event order, explicit
latency, price-time priority, partial fills, and seeded simulation
([Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf),
[ABIDES](https://par.nsf.gov/servlets/purl/10185795)). **Inference.** Apply the ownership/ordering
invariants, not old Java throughput numbers. Live sharding and research discrete-event simulation
remain independent implementations.

**Confirmed.** Cont et al. define incremental top-of-book order-flow imbalance and report a
sample-specific contemporaneous depth-conditioned price relation; they also identify omitted
liquidity and possible tautology. Recent simulation work emphasizes timing, persistent flow,
impact calibration, and multi-metric validation but is a four-stock 2026 v1 preprint
([OFI](https://arxiv.org/pdf/1011.6402),
[simulation](https://arxiv.org/pdf/2603.24137)). **Inference.** Features, coefficients, fill models,
and impact assumptions must be versioned and coverage-bound; simulated outputs are `Modeled`.

**Confirmed.** Backtest selection risk, survivorship, and look-ahead are distinct defects. PBO-style
diagnostics cannot repair future information, survivor-only universes, or implausible fills
([PBO](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting)).
Acerbi–Tasche show ES definitions diverge at discontinuities and support exact worst-tail-mass
weighting ([ES](https://arxiv.org/pdf/cond-mat/0104295)). Almgren–Chriss supplies a stylized
impact/volatility scheduling baseline, not a paper broker
([execution](https://doi.org/10.21314/JOR.2001.041)). **Inference.** Store every trial and implement
exact weighted ES; separately model fees, latency, liquidity, partial fills, rejects, cancellation,
balances, and reconciliation.

## Official Documentation Findings

**Confirmed.** Rust 1.97.0, released 2026-07-09, and Edition 2024/resolver 3 form a coherent virtual
workspace baseline, but resolver selection does not prove the locked all-feature graph
([Rust releases](https://doc.rust-lang.org/stable/releases.html),
[resolver](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)). Arrow
Decimal128 is exact fixed point but does not encode currency, tick/lot, or rounding policy. SQLite
has one writer, WAL/checkpoint constraints, per-connection foreign keys, and distinct integrity
checks ([Arrow types](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html),
[SQLite WAL](https://www.sqlite.org/wal.html)).

**Confirmed.** SEC requires an honest user agent and caps automated access at 10 requests/second;
FRED requires a per-user key and provides explicit vintages but no numeric limit in the reviewed
pages; BLS has documented v1/v2 daily, series, year, and rolling limits. Treasury REST and XML have
different pagination, schema eras, and missing-value behavior
([SEC](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[FRED](https://fred.stlouisfed.org/docs/api/fred/series_observations.html),
[BLS](https://www.bls.gov/developers/api_faqs.htm),
[Treasury REST](https://fiscaldata.treasury.gov/api-documentation/),
[Treasury XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)).
**Inference.** Implement separate budgets, pagination, caches, health, raw hashes, revisions, and
deterministic fixtures. Treasury CMTs are interpolated indicative inputs—not trades, executable
quotes, or Level 1 evidence
([methodology](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve)).

**Confirmed.** MCP 2025-11-25 requires protocol-clean stdout, initialization/version negotiation,
typed schemas, security controls, and race-aware cancellation. `tract-onnx` 0.23.4 provides local
parsing/typing/execution mechanics but no universal operator, numerical, threading, warm-up,
latency, or memory guarantee
([MCP transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
[`tract`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/)). **Inference.** Bound every MCP dimension;
hash-verify and golden-test model bundles; load/warm outside the live path; atomically activate only
validated backends; produce no action on error.

## Reputable Source Findings

**Confirmed.** ASC 820/IFRS 13 Level 1 requires an unadjusted quoted price in an active, accessible
market for an identical item at the measurement date; classification follows the lowest-level
significant input. Adjusted, proxy, third-party, or modeled evidence cannot silently become Level 1
([FASB](https://storage.fasb.org/ASU2011-04.pdf),
[IFRS interpretation](https://media.ifrs.org/2015/IFRIC/January/IFRIC-Update-January-2015.html)).
**Inference.** Store ruleset, evidence, method, reason, override, approval, and professional-review
state. Hierarchy never confers execution eligibility.

**Confirmed.** NIST SSDF 1.1, ASVS 5.0.0, and SLSA 1.2 address complementary development,
application-control, and source/build-provenance concerns
([SSDF](https://csrc.nist.gov/pubs/sp/800/218/final),
[ASVS](https://owasp.org/www-project-application-security-verification-standard/),
[SLSA](https://slsa.dev/spec/v1.2/)). **Inference.** Produce local release evidence—locked checks,
audits, fuzz smoke, SBOM, hashes, and provenance—without making cloud, containers, telemetry, or a
formal compliance claim mandatory.

**Confirmed.** SR 26-2 is current non-prescriptive banking guidance emphasizing purpose,
materiality, validation, monitoring, dependencies, inventories, change, and vendor models; Basel's
current stress principles emphasize governed severe/plausible scenarios, material-risk coverage,
data aggregation, challenge, and use
([SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm),
[Basel](https://www.bis.org/bcbs/publ/d450.htm)). **Inference.** Apply these as conservative bundle
and scenario governance, not as a claim of applicability, compliance, or approval.

## Cross-Source Synthesis

**Inference.** The target design should enforce this evidence chain:

```text
authorized source -> validated canonical state -> versioned feature/model/scenario
-> typed intent -> non-bypassable risk verdict -> paper/authorized execution -> reconciliation
```

The live side uses stable hashing to an instrument-owned single writer, bounded queues, checked
scaled integers, explicit generation/sequence/checksum/freshness/status transitions, and
quarantine/resynchronization. Coinbase `full` is only a candidate after sequenced snapshot replay
and all local gates; Kraken requires atomic decimal-preserving checksum validation on every
eligible update. A heartbeat updates connection health, not market freshness.

The research side ingests provider objects and retains fetched response bodies immutably by content
hash; provider URLs remain mutable locator metadata. It normalizes records into versioned Arrow
batches, publishes validated Parquet through manifests, and queries bounded DataFusion contexts.
SQLite stores cursors, registries, manifests, and audit/control state—not per-event facts. PIT joins
filter on defensible availability and supersession and include delisted instruments, historical
constituents, identifier changes, corporate actions, and terminal proceeds.

Features and models bind source coverage, schemas, units, windows, warm-up, null policy, code,
training/calibration periods, trials, thresholds, and fallback. Backtests bind event order, seeds,
cost/fill assumptions, scenarios, and datasets. Portfolio analytics require transactions, cash,
cost basis, currencies, reconciliation, exposure, attribution, ES, and stress; the reviewed sources
support principles, not a complete portfolio importer or calculation implementation.

Strategies emit intents only. One risk service checks source quality/freshness, instrument/account,
position/notional/exposure/leverage/capital, price/slippage, duplicates/rates, losses/drawdown, and
expiry. Paper execution independently models fees, latency, liquidity, partial fills, rejects,
cancellation races, balances, positions, and order state. CLI and MCP call these same services and
cannot construct approved orders directly.

## Recommendations or Decision Implications

### Adoption tiers

1. **Adopt and pin:** Rust 1.97.0/Edition 2024/resolver 3; Tokio/Serde/Reqwest/WebSocket mechanics;
   Arrow/Parquet, compatible DataFusion, SQLite, and a minimal-feature official MCP SDK. Keep all
   domain policy in Market Squawk-owned crates.
2. **Adopt conditionally:** start local ONNX with `tract-onnx` after per-bundle operator/golden/
   concurrency tests. Offer `ort` only as an isolated optional backend with default downloads off,
   a locally provisioned hash-verified runtime, and explicit native supply-chain review.
3. **Reference, do not embed by default:** NautilusTrader, Barter, Qlib, OpenBB, Strata, and Kraken
   CLI. Reuse concepts, public fixtures, and offline-oracle ideas only after license and independent
   correctness review; avoid LGPL/AGPL code incorporation into the dual-licensed core by default.
4. **Defer claims:** live-money execution, formal ASVS/SLSA/regulatory compliance, universal model
   compatibility, and performance acceptance until the actual implementation produces evidence.

### Staged delivery gates

**Inference.** Follow the specified order: (1) baseline/domain/workspace; (2) direct adapters,
books, sharding, source health; (3) research ingestion/catalog/Arrow/Parquet/DataFusion; (4) PIT
features, models, portfolios, and backtests; (5) shared risk and realistic paper execution; (6)
fair-value evidence and bounded MCP; (7) fuzzing, benchmarks, audits, and release hardening. Each
stage remains runnable and advances only on deterministic tests plus separate opt-in network tests.

## Risks, Gaps, and Open Questions

- **Implementation:** no reviewed source proves that the repository currently implements or passes
  the required adapters, schemas, books, risk, paper execution, portfolios, valuation, or MCP.
- **Live coverage:** Coinbase evidence is channel-specific single-venue coverage; Kraken CRC covers
  top ten only. Staleness thresholds, overflow policy, reconnect budgets, and all asset-class
  adapters remain product/test decisions.
- **Temporal data:** SEC public availability is not exact; FRED vintages are date-granular; BLS
  lacks complete pre-capture vintages; Treasury provides no exact publication/immutability promise.
- **Research/model risk:** there is no universal OFI coefficient, PBO cutoff, ES confidence,
  scenario shock, impact coefficient, fill parameter, or model validation threshold. The recent
  look-ahead and simulator preprints are provisional.
- **Portfolio/execution:** reviewed evidence does not supply complete import reconciliation, tax-lot
  policy, corporate-action processing, realistic queue position, broker reconciliation, or live
  account/order authorization.
- **Valuation:** authoritative standards require current complete standards, evidence, judgment,
  override, approval, and review; the sources do not provide a self-executing compliance engine.
- **Security/licensing:** exact dependency features, transitive licenses, credential store,
  encrypted fallback, endpoint allowlists, parser limits, hostile-file behavior, and formal
  ASVS/SLSA applicability remain unresolved. License observations are not legal advice.
- **Performance:** queue sizes, DataFusion limits, Parquet layout, model threading, and every live
  percentile/throughput/memory claim require measured target-hardware evidence.

## Source Matrix

| Decision area | Strongest evidence | Adoption decision | Preserved caveat |
| --- | --- | --- | --- |
| Live ownership | [Disruptor](https://lmax-exchange.github.io/disruptor/files/Disruptor-1.0.pdf), [Tokio](https://docs.rs/tokio/latest/tokio/sync/mpsc/) | Bounded instrument-owned single writers | Design evidence, not current Rust performance |
| Exchange integrity | [Coinbase](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels), [Kraken](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) | Source/channel-specific validators and quarantine | Sequence/checksum coverage differs; no provider-wide qualification |
| Research storage | [Arrow](https://arrow.apache.org/rust/arrow/index.html), [DataFusion](https://datafusion.apache.org/), [SQLite](https://www.sqlite.org/wal.html) | Pinned direct dependencies behind manifests/bounds | Mechanics do not supply semantics or PIT correctness |
| PIT/backtesting | [FRED/ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html), [survivorship](https://terpconnect.umd.edu/~wermers/ftpsite/FAME/Brown_Goetzmann_Ibbotson_Ross.pdf), [PBO](https://www.risk.net/journal-of-computational-finance/2471206/the-probability-of-backtest-overfitting) | Preserve availability, vintages, universes, all trials | Different bias controls are not interchangeable |
| Features/simulation | [OFI](https://arxiv.org/pdf/1011.6402), [ABIDES](https://par.nsf.gov/servlets/purl/10185795) | Versioned kernels and deterministic modeled scenarios | Sample coefficients/fidelity are not portable defaults |
| Risk/execution | [ES](https://arxiv.org/pdf/cond-mat/0104295), [Almgren–Chriss](https://doi.org/10.21314/JOR.2001.041), [Basel](https://www.bis.org/bcbs/publ/d450.htm) | Exact ES, governed stress, shared risk, calibrated paper fills | No universal limits, shocks, or realistic fill engine supplied |
| Fair value | [FASB](https://storage.fasb.org/ASU2011-04.pdf), [IFRS 13](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/) | Separate evidence-based hierarchy service | Standards/judgment required; hierarchy is not execution quality |
| Inference | [`tract`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/), [`ort`](https://github.com/pykeio/ort) | tract baseline; optional isolated ort | Bundle-specific support/security/performance; no action on error |
| MCP/control | [MCP specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [Rust SDK](https://github.com/modelcontextprotocol/rust-sdk) | Minimal pinned stdio SDK plus owned authorization/bounds | Protocol plumbing is not audit, risk, or filesystem security |
| Release | [SSDF](https://csrc.nist.gov/pubs/sp/800/218/final), [ASVS](https://owasp.org/www-project-application-security-verification-standard/), [SLSA](https://slsa.dev/spec/v1.2/) | Local evidence gate; optional hosted assurance | No blanket compliance or vulnerability-absence claim |

## Appendix A: Source Inventory

The authoritative machine-readable inventory is [`source-inventory.json`](source-inventory.json).
It contains the research topic, 2026-07-15 cutoff, decision context, batch lineage, selection
rationale, credibility/freshness signals, priority, status, and notes for all 42 families:

- `github-001`–`github-010`: ten repositories spanning trading architecture, Arrow/DataFusion,
  research/provider workflows, MCP, ONNX, valuation, and Kraken tooling.
- `papers-001`–`papers-010`: ten engineering, microstructure, simulation, temporal-bias, risk,
  execution, and fair-value studies.
- `docs-036`–`docs-049`: fourteen official toolchain, runtime, storage, protocol, provider, exchange,
  and inference documentation families.
- `reputable-sources-071`–`reputable-sources-078`: eight accounting, access-policy, security,
  supply-chain, model-risk, and stress authorities.

## Appendix B: Subagent Report Inventory

| Category | Batch deep dives | Category synthesis |
| --- | --- | --- |
| GitHub | [`batch-001`](reports/github/batch-001.md), [`batch-002`](reports/github/batch-002.md), [`batch-003`](reports/github/batch-003.md) | [`github-synthesis`](reports/category-synthesis/github-synthesis.md) |
| Papers | [`batch-001`](reports/papers/batch-001.md), [`batch-002`](reports/papers/batch-002.md), [`batch-003`](reports/papers/batch-003.md), [`batch-004`](reports/papers/batch-004.md) | [`papers-synthesis`](reports/category-synthesis/papers-synthesis.md) |
| Official documentation | [`batch-001`](reports/docs/batch-001.md), [`batch-002`](reports/docs/batch-002.md), [`batch-003`](reports/docs/batch-003.md), [`batch-004`](reports/docs/batch-004.md), [`batch-005`](reports/docs/batch-005.md) | [`docs-synthesis`](reports/category-synthesis/docs-synthesis.md) |
| Reputable sources | [`batch-001`](reports/reputable-sources/batch-001.md), [`batch-002`](reports/reputable-sources/batch-002.md) | [`reputable-sources-synthesis`](reports/category-synthesis/reputable-sources-synthesis.md) |
