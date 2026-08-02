# Usable-release capability traceability

**Audit date:** 2026-07-18

**Audit commit:** `a829278aca4d4fc27d5a0c0aaa8e5a49f2cb5659`

**Audit tree:** `6f5d9b7be896e9a5409f367c73aa4a5d95208a9c`

**Source audit:** `.agents/tmp/usable-release-traceability-audit.md`, SHA-256
`57caaad73b638eeb785157a24ab54dba1e49251859c5f104d8f6ab6d259fb731`

**Status:** current audit input; release blocked

This report persists the usable-release traceability that previously existed only in an ignored
audit. It is read with the [canonical plan](../superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md),
[gap analysis](../plans/gap-analysis.md), and
[dependency/provider decisions](2026-07-18-usable-release-dependencies.md). It does not credit a
contract, schema, mock, synthetic source, fixture, parser alone, or runnable diagnostic as a complete
product vertical.

## Current conclusion

The audit commit is a production-oriented foundation, not a usable complete release. It has strong
domain identities and financial types, separate live/extraction contracts, bounded source policy,
capture, qualification, deterministic sharding, price-level books, live snapshots, and a small
diagnostic CLI/MCP surface. It does not have the mandatory research plane, required extraction
adapters, Python product, modeling/inference, backtesting, portfolio system, realistic execution,
fair-value workflow, or complete CLI/MCP.

The following status meanings are used:

- **Implemented:** the working producer reaches its required terminal consumer and has critical
  evidence on the audit commit.
- **Partial:** useful production code exists, but the end-to-end product vertical is open.
- **Missing:** no working production vertical exists.
- **Incorrect:** a runnable behavior exists but does not satisfy the required contract.
- **Unsafe:** a bypass or authority failure exists; none may be accepted at a release checkpoint.
- **Intentionally deferred:** allowed only for scope the first local release explicitly does not
  require, such as an optional live-money adapter. It may not describe any row below.

Every mandatory row below must be **Implemented** on one clean exact head before release. Current
state is evidence, not a percentage or permission to stop.

## Producer-to-terminal-consumer matrix

| Mandatory capability and requirement map | Honest state at audit | Working producer required | Required terminal consumer | Critical closing evidence | Closing task(s) |
| --- | --- | --- | --- | --- | --- |
| Coinbase direct book/trades and qualified live path (`P-02/P-03`, `S-04`, `L-01`, `X-03`) | **Partial.** Public Level 2, heartbeat, matches, capture, sequence/book foundations exist; no authorized direct `full` snapshot/replay-to-action vertical. | `market-squawk-adapter-coinbase`: authenticated `ws-direct`, `full`, REST L3 snapshot, queued exact-sequence replay, status/coverage/precision and quarantine. Public `ws-feed` remains display/research/failover. | Instrument shard -> online features -> typed strategy -> comprehensive risk -> one-use dispatch -> realistic paper/audit. | Content-hashed protocol fixtures through production decoders; local reconnect/snapshot/gap/overflow cases; opt-in authorized endpoint smoke; exact live-to-risk-paper demonstration. | 2 |
| Kraken production book/trades (`P-02`, `S-05`, `L-19`, `X-03`) | **Partial foundation; missing adapter.** CRC32 canonicalization exists, but no stream adapter. | `market-squawk-adapter-kraken`: subscribe/ack, snapshot/update, strict transport order, per-update CRC32, exact lexemes, health, reconnect and resync; never a fabricated venue sequence. | Current shards/features and Source/Market comparison services. Execution remains disallowed while quality is below `DirectVerified`. | Official snapshot/update/checksum fixtures; fragment/control/reconnect/delete/depth/checksum cases; opt-in smoke. Default `DirectUnverified` ceiling unless a reviewed domain-contract decision proves the no-sequence composite satisfies execution integrity. | 6 |
| Realistic risk-controlled paper execution (`S-12`, `E-01`-`E-08`) | **Incorrect/unsafe.** Immediate diagnostic fills and an incomplete intent/risk boundary are not an execution adapter. | Complete strategy/intents, non-forgeable risk approval and one-use dispatch, deterministic paper adapter with fees, latency, slippage, depth, partial fills, rejection, cancel, recovery and reconciliation. | Audited orders/fills/balances/positions consumed by portfolio and bounded Bot/Execution services; no CLI/MCP/model/adapter bypass. | Type-privacy and single-use evidence; deterministic transition/cancel/recovery fixtures; cash/position/fee invariants; no-action on stale data or inference failure. | 2 |
| SQLite catalog, rights and secrets (`A-02`, `T-01`, `T-06/T-07`, `X-11`) | **Missing.** Journal/capture files are not the research catalog. | `market-squawk-data` single writer with migrations, instruments, identifiers, source operations/rights, cursors, runs, idempotency, manifests, artifacts and audit; platform OS-keyring/encrypted fallback. | Every extraction reservation and dataset publication; services receive opaque references, never credentials or paths. | Migration/restart/integrity/backup and rights-conflict evidence; OS-store and Argon2id/XChaCha fallback rotation/redaction evidence. | 3 |
| Arrow/Parquet/DataFusion research storage (`A-02`, `T-02`-`T-09`, `X-13`) | **Missing.** None is in the working package graph. | Versioned Arrow conversion, immutable content-addressed Parquet objects/manifests, crash-safe compaction, bounded manifest-pinned DataFusion query. | Required providers -> committed dataset -> query/PIT, with CLI-only bounded read-only SQL and domain services for MCP. | Decimal/time/provenance round trip; object-before-manifest crash recovery; idempotency conflict; compaction invariance; SQL/row/byte/memory/deadline/cancellation confinement; measured storage/query evidence. | 4 |
| Complete local file extraction (`S-03`, `S-10`, `S-15`-`S-18`) | **Missing.** No `adapters/` package exists. | CSV/TSV, JSON/NDJSON, XML, Excel, Parquet, SQLite/database export, OFX/QFX, broker-export and user-owned/licensed file adapters with raw hashes, bounds and explicit error policy. | Canonical observations -> rights reservation -> manifest-pinned dataset/query/PIT. | Critical hostile quoting/duplicate-key/entity/archive/path/decimal/time fixtures through production parsers; raw preservation, schema/provenance, confinement and idempotent rerun. | 7, 11 |
| SEC filings, submissions, XBRL and Company Facts (`P-06`, `S-06`, `T-05/T-07`, `X-04`) | **Missing.** Extraction contracts only. | `market-squawk-adapter-sec`: declared identity/shared rate budget, bulk and incremental acquisition, filing documents/metadata, XBRL facts/contexts/units, amendments and lineage. | Financial statements/fundamentals -> PIT -> analytics/Python/modeling/valuation -> Fundamental services. | Official rights-recorded fixtures; conditional/bulk/incremental reconciliation; numeric/unit/context rejection; amendment/revision preservation; idempotency and opt-in network smoke. | 8, 11, 12, 19 |
| FRED/ALFRED, BLS and Treasury macro (`P-06`, `S-07`-`S-09`, `T-10`, `F-12`) | **Missing.** No provider adapter or macro dataset. | Three provider adapters with shared budgets, rights admission, series metadata, explicit vintages/revisions/preliminary flags, official rates/methodology and source health. | Macro observations -> PIT -> yield/surprise/scenario/Python/modeling -> Macro services. | FRED real-time/vintage/revision and per-series-rights evidence; BLS tier/chunk/partial-result evidence; Treasury pagination/XML/schema/methodology evidence; retry/cooldown, key redaction and opt-in smokes. | 9, 11, 12, 19 |
| Portfolio import (`P-06/P-07`, `S-11`, `E-13`) | **Missing.** `PaperAccount` is neither an importer nor authoritative portfolio state. | `market-squawk-adapter-portfolio`: raw holdings/transactions/cash-flow/OFX/broker records, instrument/currency/account resolution, supplied totals and typed discrepancies. | Immutable inputs -> portfolio accounting/reconciliation -> risk account revision/backtest/fair value/services. | Raw-byte preservation, duplicate/idempotency, ambiguity rejection, totals reconciliation and credential redaction using deterministic provider/export fixtures. | 10, 16 |
| Point-in-time datasets (`P-04`, `D-09`, `T-10/T-11`, `M-02`-`M-06`) | **Missing.** Time semantics exist as domain types only. | Manifest-bound as-of selection, revision/supersession policy, historical universes/constituents/delistings, corporate actions, leakage checks, labels and chronological splits. | Content-addressed feature/label Parquet -> models, Python training, backtesting, portfolio and fair-value analysis. | Future-perturbation, delayed/unknown publication, same-date, vintage, delisting/survivorship, merger/roll/corporate-action and compaction-invariance evidence with exact dataset/config/code hashes. | 11 |
| Live and batch analytics/feature registry (`F-01`-`F-14`, `M-01`) | **Partial live kernels; missing release vertical.** Spread/midpoint/microprice and primitive momentum exist. | `market-squawk-analytics`: complete online features plus batch return/risk/fundamental/macro/scenario kernels and versioned registry metadata. | Live strategy/risk, PIT feature datasets, Python parity, portfolio/model/backtest/fair-value services. | Critical golden/property evidence at decimal/float boundaries, warm-up/null/time semantics, online/batch parity where promised, and measured live-feature/risk latency. | 2, 12 |
| Model registry, bundles and native inference (`P-05`, `M-07/M-08/M-11`) | **Missing.** No modeling package or artifact trust boundary. | `market-squawk-modeling`: registry, complete hashed bundle validator, model metadata, normalization, native backend and explicit fallback/no-action. | Research predictions and typed strategy input only after schema/feature/version/universe/threshold validation; then normal risk. | Hash/schema/feature/normalizer/universe/period/label/code/metrics/threshold mismatch evidence; corruption/resource bounds; deterministic native inference; every error yields no intent. | 13 |
| Mandatory Python finance/training product (`P-05`, positive requirement missing from old `M-10`) | **Missing.** Python's absence from the live path is a safety property, not implementation. | Locked `python/market_squawk` package plus narrow PyO3 analytics bridge: manifest/PIT reader, Decimal/currency/time/provenance conversion, Rust-kernel parity, reproducible training/evaluation/export. | Fully hashed model candidate -> Rust bundle validator; usable local research API/examples. Never a live runtime dependency. | Clean offline Python 3.10 install; dataset-hash rejection; Decimal/time round trip; Rust/Python finance parity; PIT leakage rejection; seeded training/export/bundle handoff; live dependency graph remains Python-free. | 14 |
| ONNX-compatible inference (`M-09/M-11`) | **Missing.** No runtime decision is implemented. | Required bounded `tract-onnx` backend; optional isolated local-dynamic `ort` backend with downloads/copies/fetches disabled and verified local runtime. | Same bundle validator and `InferenceBackend`; strategy/risk only after successful bounded inference. | Hostile/corrupt model, operator/shape/tensor/model/thread/deadline limits; golden native/ONNX tolerances; optional runtime hash/version confinement; no action on any failure. | 15 |
| Complete portfolio accounting and analytics (`P-07`, `E-09`-`E-13`, `F-11/F-13`) | **Missing.** No accounts/lots/performance/exposure system. | `market-squawk-portfolio`: accounts, cash, transactions, lots/cost basis, gains/income, multi-currency policy, performance/attribution, exposure, rebalance, VaR/ES and scenarios. | Authoritative account revisions -> risk/paper/backtest reconciliation plus Portfolio/Analysis/FairValue services. | Supplied-total and accounting identities; lot/corporate-action/multi-currency evidence; TWR/MWR semantics; attribution/exposure totals; discrete VaR/ES; restart/revision and risk integration. | 16 |
| Research backtesting (`P-05`, `M-12`) | **Missing.** Diagnostic replay does not qualify. | `market-squawk-backtesting`: PIT inputs, strategy/model execution, costs/slippage, corporate actions/delistings, portfolio accounting and experiment/trial governance. | Reconciled orders/fills/cash/positions/performance and lineage for strategy/model evaluation. | Look-ahead/survivorship/delisting/corporate-action fixtures; deterministic rerun; order/fill/cash/position reconciliation; recorded search/selection history. | 17 |
| ASC 820 / IFRS 13 fair value (`P-08`, `V-01`-`V-05`) | **Missing service.** Hierarchy types and type separation are only foundations. | `market-squawk-valuation`: measurement, input, method, evidence, ruleset, classification reason, override and approval audit. | Portfolio/accounting and FairValue/Analysis services only; no path to live data qualification. | Level-1 decision tables; missing evidence -> `Unclassified`; stale/delayed/proxy/adjusted/modeled/similar-instrument rejection; immutable override/approval/ruleset audit; Level 2/3 cannot become `DirectVerified`. | 18 |
| Shared services, complete CLI and typed local stdio MCP (`P-09`, `A-08`, `C-01`-`C-09`) | **Partial diagnostic; complete product surface missing.** Five tools do not satisfy MCP. | Transport-neutral bounded services; rmcp stdio lifecycle/cancellation/deadline/backpressure/audit/artifacts; every Source/Market/Research/Fundamental/Macro/Portfolio/Analysis/Model/FairValue/Bot/Execution handler; full CLI over the same services. | Local operator/MCP client with bounded inline results or opaque content-hashed artifacts; mutations traverse the sole risk/paper authority. | Protocol lifecycle/ID/cancel/output/adversarial evidence; schema/result/time/instrument limits; artifact confinement/hash; audit/redaction; prohibited surfaces; CLI/MCP parity; offline complete-domain smoke. | 5, 19 |
| Release hardening and local demonstration (`P-01/P-10`-`P-13`, `X-09`-`X-16`) | **Missing terminal evidence.** No complete candidate exists; no performance claim is authorized. | Integrated application plus focused fuzz targets, measured benchmarks, security/dependency/license/credential/artifact checks, SBOM/release docs and local demonstration. | A user can initialize, ingest live/research/portfolio data, query/PIT/analyze/train/infer/backtest, paper trade through risk, value, and use CLI/MCP entirely locally. | Clean unchanged exact-head format/clippy/test/release/audit/fuzz/benchmark/demo evidence; documented hardware/OS/toolchain/fixtures/latency/throughput/memory; four grouped quarter reviews with zero unresolved findings; pushed release commit and cleaned worktrees. | 20 |

## Dataset producer and reader closure

Dataset registration is atomic with the first real writer and reader. The data package may provide a
versioned extension mechanism, but it may not pre-create empty future tables and call them complete.

| Dataset families | First production writer | Reader that closes the release atom |
| --- | --- | --- |
| Instruments, identifiers, venues, symbol/lifecycle history | Catalog plus file/SEC/portfolio resolution | Query and PIT historical identity/universe selection |
| Trades, quotes, order books | Asynchronous publication from qualified live adapters | Market analytics and bounded Market services |
| Corporate actions | File/SEC normalization and PIT policy | PIT, portfolio accounting and backtest |
| Filings, XBRL, statements, fundamentals | SEC ingestion and normalization | PIT, fundamental analytics and Fundamental services |
| Macro series and observations | FRED/ALFRED, BLS and Treasury | PIT, yield/surprise analytics and Macro services |
| Accounts, positions, transactions, cash flows | Portfolio import, then authoritative portfolio revisions | Portfolio reconciliation/risk and Portfolio services |
| Features and labels | PIT dataset builder/analytics registry | Rust bundles, Python training and backtest |
| Predictions and models | Bundle/native/Python publication after validation | Inference, backtest and Model services |
| Strategies, orders, fills and risk decisions | Strategy registry plus sole risk/paper writer | Portfolio reconciliation and Execution services |
| Valuations and fair-value evidence | Valuation measurement/rules workflow | Approval and FairValue/Analysis services |
| Quality results and lineage | Every producer through common commit contracts | PIT selection, source/query services and release demonstration |

Every row retains schema version, source/quality/provenance, relevant source/effective/published/
available/ingested/revision/superseded times, and content identity. A writer-only table, reader over
fixtures only, or directory listing without a committed manifest is still **Missing**.

## Critical evidence policy

Tests remain thin, critical, and tied to an invariant or externally observable product behavior.
The release does not use raw test counts as a progress or quality metric. Do not add scripts or tests
that parse documentation wording, headings, checkboxes, task labels, roadmap language, or forbidden
word lists. Do not add a custom staging wrapper. Prose truth is reviewed directly; executable checks
protect code/build/security behavior.

Evidence is attached to its real implementation path:

- adapter protocol and resynchronization evidence lives beside the owning adapter under
  `adapters/*/tests`, using content-hashed, rights-recorded provider fixtures and local protocol
  servers; external smokes are separate and opt-in;
- catalog/publication/PIT/query evidence lives under `crates/market-squawk-data/tests` and proves
  transactions, crash recovery, manifests, point-in-time selection and query limits;
- analytics/model/portfolio/backtest/execution/valuation evidence lives in the owning package and
  proves only its material mathematical or authority invariants;
- MCP/CLI evidence proves protocol lifecycle, bounds, cancellation, artifacts, service parity and
  prohibited surfaces, not a catalog of implementation details; and
- exact-head release evidence and measured benchmark reports live under `docs/verification` and
  `docs/reports`, are linked to one unchanged commit/tree, and distinguish deterministic local gates
  from truthful opt-in external results.

Focused evidence closes only its lane. Integration credit requires the producer-to-terminal-
consumer demonstration, the applicable full locked Wave gate, and the grouped quarter review.

## Live qualification decisions

### Coinbase

The official [overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview) calls
authenticated `ws-direct.exchange.coinbase.com` direct access to Coinbase Exchange servers and
defines exact per-product sequences. The official
[`full` channel](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) defines the queued
WebSocket plus REST snapshot/replay algorithm.

**Decision.** The first default execution-eligible candidate is user-authorized `ws-direct` using
`full` plus the matching L3 REST snapshot. `DirectVerified` is granted only after the complete local
instrument, venue, coverage, generation, exact sequence, duplicate/out-of-order, snapshot/update,
timestamp, freshness, status, precision and bounded-queue gates pass. There is no documented full-
channel checksum; checksum is required only where the venue supports one. Public `ws-feed` remains
the zero-credential display/research/failover path and does not inherit direct-delivery evidence.

### Kraken

The official [V2 book schema](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)
has no numeric venue sequence, while the
[checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2) provides CRC32
state validation for every update.

**Decision.** Kraken's production adapter implements a named, versioned transport-order plus
per-update-checksum integrity profile and never invents `SequenceNumber`. CRC32 detects local-state
divergence but does not prove that every intermediate update was received if a later state collides
or converges. Under the current valid-sequence-progression contract, Kraken therefore defaults to
`DirectUnverified` and cannot issue immediate automated action. An adapter author cannot weaken that
ceiling. Only an explicit domain-contract change, independent review, and unchanged-head external
evidence can admit a no-venue-sequence composite as `DirectVerified`.

## Execution order and release barrier

The current [canonical plan](../superpowers/plans/2026-07-17-market-squawk-usable-complete-release.md)
closes the matrix through Tasks 0-20 and maximizes safe parallelism by disjoint producer/consumer
ownership. Shared manifests, lockfile, migrations registry, application composition, live authority,
risk dispatch and release evidence remain serialized. Exactly four grouped delivery-quarter reviews
freeze Tasks 0-6, 7-12, 13-18, and 19-20 respectively; historical Q-prefixed identifiers remain
audit locators, not extra quarters.

This report was audited before the approved implementation head existed. At Task 0/1, the
integration owner must refresh current files, public APIs, versions, source policies, requirement
statuses, closing-task ownership and evidence paths against the independently approved exact head.
Tasks 3 and 5 may proceed provisionally in disjoint worktrees against this audit anchor as the
canonical plan permits; each candidate must refresh or rebase and pass its exact-head gate before
merge. The refreshed diff and dependency graph must be reviewed before Task 2 dispatch, integration
of provisional work, or any Wave/Stage credit. Until then, this artifact is an independently useful
decision record, not approval to merge production code or claim a capability complete.
