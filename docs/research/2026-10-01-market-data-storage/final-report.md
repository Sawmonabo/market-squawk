# Market Squawk Embedded Market-Data Storage Architecture Deep Research Report

## Table of Contents

- [Executive Summary](#executive-summary)
- [Research Scope and Date](#research-scope-and-date)
- [Methodology](#methodology)
- [Source Coverage](#source-coverage)
- [Key Findings](#key-findings)
- [GitHub Ecosystem Findings](#github-ecosystem-findings)
- [Academic and Research Findings](#academic-and-research-findings)
- [Official Documentation Findings](#official-documentation-findings)
- [Reputable Source Findings](#reputable-source-findings)
- [Cross-Source Synthesis](#cross-source-synthesis)
- [Recommendations or Decision Implications](#recommendations-or-decision-implications)
- [Risks, Gaps, and Open Questions](#risks-gaps-and-open-questions)
- [Source Matrix](#source-matrix)
- [Appendix A: Source Inventory](#appendix-a-source-inventory)
- [Appendix B: Subagent Report Inventory](#appendix-b-subagent-report-inventory)

## Executive Summary

**Compaction is useful for consolidating small files within bounded ranges; repeatedly rewriting whole history into one file is unsuitable for continuous ingestion.** Recommended direction, as an **inference**: commit bounded active microbatches and indexed current/as-of rows in existing SQLite; export bounded cold ranges to immutable Parquet queried through existing DataFusion. This is unimplemented and unmeasured, not a proven performance winner.

Compare this complete lifecycle honestly with database-native DuckDB. Native analytical tables could remove enough custom export, manifest and query machinery to justify replacement. Existing dependencies reduce integration cost; they do not justify retaining an unsuitable design. [SQLite transactions](https://www.sqlite.org/lang_transaction.html), [DataFusion 54](https://docs.rs/datafusion/54.0.0/datafusion/), [DuckDB storage](https://duckdb.org/docs/current/internals/storage)

## Research Scope and Date

Research date: **2026-10-01**. Scope: one local Rust/Tauri backend serving Desktop/CLI/MCP, continuous events, history, financial point-in-time evidence, saved analyses and restart. Audit anchor: **`140cfa1c` plus preserved, incomplete and unverified WIP**. This is research, not implementation or approval. Execution must refresh paths, contracts and evidence against the approved head. Whole-app RAM measurement remains deferred until workflows are complete. [Owner-test contract](../../plans/v1-owner-test-goal.md)

Repository links target this report's intended location, `docs/research/2026-10-01-market-data-storage/`; frozen GitHub links identify historical code independently of WIP.

## Methodology

Four discovery/batch/category lanes reviewed primary repositories, full-text papers, official documentation and named-author engineering accounts. This report deduplicates their findings. Documented capability, audited implementation and proposed application policy are distinct evidence levels. Historical papers and vendor experiments supply mechanisms, not transferable benchmark results.

## Source Coverage

Fourteen source records cover four repositories, three papers, three documentation sets and four engineering accounts. Sources were opened on the research date. DataFusion claims use version 54; live documentation reports newer versions. DuckDB stable tags control repository claims; moving documentation and older article defaults require version refresh. No local workload comparison was performed. [Versioned DataFusion configuration](https://raw.githubusercontent.com/apache/datafusion/54.0.0/docs/source/user-guide/configs.md), [DuckDB stable bindings](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/README.md)

## Key Findings

SQLite currently owns catalog/control authority; Parquet holds analytical rows and raw provider originals are separately retained. The architecture explicitly says SQLite is **not a synchronous per-tick warehouse**. Proposed database microbatches deliberately revise that boundary in place; they are not current behavior. [Physical storage contract](../../architecture/market-data-provider-architecture.md#physical-storage-and-publication)

At the frozen base, cumulative publication/source-run limits of 4096 and a manifest-object ceiling of 1024 create lifetime failures. Current compaction streams the whole pinned generation into one object and retains originals. It reduces active object count without establishing disk reclamation. WIP alters lineage but remains unverified. [Frozen source-run bound](https://github.com/Sawmonabo/market-squawk/blob/140cfa1c/crates/market-squawk-data/src/manifest/availability.rs#L9), [frozen publication bindings](https://github.com/Sawmonabo/market-squawk/blob/140cfa1c/crates/market-squawk-data/src/manifest/catalog.rs), [manifest bounds](../../../crates/market-squawk-data/src/manifest.rs), [compactor](../../../crates/market-squawk-data/src/ingest/market_compaction.rs)

The streaming writer disables compression, dictionaries and statistics. Columnar compression/pruning cannot be assumed for those objects. Changing filenames also leaves single-manifest selection and a 256-route discovery ceiling unresolved; winner selection needs indexed logical relationships rather than complete partition enumeration. [Streaming writer](../../../crates/market-squawk-data/src/parquet_store/streaming.rs), [route selection](../../../crates/market-squawk-data/src/catalog/market_recovery.rs)

## GitHub Ecosystem Findings

DataFusion 54 demonstrates metadata-based multi-file pruning; its example enumerates directory entries to build an index, so copying it would reproduce lifetime enumeration. Its runtime exposes pools/cache/spill controls but warns that memory enforcement is incomplete. [Parquet index example](https://github.com/apache/datafusion/blob/54.0.0/datafusion-examples/examples/data_io/parquet_index.rs), [runtime](https://github.com/apache/datafusion/blob/54.0.0/datafusion/execution/src/runtime_env.rs)

DuckDB Rust bindings support native storage, Arrow and appenders. Append success can precede constraint failure; destructor flushing discards errors. Explicit flush and transaction commit must both succeed before durable acknowledgement. Native compilation and packaging add ownership costs. [Appender](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/crates/duckdb/src/appender/mod.rs), [commit](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/crates/duckdb/src/transaction.rs), [bindings](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/README.md)

## Academic and Research Findings

The 2019 DuckDB paper motivates embedded concurrent analytics. The 2008 column-store experiments show that layout alone does not reproduce columnar execution benefits. The 2016 RUM conjecture frames read/write/space amplification; it is neither a universal proof nor an application-RAM model. None ranks this application's current engines. [Embedded analytics](https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf), [column stores](https://www.cs.umd.edu/~abadi/papers/abadi-sigmod08.pdf), [RUM](https://www.openproceedings.org/2016/conf/edbt/paper-12.pdf)

## Official Documentation Findings

SQLite WAL supports snapshot readers alongside **one writer**. Long readers impede checkpoints; FULL synchronizes WAL at commit, while NORMAL can lose committed transactions after power/system failure. Existing configuration uses WAL/FULL. Bounded transactions and independent short read snapshots are necessary; application mutexes can still serialize callers. [WAL](https://www.sqlite.org/wal.html), [synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous), [catalog configuration](../../../crates/market-squawk-data/src/catalog.rs)

DuckDB owns transactional native storage and maintenance, with embedded read/write access in one process. DataFusion supplies execution over Parquet rather than transactional table management. Neither engine snapshots nor Parquet metadata define financial knowledge cutoffs. [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency), [transactions](https://duckdb.org/docs/current/sql/statements/transactions), [Parquet format](https://parquet.apache.org/docs/file-format/)

## Reputable Source Findings

Publisher engineering accounts distinguish small transactional inserts from later Parquet flushing, volatile buffer acceptance from durable persistence, and bounded background merges from forced single-part rewriting. These support lifecycle design, not adoption of DuckLake or ClickHouse or their numeric settings. Eventual compaction cannot supply immediate revision correctness. [Streaming patterns](https://duckdb.org/2025/10/13/duckdb-streaming-patterns), [async inserts](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse), [merge guidance](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final)

## Cross-Source Synthesis

**Proposed contract:**

1. **Durable ingestion:** buffer by bounded bytes/rows/time, preserving exact numeric types. Seal and synchronize required raw microbatch bytes and their publication coordinates before committing canonical rows, cursor/progress and evidence references in one SQLite transaction. Acknowledge only successful durable commit; no per-tick `fsync`. Uncertain retries use a unique source/event/revision identity: identical content is idempotent; mismatched content is rejected or explicitly recorded as a new revision. Precommit files are recoverable staging/orphans, never published data. [Transactions](https://www.sqlite.org/lang_transaction.html), [UPSERT](https://www.sqlite.org/lang_upsert.html)
2. **Financial selection:** retain immutable revisions and separate event/effective time from availability/knowledge time, including first-observed-local evidence where applicable. Apply both cutoffs before choosing winners; specify deterministic revision/sequence/tie rules and conflict handling. Export by committed row identity/progress, not maximum event timestamp: late observations, corrections and equal-time arrivals remain eligible.
3. **Hot/cold handoff:** seal, synchronize and verify bounded archive files before one catalog transaction publishes their membership and export progress. A reader pins a logical ingestion horizon, exact hot visibility and archive membership from one coherent snapshot. Union by canonical revision identity exactly once; retain hot rows/objects needed by active pins. Crashes before publication leave retryable staging; after publication, catalog authority identifies the complete range. Raw files and database commits have no automatic cross-store transaction.
4. **Maintenance and evidence:** compact selected time/key ranges in background under work-byte, temporary-disk and concurrency budgets. Keep financial identity independent of placement. Preserve raw originals and saved result/pin obligations; reclaim predecessor Parquet only after durable equivalent replacement and proof that no reader or saved physical identity needs it. Current hash-bound evidence cannot simply be deleted. [Merge guidance](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final)

## Recommendations or Decision Implications

**Inference: start with SQLite active rows plus DataFusion cold history.** Index source/event/revision and both clocks; keep hot storage bounded through archival progress. Select relevant cold objects through indexed catalog ranges, with schema-appropriate compression/statistics. Bulk history may publish directly to bounded Parquet through the same authority contract.

| Choice | Benefit | Full lifecycle cost / switch condition |
| --- | --- | --- |
| SQLite + DataFusion | One atomic active-row/cursor/evidence commit; existing typed consumers | Single-writer contention, row/index/WAL writes, export duplication, snapshot handoff and GC remain application-owned |
| Database-native DuckDB | Native analytical tables, transactions and internal layout/maintenance | Native packaging, exact financial-type verification, conflict handling and evidence backup; preferable if these cost less than the hybrid machinery or required workloads defeat SQLite |
| Direct Parquet batches | Natural for sizable scan-oriented historical imports | Own durable buffering, files, publication, pins and maintenance; tiny flushes recreate overhead |

A DuckDB replacement should colocate rows, cursors and evidence indexes in one transaction authority or explicitly own cross-store recovery. Do not retain SQLite/DataFusion/DuckDB together without a demonstrated gap. LSM/heavy server systems receive no recommendation: reviewed amplification theory and server examples establish no missing local capability. [DuckDB storage](https://duckdb.org/docs/current/internals/storage), [RUM](https://www.openproceedings.org/2016/conf/edbt/paper-12.pdf)

Implementation dependencies: approved-head refresh → shared logical identity/acknowledgement contract → active ingestion/indexed selectors → pinned archive handoff → bounded maintenance/reclamation → existing consumers and restart. Change canonical paths and architecture together, without migrations, compatibility layers or a new server; preserve unique WIP and backups. [Owner-test engineering rules](../../plans/v1-owner-test-goal.md#engineering-and-verification)

## Risks, Gaps, and Open Questions

Checkpoint debt needs an explicit background owner and short reader lifetimes; aggressive checkpoints can contend with writers. Backup must snapshot SQLite and pin/copy every referenced raw/Parquet/saved-evidence object consistently. Copying only a live database main file is insufficient. [Checkpoint API](https://www.sqlite.org/c3ref/wal_checkpoint_v2.html), [backup API](https://www.sqlite.org/backup.html)

Indefinite operation means **no software lifetime row/run/file cap**, not infinite disk. Disk pressure requires recoverable backpressure and owner-visible retention/archive policy; never silently delete required originals or saved evidence.

Smallest critical validation: extend existing ingestion/restart coverage to prove atomic acknowledgement, duplicate/content-mismatch handling and crash recovery across handoff; one late-revision/equal-time pinned read during archive replacement must preserve results without gaps/duplicates; one backup/restore must reopen saved evidence. Compare representative ingestion plus current/as-of and analytical reads including maintenance cost. Event rates, sizing and performance remain unknown. No giant test matrix or premature whole-app RAM measurement is justified.

## Source Matrix

| Coverage | Primary evidence | Decision boundary |
| --- | --- | --- |
| Repositories: GH01–04 | Tagged Rust/database implementations | Capability/integration, not local performance |
| Papers: PAP-01–03 | Embedded analytics, executor experiments, RUM | Historical mechanisms, not current ranking |
| Documentation: DOC-01–03 | WAL/transactions, native storage, execution/format | Engine guarantees, not financial semantics |
| Engineering: ENG-01–04 | Transactions, buffering, flushing, merges | Publisher mechanisms, not desktop benchmarks |

## Appendix A: Source Inventory

All accessed 2026-10-01. The adjacent [structured inventory](source-inventory.json) retains discovery dates, credibility signals and assignments; its `assigned` statuses describe research routing rather than incomplete report coverage.

| ID | Primary source | Reviewed anchor |
| --- | --- | --- |
| GH01 | [rusqlite](https://github.com/rusqlite/rusqlite/blob/v0.40.1/README.md) | Local 0.40.1 |
| GH02 | [DataFusion](https://github.com/apache/datafusion/blob/54.0.0/README.md) | Local 54.0.0 |
| GH03 | [DuckDB](https://github.com/duckdb/duckdb/blob/v1.5.6/README.md) | Stable 1.5.6 |
| GH04 | [duckdb-rs](https://github.com/duckdb/duckdb-rs/blob/v1.10506.0/README.md) | Stable 1.10506.0 |
| PAP-01 | [DuckDB: an Embeddable Analytical Database](https://duckdb.org/pdf/SIGMOD2019-demo-duckdb.pdf) | SIGMOD 2019 |
| PAP-02 | [Column-Stores vs. Row-Stores](https://www.cs.umd.edu/~abadi/papers/abadi-sigmod08.pdf) | SIGMOD 2008 |
| PAP-03 | [RUM Conjecture](https://www.openproceedings.org/2016/conf/edbt/paper-12.pdf) | EDBT 2016 |
| DOC-01 | [SQLite WAL](https://www.sqlite.org/wal.html) | Moving official documentation; linked transaction/sync/backup subpages |
| DOC-02 | [DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency) | Moving current documentation; native storage/transactions |
| DOC-03 | [DataFusion 54](https://docs.rs/datafusion/54.0.0/datafusion/), [Parquet](https://parquet.apache.org/docs/file-format/) | Versioned API and format specification |
| ENG-01 | [Analytics-Optimized Concurrent Transactions](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions) | 2024-10-30 |
| ENG-02 | [Streaming Patterns](https://duckdb.org/2025/10/13/duckdb-streaming-patterns) | 2025-10-13 |
| ENG-03 | [Asynchronous Inserts](https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse) | 2023-08-01; amended |
| ENG-04 | [OPTIMIZE FINAL](https://clickhouse.com/resources/engineering/clickhouse-optimize-table-final) | Updated 2026-06-15 |

## Appendix B: Subagent Report Inventory

Research provenance remains in scratch `reports/`: four `category-synthesis/{github,papers,docs,reputable-sources}-synthesis.md` reports and corresponding `{github,papers,docs,reputable-sources}/batch-001.md` deep dives. Their decision-critical findings and primary citations are reproduced here; no scratch artifact is required to understand this decision.
