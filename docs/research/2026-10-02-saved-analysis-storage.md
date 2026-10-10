# Saved investment analysis: storage design research

Research date: 2026-10-02, America/New_York. Source audit base: `3d078ba5`.
Status: recommendation, not an implemented storage change or performance claim.
The separate calendar-reader fix is pending integration; it does not implement this design.

## Decision

Keep embedded SQLite for saved investment results and the existing analytical storage for large
datasets. Make completed analyses directly queryable durable records. Remove the requirement to
reconstruct the entire decision repository before reading a saved result.

This recommendation is an engineering inference from the sources below and the current code.
SQLite explicitly identifies desktop financial-analysis applications as a suitable use case.
It supplies transactional local storage without administering another server.
[SQLite appropriate uses](https://www.sqlite.org/whentouse.html)

An immutable completed result does not require an event-sourced application. Microsoft documents
the additional query/reconstruction complexity of event sourcing and recommends adopting it only
where the benefits justify it. Our journal contains several kinds of records, including complete
analysis bundles; it is not accurate to describe every row as a business event. The concrete
problem is the application replay/read dependency, regardless of terminology.
[Microsoft event-sourcing guidance](https://learn.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)

## Current implementation and gaps

Source locations below were inspected, not inferred from earlier reports:

| Current behavior | Keep or change |
| --- | --- |
| `application/decision/persistence.rs`: SQLite WAL, synchronous FULL, atomic append and identical-request deduplication | Keep database durability and idempotency. |
| Generic `decision_records(kind, record_key, payload_json, payload_sha256)` table | Replace the saved-analysis access path with typed, indexed persisted result records. Retain structured section payloads where relational decomposition adds no useful query. |
| `DecisionApplication::open` recovers into `DecisionAuthority`; `read_investment_analysis` reads that in-memory repository | Completed-result reads should select the requested record from SQLite, independently of reconstructing all historical decision state. |
| `generate_published_investment_analysis` takes the application writer before final proposal generation/staging | Compute and serialize outside the short publication transaction; retain the necessary current authorization/cancellation check at publication. |
| Each append counts all records and sums payload lengths; fixed ceilings include 65,536 records, 256 MiB journal payload and 512 MiB database pages | Remove lifetime admission ceilings and cumulative scans from ordinary saves. Bound individual work, resident caches and response sizes instead. Disk exhaustion remains a real error. |
| Saved chart evidence retains original dataset/forecast identities; `Decision.GetInvestmentChart` accepts viewport/layer controls | Reuse these exact references and bounded reads. Audit the actual reader so filtering happens before large materialization. |

These paths share other decision behavior. Implementation must trace screen runs, recommendations,
current-share projections, outcome tracking and backup consumers before removing the old repository
dependency. This research does not authorize a blanket rewrite of virtual-paper accounting.

## Proposed data ownership

Use the existing workspace decision database and service authority. Desktop, CLI and MCP keep
using the same typed application operations; React never opens SQLite or derives financial results.

| Logical record | Persisted information and reads |
| --- | --- |
| Completed analysis | Stable analysis ID, workspace/instrument/account IDs, publication/source dates, original request ID and digest, profile/benchmark/model identities, summary fields and structured calculated results. Index the fields used by actual list/filter/sort operations. |
| Analysis sections | Persist already-calculated forecast/probability/valuation/backtest/risk explanations and values. Use section records only where separate demand loading is useful; do not make every scalar a table or serialize an unlimited monolithic document. Preserve exact decimal values. |
| Evidence references | Exact original dataset/model/artifact identifiers and hashes associated with the analysis and its sections. Reuse existing artifact metadata and retention ownership rather than introducing another blob registry. |
| Running workflow | Existing durable job/workflow records own queued/running/failed/cancelled progress. They are distinct from completed analysis records and do not require a new job engine. |

MLflow is a relevant working example of separating run metadata/parameters/metrics in a relational
backend from larger model artifacts in artifact storage. This supports the separation of concerns;
it is not a recommendation to add MLflow to Market Squawk.
[MLflow architecture](https://mlflow.org/docs/latest/self-hosting/architecture/overview/),
[backend stores](https://mlflow.org/docs/latest/tracking/backend-stores/)

## Save and recovery sequence

1. Complete provider reads and calculations using the existing jobs and immutable input identities.
   Retain the actual output values as well as provenance; reopening must not rerun a forecast or
   silently substitute current provider data.
2. Finalize any external analytical artifacts through the existing durable publication mechanism
   before referencing them as available. SQLite cannot atomically commit a separate file or another
   database. Preserve the existing artifact reservation/retention protocol through this handoff;
   do not claim a cross-store transaction or hold a database write while fetching/calculating.
3. In a short transaction, validate the exact publication request, insert the analysis and its
   sections/references, and record the unique request identity. The same ID and same digest return
   the previous result; the same ID with different input is rejected. Acknowledge after commit.
4. If job completion lives in a separate store, reconcile it to that committed analysis ID after a
   crash. A lost response must not duplicate the analysis or leave a permanently running job for
   an already committed result. Reuse the existing request reconciliation mechanism.
5. On restart, let SQLite recover its transaction state. Load current settings and the requested
   summary page. Reconcile unfinished jobs separately; do not scan/replay every completed analysis
   to make the UI usable. Validate the requested stored result and referenced evidence when read.

SQLite permits concurrent read transactions but one write transaction per database. WAL improves
reader/writer concurrency; it does not eliminate contention or make long application locks safe.
Use short service-owned writes and independent bounded reads through existing I/O workers. Never
keep SQL cursors/transactions open across provider calls, model inference or UI think time.
Keep automatic checkpointing; tune it only against observed problems. Long readers can delay it.
[SQLite transactions](https://www.sqlite.org/lang_transaction.html),
[WAL concurrency and checkpointing](https://www.sqlite.org/wal.html)

For backup, keep SQLite's online-backup API. Back up the referenced immutable artifacts consistently
with the selected database snapshot, retaining them while the backup is created. Copying a live
database file alone is not a complete WAL backup.
[SQLite backup API](https://www.sqlite.org/backup.html)

## Read and product behavior

- Saved list: SQL selects only summary columns with a stable `(published_at, analysis_id)` order.
  Use an indexed keyset cursor and a first-page upper boundary for stable paging while new results
  arrive. Do not deserialize all result bodies, count the archive on each save, or hold a transaction
  while the user moves between pages. SQLite documents the growing cost of OFFSET and the row-value
  alternative. [SQLite scrolling-window queries](https://www.sqlite.org/rowvalue.html#scrolling_window_queries)
- Detail: read one analysis and the visible sections; preserve the immutable original values.
  Live quotes are separate, timestamped data. A provider outage must not erase a stored summary.
  Missing/unreadable evidence affects its section and is reported honestly. A stored result grants
  no current trading or source-use authority; virtual paper still performs its fresh checks.
- Charts: request the selected instrument, original version, time range and layers. Select exact
  objects and columns before decoding. Existing DataFusion/Parquet supports projection and pruning;
  merely slicing a fully materialized batch does not accomplish this.
  [Apache DataFusion pruning](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/)
- Cache: cache bounded summaries/sections by workspace and immutable analysis ID. Updates invalidate
  affected lists; they do not clear the whole page or restart completed calculations. Lists and
  expanded tabs load automatically. Cancellation of a read does not cancel the durable analysis job.
- Retention: saved results keep their referenced evidence reachable. Reclaim only truly unreferenced
  temporary/artifact data under the existing ownership protocol. Do not silently delete evidence or
  stop saving after a hardcoded lifetime count to satisfy RAM goals.

## Alternatives and scope

| Option | Assessment for this application |
| --- | --- |
| SQLite plus existing analytical store | Recommended. Matches local single-user ownership, indexed small reads, transactional result publication and existing dependencies. |
| PostgreSQL | Reconsider for a genuinely shared remote service with concurrent writers. Current requirements do not justify deploying/administering it for each desktop installation. [SQLite engine selection](https://www.sqlite.org/whentouse.html) |
| DuckDB as the result/control database | Do not add a second analytical engine for this correction. DuckDB is optimized for bulk analytical work; many small concurrent queries are not its primary design target. Existing DataFusion serves the analytical role. [DuckDB workload guidance](https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads) |
| Application event sourcing plus projections | No demonstrated need for reconstructing completed analyses from all historical records. Direct immutable results already preserve what each run concluded. Keep useful audit records without making them a startup/read prerequisite. |

Pinned local dependencies are rusqlite 0.40.1 with bundled libsqlite3-sys 0.38.1 (registry amalgamation
SQLite 3.53.2), DataFusion 54.0.0 and Parquet 58.3.0. No dependency upgrade is proposed. The inspected
SQLite amalgamation is newer than the WAL-reset fixes documented by SQLite; final executable/runtime
verification remains separate from inspecting dependency sources.

## Implementation boundary and critical evidence

This is greenfield V1: change the existing implementation and all affected consumers in place.
Add no old/new compatibility stack or migration product. Preserve unique existing evidence and
backups; a development-database replacement must not silently discard it. Record the exact preserved
state and any required owner decision before replacing storage.

Use three coherent checkpoints: (1) canonical result schema/publication and all producer/read
consumers together; (2) direct paginated/section/chart reads and removal of their startup replay
dependency; (3) real saved-result reopen across Desktop/CLI/MCP and clean restart. Their exact
ownership must be refreshed against intervening changes before dispatch. Existing pure financial
validation remains; ordinary reading constructs a read model, not new execution authority.

Extend existing critical coverage only for commit/ack crash recovery, identical-request deduplication,
different-request conflict, paginated/direct reads, evidence retention and unchanged restart output.
Verify lists/details alongside a running analysis and an unavailable provider. Confirm query plans
use intended indexes and increasing archive size does not cause full startup materialization.
These are proposed checks, not claims of measured latency or whole-app RAM compliance.
