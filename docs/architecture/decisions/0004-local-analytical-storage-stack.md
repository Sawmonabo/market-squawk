# 0004: Use a Local Analytical Storage Stack

Status: Accepted

Decision date: 2026-07-16

## Context

Mutable control metadata, in-memory analytical exchange, durable columnar datasets, and analytical
SQL have different update and access patterns. SQLite provides local transactions and recovery for
small mutable authority records, but it is not the live event store or the intended format for
large columnar scans. Parquet is durable and efficient for analytical generations, but it is not a
mutable control catalog. Arrow provides a shared in-memory representation, not durable publication
authority. A query engine is still needed to plan and execute bounded analytical reads.

The product must run locally without an external database service, cloud warehouse, or mandatory
container.

## Decision

SQLite owns control metadata, Arrow owns in-memory columnar exchange, Parquet owns durable
analytical data, and DataFusion owns embedded analytical SQL.

SQLite stores migrations, source/catalog authority, cursors, run state, manifest records, lineage,
and other mutable control-plane state. Arrow `RecordBatch` values carry closed, versioned,
fingerprinted schemas across ingestion, publication, queries, and Python boundaries. Parquet stores
immutable analytical objects that are published and addressed through exact manifests rather than
directory discovery. DataFusion resolves registered manifest generations and executes bounded local
analytical queries over those admitted objects.

Publication coordinates filesystem durability and catalog authority. A generation becomes
authoritative only after its exact schema, object identities, content hashes, and lineage are
admitted. SQLite, Parquet, DataFusion, and Python remain outside the live event-to-action path.
Analytical SQL remains a bounded, manifest-pinned local CLI capability; MCP exposes the typed
application-operation registry instead.

## Consequences

- Each layer has one primary responsibility and independent recovery rules.
- Dataset readers pin a manifest generation and closed schema identity instead of scanning a
  directory for apparent completeness.
- Derived outputs retain exact typed parent generations.
- Catalog backup and Parquet backup must be coordinated so references and objects remain
  consistent.
- Schema and manifest versions are explicit migrations; data is not silently reinterpreted.
- Analytical work is local and portable but still subject to count, byte, time, cancellation, and
  query-plan limits.
- Multiple storage technologies increase implementation discipline but avoid forcing incompatible
  workloads into one store.

## Rejected alternatives

- Querying SQLite per live event or storing the live hot path in SQLite.
- Using SQLite as the sole large analytical fact store.
- Using Parquet files as mutable control-plane records.
- Treating Arrow batches as durable publication by themselves.
- Inferring a dataset from files found under a directory.
- Requiring a remote database, cloud warehouse, or external catalog service.
- Building a custom SQL engine instead of embedding DataFusion.

## Related architecture

- [Research data plane](../research-data-plane.md)
- [Control plane](../control-plane.md)
- [Data, time, and provenance](../data-time-and-provenance.md)
- [Deployment](../deployment.md)

## Evidence and sources

- [SQLite catalog configuration](../../../crates/market-squawk-data/src/catalog.rs),
  [closed Arrow schemas](../../../crates/market-squawk-data/src/schema.rs),
  [Parquet publication](../../../crates/market-squawk-data/src/parquet_store.rs), and
  [bounded DataFusion query service](../../../crates/market-squawk-data/src/query.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [Manifest and lineage authority](../../../crates/market-squawk-data/src/manifest.rs), reviewed at
  `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd`.
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html) and
  [write-ahead logging](https://www.sqlite.org/wal.html), reviewed 2026-07-23.
- [Apache Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html), reviewed
  2026-07-23.
- [Apache Parquet file format](https://parquet.apache.org/docs/file-format/), reviewed 2026-07-23.
- [Apache DataFusion introduction](https://datafusion.apache.org/user-guide/introduction.html),
  reviewed 2026-07-23.
