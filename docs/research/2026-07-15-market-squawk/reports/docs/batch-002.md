# Docs Batch 002 Deep Dive

## Table of Contents

1. [Batch Scope](#batch-scope)
2. [Sources Reviewed](#sources-reviewed)
3. [Findings](#findings)
4. [Evidence Table](#evidence-table)
5. [Source-Specific Notes](#source-specific-notes)
6. [Cross-Source Patterns](#cross-source-patterns)
7. [Limitations and Non-Findings](#limitations-and-non-findings)
8. [Source List](#source-list)

## Batch Scope

This report reviews only assigned official sources: `docs-039` (Apache Arrow Rust
and Parquet), `docs-040` (Apache DataFusion), and `docs-041` (SQLite). It focuses on
schema/versioning, Decimal128, Parquet small-file management, bounded embedded
queries, SQLite integrity/concurrency, and live-path exclusion. Sources were accessed
on **2026-07-15**. **Confirmed** statements are directly documented; **Inference**
statements apply that evidence to Market Squawk.

## Sources Reviewed

| ID | Official family | Pages reviewed | Main use |
|---|---|---|---|
| `docs-039` | Apache Arrow Rust / Parquet | [Arrow](https://arrow.apache.org/rust/arrow/index.html), [`DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html), [`Schema`](https://arrow.apache.org/rust/arrow/datatypes/struct.Schema.html), [Parquet](https://arrow.apache.org/rust/parquet/index.html), [writer properties](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html) | Typed batches, exact decimals, schemas, Parquet layout |
| `docs-040` | Apache DataFusion | [Overview](https://datafusion.apache.org/), [configuration](https://datafusion.apache.org/user-guide/configs.html), [SQL types](https://datafusion.apache.org/user-guide/sql/data_types.html), [SQL API](https://datafusion.apache.org/library-user-guide/using-the-sql-api.html) | Embedded analytical SQL and resource limits |
| `docs-041` | SQLite | [Docs](https://www.sqlite.org/docs.html), [WAL](https://www.sqlite.org/wal.html), [isolation](https://www.sqlite.org/isolation.html), [foreign keys](https://www.sqlite.org/foreignkeys.html), [STRICT](https://www.sqlite.org/stricttables.html), [PRAGMAs](https://www.sqlite.org/pragma.html) | Local catalog, concurrency, constraints, integrity |

## Findings

### 1. Schema and dataset versioning

**Confirmed.** Arrow `RecordBatch` combines a `Schema` and arrays. A schema contains
fields and string metadata; metadata does not change physical memory layout. Arrow
can project and merge compatible schemas. ([Arrow](https://arrow.apache.org/rust/arrow/index.html),
[`Schema`](https://arrow.apache.org/rust/arrow/datatypes/struct.Schema.html))

**Inference.** Arrow metadata and structural merging are not a schema registry,
lineage ledger, point-in-time policy, or proof of financial semantic compatibility.
Market Squawk should register explicit canonical Arrow schemas and store, in dataset
manifests, the canonical schema version, provider schema, transformation revision,
quality/provenance semantics, and compatibility policy. CSV/JSON inference may aid
discovery but must not silently define production schemas. Incompatible records must
fail or be quarantined.

### 2. Decimal128 and time semantics

**Confirmed.** Arrow `Decimal128(precision, scale)` represents an exact signed
128-bit integer multiplied by `10^-scale`; scale can be negative. DataFusion maps
SQL `DECIMAL(p,s)` with `p <= 38` to Decimal128 and higher precision to Decimal256.
([Arrow `DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html),
[DataFusion SQL types](https://datafusion.apache.org/user-guide/sql/data_types.html))

**Inference.** Each financial column needs an explicit precision, scale, currency or
unit, and rounding policy. Adapters should parse text directly to checked integers or
decimals and reject overflow, excess precision, or unauthorized rounding; conversion
through `f64` defeats exactness. Decimal128 does not itself encode currency, tick size,
lot size, or adjustment policy.

**Confirmed.** An Arrow timestamp with timezone is a physical instant. Without a
timezone it is a wall-clock value in an unknown timezone; an empty timezone is not
UTC. Some logical constraints, including Date64 day alignment, are not library-enforced.
([Arrow `DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html))

**Inference.** Store instants explicitly in UTC, preserve exchange/source timezone
context separately, and validate logical constraints at ingestion. Availability,
publication, effective, source, receive, and ingestion times must remain distinct.

### 3. Parquet files and compaction

**Confirmed.** Parquet files contain metadata and row groups. Rust writer controls
include row/byte limits, compression, dictionary encoding, statistics, page size,
bloom filters, sorting, and key-value metadata. The default row-group maximum is
1,048,576 rows and page size is 1 MiB; smaller pages may improve predicate pruning
but increase size. Type coercion defaults off because it can lose information, and
content-defined chunking is experimental. ([Parquet](https://arrow.apache.org/rust/parquet/index.html),
[writer properties](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html))

**Inference.** Row-group tuning does not prevent thousands of small files. Research
writes should be buffered into immutable staging files, validated, then atomically
published through a new manifest. Compaction must run outside the live path and must
verify counts/content before replacing a visible manifest. Partitioning and target
file size should follow measured query patterns; the assigned docs provide no
universal optimum. Keep coercion off unless dataset-specific round-trip tests justify
it, and do not make experimental chunking a baseline requirement.

### 4. DataFusion containment

**Confirmed.** DataFusion is an extensible Arrow-based embedded query engine. Its
target partition count defaults to available CPU cores. Runtime memory limit and temp
directory default to unset; maximum temporary-directory size defaults to 100 GiB and
zero spill fan-in means unlimited. Unbounded joins can exhaust memory. Output defaults
include at least four parallel files. For data below about 1 MiB, documentation
recommends one partition because parallel overhead can dominate.
([DataFusion overview](https://datafusion.apache.org/),
[configuration](https://datafusion.apache.org/user-guide/configs.html))

**Inference.** Embedded does not mean bounded. Market Squawk should construct
controlled contexts with allowlisted catalogs, explicit memory/temp limits, an
artifact-owned temp directory, bounded parallelism, cancellation, timeouts, and row/
byte result limits. Small transformations should reduce output parallelism or pass
through manifest-aware compaction. MCP should expose typed bounded operations, not
SQL text. CLI read-only SQL should be constrained to approved datasets because SQL
can also alter DataFusion configuration.

### 5. SQLite control-plane rules

**Confirmed.** SQLite permits one writer at a time. WAL allows concurrent readers and
a writer with snapshot isolation, but is same-host only, requires checkpointing, can
still return `SQLITE_BUSY`, and can grow behind long readers. WAL activation must be
verified from `PRAGMA journal_mode=WAL`'s returned value. ([SQLite WAL](https://www.sqlite.org/wal.html),
[isolation](https://www.sqlite.org/isolation.html))

**Inference.** Use SQLite only for configuration, cursors, manifests, registries, and
run state. Use short transactions, a bounded pool, bounded busy retries, checkpoint
monitoring, and preferably a serialized application writer for mutation-heavy paths.
Do not place analytical facts or per-event live queries there.

**Confirmed.** SQLite reports a rare concurrent WAL-reset defect fixed in 3.51.3,
with backports 3.44.6 and 3.50.7. ([SQLite WAL](https://www.sqlite.org/wal.html))

**Inference.** Pin or verify a fixed runtime version and expose it through `doctor`.

**Confirmed.** Foreign keys are disabled by default and must be enabled on every
connection; enabling them inside an active transaction has no effect. STRICT tables
accept only `INT`, `INTEGER`, `REAL`, `TEXT`, `BLOB`, and `ANY`, enforcing lossless
coercion. `integrity_check` does not find foreign-key errors; `foreign_key_check` is
separate. `user_version` is application-controlled, while `schema_version` is internal
and unsafe as an application migration counter. ([Foreign keys](https://www.sqlite.org/foreignkeys.html),
[STRICT](https://www.sqlite.org/stricttables.html), [PRAGMAs](https://www.sqlite.org/pragma.html))

**Inference.** Initialize and verify every pooled connection. Prefer STRICT tables
with explicit constraints, but retain canonical encodings for UUIDs, decimals, enums,
and timestamps. Maintain transactional migration history, optionally mirrored in
`user_version`; never write `schema_version`. Health and restore checks should run both
`integrity_check` and `foreign_key_check`.

**Confirmed.** In WAL mode, `synchronous=FULL` preserves durability through power
loss; `NORMAL` may roll back a recent commit after power/system failure; `OFF` can
permit corruption. ([PRAGMAs](https://www.sqlite.org/pragma.html))

**Inference.** Durable manifests, audit records, and execution state should default
to FULL unless a user explicitly accepts weaker durability. OFF is inappropriate.

### 6. Hard live-path exclusion

**Confirmed.** These systems perform file I/O, query planning/execution, spilling,
locking, transactions, and checkpointing with documented contention and resource
behavior.

**Inference.** SQLite/DataFusion queries, Parquet writes, compaction, manifest
transactions, and arbitrary Arrow batch construction must not occur in the
socket-to-decision path. A bounded non-blocking fan-out may asynchronously capture
validated events, but bot decisions must not await persistence. Overflow requires an
explicit audit-data policy and must not silently corrupt execution-critical state.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| Arrow batches carry a schema and column arrays. | [Arrow](https://arrow.apache.org/rust/arrow/index.html) | Crate overview | High | Canonical interchange |
| Arrow schema metadata is separate from layout. | [`Schema`](https://arrow.apache.org/rust/arrow/datatypes/struct.Schema.html) | Schema API | High | Not a manifest system |
| Decimal128 is exact fixed-point with precision/scale. | [`DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html) | Decimal variant | High | Domain invariants remain external |
| DataFusion uses Decimal128 for precision up to 38. | [SQL types](https://datafusion.apache.org/user-guide/sql/data_types.html) | Type mapping | High | Validate result precision |
| Parquet writer exposes row-group/page/statistics controls. | [Writer properties](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html) | Writer API/defaults | High | Version settings in manifests |
| DataFusion memory is unlimited by default. | [Configuration](https://datafusion.apache.org/user-guide/configs.html) | Runtime defaults | High | Application must cap resources |
| DataFusion defaults to at least four output files. | [Configuration](https://datafusion.apache.org/user-guide/configs.html) | Output defaults | High | Can amplify small files |
| SQLite serializes writers; WAL provides snapshots. | [Isolation](https://www.sqlite.org/isolation.html) | Isolation model | High | Keep transactions short |
| WAL is same-host, checkpointed, and still may be busy. | [WAL](https://www.sqlite.org/wal.html) | WAL limitations | High | Not a remote database design |
| Foreign keys must be enabled per connection. | [Foreign keys](https://www.sqlite.org/foreignkeys.html) | Runtime rules | High | Verify pool initialization |
| STRICT typing has a limited type set. | [STRICT](https://www.sqlite.org/stricttables.html) | STRICT rules | High | No native financial domain types |
| Integrity and foreign-key checks are separate. | [PRAGMAs](https://www.sqlite.org/pragma.html) | Check definitions | High | Run both |
| **Inference:** all analytical/catalog I/O stays outside the live path. | All assigned families | Documented I/O/resource behavior | High | Bounded async fan-out only |

## Source-Specific Notes

- `docs-039`: **Confirmed** reviewed Rust pages identify Arrow/Parquet 59.1.0.
  **Inference:** safe Rust does not replace hostile-input limits, semantic validation,
  fuzzing, or financial round-trip tests.
- `docs-040`: **Confirmed** DataFusion is designed for embedding and extension.
  **Inference:** separate interactive and background contexts prevent one local query
  from monopolizing CPU, memory, or temporary disk.
- `docs-041`: **Confirmed** WAL companion files may contain committed state.
  **Inference:** active backups must use SQLite-supported backup/checkpoint behavior,
  not copy only the main file.

## Cross-Source Patterns

1. Physical typing does not establish currency, provenance, availability time, or
   point-in-time correctness.
2. Defaults are not an operating policy: DataFusion is broadly unbounded, SQLite
   foreign keys default off, and Parquet settings are generic.
3. SQLite should publish immutable dataset manifests; DataFusion should query only
   published versions; Arrow schemas should be checked against those manifests.
4. Local-first still requires cancellation, resource caps, compaction, checkpoints,
   and layered integrity checks.
5. A bounded asynchronous boundary is mandatory between deterministic live state and
   research/control-plane persistence.

## Limitations and Non-Findings

- No assigned source defines a universal Parquet file size, partition key, or
  compaction interval; these require benchmarks.
- Arrow/DataFusion do not supply Market Squawk's manifest, lineage, idempotency,
  authorization, or point-in-time policy.
- DataFusion documentation does not establish arbitrary SQL as a security sandbox.
- The final crate versions, DataFusion limits, and resolved SQLite runtime were not
  selected or measured in this batch.
- SQLite WAL is not network-filesystem storage and does not remove writer contention.
- STRICT tables do not create native UUID, decimal, currency, or timestamp types.
- No evidence supports any SQLite, DataFusion, Parquet, or research I/O exception in
  the live event-to-action path.

## Source List

Official sources, accessed **2026-07-15**:

- `docs-039`: [Arrow](https://arrow.apache.org/rust/arrow/index.html),
  [`DataType`](https://arrow.apache.org/rust/arrow/datatypes/enum.DataType.html),
  [`Schema`](https://arrow.apache.org/rust/arrow/datatypes/struct.Schema.html),
  [Parquet](https://arrow.apache.org/rust/parquet/index.html),
  [writer properties](https://arrow.apache.org/rust/parquet/file/properties/struct.WriterPropertiesBuilder.html).
- `docs-040`: [DataFusion](https://datafusion.apache.org/),
  [configuration](https://datafusion.apache.org/user-guide/configs.html),
  [SQL types](https://datafusion.apache.org/user-guide/sql/data_types.html),
  [SQL API](https://datafusion.apache.org/library-user-guide/using-the-sql-api.html).
- `docs-041`: [SQLite docs](https://www.sqlite.org/docs.html),
  [WAL](https://www.sqlite.org/wal.html),
  [isolation](https://www.sqlite.org/isolation.html),
  [foreign keys](https://www.sqlite.org/foreignkeys.html),
  [STRICT](https://www.sqlite.org/stricttables.html),
  [PRAGMAs](https://www.sqlite.org/pragma.html).
