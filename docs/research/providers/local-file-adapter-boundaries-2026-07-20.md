# Local File Adapter Production Boundaries

Status: implementation decision record

As of: 2026-07-20

Scope: CSV/TSV, JSON/NDJSON, XML, XLSX, Parquet, SQLite exports, OFX, and QFX

## Table of Contents

- [Outcome](#outcome)
- [Research method](#research-method)
- [Boundary decisions](#boundary-decisions)
- [Format decisions](#format-decisions)
- [Verified producer-to-consumer contract](#verified-producer-to-consumer-contract)
- [Coverage limits and release implications](#coverage-limits-and-release-implications)
- [Research and ecosystem findings](#research-and-ecosystem-findings)
- [Source matrix](#source-matrix)

## Outcome

Market Squawk treats user-owned financial files as hostile, immutable inputs rather than ambient
filesystem paths. A source is constructed from a user-authorized root and an exact manifest digest.
Discovery obtains a fresh no-follow file capability and binds byte length plus SHA-256 evidence into
the discovered object. Extraction obtains another fresh capability, revalidates the complete static
lineage and exact bytes, parses under one cumulative resource budget, and emits canonical records
that retain the raw object evidence and canonical row evidence.

This boundary is intentionally stricter than a desktop "open whatever the extension names" import.
The format parser is not allowed to add network access, follow links, load external content, accept
caller SQL, infer accounting scale, or hide partial recovery. Unsupported or ambiguous constructs
fail closed with typed errors.

## Research method

The research was date-anchored to 2026-07-20 and prioritized specifications, upstream library API
documentation, standards-owner material, and upstream repositories. Academic searches were used as
a cross-check on parser assurance rather than as substitutes for format specifications. The most
relevant general parser study found that acceptance by one parser is not proof that a document is
format-compliant; that supports explicit structural validation around dependency parsers rather
than delegating the entire trust decision to a library. See [Looking for non-compliant documents
using error messages from multiple parsers](https://arxiv.org/abs/2012.10211).

No academic work located in the focused search supplied an OFX-specific, Rust-specific production
boundary stronger than the official OFX specification and upstream library contracts. Papers about
REST export streaming or financial-PDF extraction were excluded because they do not establish
correctness or safety properties for local OFX/QFX, SQLite, XLSX, or Parquet ingestion.

## Boundary decisions

1. **Authority precedes parsing.** A caller cannot pass an arbitrary path to discovery or
   extraction. Path traversal, absolute paths, symlinks/reparse points, non-regular files, identity
   changes, file growth, and concurrent modification are rejected by the platform capability.
2. **Evidence is checked twice.** Manifest bytes are bound to the source metadata revision. Object
   bytes are hashed at discovery and re-read/rehashed at extraction. Complete source, revision,
   dataset, request, object, media-type, effective-interval, publication-time, length, and digest
   lineage is checked before parsing.
3. **One cumulative budget covers the parse.** Source bytes, decoded/decompressed bytes, records,
   fields, columns, cells, depth, text, archive entries, row groups, sheets, elapsed time, and
   cancellation are independently bounded. Manifest bytes are rejected against the same source-byte
   ceiling before deserialization, so manifest-controlled path and object allocation cannot bypass
   the configured input bound. Provider-controlled parser scratch, metadata, decoded batches, and
   owned output copies are conservatively admitted before the dependency or adapter allocates; an
   observed allocation is still checked against the bound and provider declarations never replace
   actual validation.
4. **No partial success is hidden.** Duplicate keys, inconsistent row widths, malformed syntax,
   missing identity/mapped fields, invalid exact decimals, wrong scale, and unsupported value types
   fail the object. NDJSON does not silently skip a malformed line.
5. **Raw and canonical evidence remain distinct.** The raw file digest remains the object evidence.
   Each normalized record also retains a deterministic canonical-row reference and a digest of its
   canonical payload.

## Format decisions

### CSV and TSV

The upstream `csv` reader exposes an explicit delimiter and defaults unequal-width records to an
error when flexible records are disabled. Market Squawk makes both decisions explicit, requires a
unique nonempty UTF-8 header, validates every field as UTF-8, and charges rows, fields, columns, and
text to the request budget. The bounded raw slice also provides a conservative admission for the
header and reusable record buffers before the reader can grow them. See the official [`ReaderBuilder::delimiter`](https://docs.rs/csv/1.4.0/csv/struct.ReaderBuilder.html#method.delimiter)
and [`ReaderBuilder::flexible`](https://docs.rs/csv/1.4.0/csv/struct.ReaderBuilder.html#method.flexible)
documentation.

### JSON and NDJSON

JSON input uses a custom bounded Serde visitor so duplicate object keys, excessive nesting,
oversized strings/containers, unsupported non-scalar row fields, and aggregate decoded-memory
growth are observable and rejectable. The raw JSON or NDJSON line bounds and admits Serde's decoded
scratch before escaped strings or owned keys can allocate. NDJSON applies the same row contract independently to every
nonempty line and never implements best-effort line recovery. The relevant upstream contracts are
the [Serde data model](https://serde.rs/data-model.html) and
[`serde_json::Deserializer`](https://docs.rs/serde_json/latest/serde_json/struct.Deserializer.html).

### XML

`quick-xml` exposes separate end-name checking and empty-element expansion controls; the former is
enabled and the latter provides one uniform start/end state machine. Market Squawk additionally
rejects document types, entity/general references, processing instructions, comments, attributes,
and nested row fields, so XML cannot become a network or external-entity surface. See
[`quick_xml::reader::Config`](https://docs.rs/quick-xml/0.41.0/quick_xml/reader/struct.Config.html)
and the upstream [event model](https://docs.rs/quick-xml/0.41.0/quick_xml/events/enum.Event.html).

### XLSX

SpreadsheetML is a ZIP package with workbook, worksheet, relationship, and optional shared-string
parts; it is not one trusted XML file. Microsoft documents this package relationship structure in
[Structure of a SpreadsheetML document](https://learn.microsoft.com/en-us/office/open-xml/spreadsheet/structure-of-a-spreadsheetml-document).
The adapter therefore scans EOCD/ZIP64, central-directory, and local-header structure and admits a
conservative metadata bound before constructing the ZIP dependency. It validates every entry before
reading retained parts: entry count, declared and actual decompressed bytes, compression ratio,
overlapping data, encrypted entries, symlinks, traversal, case-colliding names, active/macro
content, content types, and internal relationship targets. Declared payload plus the one-byte
mismatch probe is admitted before reserve/decompression. Formula and cached-value handling is an
explicit manifest policy. External links and arbitrary relationship targets are rejected. The ZIP
dependency exposes the archive entry count and overlapping-file check used by this validation; see
[`ZipArchive`](https://docs.rs/zip/8.6.0/zip/read/struct.ZipArchive.html).

### Parquet

Parquet is admitted only after exact magic/footer validation and explicit bounds on footer metadata,
row groups, physical/logical columns, rows, and declared logical bytes. The validated footer and
configured column ceiling admit conservative metadata capacity before builder construction. A
closed schema/logical expansion bound admits each Arrow batch before iterator advancement; observed
batch memory and bounded scalar copies are then checked and charged cumulatively. Only exact integer, unsigned integer,
UTF-8, large UTF-8, null, and Decimal128 values enter row mapping; floating-point input is never
silently converted to financial values. The implementation uses the official Apache Arrow Rust
Parquet metadata and record-batch reader APIs. See the
[`parquet::file::metadata`](https://docs.rs/parquet/58.3.0/parquet/file/metadata/index.html)
module and the [official `apache/arrow-rs` repository](https://github.com/apache/arrow-rs).

### SQLite exports

SQLite warns that a WAL file is part of persistent database state and separating the main file from
its WAL can lose committed transactions or corrupt the copied state. Market Squawk consequently
accepts only a rollback-journal main-file image with matching header read/write versions and rejects
WAL-mode images instead of pretending one file is a complete snapshot. See SQLite's official
[WAL persistent-state guidance](https://www.sqlite.org/wal.html#the_wal_file).

The pinned [`rusqlite` 0.40.1 deserialize implementation](https://docs.rs/rusqlite/0.40.1/src/rusqlite/serialize.rs.html#99-126)
calls `sqlite3_malloc64` for exactly the supplied image size before `read_exact`; that exact owned
buffer is admitted before connection construction. SQLite's
[`sqlite3_deserialize` contract](https://www.sqlite.org/c3ref/deserialize.html) confirms that the
connection reopens the schema over the supplied in-memory buffer and retains it for the connection
lifetime.

The bundled SQLite connection is explicitly configured to `PRAGMA main.cache_size = -512` and the
negative value is read back immediately after deserialization and before schema/query reads.
SQLite documents that a negative value is an approximate KiB ceiling, that the built-in page cache
honors it, and that page-count rounding depends on page size; admission therefore includes 512 KiB
plus one validated source page, a fixed 256 KiB connection/query allowance, and the configured
maximum schema/SQL text size. See the official
[`cache_size` contract](https://www.sqlite.org/pragma.html#pragma_cache_size). The adapter does not
call `sqlite3_hard_heap_limit64` or its PRAGMA because SQLite documents that heap limit as applying
to all database connections in the process, which would create cross-source interference in a
concurrent local application. See
[`sqlite3_hard_heap_limit64`](https://www.sqlite.org/c3ref/hard_heap_limit64.html).

Defensive mode, query-only mode, untrusted schema, disabled writable schema,
disabled DQS/view/trigger/FTS-tokenizer features, SQLite runtime limits, a closed authorizer, and a
generated quoted `SELECT` restrict access to one manifest-allowlisted base table and columns. Views,
virtual tables, caller SQL, SQL functions, `REAL`, and `BLOB` values are rejected. Rows are sorted in
Rust by a required manifest order key for deterministic output. The upstream read-only/open-mode
surface is documented in [`rusqlite::OpenFlags`](https://docs.rs/rusqlite/0.40.1/rusqlite/struct.OpenFlags.html),
while the in-memory exact-byte snapshot prevents dependency-created side files or access to the
source path.

### OFX and QFX

Financial Data Exchange identifies OFX Banking 2.3 as the current banking specification, OFX 1.6 as
the last SGML specification, and OFX 2.x as XML. It also states that both 1.x SGML and 2.x XML remain
supported in the current marketplace. See the [FDX OFX Work Group](https://financialdataexchange.org/about-fdx/ofx-work-group/)
and [OFX Banking 2.3 specification](https://www.financialdataexchange.org/common/Uploaded%20files/OFX%20files/OFX%20Banking%20Specification%20v2.3.pdf).

The adapter implements separate preamble/tokenization paths for legacy OFX 1.x SGML and OFX 2.x
XML, then feeds one bounded statement collector. Legacy headers are closed and validated; declared
US-ASCII, UTF-8, and Windows-1252 decoding is explicit. XML document types, arbitrary processing
instructions, comments, attributes, and external content are rejected. Statement account and
currency must exactly match manifest policy. Transaction `FITID` values must be present and unique;
`TRNAMT`, `DTPOSTED`, ledger balance, and ledger as-of values are validated without floating point.
The original transaction fields and statement context contribute to the canonical row digest.

FDX also notes that an open format does not imply public or unrestricted connectivity to every
financial institution. This adapter reads user-authorized local exports only; it does not discover
proprietary endpoints or attempt access-control, identity, quota, or bot-defense evasion.

## Verified producer-to-consumer contract

The release-critical integration test constructs one positive fixture for every manifest format:
CSV, TSV, JSON, NDJSON, XML, XLSX, Parquet, SQLite, OFX, and QFX. For each object it performs exact
discovery and canonical extraction, admits payload-specific user-owned-file persistence rights,
reserves a stable idempotency key, publishes through the analytical Arrow/Parquet service, and
replays the same reservation without adding a logical row. It then drops and reopens the catalog
and object-store services, resolves the exact final immutable manifest, and executes a bounded
DataFusion query that asserts all ten exact identifiers and decimal values.

The same test changes the CSV source bytes, extracts the changed canonical batch, admits rights for
that new payload, and proves that reusing the original idempotency key fails with the typed catalog
`IdempotencyConflict` result.

## Coverage limits and release implications

- The local adapter accepts bank and credit-card transaction statement aggregates represented by
  the implemented OFX/QFX collector. Full investment-statement holdings, securities-list, loan,
  bill-pay, tax, image, profile, and request/response message sets are not implemented by this
  parser and must not be claimed from the broader OFX 2.3 message-set list.
- XLSX imports are flat worksheet-row extraction. Charts, drawings, pivot caches, external links,
  macros, embedded packages, and other active or unrelated package content are deliberately not
  interpreted.
- Parquet temporal, nested, binary, Boolean, and floating-point fields are not converted into
  canonical financial scalars. A manifest mapping that requires an unsupported field fails.
- SQLite input is an immutable, self-contained rollback-journal export. A live database, WAL sidecar
  set, arbitrary SQL query, database URI, or remote database is outside this local-file authority.
- These limits are truthful parser coverage, not authorization to omit the separate release-blocking
  portfolio, provider, point-in-time, analytics, modeling, execution, valuation, CLI, or MCP
  producer-to-consumer capabilities.

## Research and ecosystem findings

The mature upstream crates selected for mechanics are actively documented and scoped narrowly:
`csv` for RFC-style delimited records, `quick-xml` for pull XML events, `zip` for ZIP package
mechanics, Apache `parquet`/Arrow for columnar decoding, `rusqlite` for SQLite, and `encoding_rs` for
specified legacy character decoding. Market Squawk retains its own authority, cumulative budgets,
lineage, and financial conversion policy because none of those libraries can infer this product's
rights or accounting contracts.

An ecosystem comparison found [`ofx-rs`](https://docs.rs/ofx-rs/latest/ofx_rs/) with stated OFX 1.x
SGML and 2.x XML parsing support. It was not adopted for this boundary because its public package
contract does not replace manifest-bound account/currency policy, the shared Market Squawk resource
budget, raw-object evidence, canonical row lineage, or the closed fail-safe event policy required
here. This is a fit decision, not a claim that the crate is generally unsafe.

## Source matrix

| Area | Primary source | Decision supported |
| --- | --- | --- |
| OFX versions and market compatibility | [FDX OFX Work Group](https://financialdataexchange.org/about-fdx/ofx-work-group/) | Separate legacy SGML and current XML paths; local files do not imply endpoint access |
| OFX banking structures | [OFX Banking 2.3](https://www.financialdataexchange.org/common/Uploaded%20files/OFX%20files/OFX%20Banking%20Specification%20v2.3.pdf) | Header, account, currency, transaction, unique ID, amount, and ledger context |
| CSV behavior | [`csv::ReaderBuilder`](https://docs.rs/csv/1.4.0/csv/struct.ReaderBuilder.html) | Explicit delimiter and strict equal-width rows |
| XML reader controls | [`quick_xml::reader::Config`](https://docs.rs/quick-xml/0.41.0/quick_xml/reader/struct.Config.html) | End-name validation and explicit event policy |
| XLSX package structure | [Microsoft SpreadsheetML structure](https://learn.microsoft.com/en-us/office/open-xml/spreadsheet/structure-of-a-spreadsheetml-document) | Validate workbook, worksheet, and relationship graph inside ZIP |
| ZIP mechanics | [`zip::ZipArchive`](https://docs.rs/zip/8.6.0/zip/read/struct.ZipArchive.html) | Bound entries and reject overlapping/unsafe package members |
| Parquet/Arrow | [Apache Arrow Rust](https://github.com/apache/arrow-rs) and [`parquet` metadata](https://docs.rs/parquet/58.3.0/parquet/file/metadata/index.html) | Validate metadata and charge actual decoded Arrow batches |
| SQLite snapshot state | [SQLite WAL documentation](https://www.sqlite.org/wal.html) | Reject incomplete single-file WAL snapshots |
| SQLite deserialize ownership | [`sqlite3_deserialize`](https://www.sqlite.org/c3ref/deserialize.html) and pinned [`rusqlite` 0.40.1 source](https://docs.rs/rusqlite/0.40.1/src/rusqlite/serialize.rs.html#99-126) | Admit the exact owned image buffer before `deserialize_read_exact` |
| SQLite page cache | [`PRAGMA cache_size`](https://www.sqlite.org/pragma.html#pragma_cache_size) | Configure/read back a connection-local negative KiB ceiling and admit page rounding |
| SQLite heap limits | [`sqlite3_hard_heap_limit64`](https://www.sqlite.org/c3ref/hard_heap_limit64.html) | Do not mutate a process-global limit from a concurrent source adapter |
| SQLite connection controls | [`rusqlite::OpenFlags`](https://docs.rs/rusqlite/0.40.1/rusqlite/struct.OpenFlags.html) and [SQLite C API open flags](https://www.sqlite.org/c3ref/open.html) | Closed local connection/snapshot policy |
| General parser assurance | [Non-compliant document parser study](https://arxiv.org/abs/2012.10211) | Dependency acceptance alone is not a compliance proof |

All web sources were accessed on 2026-07-20.
