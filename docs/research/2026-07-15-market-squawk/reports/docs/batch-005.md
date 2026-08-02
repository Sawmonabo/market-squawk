# Docs Batch 005 Deep Dive

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

This report reviews only `docs-048` (U.S. Treasury Fiscal Data REST and daily
interest-rate feeds) and `docs-049` (`tract-onnx` with ONNX Runtime support-status
context). It focuses on schema/change/provenance, yield-curve semantics, trusted local
inference, and implementation tests. Sources were accessed on **2026-07-15**.
**Confirmed** statements are directly documented; **Inference** statements apply that
evidence to Market Squawk.

## Sources Reviewed

| ID | Official family | Pages reviewed | Main use |
|---|---|---|---|
| `docs-048` | U.S. Treasury | [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/), [daily-rate XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed), [daily Treasury rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve), [developer changes](https://home.treasury.gov/developer-notice-xml-changes) | REST/XML/CSV schema, pagination, changes, yield meaning |
| `docs-049` | tract / ONNX context | [`tract-onnx` 0.23.4](https://docs.rs/tract-onnx/0.23.4/tract_onnx/), [`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html), [`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html), [tract core](https://docs.rs/tract-core/latest/tract_core/), [ONNX Runtime community projects](https://onnxruntime.ai/docs/get-started/community-projects.html) | Local model parsing/planning and support boundary |

## Findings

### 1. Treasury REST and XML pagination are different contracts

**Confirmed.** Fiscal Data REST supports field selection, filtering, sorting, and
JSON/XML/CSV output; JSON is default. `page[number]` is one-based and defaults to 1,
while `page[size]` defaults to 100. JSON metadata includes response count, field
labels, data types/formats, total count, and total pages; links identify first,
previous, next, and last pages. Selecting fewer fields can trigger aggregation of
non-unique rows and summation of numeric values.
([Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/))

**Inference.** Pin fields, filters, sort, format, page size, and endpoint version in
every manifest. Follow total pages/links and validate total count before publication.
Do not project away natural-key dimensions during canonical ingestion because the API
may aggregate the result.

**Confirmed.** The daily-rate XML feed accepts GET requests by year, month, or `all`.
Only `all` uses pagination: it is zero-based, defaults to 300 rows on page 0, and ends
when a page contains no `<entry>`. The feed uses OData EDM types. Historical CSV
archives cover fixed year ranges. ([Treasury XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
[Treasury developer changes](https://home.treasury.gov/developer-notice-xml-changes))

**Inference.** Implement separate paginator types; sharing the REST one-based cursor
with the XML zero-based feed would skip or duplicate data. Use CSV archives for
bootstrap/reconciliation and year/month XML for bounded incremental refresh. Deduplicate
by dataset, rate date, maturity/field, and semantic value while retaining source URL,
raw payload hash, fetch time, schema version, and run ID.

### 2. Treasury schemas and yield semantics change over time

**Confirmed.** Treasury added a 1.5-month CMT/XML field in February 2025 and a
4-month field in October 2022. A 2022 migration changed feed URLs and pagination; the
new XML omits a tag when no value exists rather than always emitting an explicit null.
([Treasury developer changes](https://home.treasury.gov/developer-notice-xml-changes))

**Inference.** Parse by field name, tolerate additive fields, and distinguish absent,
null, zero, and parse failure. Schema fixtures must span both sides of every documented
series break. Unknown maturities should be quarantined until registry mapping is added,
not silently dropped.

**Confirmed.** Constant Maturity Treasury rates are interpolated from the daily par
yield curve. Inputs are indicative bid-side—not transaction—prices for recently
auctioned Treasury securities, obtained by the Federal Reserve Bank of New York near
3:30 p.m. each trading day. Fixed-maturity values can exist even without an outstanding
security at exactly that maturity. Treasury has used a monotone-convex spline since
December 6, 2021; earlier official rates used the prior methodology.
([Daily Treasury rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve))

**Inference.** These are official derived research inputs, not executable quotes,
actual trades, or quoted prices for an identical security. Store curve family,
maturity, observation date, units, source, fetch/availability evidence, and methodology
era. Never promote a CMT to `DirectVerified` or ASC 820 Level 1 evidence merely because
Treasury publishes it. Preserve revised payloads rather than overwriting by date; the
reviewed pages do not guarantee immutability or an exact publication timestamp.

### 3. `tract-onnx` is a candidate backend, not a compatibility guarantee

**Confirmed.** Current Rustdoc identifies `tract-onnx` 0.23.4, published July 8,
2026, under MIT or Apache-2.0. Its `Onnx` API parses protobuf models from paths/readers
and builds an `InferenceModel`; that model may have partially determined types and
shapes. A `TypedModel` has completely determined types and shapes, and tract core
builds an execution plan and runs tensors. Rustdoc reports about 48.68% documentation
coverage. ([`tract-onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/),
[`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html),
[`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html),
[tract core](https://docs.rs/tract-core/latest/tract_core/))

**Confirmed.** ONNX Runtime's documentation lists Rust under external community
projects rather than its first-party language APIs.
([ONNX Runtime community projects](https://onnxruntime.ai/docs/get-started/community-projects.html))

**Inference.** Do not describe tract as an official ONNX Runtime Rust binding or claim
universal ONNX operator/opset support. Each model bundle must pin model format/opset,
`tract-onnx` version/features, input/output names, dtypes and shapes, normalization,
operator inventory, artifact SHA-256, and expected numerical tolerances. Import,
typing, optimization, and golden-vector comparison must all succeed before activation.

### 4. Model loading, warm-up, threading, and fallback must fail closed

**Inference.** Load only from the controlled model-bundle directory. Check file type,
maximum size, manifest, and hash before parsing; reject external references, unexpected
files, schema mismatch, unsupported operations, non-finite outputs, and output-count/
shape errors. Remote model loading and runtime code/plugins remain prohibited.

**Inference.** Parsing, typing, optimization, allocation, and warm-up occur outside the
live path. Run representative warm-up inputs, record latency/memory, then atomically
publish an immutable ready backend. The reviewed Rustdoc does not specify plan-level
threading, deterministic concurrency, or resource bounds; choose serialized or
per-worker state only after stress/race and benchmark evidence, and record thread count
with results.

**Inference.** Any load, validation, warm-up, or inference error yields no automated
action. An alternate backend is a fallback only if the same bundle was independently
validated against it and policy explicitly names it; otherwise use the model bundle's
non-action behavior. Python and filesystem access remain outside live inference.

### 5. Required implementation tests

**Inference.** Deterministic coverage should include:

- **Treasury:** REST one-based and XML zero-based pagination; empty terminal XML page;
  metadata/row-count reconciliation; projection-triggered aggregation guard; XML
  absent versus null tags; pre/post-2021 methodology and 2022/2025 schema fixtures;
  CSV/XML overlap deduplication; additive maturity detection; malformed EDM values;
  payload revision preservation; curve points never execution-qualify.
- **tract:** correct/wrong artifact hash; oversized/truncated protobuf; unsupported
  operator/opset; symbolic versus fixed shapes; dtype/name/order mismatch; golden
  vectors and tolerance boundaries; NaN/Inf/wrong-shape outputs; cold versus warmed
  latency; repeated and concurrent runs; version-upgrade equivalence; failed hot-swap
  retains prior approved backend; every error returns no action.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| Fiscal Data REST pages are one-based and default to 100 rows. | [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/) | pagination contract | High | XML differs |
| REST metadata exposes types, formats, counts, and pages. | [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/) | meta object | High | Validate before publish |
| XML `all` pagination is zero-based, 300 rows, ending on empty entry. | [Treasury XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | feed pagination | High | Separate paginator |
| Treasury has made additive XML/CSV schema changes. | [Developer changes](https://home.treasury.gov/developer-notice-xml-changes) | 2022/2025 notices | High | Forward-compatible parser |
| CMTs are interpolated from indicative bid-side prices. | [Daily rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve) | methodology description | High | Not execution data |
| `tract-onnx` parses ONNX into inference models. | [`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html) | model methods | High | Validate per bundle |
| Typed models have determined types and shapes. | [`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html) | type description | High | Still needs golden tests |
| Rust is an external ONNX Runtime community project. | [ONNX Runtime](https://onnxruntime.ai/docs/get-started/community-projects.html) | support classification | High | Avoid first-party claim |
| **Inference:** every inference failure produces no action. | tract family | incomplete compatibility guarantees | High | Fail closed |

## Source-Specific Notes

- `docs-048`: **Inference.** Preserve the provider's maturity availability gaps and
  methodology eras; a rectangular curve table must not manufacture missing history.
- `docs-049`: **Inference.** Pin 0.23.4 rather than `latest`; upgrades require bundle-
  level numerical and performance requalification.

## Cross-Source Patterns

1. Version, schema, and method changes are part of the data/model meaning.
2. A successful parse is weaker than semantic validation and reproducibility.
3. Raw hashes plus immutable manifests support revisions, trust, and auditability.
4. Both Treasury ingestion and model preparation stay outside the live decision path.

## Limitations and Non-Findings

- The reviewed Treasury pages do not guarantee exact publication time or immutable
  historical payloads.
- CMTs are modeled par yields, not transactions, instrument prices, or full curves on
  non-trading days.
- The assigned tract Rustdoc promises no universal operator/opset coverage, numerical
  equivalence, latency, memory bound, warm-up behavior, or concurrency model.
- Rustdoc coverage is incomplete; model acceptance must be empirical and bundle-
  specific.
- No model was executed and no performance claim is made.
- No source outside the two assigned families was reviewed.

## Source List

Official sources, accessed **2026-07-15**:

- `docs-048`: [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/),
  [daily-rate XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
  [daily Treasury rates](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?field_tdr_date_value_month=0&type=daily_treasury_yield_curve),
  [developer changes](https://home.treasury.gov/developer-notice-xml-changes).
- `docs-049`: [`tract-onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/),
  [`Onnx`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/model/struct.Onnx.html),
  [`TypedModel`](https://docs.rs/tract-onnx/0.23.4/tract_onnx/prelude/type.TypedModel.html),
  [tract core](https://docs.rs/tract-core/latest/tract_core/),
  [ONNX Runtime community projects](https://onnxruntime.ai/docs/get-started/community-projects.html).
