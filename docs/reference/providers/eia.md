# EIA API v2

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Selected macro/energy source; credential input exists, adapter and product composition do not yet ship |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |

This page defines the contract Market Squawk must implement. It does not turn a configured key or
a successful HTTP response into an available product capability.

## Role and product workflows

EIA supplies source-attributed energy observations for macro research, market context, forecasting,
valuation, screening, and point-in-time backtests. Initial families should include petroleum,
natural gas, electricity, inventories, production, consumption, and prices selected through EIA's
route metadata rather than a hard-coded universal schema.

The frontend consumes typed macro/commodity observations only. It never constructs an EIA URL,
handles the API key, or parses a provider response.

## Authentication and setup

- **VERIFIED PROVIDER FACT:** API v2 requires an individual API key carried as the `api_key` URL
  query parameter. The reviewed documentation does not define a header-authentication alternative.
- **APPLICATION POLICY:** the operator supplies only `EIA_ENABLED=true` and `EIA_API_KEY` through
  the existing credential-import design. The key and the complete secret-bearing query string must
  be redacted from logs, errors, traces, receipts, cache keys, and diagnostics.
- **APPLICATION POLICY:** a configured key means `Configured` or `Probe required`; it does not mean
  `Available` until the complete acceptance path below passes.

See [provider setup](../../operations/provider-account-setup.md) and the
[credential template](../market-squawk-provider-credentials.env.example).

## Exact endpoint and data contract

| Surface | Exact contract |
| --- | --- |
| API root and metadata | `GET https://api.eia.gov/v2/` and hierarchical `GET https://api.eia.gov/v2/{route}` |
| Route data | `GET https://api.eia.gov/v2/{route}/data` |
| Projection | `data[]` |
| Dimensions | `facets[{facet}][]`, `frequency`, `start`, and `end` |
| Ordering | `sort[0][column]` and `sort[0][direction]` |
| Pagination | `length`, `offset`, and response `total` |
| Output | Provider-supported `out`/format selection; JSON is the initial canonical ingestion path |

- **VERIFIED PROVIDER FACT:** responses echo the interpreted request and the serving API version.
  Those values are evidence, not disposable metadata.
- **VERIFIED PROVIDER FACT:** JSON pages are limited to **5,000 rows** and XML pages to **300
  rows**. Offset pagination and response `total` define completion.
- **VERIFIED PROVIDER FACT:** API v2 changes through **v2.1.12 (March 2026)** include string-valued
  JSON data, default-order changes, improved HTTP/body error consistency, inclusive equal-year
  bounds, and a corrected `total` calculation.
- **APPLICATION POLICY:** every acquisition identity binds route, selected columns, facets,
  frequency, bounds, explicit sort, format, observed API version, and the secret-free request echo.
  A generation is incomplete until its bounded offset chain reaches `total` without duplicate or
  missing page coverage.

## Provenance, clocks, and revisions

Retain provider, exact route, series/dimension coordinates, unit, frequency, observation period,
API version, requested and returned row counts, `total`, offset, raw digest, received time, ingested
time, and local source-visible availability time.

- **VERIFIED PROVIDER FACT:** the reviewed contract does not provide a universal vintage or
  immutable revision identifier.
- **APPLICATION POLICY:** observation date is not historical availability. Reacquired changed
  values append a new locally observed revision and preserve the earlier raw receipt.
- **APPLICATION POLICY:** provider-native missing values remain missing evidence; string values are
  parsed through the route's metadata and unit contract, never coerced by appearance and never
  replaced with zero.

## Capacity and adaptive admission

| Claim | Evidence class and treatment |
| --- | --- |
| Page size | **VERIFIED PROVIDER FACT:** at most **5,000 JSON rows** or **300 XML rows** per page |
| Request rate | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no numeric request rate, cooldown duration, stable quota header, or concurrency ceiling was published in the reviewed first-party contract |
| Provider response to overuse | **VERIFIED PROVIDER FACT:** EIA warns that overuse can temporarily suspend a key |
| Safety budget | **APPLICATION POLICY:** one shared EIA queue admits at most **1 request/second** and may only lower that rate without reviewed evidence |

Admission uses actual response bytes, returned rows, pages, latency, HTTP status, retry evidence,
queue lag, parser failures, and publication pressure. Interactive reads and current scheduled
releases outrank broad backfill. A rate refusal opens one provider-wide cooldown; jobs do not start
independent retry storms.

## Runtime evidence

- **RUNTIME-MEASURED VALUE:** on **2026-08-11**, a configured retail-sales query returned HTTP
  **200**.
- **UNVERIFIED ENTITLEMENT/ASSUMPTION:** that single response proves key reachability and a useful
  shape only. It does not establish sustainable throughput, route-wide entitlement, schema
  stability, completeness, or a provider limit.

## Canonical storage and point-in-time selection

The target path is:

```text
bounded secret-free raw page
  -> route-specific typed validation
  -> market_squawk.research_observations::MacroObservation
  -> immutable Parquet generation + manifest
  -> exact-cutoff PIT selector
  -> typed macro/research/model read
```

Canonical identity includes route, series/dimensions, frequency, unit, observation period, release
or provider-visible availability when supplied, local availability, revision, raw digest, and
schema/API version. SQLite owns provider state, quota permits, jobs, page checkpoints, health,
manifests, and restart recovery. Arrow is the validation boundary; Parquet generations are
immutable.

## Scheduling and degradation

- **APPLICATION POLICY:** discover and freeze each admitted route through metadata before data
  collection. Schedule by the series' actual frequency or release pattern; never poll monthly or
  weekly data as if it were live market data.
- **APPLICATION POLICY:** backfills use explicit stable sort, bounded pages, durable offsets, and
  resumable checkpoints. Current/release work preempts backfill.
- **APPLICATION POLICY:** HTTP refusals, changed metadata, inconsistent totals, duplicate pages,
  unparseable values, or publication failure yield `Degraded` or `Unavailable`; stale values do not
  silently stand in for current evidence.

## Repository integration status and seams

- The credential template declares `EIA_ENABLED` and `EIA_API_KEY`; its parser/import path remains
  design-only.
- No EIA adapter, provider profile, doctor, publisher, typed operation, or frontend composition is
  present at the audit basis.
- Implementation must reuse the existing provider-profile registry, durable provider-rate
  authority, bounded extraction/raw-receipt path, canonical `MacroObservation`, immutable
  Arrow/Parquet publication, SQLite checkpoints/manifests, and PIT readers. It must not introduce a
  second credential or scheduler system.

Related maintained contracts are the [provider architecture](../../architecture/market-data-provider-architecture.md),
[research data plane](../../architecture/research-data-plane.md), and
[shipping source coverage](../source-coverage.md).

## Doctor and end-to-end acceptance

The provider doctor must prove, without exposing the key:

1. credential generation and URL redaction;
2. one metadata route and one bounded data page;
3. API version and interpreted-request echo;
4. requested, returned, missing, `total`, offset, bytes, latency, and HTTP status;
5. route-specific columns, units, frequency, string-value parsing, and missing-value handling; and
6. typed health and refusal/cooldown state.

Availability requires the same exact contract to complete a bounded page chain, retain raw
evidence, normalize, publish atomically, survive restart, answer an exact-cutoff typed read, and
appear in the intended macro/model workflow with freshness and degradation explanations.

## Hard gaps

- Numeric request/concurrency limit, cooldown duration, and stable quota headers are unpublished.
- No universal cross-route schema, natural key, release clock, revision identifier, or vintage
  history is established.
- A successful current response cannot reconstruct what the provider exposed at an earlier model
  cutoff.
- Route discovery and the product-specific energy feature catalog remain implementation work.

## First-party sources

- U.S. Energy Information Administration,
  [API Technical Documentation](https://www.eia.gov/opendata/documentation.php), accessed
  2026-08-11.
- U.S. Energy Information Administration,
  [API Technical Documentation v2.1.0 (PDF)](https://www.eia.gov/opendata/documentation/APIv2.1.0.pdf),
  November 2022, accessed 2026-08-11.

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
