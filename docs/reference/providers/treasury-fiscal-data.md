# Treasury Fiscal Data

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | One Fiscal Data dataset is implemented; broader auction, debt, and fiscal coverage remains a target |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |

## Role and product workflows

Treasury Fiscal Data supplies official auction, debt, interest-rate, and fiscal observations for
macro/rate research, valuation context, regime features, screening, forecasts, and point-in-time
backtests. Each admitted dataset keeps its own field dictionary, natural key, cadence, and revision
contract; the common API envelope is not a universal schema.

The currently implemented vertical is Average Interest Rates on U.S. Treasury Securities. It does
not prove that all Fiscal Data datasets are integrated.

## Authentication and setup

- **VERIFIED PROVIDER FACT:** the API is GET-only and requires no registration, token, or API key.
- **APPLICATION POLICY:** `TREASURY_FISCAL_DATA_ENABLED=true` requests admission of the existing
  no-credential profile. It does not bypass provider doctor, publication, PIT selection, or product
  composition.
- **UNVERIFIED ENTITLEMENT/ASSUMPTION:** the official response table describes HTTP **403** as an
  invalid-key error even though the same documentation says no key is required. Treat such a
  response as a typed unexplained provider refusal; never invent a credential field.

## Exact endpoints and data families

| Surface | Exact contract |
| --- | --- |
| API family | `GET https://api.fiscaldata.treasury.gov/services/api/fiscal_service/{version}/{domain}/{dataset}` |
| Implemented dataset | `GET https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates` |
| Projection | `fields=...` |
| Filters | `filter={field}:{lt|lte|gt|gte|eq|in}:{value}` |
| Ordering | `sort={field},-{field},...` |
| Pagination | one-based `page[number]` and `page[size]` |
| Formats | JSON by default; CSV and XML are also documented |

- **VERIFIED PROVIDER FACT:** JSON defaults to page **1** and **100 rows**. Response `meta` includes
  counts, types/formats, and total pages; `links` includes self/first/previous/next/last.
- **VERIFIED PROVIDER FACT:** all wire values, including the provider's null representation, arrive
  as strings and must be interpreted through dataset metadata.
- **VERIFIED PROVIDER FACT:** omitting fields can group rows that are no longer unique and sum
  numeric values. Projection therefore changes dataset semantics.
- **APPLICATION POLICY:** every dataset profile freezes its endpoint/version, complete uniqueness
  fields, filters, deterministic sort, format, page chain, field dictionary, and raw-page identity.
  Never reuse an average-rate key or mapper for a different dataset by analogy.

The implemented Average Interest Rates profile requests:

```text
record_date
security_type_desc
security_desc
avg_interest_rate_amt
src_line_nbr
record_fiscal_year
record_fiscal_quarter
record_calendar_year
record_calendar_quarter
record_calendar_month
record_calendar_day
```

Its canonical natural key is
`record_date + security_type_desc + security_desc + src_line_nbr`, with deterministic sort
`record_date,src_line_nbr`.

## Provenance, clocks, and revisions

Retain dataset/version, projected fields, dictionary identity, query/filter/sort, page number/size,
response links/meta, raw digest, provider record key, observation/effective date, provider-visible
publication when available, local received/ingested/availability times, and durable revision.

- **VERIFIED PROVIDER FACT:** the common API documentation does not establish one natural key,
  cadence, publication clock, or revision policy for every dataset.
- **APPLICATION POLICY:** publication and historical availability may not be inferred from
  `record_date`. Changed reacquisitions append locally observed revisions and preserve prior raw
  pages.
- **APPLICATION POLICY:** provider string null, absent field, zero, and unparsable value remain
  distinct states.

## Capacity and adaptive admission

| Claim | Evidence class and treatment |
| --- | --- |
| Default page | **VERIFIED PROVIDER FACT:** page **1**, **100 rows** |
| Provider maximum | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no maximum page size, request quota/window, concurrency limit, or stable quota-header contract was found |
| Target safety budget | **APPLICATION POLICY:** at most **1 request/second** through the configured Treasury authority; actual pressure may only lower it |
| Current repository budget | **APPLICATION POLICY:** the audited built-in profiles currently share `us-treasury` at **100 requests/minute** with concurrency **2** |

The current repository budget and the target safety budget are not the same contract. Reconcile the
shared authority before acceptance; neither value is a provider limit. Admission also accounts for
response bytes, pages, returned rows, latency, HTTP 429/`Retry-After`, server errors, queue lag, and
publication pressure. Dataset refreshes and interactive reads outrank broad backfills.

## Runtime evidence

- **RUNTIME-MEASURED VALUE:** on **2026-08-11**, a one-row Treasury auction query returned HTTP
  **200** and one row.
- **UNVERIFIED ENTITLEMENT/ASSUMPTION:** that probe proves route reachability and shape only; it is
  not a throughput, completeness, schema-stability, or revision test.

## Canonical storage and point-in-time selection

```text
bounded Fiscal Data JSON page + meta/links
  -> dataset-specific closed parser and natural-key validation
  -> dataset-appropriate market_squawk.research_observations variant
  -> immutable Parquet generation + manifest
  -> exact-cutoff PIT selector
  -> typed fiscal/rate/research read
```

Average rates and scalar fiscal/debt series map to `MacroObservation`. Structured auction records
must use a reviewed first-class auction observation or another closed dataset-specific canonical
variant; they must not be flattened into unrelated scalar fields or an untyped map. That structured
variant is not present at the audit basis.

Exact decimals and provider-native missing evidence are required. SQLite owns profile state, the
shared request budget, jobs, one-based page checkpoints, health, manifests, and recovery. Readers
discover completed generations through manifests, never by scanning raw files or accepting a
partial page chain.

## Scheduling and degradation

- **APPLICATION POLICY:** schedule each admitted dataset by its own published cadence and natural
  incremental coordinate. Backfill is resumable and lower priority than current/release work.
- **APPLICATION POLICY:** follow provider `links` and total-page metadata under page/row/byte and
  duplicate guards. Missing uniqueness fields, semantic aggregation, repeated pages, inconsistent
  metadata, or a changed dictionary fail the generation closed.
- **APPLICATION POLICY:** HTTP refusal or outage affects only this source. The product exposes
  `Degraded` or `Unavailable` rather than substituting a stale or semantically different series.

## Repository integration status and seams

- [`market-squawk-adapter-treasury`](../../../adapters/market-squawk-adapter-treasury/src/lib.rs)
  implements strict Fiscal Data HTTP, parsing, pagination evidence, normalization, and raw lineage
  for `AverageInterestRatesV2` only.
- [`query.rs`](../../../adapters/market-squawk-adapter-treasury/src/query.rs) freezes the exact
  fields, natural key, filters, sort, and bounded page request. Its **10,000-row** parser ceiling is
  **APPLICATION POLICY**, not an upstream maximum.
- [`built_in_profiles.rs`](../../../crates/market-squawk-sources/src/onboarding/built_in_profiles.rs)
  exposes `treasury.fiscal-data` with a bounded no-key probe and the current shared budget.
- Broad auction, debt, and other Fiscal Data dataset profiles, their dictionaries/mappers, and
  complete frontend research composition are not implemented at the audit basis.

Reuse the existing raw-receipt, Arrow/Parquet, SQLite checkpoint/manifest, provider-rate, canonical
`MacroObservation`, PIT, and typed-operation boundaries. Do not add a parallel provider framework.

## Doctor and end-to-end acceptance

The doctor must prove:

1. exact dataset/version and no-credential route;
2. one bounded page with explicit complete uniqueness projection and deterministic sort;
3. response `meta`, `links`, requested/returned rows, bytes, latency, and HTTP status;
4. dictionary-driven string/null/date/decimal conversion;
5. one-based page and terminal completeness behavior; and
6. shared budget, cooldown, and typed error state without requesting a nonexistent key.

A dataset becomes available only after its complete page chain is retained, normalized, published
atomically, selected at an exact cutoff, served through a bounded typed read, consumed by its
macro/rate workflow, and recovered after restart. Each new Fiscal Data dataset repeats this gate;
success of Average Interest Rates cannot grandfather it.

## Hard gaps

- Numeric request/concurrency limit, stable quota headers, and maximum page size are unpublished.
- The common API does not supply a dataset-wide natural key, cadence, release clock, revision
  policy, or snapshot-isolation guarantee.
- Only Average Interest Rates V2 currently has a closed repository profile and mapper.
- Auction, debt, fiscal, and other selected families still require exact dictionaries, natural
  keys, dataset-appropriate canonical variants, incremental rules, publication, typed reads, and
  workflow composition.

## First-party sources

- U.S. Treasury Fiscal Data,
  [API Documentation](https://fiscaldata.treasury.gov/api-documentation/), accessed 2026-08-11.
- U.S. Treasury Fiscal Data,
  [Average Interest Rates on U.S. Treasury Securities](https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/),
  used by the implemented profile.

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
