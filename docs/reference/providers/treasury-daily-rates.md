# Treasury daily interest rates

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | All five documented families have repository adapter/profile support; full product composition remains pending |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |

## Role and product workflows

This source supplies official nominal par yields, Treasury bill rates, long-term rates, real par
yields, and real long-term rates. The canonical observations support yield-curve levels and slopes,
nominal/real comparisons, rate-regime features, valuation context, forecasts, screens, and
point-in-time backtests.

The five families remain separate source datasets. A derived spread or curve feature records its
input generations and is never written back into the source observations.

## Authentication and setup

- **VERIFIED PROVIDER FACT:** the selected XML feed is public GET and requires no account, key, or
  token.
- **APPLICATION POLICY:** `TREASURY_DAILY_RATES_ENABLED=true` admits the existing no-credential
  profile. It does not by itself prove freshness, complete retrieval, durable publication, or
  frontend availability.

## Exact endpoint and data families

The endpoint is:

```text
GET https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml
```

| Family | Exact `data` value | Historical start |
| --- | --- | --- |
| Nominal par yield curve | `daily_treasury_yield_curve` | **VERIFIED PROVIDER FACT:** **1990** |
| Treasury bills | `daily_treasury_bill_rates` | **VERIFIED PROVIDER FACT:** **2002** |
| Long-term rates | `daily_treasury_long_term_rate` | **VERIFIED PROVIDER FACT:** **2000** |
| Real par yield curve | `daily_treasury_real_yield_curve` | **VERIFIED PROVIDER FACT:** **2003** |
| Real long-term rates | `daily_treasury_real_long_term` | **VERIFIED PROVIDER FACT:** **2000** |

| Retrieval mode | Exact query contract |
| --- | --- |
| Calendar year | `data={family}&field_tdr_date_value=YYYY` |
| Calendar month | `data={family}&field_tdr_date_value_month=YYYYMM` |
| All history | `data={family}&field_tdr_date_value=all&page={zero_based_page}` |

- **VERIFIED PROVIDER FACT:** only all-history retrieval is paginated. It begins at page **0**,
  defaults to **300 rows/page**, and completes only after the next page contains no `<entry>` data.
- **VERIFIED PROVIDER FACT:** the **2025** additive change introduced `BC_1_5MONTH` for the nominal
  curve and six six-week bill fields: `ROUND_B1_CLOSE_6WK_2`, `ROUND_B1_YIELD_6WK_2`,
  `MATURITY_DATE_6WK`, `CUSIP_6WK`, `CS_6WK_CLOSE_AVG`, and `CS_6WK_YIELD_AVG`.
- **VERIFIED PROVIDER FACT:** unavailable values can be represented by an absent element rather
  than the older `m:null="true"` form.
- **APPLICATION POLICY:** parse a closed known vocabulary per family while detecting additive
  fields. An unknown field blocks canonical publication until its meaning is reviewed; an absent
  field remains missing and is never converted to zero.

## Provenance, clocks, and revisions

Retain family, exact query/period/page, Atom feed identity/title, provider record identity, schema
revision, observation date, provider publication/update time when present, local received and
ingested times, raw digest, and durable revision.

- **VERIFIED PROVIDER FACT:** the reviewed source does not publish an exact daily availability
  time, immutable revision identifier, correction ledger, checksum/ETag contract, or historical
  vintage archive.
- **APPLICATION POLICY:** observation date is not availability. Reacquired changes append a new
  locally observed revision and retain the earlier response; they never rewrite the information set
  used by a prior model run.
- **APPLICATION POLICY:** provider publication ordering is retained when present. Otherwise only
  local availability is known, and the row cannot be backdated into an earlier PIT cutoff.

## Capacity and adaptive admission

| Claim | Evidence class and treatment |
| --- | --- |
| All-history page | **VERIFIED PROVIDER FACT:** page **0** origin, **300-row** default, empty-`<entry>` termination |
| Provider maximum | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no numeric request quota, concurrency ceiling, maximum pages, or stable rate/retry-header contract was found |
| Target safety budget | **APPLICATION POLICY:** at most **1 request/second** through the configured Treasury authority; actual pressure may only lower it |
| Current repository budget | **APPLICATION POLICY:** the audited built-in profiles currently share `us-treasury` at **100 requests/minute** with concurrency **2** |

The current and target application budgets must be reconciled before acceptance; neither is an
upstream limit. Admission also watches response bytes, entries, duplicate payloads/rows, page order,
latency, HTTP 429/`Retry-After`, queue lag, parser failures, and write pressure.

## Runtime evidence

- **RUNTIME-MEASURED VALUE:** on **2026-08-11**, the **2025** daily-rate XML request returned HTTP
  **200** with **249 entries**.
- **UNVERIFIED ENTITLEMENT/ASSUMPTION:** that bounded result proves current reachability and shape,
  not stable throughput, all-history completeness, correction behavior, or a provider ceiling.

## Canonical storage and point-in-time selection

```text
bounded Atom/XML page
  -> family-specific strict parser + cross-page completeness tracker
  -> market_squawk.research_observations::MacroObservation
  -> immutable Parquet generation + manifest
  -> exact-cutoff PIT rate selector
  -> typed macro/rate/model read
```

Series identity binds family and metric/maturity; value uses an exact decimal; unit and explicit
provider-native missing evidence are mandatory. SQLite owns the no-key profile, shared budget,
jobs, page checkpoints, health, manifests, and recovery. Derived slopes, spreads, real/nominal
comparisons, and regime features live in separately versioned derived generations.

## Scheduling and degradation

- **APPLICATION POLICY:** refresh current-year family data on a daily/release-driven cold lane and
  use year/month retrieval for bounded repair. Reserve all-history paging for explicit resumable
  backfill.
- **APPLICATION POLICY:** all-history publication requires the empty terminal page plus page, row,
  byte, duplicate, order, and date guards. A truncated or repeated page chain remains incomplete.
- **APPLICATION POLICY:** additive schema changes, missing publication evidence, provider refusal,
  parse failure, or incomplete pagination yield `Degraded` or `Unavailable`. No stale family or
  nominal proxy silently replaces a missing real-rate family.

## Repository integration status and seams

- [`market-squawk-adapter-treasury`](../../../adapters/market-squawk-adapter-treasury/src/lib.rs)
  has closed support for all five families, year/month/all-history queries, strict parsing,
  provider-native lineage, canonical macro normalization, and bounded pagination tracking.
- [`daily_rates/query.rs`](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/query.rs)
  freezes family keys, historical start years, feed identities, quality, and schema revisions.
- [`daily_rates/pagination.rs`](../../../adapters/market-squawk-adapter-treasury/src/daily_rates/pagination.rs)
  enforces cross-page order, duplicate, continuity, and terminal behavior.
- [`built_in_profiles.rs`](../../../crates/market-squawk-sources/src/onboarding/built_in_profiles.rs)
  exposes `treasury.daily-rates-xml` with a bounded no-key probe and the current shared budget.
- The adapter/profile foundation does not by itself prove current immutable publication, a typed
  Desktop/CLI/MCP rate read, model composition, or restart-complete product behavior.

## Doctor and end-to-end acceptance

The doctor must prove:

1. exact no-key endpoint and all five family keys;
2. one bounded year response with feed identity, schema vocabulary, entries, bytes, latency, and
   status;
3. absent-versus-zero handling and exact decimal/date conversion;
4. publication/update and local availability clocks without invention;
5. page-origin and empty-terminal behavior using static/replay evidence; and
6. the reconciled shared Treasury budget and cooldown state.

Availability requires each intended family to retain bounded raw evidence, normalize, publish an
atomic complete generation, survive restart, answer an exact-cutoff typed read, and feed the
macro/rate workflow with explicit freshness and revision state.

## Hard gaps

- Numeric request/concurrency ceiling, stable quota headers, and maximum page count are unpublished.
- Stable ordering or snapshot isolation across live all-history pages is not promised.
- Exact daily publication time, immutable revision IDs, correction history, checksum/ETag, and a
  complete public family XSD are absent from the reviewed contract.
- Historical-as-known selection begins with retained local reacquisitions; the source does not
  provide a complete vintage archive.
- Final typed application reads and workflow/restart composition remain to be proven.

## First-party sources

- U.S. Department of the Treasury,
  [Treasury Daily Interest Rate XML Feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
  accessed 2026-08-11.
- U.S. Department of the Treasury,
  [Developer Notice — XML Changes](https://home.treasury.gov/developer-notice-xml-changes), accessed
  2026-08-11.

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
