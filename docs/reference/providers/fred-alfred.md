# FRED and ALFRED provider contract

FRED/ALFRED is Market Squawk's selected revision-aware macro source. This page defines the target
contract and current evidence; it does not claim that the complete durable provider-to-Console
workflow is release-proven.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Selected macro provider; existing adapter foundation, incomplete v1/v2 and product composition |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Credential input | `FRED_ENABLED`, `FRED_API_KEY` |
| Canonical destination | Macro `market_squawk.research_observations` |

## Role and product workflows

**APPLICATION POLICY:** FRED/ALFRED supplies series metadata, observations, release-aware values,
and historical revision coordinates for macro research. It supports economic-regime features,
valuation context, forecasts, time-ordered backtests, Opportunities evidence, and the plain-language
investment brief. It is not a current market-price feed.

Typical selected families include labor, inflation, policy rates, yield spreads, credit, money,
production, consumption, and financial conditions. Series admission is explicit: a broad provider
catalog does not make every series relevant or point-in-time complete.

## Authentication and setup

**VERIFIED PROVIDER FACT:** v1 requests require a registered 32-character lowercase-alphanumeric
API key. Market Squawk sends it as the redacted `api_key` query parameter. See the
[API-key guide](https://fred.stlouisfed.org/docs/api/api_key.html).

**RUNTIME-MEASURED VALUE:** the configured key completed one v1 `UNRATE` observation request and
one v2 release-observations request using bearer authentication, both with HTTP 200 on 2026-08-11.

**APPLICATION POLICY:** the credential file holds only `FRED_API_KEY`. URLs, series contracts,
pagination limits, request budgets, and authentication mode per API version remain code-owned.
Secrets are redacted from URLs, errors, traces, receipts, and provider-health output.

## Exact surfaces and data families

| Evidence | Read surface | Admitted meaning |
| --- | --- | --- |
| **VERIFIED PROVIDER FACT** | `GET https://api.stlouisfed.org/fred/series` | Exact series metadata, including provider identity, units, frequency, seasonal adjustment, observation range, and last-updated text |
| **VERIFIED PROVIDER FACT** | `GET https://api.stlouisfed.org/fred/series/observations` | Values for one `series_id`, observation dates, real-time intervals, transforms, aggregation, output modes, and offset pagination |
| **VERIFIED PROVIDER FACT** | `GET https://api.stlouisfed.org/fred/series/vintagedates` | Dates on which the selected series received a new or revised value; dates without a series change are excluded |
| **VERIFIED PROVIDER FACT** | `GET https://api.stlouisfed.org/fred/v2/release/observations` | Release-oriented observations with bearer authentication, up to 500,000 rows/page, and cursor pagination through `has_more`/`next_cursor` |

**VERIFIED PROVIDER FACT:** observation `output_type` distinguishes current real-time-period data,
all observations by vintage, only new/revised observations by vintage, and initial-release data.
Server-side transforms and lower-frequency aggregation alter the returned value's meaning.

**APPLICATION POLICY:** raw source generations prefer provider-authored untransformed values.
Requested transformations or aggregation are separate, provenance-bound observations and never
silently replace the source value.

## Provenance, clocks, revisions, and missingness

One FRED row must retain:

- series ID, metadata-response identity, exact raw value, checked decimal or provider missing marker
  `.`, unit, frequency, seasonal adjustment, and raw-page digest;
- observation date, row and page `realtime_start`/`realtime_end`, selected vintage dates, request
  output type, observation bounds, v1 offset or v2 cursor/limit/completion state, local receipt,
  ingest, and publication time;
- provider revision identity, supersession interval, first local availability, schema identity, and
  exact parent generation.

**VERIFIED PROVIDER FACT:** `series/vintagedates` is a revision-event index, while arbitrary as-of
dates can be requested through `series/observations`. They are different coordinates and neither
is the observation period.

**APPLICATION POLICY:** a provider real-time date is not an exact public-release timestamp. Point-
in-time selection retains the provider interval and local first availability separately. Later
revisions append new lineage; they never overwrite the earlier information set.

## Official limits and adaptive admission

| Dimension | Contract |
| --- | --- |
| V1 observation page | **VERIFIED PROVIDER FACT:** `limit` is 1–100,000 and defaults to 100,000; `offset` is zero-based |
| V1 vintage-date page | **VERIFIED PROVIDER FACT:** `limit` is 1–10,000 and defaults to 10,000 |
| Requested vintage dates | **VERIFIED PROVIDER FACT:** output/series/format-specific caps range from 55 to 2,000; there is no universal vintage-request value |
| V1 throttling | **VERIFIED PROVIDER FACT:** HTTP 429 and a structured error body are documented. **UNVERIFIED ENTITLEMENT/ASSUMPTION:** the reviewed v1 pages publish no numeric requests-per-window ceiling, stable rate headers, or `Retry-After` contract |
| V2 observation page | **VERIFIED PROVIDER FACT:** `limit` is 1–500,000 and defaults to 500,000; continuation uses `has_more` and `next_cursor` |
| V2 throttling | **VERIFIED PROVIDER FACT:** clients may make up to 2 requests/second; excess traffic receives HTTP 429 and temporary blocking may follow repeated violations |
| Shared application budget | **APPLICATION POLICY:** one v1/v2 provider queue at 1 request/second until retained measurements justify a reviewed change |

The queue records attempts, status, bytes, latency, page closure, returned observations, structured
provider errors, retries, and cooldown. HTTP 429 or lower measured service capacity overrides the
application target.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** the 2026-08-11 bounded probe returned HTTP 200 with one `UNRATE` v1
observation and HTTP 200 from the configured v2 release-observations surface. This proves the tested
credential and response shapes only; it is not a throughput, completeness, or future-entitlement
guarantee.

## Canonical schema, storage, and PIT destination

The provider-native page is retained as a bounded content-addressed raw object. Closed parsing maps
each value into the existing `MacroObservation` shape: `ResearchContext`, series identity, exact
decimal or explicit missing value, and unit. The context retains exact source, revision,
availability, effective and published coordinates, quality, and payload identity.

Canonical rows publish to immutable Parquet generations under
`market_squawk.research_observations`; SQLite owns provider state, quotas, jobs, offsets, manifests,
and recovery. A PIT selector resolves the exact series revision available at the requested cutoff.
Derived features, backtests, forecasts, and recommendations bind that manifest as a parent.

## Scheduling and degradation

**APPLICATION POLICY:** polling follows each selected series or release cadence. Monthly data is not
polled every minute. Metadata and vintage discovery refresh on activation, scheduled release,
observed provider update, gap repair, or explicit user research.

Acquisition priority is interactive request, expected release, unresolved page/revision repair,
then background history. A throttle or provider failure pauses background work first. Partial page
chains remain unpublished; current-definition direct-agency data may supplement a workflow but
cannot impersonate an unavailable ALFRED vintage.

## Current repository integration seams and status

The existing `market-squawk-adapter-fred` crate already provides bounded key handling, exact series
metadata, observation-page parsing, explicit missing values, offset closure, canonical macro
normalization, revision lineage, raw evidence, and provider-rate authority integration. A
`FredVintagePage` parser exists, but the production source currently acquires series metadata and
`output_type=1` observation pages; the complete vintage-date and v2 release acquisition path is not
composed end to end.

The current built-in profile still contains an older two-window application budget of 2 requests
per second and 120 per minute. That internal policy is not an upstream fact and must be reconciled
to the maintained 1-request/second target before implementation resumes. No FRED/ALFRED macro read
is yet proven through the complete Desktop/CLI/MCP investment workflow.

Reuse these seams rather than creating another adapter framework:

- `adapters/market-squawk-adapter-fred` for provider-native acquisition and normalization;
- existing provider-rate authority for the one shared queue;
- `MacroObservation`, Arrow schema registry, immutable Parquet publication, manifests, and PIT
  selection;
- existing onboarding/protected-secret path and typed application operations.

## Doctor and end-to-end acceptance gates

The provider doctor must:

1. validate and redact the configured key;
2. fetch one code-owned series metadata record and verify the requested/returned identity;
3. fetch a bounded v1 observation page and validate status, schema, count/offset/limit, missing
   values, clocks, and exact page digest;
4. exercise the enabled v1 vintage-date or v2 release path with its own authentication and
   pagination contract; validate `has_more`/`next_cursor` for v2;
5. record rate headers or their absence, response bytes/latency, and readiness without returning
   raw values or secrets.

End-to-end acceptance requires complete page traversal, immutable raw and canonical publication,
two-cutoff PIT selection that reproduces distinct vintages where available, restart recovery from
the same manifest, a derived macro feature with the exact parent, and a bounded typed workflow read
consumed by the Console. A successful doctor alone leaves the workflow unproven.

## Hard gaps

- No reviewed numeric v1 request-rate window or stable rate-header contract exists; v2's documented
  2-request/second throttle must not be generalized to v1.
- Not every series has historical vintages or a precise intraday public-availability timestamp.
- The selected pages do not define one formal versioned schema or every missing-value variation.
- The current repository does not yet complete v2 release acquisition, vintage-date acquisition,
  typed workflow composition, or restart proof.

## First-party sources

- [FRED API overview](https://fred.stlouisfed.org/docs/api/fred/overview.html)
- [Series metadata](https://fred.stlouisfed.org/docs/api/fred/series.html)
- [Series observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
- [Series vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html)
- [FRED API errors](https://fred.stlouisfed.org/docs/api/fred/errors.html)
- [FRED API-key guide](https://fred.stlouisfed.org/docs/api/api_key.html)
- [FRED API v2 release observations](https://fred.stlouisfed.org/docs/api/fred/v2/release_observations.html)
- [FRED API v2 errors and throttling](https://fred.stlouisfed.org/docs/api/fred/v2/errors.html)
- [FRED API v2 authentication](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html)

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
- [Provider account setup](../../operations/provider-account-setup.md)
