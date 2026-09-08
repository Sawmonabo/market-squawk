# Bureau of Labor Statistics provider contract

BLS is Market Squawk's selected direct source for labor, inflation, employment, wage, and
productivity observations. This page separates the provider's request limits from Market Squawk's
more conservative operating policy and from the current repository implementation.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Selected macro provider; existing v1/v2 adapter foundation, registered-v2 reconciliation and workflow composition incomplete |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Credential input | `BLS_ENABLED`, `BLS_REGISTRATION_KEY` |
| Canonical destination | Macro `market_squawk.research_observations` |

## Role and product workflows

**APPLICATION POLICY:** BLS supplies direct labor-market and inflation observations for regime
features, forecasts, time-ordered backtests, valuation context, opportunity screening, and plain-
language evidence. Series IDs are selected explicitly; BLS does not provide one universal series
catalog through the time-series endpoint.

Useful families include CPI, employment and unemployment, hours and earnings, wages, productivity,
producer prices, and survey-specific labor measures. Series title, unit, frequency, seasonal
adjustment, and measure are admitted metadata rather than inferred from the identifier or values.

## Authentication and setup

**VERIFIED PROVIDER FACT:** unregistered API v1 requires no key. Registered API v2 registration
asks for email and organization, uses a CAPTCHA, sends the key by email, and requires renewal at
least annually. See the [BLS FAQ](https://www.bls.gov/developers/api_faqs.htm) and
[registration page](https://data.bls.gov/registrationEngine/).

**APPLICATION POLICY:** Market Squawk selects registered v2 for normal configured acquisition and
retains v1 as a lower-capacity no-key path. The credential file holds only
`BLS_REGISTRATION_KEY`; the series plan, years, endpoint, and budgets remain code-owned. A key is
write-only input and never appears in status, logs, receipts, or recovery records.

## Exact surfaces and data families

| Evidence | Read surface | Admitted meaning |
| --- | --- | --- |
| **VERIFIED PROVIDER FACT** | `POST https://api.bls.gov/publicAPI/v2/timeseries/data/` | Registered multi-series observations with optional catalog, calculations, annual averages, and aspects |
| **VERIFIED PROVIDER FACT** | `POST https://api.bls.gov/publicAPI/v1/timeseries/data/` | Lower-capacity unregistered observations |
| **VERIFIED PROVIDER FACT** | v2 latest, popular-series, all-surveys, and single-survey operations documented in the v2 signature reference | Bounded discovery and current-point helpers; these are distinct contracts, not interchangeable time-series responses |

The observation envelope carries status, response time, messages, series ID, year, period,
period name, string value, footnotes, and optional latest/catalog/aspect fields.

**VERIFIED PROVIDER FACT:** BLS can return top-level `REQUEST_SUCCEEDED` while its message reports
an invalid series and the series data are empty. Transport success is not semantic success.

## Provenance, clocks, revisions, and missingness

Each row retains series ID, selected metadata evidence, year and period code, exact raw value,
checked decimal or explicit missing state, period label, latest marker, preliminary state,
footnotes/aspects, request plan, response digest, receipt time, and canonical publication time.

**VERIFIED PROVIDER FACT:** a `P` footnote can mark an observation preliminary. The reviewed API
does not expose an ALFRED-style historical-vintage endpoint.

**APPLICATION POLICY:** BLS revisions are locally observed versions unless a separate source-
authored correction/release artifact proves more. Scheduled release time, actual public
availability, local receipt, erratum publication, withdrawal, correction, and superseding payload
remain different clocks and states. Earlier values are never overwritten.

The official [Errata index](https://www.bls.gov/errata/) is captured as separate correction
evidence. A notice can precede a replacement value; its status alone cannot prove that the corrected
API payload was reacquired.

## Official limits and adaptive admission

| Dimension | Public v1 | Registered v2 |
| --- | ---: | ---: |
| Daily queries | **VERIFIED PROVIDER FACT:** 25 | **VERIFIED PROVIDER FACT:** 500 |
| Series per query | **VERIFIED PROVIDER FACT:** 25 | **VERIFIED PROVIDER FACT:** 50 |
| Years per query | **VERIFIED PROVIDER FACT:** 10 | **VERIFIED PROVIDER FACT:** the comparison table says 20, while a later answer on the same FAQ says a request must not exceed 10 |
| Burst window | **VERIFIED PROVIDER FACT:** 50 requests per 10 seconds | **VERIFIED PROVIDER FACT:** 50 requests per 10 seconds |

**APPLICATION POLICY:** registered v2 is admitted at 400 queries/day, 1 request/second, 50 series
per request, and no more than 10 inclusive years per request until the provider resolves the
conflicting 10/20-year statements or a reviewed controlled contract does so. The durable quota
ledger counts every attempt and survives restart.

HTTP 429 pauses the queue. Provider messages, empty series, missing requested IDs, or observations
outside the requested year window make the page partial or invalid; they never count as successful
observations.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** a bounded registered-v2 request for one configured series returned
HTTP 200 and `REQUEST_SUCCEEDED` on 2026-08-11. This was a credential/shape probe, not a throughput,
limit, correction, or complete-history test.

## Canonical schema, storage, and PIT destination

Bounded raw BLS responses and separately verified series metadata are content-addressed. Closed
normalization maps each row into the existing `MacroObservation` contract with exact series, unit,
decimal-or-missing value, source quality, effective period, local availability, response identity,
and locally observed revision lineage.

Canonical rows publish to immutable Parquet generations under
`market_squawk.research_observations`; SQLite owns quota windows, request plans, jobs, page objects,
health, manifests, and recovery. PIT selection uses the value actually observed by the cutoff. It
does not backfill a later correction into an earlier model or recommendation.

## Scheduling and degradation

**APPLICATION POLICY:** acquisition is release-aware and series-specific. Expected labor or
inflation releases receive priority; monthly or quarterly series are not polled continuously.
Interactive lookup and correction reconciliation outrank background history.

Under pressure, pause historical catch-up, reduce metadata refresh, and defer low-priority series
before compromising the current release set. Exhausting the daily ledger makes registered v2
Unavailable until the next admitted window; the runtime may use v1 only for a workload that fits
v1's independently declared limits and provenance.

## Current repository integration seams and status

The existing `market-squawk-adapter-bls` crate already contains public-v1 and registered-v2
authorization, deterministic series/year chunking, bounded request bodies and responses,
requested-versus-returned validation, explicit partial state, footnote/preliminary retention,
separately verified series metadata, canonical macro normalization, local revision evidence, and
shared provider-budget integration.

The current v2 `BlsRequestPlan` still enforces the documented table value of 20 years per request.
That conflicts with the maintained conservative 10-year **APPLICATION POLICY** and must be fixed
before registered-v2 work resumes. The built-in v2 profile is currently marked refresh-required,
and the complete registered acquisition-to-Console workflow is not release-proven. BEA or FRED
metadata must not be used to fill missing BLS series semantics.

Reuse the current adapter, onboarding, provider-rate, `MacroObservation`, Arrow/Parquet, manifest,
PIT, and typed-application boundaries; do not add a second macro framework.

## Doctor and end-to-end acceptance gates

The provider doctor must:

1. distinguish v1 from v2 and validate/redact the selected credential generation;
2. request one code-owned series and bounded year, then validate HTTP and body status;
3. prove the exact requested/returned series set, nonempty data where expected, year bounds,
   messages, footnotes, raw values, and response digest;
4. report remaining durable daily/burst budget and renewal state without returning the key;
5. mark the source Degraded or Unavailable on semantic-success contradictions.

End-to-end acceptance requires multi-series and multi-window chunk closure, exact metadata,
immutable raw/canonical publication, a preliminary-to-revised or correction fixture, restart-safe
quota and manifest recovery, PIT selection, and a typed macro/feature workflow consumed by the
Console. Live network checks remain explicitly opt-in.

## Hard gaps

- The provider FAQ internally conflicts between 10 and 20 years for registered requests.
- The API has no source-authored historical-vintage replay equivalent to ALFRED.
- Catalog/unit metadata is not universal across surveys.
- The Errata page has no stable machine API, correction ID, update-frequency guarantee, or exact
  mapping to every affected series.
- Registered-v2 evidence/profile refresh, conservative chunking, product composition, and restart
  proof remain incomplete in the repository.

## First-party sources

- [BLS Public Data API FAQ](https://www.bls.gov/developers/api_faqs.htm)
- [BLS API v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm)
- [BLS registration](https://data.bls.gov/registrationEngine/)
- [BLS Errata](https://www.bls.gov/errata/)

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
