# U.S. Census Data API provider contract

The Census Data API is Market Squawk's selected source for demographic, household, business,
trade, and geographic statistical evidence. Its current official query contract needs a fresh
freeze before adapter implementation because the assigned Query Components page changed content.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Selected target provider; configured probe evidence exists, current query contract and adapter are incomplete |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Credential input | `CENSUS_ENABLED`, `CENSUS_API_KEY` |
| Canonical destination | Macro/reference `market_squawk.research_observations` |

## Role and product workflows

**VERIFIED PROVIDER FACT:** the current Available Data guide identifies ACS 1-Year, ACS 5-Year,
ACS Supplemental and Migration Flows, Economic Indicators Time Series, Decennial Census, Economic
Census, County Business Patterns and Nonemployer Statistics, Population Estimates and Projections,
and International Trade as major API families. The provider adds datasets frequently and directs
users to its discovery tooling for the current complete list.

**APPLICATION POLICY:** selected Census rows support regional and demographic features, household
and business context, demand/regime research, industry screens, forecasts, and investment briefs.
Every request is an exact dataset-vintage-variable-geography coordinate; the popular-family list is
not a code-owned catalog.

## Authentication and setup

**APPLICATION POLICY:** the selected configured profile requires `CENSUS_API_KEY`, sends it as a
redacted query parameter, and never exposes it in URLs, logs, traces, errors, receipts, or provider
status. Use the official [key request](https://api.census.gov/data/key_signup.html).

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the assigned current guide pages did not re-establish exact
key thresholds or whether every selected dataset requires a key. The application uses the key
conservatively; possession of it does not prove a dataset, variable, geography, or quota.

## Exact surface and data families

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the maintained target route family is
`GET https://api.census.gov/data/{year}/{dataset}` with dataset-specific `get`, geography, predicate,
and `key` parameters. Before implementation, current first-party discovery and query pages must be
frozen for every admitted dataset because the reviewed URL ending in
`api-user-guide.Query_Components.html` returned the guide Overview rather than its expected query
grammar.

**VERIFIED PROVIDER FACT:** Census statistical observations are generally associated with a Census
geographic boundary identified by FIPS and a dataset vintage or reference year. TIGERweb boundary
services and the Census Geocoder are separate products; a Data API row does not silently include or
authorize geometry/geocoding evidence.

The provider-native contract must retain dataset, year/vintage, variables and groups, predicates,
geography/FIPS hierarchy, response header, values, annotations, and exact discovery metadata.

## Provenance, clocks, revisions, and missingness

Every row retains dataset and release identity, reference year/vintage, observation period when
supplied, variable/group identity and label, concept, exact raw value, checked value or missing/
annotation state, geography and FIPS coordinates, request/response digest, local receipt,
availability, ingest, and canonical publication times.

**APPLICATION POLICY:** a Census vintage/reference year describes the statistical product, not an
ALFRED-style as-known revision history. Reacquired changed payloads append locally observed
revisions. Later releases or metadata cannot rewrite earlier model, backtest, or recommendation
inputs.

## Official limits and adaptive admission

| Dimension | Contract |
| --- | --- |
| Numeric request rate | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no current numeric request/window ceiling was established by the reviewed first-party pages |
| Daily request count | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no current daily quota was established |
| Variables, response rows, and pagination | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** the reviewed pages did not establish current maxima or a universal pagination contract |
| Shared application rate | **APPLICATION POLICY:** 1 request/second and 400 requests/day until a frozen current contract and retained measurements justify a reviewed change |

One provider queue records every attempt, bytes, returned rows, header/variable closure, latency,
errors, 429/cooldown evidence, and dataset/metadata generation. A response that omits requested
variables or changes its header is partial or schema drift, not success.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** a keyed 2024 ACS one-year request returned HTTP 200 with two rows,
including the response header, on 2026-08-11. This proves the tested credential and request only;
it is not a rate, general dataset entitlement, schema-stability, or completeness guarantee.

## Canonical schema, storage, and PIT destination

Bounded discovery, metadata, and data responses are stored as separate content-addressed raw
objects. Provider-native parsing validates the exact response header and maps each variable/
geography cell into a closed macro/reference observation with exact unit or annotation semantics.
Unknown rows do not enter a generic canonical JSON payload.

Immutable Parquet generations publish under `market_squawk.research_observations`; SQLite owns the
credential generation, budgets, admitted dataset contracts, discovery metadata, jobs, checkpoints,
manifests, health, and recovery. PIT selectors choose only the dataset/metadata revision locally
available at the cutoff. Derived features bind exact parents and geography definitions.

## Scheduling and degradation

**APPLICATION POLICY:** Census is release- and dataset-driven cold research. Activation refreshes
bounded discovery; scheduled jobs follow each admitted product's actual release cadence. It is not
polled as a current market feed.

Interactive research and known release refresh outrank broad historical acquisition. Under rate,
schema, or byte pressure, background datasets pause first. A drifted query or missing variable
degrades only Census-dependent features and must surface Unavailable rather than silently selecting
a nearby year, geography, or proxy.

## Current repository integration seams and status

Repository inspection found no Census adapter crate, built-in provider profile, current query-
contract freeze, canonical mapper, publication job, typed application read, or Desktop composition.
The credential template and account guide describe target setup, and one keyed ACS request
succeeded, but that is not a shipping vertical.

Implementation must reuse existing onboarding and protected secrets, provider-rate authority, raw
capture, research observation and Arrow/Parquet schemas, manifests, PIT selectors, checkpoints,
typed application operations, and readiness states. Dataset-specific parsing belongs below the
application boundary; the frontend never constructs Census URLs or interprets provider arrays.

## Doctor and end-to-end acceptance gates

The provider doctor must:

1. validate/redact the key and freeze the current official discovery/query contract;
2. resolve one code-owned dataset, vintage, variable group, and geography;
3. issue one bounded request and validate HTTP/body success, exact header, requested variables,
   geography/FIPS, row count, missing/annotation grammar, bytes, and response digest;
4. record rate headers or their absence, schema identity, and readiness without exposing the key.

End-to-end acceptance requires metadata and data raw objects, closed canonical rows, complete
publication, a two-vintage or locally observed revision case, restart-safe job/manifest recovery,
PIT selection, and a bounded macro/reference feature consumed by the Console. Geometry or geocoder
work requires its own separately reviewed contract.

## Hard gaps

- The assigned current Query Components URL no longer supplied the expected query grammar.
- Numeric rate/daily ceilings, variable/row maxima, pagination, stable error behavior, and revision
  mechanics were not established by the reviewed pages.
- The Data API does not itself close historical as-known revisions or boundary-shape history.
- No Census adapter, publication job, PIT read, or product workflow currently exists in the
  repository.

## First-party sources

- [Census developer portal](https://www.census.gov/data/developers.html)
- [Census Data API user guide](https://www.census.gov/data/developers/guidance/api-user-guide.html)
- [Available Data](https://www.census.gov/data/developers/guidance/api-user-guide.Available_Data.html)
- [Assigned Query Components URL, currently rendering Overview](https://www.census.gov/data/developers/guidance/api-user-guide.Query_Components.html)
- [Census API key request](https://api.census.gov/data/key_signup.html)

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
