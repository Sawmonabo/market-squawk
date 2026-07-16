# Docs Batch 004 Deep Dive

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

This report reviews only `docs-045` (SEC EDGAR public-data APIs), `docs-046`
(FRED/ALFRED API), and `docs-047` (BLS Public Data API). It focuses on lawful request
behavior, endpoint and bulk choices, availability/revision provenance, pagination,
idempotency, validation, and test isolation. Sources were accessed on **2026-07-15**.
**Confirmed** statements are directly documented; **Inference** statements apply that
evidence to Market Squawk.

## Sources Reviewed

| ID | Official family | Pages reviewed | Main use |
|---|---|---|---|
| `docs-045` | U.S. SEC EDGAR | [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces), [developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | Submissions, XBRL facts/frames, bulk archives, access policy |
| `docs-046` | Federal Reserve Bank of St. Louis | [FRED overview](https://fred.stlouisfed.org/docs/api/fred/overview.html), [observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html), [real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html), [ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html), [API keys](https://fred.stlouisfed.org/docs/api/api_key.html) | Macro series, pagination, vintages, revisions |
| `docs-047` | U.S. Bureau of Labor Statistics | [Getting started](https://www.bls.gov/developers/home.htm), [v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm), [FAQ/limits](https://www.bls.gov/developers/api_faqs.htm) | Public time series, request signatures, limits, errors |

## Findings

### 1. Request policy must be provider-specific and fail politely

**Confirmed.** SEC permits scripted access but sets a current maximum of 10 requests
per second and asks automated clients to declare a `User-Agent` containing organization
and administrative contact. SEC's data APIs require neither authentication nor an API
key. ([SEC developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces))

**Inference.** Use one process-wide SEC token bucket configured below the published
ceiling, a truthful stable user agent, bounded concurrency, response caching, and
capped exponential backoff with jitter for throttling or transient failures. Stop and
degrade source health after a retry budget; do not rotate identities, IPs, proxies, or
headers to evade blocking. Cache immutable raw responses by source URL and payload
hash, and prefer bulk archives for backfill to reduce load.

**Confirmed.** Every FRED web-service request requires a 32-character lowercase
alphanumeric API key. FRED directs developers to use a distinct key per application
and says each application user should use their own key.
([FRED API keys](https://fred.stlouisfed.org/docs/api/api_key.html))

**Inference.** Treat FRED keys as user-provided secrets: keyring/encrypted storage,
redacted URLs/logs, and no key sharing. The assigned FRED pages publish no numeric
request ceiling, so Market Squawk must not invent one; use conservative configurable
concurrency, caching, retry/backoff, and provider health while honoring any returned
throttling response.

**Confirmed.** BLS v1 is unregistered and limited to 25 daily queries, 25 series per
query, and 10 years per query. Registered v2 allows 500 daily queries, 50 series, and
20 years. Both document 50 requests per 10 seconds; HTTP 429 means too many requests.
([BLS getting started](https://www.bls.gov/developers/home.htm),
[BLS FAQ](https://www.bls.gov/developers/api_faqs.htm))

**Inference.** The default zero-mandatory-cost adapter can use v1 within its limits;
v2 is an optional user-authorized free-key configuration. Track daily and rolling
windows locally before sending, batch within series/year caps, and never obtain or
rotate registrations to evade limits.

### 2. Endpoint and bulk choices should minimize requests without losing provenance

**Confirmed.** SEC submissions JSON contains current identity metadata and at least
one year or 1,000 recent filings, whichever is more; older history is referenced in
additional JSON files with date ranges. Company Facts returns all company concepts in
one call. XBRL Frames returns, for a requested standard-taxonomy concept/unit/period,
one last-filed fact per entity that best fits the requested calendar period. Company
fiscal dates may differ from the frame dates.
([SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces))

**Confirmed.** SEC calls nightly bulk ZIP archives the most efficient way to fetch
large amounts of API data. `companyfacts.zip` and `submissions.zip` are republished
nightly around 3:00 a.m. ET, while APIs update throughout the day.
([SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces))

**Inference.** Bootstrap and periodic reconciliation should use verified bulk ZIPs;
incremental collection should use submissions/companyfacts APIs. Traverse every
additional-submission file, but deduplicate overlaps with the bulk snapshot. Frames
fit cross-sectional screening, not authoritative reconstruction of a filing: retain
the actual fact start/end, accession/source reference, filing date, taxonomy, unit,
and frame-selection context.

**Confirmed.** FRED observations supports JSON/XML/XLSX/compressed CSV, `limit` from
1 to 100,000, nonnegative `offset`, ascending/descending observation-date order, and
explicit observation and real-time ranges. It can return normal observations, all
vintages, changed vintages, or initial releases. Vintage dates can replace a real-time
range. ([FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html))

**Inference.** Prefer JSON for validated incremental work. Pin `realtime_start`,
`realtime_end`, observation bounds, output type, and ascending order; advance offset
until the returned count is exhausted. A manifest must record every request parameter
because transformations, aggregation, and default “today” semantics change meaning.

**Confirmed.** BLS uses GET for a single series and POST for multi-series or
parameterized requests. v2 POST accepts series IDs, start/end year, and optional
catalog, calculations, annual average, aspects, and registration key. Responses
include status, messages, series ID, year, period, value, and footnotes.
([BLS getting started](https://www.bls.gov/developers/home.htm),
[BLS v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm))

**Inference.** Chunk BLS requests deterministically by sorted series IDs and year
windows; there is no offset-pagination contract in the assigned pages. Preserve
footnotes and optional catalog metadata. A top-level `REQUEST_SUCCEEDED` is
insufficient because BLS documents invalid-series messages with empty series data;
validate messages and each requested series independently.

### 3. Point-in-time correctness requires explicit availability and revision records

**Confirmed.** SEC says submissions normally reach sec.gov 1–3 minutes after the
EDGAR timestamp, but lag can increase and is not guaranteed. Data API processing is
typically under a second for submissions and under a minute for XBRL, with longer
delays possible at peaks. ([SEC developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces))

**Inference.** An acceptance/filing timestamp is not proof of when Market Squawk could
observe the record. Store source timestamps plus `received_at`/`ingested_at`; set
`available_at` only from defensible source evidence or local first-successful fetch.
Keep raw payload hashes so amended filings and changed facts become new revisions,
never destructive overwrites.

**Confirmed.** FRED real-time periods describe when information was known until it
changed; default real-time bounds are today and the interval is closed at both ends.
ALFRED adds real-time periods for original releases and later revisions. Vintage dates
are dates on which values were released or revised, excluding release dates with no
data change. ([FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html),
[ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html),
[FRED vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html))

**Inference.** Canonical identity should include series ID, observation date,
real-time start/end or vintage date, units/transformation, and value. Preserve all
vintages and derive supersession rather than replacing prior values. Date-granularity
vintages do not establish an intraday publication timestamp.

**Confirmed.** BLS responses can mark observations “Preliminary” through footnotes.
The assigned API pages document current historical series retrieval but no vintage-
history or exact publication-time endpoint. ([BLS v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm))

**Inference.** Store BLS snapshots with fetch time, raw hash, value, and all footnotes;
when a value changes, supersede rather than mutate. Market Squawk cannot claim
pre-capture BLS point-in-time history from this API evidence alone.

### 4. Idempotency and schema validation belong in the adapter

**Inference.** Suggested natural identities are: SEC accession plus taxonomy/concept/
unit/period/dimensions; FRED series plus observation date and real-time/vintage
identity; BLS series plus year/period and captured revision. Each ingestion object
also needs source URL, request hash, payload hash, fetch timestamps, parser/schema
version, and run ID. Upsert only byte/semantic duplicates; preserve conflicting or
revised records and flag unexpected collisions.

**Inference.** Validate envelopes, required fields, dates, identifiers, numeric text,
array alignment, counts, and enum values before normalization. Preserve unknown fields
in raw payloads for forward compatibility, but quarantine missing required fields,
misaligned SEC columnar arrays, incomplete FRED pages, BLS partial errors, and response
series not requested. Publish a dataset only after whole-run count/hash checks pass.

### 5. Deterministic and external tests must be separated

**Inference.** The default suite should use small, redacted, immutable provider
fixtures and cover:

- SEC recent/additional history overlap, leading-zero CIK, amendments, XBRL units and
  periods, custom-taxonomy exclusion, mismatched column arrays, frame fiscal-date
  differences, corrupt ZIP, and identical bulk/API deduplication.
- FRED multi-page count/offset, pinned real-time intervals, initial/revised vintages,
  same-date duplicates, changed values, missing/invalid numeric text, and secret
  redaction from request diagnostics.
- BLS v1/v2 chunk boundaries, daily/rolling-budget rejection, preliminary footnotes,
  top-level success with per-series error, missing series, schema drift, 429 backoff,
  and idempotent repeated snapshots.

**Inference.** Live network tests must be a separate opt-in target, use user-provided
credentials, run serially beneath published limits, and tolerate provider unavailability
without weakening deterministic tests. They should verify contracts and record source
health, not assert volatile observation values.

## Evidence Table

| Claim | Source | Evidence | Confidence | Notes |
|---|---|---|---|---|
| SEC allows at most 10 requests/second and requires a declared user agent. | [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | programmatic-download guidance | High | Configure below ceiling |
| SEC bulk archives are the efficient large-backfill route and update nightly. | [SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | bulk/update sections | High | APIs serve incremental updates |
| SEC availability lag is variable and not guaranteed. | [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | lag guidance | High | Capture local availability |
| FRED requires per-user application keys. | [FRED keys](https://fred.stlouisfed.org/docs/api/api_key.html) | key requirements | High | Secrets must be redacted |
| FRED observations supports count/limit/offset and explicit real-time ranges. | [FRED observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html) | parameters | High | Pin all semantics |
| ALFRED records original and revised real-time periods. | [ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html) | archival description | High | Preserve revisions |
| BLS limits differ for registered v2 and unregistered v1. | [BLS FAQ](https://www.bls.gov/developers/api_faqs.htm) | query-limit table | High | v1 remains baseline |
| BLS returns messages and per-observation footnotes. | [BLS v2](https://www.bls.gov/developers/api_signature_v2.htm) | response schemas | High | Inspect partial errors |
| **Inference:** provider snapshots must be immutable and idempotent. | All assigned families | revisions, pagination, overlapping retrieval | High | Publish only validated runs |
| **Inference:** external network tests remain opt-in. | All assigned families | keys, quotas, variable availability | High | Default suite is deterministic |

## Source-Specific Notes

- `docs-045`: **Inference.** Use bulk for baseline, API for incremental updates, and
  periodic bulk reconciliation; never poll every company at maximum rate.
- `docs-046`: **Confirmed.** Default real-time period is today. **Inference.** Omitting
  it creates non-reproducible research queries.
- `docs-047`: **Confirmed.** v1 needs no registration. **Inference.** This satisfies
  the baseline even when no user supplies a v2 key.

## Cross-Source Patterns

1. A source timestamp, revision date, and local first-observed time are different.
2. Backfills and incremental APIs overlap; content identity and manifests prevent
   duplication without erasing revisions.
3. Public access still requires honest identity, quotas, caching, and bounded backoff.
4. Provider success envelopes do not replace record-level schema and completeness
   validation.
5. These are research-plane adapters only and never belong in the live decision path.

## Limitations and Non-Findings

- The assigned FRED pages state no numeric request-rate limit; none is asserted here.
- SEC timing is typical, not guaranteed, and bulk archives are only nightly snapshots.
- SEC Frames choose best calendar alignment and are not exact common-period facts.
- FRED/ALFRED vintages are date-granular in the reviewed API, not intraday timestamps.
- The reviewed BLS API does not expose complete vintage history or exact availability
  timestamps; history before local capture cannot be reconstructed from it alone.
- Provider schemas may evolve; the sources do not supply Market Squawk manifests,
  idempotency keys, or atomic dataset publication.
- No live requests, performance benchmarks, or sources outside the assigned official
  families were used.

## Source List

Official sources, accessed **2026-07-15**:

- `docs-045`: [SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
  [developer FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions).
- `docs-046`: [FRED overview](https://fred.stlouisfed.org/docs/api/fred/overview.html),
  [observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html),
  [vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html),
  [real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html),
  [ALFRED](https://fred.stlouisfed.org/docs/api/fred/alfred.html),
  [API keys](https://fred.stlouisfed.org/docs/api/api_key.html).
- `docs-047`: [BLS getting started](https://www.bls.gov/developers/home.htm),
  [v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm),
  [FAQ](https://www.bls.gov/developers/api_faqs.htm).
