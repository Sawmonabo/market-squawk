# Federal Reserve Board Data Download Program

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | H.15 profile, bounded doctor, activation binding, and registered research-source path implemented; focused proof and product availability remain open |
| Evidence cutoff | 2026-08-12, America/New_York |
| Audit basis | `7fb52c19dc273fe44d3846f1986c61c1321032fd` plus the preserved Wave 6 overlay |

## Role and product workflows

The Board's Data Download Program (DDP) supplies direct statistical-release observations such as
H.15 rates, with source-authored series labels, units, multipliers, dimensions, and release
structure. These observations support macro/rate research, economic-regime features, valuation,
forecasting, screening, and backtests.

DDP is a current-definition source. It is not the authority for what a series looked like before a
revision, and the product must expose that limitation at historical cutoffs.

## Authentication and setup

- **VERIFIED PROVIDER FACT:** the reviewed DDP help documents browser and automated downloads
  without an API key or account credential.
- **APPLICATION POLICY:** `FEDERAL_RESERVE_BOARD_DIRECT_ENABLED=true` creates no credential. The
  one-time import records enabled intent; a later onboarding operation must pass the exact bounded
  H.15 doctor before profile activation evidence exists.
- **IMPLEMENTED CODE FACT:** built-in profile
  `federal-reserve-board.data-download-program` revision 4 is `available` for this exact no-key
  H.15 doctor. That release state admits reviewed profile/onboarding prerequisites; it does not
  claim durable data publication or product availability.

## Exact endpoints, formats, and data families

| Surface | Exact contract |
| --- | --- |
| DDP gateway | `https://www.federalreserve.gov/datadownload/` |
| DDP help | `https://www.federalreserve.gov/datadownload/help/` |
| Initial H.15 descriptor | `https://www.federalreserve.gov/datadownload/Download.aspx?rel=H15` |
| H.15 full-history production CSV | `https://www.federalreserve.gov/datadownload/Download.aspx?filetype=csv&label=include&lastObs=&layout=seriescolumn&rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&type=package` |
| H.15 bounded doctor CSV | `https://www.federalreserve.gov/datadownload/Output.aspx?filetype=csv&label=include&lastobs=10&layout=seriescolumn&rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&type=package` |
| Change/correction channel | `https://www.federalreserve.gov/feeds/DataDownload.html` |

- **VERIFIED PROVIDER FACT:** DDP supports custom or preformatted packages in CSV, Excel, and
  XML/SDMX; a complete statistical release can be downloaded in SDMX with common, release-specific,
  and dataset schema/structure files.
- **VERIFIED PROVIDER FACT:** one output file can contain only **one frequency**.
- **VERIFIED PROVIDER FACT:** custom packages can be bounded by date or observation count and carry
  series labels, units, currency where applicable, and unit multipliers.
- **APPLICATION POLICY:** each dataset identity freezes release, series, frequency, date or
  observation bounds, output format/layout, exact generated automation URL, and all matching
  schema/structure digests. A copied URL is evidence for that package only, not a universal DDP
  endpoint template.
- **IMPLEMENTED CODE FACT:** the admitted initial CSV contains exactly these H.15 Treasury
  constant-maturity series: `1m`, `3m`, `6m`, `1y`, `2y`, `3y`, `5y`, `7y`, `10y`, `20y`, and
  `30y`. Values retain the Board's exact decimal representation and
  `percent_per_year` presentation unit.
- **APPLICATION POLICY:** the doctor and production contracts have distinct URL, request, and
  contract digests. A ten-row doctor response cannot be published as a complete history.

## Feed provenance, clocks, and revisions

Retain Board, release, series, frequency, unit/multiplier, observation period, scheduled release
time when separately established, source-visible DDP publication time, route-visible availability,
local received/ingested times, file/schema digests, and local revision.

- **VERIFIED PROVIDER FACT:** DDP exposes only currently defined data; pre-revision and real-time
  data are not available through the reviewed contract.
- **APPLICATION POLICY:** observation period, scheduled release, release-page publication, DDP
  route availability, correction/repost time, and local observation are separate clocks.
- **APPLICATION POLICY:** a corrected or reposted file appends a new immutable generation and
  supersedes through explicit evidence. Current values may not be backdated into a historical model
  cutoff.

## Capacity and adaptive admission

| Claim | Evidence class and treatment |
| --- | --- |
| File frequency | **VERIFIED PROVIDER FACT:** one frequency per output file |
| Provider request ceiling | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** no numeric request/window, concurrency limit, stable quota headers, universal series maximum, payload maximum, or retry contract was found |
| Safety budget | **APPLICATION POLICY:** one release-driven Board queue admits at most **1 request/minute** and may only lower that rate without reviewed evidence |

Admission accounts for file and schema bytes, series and observation counts, latency, response
status, retry evidence, duplicate/reposted digests, parser cost, queue pressure, and publication
capacity. Release retrieval outranks historical refresh; there is no periodic high-frequency poll.

## Runtime evidence

- **RUNTIME-MEASURED VALUE (2026-08-12):** the exact bounded doctor URL returned HTTP 200 and
  2,663 bytes containing the exact 11 admitted H.15 series and ten observation rows per series.
- **IMPLEMENTED CODE FACT:** onboarding accepts the response only after the adapter's strict H.15
  CSV parser validates all six metadata rows, all exact series identities, units, multipliers,
  currencies, periods, decimals/missing values, 11-series count, and 110 total observations.
- **APPLICATION POLICY:** this dated probe is reachability/schema evidence only. It neither proves
  future uptime nor creates a durable analytical generation.

## Canonical storage and point-in-time selection

```text
bounded release file + exact SDMX structure/schema artifacts
  -> release/frequency-specific structural validation
  -> market_squawk.research_observations::MacroObservation
  -> immutable Parquet generation + manifest
  -> current-definition-aware PIT selector
  -> typed macro/rate/model read
```

Canonical series identity includes release and Board series coordinates; exact decimals, units,
multipliers, provider-native missing values, file/schema digests, and every clock above are
mandatory. SQLite owns profile, request permits, release jobs, health, file/checkpoint identities,
manifests, correction lineage, and recovery.

A PIT selector may use a locally retained earlier DDP acquisition if it was available at the
cutoff. It may not claim that DDP itself supplies a complete vintage history.

## Scheduling and degradation

- **APPLICATION POLICY:** schedule by the statistical release's actual cadence and publication
  channel. Recheck the DDP announcement/correction channel around a selected release rather than
  polling every series independently.
- **APPLICATION POLICY:** release-wide acquisition closes only after the data file and every bound
  structure/schema artifact validate under byte, observation, dimension, and parser bounds.
- **APPLICATION POLICY:** route delay, correction notice, schema drift, missing structural artifact,
  or provider outage yields `Degraded` or `Unavailable`. A different channel's earlier value cannot
  silently claim DDP availability.

## Repository integration status and seams

- The strict credential importer maps the no-key
  `FEDERAL_RESERVE_BOARD_DIRECT_ENABLED` intent to profile revision 4 and returns a secret-free
  `probe_required` disposition. Import itself performs no network request or activation.
- The built-in profile owns the exact GET allowlist, ten-observation selector, local-personal
  research rights, and one-request-per-minute/single-flight application budget.
- The onboarding service performs the exact GET and parses it with the adapter's frozen H.15
  contract. The adapter also contains authority-governed HTTPS retrieval, typed H.15 parsing,
  exact raw-capture handoff, canonical macro mapping, correction/repost modeling, and publication
  primitives.
- The application now owns the exact no-key activation request/spec, constructs the production
  profile and source under the active onboarding lease, binds rich extraction output into the
  shared capture protocol, registers the exact analytical dataset identity, and serializes/restores
  the Board lifecycle surface.
- Focused activation/restart proof is still upstream-blocked. No completed live production
  retrieval has yet proved raw sealing, durable canonical generation/manifests, or restart recovery.
  PIT typed reads, macro/rate application operations, Desktop consumption, restart acceptance, and
  release acceptance remain open.
- Reuse the existing research data plane for those seams. Do not create a parallel macro store,
  scheduler, or dashboard-only fetch path.

Related contracts are the [provider architecture](../../architecture/market-data-provider-architecture.md),
[research data plane](../../architecture/research-data-plane.md), and
[shipping source coverage](../source-coverage.md).

## Doctor and end-to-end acceptance

The implemented onboarding doctor proves:

1. current no-key reachability of the exact bounded H.15 URL;
2. HTTP success under the exact endpoint/query allowlist and bounded response size;
3. exact 11-series series-column CSV structure and ten observations per series;
4. labels, units, multipliers, currencies, exact identifiers, periods, decimals, and missing values;
5. one shared request-per-minute, single-flight application budget; and
6. response-body evidence digest without inventing a vintage or durable dataset.

End-to-end product availability still requires the distinct full-history contract to complete a
live governed retrieval, retain and seal raw evidence before durable canonical publication,
survive a proven restart, answer a bounded exact-cutoff typed read, and appear in its macro/rate
workflow with `current-definition` and freshness limitations visible.

## Hard gaps

- Only the exact initial H.15 Treasury constant-maturity package is frozen. Other Board releases,
  series, formats, and schemas require separate contracts and bounded qualification.
- Numeric request/concurrency/payload limits, quota headers, and retry semantics are unpublished.
- DDP does not supply pre-revision, real-time, or complete historical-as-known data.
- Release-page, DDP-route, correction, and local availability can differ; no universal finality
  event is documented.
- Focused activation/restart proof is blocked upstream; executed durable live publication,
  manifests, PIT typed reads, workflow/Desktop consumption, and restart/release acceptance remain
  incomplete.

## First-party sources

- Board of Governors of the Federal Reserve System,
  [Data Download Program](https://www.federalreserve.gov/datadownload/), accessed 2026-08-12.
- Board of Governors of the Federal Reserve System,
  [Data Download Program Help](https://www.federalreserve.gov/datadownload/help/), accessed
  2026-08-12; page states last update 2017-03-18.
- Board of Governors of the Federal Reserve System,
  [Data Download Program announcements](https://www.federalreserve.gov/feeds/DataDownload.html),
  accessed 2026-08-12.

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
