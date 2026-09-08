# Tiingo Starter

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Status | Optional supported-fund NAV and curated EOD source; configured credential surface exists, adapter and workflow do not ship |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |

## Role and product workflows

Tiingo's selected role is intentionally narrow:

- daily NAV history for supported mutual funds;
- curated raw and adjusted equity/ETF EOD validation; and
- source-reported cash dividends and split factors carried alongside those EOD observations.

This enables fund detail/history, price-versus-NAV research where applicable, independent EOD
validation, corporate-action-aware historical features, and PIT model inputs. It is not an
intraday mutual-fund source, and the selected lane does not require Tiingo real-time streaming.

## Authentication and setup

- **VERIFIED PROVIDER FACT:** Tiingo requires an account token for API access.
- **APPLICATION POLICY:** the operator sets `TIINGO_ENABLED=true` and imports only
  `TIINGO_API_TOKEN` through the existing credential design. The token is redacted from URLs,
  headers, logs, errors, traces, receipts, and diagnostics.
- **APPLICATION POLICY:** Tiingo remains optional. Disabled, unsupported, quota-exhausted, or
  unproven fund coverage is represented as `Unavailable`; no substitute intraday NAV is fabricated.

See [provider setup](../../operations/provider-account-setup.md) and the
[credential template](../market-squawk-provider-credentials.env.example).

## Exact endpoints and data families

| Surface | Exact contract |
| --- | --- |
| Ticker metadata | `GET https://api.tiingo.com/tiingo/daily/{ticker}` |
| Latest EOD/NAV | `GET https://api.tiingo.com/tiingo/daily/{ticker}/prices` |
| Historical EOD/NAV | `GET https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={YYYY-MM-DD}&endDate={YYYY-MM-DD}` |
| Optional response controls | Provider-documented `format` and `resampleFreq` parameters |

Metadata includes ticker, name, exchange code, description, start date, and end date. EOD rows
include date; raw `open`, `high`, `low`, `close`, and `volume`; adjusted `adjOpen`, `adjHigh`,
`adjLow`, `adjClose`, and `adjVolume`; `divCash`; and `splitFactor`.

- **VERIFIED PROVIDER FACT:** the daily ticker archive contains both currently supported and
  reserved future symbols. The per-ticker metadata endpoint, including non-null coverage dates, is
  the current availability check.
- **VERIFIED PROVIDER FACT:** for a mutual fund, daily `open`, `high`, `low`, and `close` can all
  contain that day's NAV.
- **APPLICATION POLICY:** metadata admission precedes every new ticker. Archive membership never
  proves price/NAV availability, and a mutual-fund NAV wire row is not interpreted as four intraday
  trades.

## Feed provenance, clocks, and revisions

Retain provider, ticker/provider instrument, canonical instrument resolution, asset and share-class
identity, raw-versus-adjusted surface, date, currency/unit, metadata coverage interval, provider
availability guidance, local received/ingested/availability times, raw digest, and durable revision.

- **VERIFIED PROVIDER FACT:** most U.S. equity EOD data is available around **5:30 p.m. Eastern**,
  exchanges can send corrections through **8:00 p.m. Eastern**, and Tiingo updates values as those
  corrections arrive.
- **VERIFIED PROVIDER FACT:** mutual-fund NAV is described as available after **midnight Eastern**.
- **VERIFIED PROVIDER FACT:** the reviewed EOD contract exposes no immutable provider revision ID
  or exact finality event.
- **APPLICATION POLICY:** initial EOD data remains provisional through the documented correction
  window. Changed reacquisitions append immutable revisions; raw and adjusted values, `divCash`, and
  `splitFactor` remain separate source-authored fields.

## Official limits and adaptive admission

| Starter dimension | Evidence class and limit |
| --- | --- |
| Unique symbols | **VERIFIED PROVIDER FACT:** **500/month** |
| Requests | **VERIFIED PROVIDER FACT:** **50/hour** and **1,000/day** |
| Bandwidth | **VERIFIED PROVIDER FACT:** **1 GB/month** |
| Missing contract details | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** reset instants/time zones, concurrency and WebSocket ceilings, multi-ticker batch maximum, burst behavior, quota headers, failed-call accounting, pagination, and overage behavior are unpublished in the reviewed pages |

Market Squawk's persistent ledger admits a request only when every dimension remains available:

| Application dimension | Budget |
| --- | --- |
| Requests | **APPLICATION POLICY:** **40/hour** and **800/day** |
| Unique symbols | **APPLICATION POLICY:** **400/month** |
| Bandwidth | **APPLICATION POLICY:** **800 MB/month** |

Hourly, daily, unique-symbol, and byte counters are conjunctive and survive restart. Admission uses
the configured plan/evidence date plus actual response bytes, requested/returned symbols, latency,
HTTP refusal/retry evidence, queue lag, correction work, and publication pressure. An uncertain
reset does not optimistically restore capacity; operator review or a conservative elapsed-window
rule is required.

## Runtime evidence

- **RUNTIME-MEASURED VALUE:** on **2026-08-11**, the configured token returned HTTP **200** for AAPL
  metadata, HTTP **200** for VTSAX metadata, and HTTP **200** with **one row** for the VTSAX latest
  daily-price request.
- **UNVERIFIED ENTITLEMENT/ASSUMPTION:** these probes establish token reachability and two useful
  symbol shapes only. They do not prove the supported-fund universe, history depth, correction
  finality, sustainable throughput, counter reset behavior, or a batch contract.

## Canonical storage and point-in-time selection

The target canonical split is:

| Tiingo evidence | Canonical destination |
| --- | --- |
| Equity/ETF raw EOD | `market_squawk.research_observations::MarketBarObservation` with `MarketBarAdjustment::Raw` |
| Equity/ETF adjusted EOD | A separate `MarketBarObservation` with exact provider adjustment identity; never overwrite raw |
| Cash dividend/split factor | `CorporateActionObservation` only after exact action/effective semantics validate; otherwise retained provider-native evidence |
| Mutual-fund daily NAV | A first-class instrument-scoped `FundNavObservation` target with NAV date/value/currency/context; this canonical variant is absent today and must be added rather than pretending NAV is traded OHLC |

The shared [canonical schema contract](../market-data-canonical-schemas.md) owns the exact
`ResearchObservation::FundNav(FundNavObservation)` variant. Tiingo mapping must provide the exact
fund/share-class and provider instrument, `CalendarDate` NAV date, observed `Money` and currency or
closed missing state, source publication time/date when supplied, conservative availability and
local receipt/publication clocks, revision/supersession, raw lineage, and request disposition. Its
natural family excludes revision and includes source/channel, share class, provider instrument,
NAV date, valuation basis, and currency. Same-day corrections append; EOD market bars remain
separate evidence and cannot fill an unavailable NAV.

The full path is bounded raw response to typed Tiingo validation, canonical observation, immutable
Parquet generation/manifest, exact-cutoff PIT selection, and bounded Funds/Markets/model reads.
SQLite owns metadata admission, persistent quota counters, jobs, ticker/date checkpoints, correction
state, manifests, and recovery.

## Scheduling and degradation

- **APPLICATION POLICY:** query metadata once before admitting a new symbol and refresh it only on
  a justified coverage check; do not repeatedly spend quota on stable metadata.
- **APPLICATION POLICY:** acquire equity/ETF EOD after initial availability and perform a bounded
  correction-aware reacquisition after the stated correction window. Acquire supported mutual-fund
  NAV once after its stated availability; never poll it throughout the day.
- **APPLICATION POLICY:** current NAV/EOD and explicit fund requests outrank broad validation and
  backfill. When any quota dimension is pressured, pause historical breadth first, then independent
  EOD validation; preserve already admitted current fund work.
- **APPLICATION POLICY:** quota exhaustion, unsupported metadata, missing NAV, changed schema,
  correction conflict, or publication failure yields `Degraded` or `Unavailable`, not stale or
  invented values.

## Repository integration status and seams

- The credential template and setup guide contain `TIINGO_ENABLED` and `TIINGO_API_TOKEN`; the
  common credential importer remains design-only.
- No Tiingo provider profile, adapter, metadata catalog, persistent four-dimensional quota ledger,
  parser, canonical NAV type, publisher, typed fund read, or frontend composition exists at the
  audit basis.
- The current generic provider-rate authority does not yet account for monthly unique symbols and
  monthly bandwidth. Extend that durable authority and SQLite state in place rather than creating a
  Tiingo-local in-memory limiter.
- Reuse existing provider onboarding, bounded extraction/raw receipts, instrument resolution,
  `ResearchObservation`, Arrow/Parquet publication, manifests, PIT selectors, and typed application
  operations.

Related maintained contracts are the [provider architecture](../../architecture/market-data-provider-architecture.md),
[research data plane](../../architecture/research-data-plane.md), and
[shipping source coverage](../source-coverage.md).

## Doctor and end-to-end acceptance

The doctor must prove, without exposing the token:

1. exact configured plan/evidence generation and authentication redaction;
2. metadata for one known equity and one intended mutual fund;
3. non-null coverage dates and exact ticker/share-class resolution;
4. one bounded latest/history response with raw/adjusted/action fields, bytes, latency, status, and
   requested/returned rows;
5. NAV-versus-traded-EOD classification and exact decimals/missing fields; and
6. all persistent quota dimensions before and after restart.

Availability requires one supported fund to complete metadata admission, raw retention,
`FundNavObservation` normalization, immutable publication, exact-cutoff typed fund read, frontend
fund-history composition, correction/revision handling, quota persistence, and restart recovery.
Equity/ETF validation repeats the gate for raw and adjusted bar identities.

## Hard gaps

- Current per-symbol support beyond the probed examples is not established by headline coverage.
- Counter reset rules, concurrency/WebSocket/batch limits, quota headers, failed-call accounting,
  and pagination are undocumented in the reviewed pages.
- Precision/rounding, immutable revision IDs, a corrections ledger, and an exact finality signal are
  not documented.
- The canonical fund NAV type, four-dimensional durable quota ledger, adapter, publication, typed
  reads, and end-to-end Funds workflow remain unimplemented.
- Unsupported mutual funds remain explicitly unavailable; no intraday NAV or guaranteed broad fund
  universe is implied.

## First-party sources

- Tiingo, [General Documentation](https://www.tiingo.com/documentation/general), accessed
  2026-08-11.
- Tiingo, [End-of-Day Stock Price API Documentation](https://www.tiingo.com/documentation/end-of-day),
  accessed 2026-08-11.
- Tiingo, [API Pricing](https://www.tiingo.com/about/pricing), accessed 2026-08-11.
- Tiingo, [API token location](https://www.tiingo.com/kb/article/where-to-find-your-tiingo-api-token/),
  accessed 2026-08-11.

## Related maintained contracts

- [Canonical schema and evidence contract](../market-data-canonical-schemas.md)
- [Provider architecture](../../architecture/market-data-provider-architecture.md)
