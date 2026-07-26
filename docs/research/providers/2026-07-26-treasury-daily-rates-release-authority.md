# Treasury daily rates release authority — 2026-07-26

Treasury daily interest rates are a mandatory Market Squawk V1 research capability. This record
establishes the current official source, coverage, durable-use basis, code gap, and release proof
required before the capability can be called complete.

| Field | Value |
| --- | --- |
| Document type | Provider source and rights decision |
| Audience | Treasury adapter, research ingestion, onboarding, and release owners |
| Status | Approved source basis and implemented product path; exact-head external acceptance remains required |
| Researched on | 2026-07-26 |
| Provider | U.S. Department of the Treasury, Office of Debt Management |
| Cost/account | Public HTTPS feed; no account, key, subscription, or paid service |
| Implementation | `50912c18271a0389fb5ac8817555230930dd0506` |
| Supersedes | The 2026-07-25 conclusion that daily-rate XML durable use remained unestablished |

## Contents

- [Decision](#decision)
- [Official source and coverage](#official-source-and-coverage)
- [Durable-use authority](#durable-use-authority)
- [Implemented authority boundary](#implemented-authority-boundary)
- [Required release evidence](#required-release-evidence)
- [Sources](#sources)

## Decision

Market Squawk V1 must retrieve, validate, normalize, persist, query, and recover all five official
Treasury daily-rate families:

1. Daily Treasury par yield curve rates.
2. Daily Treasury bill rates.
3. Daily Treasury long-term rates.
4. Daily Treasury real par yield curve rates.
5. Daily Treasury real long-term rates.

These are official delayed research observations. They are not live execution data and cannot be
promoted to `DirectVerified` or treated as Level 1 fair-value evidence solely because Treasury
published them.

The release must not accept a source-only onboarding session as proof of this capability. It must
prove a working research adapter, durable local publication, queryability, provenance, and restart
recovery for every family.

## Official source and coverage

Treasury documents one HTTPS XML endpoint with a required `data` family and a year, month, or
all-history selector. The all-history form is zero-indexed and returns 300 rows per complete page;
Treasury instructs developers to increment page numbers until the feed contains no Atom entries.
The adapter therefore treats an empty feed as the terminal response, rejects a malformed empty
`<entry/>`, and verifies cross-page request order, payload and row uniqueness, ascending dates,
page-size progression, and whole-query resource limits.

The official start years are:

| Family | Provider key | Available from |
| --- | --- | ---: |
| Nominal par yield curve | `daily_treasury_yield_curve` | 1990 |
| Bill rates | `daily_treasury_bill_rates` | 2002 |
| Long-term rates | `daily_treasury_long_term_rate` | 2000 |
| Real par yield curve | `daily_treasury_real_yield_curve` | 2003 |
| Real long-term rates | `daily_treasury_real_long_term` | 2000 |

The current endpoint was inspected on 2026-07-26 with bounded January 2026 month requests for all
five provider keys. The returned OData/Atom structures confirmed distinct schemas:

- nominal curves publish a date and nominal constant-maturity fields;
- bill rates publish bank-discount and coupon-equivalent rates, maturity dates, and CUSIPs;
- long-term rates publish multiple typed rate rows per quote date;
- real curves publish 5-, 7-, 10-, 20-, and 30-year real yields; and
- real long-term rates publish one rate per quote date.

Those observations establish current schema research, not final application acceptance. The
release producer must still acquire and validate fresh official bodies through the shipping
adapter.

## Durable-use authority

The federal Data.gov catalog identifies the matching Treasury Office of Debt Management datasets
as public-access datasets under CC0 1.0. The catalog contains matching records for nominal yield
curves, Treasury bills, real yield curves, long-term rates, and real long-term rates.

This official catalog evidence corrects the prior research conclusion that the XML surface lacked
durable-use authority. The catalog records identify the same Treasury rate families exposed by the
documented XML feed and supply the missing dataset-level license.

CC0 covers copying, adapting, extracting, reusing, and redistributing covered data for any purpose.
In addition, 17 U.S.C. § 105 generally excludes United States Government works from domestic
copyright protection, and 17 U.S.C. § 102(b) excludes facts and methods from copyright protection.

Market Squawk may therefore admit these exact Treasury rate datasets for:

- retrieval and local display;
- durable raw and normalized storage;
- historical research and point-in-time datasets;
- analytics, backtesting, and model training;
- controlled local export; and
- derived research outputs.

The admission applies to the Treasury rate datasets, not Treasury seals, trademarks, unrelated
website media, or third-party material outside the identified datasets. Market Squawk must retain
the official dataset identity, retrieval URL, retrieval time, payload digest, provider record
identity, and publication/effective-time evidence.

## Implemented authority boundary

The production path now includes:

1. A closed `TreasuryDailyRateFamily` type for all five official families, provider keys, start
   years, typed schemas, and stable dataset and series identities.
2. Exact year, month, and bounded all-history requests through the allowlisted Treasury endpoint.
3. Strict namespace-aware parsing with checked decimals, null semantics, dates, maturities, rate
   types, CUSIPs, duplicate rejection, and cross-page integrity tracking.
4. Canonical `OfficialDelayed` macro observations carrying exact payload, source-record,
   publication, availability, ingestion, revision, and lineage evidence.
5. A no-credential portal form that selects an inclusive year range and activates every family
   available within that range as one durable research runtime.
6. Discovery, extraction, Arrow/Parquet publication, DataFusion-backed query, and restart recovery
   through the same application services used by CLI and MCP.
7. A release producer that retrieves, publishes, queries, and recovers one configured common year
   for every family, plus a closer that recomputes the exact family, dataset, request, object, and
   payload bindings rather than trusting report labels.

This implementation does not close the external acceptance predicate by itself. The unchanged
release candidate must still retrieve fresh official bodies through the shipping adapter and
publish the resulting exact-head evidence.

## Required release evidence

Task 19A and Task 20 may close this capability only when one unchanged release candidate proves:

- the official profile and CC0 evidence are current and digest-bound;
- the portal activates the concrete Treasury daily research runtime;
- the shipping adapter successfully retrieves an official response for every family;
- every response passes its family-specific parser and normalization contract;
- at least one observation from every family is durably published and queryable;
- raw payload, normalized record, dataset manifest, and lineage identities agree;
- restart recovery restores the same active source generation and published datasets;
- no source-only session, fixture, synthetic response, or metadata declaration substitutes for the
  official acceptance trace; and
- the terminal closer rejects missing families, missing persistence, missing runtime evidence, or
  failed recovery.

Focused verification should remain consolidated: one grouped Treasury adapter harness covering the
five schemas, one existing onboarding/application harness for activation and recovery, and one
existing release-closing harness for the terminal predicates. No separate per-family test
executables or documentation tests are required.

## Sources

- [Treasury Daily Interest Rate XML Feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
  — official request grammar, provider keys, pagination, schemas, and availability.
- [Treasury XML developer notice](https://home.treasury.gov/developer-notice-xml-changes) —
  current feed migration, developer-consumption guidance, schema changes, and historical-download
  behavior.
- [Daily Treasury rate archives](https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives)
  — official historical CSV/XML availability.
- [Data.gov: Daily Treasury yield curve rates](https://catalog.data.gov/dataset/interest-rate-statistics-daily-treasury-yield-curve-rates)
  — public access, Treasury publisher, daily frequency, and CC0.
- [Data.gov: Daily Treasury bill rates](https://catalog.data.gov/dataset/interest-rate-statistics-daily-treasury-bill-rates)
  — public access, Treasury publisher, daily frequency, and CC0.
- [Data.gov: Daily Treasury real yield curve rates](https://catalog.data.gov/dataset/daily-treasury-real-yield-curve-rates)
  — public access, Treasury publisher, daily frequency, and CC0.
- [Data.gov: Daily Treasury long-term rate data](https://catalog.data.gov/dataset/daily-treasury-long-term-rate-data)
  — public access, Treasury publisher, daily frequency, and CC0.
- [Data.gov: Daily Treasury real long-term rates](https://catalog.data.gov/dataset/daily-treasury-real-long-term-rates)
  — public access, Treasury publisher, daily frequency, and CC0.
- [CC0 1.0 legal code](https://creativecommons.org/publicdomain/zero/1.0/legalcode) — reuse,
  extraction, database-rights, and public-license fallback terms.
- [17 U.S.C. § 105](https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title17-section105&num=0&edition=prelim)
  — United States Government works.
- [17 U.S.C. § 102](https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title17-section102&num=0&edition=prelim)
  — copyright subject matter and the facts/methods boundary.
