# Treasury daily rates release authority — 2026-07-26

Treasury daily interest rates are a mandatory Market Squawk V1 research capability. This record
establishes the current official source, coverage, durable-use basis, code gap, and release proof
required before the capability can be called complete.

| Field | Value |
| --- | --- |
| Document type | Provider source and rights decision |
| Audience | Treasury adapter, research ingestion, onboarding, and release owners |
| Status | Approved source basis; implementation and exact-head acceptance remain required |
| Researched on | 2026-07-26 |
| Provider | U.S. Department of the Treasury, Office of Debt Management |
| Cost/account | Public HTTPS feed; no account, key, subscription, or paid service |
| Supersedes | The 2026-07-25 conclusion that daily-rate XML durable use remained unestablished |

## Contents

- [Decision](#decision)
- [Official source and coverage](#official-source-and-coverage)
- [Durable-use authority](#durable-use-authority)
- [Current implementation gap](#current-implementation-gap)
- [Required implementation](#required-implementation)
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
all-history selector. The all-history form is zero-indexed and paginated; Treasury instructs
developers to continue until a page contains no Atom entries.

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

## Current implementation gap

The repository currently implements only the nominal daily par-yield-curve family:

- `TreasurySourceConfig` has a single daily XML variant, `DailyParYieldCurve`.
- `TreasuryYieldCurveProfile` fixes the provider key to `daily_treasury_yield_curve`.
- the parser and normalizer understand only nominal curve maturity fields;
- the onboarding portal submits `treasury.daily-rates-xml` as a source-only session; and
- terminal provider validation performs no Treasury daily-runtime or persistence check.

The existing nominal parser, bounded HTTP client, research-source contract, lineage handling, and
publication coordinator are reusable foundations. They do not prove complete Treasury daily-rate
coverage.

## Required implementation

The production adapter must add:

1. A closed `TreasuryDailyRateFamily` type covering all five provider keys, schemas, official start
   years, and stable dataset/series identities.
2. Bounded year, month, and all-history requests with exact allowlisted URLs, zero-based pagination,
   terminal-page detection, cancellation, timeouts, body limits, and source health.
3. Strict namespace-aware parsers for all five schemas, including provider null semantics,
   checked decimals, dates, maturities, rate types, CUSIPs, duplicate detection, and unknown-field
   policy.
4. Canonical macro observations with explicit units, effective date, published/available/ingested
   times, source record identity, exact payload digest, quality, rights evidence, and revision
   handling.
5. Discovery and extraction across every selected family and requested time range.
6. Portal configuration that activates a real Treasury daily research adapter. The default V1
   setup selects all five families and a bounded historical range; advanced users may narrow the
   family set or range.
7. Durable publication through the existing Arrow/Parquet research coordinator, manifest and
   lineage authority, deduplication, point-in-time query path, and restart recovery.
8. Updated source coverage, health, doctor, CLI, MCP, and reference output that reports the exact
   admitted families and time coverage.

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
