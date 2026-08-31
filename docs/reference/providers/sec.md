# SEC EDGAR, XBRL, N-PORT, and N-CEN contract

SEC is the selected regulatory source for company filings, context-exact XBRL fundamentals, and
fund/investment-company reports. Its public APIs and bulk publications are separate evidence
surfaces and must remain separately versioned.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Admission | Core regulatory/fundamental/fund source |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Current repository status | Submissions, companion history, Company Facts, filing documents, XBRL parsing, and filing/fundamental publication foundations exist; bulk ZIP, company-concept/frames, N-PORT/N-CEN, fund-holdings schema, and complete Console composition remain incomplete |

## Role and product workflows

| Data family | Canonical/product use | Boundary |
| --- | --- | --- |
| Submissions and filing documents | Filing timeline, issuer/company identity evidence, amendment history, Fundamentals and Filings views | CIK/ticker helpers do not by themselves establish security identity |
| Company Facts and filing XBRL | Context-exact facts for revenue, margins, earnings, cash, debt, shares, cash flow, valuation, features, and models | Do not flatten taxonomy, unit, period, dimensions, accession, or amendment context |
| Company Concept and Frames | Bounded concept drill-down and cross-company research | Target only; frame alignment is not proof of identical fiscal periods |
| N-PORT | Fund/ETF holdings, issuer/asset/country/derivative exposure, concentration, overlap, and holdings change | Quarterly derived files may contain errors or omit filing metadata; link back to filings |
| N-CEN | Investment-company and ETF operational/reference metadata | Current derived bulk omits accepted schema 3.1 filings |
| Nightly bulk archives | Broad bootstrap and reconciliation | Bulk publication is not the same evidence as the originating filing/API response |

## Authentication and setup

**VERIFIED PROVIDER FACT:** the selected `data.sec.gov` public APIs require no API key. Automated
clients must send an identifying `User-Agent` containing an organization/name and administrative
contact. The public data APIs do not support browser CORS. See the
[EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) and
[SEC Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions).

The target credential input contains validated non-secret identity fields only:

```text
SEC_ENABLED=true
SEC_USER_AGENT_ORGANIZATION="<YOUR_NAME_OR_ORGANIZATION>"
SEC_USER_AGENT_EMAIL="<MONITORED_CONTACT_EMAIL>"
```

**APPLICATION POLICY:** the Rust provider service constructs the header, rejects malformed or
placeholder identity, and owns every SEC request. Frontend code never calls SEC directly.

## Exact surfaces and data families

| Surface | Exact locator/family |
| --- | --- |
| Current submissions | `GET https://data.sec.gov/submissions/CIK##########.json` |
| Provider-declared older submission pages | `GET https://data.sec.gov/submissions/{returned_file_name}` |
| Company Facts | `GET https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json` |
| Company Concept | `GET https://data.sec.gov/api/xbrl/companyconcept/CIK##########/{taxonomy}/{tag}.json` |
| XBRL Frames | `GET https://data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{frame}.json` |
| Filing/XBRL document | `GET https://www.sec.gov/Archives/edgar/data/{numeric_cik}/{accession_without_dashes}/{document}` |
| Bulk submissions | `https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` |
| Bulk Company Facts | `https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` |
| N-PORT quarterly datasets | [Form N-PORT Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets) and the exact quarter/readme linked there |
| N-CEN quarterly datasets | [Form N-CEN Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-cen-data-sets) and the exact quarter/readme linked there |
| Schema authority | [EDGAR Technical Specifications](https://www.sec.gov/submit-filings/technical-specifications) plus exact linked taxonomy/XSD/manual generation |

**VERIFIED PROVIDER FACT:** submission objects use zero-padded ten-digit CIKs and contain company
metadata plus at least one year or the `1,000` most recent filings, whichever is more. Returned
metadata names additional history files when present.

**VERIFIED PROVIDER FACT:** Company Facts returns all supported non-custom-taxonomy whole-entity
concepts for one filer. Company Concept narrows to one taxonomy/tag. Frames return the most recently
filed fact per entity that best fits a requested calendar frame; issuer period boundaries may still
differ.

## Provenance, clocks, revisions, and completeness

Retain at minimum:

- ten-digit CIK, stable company/security resolution evidence, accession, form, primary document,
  filing date, report date, acceptance timestamp, amendment/original state, and former names;
- taxonomy, concept/tag, unit, exact value, start/end/instant period, fiscal year/period, frame,
  dimensions/context ID where present, consolidation/restatement state, and source filing;
- raw locator, response/archive bytes and digest, conditional validators, received and first-local-
  availability times, parser/schema generation, and canonical manifest/revision;
- N-PORT/N-CEN quarter/archive, accepted filing schema, underlying filing/accession, report/filed/
  available clocks, table/row identity, extraction caveat, and derived-bulk coverage state; and
- explicit missing, omitted, invalid, amended, superseded, conflicting, or bulk-not-represented
  state.

Filing acceptance, filing date, report period, quarterly bulk cutoff, provider publication, and
local first observation are distinct clocks. Company Facts occurrences and filing amendments append
revision lineage; they are never overwritten. A missing derived N-CEN row cannot mean no filing.

## Official limits, application budgets, and scheduling

**VERIFIED PROVIDER FACT:** SEC states a maximum of `10 requests/second` for automated access.
Typical update timing is under `1 second` for submissions and under `1 minute` for XBRL, but can be
longer at peaks. Filing documents are often available `1–3 minutes` after the EDGAR timestamp, with
no guaranteed maximum. Nightly API bulk archives are generally republished around `03:00 ET`.
See the [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
and [Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions).

**VERIFIED PROVIDER FACT:** N-PORT and N-CEN derived datasets are published quarterly. Filings
after `17:30 ET` on the last business day of a quarter roll into the next posting. N-PORT can
contain registrant/extraction errors and omit filing metadata. N-CEN bulk currently omits schema
`3.1`, although EDGAR accepts that schema. See the [N-PORT datasets](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets),
[N-CEN datasets](https://www.sec.gov/data-research/sec-markets-data/form-n-cen-data-sets), and
[technical specifications](https://www.sec.gov/submit-filings/technical-specifications).

**APPLICATION POLICY:** the maintained target is one app-wide SEC queue at `2 requests/second`,
with broad bootstraps and reconciliation using official bulk files before per-CIK calls. Track
attempts, bytes, status, latency, cache validators, returned filings/facts, companion files,
partial parses, and provider messages.

**APPLICATION POLICY:** the audit-anchor built-in profile currently admits `8 requests/second` and
`4` concurrent requests. That older code-owned policy must be reconciled down to the maintained
`2 requests/second` target before this contract is accepted; neither value changes the provider's
official `10 requests/second` ceiling.

**APPLICATION POLICY:** schedule tracked-CIK submissions/Company Facts incrementally, nightly bulk
bootstrap/reconciliation after the publication window, and N-PORT/N-CEN by exact quarter release.
Do not make one request per concept when Company Facts satisfies the workload. Backfill yields to
current filing, interactive, and recovery work.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** on 2026-08-11, a bounded request using the configured truthful
`User-Agent` returned HTTP `200` for Apple submissions with `1,001` recent filing entries and one
declared historical companion file. Apple Company Facts returned HTTP `200` with `503` `us-gaap`
concepts.

These are credential/shape observations, not throughput, completeness, publication-lag, issuer-
universe, or future-schema guarantees. No frozen runtime receipt proves Company Concept, Frames,
bulk ZIP reconciliation, N-PORT, N-CEN, or the target fund-holdings product path.

## Canonical storage and point-in-time destination

```text
submissions / filings / XBRL / fund archives
    -> exact raw representation + provider-native typed record
    -> research_observations / company identity / fund_holdings
    -> immutable Parquet generation + manifest
    -> PIT fundamentals, filings, funds, features, models, and recommendations
```

| Source family | Canonical destination |
| --- | --- |
| Filing metadata | Existing filing kind in `market_squawk.research_observations` |
| Company Facts and filing XBRL | Existing context-exact fundamental kind in `market_squawk.research_observations` |
| Company metadata/CIK associations | Existing company-identity observation and governed provider-identity resolution |
| N-PORT holdings | Target `market_squawk.fund_holdings` with fund/share-class, filing/report, security/issuer, quantity/value/currency/percentage, classifications, derivatives, and omitted/confidential/missing state |
| N-CEN metadata | Target typed investment-company/fund metadata linked to exact filing/schema and fund identity |

PIT selection uses evidence available at the decision cutoff, including acceptance/availability and
amendment lineage. A later amended filing, corrected bulk archive, or new taxonomy creates a new
generation; it cannot rewrite an earlier model, backtest, valuation, or recommendation input.

## Repository integration status and seams

Repository evidence at the audit basis shows:

- [`market-squawk-adapter-sec`](../../../adapters/market-squawk-adapter-sec/src/lib.rs) implements
  bounded current submissions, provider-declared companion pages, Company Facts, filing-document
  retrieval, XBRL/Inline-XBRL parsing, raw evidence, representation tracking, and point-in-time
  normalization;
- [`contracts.rs`](../../../adapters/market-squawk-adapter-sec/src/client/contracts.rs) owns strict
  CIK/accession/document locators and the selected current endpoint families;
- [`normalize.rs`](../../../adapters/market-squawk-adapter-sec/src/normalize.rs) emits canonical
  filing and Company Facts observations with conservative availability and append-only revision
  order;
- [`provider_activation/mod.rs`](../../../apps/market-squawk/src/provider_activation/mod.rs)
  activates the existing `sec.edgar-public` surface through the shared source, budget, identity,
  raw-store, and representation authorities; and
- the release/provider path exercises distinct submissions and Company Facts publications and
  validates canonical filing/fundamental rows in
  [`release/providers.rs`](../../../apps/market-squawk/src/release/providers.rs).

The current locator set does not implement Company Concept, Frames, nightly bulk ZIPs, N-PORT, or
N-CEN. No `market_squawk.fund_holdings` schema exists yet, and the target credential-file importer
is not implemented. Existing SEC foundations must be extended in place; a second adapter, schema
registry, raw store, or quota authority would be duplication.

## Doctor and end-to-end acceptance gates

SEC becomes Available for each exact workflow only after:

1. The configured organization/contact validates and the doctor sends the constructed identifying
   header without exposing it as a secret or accepting placeholders.
2. A bounded submissions request follows every provider-declared companion needed for the selected
   range and proves CIK, accession uniqueness, requested/returned filing counts, clocks, and
   terminal completeness.
3. Company Facts and selected filing XBRL parse under frozen schemas, retain every context/unit/
   period/accession coordinate, and publish no unresolved mandatory instrument identity.
4. Broad bootstrap and reconciliation prove complete archive bytes, safe extraction, exact
   manifests, and row/page/archive closure under the shared `2 requests/second` target.
5. N-PORT/N-CEN jobs bind the exact quarter, readme/layout/XSD generation, underlying filing, and
   declared coverage exclusions before fund rows publish.
6. Raw objects and canonical observations publish atomically; PIT queries reproduce the expected
   filing/fact/holding state at a historical cutoff.
7. Fundamentals, Filings, Funds/ETFs, valuation, and model reads consume bounded typed operations
   and preserve unavailable/conflict states.
8. Restart reopens the exact manifests and typed results, while 429, changed schema, partial bulk,
   missing identity, or delayed publication degrades only the affected SEC workflow.

## Hard gaps

- Current ticker/CIK helper files are discovery aids and are not guaranteed accurate or complete;
  complete issuer-security lifecycle identity remains external.
- SEC publishes no uptime target, guaranteed maximum filing/API lag, endpoint-specific retry
  schedule, or checksum manifest for every JSON response.
- Company Concept, Frames, bulk ZIP reconciliation, N-PORT, and N-CEN are not implemented in the
  current adapter.
- N-CEN quarterly bulk omits accepted schema 3.1 filings; full filing and derived-bulk coverage
  must remain separate.
- N-PORT/N-CEN derived datasets can lag, omit metadata, or contain source/extraction errors and
  therefore cannot replace complete filings.
- XBRL taxonomy/context diversity and issuer-specific custom facts prevent a rigid universal
  statement mapping without explicit derivation and missingness evidence.

## First-party sources

- [EDGAR Application Programming Interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
- [SEC Webmaster Frequently Asked Questions](https://www.sec.gov/about/webmaster-frequently-asked-questions)
- [Form N-PORT Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-port-data-sets)
- [Form N-CEN Data Sets](https://www.sec.gov/data-research/sec-markets-data/form-n-cen-data-sets)
- [EDGAR Technical Specifications](https://www.sec.gov/submit-filings/technical-specifications)
- [SEC Developer Resources](https://www.sec.gov/about/developer-resources)

Related Market Squawk authorities: [provider architecture](../../architecture/market-data-provider-architecture.md),
[canonical schema and evidence contract](../market-data-canonical-schemas.md),
[shipping source coverage](../source-coverage.md), and the
[provider credential template](../market-squawk-provider-credentials.env.example).
