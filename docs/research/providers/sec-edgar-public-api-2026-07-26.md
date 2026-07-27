# SEC EDGAR public API contract — 2026-07-26

Document type: provider release-authority decision  
Audience: source, research, onboarding, and release maintainers  
Status: current official-source basis; unchanged-head runtime acceptance remains required  
Last substantive review: 2026-07-26

## Decision

The public EDGAR data APIs provide a zero-fee, no-account, no-key path for Market Squawk's SEC
submissions and company-facts ingestion.

The release profile is limited to:

- `https://data.sec.gov/submissions/CIK##########.json`; and
- `https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json`.

The SEC states that these APIs require no authentication or API key. The submissions API exposes
filing history and the company-facts API exposes normalized XBRL facts. Both must retain exact CIK,
filing, accession, source-body, receipt, publication, revision, and lineage evidence.

## Identifier assistance

Market Squawk setup must help the operator resolve a company name or ticker to a candidate CIK by
using the SEC's `company_tickers.json` or `company_tickers_exchange.json` association file. The SEC
periodically updates these files but does not guarantee their accuracy or scope, so a returned
association is lookup assistance rather than authoritative instrument identity. The selected CIK
must be validated, retained with its association evidence, and formatted as exactly 10 decimal
digits, including leading zeroes, before calling either release endpoint.

## Operating controls

1. Send a truthful declared `User-Agent` containing the application or organization and a
   monitored administrative email.
2. Enforce the SEC's aggregate maximum of 10 requests per second through the shared provider
   budget.
3. Download only the bounded records needed for the request and preserve correction, removal, and
   replacement semantics instead of overwriting prior observations.
4. Retain embedded-content provenance. The public EDGAR record does not silently transfer rights
   in third-party material included in a filing.
5. Keep the profile at `OfficialDelayed`; the APIs' dissemination timing does not make them live
   market data or execution-quality evidence.

## Release acceptance

The profile is setup-available from this code-owned evidence. V1 release acceptance remains blocked
until Market Squawk completes a real bounded response using the operator's declared contact,
immutable local publication, a nonempty DataFusion-backed query, shutdown, and exact restart
recovery. A browser fetch, fixture, or adapter construction is not that proof.

## Official sources

- [EDGAR Application Programming Interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
  — no authentication or API key; submissions and XBRL company-facts endpoints; update behavior.
- [Accessing EDGAR Data](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data)
  — free public access, 10 requests per second, declared `User-Agent`, correction/removal behavior,
  and periodically updated ticker/name/CIK association files whose accuracy and scope are not
  guaranteed.
- [SEC Webmaster Frequently Asked Questions](https://www.sec.gov/about/webmaster-frequently-asked-questions)
  — public-site reuse and excluded-content boundaries.
