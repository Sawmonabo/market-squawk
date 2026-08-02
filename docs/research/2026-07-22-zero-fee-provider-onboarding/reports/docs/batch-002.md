# Official Documentation Batch 002: Kraken, SEC, and FRED/ALFRED


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews the assigned Kraken operational sources, SEC EDGAR documentation, and FRED/
ALFRED account, authentication, error, and legal sources as of 2026-07-22. No live endpoint,
credential, account, or provider-controlled action was exercised.

## Sources Reviewed

| ID | First-class source | Evidence role |
| --- | --- | --- |
| DOC-013 | [Kraken Spot key creation](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key) | Human creation and restrictions |
| DOC-015 | [Kraken CLI](https://docs.kraken.com/home/cli) | Public/paper no-key and private-key consumption |
| DOC-016 | [Kraken API rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-) | Public/private rate dimensions |
| DOC-017 | [Kraken API-key security](https://support.kraken.com/articles/api-key-security) | Minimum permissions and lifecycle guidance |
| DOC-018 | [Kraken developer index](https://docs.kraken.com/llms.txt) | Exchange-versus-Embed product boundary |
| DOC-019 | [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Anonymous submissions/XBRL and bulk access |
| DOC-020 | [SEC webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | Declared identity, 10 requests/second, scoped reuse |
| DOC-021 | [FRED v1 API keys](https://fred.stlouisfed.org/docs/api/api_key.html) | Account/key prerequisite and query transport |
| DOC-022 | [FRED v2 API keys](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html) | Bearer transport |
| DOC-023 | [FRED account registration](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/) | Free account and human web step |
| DOC-024 | [FRED API errors](https://fred.stlouisfed.org/docs/api/fred/errors.html) | Missing/invalid key and `429` semantics |
| DOC-025 | [FRED legal terms](https://fred.stlouisfed.org/legal/) | Storage, database, modeling, and third-party rights gate |

## Findings

1. **Confirmed:** Kraken private keys are created through a provider-controlled human workflow;
   public and paper commands remain usable without them. Key permissions, restrictions, optional
   2FA, storage, replacement, and deletion are product-specific.
   [Key creation](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key),
   [CLI](https://docs.kraken.com/home/cli),
   [key security](https://support.kraken.com/articles/api-key-security)
2. **Confirmed:** Kraken documents independent public, private, and trading counters. The developer
   index also distinguishes retail Exchange from partner-oriented Embed surfaces.
   [Rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-),
   [developer index](https://docs.kraken.com/llms.txt)
3. **Confirmed:** SEC EDGAR submissions and XBRL data are publicly reachable without a credential.
   Automated clients must declare identity/contact and stay at or below the current aggregate
   10-requests-per-second ceiling. Scoped Government-created EDGAR content is described as free to
   access and reuse. [SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
   [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
4. **Confirmed:** FRED v1 and v2 require a key linked to a FRED account; the account is described as
   free, registration remains a human web step, and v2 uses Bearer transport. A read response can
   establish request acceptance but not rights or a complete key-introspection result.
   [v1 keys](https://fred.stlouisfed.org/docs/api/api_key.html),
   [v2 keys](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html),
   [registration](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/),
   [errors](https://fred.stlouisfed.org/docs/api/fred/errors.html)
5. **Confirmed:** current FRED terms materially conflict with mandatory Market Squawk persistence,
   database, and modeling use and leave third-party-series rights responsibility with the user.
   **Inference:** successful authentication must never bypass a qualified, scope-specific rights
   decision. [FRED legal terms](https://fred.stlouisfed.org/legal/)

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Release effect |
| --- | --- | --- | --- | --- |
| Kraken key creation is human-resumed | DOC-013, DOC-015, DOC-017 | Confirmed fact | High | Manual import, exact permission verification |
| Kraken limit and product surfaces differ | DOC-016, DOC-018 | Confirmed fact | High | Separate counters and capability records |
| SEC EDGAR is anonymous with declared identity and aggregate ceiling | DOC-019, DOC-020 | Confirmed fact | High | Documentation-ready; runtime smoke pending |
| FRED account is free but API use requires a key | DOC-021, DOC-022, DOC-023 | Confirmed fact | High | Human-resumed import is technically possible |
| Key acceptance is not a FRED rights grant | DOC-024, DOC-025 | Fact plus engineering inference | High | Hard rights gate before persistence/modeling/export |

## Limitations and Non-Findings

- Kraken private account cost, jurisdiction, eligibility, and durable-use rights were not proved.
- SEC availability descriptions are not service-level guarantees; runtime health remains untested.
- FRED publishes `429` behavior but the reviewed pages do not establish one universal numeric quota.
- No reviewed FRED source supplies an automated account-registration or key-issuance interface.

## Source List

DOC-013, DOC-015 through DOC-025 are registered in `source-inventory.json` and assigned to
`docs-batch-002`; their access state and digest/reference are recorded there.
