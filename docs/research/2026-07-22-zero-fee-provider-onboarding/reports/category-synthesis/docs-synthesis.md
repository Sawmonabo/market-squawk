# Official Documentation Synthesis


## Table of Contents

- [Category Scope](#category-scope)
- [Sources Covered](#sources-covered)
- [High-Confidence Findings](#high-confidence-findings)
- [Medium- and Low-Confidence Findings](#medium--and-low-confidence-findings)
- [Conflicts and Disagreements](#conflicts-and-disagreements)
- [Trends and Patterns](#trends-and-patterns)
- [Implications for Market Squawk](#implications-for-market-squawk)
- [Gaps](#gaps)
- [Source Matrix](#source-matrix)

## Category Scope

This synthesis merges `docs-batch-001` through `docs-batch-004`: 46 first-class official
provider/government, IETF, and operating-system sources accessed as of 2026-07-22. Provider facts
come from official provider/government pages; OAuth behavior comes from RFCs; native-store behavior
comes from platform documentation. No runtime or provider state was changed.

## Sources Covered

- Coinbase: DOC-001 through DOC-010.
- Kraken: DOC-011 through DOC-018 and canonical DOC-014.
- SEC/FRED/BLS/Treasury: DOC-019 through DOC-031 and DOC-FRED-RT-001.
- OAuth standards: PAPER-001 through PAPER-007, reclassified as official documentation because
  RFCs are standards rather than academic papers.
- Native stores: DOC-032 through DOC-038.

All are assigned in `source-inventory.json`; mutable responses carry a digest or an explicit stable
revision/reference and refresh requirement.

## High-Confidence Findings

1. Coinbase and Kraken public market data are distinct no-key surfaces; private own-account data
   requires user-created authority. [Coinbase](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api),
   [Kraken](https://docs.kraken.com/exchange/guides/overview)
2. Private exchange activation can verify observed authority, but provider-controlled creation,
   consent, MFA/2FA, restrictions, and one-time secret handling remain human boundaries.
   [Coinbase permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions),
   [Kraken key info](https://docs.kraken.com/api/docs/rest-api/get-api-key-info)
3. SEC EDGAR has a documented anonymous path with declared client identity/contact, a current
   aggregate 10 requests/second ceiling, and scoped public-content reuse evidence.
   [SEC API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
   [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions)
4. FRED account/key mechanics are technically supportable, but current terms conflict with required
   persistence/database/modeling behavior and retain third-party-series obligations. Authentication
   never grants those rights. [FRED terms](https://fred.stlouisfed.org/legal/)
5. BLS v1/v2 have distinct quota and human-registration boundaries. BLS terms also provide
   affirmative secondary-use language together with access-date citation, disclaimer, truthful-
   representation, rate, and third-party-rights duties. [BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
   [BLS terms](https://www.bls.gov/developers/termsOfService.htm)
6. Treasury XML and Fiscal Data are separate surfaces. Only Fiscal Data's reviewed documentation
   supplies broad explicit reuse evidence for its exact API/dataset provenance.
   [Treasury XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
   [Fiscal Data](https://fiscaldata.treasury.gov/api-documentation/)
7. OAuth standards define profiles, not provider capabilities. Native browser/PKCE, device flow,
   metadata, DCR, registration management, security, and revocation remain disabled unless the exact
   provider admits them. [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252),
   [RFC 9700](https://www.rfc-editor.org/rfc/rfc9700)
8. Apple, Windows, and Secret Service storage surfaces have materially different selectors, access,
   prompt, persistence, deletion, and failure semantics.

## Medium- and Low-Confidence Findings

- **Medium confidence / engineering inference:** BLS can enter a scoped documentation-ready state
  when the rights record binds exact BLS provenance and duties. This is not a blanket grant for
  third-party content or every redistribution/product use.
- **Medium confidence:** exchange durable-use and private-cost/eligibility evidence remains
  incomplete; no denial or universal permission is inferred.
- **Low confidence / not admitted:** current mandatory providers' generally available device-flow or
  DCR support. Standards availability is not provider evidence.
- **Not admitted:** runtime availability, latency, quota behavior, credential verification, or OS
  store behavior; none was exercised.

## Conflicts and Disagreements

- BLS older examples can omit a v2 key while current FAQ/registration evidence requires registered
  v2 use. Require the key and treat anonymous v2 as unknown.
- BLS affirmative secondary-use language coexists with attribution/disclaimer, representation,
  limits, and third-party-rights obligations. Preserve both sides.
- Fiscal Data says no token is required while a `403` description mentions an invalid key. Do not
  invent a credential; verify anonymous runtime behavior.
- SEC access/update timing descriptions differ by surface and are not SLAs.
- Coinbase Exchange rotation is product-scoped and cannot define all Coinbase App key lifecycle.
- Windows Credential Manager persistence and DPAPI machine scope are separate concepts.

## Trends and Patterns

- Access, price, authentication, rate, rights, and runtime evidence are independent dimensions.
- Human-controlled provider actions recur even when the portal can remove search/configuration toil.
- Provider metadata can narrow a code-owned capability; it cannot create new authority.
- Store/catalog and remote/local lifecycle steps have no portable atomic transaction.

## Implications for Market Squawk

Use one versioned capability record per provider surface with setup mode, human boundary, minimum
and maximum accepted authority, verifier, rate dimensions, rights decision, lifecycle support,
source IDs/digests, and refresh trigger. Activation is `ActiveScoped` only when every required gate
passes. This is an engineering synthesis, not a claim that the cited sources define Market Squawk's
internal type names.

BLS correction: both tiers may proceed to scoped implementation and separately authorized runtime
smoke when terms duties are bound. The v2 key changes quota/features, not the underlying rights
record. FRED remains hard-blocked for persistence/modeling/export pending qualified resolution.

## Gaps

- Authorized runtime smokes for every claimed provider surface.
- Exact exchange durable-use rights and private cost/eligibility.
- Treasury XML feed-specific rights.
- OS-store behavior on each claimed platform.
- Qualified FRED/series-owner decision.
- Exact provider OAuth/device/DCR support if later requested.

## Source Matrix

| Batch | Sources | Primary decision area |
| --- | --- | --- |
| docs-batch-001 | DOC-001–DOC-012 | Coinbase and initial Kraken boundaries |
| docs-batch-002 | DOC-013, DOC-015–DOC-025 | Kraken, SEC, FRED |
| docs-batch-003 | DOC-FRED-RT-001, DOC-026–DOC-031, PAPER-001–PAPER-005 | FRED time, BLS, Treasury, authorization standards |
| docs-batch-004 | PAPER-006–PAPER-007, DOC-032–DOC-038, DOC-014 | OAuth security/revocation, native stores, Kraken verification |
