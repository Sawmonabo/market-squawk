# Official Documentation Batch 003: FRED Time, BLS, Treasury, and Authorization Standards


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [BLS Rights Correction](#bls-rights-correction)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews FRED/ALFRED vintage semantics; BLS v1/v2 access, registration, request, and terms;
Treasury XML/Fiscal Data; and five IETF authorization standards. RFCs are classified as official
standards documentation, not academic papers. No provider action or live API request was performed.

## Sources Reviewed

| ID | First-class source | Evidence role |
| --- | --- | --- |
| DOC-FRED-RT-001 | [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html) | Vintage and point-in-time semantics |
| DOC-026 | [BLS API FAQ](https://www.bls.gov/developers/api_FAQs.htm) | v1/v2 quotas, registration, renewal, and `429` |
| DOC-027 | [BLS registration](https://data.bls.gov/registrationEngine/) | Email/organization, CAPTCHA, terms, human boundary |
| DOC-028 | [BLS v2 request signatures](https://www.bls.gov/developers/api_signature_v2.htm) | `registrationkey` request contract |
| DOC-029 | [BLS API terms](https://www.bls.gov/developers/termsOfService.htm) | Secondary use, attribution, disclaimer, representation, limits |
| DOC-030 | [Treasury daily-rate XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | Anonymous XML/OData mechanics |
| DOC-031 | [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/) | Anonymous API and scoped reuse terms |
| PAPER-001 | [RFC 8252](https://www.rfc-editor.org/info/rfc8252/) | Native external-browser and PKCE profile |
| PAPER-002 | [RFC 8628](https://www.rfc-editor.org/info/rfc8628/) | Device flow, human authorization, polling, expiry |
| PAPER-003 | [RFC 7591](https://www.rfc-editor.org/info/rfc7591/) | Dynamic client registration and prerequisites |
| PAPER-004 | [RFC 7592](https://www.rfc-editor.org/info/rfc7592/) | Registration-management lifecycle |
| PAPER-005 | [RFC 8414](https://www.rfc-editor.org/info/rfc8414/) | Issuer/endpoints/capability metadata |

## Findings

1. **Confirmed:** FRED/ALFRED real-time periods encode when values were known and are necessary for
   point-in-time research. They do not cure the separate FRED rights gate.
   [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
2. **Confirmed:** BLS v1 is an unregistered, lower-limit path; v2 is a registered higher-limit path.
   Registration collects human-provided details, includes CAPTCHA/terms, delivers a key by email,
   and is renewed at least annually. The v2 request schema includes `registrationkey`.
   [BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
   [registration](https://data.bls.gov/registrationEngine/),
   [v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm)
3. **Confirmed:** BLS terms contain affirmative secondary-use language: data accessed through
   BLS.gov generally should not carry end-use controls. The same terms require access-date citation,
   a BLS quality/timeliness disclaimer, truthful representation, logo restrictions, compliance with
   limits, and respect for third-party rights. [BLS terms](https://www.bls.gov/developers/termsOfService.htm)
4. **Inference:** BLS can be implemented as a scoped, documentation-ready source rather than being
   blocked by an asserted absence of any durable-use evidence. Activation must still bind exact BLS
   provenance and duties, reject out-of-scope/third-party material not admitted by the record, and
   wait for bounded runtime evidence.
5. **Confirmed:** Treasury XML is anonymously documented but the selected page does not provide a
   feed-specific durable-use grant. Fiscal Data explicitly documents no account/token and broad
   reuse for the exact API/dataset provenance, subject to dataset-specific exceptions.
   [Treasury XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed),
   [Fiscal Data](https://fiscaldata.treasury.gov/api-documentation/)
6. **Confirmed:** native OAuth requires an external user-agent and PKCE; device authorization retains
   a human approval boundary and bounded polling; metadata and DCR describe capabilities but do not
   create provider support, client eligibility, or a provider user account.
   [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252),
   [RFC 8628](https://www.rfc-editor.org/rfc/rfc8628),
   [RFC 7591](https://www.rfc-editor.org/rfc/rfc7591),
   [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414)
7. **Confirmed:** RFC 7592 lifecycle operations are available only when the authorization server
   actually implements and authorizes that protocol. **Inference:** a mutating timeout is an
   indeterminate remote state, not proof of failure or success.
   [RFC 7592](https://www.rfc-editor.org/rfc/rfc7592)

## BLS Rights Correction

The prior synthesis incorrectly characterized BLS as having no affirmative durable-use evidence.
The complete evidence is two-sided:

- **Confirmed permission signal:** the official terms say BLS.gov data generally should not include
  end-use controls.
- **Confirmed obligations:** record access date, display the required quality/timeliness disclaimer,
  avoid false representation and unauthorized logo use, obey rate/access limits, and respect any
  third-party intellectual-property rights.
- **Engineering inference:** admit only the exact BLS data surface and intended operation set covered
  by a versioned terms record. Broader redistribution, bundled third-party content, or a materially
  different product use must trigger refresh/qualified review rather than inherit a blanket grant.

This supports scoped implementation and runtime verification for both v1 and v2. The v2 key changes
quota/features; it neither enlarges nor reduces the underlying data-use terms.

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Release effect |
| --- | --- | --- | --- | --- |
| FRED vintages preserve availability semantics | DOC-FRED-RT-001 | Confirmed fact | High | Bind point-in-time source revisions |
| BLS v1/v2 have distinct limits and human boundaries | DOC-026, DOC-027, DOC-028 | Confirmed fact | High | Separate capability records and renewal state |
| BLS supplies affirmative secondary-use language and explicit duties | DOC-029 | Confirmed fact | High | Scoped rights admission, not blanket rights absence |
| BLS release state remains scope- and runtime-conditioned | DOC-026, DOC-029 | Engineering inference | Medium-high | Documentation-ready after binding duties; smoke pending |
| Treasury XML and Fiscal Data have different rights evidence | DOC-030, DOC-031 | Confirmed fact | High | Never inherit Fiscal Data terms into XML |
| OAuth standards do not prove provider support | PAPER-001 through PAPER-005 | Confirmed standards boundary | High | Fail closed unless exact provider capability is admitted |

## Limitations and Non-Findings

- This is engineering evidence analysis and does not determine legal rights or obligations.
- No BLS runtime, registration, key email, or annual renewal was exercised.
- No selected source establishes that every third-party datum reachable through a government site is
  Government-authored or covered by the same terms.
- No current mandatory Market Squawk provider is proved to support device flow or DCR solely because
  the RFC exists.

## Source List

DOC-FRED-RT-001, DOC-026 through DOC-031, and PAPER-001 through PAPER-005 are registered in
`source-inventory.json` and assigned to `docs-batch-003` with access and digest/reference metadata.
