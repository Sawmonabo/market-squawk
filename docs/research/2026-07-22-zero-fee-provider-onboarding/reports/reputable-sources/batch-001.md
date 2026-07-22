# Reputable Sources Batch 001: Authorization, Secret Lifecycle, Audit, and Rights


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews six selected vendor-neutral or public-sector sources as of 2026-07-22. These
sources support cross-provider engineering controls; provider documentation and IETF standards
remain authoritative for provider capability and protocol semantics.

## Sources Reviewed

| ID | Organization and source | Credibility rationale | Principal evidence | Limitation |
| --- | --- | --- | --- | --- |
| REP-001 | [OWASP OAuth2 Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/OAuth2_Cheat_Sheet.html) | OWASP-maintained, vendor-neutral implementation guidance tied to current OAuth BCPs | Authorization Code with PKCE and transaction binding | Non-normative; does not establish provider support |
| REP-002 | [UK NCSC API authentication and authorisation](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation) | National cyber-security agency; version 1.0, published/reviewed 2025-04-03 | Provider-issued temporary credentials, secure storage, least privilege, default deny, per-request checks | Advisory; not a provider contract |
| REP-003 | [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html) | Final U.S. government digital-identity publication | Authentication intent, authenticator lifecycle/invalidation, protected channels | API keys are not automatically NIST authenticators; no compliance claim |
| REP-004 | [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) | OWASP lifecycle and incident-response guidance | Creation, rotation, revocation, expiration, least privilege, non-secret audit | Cloud examples are contextual; no mandatory external secret service |
| REP-007 | [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html) | OWASP security-logging guidance | Event identity/time/result with access tokens, passwords, keys, and primary secrets excluded | Retention/legal policy remains local/jurisdictional |
| REP-009 | [NIST Research Data Framework 2.0](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html) | Final NIST research-data governance framework with DOI and named authors | Terms, licenses, permissions, purpose/duration, security, and citation obligations | Governance framework, not provider-specific legal advice |

## Findings

1. **Confirmed guidance:** a delegated application should use provider-issued credentials rather
   than collect provider passwords; permissions should be least-privilege, deny-by-default, and
   verified for the operation. [NCSC](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation),
   [OWASP OAuth2](https://cheatsheetseries.owasp.org/cheatsheets/OAuth2_Cheat_Sheet.html)
2. **Confirmed guidance:** credentials need a complete lifecycle—creation, controlled storage,
   rotation, revocation, expiry, incident response, and audit—and secret bytes must not enter logs.
   [OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html),
   [OWASP Logging](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
3. **Confirmed guidance:** NIST separates authenticator issuance, use, maintenance, invalidation,
   and session assurance. **Inference:** privileged portal actions should require explicit user intent
   and record lifecycle evidence, but Market Squawk must not claim an AAL or NIST conformance from
   that analogy. [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html)
4. **Confirmed guidance:** research-data use depends on exact ownership, terms/licenses, permissions,
   intended purpose/duration, security, and citation obligations. **Inference:** rights admission
   should bind a source/surface/dataset, terms digest/time, operations, retention, derivatives,
   modeling/export/redistribution, attribution, reviewer, and refresh trigger.
   [NIST RDaF](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html)
5. **Engineering synthesis:** remote revocation, local deletion, catalog retirement, and audit
   closeout are distinct facts; a provider may support only a subset. The state model must preserve
   `unsupported` and `indeterminate` rather than fabricate completion.

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Decision implication |
| --- | --- | --- | --- | --- |
| Never collect provider passwords for delegated API access | REP-001, REP-002 | Confirmed guidance | High | Provider login remains in provider-controlled browser/UI |
| Exact least privilege and default deny | REP-001, REP-002 | Confirmed guidance | High | Requested-versus-observed authority gate |
| Secret lifecycle and non-secret audit are mandatory | REP-004, REP-007 | Confirmed guidance | High | Typed lifecycle with redacted audit |
| Explicit user intervention informs privileged local actions | REP-003 | Scoped engineering inference | Medium | No AAL/compliance claim |
| Rights admission must be purpose- and provenance-specific | REP-009 | Guidance plus engineering inference | Medium-high | Versioned rights record and refresh gate |

## Limitations and Non-Findings

- These sources do not establish provider account eligibility, price, endpoint availability, terms,
  quotas, OAuth support, credential issuance, or revocation APIs.
- OWASP guidance is non-normative; NCSC/NIST scope cannot be silently converted into provider terms
  or a Market Squawk compliance claim.
- No selected source validates the repository's existing encrypted vault. It remains an existing
  candidate requiring direct exact-commit admission, not an absent feature.

## Source List

REP-001, REP-002, REP-003, REP-004, REP-007, and REP-009 are first-class inventory records assigned
to `reputable-sources-batch-001`, with per-source access and digest/reference fields.
