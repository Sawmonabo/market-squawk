# Reputable Sources Discovery Report


## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Candidate Sources](#candidate-sources)
- [Cross-Source Design Implications](#cross-source-design-implications)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps and Legal-Review Dependencies](#coverage-gaps-and-legal-review-dependencies)
- [Source List](#source-list)

## Research Scope

This discovery covers current, vendor-neutral security and governance guidance relevant to a local
Market Squawk onboarding portal as of **2026-07-22**. It is deliberately complementary to the
official-provider discovery: it evaluates secure OAuth/native-app patterns, explicit user intent,
secret storage and lifecycle, audit boundaries, and defensible data-rights admission. It does not
establish that any provider offers a flow, that an account or API is free, or that a proposed use is
legally permitted.

Final selection and assignment are canonical in `source-inventory.json`: REP-001, REP-002,
REP-003, REP-004, REP-007, and REP-009 are assigned. REP-005, REP-006, and REP-008 are explicitly
excluded as duplicative or indirect for the final decision.

Candidate selection favored NIST, OWASP, and the UK National Cyber Security Centre (NCSC). Each is
authoritative within its stated security or governance scope, but none overrides provider
documentation, provider terms, or qualified legal advice.

## Search Queries Used

Representative queries (search-result snippets were discovery aids only, never evidence):

- `site:cheatsheetseries.owasp.org OAuth2 Cheat Sheet PKCE native applications authorization code`
- `site:cheatsheetseries.owasp.org Secrets Management Cheat Sheet rotation revocation audit logging`
- `site:cheatsheetseries.owasp.org Transaction Authorization user acknowledgement final control`
- `site:cheatsheetseries.owasp.org Cryptographic Storage operating system secure storage`
- `site:pages.nist.gov/800-63-4 authentication intent keychain invalidation`
- `site:ncsc.gov.uk securing HTTP APIs OAuth least privilege deny by default`
- `site:ncsc.gov.uk service credential lifecycle rotation revocation`
- `site:nist.gov research data framework usage agreements terms licenses required permissions`

## Candidate Sources

| ID | Source | Type and Authority | Freshness | Priority | Key Finding | Portal Implication |
| --- | --- | --- | --- | --- | --- | --- |
| REP-001 | [OWASP OAuth2 Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/OAuth2_Cheat_Sheet.html) | Community-maintained application-security guidance; high implementation authority, non-normative | Live page accessed 2026-07-22; incorporates RFC 9700-era guidance | P0 | Authorization Code with PKCE is the current baseline for native/public clients; transaction-specific state must be bound to the initiating user-agent session. | **Inference:** implement OAuth only where provider metadata/docs explicitly support it; bind callback, PKCE verifier, state, provider, and initiation session; fail closed on mismatch or expiry. |
| REP-002 | [NCSC: API authentication and authorisation](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation) | UK government cyber-security guidance; high cross-provider implementation authority, non-normative outside its audience | Published and reviewed 2025-04-03 | P0 | User-delegated API access should use provider-issued temporary credentials rather than collecting the user's login credentials; authorization should be least-privilege, default-deny, and checked on every request. | Never request or retain provider account passwords. Admit only provider-issued keys/tokens whose verified scopes satisfy the selected capability and no broader authority than the user approved. |
| REP-003 | [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html) | Final U.S. government digital-identity standard; normative for its federal scope and strong contextual authority elsewhere | Final 2025-07-31; accessed 2026-07-22 | P0 | Authentication intent requires explicit claimant intervention; exportable authentication keys should use appropriate keychain storage and be access-controlled; compromised or user-requested authenticators require prompt invalidation. | **Inference:** privileged credential installation, replacement, and revocation should require explicit user action and show the provider, account identity, scopes, and operation being approved. Use OS-backed storage and expose prompt compromise/revocation handling. |
| REP-004 | [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) | Community-maintained application-security guidance; high practical authority, non-normative | Live page accessed 2026-07-22 | P0 | Secrets need creation, rotation, revocation, expiration, least privilege, and tamper-resistant audit; plaintext secrets must never be logged. Provider mechanisms vary. | Model provider-specific lifecycle capability explicitly. Store secret handles plus non-secret metadata, support manual-resume states where provider automation is absent, and never put token/key bytes in SQLite, logs, artifacts, or audit payloads. |
| REP-005 | [OWASP Cryptographic Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html) | Community-maintained cryptographic-storage guidance; high implementation authority, non-normative | Live page accessed 2026-07-22 | P0 | Minimize retained sensitive data and use secure storage supplied by the operating system or framework instead of configuration files; protection must follow a threat model. | Make the OS credential store the default credential boundary. A fallback must remain encrypted, authenticated, user-unlocked, and explicitly configured; plaintext files and persisted environment-variable substitutes are not acceptable long-term stores. |
| REP-006 | [OWASP Transaction Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html) | Financial-transaction security guidance; high relevance to confirmation UX, but indirect for credential enrollment | Live page accessed 2026-07-22 | P0 | Users should identify and acknowledge significant authorization data; changed data invalidates prior approval; final execution must be tied to a unique, time-limited authorization. | **Inference:** treat credential activation or permission escalation as a confirmation transaction: show provider/account/scopes/rights version, bind an expiring approval nonce, and restart approval if any material field changes. |
| REP-007 | [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html) | Community-maintained security-logging guidance; high implementation authority, non-normative | Live page accessed 2026-07-22 | P0 | Audit records should capture who, what, when, and where, including token administration and terms/consent events, but access tokens, passwords, encryption keys, and primary secrets should not be recorded directly. | Audit onboarding state transitions, scope decisions, terms-version acknowledgement, verification results, rotation/revocation attempts, and actor identity; record only a non-secret credential identifier/fingerprint and redact provider payloads. |
| REP-008 | [NCSC Cloud Security Principle 10: Identity and authentication](https://www.ncsc.gov.uk/collection/cloud/the-cloud-security-principles/principle-10-identity-and-authentication) | UK government cyber-security guidance; credible lifecycle and usability context, not a provider contract | Version 2.1; reviewed 2023-06-07; accessed 2026-07-22 | P1 | Service credentials should have lifecycle processes, compromised credentials should be revoked quickly, and strong authentication should remain usable. | Track verification time, expiry, compromise/revocation status, replacement identity, and last lifecycle check. Reduce user effort without silently suppressing provider MFA, consent, or re-verification. |
| REP-009 | [NIST Research Data Framework v2.0 (SP 1500-18r2)](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html) | U.S. government research-data governance framework; high vocabulary/governance authority, not legal advice | Published 2024-02; accessed 2026-07-22 | P0 | Data use is governed by ownership, terms, licenses, permissions, purpose/duration restrictions, security protocols, and citation obligations; machine-actionable licenses can encode some conditions. | Maintain a versioned rights admission that binds provider/source, canonical terms URI and digest, retrieved/effective time, intended uses, retention/derivative restrictions, attribution, reviewer/user decision, and re-admission trigger. |

## Cross-Source Design Implications

The following are **engineering inferences**, not claims made verbatim by any single source:

1. A provider capability descriptor should distinguish anonymous access, manually issued API keys,
   Authorization Code with PKCE, and Device Authorization. A flow is disabled unless current
   provider documentation or metadata affirmatively supports it.
2. Account authentication and credential authorization are separate boundaries. The portal must
   not collect a user's provider password; it may launch or resume an official provider flow and
   accept only the resulting provider-issued credential.
3. The portal should represent unavoidable human steps as durable, resumable states rather than
   pretending they are automatable. Provider MFA, CAPTCHA, identity checks, terms acceptance, and
   consent remain provider-controlled interactions.
4. Credential activation and privilege changes should show the exact provider, resolved account,
   requested scopes, local consumers, and rights version, then bind the user's time-limited
   confirmation to those immutable facts. Any change invalidates the confirmation.
5. Secret bytes cross directly into OS-backed secure storage. SQLite and audit storage retain only
   the credential handle, provider/account identity, permitted scopes, creation/verification/
   expiry status, non-secret fingerprint, and lifecycle state.
6. Rotation and revocation are provider-capability-driven state machines. Where no provider API is
   documented, the portal should guide and resume a manual provider step, verify the new remote
   state, atomically switch local consumers, and retire the old local secret.
7. Rights admission must fail closed when current terms, ownership, permitted use, retention,
   attribution, or redistribution obligations are unknown or materially changed. Security
   guidance can shape the record; it cannot decide the legal result.

## Excluded Sources

| Source | Reason Excluded |
| --- | --- |
| IETF RFC 8252, RFC 8628, RFC 9700, RFC 7009, RFC 8414, and RFC 7591 | Already selected by the official-documentation discovery. They are the primary protocol authority and should be deep-read there, not duplicated as secondary evidence. They also cannot prove that a provider supports a flow. |
| Provider help pages, API references, and terms | Owned by the official-provider category. Those sources, not this report, decide available flows, fees, permissions, quotas, and terms. |
| Auth0, Okta, cloud-vault, password-manager, and credential-broker tutorials | Useful implementation examples but product-specific or commercially interested; unnecessary given stronger vendor-neutral guidance. |
| CISA Secure by Design Pledge | Broad voluntary pledge, not a credential-onboarding specification; the live page also returned HTTP 403 to the research fetcher, so search snippets were not used as evidence. |
| NIST SP 800-57 Part 1 Rev. 5 | Strong cryptographic-key guidance, but API credentials are not uniformly cryptographic keying material; selected sources address the portal boundary more directly. |
| NIST Privacy Framework 1.1 Initial Public Draft | Broad privacy framework still in draft at the as-of date; RDaF v2.0 more directly covers data terms, licenses, permissions, and use constraints. |
| NCSC iOS application storage guidance and OWASP mobile storage tests | Platform-specific. Official Apple, Windows, and freedesktop secret-store documentation is already selected in the official-documentation category. |
| Blogs, generic listicles, SEO pages, and search-result snippets | Insufficient authority or source provenance. |

## Coverage Gaps and Legal-Review Dependencies

1. **Provider support remains a primary-source question.** No generic security source establishes
   that Coinbase, Kraken, FRED, BLS, or another provider exposes OAuth, device flow, dynamic client
   registration, key issuance, introspection, rotation, or revocation.
2. **Human interaction cannot be universally eliminated.** Security guidance supports explicit
   user intent; provider documentation determines which browser, MFA, CAPTCHA, identity, consent,
   or terms step is mandatory. The product should resume those steps, not promise zero-touch
   enrollment.
3. **“Automated rotation” is conditional.** Generic OWASP/NCSC guidance expresses a desired
   lifecycle outcome. It does not authorize undocumented provider endpoints. Manual rotation with
   verified cutover is the correct fallback where official automation is absent.
4. **OS keyring behavior needs primary platform validation.** These sources support the secure-store
   boundary, but Apple, Windows, and freedesktop documentation must define actual availability,
   user-presence prompts, access controls, deletion, and headless behavior.
5. **Rights admission requires legal judgment.** NIST RDaF supplies governance concepts, not a
   legal opinion. Ambiguous ownership, ML/model use, caching, derived data, redistribution,
   attribution, jurisdiction, or mutable terms require qualified legal review before provider
   enablement.
6. **NIST intent is contextual.** SP 800-63B-4 governs authentication in its stated scope. Applying
   its explicit-intent principle to credential installation and privilege escalation is a
   security-design inference, not a claim of NIST compliance.

## Source List

All sources were retrieved on 2026-07-22.

- [OWASP OAuth2 Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/OAuth2_Cheat_Sheet.html)
- [NCSC: API authentication and authorisation](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation)
- [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [OWASP Cryptographic Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html)
- [OWASP Transaction Authorization Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Transaction_Authorization_Cheat_Sheet.html)
- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [NCSC Cloud Security Principle 10](https://www.ncsc.gov.uk/collection/cloud/the-cloud-security-principles/principle-10-identity-and-authentication)
- [NIST Research Data Framework v2.0](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html)
