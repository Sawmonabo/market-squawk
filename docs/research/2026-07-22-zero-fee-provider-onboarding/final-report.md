# Task 19A Deep Research Report: Zero-Fee Provider Onboarding and Local Secret Activation


## Table of Contents

- [Executive Summary](#executive-summary)
- [Research Scope and Date](#research-scope-and-date)
- [Methodology](#methodology)
- [Mutable Source Refresh (2026-07-23)](#mutable-source-refresh-2026-07-23)
- [Source Coverage](#source-coverage)
- [Key Findings](#key-findings)
- [Provider Release Matrix](#provider-release-matrix)
- [Human and Automation Boundaries](#human-and-automation-boundaries)
- [Native Authorization and Secret Lifecycle](#native-authorization-and-secret-lifecycle)
- [GitHub Ecosystem Findings](#github-ecosystem-findings)
- [Academic and Research Findings](#academic-and-research-findings)
- [Official Documentation Findings](#official-documentation-findings)
- [Reputable Source Findings](#reputable-source-findings)
- [Cross-Source Synthesis](#cross-source-synthesis)
- [Decision and Recommended Implementation](#decision-and-recommended-implementation)
- [Task 19A Implementation Acceptance Criteria](#task-19a-implementation-acceptance-criteria)
- [Risks, Gaps, and Open Questions](#risks-gaps-and-open-questions)
- [Source Matrix](#source-matrix)
- [Appendix A: Source Inventory](#appendix-a-source-inventory)
- [Appendix B: Research Artifact Inventory](#appendix-b-research-artifact-inventory)

## Executive Summary

**Decision: proceed with Task 19A as a capability-gated local onboarding portal, but do not claim
universal automatic signup, universal zero-fee eligibility, universal private-account access, or
release readiness for every provider surface.** The evidence supports a strong product that removes
search and configuration toil: it can configure no-secret sources automatically, send users to the
exact official provider action when a human step is required, resume after credential import or
authorization, verify least privilege, store secrets locally, and maintain explicit lifecycle and
rights state. It cannot legitimately replace provider login, consent, MFA, CAPTCHA, identity checks,
terms acceptance, or manual key issuance where the provider retains those controls.

The provider decision is surface-specific:

- **SEC EDGAR and Treasury Fiscal Data** have documented no-secret access and affirmative scoped
  reuse evidence. Fiscal Data remains documentation-ready after bounded runtime smoke evidence. The
  SEC record is now `RefreshRequired` because its exact official content could not be captured with
  HTTP 200 during the mandatory digest refresh; activation remains unavailable until that evidence
  gap is closed. The last reviewed SEC content requires a declared application/company and
  administrative contact in the `User-Agent` and limits automated access to 10 requests per second.
  [SEC APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
  [SEC fair-access/reuse FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
  [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/)
- **Coinbase and Kraken public market data** have documented no-key paths and should be the default
  exchange onboarding modes. Their reviewed evidence does not yet admit durable storage/modeling/
  redistribution rights or establish a permanent zero-fee promise across every product activity.
  [Coinbase Advanced Trade](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api),
  [Kraken Exchange overview](https://docs.kraken.com/exchange/guides/overview)
- **Coinbase and Kraken private account data** use human-created credentials. The portal can import
  and verify them: Coinbase exposes view/trade/transfer/receive and portfolio metadata, while Kraken
  exposes exact permissions, restrictions, expiry, query bounds, and IP allowlisting. Private cost,
  account eligibility, durable-use rights, and automatic remote lifecycle remain incomplete.
  [Coinbase key permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions),
  [Kraken key information](https://docs.kraken.com/api-reference/account-data/get-api-key-info)
- **BLS v1** has a documented no-key path and **BLS v2** is an optional human-resumed higher-quota
  path with CAPTCHA, email delivery, and annual renewal. The official BLS terms provide affirmative
  secondary-use language while imposing access-date citation, disclaimer, truthful-representation,
  limit-compliance, and third-party-rights duties. Both tiers can proceed as scoped, documentation-
  ready capabilities after those duties are bound and runtime smokes pass; neither receives a
  blanket grant for out-of-scope or third-party material. Both BLS records are currently
  `RefreshRequired` because the mandatory exact-content digest capture remains unresolved. The
  **Treasury daily-rate XML feed** still lacks feed-specific durable-use evidence.
  [BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
  [BLS terms](https://www.bls.gov/developers/termsOfService.htm),
  [Treasury XML feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
- **FRED/ALFRED is blocked for Market Squawk ingestion, persistence, modeling, export, and AI-facing
  use pending qualified, scope-specific rights resolution.** A FRED account is described as free and
  API keys are technically supportable, but the current legal evidence conflicts with mandatory
  storage/database and modeling behavior and leaves third-party-series rights with the user. Key
  authentication is not a rights grant. [FRED account registration](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/),
  [FRED legal terms](https://fred.stlouisfed.org/legal/)

The authorization and storage architecture is also conditional:

- Native OAuth uses the external system browser, a native/public-client registration, exact
  issuer/redirect/session binding, and transaction-specific PKCE `S256`. Device authorization and
  dynamic client registration are disabled until the exact provider advertises and authorizes them.
  [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html),
  [RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html),
  [RFC 8414](https://www.rfc-editor.org/rfc/rfc8414),
  [RFC 7591](https://www.rfc-editor.org/rfc/rfc7591)
- Apple Keychain, Windows Credential Manager, and freedesktop Secret Service require different
  locators, prompt/session behavior, persistence policy, and typed failures. Secret bytes stay out
  of SQLite, logs, artifacts, and MCP results; the catalog retains only opaque references and
  non-secret lifecycle evidence.
- The external research did not select a portable encrypted-vault construction. **Project context
  supplied for Task 19A says Market Squawk already contains an Argon2id plus
  XChaCha20-Poly1305, capability-confined, crash-consistent encrypted vault with rotation, recovery,
  and authority generations in `market-squawk-platform::secrets`.** That is an existing candidate,
  not an absent capability and not an external-source finding. Task 19A must independently audit and
  admit it against the complete KDF, AEAD, key-custody, recovery, migration, and anti-rollback
  criteria. It should not invent a second cryptosystem unless the existing implementation fails the
  admission review.

No runtime provider probe, authorization flow, credential creation, or OS-store operation was
performed during this research. Therefore, no provider surface is release-approved solely by this
report. The acceptance criteria below convert the evidence into objective implementation and release
gates.

## Research Scope and Date

**Topic:** Zero-fee provider onboarding portal for Market Squawk: official user authorization,
account and API credential issuance, local secret activation, and automation boundaries.

**As-of date:** 2026-07-23. The original multi-category research completed on 2026-07-22; the
mutable-source refresh below is anchored to 2026-07-23.

**Decision context:** Design mandatory Task 19A so users can activate useful zero-fee sources with
the least possible setup burden, while preserving provider-controlled human actions, least privilege,
local secret security, data-use rights, quotas, cancellation, lifecycle, and audit.

The completed evidence covers:

- Coinbase Advanced Trade public and private own-account surfaces;
- Kraken Spot public and private own-account surfaces;
- SEC EDGAR submissions/XBRL;
- FRED/ALFRED v1/v2;
- BLS v1 unregistered and v2 registered;
- Treasury daily-interest-rate XML and Fiscal Data;
- native OAuth, device authorization, metadata, DCR/registration management, token revocation;
- Apple Keychain, Windows Credential Manager/DPAPI, and freedesktop Secret Service.

The report does not offer legal advice. “Rights admitted,” “rights pending,” and “blocked” are
engineering release states derived from the reviewed official evidence. Qualified legal review and
written permissions remain necessary where identified.

## Methodology

The final synthesis semantically deduplicates four canonical category reports and seven completed
batch reports. It uses official provider/government documentation and IETF/OS primary sources for
external capabilities; exact-commit GitHub evidence for implementation fit; three original formal/
empirical papers for composition risk; and selected OWASP/NCSC/NIST guidance for cross-provider
security and data-governance implications. Secondary guidance never overrides provider contracts.

The synthesis separates five questions that must not be collapsed:

1. Can the endpoint be reached without a credential?
2. Is a paid subscription or account prerequisite positively documented?
3. Can Market Squawk acquire and verify the required authority through an official provider flow?
4. Do the reviewed terms admit the intended storage, modeling, export, and redistribution behavior?
5. Has the actual implementation and live provider behavior been verified?

Every material fact is cited and labeled separately from engineering inference or project policy.
Conflicts and explicit non-findings are retained rather than resolved by assumption. The root
`source-inventory.json` is the canonical 60-source selection/assignment ledger. Every selected
source has category, priority, assigned batch, retrieval/access state, and a response digest, exact
Git commit, or explicit stable reference plus mandatory refresh status.

## Mutable Source Refresh (2026-07-23)

Task 19A's mandatory pre-implementation refresh covered `DOC-009`, `DOC-010`, `DOC-014`,
`DOC-019`, `DOC-020`, `DOC-026`, `DOC-028`, and `DOC-029`. Retrieval used the current official
provider or government URL, normal HTTP content negotiation, and a declared Market Squawk research
user agent. A response-body digest is content authority only when the response contains the official
source. A `403` access-denial body or a `404` body is retained only as retrieval-health or URL-
migration evidence and is never substituted for a terms, policy, or schema digest.
Official-source search and standard alternate representations found no authorized exact-content
HTTP 200 route for the five SEC/BLS pages; semantically adjacent endpoints were not substituted.

| ID | Retrieved at (UTC) | Final official URL | HTTP | Response-body SHA-256 | Change assessment |
| --- | --- | --- | ---: | --- | --- |
| `DOC-009` | `2026-07-23T06:44:57Z` | [Coinbase Developer Platform Terms](https://www.coinbase.com/legal/developer-platform/terms-of-service) | 200 | `dc6dad1fc5690b345c9d95436d72abd8864cd8c6315ebfe65fbbd010a6fe4273` | No semantic change observed. The page still states June 23, 2026 and preserves the mutable fees, limits, storage/use, and third-party-content boundaries. |
| `DOC-010` | `2026-07-23T06:44:58Z` | [Coinbase Exchange key rotation](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key) | 200 | `79ec908d39947c2476b43643565cdee6619b07a75848c1dab0a20578f8110a92` | No semantic change observed. It remains Exchange-specific, revoke-first, human-controlled rotation guidance. |
| `DOC-014` | `2026-07-23T06:44:03Z` | [Kraken Get API Key Info](https://docs.kraken.com/api-reference/account-data/get-api-key-info) | 200 | `2850d341b212b88fe38ba1d754175a707c0500e553213a727f59e5841137a275` | The former `/api/docs/rest-api/get-api-key-info` route returned 404 at `2026-07-23T06:47:13Z` (`e37756d4f2a2e9cc0cd430a952a0ed4822e97e05576e6c01ed2e9bcba655b31d`). The official canonical URL changed, but the endpoint, permission, restriction, expiry, query-bound, IP-allowlist, and sensitive-response semantics did not. The official [Markdown representation](https://docs.kraken.com/api-reference/account-data/get-api-key-info.md) returned 200 at `2026-07-23T06:47:15Z`; its admitted content digest is `60e3b211ba2c5d94f03d73a767022149e2d29203d334487080aff8360bcadd0c`. |
| `DOC-019` | `2026-07-23T06:45:13Z` | [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | 403 | `2a6cea3a1a230d6aa30b151ff80e844fc3007c1d1b0996536bfa6e60f79606b4` | Digest is the SEC rate-threshold denial body, not documentation content. The official rendered page still states no-key `data.sec.gov` access and April 8, 2025 review, but the content-digest gate remains open. |
| `DOC-020` | `2026-07-23T06:45:13Z` | [SEC Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | 403 | `e901cf48f0dd1d287d5009182ad4f0479550d06f6518f096271a88dc4a7d70ee` | Digest is the SEC rate-threshold denial body, not policy content. The official rendered page still states the declared `User-Agent`, 10 requests/second, and August 23, 2024 review; the content-digest gate remains open. |
| `DOC-026` | `2026-07-23T06:45:13Z` | [BLS API FAQ](https://www.bls.gov/developers/api_FAQs.htm) | 403 | `6ce8eef3fca865c1a9c21812cfd44b9b3ca7d45c00223ccc10e87775241a3758` | Digest is the BLS access-denial body, not documentation content. The official rendered page still shows the v1/v2 limits, registration, annual renewal, `429`, and August 30, 2023 modification date; the content-digest gate remains open. |
| `DOC-028` | `2026-07-23T06:45:14Z` | [BLS v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm) | 403 | `d80a450b4a840e7552e0602bff40419f935c72092500d709259d3428160091cd` | Digest is the BLS access-denial body, not request-schema content. The official rendered page still includes `registrationkey` and the October 5, 2020 modification date; the content-digest gate remains open. |
| `DOC-029` | `2026-07-23T06:45:14Z` | [BLS API terms](https://www.bls.gov/developers/termsOfService.htm) | 403 | `ad9ae09da74dca957f90a59469c6784b2789155d7fc29e87a70c57ba51110820` | Digest is the BLS access-denial body, not terms content. The official rendered page still contains the secondary-use language and its citation, disclaimer, representation, limit, and third-party duties, with the August 30, 2023 modification date; the content-digest gate remains open. |

The Coinbase records and the migrated Kraken record now have admissible official response-body
digests. No provider-capability or acceptance-criterion semantics changed. The SEC surface and both
BLS surfaces nevertheless remain `RefreshRequired` and cannot reach `ActiveScoped`: their exact
official content bodies were not captured successfully. `T19A-AC-01`, `T19A-AC-06`,
`T19A-AC-08`, `T19A-AC-21`, and `T19A-AC-22` therefore remain unsatisfied for those affected
records until an authorized retrieval returns the official content with HTTP 200 and its digest is
recorded. This is an evidence-availability narrowing, not evidence that the documented SEC or BLS
provider behavior itself changed.

## Source Coverage

| Evidence class | Completed coverage | Final use | Limitation |
| --- | --- | --- | --- |
| Official provider/government documentation | 32 source IDs across Coinbase, Kraken, SEC, FRED/ALFRED, BLS, and Treasury | Provider capabilities, human boundaries, quotas, rights evidence, lifecycle, release states | No runtime probe; provider terms and limits are mutable; five SEC/BLS content digests remain refresh-blocked |
| IETF/RFC Editor standards | 7 distinct RFC source IDs | Native OAuth, device flow, issuer metadata, DCR, DCR management, current OAuth security, revocation | Standards do not establish provider support or client eligibility |
| Official OS/platform documentation | 7 distinct source IDs | Apple, Windows, DPAPI, and Secret Service storage contracts | No platform runtime validation; Secret Service 0.2 is a draft |
| Exact-commit GitHub repositories | 5 | Candidate library/product architecture and maintenance evidence | Not provider capability, rights, or release evidence |
| Original formal/empirical papers | 3 | OAuth composition and cross-app threat evidence | Not a direct Market Squawk evaluation |
| OWASP/NCSC/NIST reputable sources | 6 | Least privilege, secret lifecycle, audit, user intent, data-rights governance | Advisory/contextual; never provider terms |
| Completed batch/category reports | 7 batches / 4 syntheses | Auditable source-to-category-to-final lineage | Does not replace runtime evidence |
| Runtime evidence | 0 provider/store operations | None | Availability, response bytes, latency, throttling, prompts, and platform behavior remain unverified |

## Key Findings

### 1. Task 19A is a capability router, not one universal signup workflow

The supported setup modes are:

```text
NoCredential
ManualApiKeyImport
OAuthAuthorizationCodePkce
OAuthDevice
DynamicClientRegistration
```

Each provider surface selects only modes admitted by current official evidence. A provider’s public
market-data path does not imply private account authority. An OAuth standard does not imply provider
OAuth. A CLI that consumes a key does not prove it can issue one. DCR registers an OAuth client; it
does not create a provider user account or personal API key.
[RFC 7591](https://www.rfc-editor.org/rfc/rfc7591),
[Coinbase key authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication),
[Kraken key creation](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key)

### 2. “No key,” “free,” and “permitted durable use” are independent

SEC EDGAR and Fiscal Data provide the strongest combined evidence: no credential plus affirmative
free/reuse language for the exact content/API scope. Coinbase/Kraken public data, BLS v1, and
Treasury XML have no-secret technical paths but weaker or absent price/rights evidence. FRED’s
account is free and key issuance is documented, but the present rights evidence blocks mandatory
Market Squawk durable use. [SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[Fiscal Data](https://fiscaldata.treasury.gov/api-documentation/),
[FRED legal terms](https://fred.stlouisfed.org/legal/)

The portal must therefore display separate fields for credential cost/access state, rate tier,
rights admission, and release state. A green “connected” indicator cannot imply all four.

### 3. Human interaction is an explicit product boundary

Coinbase and Kraken private keys require provider login and key creation. FRED requires account
registration/login and a key request. BLS v2 requires organization/email, CAPTCHA, terms acceptance,
emailed-key retrieval, and annual renewal. Native OAuth requires the external browser, and device
authorization requires approval on a browser-capable device. These are normal `PendingUserAction`
states, not failures and not candidates for hidden retry loops.
[FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html),
[BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
[RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html),
[RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html)

### 4. Credential capture never equals activation

Coinbase and Kraken expose strong permission metadata endpoints. OAuth deployments may expose
granted scopes, issuer/audience/resource claims, or introspection, but those are provider-specific.
FRED/BLS minimal reads show request acceptance only and do not prove identity, lifecycle, or data-use
rights. Every credential passes exact secure-store write/read, provider permission, account/issuer,
expiry, rights, and rate admission before `ActiveScoped`.
[Coinbase key permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions),
[Kraken key information](https://docs.kraken.com/api-reference/account-data/get-api-key-info),
[FRED errors](https://fred.stlouisfed.org/docs/api/fred/errors.html),
[BLS v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm)

### 5. Remote and local credential lifecycle are separate

RFC 9700 refresh rotation/replay detection, RFC 7009 token revocation, RFC 7592 registration
management, provider manual key deletion, OS secure-store deletion, catalog tombstoning, and orphan
cleanup are distinct operations. Local deletion does not prove remote invalidation. Cancellation or
timeout after a mutating request may leave indeterminate provider state and requires reconciliation.
[RFC 9700](https://www.rfc-editor.org/rfc/rfc9700),
[RFC 7009](https://www.rfc-editor.org/rfc/rfc7009),
[RFC 7592](https://www.rfc-editor.org/rfc/rfc7592)

### 6. The local secret boundary is viable but platform-specific

Apple Keychain, Windows Credential Manager, and Secret Service all support secret CRUD, but their
identity, persistence, prompting, locking, session, migration, and error semantics differ. The
application can expose one typed interface while preserving those distinctions. No reviewed backend
shares an atomic transaction with SQLite, so generation-bound pending/activation/recovery state is
required. [Apple Keychain](https://developer.apple.com/documentation/security/keychain-services/),
[Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management),
[Secret Service](https://specifications.freedesktop.org/secret-service/latest-single/)

The existing `market-squawk-platform::secrets` encrypted vault is the candidate fallback supplied by
project context. It must be audited, not replaced reflexively. The external evidence does not by
itself validate its parameterization, key custody, persistence, migration, rollback resistance, or
failure behavior.

### 7. Provider rates are endpoint-class policy, not one global field

SEC publishes 10 requests/second; BLS publishes daily, series/query, years/query, and 10-second
dimensions; Coinbase discovery evidence gives an authenticated-user hourly default; Kraken
separates public/private/trading counters; FRED documents `429` without a universal number; Treasury
does not publish a numeric ceiling in the reviewed pages. Each adapter needs an evidence-dated,
surface-specific limiter and explicit unknown state.
[SEC FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions),
[BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
[Coinbase rate limits](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting),
[Kraken rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-),
[FRED errors](https://fred.stlouisfed.org/docs/api/fred/errors.html)

## Provider Release Matrix

| Provider surface | Default setup | Human boundary | Rights / minimum verification | Evidence classification | Confidence | Release decision |
| --- | --- | --- | --- | --- | --- | --- |
| **Coinbase Advanced Trade public** | No credential | None | Durable-use/permanent-price evidence incomplete; bounded product/book/freshness probe required | Documented access fact plus conservative rights inference | Medium-high | `onboarding_ready_rights_limited` |
| **Coinbase private own-account** | Manual user-created App key; partner OAuth separate | Provider login, permissions/portfolio, secret handoff | Require view/expected portfolio; reject trade/transfer/receive; private eligibility/cost/rights incomplete | Documented authority surface plus least-privilege inference | Medium-high | `optional_private_path_evidence_incomplete` |
| **Kraken Spot public** | No credential | None | Durable-use/permanent-price evidence incomplete; bounded book/freshness/sequence/checksum probe | Documented access fact plus conservative rights inference | Medium-high | `onboarding_ready_rights_limited` |
| **Kraken private own-account** | Manual user-created key/secret | Kraken Pro login, restrictions, optional key 2FA, secret handoff | Exact query-only `GetApiKeyInfo` profile; private eligibility/cost/rights incomplete | Documented verification surface plus least-privilege inference | Medium-high | `optional_private_path_evidence_incomplete` |
| **SEC EDGAR submissions/XBRL** | No credential; declared non-secret client/contact | User confirms administrative contact | Current aggregate limit at or below 10/s; bounded CIK/accession probe; public EDGAR-only provenance | Confirmed official facts; scope binding is engineering policy | High | `documentation_ready_no_secret_runtime_smoke_pending` |
| **FRED/ALFRED v1/v2** | Human-created key | Free-account registration/login and key request | Current terms conflict with mandatory persistence/database/modeling use; third-party-series rights remain; read is not introspection | Confirmed terms/key facts; hard-gate effect is engineering release policy | High | **`blocked_pending_qualified_rights_resolution`** |
| **BLS v1 unregistered** | No credential, lower quota | None | Bind BLS provenance, access date, disclaimer, truthful representation, limits, third-party scope; bounded v1 probe | Confirmed secondary-use language/duties plus scoped-admission inference | Medium-high | `documentation_ready_no_secret_scoped_rights_runtime_smoke_pending` |
| **BLS v2 registered** | Optional emailed registration key | Organization/email, CAPTCHA, terms, email retrieval, annual renewal | Same BLS rights record; exact higher-tier limits; bounded keyed POST; key changes quota/features only | Confirmed registration/terms facts plus scoped-admission inference | Medium-high | `optional_human_resumed_scoped_rights_runtime_smoke_pending` |
| **Treasury daily-rate XML** | No documented credential | None | Feed-specific durable-use evidence incomplete; bounded XML/OData/date/value/pagination probe | Confirmed access fact plus conservative rights inference | Medium | `technically_available_no_secret_durable_rights_pending` |
| **Treasury Fiscal Data** | Explicit no-account/no-token | None | Bind broad reuse terms to exact API/dataset; validate data/meta/links/version | Confirmed official facts; provenance binding is engineering policy | High | `documentation_ready_no_secret_runtime_smoke_pending` |

The table preserves each surface's underlying semantic decision. `RefreshRequired` is a
higher-priority evidence state: SEC and both BLS rows are currently unavailable despite those
baseline decisions. The machine-readable matrix records that override with `refresh_state`,
`activation_available`, and the exact blocking source IDs.

Provider matrix evidence:
[Coinbase public/private boundary](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api),
[Coinbase authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication),
[Kraken public/private boundary](https://docs.kraken.com/exchange/guides/overview),
[Kraken key creation](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key),
[SEC API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces),
[FRED key](https://fred.stlouisfed.org/docs/api/api_key.html),
[BLS FAQ](https://www.bls.gov/developers/api_FAQs.htm),
[Treasury XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed), and
[Fiscal Data](https://fiscaldata.treasury.gov/api-documentation/).

### Release-state interpretation

- `documentation_ready_no_secret_runtime_smoke_pending` means official access and scoped rights
  evidence are sufficient for implementation, but the live endpoint was not verified here.
- `onboarding_ready_rights_limited` means the no-key technical route is documented but durable-use
  rights are not admitted.
- `optional_private_path_evidence_incomplete` means manual import and authority verification are
  implementable but private cost/eligibility, rights, lifecycle, or runtime evidence remains.
- `documentation_ready_no_secret_scoped_rights_runtime_smoke_pending` means official BLS access and
  secondary-use evidence support scoped implementation after duties are bound; runtime is unproved.
- `optional_human_resumed_scoped_rights_runtime_smoke_pending` adds BLS's provider-controlled
  registration/key/renewal boundary without changing the underlying scoped rights record.
- `technically_available_no_secret_durable_rights_pending` means public access exists but durable
  publication/modeling stays closed.
- `blocked_pending_qualified_rights_resolution` is a hard gate for the intended Market Squawk use.

## Human and Automation Boundaries

### What Market Squawk can automate safely

- Detect and activate admitted no-secret provider surfaces.
- Collect non-secret configuration such as SEC administrative contact.
- Open an exact official provider deep link so the user does not search for enrollment/key pages.
- Prepare and track a provider-specific `PendingUserAction` workflow.
- Launch an admitted external-browser OAuth request and receive a bounded callback.
- Display an admitted device authorization URI/code and run a bounded standards-compliant poller.
- Import a user-provided credential directly into the selected secure store without durable
  plaintext staging.
- Verify provider permissions/restrictions/account binding with a non-mutating provider operation.
- Rotate generations, reconcile crashes, request documented remote revocation, delete exact local
  items, and audit non-secret outcomes.

### What remains provider/user-controlled

- Account signup, provider password entry, MFA, CAPTCHA, identity and jurisdiction checks, terms
  acceptance, consent, permission selection, and one-time secret viewing.
- Provider approval for OAuth clients, Embed/partner products, DCR, device flow, or other special
  programs.
- Provider-side token-family behavior, key issuance, revocation, deletion, and quota enforcement.
- Qualified rights decisions and written permissions.

The product goal should be **“no searching and no redundant configuration”**, not “no browser or
human action.” Native OAuth explicitly requires an external user-agent, and device authorization
explicitly requires user approval. [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html),
[RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html)

## Native Authorization and Secret Lifecycle

### Native authorization profile

Enable `OAuthAuthorizationCodePkce` only when the exact provider deployment establishes:

- native/public-client registration for Market Squawk;
- a supported claimed-HTTPS, private-use, or literal-loopback redirect;
- trusted expected issuer and authorization/token endpoints;
- PKCE `S256`, exact scopes, and an admitted token endpoint authentication method;
- issuer/mix-up, state/CSRF, redirect, expiry, single-consumption, audience/resource, and granted-
  scope verification;
- sender-constrained or rotated/replay-detected refresh tokens for persistent public-client use.

A static secret shipped in the executable is not confidential client authentication. Late or
mismatched callbacks are rejected. The loopback listener accepts one transaction and closes on
success, denial, cancellation, or timeout. [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252.html),
[RFC 9700](https://www.rfc-editor.org/rfc/rfc9700)

### Device authorization profile

Enable `OAuthDevice` only when provider documentation or trusted metadata advertises the device
grant and device authorization endpoint for the exact client. Poll no faster than the supplied
interval, or five seconds if absent; add five seconds after `slow_down`; back off on connection
timeouts; stop on denial, expiry, cancellation, success, or other terminal error; never restart
without explicit user action. Device codes are secrets; user codes and complete verification URIs
are short-lived UI material and are not durable audit content.
[RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html)

### Metadata, DCR, and revocation profile

Issuer metadata is accepted only from an allowlisted HTTPS issuer and only when the returned issuer
matches exactly. Runtime metadata can narrow but cannot broaden the adapter’s reviewed capabilities.
DCR is enabled only if the provider exposes and authorizes registration and defines any initial
access token/software statement requirements. RFC 7592 registration management remains optional and
Experimental. RFC 7009 remote token revocation is called only where supported.
[RFC 8414](https://www.rfc-editor.org/rfc/rfc8414),
[RFC 7591](https://www.rfc-editor.org/rfc/rfc7591),
[RFC 7592](https://www.rfc-editor.org/rfc/rfc7592),
[RFC 7009](https://www.rfc-editor.org/rfc/rfc7009)

### Secret-store profile

The application-facing backend supports capability probe, exact create/read/replace/delete,
interaction, cancellation, deadline, reconciliation, and bounded-memory cleanup. Typed outcomes
include unavailable, not found, conflict, locked/interaction required, user cancelled, policy denied,
missing user/session service, deadline, indeterminate completion, transient/permanent failure, and
cleanup required.

Backend specifics are preserved:

- Apple queries use an exact Market Squawk namespace or persistent reference; broad update/delete
  matches are rejected. Accessibility and user presence are explicit policy.
- Windows uses exact `TargetName` plus `CRED_TYPE_GENERIC`, same-user/same-machine persistence, and
  correct returned-buffer clearing/freeing. `CRYPTPROTECT_LOCAL_MACHINE` is not the per-user DPAPI
  fallback default because it broadens machine access.
- Secret Service uses safe non-sensitive lookup attributes, not durable object paths. Attributes may
  be unencrypted; locked/prompted operations can partially complete or race; headless service/prompt
  availability is not assumed.

[Apple update/delete](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items),
[Apple accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility),
[Windows Credential Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management),
[`CryptProtectData`](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata),
[Secret Service](https://specifications.freedesktop.org/secret-service/latest-single/)

### Split catalog/store recovery

No backend shares a transaction with SQLite. Use a generation-bound protocol:

```text
reserve catalog PendingStore generation without secret bytes
-> write exact backend candidate
-> read/validate exact item where safe
-> verify provider permissions and rights/rate admission
-> atomically activate catalog generation
-> retain prior generation until verified cutover when overlap is allowed
-> reconcile orphaned/pending generations after crash
```

Remote revocation, local deletion, catalog retirement, and cleanup are separate states. Cancellation
prevents later activation but cannot prove an already completed provider/store side effect was
rolled back.

## GitHub Ecosystem Findings

The exact-commit GitHub batch covers GitHub CLI, Git Credential Manager, keyring-rs, oauth2-rs, and
Kraken CLI, including stars, forks, license, release/push freshness, maintenance evidence, relevance,
and caveats. [GitHub batch](reports/github/batch-001.md)

**Confirmed ecosystem facts:** maintained Rust candidates exist for typed OAuth and native stores;
GCM demonstrates a cross-platform human sign-in plus secure-store workflow; Kraken CLI corroborates
the public/paper no-key versus private-key boundary. **Engineering inference:** keyring-rs and
oauth2-rs are strong candidates for evaluation under Market Squawk's lock/license/security process,
but they remain subordinate to code-owned provider capabilities.
[keyring-rs](https://github.com/open-source-cooperative/keyring-rs),
[oauth2-rs](https://github.com/ramosbugs/oauth2-rs),
[GCM](https://github.com/git-ecosystem/git-credential-manager),
[Kraken CLI](https://github.com/krakenfx/kraken-cli)

Repository activity does not establish provider support, pricing, rights, or release fitness. The
existing Market Squawk encrypted vault remains project implementation context and needs direct
exact-commit admission; no external repository validates it.

## Academic and Research Findings

Three original papers now complete the academic lane. Formal OAuth and Financial-grade API analyses
found attacks in incomplete compositions and proved corrected modeled profiles only under explicit
assumptions. A 2025 USENIX measurement of 18 integration platforms reported cross-app takeover or
request-forgery classes when applications were not sufficiently differentiated.
[OAuth formal analysis](https://arxiv.org/abs/1601.01229),
[FAPI formal analysis](https://arxiv.org/abs/1901.11520),
[USENIX integration-platform study](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan)

**Engineering inference:** Market Squawk must bind each authorization to provider, adapter, account,
client, issuer, redirect, initiation session, scopes, state, PKCE verifier, expiry, and one-time
consumption. The studies do not prove a Market Squawk defect and do not enable a provider protocol.
The RFC sources remain official standards documentation rather than academic evidence.

## Official Documentation Findings

Official provider/government documentation supplies every provider-specific release classification
in this report. It establishes:

- public/private boundaries and verification metadata for Coinbase and Kraken;
- anonymous API, declared identity, current fair-access limit, and scoped reuse for SEC EDGAR;
- key transports, errors, vintage semantics, and release-blocking terms for FRED/ALFRED;
- BLS unregistered/registered quotas, registration handoff, annual renewal, affirmative secondary-
  use language, and attribution/disclaimer/representation/limit/third-party duties;
- anonymous XML mechanics and separate open-license Fiscal Data behavior for Treasury.

Official OS documentation establishes the backend-specific local secret behaviors. No provider or
platform documentation establishes a universal end-to-end guarantee; Task 19A must combine the
contracts without erasing their differences.

## Reputable Source Findings

Selected OWASP, NCSC, and NIST sources support four cross-provider controls: never collect provider
passwords for delegated API access; verify least privilege/default deny; manage creation, storage,
rotation, revocation, expiry, incident response, and redacted audit as one lifecycle; and bind data
rights to provenance, purpose, restrictions, and citation obligations.
[NCSC API guidance](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation),
[OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html),
[OWASP Logging](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html),
[NIST RDaF](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html)

These are guidance sources, not provider contracts. Applying NIST authentication-intent guidance to
privileged portal actions is explicitly an engineering inference and does not claim an assurance
level or NIST compliance.

## Cross-Source Synthesis

### One activation state must carry multiple independent admissions

```text
ProviderCapabilityAdmitted
AND HumanBoundaryCompleted
AND CredentialOrAnonymousRuntimeVerified
AND LeastPrivilegeVerified
AND SecureStoreAdmitted (when a secret exists)
AND RatePolicyAdmitted
AND RightsAdmittedForRequestedUse
= ActiveScoped
```

Failure of one gate cannot be papered over by another. Examples:

- A FRED key can authenticate successfully while persistence/modeling remains blocked.
- BLS v2 can increase quota while the same scoped terms duties and provenance boundary still apply.
- Coinbase/Kraken public endpoints can require no key while durable-use rights remain unadmitted.
- A token can be stored securely while carrying excessive scope and therefore remain inactive.
- A local secret can be deleted while the remote credential remains valid.

### Recommended portal state model

```text
Unavailable
AnonymousAvailable
UserActionRequired
CredentialImportedUnverified
ProtocolValidated
StoredUnverified
VerifiedLeastPrivilege
RightsAdmissionPending
RuntimeVerificationPending
ActiveScoped
RefreshRequired
RotationPending
RevocationUnconfirmed
IndeterminateRemoteState
CleanupRequired
Blocked
```

Every transition records provider/surface, adapter and evidence revision, credential generation,
actor class, timestamp, result, and non-secret evidence digest. It never records bearer material,
provider passwords, PKCE verifiers, authorization/device/user codes, complete verification URIs, or
secret-store buffers.

### Rights and source provenance are part of onboarding

The portal cannot finish activation by testing network access alone. It binds the exact provider
surface, source/document digest, intended operations (`display`, `persist`, `model`, `export`,
`redistribute`), retention scope, attribution/disclaimer, third-party rights, reviewer decision, and
refresh trigger. Fiscal Data rights cannot be copied to Treasury XML; SEC public filing rights do
not cover every `sec.gov` asset; a user-supplied FRED export remains FRED-origin content unless
separate provenance proves otherwise.

### Refresh is a capability invalidation event

Material changes in provider terms, quotas, authentication, registration, permissions, endpoint
schema, canonical URLs, OAuth metadata, key lifecycle, OS backend policy, or existing-vault format
invalidate only the affected capability. The portal enters `RefreshRequired`; it does not silently
continue under stale evidence.

## Decision and Recommended Implementation

### Proceed now

1. Build one provider-capability registry and state machine shared by CLI and local portal services.
2. Implement no-secret onboarding for the documented public surfaces, while keeping runtime and
   rights gates separate.
3. Implement human-resumed manual credential import for Coinbase/Kraken private paths and BLS v2,
   with exact least-privilege verification and no durable plaintext staging.
4. Implement SEC declared `User-Agent`, aggregate limiter, and scoped rights provenance.
5. Implement Fiscal Data anonymous activation with dataset/version/license lineage.
6. Implement BLS scoped terms/provenance duties and keep activation pending until the bounded runtime
   smoke passes; keep Treasury XML durable publication closed until feed-specific rights admission.
7. Keep FRED/ALFRED durable ingestion, modeling, export, and AI-facing use blocked pending qualified
   resolution; a key-entry form may not enable the adapter.
8. Implement the cross-platform secret-store interface and generation recovery outside the live
   path.
9. Audit the existing `market-squawk-platform::secrets` Argon2id/XChaCha20-Poly1305 vault against the
   acceptance criteria below. Reuse and harden it if admitted; design a replacement only if it fails.
10. Keep OAuth, device, DCR, introspection, and remote revocation behind exact provider capability
    flags. Do not expose unsupported flows in the user interface.

### Do not claim at release

- automatic creation of provider user accounts or personal API keys;
- elimination of provider-controlled browser/user actions;
- permanent free access where the official evidence is silent;
- universal Coinbase/Kraken private eligibility or rights;
- FRED durable-use compliance before qualified resolution;
- cross-platform secure-store equivalence;
- an admitted portable encrypted fallback before the existing vault review passes;
- remote revocation merely because the local secret was deleted;
- runtime availability or performance before bounded evidence exists.

## Task 19A Implementation Acceptance Criteria

The following criteria are cumulative. “Pass” requires exact implementation and evidence at the
candidate commit; contracts or UI scaffolding alone do not pass.

Evidence classification is explicit: AC-01 through AC-13 and AC-15 through AC-23 are engineering
requirements derived from the cited external evidence; AC-14 is a project-context admission gate for
an existing repository implementation. In **AC-21**, the rate-policy clause is externally derived,
while the heartbeat-versus-market-freshness clause is a **Market Squawk project architecture
requirement**. **AC-24 is entirely a Market Squawk project architecture/authority requirement**, not
a fact proved by provider, RFC, OS, GitHub, paper, or reputable-source evidence.

| ID | Acceptance criterion | Required evidence | Blocking scope |
| --- | --- | --- | --- |
| **T19A-AC-01** | Every provider surface has a versioned capability record containing setup mode, official entry URI/issuer, human boundary, credential kind, minimum/max accepted authority, verifier, rate policy, rights state, lifecycle support, evidence IDs/digests, and refresh trigger. Runtime metadata may narrow but never broaden it. | Catalog/config schema plus focused serialization/invariant evidence using all ten surface records | All Task 19A activation |
| **T19A-AC-02** | The portal exposes distinct `NoCredential`, `ManualApiKeyImport`, `OAuthAuthorizationCodePkce`, `OAuthDevice`, and `DynamicClientRegistration` modes and hides/marks unsupported provider modes. | End-to-end service/CLI/portal state evidence; no mode inferred from a generic library | Affected provider capability |
| **T19A-AC-03** | Human-controlled provider steps enter durable `UserActionRequired`/resume state with exact official deep link and requested permissions. Cancellation does not loop or fabricate success. | Focused state-transition evidence for manual key, browser OAuth, and device flow paths | Credentialed onboarding |
| **T19A-AC-04** | Coinbase public activation requests no secret. Coinbase private activation imports only a user-created App key, binds expected portfolio, requires view, and rejects trade/transfer/receive authority. | Bounded authorized provider smoke or contract fixture for `key_permissions`; redacted audit output | Coinbase private activation |
| **T19A-AC-05** | Kraken public activation requests no secret. Kraken private activation signs the exact key-info request, requires an allowlisted query-only permission set/restrictions, rejects unexpected write/funding/withdrawal/earn authority, and redacts sensitive key-info fields. | Bounded authorized key-info evidence plus exact permission comparison | Kraken private activation |
| **T19A-AC-06** | SEC activation requires no credential, requires a validated non-secret administrative contact in the declared `User-Agent`, enforces one aggregate conservative limiter at or below the current 10/s ceiling, and binds public EDGAR-only rights provenance. | Bounded known-CIK/accession semantic smoke, limiter evidence, and stored source/right lineage | SEC activation |
| **T19A-AC-07** | FRED/ALFRED cannot transition to persistence, modeling, export, or AI-facing active states unless a qualified, scope-bound rights decision and any required Bank/series-owner permissions are recorded. Key acceptance alone never opens the gate. | Hard policy test at service boundary plus rights-decision record schema; terms digest refresh | FRED/ALFRED |
| **T19A-AC-08** | BLS v1 enforces all lower-tier limits. BLS v2 requires human registration/key import, enforces higher-tier limits, tracks at-least-annual renewal, and never assumes anonymous v2. Both bind the same scoped BLS rights record: exact BLS provenance, access-date citation, required disclaimer, truthful representation, limit compliance, and third-party-rights boundary. | Exact quota/renewal policy, terms-duty record, scope rejection, and bounded semantic runtime evidence | BLS activation/publication |
| **T19A-AC-09** | Treasury XML uses anonymous bounded period requests, validates XML/date/value/pagination, and remains durability-blocked pending feed-specific rights. Fiscal Data uses no token, validates data/meta/links/version, and binds its license to exact dataset provenance. | Two bounded runtime smokes and distinct rights records; no license inheritance | Treasury activation/publication |
| **T19A-AC-10** | Native OAuth is impossible to enable without provider-specific native/public-client, redirect, issuer/endpoints, PKCE `S256`, scope, and refresh-replay admission. It uses the external browser, exact transaction binding, one callback consumption, and no embedded login. | Focused protocol state tests and provider-capability fixture; security review of callback/listener ownership | OAuth capability |
| **T19A-AC-11** | Device flow is impossible to enable without provider support. Polling honors server interval, five-second default, cumulative `slow_down`, timeout backoff, absolute expiry, cancellation, and explicit-user restart. Device/user codes and complete verification URI are absent from durable audit/logs. | Deterministic protocol-state tests covering each terminal branch and redaction | Device capability |
| **T19A-AC-12** | DCR and RFC 7592 management are disabled unless exact issuer/provider support and prerequisites are admitted. A mutating timeout enters `IndeterminateRemoteState`; registration credentials go directly to the secure store. DCR is never labeled account or API-key creation. | Capability gate, reconciliation evidence, and UI terminology review | DCR capability |
| **T19A-AC-13** | Apple, Windows Credential Manager, and Secret Service backends implement exact create/read/replace/delete, capability probe, prompt/cancel/deadline, and typed errors without flattening locked, cancelled, not-found, session-unavailable, or indeterminate states. | Focused platform validation on each supported OS/backend, including headless/locked cases where claimed | Durable credential activation by platform |
| **T19A-AC-14** | The existing `market-squawk-platform::secrets` vault is independently reviewed at the candidate commit. Admission verifies versioned Argon2id parameters and salts; XChaCha20-Poly1305 nonce uniqueness, associated-data/domain binding, and authentication failure behavior; unlock/key custody and memory lifetime; capability confinement; atomic/crash-consistent writes; authority/credential generations; rotation/recovery; corruption and wrong-key failure; migration/backup policy; rollback detection or explicit residual risk; file permissions; and no plaintext fallback. A new cryptosystem is considered only after a documented failure. | Code/design review, focused cryptographic-format/invariant tests, crash/fault injection, migration fixture, and threat-model decision | Encrypted fallback claim |
| **T19A-AC-15** | SQLite/catalog/logs/artifacts/MCP contain only opaque `SecretRef` and non-secret metadata. Tokens, API secrets, private keys, DCR access tokens, PKCE verifier, authorization/device/user codes, one-time state values, complete verification URIs, and provider passwords never persist there. | Targeted storage/log/audit inspection under success and failure fixtures | All credentialed activation |
| **T19A-AC-16** | Store/catalog activation is generation-bound and crash-recoverable: pending reservation, exact write/read, permission verification, atomic activation, prior-generation retention until cutover, orphan reconciliation, and exact cleanup. No broad Apple selector or ambiguous Secret Service attribute can modify another item. | Fault injection at every boundary and idempotent restart evidence | All durable credentials |
| **T19A-AC-17** | Permission verification records requested versus observed authority, restrictions, issuer/audience/resource/account binding, expiry, verifier revision, assurance limitation, and non-secret digest. Missing or excess authority fails closed. | Focused verifier contract tests for exact match, missing, excess, mismatch, expiry, and semantic-error cases | Credential activation |
| **T19A-AC-18** | Rotation/revocation separately records replacement issue/store/verify/cutover, remote old-credential revoke result or unsupported/indeterminate state, local old-item deletion, catalog retirement, and cleanup. Local deletion is never presented as confirmed remote revocation. | State-machine and crash-recovery evidence for overlap and revoke-first provider orders | Credential lifecycle |
| **T19A-AC-19** | Every browser, device, provider network, secure-store prompt/operation, and cleanup job has one owner, monotonic deadline, cancellation, terminal state, and bounded retry budget. Cancellation blocks later activation but does not erase indeterminate external effects. | Focused cancellation/deadline tests and no orphan worker/listener evidence | All onboarding operations |
| **T19A-AC-20** | Audit includes actor class, opaque operation/credential generation, provider/capability/evidence revision, human boundary, requested/observed authority summary, timestamps, deadlines, and lifecycle results—never secret material or raw errors that may echo it. | Redaction tests on success/failure/cancel/provider-error paths and controlled audit query | All Task 19A operations |
| **T19A-AC-21** | Rate policies are provider/product/protocol/endpoint-class/version specific, enforce every documented dimension, treat unknown as conservative bounded policy, and refresh on `429` or evidence change. Heartbeat/connection health does not masquerade as market-data freshness. | Policy fixtures and bounded runtime observations; no global “unlimited” default | All provider calls |
| **T19A-AC-22** | Rights admission binds exact source/surface/dataset/version, terms URI/digest/retrieval/effective times, requested operations, retention, derivatives/modeling/export/redistribution, attribution/disclaimer, third-party rights, reviewer, decision, and refresh trigger. Network success cannot bypass it. BLS must preserve its affirmative secondary-use evidence and its duties; it is not represented as a pure rights non-finding. | Rights registry and service-boundary evidence for FRED hard denial, scoped BLS admission/duty enforcement, Treasury XML and exchange unknowns, and Fiscal Data provenance | Persistence/modeling/export |
| **T19A-AC-23** | Separately authorized bounded runtime smokes prove current endpoint/auth/store behavior; deterministic default tests remain offline. Evidence records exact commit, provider surface, time, request class, semantic result, and redacted response digest. | Smoke artifact set for each surface claimed active and each supported OS backend | Release claim |
| **T19A-AC-24** | All onboarding, OAuth, MCP, persistence, catalog, and secret-store work remains outside the live event-to-action path. No strategy, execution adapter, CLI, portal, or MCP operation can use a credential lacking `ActiveScoped` authority. | Architecture trace plus focused authority-boundary tests | Whole product safety |

Only focused tests that prove these high-risk contracts are required; acceptance does not call for
duplicative prose checks or a separate test for every documentation sentence.

## Risks, Gaps, and Open Questions

### Release-blocking or capability-blocking

1. **SEC/BLS mutable-source digests:** `DOC-019`, `DOC-020`, `DOC-026`, `DOC-028`, and
   `DOC-029` remain `RefreshRequired` after standards-compliant direct retrieval returned CDN access
   denials. SEC and both BLS capability records remain unavailable until exact official content is
   captured with HTTP 200; denial-body hashes are retrieval health only.
2. **FRED rights:** qualified, scope-specific review and any required written Bank/series-owner
   permissions remain unresolved. FRED stays blocked.
3. **Exchange durable-use rights:** Coinbase/Kraken public and private storage/modeling/reuse evidence
   remains incomplete.
4. **Treasury XML rights:** technical availability does not yet admit durable publication. BLS is
   separately blocked only until the scoped terms duties and provenance boundary are implemented
   and its source refresh and runtime smoke pass; it is not blocked for lack of any affirmative
   secondary-use text.
5. **Existing encrypted vault admission:** the implementation exists, but Task 19A has not yet
   independently established that it satisfies every T19A-AC-14 requirement at the release commit.
6. **Provider OAuth/device/DCR:** no generally available capability for current mandatory provider
   paths is admitted solely by this evidence. Coinbase OAuth is approved-partner constrained; Kraken
   Embed OAuth is a separate B2B surface.
7. **Runtime verification:** no provider endpoint, OAuth flow, credential, or OS store was exercised.

### Documentation conflicts to preserve

- BLS newer FAQ requires v2 registration while older examples omit a key: require the key and treat
  anonymous v2 as unknown.
- Fiscal Data says no token is required but its `403` description mentions an invalid API key: do
  not invent a credential; monitor anonymous behavior.
- SEC typical filing/API delays describe different paths and are not SLAs.
- Kraken’s key-info documentation moved from the now-404 `/api/docs/rest-api/` route to
  `/api-reference/account-data/` on 2026-07-23; URL/schema drift triggers refresh.
- Coinbase Exchange rotation guidance is supporting evidence and is not assumed to define every
  Coinbase App key lifecycle.
- Windows Credential Manager “local machine” persistence must not be confused with DPAPI’s broader
  `CRYPTPROTECT_LOCAL_MACHINE` flag.

### Open evidence questions

- Exact WebSocket-specific rate/connection/health rules for Coinbase and Kraken.
- Exact provider private-account cost, eligibility, jurisdiction, and verification prerequisites.
- Exact Treasury XML feed-specific rights and any BLS use that extends beyond the scoped official
  terms/provenance record or includes third-party material.
- Provider-specific OAuth public-client/PKCE, token lifetime, replay protection, introspection,
  revocation, native redirect, device, and DCR support if any is later requested.
- Measured OS-store behavior under locked, cancelled, headless, multi-user, upgrade, delete-failure,
  and crash conditions.
- Existing vault rollback model, backup/recovery contract, migration/version policy, and threat-model
  residual risks at the exact candidate commit.
- Product-selected numeric deadlines for browser callbacks, provider requests, prompts, store calls,
  and cleanup, while preserving provider/RFC pacing.

### Evidence-lineage state

The prior workflow gap is corrected. The root `source-inventory.json` contains 60 selected sources,
17 explicit exclusions, seven genuine batch assignments, per-source access state, and a digest,
exact Git commit, or refresh-required stable reference. Four canonical category syntheses feed this
report. A fresh independent verifier must still decide whether the repaired evidence chain passes.

## Source Matrix

The original source set was accessed or retrieved on 2026-07-22 unless another freshness date is
shown. The eight mutable records listed above were refreshed on 2026-07-23. The matrix is
deduplicated by source ID and URL.

### Coinbase

| ID | Official source | Decision use |
| --- | --- | --- |
| DOC-001 | [Advanced Trade REST API](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api) | Public/private endpoint boundary and public cache behavior |
| DOC-002 | [API-key authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication) | Human key creation, ECDSA/JWT, enable/regenerate/disable, IP restriction |
| DOC-003 | [Authorization and permissions](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization) | View/trade/transfer/receive and OAuth scope model |
| DOC-004 | [Get API Key Permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions) | Exact permission and portfolio verification |
| DOC-005 | [OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview) | Approved-partner and browser-consent boundary |
| DOC-006 | [OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference) | Refresh rotation and revoke endpoint for admitted OAuth clients |
| DOC-007 | [App rate limiting](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting) | Authenticated-user default quota and `429` |
| DOC-008 | [CDP for agents / CLI](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents) | CLI imports rather than issues a portal key |
| DOC-009 | [Developer Platform Terms](https://www.coinbase.com/legal/developer-platform/terms-of-service) | Mutable rate/terms boundary; last modified 2026-06-23 |
| DOC-010 | [Exchange API-key rotation](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key) | Manual rotation supporting evidence with Exchange product caveat |

### Kraken

| ID | Official source | Decision use |
| --- | --- | --- |
| DOC-011 | [Exchange overview](https://docs.kraken.com/exchange/guides/overview) | Public/private workflow and public endpoints |
| DOC-012 | [REST API keys](https://docs.kraken.com/exchange/guides/rest/api-keys) | Minimum privilege, purpose-bound keys, allowlisting, rotation guidance |
| DOC-013 | [Create a Spot API key](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key) | Human UI, restrictions, secret form, optional 2FA; updated 2025-08-08 |
| DOC-014 | [Get API Key Info](https://docs.kraken.com/api-reference/account-data/get-api-key-info) | Exact permission/restriction/expiry verification; canonical URL refreshed 2026-07-23 |
| DOC-015 | [Kraken CLI](https://docs.kraken.com/home/cli) | Public/paper mode and manual-key setup boundary |
| DOC-016 | [API rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-) | Public IP/pair guidance and private distinctions; updated 2026-03-27 |
| DOC-017 | [API-key security](https://support.kraken.com/articles/api-key-security) | Replacement/deletion and provider revocation/dormancy; updated 2025-03-31 |
| DOC-018 | [Developers documentation index](https://docs.kraken.com/llms.txt) | Retail Exchange versus approval-gated Embed OAuth |

### SEC, FRED/ALFRED, BLS, and Treasury

| ID | Official source | Decision use |
| --- | --- | --- |
| DOC-019 | [SEC EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | Anonymous submissions/XBRL, archives, cadence, CORS; reviewed 2025-04-08 |
| DOC-020 | [SEC Webmaster FAQ](https://www.sec.gov/about/webmaster-frequently-asked-questions) | User-Agent, 10/s limit, scoped public reuse; reviewed 2024-08-23 |
| DOC-021 | [FRED API keys v1](https://fred.stlouisfed.org/docs/api/api_key.html) | Account/key requirement, per-app key, query transport |
| DOC-022 | [FRED API keys v2](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html) | Bearer transport and separate v2 requirement |
| DOC-023 | [FRED account registration](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/) | Free account and human registration |
| DOC-024 | [FRED API errors](https://fred.stlouisfed.org/docs/api/fred/errors.html) | Invalid key and `429`; no universal numeric quota |
| DOC-025 | [FRED legal notices and terms](https://fred.stlouisfed.org/legal/) | Storage/database, AI/ML, attribution, and third-party rights decision gate |
| DOC-FRED-RT-001 | [FRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html) | Current-view versus ALFRED vintage/as-of semantics |
| DOC-026 | [BLS API FAQ](https://www.bls.gov/developers/api_FAQs.htm) | v1/v2 boundary, quotas, registration, renewal, errors; modified 2023-08-30 |
| DOC-027 | [BLS registration](https://data.bls.gov/registrationEngine/) | Organization/email, CAPTCHA, terms, emailed key |
| DOC-028 | [BLS v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm) | `registrationkey` and probe shape; modified 2020-10-05 |
| DOC-029 | [BLS API terms](https://www.bls.gov/developers/termsOfService.htm) | Affirmative secondary use plus citation/disclaimer, truthful representation, limits, and third-party duties; modified 2023-08-30 |
| DOC-030 | [Treasury daily-rate XML](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | Anonymous XML, filters, pagination, coverage; no feed-specific license found |
| DOC-031 | [Fiscal Data API](https://fiscaldata.treasury.gov/api-documentation/) | No account/token, formats/versioning, response metadata, open-use license |

### Authorization standards and operating-system documentation

| ID | Primary source | Decision use |
| --- | --- | --- |
| PAPER-001 | [RFC 8252: OAuth 2.0 for Native Apps](https://www.rfc-editor.org/info/rfc8252/) | External browser, redirects, PKCE, public-client boundary; BCP 212 |
| PAPER-002 | [RFC 8628: Device Authorization Grant](https://www.rfc-editor.org/info/rfc8628/) | Human handoff, polling/backoff, expiry, capability marker |
| PAPER-003 | [RFC 7591: Dynamic Client Registration](https://www.rfc-editor.org/info/rfc7591/) | Optional client registration and provider prerequisites |
| PAPER-004 | [RFC 7592: DCR Management](https://www.rfc-editor.org/info/rfc7592/) | Optional read/update/delete and credential rotation; Experimental |
| PAPER-005 | [RFC 8414: Authorization Server Metadata](https://www.rfc-editor.org/info/rfc8414/) | Exact issuer and endpoint/capability metadata |
| PAPER-006 | [RFC 9700: OAuth 2.0 Security BCP](https://www.rfc-editor.org/info/rfc9700/) | Current redirect, PKCE, mix-up, minimum privilege, refresh replay baseline; BCP 240 |
| PAPER-007 | [RFC 7009: Token Revocation](https://www.rfc-editor.org/info/rfc7009/) | Provider-supported token revocation |
| DOC-032 | [Apple Keychain Services](https://developer.apple.com/documentation/security/keychain-services/) | Small-secret storage, CRUD, exact lookup/persistent reference |
| DOC-033 | [Apple update/delete](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items) | Exact lifecycle and broad-match risk |
| DOC-034 | [Apple accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility) | Unlock/passcode/device-only/user-presence policies |
| DOC-035 | [Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management) | Generic credential identity, persistence, session, prompt/error boundary |
| DOC-036 | [`CredWriteW` / `CredDeleteW`](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew) | Exact create/replace/delete lifecycle |
| DOC-037 | [`CryptProtectData`](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata) | Windows same-user/machine fallback and broader-machine caveat; updated 2026-05-15 |
| DOC-038 | [Secret Service API 0.2 DRAFT](https://specifications.freedesktop.org/secret-service/latest-single/) | Login-session service, attributes, CRUD, locks/prompts, partial outcomes; published 2026-04-08 |

### GitHub repositories

| ID | Exact-commit source | Decision use |
| --- | --- | --- |
| GH-001 | [`cli/cli@efe3f16`](https://github.com/cli/cli/commit/efe3f165dd297c85fff11473dbf586f2d39fbf86) | Mature cross-platform CLI/human-onboarding reference |
| GH-003 | [`git-credential-manager@2fe99b8`](https://github.com/git-ecosystem/git-credential-manager/commit/2fe99b867b710265e3273b48da7513d91e6ef8eb) | Browser-mediated auth plus platform-store reference |
| GH-004 | [`keyring-rs@17054f0`](https://github.com/open-source-cooperative/keyring-rs/commit/17054f05971a4e8eabbcd5970ad37bcfa7e61048) | Rust native-store adapter candidate |
| GH-005 | [`oauth2-rs@72ce744`](https://github.com/ramosbugs/oauth2-rs/commit/72ce74401c26eb4dc85dcbfde587bbcfc149e3ae) | Typed Rust OAuth building-block candidate |
| GH-008 | [`kraken-cli@aa32814`](https://github.com/krakenfx/kraken-cli/commit/aa32814cea70913a70c9909693a7abd762963e83) | Official public/paper/private boundary; experimental caveat |

### Academic and research papers

| ID | Primary source | Decision use |
| --- | --- | --- |
| PAPER-009 | [Fett, Küsters, Schmitz: OAuth 2.0 formal analysis](https://arxiv.org/abs/1601.01229) | Coherent participant/session binding under explicit model assumptions |
| PAPER-010 | [Luo et al.: cross-app attacks in integration platforms](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan) | Empirical app-differentiation threat evidence |
| PAPER-012 | [Fett, Hosseyni, Küsters: FAPI formal analysis](https://arxiv.org/abs/1901.11520) | Complete financial-grade profile/conformance implication |

### Reputable security and data-governance sources

| ID | Source | Decision use |
| --- | --- | --- |
| REP-001 | [OWASP OAuth2](https://cheatsheetseries.owasp.org/cheatsheets/OAuth2_Cheat_Sheet.html) | PKCE and transaction-binding implementation guidance |
| REP-002 | [NCSC API authentication/authorization](https://www.ncsc.gov.uk/collection/securing-http-based-apis/2-api-authentication-and-authorisation) | No provider passwords; least privilege/default deny |
| REP-003 | [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html) | Scoped user-intent/lifecycle context; no AAL claim |
| REP-004 | [OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) | Credential lifecycle, incident response, audit |
| REP-007 | [OWASP Logging](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html) | Non-secret audit boundary |
| REP-009 | [NIST RDaF 2.0](https://nvlpubs.nist.gov/nistpubs/SpecialPublications/1500-18/NIST.SP.1500-18r2.html) | Purpose/provenance/terms/citation rights record |

## Appendix A: Source Inventory

The canonical machine-readable inventory is `source-inventory.json`; the companion
`final-report.json` carries final decisions and the 24 Task 19A acceptance criteria. Every referenced
ID resolves to one selected inventory record and an assigned canonical batch.

Source counts:

```text
Official provider/government sources  32
IETF/RFC standards                     7
OS/platform documentation              7
Exact-commit GitHub repositories       5
Original formal/empirical papers       3
OWASP/NCSC/NIST reputable sources      6
Total selected                        60
```

## Appendix B: Research Artifact Inventory

Canonical inputs synthesized here:

- `reports/docs/batch-001.md` through `batch-004.md`
- `reports/github/batch-001.md`
- `reports/papers/batch-001.md`
- `reports/reputable-sources/batch-001.md`
- all four reports under `reports/category-synthesis/`

Canonical outputs:

- `final-report.md`
- `final-report.json`
- `reports/verification/evidence-audit.md` (currently the prior FAIL, awaiting fresh independent
  replacement after this remediation)

No provider state, authorization state, secret-store state, tracked source code, application
configuration, or Cargo artifact was changed while producing these reports.
