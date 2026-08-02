# GitHub and Papers Discovery Report


## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Decision Summary](#decision-summary)
- [GitHub Implementation Candidates](#github-implementation-candidates)
- [Standards and Research Candidates](#standards-and-research-candidates)
- [Implementation Implications](#implementation-implications)
- [Excluded Sources and Non-Findings](#excluded-sources-and-non-findings)
- [Coverage Gaps](#coverage-gaps)
- [Source List](#source-list)

## Research Scope

This discovery pass covers the two primary-source categories not addressed by the companion
official-documentation report: active official or upstream GitHub implementations and original
standards or research papers relevant to Task 19A. The topic is a local, zero-fee provider
onboarding portal for Market Squawk, including provider-supported user authorization, credential
activation, secure local storage, lifecycle management, and evidence-backed provenance. Research
is anchored to **2026-07-22**.

Final selection and assignment are canonical in `source-inventory.json`: GH-001, GH-003, GH-004,
GH-005, and GH-008 enter the GitHub batch; PAPER-009, PAPER-010, and PAPER-012 enter the academic
batch; PAPER-001 through PAPER-007 enter official-docs batches because RFCs are standards, not
academic papers. Remaining candidates are explicitly excluded with reasons.

No product code or provider state was changed. No login, authorization, registration, account
creation, key issuance, or credential test was initiated. GitHub repository activity and licenses
were checked against the public GitHub API; implementation claims were checked against repository
source or maintained README material. Standards came from the RFC Editor or IETF, and papers from
their original arXiv or conference pages.

The companion documentation discovery already establishes the provider-specific baseline:
Coinbase and Kraken public market data can operate without credentials; their private retail keys
are created through human-controlled provider pages; SEC and Treasury public ingestion need no
account; FRED/ALFRED require a free account and key; BLS has an unregistered tier and an optional
free registration key. This report does not duplicate that evidence.

## Search Queries Used

Search snippets were lead discovery only; retained findings were verified in the opened primary
source.

1. `site:github.com/cli OAuth device flow web flow keyring source`
2. `site:github.com official OAuth CLI device authorization PKCE localhost callback`
3. `site:github.com keyring-rs macOS Keychain Windows Credential Manager Secret Service Rust`
4. `site:github.com git-credential-manager credential stores device browser authentication`
5. `site:github.com/ramosbugs/oauth2-rs device authorization revocation PKCE`
6. `site:github.com/openid AppAuth dynamic client registration external browser PKCE`
7. `site:github.com/coinbase official Advanced Trade SDK public endpoints API key`
8. `site:github.com/krakenfx official CLI API key setup public paper mode`
9. `site:rfc-editor.org OAuth native apps device authorization dynamic client registration metadata revocation security BCP`
10. `site:usenix.org OAuth integration platform cross-app attacks account security interfaces`
11. `site:arxiv.org OAuth formal security analysis financial-grade API`
12. GitHub API repository metadata lookups for activity, license, stars, forks, and default branch.

## Decision Summary

The portal must select from an explicit provider capability matrix rather than offer one universal
“automatic signup” flow:

| Capability | What the portal can safely automate | Hard boundary |
| --- | --- | --- |
| `NoCredential` | Configure and verify a public source without storing a secret. | Provider usage requirements and coverage still apply. |
| `ManualApiKeyImport` | Deep-link to the official page, securely import the returned key, verify permissions, and track lifecycle metadata. | An SDK or CLI that consumes a key is not evidence that it can issue the key. |
| `OAuthAuthorizationCodePkce` | Generate state and PKCE, open the system browser, receive a loopback/app callback, exchange tokens, and persist them securely. | Native-app best practice requires an external user agent; login and consent are human actions. |
| `OAuthDevice` | Display the provider-issued verification URI/code and poll with bounded cancellation and protocol-defined backoff. | It works only when the provider advertises support and still requires user approval on a browser-capable device. |
| `DynamicClientRegistration` | Register client metadata and manage the resulting client registration when the provider exposes and authorizes those endpoints. | It registers an OAuth client. It does not create a provider user account or personal API key. |

This means “no local browser search” is achievable through a direct official deep link, device
authorization where supported, or no-credential configuration. “No browser or human interaction”
cannot be promised for providers whose supported flow requires login, consent, CAPTCHA, terms
acceptance, or manual API-key issuance.

## GitHub Implementation Candidates

| ID | Repository | Authority and freshness | Key finding | Implementation implication |
| --- | --- | --- | --- | --- |
| GH-001 | [`cli/cli`](https://github.com/cli/cli) | Official GitHub CLI; MIT; active through 2026-07-22. | Its auth flow chooses device or browser authorization, validates browser URLs, presents/copies one-time codes, supports multiple accounts, and wraps keyring operations with timeouts. | Reference the orchestration boundary: non-secret account metadata in config/catalog, opaque secure-store references for tokens, bounded keyring calls, and browser fallback only when supported. |
| GH-002 | [`cli/oauth`](https://github.com/cli/oauth) | Official GitHub CLI OAuth library; MIT; active through 2026-07-21. | Implements RFC 8628 device authorization and a localhost web-flow fallback; it explicitly does not claim universal provider compatibility. | Put protocol choices behind declared provider capabilities and test provider-specific behavior rather than assuming every OAuth server supports both flows. |
| GH-003 | [`git-ecosystem/git-credential-manager`](https://github.com/git-ecosystem/git-credential-manager) | Official Git ecosystem credential manager; active through 2026-07-20; README states MIT. | Supports native macOS/Windows stores and multiple Linux stores, distinguishes browser/device auth modes, and documents that Linux has no universal default and some stores need an unlocked GUI session. | Make secure-store availability a typed activation prerequisite. Headless Linux needs an explicit supported backend or an encrypted fallback; plaintext must never be silently selected. |
| GH-004 | [`open-source-cooperative/keyring-rs`](https://github.com/open-source-cooperative/keyring-rs) | Maintained upstream Rust keyring project; Apache-2.0/MIT; active through 2026-07-20. | Exposes set/get/delete for text or binary secrets over native platform stores and permits selecting only required backends. | Strong Rust integration candidate. Compile only relevant backends, probe the actual store, retain only an opaque entry identifier in SQLite, and delete store entries during lifecycle closeout. |
| GH-005 | [`ramosbugs/oauth2-rs`](https://github.com/ramosbugs/oauth2-rs) | Maintained upstream Rust OAuth 2.0 library; Apache-2.0/MIT; active in 2026. | Supplies typed authorization-code/PKCE, device authorization, introspection, and revocation primitives; device polling handles pending, slow-down, interval, expiration, and timeout semantics. | Use as protocol plumbing behind a hardened adapter that enforces allowlisted issuers/endpoints, minimum scopes, cancellation, exact redirect binding, and bounded polling. It is not evidence of provider support or account issuance. |
| GH-006 | [`openid/AppAuth-Android`](https://github.com/openid/AppAuth-Android) | OpenID Foundation reference implementation; Apache-2.0; active in 2026. | Uses an external browser with PKCE, rejects embedded WebView authorization, persists authorization state, and supports RFC 7591 dynamic client registration when advertised/configured. | Use as a state-machine and DCR reference, not a Rust dependency. A static secret embedded in a native application is not confidential; DCR must be gated by provider metadata and policy. |
| GH-007 | [`coinbase/coinbase-advanced-py`](https://github.com/coinbase/coinbase-advanced-py) | Official Coinbase SDK; Apache-2.0; active through 2026-06-19. | Public clients work without credentials; authenticated clients accept a CDP/Exchange key and sign requests, but the SDK directs users to create the key in the portal. | Default Coinbase market-data onboarding to no credential. Treat private onboarding as manual-key import plus least-privilege verification; do not infer key-issuance capability from the SDK. |
| GH-008 | [`krakenfx/kraken-cli`](https://github.com/krakenfx/kraken-cli) | Official Kraken CLI; MIT; active through 2026-04-20. | Public and paper modes need no key; private setup consumes a key created in Kraken settings. It protects local configuration permissions and avoids logging secrets. | Default to public/paper configuration. Private activation remains a manual query-only-key import, with OS keyring storage stronger than copying the CLI's file-storage pattern. |
| GH-009 | [`docker/docker-credential-helpers`](https://github.com/docker/docker-credential-helpers) | Official Docker credential-helper suite; MIT; active through 2026-07-21. | Separates a stable credential-helper contract from macOS Keychain, Windows Credential Manager, Secret Service, and pass implementations. | Reference its backend boundary for a credential-store trait or optional helper process, while keeping Market Squawk functional without a mandatory external runtime. |
| GH-010 | [`RustCrypto/utils` (`zeroize`)](https://github.com/RustCrypto/utils/tree/master/zeroize) | RustCrypto utility maintained upstream; Apache-2.0/MIT for `zeroize`; active through 2026-07-22. | Provides explicit, compiler-resistant zeroing for secret-bearing memory with no insecure fallback. | Use short-lived redacted secret buffers and zeroize them after exchange/storage. This is defense in depth, not a substitute for native encrypted storage or lifecycle revocation. |

## Standards and Research Candidates

| ID | Standard or paper | Authority | Key finding | Implementation implication |
| --- | --- | --- | --- | --- |
| PAPER-001 | [RFC 8252: OAuth 2.0 for Native Apps](https://www.rfc-editor.org/info/rfc8252/) | IETF Best Current Practice, October 2017. | Native apps use an external user agent; public clients use PKCE; embedded user agents are prohibited. | Open the system browser for authorization-code flows and bind a loopback/app redirect with state and PKCE. Do not embed login pages or promise invisible authorization. |
| PAPER-002 | [RFC 8628: OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/info/rfc8628/) | IETF Proposed Standard, August 2019. | Defines device/user codes, verification URIs, polling interval, pending/slow-down responses, expiration, and user approval on a second device. | Implement only for providers that advertise it; polling must be bounded, cancellable, expiration-aware, and increase by five seconds on `slow_down`. |
| PAPER-003 | [RFC 7591: OAuth 2.0 Dynamic Client Registration](https://www.rfc-editor.org/info/rfc7591/) | IETF Proposed Standard, July 2015. | A registration endpoint accepts client metadata and returns a client identifier and possibly credentials; an initial access token may be required. | Model DCR as optional client-registration provisioning. Never represent it as provider account creation or personal API-key issuance. |
| PAPER-004 | [RFC 7592: OAuth 2.0 Dynamic Client Registration Management](https://www.rfc-editor.org/info/rfc7592/) | IETF Experimental RFC, July 2015. | Defines read, update, and delete operations plus registration access tokens and credential rotation. | Where a provider explicitly supports it, retain registration-management references in the secure store and support remote deletion/rotation before local cleanup. Preserve its Experimental status in design decisions. |
| PAPER-005 | [RFC 8414: OAuth 2.0 Authorization Server Metadata](https://www.rfc-editor.org/info/rfc8414/) | IETF Proposed Standard, June 2018. | Defines well-known metadata for issuer, endpoints, capabilities, and optional registration endpoint; returned issuer must exactly match the requested issuer. | Discover capabilities only from an allowlisted HTTPS issuer, require exact issuer equality, cache a digest/version of validated metadata, and reject unexpected endpoint drift. |
| PAPER-006 | [RFC 9700 / BCP 240: Best Current Practice for OAuth 2.0 Security](https://www.rfc-editor.org/info/rfc9700/) | IETF Best Current Practice, January 2025. | Updates OAuth threat guidance and deprecates insecure modes; emphasizes authorization-code protections, redirect validation, scope restriction, and robust token handling. | Base the portal OAuth profile on authorization code plus PKCE, exact redirects, strict state/issuer binding, minimum scopes, and supported refresh-token protections—not legacy implicit or password grants. |
| PAPER-007 | [RFC 7009: OAuth 2.0 Token Revocation](https://www.rfc-editor.org/info/rfc7009/) | IETF Proposed Standard, August 2013. | Defines revocation of refresh/access tokens and the client's need to tolerate token invalidation. | Deactivation should revoke remotely when the provider publishes the endpoint, record the result, then remove the local secure-store entry; local deletion alone is not complete revocation. |
| PAPER-008 | [NIST SP 800-63B-4: Authentication and Authenticator Management](https://www.nist.gov/publications/nist-sp-800-63b-4digital-identity-guidelines-authentication-and-authenticator) | NIST final publication, August 2025. | Defines lifecycle and assurance considerations for authenticators and authentication events. | Apply lifecycle discipline and reauthentication to sensitive local portal operations. Do not misclassify provider API keys as human authenticators or claim a NIST assurance level without satisfying the complete profile. |
| PAPER-009 | [A Comprehensive Formal Security Analysis of OAuth 2.0](https://arxiv.org/abs/1601.01229) | Fett, Küsters, and Schmitz; original formal analysis, 2016. | Found attacks in common OAuth assumptions and proves a hardened profile only when protocol participants and session values are bound correctly. | Treat authorization as a typed state machine binding provider, issuer, redirect, state, code, and local session; do not hand-roll loosely coupled callback logic. |
| PAPER-010 | [Universal Cross-app Attacks: Exploiting and Securing OAuth 2.0 in Integration Platforms](https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan) | Peer-reviewed USENIX Security 2025 paper. | Across 18 integration platforms, the authors found cross-app token and request-forgery classes caused by insufficient app differentiation. | Namespace callbacks and pending transactions by provider, adapter, account, client, redirect URI, and one-time state. A shared ambiguous callback or token slot is unsafe. |
| PAPER-011 | [Inconsistent, Incomplete, and Insecure: A Survey of Account Security Interfaces](https://www.usenix.org/conference/usenixsecurity26/presentation/bhattacharya) | USENIX Security 2026 accepted/prepublication research. | A study of 100 services found inconsistent access reporting and many spoofable or misleading device/location descriptions. | Show only evidence-backed provider, credential type, scopes, secure-store reference, and lifecycle timestamps. Do not overstate device, location, or access provenance that the provider did not authenticate. |
| PAPER-012 | [An Extensive Formal Security Analysis of the OpenID Financial-grade API](https://arxiv.org/abs/1901.11520) | Fett, Hosseyni, and Küsters; original formal analysis, 2019. | Even a high-assurance financial API profile required additional mitigations before its security properties could be proven. | If broker OAuth or execution authority is added, adopt a complete supported high-assurance profile and conformance tests rather than assembling selected controls ad hoc. |

## Implementation Implications

### 1. Provider capability and state contracts

Each adapter should declare its supported onboarding capabilities, required human step, official
entry URI, credential kind, minimum permissions, verification operation, rotation/revocation
mechanism, and account-cost classification. Runtime discovery may narrow those capabilities but
must not broaden the adapter's allowlisted set.

A credential lifecycle should be explicit, for example:

```text
Pending -> Active -> RefreshRequired | Expired -> Revoked -> Deleted
```

Transitions carry timestamps, provider/adapter identity, verification evidence, and typed failure
reasons. A provider requiring manual action should enter `PendingUserAction`, not loop or fabricate
an automation path.

### 2. Secret/reference separation

The local catalog should retain only non-secret metadata: provider and source IDs, adapter revision,
authorization method, pseudonymous account ID where available, granted scopes/permissions,
credential type, secure-store backend plus opaque entry ID, created/verified/refreshed/expires/
revoked/deleted timestamps, validated discovery/configuration digest, and verification-evidence
digest. Tokens, private keys, API secrets, device codes, registration access tokens, and refresh
tokens belong only in a native secure store or short-lived redacted memory.

Native-store access must be bounded and cancellable. Store unavailability, a locked store, denied
user presence, or an unsupported headless environment is an explicit activation state. No silent
plaintext fallback is acceptable.

### 3. OAuth transaction boundaries

Every transaction binds the allowlisted provider and issuer, adapter revision, client identifier,
redirect URI, requested scopes, state, PKCE verifier/challenge, and local pending-session ID.
Authorization-server metadata is accepted only from the expected HTTPS issuer and only when the
returned issuer matches exactly. Device polling follows provider and RFC interval/expiry behavior
and honors cancellation. Callback processing consumes pending state once.

Refresh, introspection, revocation, and dynamic registration are optional provider capabilities,
not universal assumptions. The portal records remote lifecycle outcomes and removes local secrets
only after the configured remote action is attempted or explicitly marked unavailable.

### 4. Provenance and user-visible evidence

Activation evidence should display what is known: official provider/issuer, authorization method,
credential type, actual scopes or key permissions, last successful verification, expiration,
adapter/configuration revision, and secure-store status. It should not display secret material or
infer device/location identity from weak provider text. Public/no-credential adapters still need
source, coverage, usage-policy, and health provenance.

## Excluded Sources and Non-Findings

| Source | Disposition | Reason |
| --- | --- | --- |
| [`fedspendingtransparency/fiscal-data`](https://github.com/fedspendingtransparency/fiscal-data) | Excluded from candidate inventory. | Official and active, but primarily the Fiscal Data web application; it adds no credential/onboarding implementation beyond the already selected official API documentation. |
| `openid/AppAuth-iOS` | Deduplicated. | Strong official reference, but its core external-agent/PKCE/DCR lessons duplicate GH-006; the Android repository was retained because its maintained README exposes all three in one reviewable source. |
| Unofficial FRED, BLS, SEC, and Treasury client libraries | Excluded. | They can demonstrate consumption, but cannot establish an official account/key-issuance or authorization capability. Provider/government documentation remains authoritative. |
| Older keyring forks and thin wrappers | Excluded. | GH-003, GH-004, and GH-009 cover the maintained native-store and backend-contract patterns without adding duplicate implementation evidence. |
| Blog posts, tutorials, and search snippets | Excluded. | Primary repositories, RFCs, vendor/government documentation, and original papers were available. |
| OAuth QR-login and generic password-manager papers | Excluded. | They do not define the provider-supported RFC 8628 flow or the local portal's credential contract. |

No official GitHub implementation was found that automatically creates retail Coinbase or Kraken
accounts or issues their personal API keys. No official FRED, BLS, SEC, or Treasury repository was
found that adds a supported account/key-issuance flow beyond the companion documentation findings.
No reviewed Rust OAuth library was found to establish provider support by itself; protocol support
in a library is not a provider capability declaration.

## Coverage Gaps

1. Provider-specific deep dives still must confirm exact callback schemes, OAuth discovery
   metadata, scopes, token lifetimes, revocation behavior, and DCR policy before enabling each
   OAuth capability. Today, the mandatory source set is principally no-credential or manual-key
   import.
2. A production implementation must test OS keyring behavior on each supported operating system,
   including locked stores, denied prompts, multi-user sessions, headless Linux, delete failures,
   and upgrade/migration. Repository documentation cannot substitute for platform verification.
3. The project must choose and document its encrypted local fallback, if any. This discovery
   establishes why plaintext is unacceptable but does not choose the key-derivation, encryption,
   recovery, or backup design.
4. `oauth2-rs` provides useful protocol primitives but does not implement a complete Market Squawk
   policy layer or prove RFC 7591 provider support. Dynamic registration would need a separately
   reviewed implementation if a required provider publishes that capability.
5. PAPER-011 is accepted 2026 conference material and should be treated as prepublication until
   final proceedings are available.

## Source List

### GitHub

1. `cli/cli`: https://github.com/cli/cli
   - Auth-flow source: https://github.com/cli/cli/blob/ae66a1c02e08366858f3070664f493afbe0cdf18/internal/authflow/flow.go
   - Keyring wrapper: https://github.com/cli/cli/blob/ae66a1c02e08366858f3070664f493afbe0cdf18/internal/keyring/keyring.go
   - Multiple-account model: https://github.com/cli/cli/blob/trunk/docs/multiple-accounts.md
2. `cli/oauth`: https://github.com/cli/oauth
3. Git Credential Manager: https://github.com/git-ecosystem/git-credential-manager
   - Credential stores: https://github.com/git-ecosystem/git-credential-manager/blob/main/docs/credstores.md
   - Configuration/auth modes: https://github.com/git-ecosystem/git-credential-manager/blob/main/docs/configuration.md
4. `keyring-rs`: https://github.com/open-source-cooperative/keyring-rs
5. `oauth2-rs`: https://github.com/ramosbugs/oauth2-rs
   - Reviewed source revision: https://github.com/ramosbugs/oauth2-rs/blob/72ce74401c26eb4dc85dcbfde587bbcfc149e3ae/oauth2/src/lib.rs
   - Device-flow implementation: https://github.com/ramosbugs/oauth2-rs/blob/72ce74401c26eb4dc85dcbfde587bbcfc149e3ae/oauth2/src/devicecode.rs
6. AppAuth for Android: https://github.com/openid/AppAuth-Android
7. Coinbase Advanced Python SDK: https://github.com/coinbase/coinbase-advanced-py
8. Kraken CLI: https://github.com/krakenfx/kraken-cli
9. Docker credential helpers: https://github.com/docker/docker-credential-helpers
10. RustCrypto `zeroize`: https://github.com/RustCrypto/utils/tree/master/zeroize

### Standards and papers

1. RFC 8252: https://www.rfc-editor.org/info/rfc8252/
2. RFC 8628: https://www.rfc-editor.org/info/rfc8628/
3. RFC 7591: https://www.rfc-editor.org/info/rfc7591/
4. RFC 7592: https://www.rfc-editor.org/info/rfc7592/
5. RFC 8414: https://www.rfc-editor.org/info/rfc8414/
6. RFC 9700 / BCP 240: https://www.rfc-editor.org/info/rfc9700/
7. RFC 7009: https://www.rfc-editor.org/info/rfc7009/
8. NIST SP 800-63B-4: https://www.nist.gov/publications/nist-sp-800-63b-4digital-identity-guidelines-authentication-and-authenticator
9. Fett, Küsters, and Schmitz (2016): https://arxiv.org/abs/1601.01229
10. Luo et al. (USENIX Security 2025): https://www.usenix.org/conference/usenixsecurity25/presentation/luo-kaixuan
11. Bhattacharya et al. (USENIX Security 2026): https://www.usenix.org/conference/usenixsecurity26/presentation/bhattacharya
12. Fett, Hosseyni, and Küsters (2019): https://arxiv.org/abs/1901.11520
