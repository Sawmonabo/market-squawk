# Official Documentation Discovery Report


## Table of Contents

- [Research Scope](#research-scope)
- [Search Queries Used](#search-queries-used)
- [Decision Snapshot](#decision-snapshot)
- [Provider Onboarding Findings](#provider-onboarding-findings)
- [Local Secret Activation and OAuth Standards](#local-secret-activation-and-oauth-standards)
- [Candidate Sources](#candidate-sources)
- [Excluded Sources](#excluded-sources)
- [Coverage Gaps and Explicit Non-Findings](#coverage-gaps-and-explicit-non-findings)
- [Source List](#source-list)

## Research Scope

This discovery pass identifies official primary documentation worth assigning to deep-dive batches for mandatory Task 19A: a local Market Squawk onboarding portal for zero-fee providers. It is anchored to **2026-07-22** and to repository audit base `fe600f7c50af34482bb95feacacf6d0fdc2dbb03`.

The decision questions are: whether each provider has a useful no-account path; whether a free account, API key, OAuth grant, device grant, API, or official CLI can issue credentials; which steps necessarily involve a human; how a credential can be verified with minimum privilege; how it can be rotated or revoked; where it should be stored locally; and which rate or use terms constrain the design. Discovery used only documentation, standards, and legal pages controlled by the provider, government publisher, operating-system vendor, freedesktop.org, or the RFC Editor. No provider enrollment form was submitted, no login or authorization flow was started, no credential was created or tested, and no external state was changed.

“Not found” statements below are bounded non-findings from the reviewed official documentation, not proofs that an undocumented or private mechanism cannot exist. “Zero-fee” means that the documented onboarding path does not require a paid API subscription; it does not imply that trading, transfers, premium tiers, or all uses of an account are free.

## Search Queries Used

Search-result snippets were used only to locate candidate pages. Every factual statement retained below was checked against the directly opened official page.

1. `site:docs.cdp.coinbase.com Coinbase App public endpoints authentication API key OAuth scope revoke rate limit`
2. `site:help.coinbase.com Coinbase API key rotate revoke regenerate`
3. `site:docs.cdp.coinbase.com CDP CLI API key agents configure verify`
4. `site:coinbase.com/legal developer platform terms API limits`
5. `site:docs.kraken.com Exchange API key create permissions rate limits OAuth CLI`
6. `site:support.kraken.com create API key security rate limits`
7. `site:docs.kraken.com OAuth Embed B2B partner approval Kraken official`
8. `site:sec.gov EDGAR API no authentication user agent 10 requests second`
9. `site:fred.stlouisfed.org/docs/api api key v1 v2 login errors rate terms`
10. `site:fredhelp.stlouisfed.org FRED free account register API access`
11. `site:bls.gov/developers API registration key CAPTCHA quota terms`
12. `site:home.treasury.gov XML daily treasury interest rate feed API`
13. `site:fiscaldata.treasury.gov api documentation no account token license rate`
14. `site:developer.apple.com Security Keychain Services update delete accessibility user presence`
15. `site:learn.microsoft.com wincred Credential Management API CredWrite CredDelete DPAPI`
16. `site:specifications.freedesktop.org Secret Service latest prompt locked attributes draft`
17. `site:rfc-editor.org OAuth native apps device authorization security BCP revocation metadata dynamic client registration`

## Decision Snapshot

| Provider | Useful no-account baseline | Credentialed path | Official issuance automation found | Unavoidable human step | Minimum-risk verification | Rotation / revocation evidence |
| --- | --- | --- | --- | --- | --- | --- |
| Coinbase | Advanced Trade public market endpoints do not require authentication. | A Coinbase App API key is used for the owner’s account; delegated access uses OAuth. | The official CDP CLI imports/configures a portal-created key; it does not issue one. OAuth client creation is limited to approved partners. No retail device grant was found. | Portal login and API-key creation; browser login/consent for OAuth. | Authenticated read of `GET /api/v3/brokerage/key_permissions`, rejecting unexpected trade, transfer, or receive capability. | Coinbase App keys can be regenerated/disabled; OAuth exposes revocation and rotates refresh tokens. The reviewed Exchange rotation article describes manual delete/create and is product-scoped. |
| Kraken | Spot public REST/WebSocket market data and CLI paper mode can run without authentication. | A Kraken account API key is used for private Spot methods. | The official CLI consumes a key created in Kraken Settings. OAuth appears in the separately approved B2B Embed product, not the retail Spot onboarding docs. No retail device grant was found. | Kraken Pro login, permission selection, and key generation. | Authenticated read of `Get API Key Info`, checking permissions, restrictions, expiry, allowlist, and last use. | Official security guidance recommends creating replacement keys and deleting obsolete keys; the public docs do not expose a key-management API. |
| SEC EDGAR public data | Yes. `data.sec.gov` submissions/XBRL APIs and public archives need no API key. | None for read-only public-data ingestion. | Not applicable. EDGAR Next API tokens are for filing/management, not public market-data reads. | None, but the client must identify itself in `User-Agent`. | Availability/schema probe plus accession/filing identity checks; there is no credential to verify. | Not applicable. |
| FRED / ALFRED | The website is browsable without an account, but the documented v1 and v2 APIs require a key. | A free FRED account and per-application API key. | No supported key-issuance API, OAuth, device, or CLI flow was found. | Account registration/login and key request in the FRED account UI. | Inference: one minimal read request and explicit handling of missing/invalid-key errors; no key-introspection endpoint was found. | Public documentation found here does not specify an API for rotation/revocation. |
| BLS | Yes. Unregistered v1 requests work at lower quotas. | Optional v2 registration key for higher limits. | No issuance API, OAuth, device, or CLI flow was found. | Organization/email entry, CAPTCHA, terms acceptance, and retrieval of the emailed key. | Inference: one minimal v2 query with `registrationkey`; no introspection endpoint was found. | Keys renew yearly; public reset/revoke mechanics were not found. |
| U.S. Treasury | Yes. The daily interest-rate XML feed and Fiscal Data API are open. | None. | Not applicable. Fiscal Data explicitly says no account or token is required. | None. | Availability, schema/version, date, and data-sanity checks; there is no credential to verify. | Not applicable. |

The matrix is supported by Coinbase’s [Advanced Trade REST overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api), [API-key authentication guide](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication), [OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview), and [key-permissions endpoint](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions); Kraken’s [Exchange overview](https://docs.kraken.com/exchange/guides/overview), [API-key guide](https://docs.kraken.com/exchange/guides/rest/api-keys), [key creation procedure](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key), [key-info endpoint](https://docs.kraken.com/api/docs/rest-api/get-api-key-info), and [CLI guide](https://docs.kraken.com/home/cli); the SEC’s [EDGAR API page](https://www.sec.gov/search-filings/edgar-application-programming-interfaces); the FRED [v1](https://fred.stlouisfed.org/docs/api/api_key.html) and [v2](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html) key pages; the [BLS API FAQ](https://www.bls.gov/developers/api_FAQs.htm); and the Treasury [interest-rate feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) and [Fiscal Data API documentation](https://fiscaldata.treasury.gov/api-documentation/).

## Provider Onboarding Findings

### Coinbase

Coinbase supports the cleanest zero-secret baseline for this portal: the Advanced Trade REST reference labels its market endpoints public and says public endpoints require no authentication, while private endpoints require keys. The page also notes a one-second cache on public REST responses and directs lower-latency users to WebSocket or no-cache requests. [Coinbase Advanced Trade REST API](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api)

For access to the user’s own account, Coinbase documents a human CDP Portal flow: navigate to API keys, create a key, choose IP allowlisting and portfolio/permission restrictions, and copy the secret when it is presented. API access is disabled by default and can be enabled, regenerated, or disabled. The key authenticates JWT-bearing requests and must be stored securely. Coinbase distinguishes this from delegated access: accessing other users’ accounts requires OAuth. [Coinbase API-key authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication)

Least privilege is concrete rather than aspirational. Coinbase App key permissions include View, Trade, Transfer, and Receive, while OAuth has granular `service:resource:action` scopes. A market-data/onboarding portal should request only read/view access and fail closed if the returned key capability exceeds its declared need. [Coinbase authorization](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization) The authenticated `GET /api/v3/brokerage/key_permissions` endpoint returns `can_view`, `can_trade`, `can_transfer`, `can_receive`, and portfolio information, making it the strongest documented non-mutating activation check. [Get API Key Permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions)

OAuth is not a general retail escape hatch for Task 19A. Coinbase states that OAuth client creation is currently limited to approved partners. Its authorization-code flow sends a user to Coinbase login/authorization, so browser interaction and consent remain required even for an approved client. The reference documents token refresh with a newly returned refresh token and a revocation endpoint; the scopes guide says adding scopes requires reauthorization. [Coinbase OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview), [OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference), [OAuth scopes](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/scopes)

The official `cdp` CLI is useful for configuration, but its agent guide instructs the user to sign in to the Portal, create and download a JSON API key, and then import/configure it. It is therefore a credential consumer and verifier, not an official non-interactive issuance route. Its CDP/onchain product scope also must not be silently conflated with Coinbase App Advanced Trade. [CDP for agents](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents)

Coinbase App documents a default API-key/OAuth-user limit of 10,000 requests per hour and `429` on excess traffic. The current CDP terms, last modified 2026-06-23, permit Coinbase to establish or change API limits and prohibit circumventing them. [Coinbase App rate limiting](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting), [Coinbase Developer Platform Terms](https://www.coinbase.com/legal/developer-platform/terms-of-service) The Exchange help article says rotation is manual: revoke/delete the old key, create a least-privileged/IP-restricted replacement, copy its one-time secret, update the application, and verify it. Because that article is labeled Coinbase Exchange, it is supporting operational evidence rather than proof that every Coinbase App key has identical UI semantics. [How to rotate your API key](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key)

**Inference for Task 19A:** offer public Advanced Trade onboarding as the default with no secret at all. Put private Coinbase activation behind an explicit user choice, open only the documented Portal page, collect the generated credential into an OS secret store, call the permissions endpoint, and reject any write/transfer capability. Do not market OAuth or the CDP CLI as automated retail key issuance, and do not implement device flow unless Coinbase later publishes and supports it for this product.

### Kraken

Kraken’s Exchange overview separates a public market-data workflow—Spot REST `/0/public/*` and public WebSocket—from private account/order methods. Its API-key guide likewise says market data needs no key, then maps private use cases to permissions and recommends separate purpose-bound keys, IP allowlists, rotation, and minimum privileges. [Kraken Exchange overview](https://docs.kraken.com/exchange/guides/overview), [Kraken REST API keys](https://docs.kraken.com/exchange/guides/rest/api-keys)

The documented issuance route is human-controlled. Kraken’s current support procedure requires login to Kraken Pro, navigation to Settings → API, selection of permissions and optional restrictions, and generation of the key. The form supports IP whitelisting, expiration, time bounds, and optional API-key 2FA; Kraken warns users to store the private key securely. [How to create an API key](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key)

The strongest activation check is the private `Get API Key Info` endpoint. It requires no special API permission beyond a valid authenticated request and returns the key name, permissions, restrictions, expiration, IP allowlist, and last-used information. [Get API Key Info](https://docs.kraken.com/api/docs/rest-api/get-api-key-info) The official `kraken` CLI supports public and paper modes without authentication and an interactive `kraken setup`, but directs the user to Settings → API to generate credentials; it therefore configures/uses keys rather than issuing them. [Kraken CLI](https://docs.kraken.com/home/cli)

Kraken’s current official documentation index distinguishes retail Exchange from Embed and says OAuth is an Embed B2B capability whose access requires partner approval. That is positive evidence for a product boundary, not evidence for retail Spot OAuth. [Kraken Developers documentation index](https://docs.kraken.com/llms.txt) No retail Spot OAuth, device authorization grant, or public key-management API was found in the reviewed Exchange/CLI documentation.

The rate-limit support page, updated 2026-03-27, says public REST calls are limited by IP and currency pair and that one call per second or less remains within limits; private limits depend on the method and account state. The Exchange rate guide separately documents call counters and decay, so the client must model endpoint class rather than rely on one global number. [Kraken API rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-), [Kraken REST rate limits](https://docs.kraken.com/exchange/guides/rest/ratelimits) Security guidance recommends encrypted/password-manager storage, minimum permissions, 2FA where appropriate, regular replacement, and deletion of obsolete keys; Kraken may also revoke keys for security reasons or dormancy. [Kraken API key security](https://support.kraken.com/articles/api-key-security)

**Inference for Task 19A:** default to public Spot market data or CLI paper mode with no secret. For private activation, deep-link to the human key-generation page, require a query-only key, store it in the OS secret service, call `Get API Key Info`, and block activation on trading, funding, withdrawal, or unexpected network/time privileges. Do not treat B2B Embed OAuth as a generally available retail flow.

### SEC EDGAR

The SEC’s submissions and XBRL JSON APIs on `data.sec.gov` require neither authentication nor API keys. The SEC recommends bulk ZIP archives for large retrievals and notes that the data APIs do not support CORS, which favors a native/local backend rather than browser JavaScript calling them directly. [EDGAR Application Programming Interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)

Programmatic access must identify the client and remain within fair-access limits. The SEC’s webmaster FAQ permits automated downloading, sets a current maximum of 10 requests per second across machines, and asks clients to declare a `User-Agent` containing company name and an administrative contact address. The same page says EDGAR’s public content is free to access and reuse. [SEC Webmaster Frequently Asked Questions](https://www.sec.gov/about/webmaster-frequently-asked-questions)

EDGAR Next filer-user API tokens belong to filing and account-management workflows, not read-only public submissions/XBRL ingestion. Requiring such a token in Market Squawk would add needless authority and human account setup. [Create and Manage Filer User API Tokens](https://www.sec.gov/submit-filings/filer-support-resources/how-do-i-guides/create-manage-filer-user-api-tokens)

**Inference for Task 19A:** present SEC as “ready—no credential,” collect only the non-secret administrative contact needed to build a compliant `User-Agent`, and verify the adapter with a small schema/accession probe. Never ask a market-data user for an EDGAR Next filer token.

### FRED and ALFRED

FRED is the only government-data provider in this scope whose documented API baseline requires a user secret. Every v1 request needs a 32-character API key; the user must sign in to a FRED account to request or view it, and the page tells users to obtain distinct keys for distinct applications. V1 carries the key in a query parameter. [FRED API keys](https://fred.stlouisfed.org/docs/api/api_key.html) V2 also requires a key but uses an HTTP Bearer `Authorization` header, avoiding query-string exposure; v2 is a separate bulk-FRED surface rather than a drop-in replacement for every FRED/ALFRED v1 use. [FRED API v2 keys](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html)

FRED describes its account as free and lists API access among account features. Registration is a human web flow using email and password. [Register for a FRED account](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/) The v1 API also covers ALFRED real-time/vintage semantics under the same key model; the provider’s real-time-period documentation explains `realtime_start` and `realtime_end`. [FRED/ALFRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)

The error reference documents missing, invalid, or unregistered key failures and `429 Too Many Requests`, but the reviewed official pages do not publish a universal numeric quota. [FRED API errors](https://fred.stlouisfed.org/docs/api/fred/errors.html) No official OAuth, device grant, CLI issuance, dynamic key-registration API, key-introspection endpoint, or documented public rotation/revocation API was found.

The current FRED legal page is a material decision gate, not boilerplate. It permits the Bank to impose or adjust API limits, requires attribution and registration of a valid key, and assigns responsibility for third-party-series rights. More importantly for Market Squawk, the current terms prohibit using FRED content/API in machine-learning or AI development/training and prohibit storing, caching, archiving, database inclusion, and wholesale downloading of FRED content. [FRED Terms of Use, Privacy Policy, and Disclaimers](https://fred.stlouisfed.org/legal/)

**Inference for Task 19A:** the portal can technically support a human-created FRED key, preferably using a Bearer-header surface where the required endpoints exist, and validate it with one minimal read while redacting URLs and headers. However, persistent local ingestion, point-in-time archives, feature/model pipelines, and AI-facing tools must remain disabled unless a qualified legal/product review establishes a compliant use. This is the most consequential provider-specific blocker found in discovery.

### BLS

BLS offers a genuine credential-free baseline. Its FAQ documents unregistered v1 access at 25 queries per day, 25 series per query, 10 years per query, and 50 requests per 10 seconds. Registered v2 raises these to 500 queries per day, 50 series, 20 years, and the same 50 requests per 10 seconds. [BLS API Frequently Asked Questions](https://www.bls.gov/developers/api_FAQs.htm)

V2 key issuance is not automatable under the documented flow. The registration page asks for organization and email, requires a CAPTCHA and terms acceptance, and sends the key by email. The FAQ says the key must be renewed yearly. [BLS registration](https://data.bls.gov/registrationEngine/), [BLS API Frequently Asked Questions](https://www.bls.gov/developers/api_FAQs.htm) The v2 signature page shows the `registrationkey` field in the JSON POST body, providing a low-risk way to verify a supplied key with a minimal data query. [BLS API v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm)

BLS terms provide affirmative secondary-use language while requiring access-date citation, a quality/timeliness disclaimer, truthful representation, compliance with limits, and respect for third-party rights. [BLS Public Data API Terms of Service](https://www.bls.gov/developers/termsOfService.htm) No official account/password system, OAuth, device grant, CLI issuance, introspection, immediate reset, or revocation API was found in the reviewed BLS developer pages.

**Inference for Task 19A:** implement unregistered v1 and optional higher-quota v2 after binding the exact BLS provenance and official terms duties; release activation still requires a bounded runtime smoke. The portal may open the official registration page and resume after the CAPTCHA/email handoff. Treat annual renewal as lifecycle state and validate the imported key with one bounded POST.

### U.S. Treasury

Treasury’s daily interest-rate XML feed documents direct GET retrieval for daily Treasury par yields, bill rates, long-term rates, and real yields, including year/month filtering and pagination. It has no account, key, or authorization parameter. [Treasury Daily Interest Rate XML Feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)

The separate Fiscal Data API states explicitly that it is open and requires neither a user account nor token. Its documentation describes GET/JSON access, and its licensing section permits copying, adapting, and redistribution for commercial or noncommercial use without restriction. [Fiscal Data API Documentation](https://fiscaldata.treasury.gov/api-documentation/) The reviewed pages do not publish a numeric request ceiling. The Fiscal Data response-code table also mentions an invalid API key despite the same page’s no-token statement; this internal documentation inconsistency should be tested and monitored rather than “resolved” by inventing a key requirement.

**Inference for Task 19A:** present both Treasury adapters as ready without a credential. Validate a small GET for transport, schema, date, pagination, and plausible values; apply conservative concurrency, caching, retry/backoff, and endpoint-change monitoring despite the absence of a published numeric rate.

## Local Secret Activation and OAuth Standards

### OS-native secret stores

Apple Keychain Services is the native encrypted store for passwords and other small secrets. Its APIs add, find, update, and delete keychain items; separate accessibility controls let an application bind access to device-unlock/passcode state and, when requested, user presence. [Apple Keychain Services](https://developer.apple.com/documentation/security/keychain-services/), [Updating and deleting keychain items](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items), [Restricting keychain item accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility) A user confirmation prompt can therefore be an intended OS security boundary, not an error to bypass.

Windows Credential Management provides user-scoped storage and `CredWriteW`/`CredDeleteW` operations for creating, updating, and deleting credentials in the user’s logon session. DPAPI’s `CryptProtectData` is a lower-level fallback that normally binds decryption to the same user and machine; its `CRYPTPROTECT_LOCAL_MACHINE` flag broadens access to any user on the machine and should not be a default for per-user provider secrets. [Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management), [CredWriteW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew), [CredDeleteW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-creddeletew), [CryptProtectData](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata)

On Linux and other freedesktop desktops, Secret Service defines D-Bus collections/items, sessions, locking, prompting, create/replace, set-secret, and delete operations. It explicitly warns that item attributes are not secret and may be stored unencrypted; secrets must never be put in labels or attributes. Unlock/create/delete may return a Prompt that the application must complete or handle as dismissed. The current 0.2 publication is marked **DRAFT**, and the specification does not mandate one universal access-control policy, so behavior depends on the desktop implementation. [Secret Service API Draft](https://specifications.freedesktop.org/secret-service/latest-single/)

**Inference for Task 19A:** persist only a provider ID plus an opaque OS-secret handle in application state. The secret value must not appear in SQLite, config files, logs, crash reports, URLs, command lines, clipboard history, telemetry, or audit events. Activation should be a two-phase operation: write a candidate secret under a temporary handle; issue the minimum non-mutating provider verification; atomically promote the handle on success; and delete the candidate on failure. Where a provider permits overlapping credentials, rotation should retain the known-good handle until the replacement verifies, then swap and delete the old item. Where the documented provider order revokes the old key first, the portal must disclose the interruption and follow that order. Revocation should call a provider endpoint when one exists, delete the local item, and record only provider, handle/fingerprint, declared permissions, timestamps, actor, result, and correlation ID. If the OS store is unavailable or locked, fail closed rather than silently falling back to plaintext.

### Native-app, device, discovery, and revocation standards

RFC 8252 requires native apps to conduct OAuth authorization in an external user-agent, normally the system browser, and requires public native clients to use PKCE. It supports loopback redirects for desktop apps and explains why native apps cannot safely rely on an embedded client secret. [RFC 8252: OAuth 2.0 for Native Apps](https://www.rfc-editor.org/rfc/rfc8252)

RFC 8628 defines device authorization for input-constrained clients. It still requires a human to visit a verification URI, authenticate, enter/confirm a user code, and accept or decline access while the client polls within the published interval and `slow_down` rules. Server support is discoverable only when the grant and `device_authorization_endpoint` are advertised. [RFC 8628: OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/rfc/rfc8628)

RFC 9700, published 2025-01, is the current OAuth security BCP. It requires PKCE support, recommends authorization code over implicit, forbids the resource-owner-password grant, and requires public-client refresh-token replay detection through sender-constraining or rotation. [RFC 9700: Best Current Practice for OAuth 2.0 Security](https://www.rfc-editor.org/rfc/rfc9700) RFC 7009 defines an HTTPS token-revocation endpoint, while RFC 8414 defines authorization-server metadata for endpoints, scopes, grants, registration, revocation, and introspection. [RFC 7009: OAuth 2.0 Token Revocation](https://www.rfc-editor.org/rfc/rfc7009), [RFC 8414: OAuth 2.0 Authorization Server Metadata](https://www.rfc-editor.org/rfc/rfc8414)

RFC 7591 defines dynamic client registration but leaves issuance of any initial access token or software statement out of scope; a server may require those before registration. The RFC’s existence therefore does not authorize a client to self-register against an arbitrary provider. [RFC 7591: OAuth 2.0 Dynamic Client Registration Protocol](https://www.rfc-editor.org/rfc/rfc7591)

**Inference for Task 19A:** standards describe how to implement a provider-supported flow; they do not create provider support. Enable native-browser OAuth, device authorization, token revocation, metadata discovery, or dynamic registration only when the selected provider’s official product documentation or live signed metadata explicitly advertises that capability. For the reviewed retail Coinbase/Kraken surfaces, that gate is not met for a generally available device flow or automatic client/key issuance.

## Candidate Sources

Final selection and batch assignment are canonical in `source-inventory.json`. DOC-001 through
DOC-038 are assigned. DOC-039 through DOC-044 are excluded duplicates because the same RFCs are
registered once as PAPER-001 through PAPER-007 and classified as official standards documentation.
`P0` directly decides an onboarding path or release gate; `P1` supplies scoped operational detail.

| ID | Source | URL | Type | Credibility Signal | Freshness Signal | Priority | Rationale |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DOC-001 | Coinbase Advanced Trade REST API | [Official page](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api) | Product API documentation | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P0 | Establishes public/no-auth versus private/key boundary. |
| DOC-002 | Coinbase API-key authentication | [Official page](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication) | Authentication guide | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P0 | Defines human issuance, key format, JWT use, restrictions, storage, enable/disable/regenerate. |
| DOC-003 | Coinbase authorization and permissions | [Official page](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization) | Authorization guide | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P0 | Defines key permissions and granular OAuth scope model for least privilege. |
| DOC-004 | Coinbase Get API Key Permissions | [Official page](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions) | REST endpoint reference | Coinbase-generated API reference | Current live reference accessed 2026-07-22 | P0 | Supplies a documented read-only activation/introspection check. |
| DOC-005 | Coinbase OAuth overview | [Official page](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview) | OAuth product guide | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P0 | Establishes delegated-access use and approved-partner client restriction. |
| DOC-006 | Coinbase OAuth reference | [Official page](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference) | OAuth endpoint reference | Coinbase CDP-owned protocol reference | Current live docs accessed 2026-07-22 | P0 | Defines browser authorization, token exchange/refresh, rotation, and revoke endpoint. |
| DOC-007 | Coinbase App rate limiting | [Official page](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting) | API policy documentation | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P0 | Provides default key/OAuth quota and `429` behavior. |
| DOC-008 | CDP for agents / CLI | [Official page](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents) | Official CLI guide | Coinbase CDP-owned documentation | Current live docs accessed 2026-07-22 | P1 | Shows that the CLI imports a manually created portal key rather than issuing it. |
| DOC-009 | Coinbase Developer Platform Terms | [Official page](https://www.coinbase.com/legal/developer-platform/terms-of-service) | Provider legal terms | Coinbase legal domain | Last modified 2026-06-23 | P0 | Governs API-limit changes, circumvention, and continued platform use. |
| DOC-010 | Coinbase Exchange API-key rotation | [Official page](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key) | Official support procedure | Coinbase Help Center | Current page accessed 2026-07-22; explicit Exchange scope | P1 | Documents manual revoke/create/verify rotation while preserving product-boundary caveat. |
| DOC-011 | Kraken Exchange overview | [Official page](https://docs.kraken.com/exchange/guides/overview) | Product API guide | Kraken Developers-owned documentation | Current redesigned docs accessed 2026-07-22 | P0 | Establishes public market-data versus private-account workflow. |
| DOC-012 | Kraken REST API keys | [Official page](https://docs.kraken.com/exchange/guides/rest/api-keys) | Authentication/permission guide | Kraken Developers-owned documentation | Current redesigned docs accessed 2026-07-22 | P0 | Defines permissions, allowlisting, minimum privilege, separation, and rotation guidance. |
| DOC-013 | Kraken API-key creation | [Official page](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key) | Official support procedure | Kraken Support-owned article | Updated 2025-08-08 | P0 | Proves the human-controlled settings flow, restrictions, and secret handling. |
| DOC-014 | Kraken Get API Key Info | [Official page](https://docs.kraken.com/api/docs/rest-api/get-api-key-info) | REST endpoint reference | Kraken-generated API reference | Current live reference accessed 2026-07-22 | P0 | Provides read-only permissions/restrictions/expiry verification. |
| DOC-015 | Kraken CLI | [Official page](https://docs.kraken.com/home/cli) | Official CLI documentation | Kraken Developers and official CLI project | Current 2026 documentation accessed 2026-07-22 | P0 | Distinguishes no-auth public/paper modes from interactive configuration of UI-issued keys. |
| DOC-016 | Kraken API rate limits | [Official page](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-) | Official support policy | Kraken Support-owned article | Updated 2026-03-27 | P0 | Supplies public-IP/pair guidance and private limit context. |
| DOC-017 | Kraken API-key security | [Official page](https://support.kraken.com/articles/api-key-security) | Official security guidance | Kraken Support-owned article | Updated 2025-03-31 | P0 | Covers secure storage, minimum permissions, 2FA, replacement, deletion, and provider revocation. |
| DOC-018 | Kraken Developers documentation index | [Official page](https://docs.kraken.com/llms.txt) | Provider-published docs index | Served directly by docs.kraken.com and maps product families | Live index accessed 2026-07-22 | P1 | Records the retail Exchange versus approved B2B Embed OAuth boundary. |
| DOC-019 | SEC EDGAR APIs | [Official page](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) | U.S. government API documentation | SEC `.gov` primary source | Last reviewed 2025-04-08 | P0 | Establishes no-auth public APIs, update/bulk patterns, and CORS limitation. |
| DOC-020 | SEC Webmaster FAQ | [Official page](https://www.sec.gov/about/webmaster-frequently-asked-questions) | U.S. government access policy | SEC `.gov` primary source | Last reviewed 2024-08-23; current page accessed 2026-07-22 | P0 | Defines declared `User-Agent`, 10 requests/second, automated access, and reuse. |
| DOC-021 | FRED API keys (v1) | [Official page](https://fred.stlouisfed.org/docs/api/api_key.html) | Federal Reserve Bank API documentation | FRED primary documentation | Current page accessed 2026-07-22 | P0 | Defines account/key requirement, per-application key, and query-parameter transport. |
| DOC-022 | FRED API v2 keys | [Official page](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html) | Federal Reserve Bank API documentation | FRED primary documentation | Current v2 page accessed 2026-07-22 | P0 | Defines Bearer-key transport and v2 requirement. |
| DOC-023 | Register for a FRED account | [Official page](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/) | Provider account help | Federal Reserve Bank of St. Louis help site | Current page accessed 2026-07-22 | P0 | Confirms free account and unavoidable human registration. |
| DOC-024 | FRED API errors | [Official page](https://fred.stlouisfed.org/docs/api/fred/errors.html) | API error reference | FRED primary documentation | Current page accessed 2026-07-22 | P1 | Supports bounded key validation and records `429` without inventing a numeric limit. |
| DOC-025 | FRED legal terms | [Official page](https://fred.stlouisfed.org/legal/) | Provider legal terms | Federal Reserve Bank of St. Louis legal page | Current terms accessed 2026-07-22 | P0 | Material restrictions on caching/database use, AI/ML use, attribution, third-party rights, and limits. |
| DOC-026 | BLS API FAQ | [Official page](https://www.bls.gov/developers/api_FAQs.htm) | U.S. government API documentation | BLS `.gov` primary source | Last modified 2023-08-30; accessed 2026-07-22 | P0 | Defines no-key v1, optional-key v2, exact quotas, CAPTCHA/email issuance, renewal, and `429`. |
| DOC-027 | BLS registration | [Official page](https://data.bls.gov/registrationEngine/) | U.S. government registration form | BLS official data subdomain | Live form accessed read-only 2026-07-22 | P0 | Proves organization/email, CAPTCHA, and terms acceptance as human boundaries. |
| DOC-028 | BLS API v2 signatures | [Official page](https://www.bls.gov/developers/api_signature_v2.htm) | API request reference | BLS `.gov` primary source | Current page accessed 2026-07-22 | P0 | Defines the request-body key field for a minimal activation probe. |
| DOC-029 | BLS API terms | [Official page](https://www.bls.gov/developers/termsOfService.htm) | U.S. government terms | BLS `.gov` primary source | Last modified 2023-08-30; accessed 2026-07-22 | P0 | Provides affirmative secondary-use language plus citation/disclaimer, truthful-representation, limit-compliance, and third-party-rights duties. |
| DOC-030 | Treasury Daily Interest Rate XML Feed | [Official page](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed) | U.S. government data-feed documentation | Treasury `.gov` primary source | Current 2026 feed documentation accessed 2026-07-22 | P0 | Establishes direct no-auth rate data, filters, pagination, and response form. |
| DOC-031 | Fiscal Data API Documentation | [Official page](https://fiscaldata.treasury.gov/api-documentation/) | U.S. government API documentation | Treasury Fiscal Data primary site | Current page accessed 2026-07-22 | P0 | Explicitly establishes no account/token and open-use licensing. |
| DOC-032 | Apple Keychain Services | [Official page](https://developer.apple.com/documentation/security/keychain-services/) | OS security API documentation | Apple Developer primary documentation | Current platform docs accessed 2026-07-22 | P0 | Native encrypted small-secret store and retrieval integration pattern. |
| DOC-033 | Apple updating/deleting keychain items | [Official page](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items) | OS security API documentation | Apple Developer primary documentation | Current platform docs accessed 2026-07-22 | P0 | Supports atomic replacement and local deletion semantics. |
| DOC-034 | Apple keychain-item accessibility | [Official page](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility) | OS security API documentation | Apple Developer primary documentation | Current platform docs accessed 2026-07-22 | P0 | Defines lock/passcode/device-only/user-presence tradeoffs. |
| DOC-035 | Windows Credentials Management | [Official page](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management) | OS security API documentation | Microsoft Learn primary documentation | Current page accessed 2026-07-22 | P0 | Establishes user-session native credential storage. |
| DOC-036 | Windows CredWriteW / CredDeleteW family | [Official page](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew) | OS security API reference | Microsoft Learn primary API reference | Current page accessed 2026-07-22 | P0 | Supplies create/update operation; companion CredDeleteW supplies deletion. |
| DOC-037 | Windows CryptProtectData | [Official page](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata) | OS cryptographic API reference | Microsoft Learn primary API reference | Updated 2026-05-15 | P1 | Documents same-user/machine protection and dangerous broader-machine flag. |
| DOC-038 | freedesktop Secret Service API | [Official page](https://specifications.freedesktop.org/secret-service/latest-single/) | Desktop interoperability specification | freedesktop.org primary specification | Version 0.2 DRAFT, published 2026-04-08 | P0 | Defines Linux secret items, locks/prompts, replacement/deletion, and non-secret attributes. |
| DOC-039 | RFC 8252 OAuth for Native Apps | [Official page](https://www.rfc-editor.org/rfc/rfc8252) | IETF Best Current Practice | RFC Editor / IETF consensus publication | BCP 212; current status checked 2026-07-22 | P0 | Requires external browser, PKCE, and native redirect patterns. |
| DOC-040 | RFC 8628 Device Authorization Grant | [Official page](https://www.rfc-editor.org/rfc/rfc8628) | IETF standards-track RFC | RFC Editor / IETF consensus publication | Current status checked 2026-07-22 | P0 | Defines device flow, mandatory human action, polling/backoff, and discovery marker. |
| DOC-041 | RFC 9700 OAuth Security BCP | [Official page](https://www.rfc-editor.org/rfc/rfc9700) | IETF Best Current Practice | RFC Editor / IETF consensus publication | Published 2025-01 | P0 | Current PKCE, grant, token replay, refresh rotation, and metadata guidance. |
| DOC-042 | RFC 7009 Token Revocation | [Official page](https://www.rfc-editor.org/rfc/rfc7009) | IETF standards-track RFC | RFC Editor / IETF consensus publication | Current status checked 2026-07-22 | P1 | Defines provider-supported HTTPS token revocation semantics. |
| DOC-043 | RFC 8414 Authorization Server Metadata | [Official page](https://www.rfc-editor.org/rfc/rfc8414) | IETF standards-track RFC | RFC Editor / IETF consensus publication | Current status checked 2026-07-22 | P0 | Defines discovery of endpoints, grants, scopes, registration, revocation, and introspection. |
| DOC-044 | RFC 7591 Dynamic Client Registration | [Official page](https://www.rfc-editor.org/rfc/rfc7591) | IETF standards-track RFC | RFC Editor / IETF consensus publication | Current status checked 2026-07-22 | P1 | Defines automated client registration and its provider-controlled prerequisites/limits. |

## Excluded Sources

| Source | URL | Reason Excluded |
| --- | --- | --- |
| Search-engine result snippets | N/A | Discovery aid only; snippets were not treated as evidence. |
| Third-party Coinbase/Kraken wrappers, blog tutorials, and credential brokers | N/A | Not authoritative for provider-supported issuance, permissions, revocation, or terms. |
| SEC EDGAR Next filer token as an onboarding credential | [Official boundary page](https://www.sec.gov/submit-filings/filer-support-resources/how-do-i-guides/create-manage-filer-user-api-tokens) | Authoritative but out of scope for read-only public market-data ingestion; selecting it would add filing authority. |
| FRED demo/sample keys | [FRED API documentation](https://fred.stlouisfed.org/docs/api/fred/) | Examples are not user-specific production credentials and do not establish issuance, rotation, or acceptable use. |
| Legacy Treasury XML endpoint variants | [Treasury developer notice](https://home.treasury.gov/developer-notice-xml-changes) | Useful migration history, but the current documented feed and Fiscal Data API are the selected onboarding surfaces. |
| OAuth RFCs as proof that a provider supports a flow | [RFC Editor](https://www.rfc-editor.org/) | Standards define interoperable behavior; provider documentation or metadata must independently opt into each flow. |
| Plaintext config files, environment variables as long-lived storage, command-line flags, and clipboard automation | N/A | They do not meet the local-secret requirement and can leak through files, process listings, logs, shell history, or clipboard history. |

## Coverage Gaps and Explicit Non-Findings

1. **Provider absence claims are bounded.** The reviewed current retail [Coinbase App/Advanced Trade](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication) and [Kraken Exchange](https://docs.kraken.com/exchange/guides/rest/api-keys) docs did not reveal public device authorization, dynamic client registration, retail key-issuance APIs, or key-management APIs. Private, partner, institutional, or future mechanisms may exist. [Coinbase OAuth client creation](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview) and [Kraken Embed OAuth](https://docs.kraken.com/llms.txt) are approval-gated rather than generally available.
2. **Pricing is not uniformly explicit.** [Coinbase](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api), [Kraken](https://docs.kraken.com/exchange/guides/overview), and government public endpoints establish no-credential access, and [FRED](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/) explicitly calls its account free, but the selected Coinbase/Kraken pages do not amount to a durable promise that every account, endpoint, trading activity, or future quota is free. Release planning should recheck pricing and product eligibility at the refresh gate.
3. **FRED is a material policy blocker.** The current [FRED legal page](https://fred.stlouisfed.org/legal/) has AI/ML and storage/cache/database restrictions that appear to conflict with Market Squawk’s normal research, archival, feature, and AI-tooling patterns. Discovery does not provide legal interpretation; provider enablement requires qualified review or a deliberately constrained design.
4. **Numeric rate limits are incomplete.** [FRED](https://fred.stlouisfed.org/docs/api/fred/errors.html) documents `429` but no universal number in the reviewed pages, while the selected [Treasury](https://fiscaldata.treasury.gov/api-documentation/) pages publish no numeric ceiling. [Coinbase](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting) limits vary by API surface, and [Kraken](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-) has distinct public/private/trading counters. The portal must show surface-specific policy and use conservative bounded clients.
5. **Lifecycle endpoints are uneven.** Coinbase documents [key regeneration/disable](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication) and [OAuth revoke](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference); Kraken documents [manual replacement/deletion](https://support.kraken.com/articles/api-key-security); FRED and BLS public docs did not expose introspection, reset, revoke, or automated rotation APIs. [BLS](https://www.bls.gov/developers/api_FAQs.htm) documents annual renewal. A portal cannot promise one-click remote revocation where the provider does not document it.
6. **Key verification has different assurance.** [Coinbase](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions) and [Kraken](https://docs.kraken.com/api/docs/rest-api/get-api-key-info) have explicit permission/key-info endpoints. [FRED](https://fred.stlouisfed.org/docs/api/fred/errors.html) and [BLS](https://www.bls.gov/developers/api_signature_v2.htm) expose ordinary authenticated data calls and errors rather than an introspection endpoint; the proposed minimal-read verification is an inference and cannot prove all permissions or lifecycle state.
7. **Treasury documentation contains an inconsistency.** [Fiscal Data](https://fiscaldata.treasury.gov/api-documentation/) says no token is required while its response-code table mentions an invalid API key. A batch deep dive should reproduce a read-only anonymous request and preserve the response as exact-date evidence.
8. **Desktop secret behavior varies.** [Apple](https://developer.apple.com/documentation/security/keychain-services/) and [Windows](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management) APIs are platform-specific; [freedesktop Secret Service](https://specifications.freedesktop.org/secret-service/latest-single/) is a draft interoperability specification whose prompts and access policy depend on the installed implementation. Headless Linux and locked/unavailable stores need an explicit unsupported or user-remediated state, not plaintext fallback.
9. **OAuth client-secret suitability remains provider-specific.** [RFC 8252](https://www.rfc-editor.org/rfc/rfc8252) explains the native/public-client constraint. Coinbase’s [OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference) describes a client secret, while approved-client registration details were not available for evaluation. Any future OAuth implementation needs provider confirmation of native/public-client and PKCE support.
10. **Terms refresh is mandatory.** Legal pages, quotas, partner eligibility, and help-center flows are mutable. Refresh DOC-007, DOC-009, DOC-016, DOC-020, DOC-025, DOC-026, DOC-029, and DOC-031 immediately before implementation approval and again before release-gate approval.
11. **Two official pages were intermittently unavailable during final link checking.** The Kraken [`Get API Key Info`](https://docs.kraken.com/api/docs/rest-api/get-api-key-info) URL returned an HTTP 500 during the final automated check after being directly reviewed earlier in discovery, and the live [BLS registration form](https://data.bls.gov/registrationEngine/) timed out. Both canonical official URLs remain selected, but the batch reader should retry them and preserve exact-date availability evidence.

## Source List

All sources were accessed on 2026-07-22 unless a different publication/update date is stated.

### Coinbase

- [Advanced Trade REST API](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api)
- [API-key authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication)
- [Authorization and permissions](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization)
- [Get API Key Permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions)
- [OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview)
- [OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference)
- [OAuth scopes](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/scopes)
- [Coinbase App rate limiting](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting)
- [CDP for agents](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents)
- [Coinbase Developer Platform Terms of Service](https://www.coinbase.com/legal/developer-platform/terms-of-service) — last modified 2026-06-23.
- [How to rotate your API key](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key)

### Kraken

- [Exchange overview](https://docs.kraken.com/exchange/guides/overview)
- [REST API keys](https://docs.kraken.com/exchange/guides/rest/api-keys)
- [REST authentication](https://docs.kraken.com/exchange/guides/rest/authentication)
- [How to create an API key](https://support.kraken.com/articles/360000919966-how-to-create-an-api-key) — updated 2025-08-08.
- [Get API Key Info](https://docs.kraken.com/api/docs/rest-api/get-api-key-info)
- [Kraken CLI](https://docs.kraken.com/home/cli)
- [Kraken API rate limits](https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-) — updated 2026-03-27.
- [REST rate limits](https://docs.kraken.com/exchange/guides/rest/ratelimits)
- [API key security](https://support.kraken.com/articles/api-key-security) — updated 2025-03-31.
- [Kraken Developers documentation index](https://docs.kraken.com/llms.txt)

### SEC EDGAR

- [EDGAR Application Programming Interfaces](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) — last reviewed 2025-04-08.
- [SEC Webmaster Frequently Asked Questions](https://www.sec.gov/about/webmaster-frequently-asked-questions) — last reviewed 2024-08-23.
- [Create and Manage Filer User API Tokens](https://www.sec.gov/submit-filings/filer-support-resources/how-do-i-guides/create-manage-filer-user-api-tokens) — product-boundary source, not selected for read-data onboarding.

### FRED and ALFRED

- [FRED API keys](https://fred.stlouisfed.org/docs/api/api_key.html)
- [FRED API v2 keys](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html)
- [Register for a FRED account](https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/)
- [FRED/ALFRED real-time periods](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)
- [FRED API errors](https://fred.stlouisfed.org/docs/api/fred/errors.html)
- [FRED Terms of Use, Privacy Policy, and Disclaimers](https://fred.stlouisfed.org/legal/)

### BLS

- [BLS API Frequently Asked Questions](https://www.bls.gov/developers/api_FAQs.htm) — last modified 2023-08-30.
- [BLS API registration](https://data.bls.gov/registrationEngine/)
- [BLS API v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm)
- [BLS Public Data API Terms of Service](https://www.bls.gov/developers/termsOfService.htm) — last modified 2023-08-30.

### U.S. Treasury

- [Treasury Daily Interest Rate XML Feed](https://home.treasury.gov/treasury-daily-interest-rate-xml-feed)
- [Fiscal Data API Documentation](https://fiscaldata.treasury.gov/api-documentation/)
- [Treasury Developer Notice: XML Changes](https://home.treasury.gov/developer-notice-xml-changes) — lifecycle context only.

### OS secret stores

- [Apple Keychain Services](https://developer.apple.com/documentation/security/keychain-services/)
- [Apple: Updating and deleting keychain items](https://developer.apple.com/documentation/security/updating-and-deleting-keychain-items)
- [Apple: Restricting keychain item accessibility](https://developer.apple.com/documentation/security/restricting-keychain-item-accessibility)
- [Windows Credentials Management](https://learn.microsoft.com/en-us/windows/win32/secauthn/credentials-management)
- [Windows CredWriteW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-credwritew)
- [Windows CredDeleteW](https://learn.microsoft.com/en-us/windows/win32/api/wincred/nf-wincred-creddeletew)
- [Windows CryptProtectData](https://learn.microsoft.com/en-us/windows/win32/api/dpapi/nf-dpapi-cryptprotectdata) — updated 2026-05-15.
- [freedesktop Secret Service API 0.2 DRAFT](https://specifications.freedesktop.org/secret-service/latest-single/) — published 2026-04-08.

### OAuth / IETF

- [RFC 8252: OAuth 2.0 for Native Apps](https://www.rfc-editor.org/rfc/rfc8252) — BCP 212, published 2017-10.
- [RFC 8628: OAuth 2.0 Device Authorization Grant](https://www.rfc-editor.org/rfc/rfc8628) — published 2019-08.
- [RFC 9700: Best Current Practice for OAuth 2.0 Security](https://www.rfc-editor.org/rfc/rfc9700) — BCP 240, published 2025-01.
- [RFC 7009: OAuth 2.0 Token Revocation](https://www.rfc-editor.org/rfc/rfc7009) — published 2013-08.
- [RFC 8414: OAuth 2.0 Authorization Server Metadata](https://www.rfc-editor.org/rfc/rfc8414) — published 2018-06.
- [RFC 7591: OAuth 2.0 Dynamic Client Registration Protocol](https://www.rfc-editor.org/rfc/rfc7591) — published 2015-07.
