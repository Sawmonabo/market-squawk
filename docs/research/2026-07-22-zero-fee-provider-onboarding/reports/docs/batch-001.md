# Official Documentation Batch 001: Coinbase and Kraken Entry Boundaries


## Table of Contents

- [Batch Scope](#batch-scope)
- [Sources Reviewed](#sources-reviewed)
- [Findings](#findings)
- [Evidence Table](#evidence-table)
- [Limitations and Non-Findings](#limitations-and-non-findings)
- [Source List](#source-list)

## Batch Scope

This batch reviews the assigned Coinbase documentation set and the first two Kraken documentation
sources as of 2026-07-22. It establishes access, credential-issuance, permission, OAuth, rate,
rights, and lifecycle boundaries. It does not perform account creation, key issuance, OAuth, or a
provider runtime probe.

## Sources Reviewed

| ID | First-class source | Evidence role |
| --- | --- | --- |
| DOC-001 | [Coinbase Advanced Trade REST API](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api) | Public/private endpoint boundary |
| DOC-002 | [Coinbase API-key authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication) | Human key creation, JWT use, key lifecycle |
| DOC-003 | [Coinbase authorization and permissions](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization) | View/trade/transfer/receive authority |
| DOC-004 | [Coinbase key-permissions endpoint](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions) | Read-based permission and portfolio verification |
| DOC-005 | [Coinbase OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview) | Delegated-access and partner-approval boundary |
| DOC-006 | [Coinbase OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference) | Browser authorization, token, refresh, and revoke endpoints |
| DOC-007 | [Coinbase rate limiting](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting) | Product/user-specific limiting and `429` behavior |
| DOC-008 | [Coinbase official CLI/agent guide](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents) | CLI imports and verifies an already-created key |
| DOC-009 | [Coinbase Developer Platform terms](https://www.coinbase.com/legal/developer-platform/terms-of-service) | Mutable rights, limit, and product terms |
| DOC-010 | [Coinbase Exchange key rotation](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key) | Product-scoped manual replacement/revocation evidence |
| DOC-011 | [Kraken Exchange overview](https://docs.kraken.com/exchange/guides/overview) | Public/private REST and WebSocket boundary |
| DOC-012 | [Kraken REST API keys](https://docs.kraken.com/exchange/guides/rest/api-keys) | Minimum permissions, restrictions, and rotation guidance |

## Findings

1. **Confirmed:** Coinbase Advanced Trade and Kraken Spot expose public market-data operations that
   do not require a private credential. Private account and order operations cross a separate
   authentication boundary. [Coinbase API](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api),
   [Kraken overview](https://docs.kraken.com/exchange/guides/overview)
2. **Confirmed:** Coinbase App keys are created in a provider-controlled portal; the official CLI
   consumes an existing key and does not prove an API for creating a Coinbase user account or App
   key. [Coinbase authentication](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/api-key-authentication),
   [official CLI guide](https://docs.cdp.coinbase.com/get-started/build-with-ai/cdp-for-agents)
3. **Confirmed:** Coinbase provides a read endpoint exposing key capabilities and portfolio binding.
   **Inference:** a research-only activation should require `view`, bind the intended portfolio, and
   reject trade, transfer, or receive authority. [Authorization](https://docs.cdp.coinbase.com/coinbase-app/authentication-authorization/authorization),
   [key permissions](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/data-api/get-api-key-permissions)
4. **Confirmed:** Coinbase OAuth is a distinct delegated-access product with provider-controlled
   client eligibility and browser consent. The existence of OAuth endpoints does not establish a
   generally available native public-client registration for Market Squawk.
   [OAuth overview](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/overview),
   [OAuth reference](https://docs.cdp.coinbase.com/coinbase-app/oauth2-integration/reference)
5. **Confirmed:** Coinbase and Kraken document different rate models and `429`/counter behavior.
   **Inference:** limits must be versioned per product, protocol, endpoint class, and identity; a
   single provider-wide number is unsafe. [Coinbase rate limits](https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting),
   [Kraken key guide](https://docs.kraken.com/exchange/guides/rest/api-keys)
6. **Confirmed:** Coinbase Exchange rotation is explicitly product-scoped. It cannot be promoted to
   a universal Coinbase App lifecycle contract. [Coinbase Exchange rotation](https://help.coinbase.com/en/exchange/managing-my-account/how-to-rotate-your-api-key)
7. **Confirmed:** the reviewed Coinbase terms are mutable and do not create a permanent universal
   zero-fee or durable-use guarantee for every endpoint and downstream operation.
   [Coinbase terms](https://www.coinbase.com/legal/developer-platform/terms-of-service)

## Evidence Table

| Claim | Source IDs | Classification | Confidence | Release effect |
| --- | --- | --- | --- | --- |
| Public exchange data and private authority are separate surfaces | DOC-001, DOC-011 | Confirmed fact | High | Default public onboarding requests no secret |
| Coinbase key issuance is human/provider controlled | DOC-002, DOC-008 | Confirmed fact | High | Use resumable manual import |
| Coinbase research permission profile can be checked | DOC-003, DOC-004 | Fact plus engineering inference | High | Fail closed on excess authority |
| Coinbase OAuth cannot be assumed generally available | DOC-005, DOC-006 | Confirmed limitation | High | Capability remains disabled without exact admission |
| Rates cannot be represented by one provider-wide scalar | DOC-007, DOC-012 | Engineering inference from documented differences | High | Versioned multidimensional limiter |
| Exchange rotation evidence has a narrow product scope | DOC-010 | Confirmed limitation | High | Do not infer App-key lifecycle parity |
| Durable zero-fee/use rights are not established for every Coinbase surface | DOC-009 | Explicit non-finding | Medium | Rights remain a separate admission |

## Limitations and Non-Findings

- No reviewed source provides an official API that creates a Coinbase or Kraken user account.
- No reviewed source proves permanent zero-cost private eligibility in every jurisdiction.
- No account, key, OAuth client, token, or provider state was created or changed.
- Documentation does not substitute for runtime, permission, rate, or rights admission evidence.

## Source List

DOC-001 through DOC-012 are registered in `source-inventory.json` and assigned to
`docs-batch-001`; their access state and response digest or stable reference are recorded there.
