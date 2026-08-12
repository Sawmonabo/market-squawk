# Provider accounts and credential preparation

Use this runbook to create the accounts and keys required by the selected provider stack,
then fill the local credential template. It does not enable providers by itself.

| Field | Value |
| --- | --- |
| Document type | Operator setup runbook |
| Audience | Local Market Squawk owner/operator |
| Status | Account preparation is current; direct file import is required but not implemented |
| Last substantive review | 2026-08-11 |
| Implementation review basis | Provider documentation and capacity review current on 2026-08-11; repository audit base `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus preserved overlay |

## Important current boundary

The exact fill-in file is
[market-squawk-provider-credentials.env.example](../reference/market-squawk-provider-credentials.env.example).
Copy it outside this repository and fill placeholders there. The strict
`market-squawk-provider-credentials/v1` thin parser/entry point does not yet exist, so the current
application cannot consume the completed file. Several providers also still need adapters. Do not
source the file in a shell or put secrets in ordinary application environment variables.

The standard local path is
`~/.config/market-squawk/market-squawk-provider-credentials.env`; its containing directory must
exist and the file must be readable only by the local owner. The repository example and local file
must have the same 32 field names, including the schema identifier, but enabled flags and secret
values are operator-specific. Endpoint, callback, rate, batch, and cadence settings do not belong
in this credential file.

Market Squawk's target support is one thin parser/entry point in the existing provider-onboarding
and protected-secret-store path. It is not a new provider, adapter, crate, service, secret store, or
runtime configuration system. It accepts only documented fields, delegates them to the already
owning provider flows, discovers runtime account IDs/tokens there, returns one redacted receipt, and
rejects unknown, duplicate, or malformed input.

### What “enabled” and “available” mean

```text
filled file -> imported -> doctor/entitlement -> producing -> durably published
            -> typed application read -> composed Desktop/CLI/MCP workflow
```

An `*_ENABLED=true` field requests import or a bounded probe. It does not prove entitlement, start
a broad scheduler, or make a frontend capability available. The product may show a source as
Configured while its data remains Probe required, Setup required, Degraded, or Unavailable. Only
the complete chain above permits a workflow to be enabled.

For Schwab specifically, `SCHWAB_ENABLED=true` means the owner wants the optional read-only
market-data OAuth/doctor and currently admitted Schwab data lanes. It never authorizes account,
position, transaction, or order use. The product becomes Available only after current consent,
entitlement, raw/canonical publication, typed reads, and the focused workflow proof. If the owner
unlinks/revokes Schwab, its access and refresh generations are invalidated and Schwab-backed
workflows become Unavailable without affecting Alpaca or public sources. For IEX HIST, enablement
makes explicitly selected feed/date jobs eligible; it never starts a full archive.

## Account checklist

| Provider | What you must create | Template field | Current product status |
| --- | --- | --- | --- |
| Alpaca Paper Only / Basic | Free email-only Paper account and Paper key pair | `ALPACA_KEY_ID`, `ALPACA_SECRET_KEY`; realm is exactly `paper` | Substantive integration exists; this credential passed IEX snapshots and indicative-option REST, but recurring/historical/WebSocket release acceptance remains open |
| Yahoo adaptive enrichment | No account or key | Enabled for explicit-demand, runtime-admitted reads; broad WARM remains disabled | Adapter and normal-session acceptance remain incomplete; no numeric provider capacity is published |
| Nasdaq Trader, OCC, Cboe reference | No account or key | Enable flags only | Nasdaq equity/ETF listing foundation exists; Nasdaq option/bond lifecycle coverage and OCC/Cboe adapters remain incomplete |
| IEX HIST | No account or key | Enabled for explicitly selected, byte-admitted feed/date T+1 jobs | Cold PCAP adapter/storage benchmark absent; no automatic catalog download |
| Charles Schwab Trader API — Individual | Existing Schwab brokerage relationship, approved Individual app, and interactive OAuth consent | `SCHWAB_APP_KEY`, `SCHWAB_APP_SECRET`; callback and OAuth tokens are deliberately absent | Optional complementary provider; REST and five Streamer services passed a bounded read-only probe, while normal-session capacity and maintained adapter/workflow composition remain open |
| BLS | Free registered API v2 key | `BLS_REGISTRATION_KEY` | Provider foundation exists |
| BEA | Free API UserID | `BEA_USER_ID` | Adapter absent |
| Census | Free Data API key | `CENSUS_API_KEY` | Adapter absent |
| EIA | Free API v2 key | `EIA_API_KEY` | Adapter absent |
| FRED/ALFRED | Free FRED account and API key | `FRED_API_KEY` | v1 foundation exists; v2 release-bulk absent |
| Tiingo Starter | Optional free Starter account and token | `TIINGO_API_TOKEN` | Optional adapter/quota ledger absent |
| SEC | No key; truthful organization/name and monitored email | `SEC_USER_AGENT_ORGANIZATION`, `SEC_USER_AGENT_EMAIL` | Company filing/fact foundations exist; N-PORT/N-CEN absent |
| Treasury Fiscal Data/daily rates | No account or key | Enable flags only | Existing foundations |
| Federal Reserve Board | No account or key | Enable flag only | Direct adapter absent |

## 1. Alpaca Paper Only / Basic core

Official pages: [Paper Trading](https://docs.alpaca.markets/us/docs/paper-trading),
[Paper signup](https://app.alpaca.markets/signup), and
[market-data plans](https://docs.alpaca.markets/us/docs/about-market-data-api).

1. Create the free Paper Only account using the email-only path.
2. In the Paper dashboard, generate a Paper key pair and record the key ID and secret when shown.
3. Fill `ALPACA_KEY_ID` and `ALPACA_SECRET_KEY`.
4. Leave `ALPACA_TRADING_API_ENVIRONMENT="paper"` exactly as supplied by the template.

Do not use a live key in this free-core input. Market Squawk uses market-data hosts only and grants
no order authority. Current equities are IEX-only. The current Paper credential returned indicative
option-chain data and therefore admits a credential-generation-specific REST option lane; the
option WebSocket still needs its focused probe. Fixed income returned HTTP 403 and remains
Unavailable until a changed entitlement and fresh doctor prove otherwise.

## 2. No-account market research sources

No key is created for these sources:

- **Yahoo adaptive enrichment:** enabled for explicit interactive/watchlist reads admitted from
  current runtime health. No numeric watchlist, batch, WebSocket, or daily provider maximum is
  published, so the application owns bounded concurrency, caching, and the shared 429 circuit.
  It is never the broad scanner.
- **Nasdaq Trader:** current equity/ETF/bond and option-reference directories.
- **OCC and Cboe:** current listed option-product/series and daily reference files.
- **IEX HIST:** enabled as an available T+1 source for exact feed/date cold jobs. The flag never
  starts a full-catalog or automatic daily download; each job must be byte-admitted, resumable,
  and fail-closed until storage and decoder throughput are benchmarked.

These flags do not turn reference files into real-time quotes. The exact source roles are defined in
the [provider architecture](../architecture/market-data-provider-architecture.md).

## 3. Schwab owner-enabled read-only market data

Schwab is optional and account-backed. It complements the no-brokerage base; it is not required for
Market Squawk to work. Official Trader API documentation requires a Schwab brokerage relationship,
an approved Individual app, three-legged OAuth, user consent/account selection, and a registered
HTTPS callback. Access tokens last 30 minutes and refresh tokens seven days. Normal launches use
the protected refresh token; the browser flow returns only when consent, refresh expiry/revocation,
credential/security changes, or another documented restart condition requires it.

1. Keep only `SCHWAB_APP_KEY` and `SCHWAB_APP_SECRET` in the credential input.
2. Register Market Squawk's code-owned callback exactly as `https://127.0.0.1:8182` with no trailing
   slash. Do not add a callback field to the credential file.
3. Complete the Schwab authorization/consent page when the app requests it. A browser failure at the
   loopback URL is not surprising unless the bounded local callback listener is running.
4. Let Market Squawk exchange and rotate the complete token set in its protected secret store.
   Never paste an access or refresh token into the credential file.
5. The runtime may call only the code-owned `/marketdata/v1` read allowlist plus the minimum
   read-only `/trader/v1/userPreference` Streamer bootstrap. It discards unrelated preference
   fields and never calls account, position, transaction, order, preview, replace, or cancel paths.

**RUNTIME-MEASURED VALUE:** the configured app/account returned AAPL/MSFT/SPY quotes, a 350-contract
SPY chain, 35 expiration groups per side, 12,963 AAPL minute candles, equity/option/bond market
hours, AAPL fundamentals, `$DJI` movers, VTSAX, `$SPX`, `EUR/USD`, and `/ES`; quote probes returned
50/50, 100/100, 200/200, and 500/500 symbols. One authenticated Streamer session accepted login and
returned data for `LEVELONE_EQUITIES`, `NASDAQ_BOOK`, `CHART_EQUITY`, `LEVELONE_OPTIONS`, and
`OPTIONS_BOOK` after hours.

Those are dated account observations, not permanent provider limits. Official Schwab material
confirms one Streamer connection/user and that a symbol ceiling exists, but publishes no numeric
market-data REST rate, REST batch maximum, or Streamer symbol maximum. The maintained adapter must
therefore use one multiplexed socket, one adaptive provider queue, requested-versus-returned and
partial-result accounting, and a full normal-session soak before promising recurring breadth.

Official references: [Individual Developer role](https://developer.schwab.com/user-guides/individual-developer/about-individual-developer-role),
[OAuth restart versus refresh](https://developer.schwab.com/user-guides/apis-and-apps/oauth-restart-vs-refresh-token),
[Trader API OAuth/security](https://contentdelivery.schwab.com/api/content/rtcontent/asset/retail-trader-api-production--trader-api--individual--documentation),
and [Streamer contract](https://contentdelivery.schwab.com/api/content/rtcontent/asset/market-data-production--trader-api--individual--documentation).

## 4. BLS registered API v2

Official pages: [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm),
[registration](https://data.bls.gov/registrationEngine/), and
[v2 signature](https://www.bls.gov/developers/api_signature_v2.htm).

1. Open the registration page and supply the requested email, organization/name, and CAPTCHA.
2. Complete email verification and record the issued registration key.
3. Put it in `BLS_REGISTRATION_KEY` and renew it when BLS requires.

The data endpoint is `https://api.bls.gov/publicAPI/v2/timeseries/data/`.

## 5. BEA

Official pages: [BEA API signup](https://apps.bea.gov/api/signup/) and
[API guide](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf).

1. Register using a name or organization and valid email.
2. Activate the registration from the email BEA sends.
3. Put the issued 36-character UserID in `BEA_USER_ID`.

The code-owned endpoint is `https://apps.bea.gov/api/data`, with the credential sent as `UserID`.

## 6. Census Data API

Official pages: [Developer portal](https://www.census.gov/data/developers.html),
[free key request](https://api.census.gov/data/key_signup.html), and
[current guide](https://www.census.gov/data/developers/guidance/api-user-guide.html).

1. Request a free key using the official form.
2. Activate the key through the email link.
3. Put it in `CENSUS_API_KEY`.

The data root is `https://api.census.gov/data`; the key is a query parameter and must be redacted
from logs. Current Census guidance requires a key for Data API queries but publishes no numeric
ceiling for the selected keyed surfaces, so template pacing is Market Squawk policy.

## 7. EIA API v2

Official pages: [free key registration](https://www.eia.gov/opendata/register.php) and
[API v2 documentation](https://www.eia.gov/opendata/documentation.php).

1. Register with the requested name, email, organization category, and use description.
2. Record the key EIA emails and put it in `EIA_API_KEY`.
3. EIA requires `api_key` in the URL for API calls; Market Squawk must redact the entire query
   string. Use keyless official bulk files for large history where appropriate.

The API root is `https://api.eia.gov/v2`.

## 8. FRED and ALFRED

Official pages: [API key documentation](https://fred.stlouisfed.org/docs/api/api_key.html),
[FRED account key page](https://fredaccount.stlouisfed.org/apikeys),
[v1 API](https://fred.stlouisfed.org/docs/api/fred/), and
[v2 key documentation](https://fred.stlouisfed.org/docs/api/fred/v2/api_key.html).

1. Create or sign in to a FRED account.
2. Request a distinct API key for Market Squawk and record it.
3. Put it in `FRED_API_KEY`.

Market Squawk uses the same key as a redacted v1 `api_key` query parameter and a v2
`Authorization: Bearer` value. FRED/ALFRED is mandatory for V1; direct government sources
complement rather than replace its series, vintage, and release evidence.

**VERIFIED PROVIDER FACT:** v1 series observations use offset pagination with up to 100,000 rows
per page; the reviewed v1 pages publish no numeric request-rate ceiling. V2 release observations
use `has_more`/`next_cursor`, allow up to 500,000 rows per page, and document a 2-request/second
throttle. **APPLICATION POLICY:** Market Squawk uses one shared 1-request/second v1/v2 queue while
keeping each version's authentication and pagination contract separate.

## 9. Optional Tiingo Starter

Official pages: [signup/documentation](https://www.tiingo.com/documentation/general),
[token location](https://www.tiingo.com/kb/article/where-to-find-your-tiingo-api-token/), and
[pricing](https://www.tiingo.com/about/pricing).

1. Create a free Tiingo Starter account and verify the email.
2. Open `https://api.tiingo.com/account/api/token` and record the assigned token.
3. Set `TIINGO_ENABLED=true` and put it in `TIINGO_API_TOKEN` only if daily mutual-fund NAV or the
   curated independent EOD lane is required.

Tiingo is optional, but no other selected source fills supported-symbol daily mutual-fund NAV.
When it is disabled, that feature must report Unavailable.

## 10. SEC identifying User-Agent

Official pages: [EDGAR APIs](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
and [developer access guidance](https://www.sec.gov/about/developer-resources).

SEC's reviewed data APIs require no API key. Fill a truthful name/organization and monitored email
in `SEC_USER_AGENT_ORGANIZATION` and `SEC_USER_AGENT_EMAIL`. Market Squawk constructs the User-Agent
and prefers official bulk archives for large bootstrap work.

## 11. No-key government sources

- **Treasury Fiscal Data:** no account or key; use the
  [official API](https://fiscaldata.treasury.gov/api-documentation/).
- **Treasury daily-rate XML:** no account or key; the existing mandatory source owns all five
  admitted rate families.
- **Federal Reserve Board direct releases:** no account or key; use code-owned release descriptors,
  beginning with [H.15](https://www.federalreserve.gov/datadownload/Download.aspx?rel=H15).

## Completion checklist

```text
[ ] ALPACA_KEY_ID
[ ] ALPACA_SECRET_KEY
[x] ALPACA_TRADING_API_ENVIRONMENT = paper
[ ] SCHWAB_APP_KEY (optional owner-enabled read-only market data)
[ ] SCHWAB_APP_SECRET (optional owner-enabled read-only market data)
[ ] BLS_REGISTRATION_KEY
[ ] BEA_USER_ID
[ ] CENSUS_API_KEY
[ ] EIA_API_KEY
[ ] FRED_API_KEY
[ ] TIINGO_API_TOKEN (optional; required for daily mutual-fund NAV)
[ ] SEC_USER_AGENT_ORGANIZATION
[ ] SEC_USER_AGENT_EMAIL
[x] Yahoo adaptive enrichment: no key; explicit-demand enabled, broad WARM disabled
[x] Nasdaq Trader reference: no key
[x] OCC/Cboe option reference: no key
[x] IEX HIST: no key; selected feed/date cold jobs available, automatic archive disabled
[x] Treasury Fiscal Data: no key
[x] Treasury daily rates: no key
[x] Federal Reserve Board direct: no key
```

Completing the checklist closes credential collection only. It does not close the missing importer,
missing adapters, provider entitlements, authenticated probes, or the external data gaps documented
in the [provider architecture](../architecture/market-data-provider-architecture.md).
The shared canonical destinations and point-in-time evidence rules are in the
[canonical schema contract](../reference/market-data-canonical-schemas.md).
