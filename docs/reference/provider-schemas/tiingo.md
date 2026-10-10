# Tiingo EOD and action response projection parsed by this application

Contents: [GET routes and envelopes](#get-routes-and-envelopes) · [Metadata](#metadata) · [DailyPrice](#dailyprice) · [Distribution](#distribution) · [Split](#split) · [Missing schema evidence](#missing-schema-evidence)

Scope: selected EOD/reference and distribution/split routes. This is not a complete Tiingo API specification.

[EOD documentation](https://www.tiingo.com/documentation/end-of-day), [distribution documentation](https://www.tiingo.com/documentation/corporate-actions/dividends), [split documentation](https://www.tiingo.com/documentation/corporate-actions/splits).
Source requests: [request.rs](../../../adapters/market-squawk-adapter-tiingo/src/request.rs), [corporate_actions.rs](../../../adapters/market-squawk-adapter-tiingo/src/request/corporate_actions.rs).

## GET routes and envelopes

| URL | Response | Continuation |
|---|---|---|
| `https://api.tiingo.com/tiingo/daily/{ticker}` | Metadata object | None |
| `https://api.tiingo.com/tiingo/daily/{ticker}/prices` | DailyPrice[] | Latest or startDate/endDate window |
| `https://api.tiingo.com/tiingo/corporate-actions/{ticker}/distributions` | Distribution[] | startExDate/endExDate window |
| `https://api.tiingo.com/tiingo/corporate-actions/splits` | Split[] for all returned tickers | exDate filter, not a ticker-only batch |
| `https://api.tiingo.com/tiingo/corporate-actions/distributions` | Documented batch distributions | exDate filter; not used by the current individual-symbol request |

No provider cursor is decoded. Application-created date windows are not provider pagination tokens. Non-success response bodies are preserved as bounded raw evidence; a stable typed error envelope is not established here.

**Presence boundary:** the local metadata/daily/action parsers require exact member sets. Several values can be JSON null. This establishes decoder acceptance, not upstream mandatory presence. Official metadata docs explicitly explain null coverage dates; dividend docs explain unavailable nullable dates and non-null exDate/amount. No original Tiingo example was inspected for this reference.

## Metadata

Root object. startDate/endDate are coverage dates, not event clocks; both null means unavailable price coverage in the current mapper. Date strings use calendar-date interpretation. Other optional/null members reflect local parsing unless documented above.

| Field | Wire value / local allowance | Meaning |
|---|---|---|
| `ticker` | string | Provider trading alias for this record |
| `name` | string | Security/fund display name |
| `exchangeCode` | string | Provider listing exchange identifier |
| `description` | string/null | Issuer/security business description when supplied |
| `startDate` | string/null | Earliest available daily-price coverage date |
| `endDate` | string/null | Latest available daily-price coverage date |

Source: [decoder.rs](../../../adapters/market-squawk-adapter-tiingo/src/decoder.rs).

## DailyPrice

Root array, prefix `$[]`. Daily date is a source date string; the local decoder validates its daily coordinate and increasing row order. OHLC/adjusted OHLC are numeric prices; volume/adjVolume are share-count fields documented as integer values, while the local parser preserves numeric decimal lexemes. divCash identifies ex-date distribution, splitFactor is an adjustment factor. Raw and adjusted values are independent fields.

| Field | Wire value / local allowance | Meaning |
|---|---|---|
| `date` | string | Daily source coordinate; date string parsed to a civil day |
| `open` | number/null | Unadjusted opening price, or mutual-fund NAV |
| `high` | number/null | Unadjusted daily highest price, or mutual-fund NAV |
| `low` | number/null | Unadjusted daily lowest price, or mutual-fund NAV |
| `close` | number/null | Unadjusted daily closing price, or mutual-fund NAV |
| `volume` | number/null | Unadjusted traded share count; fund NAV is not volume |
| `adjOpen` | number/null | Opening price after source adjustments |
| `adjHigh` | number/null | Daily highest price after source adjustments |
| `adjLow` | number/null | Daily lowest price after source adjustments |
| `adjClose` | number/null | Daily closing price after source adjustments |
| `adjVolume` | number/null | Traded share count after source split adjustments |
| `divCash` | number/null | Cash distribution on the ex-date represented by this row |
| `splitFactor` | number/null | Split adjustment ratio; separate from distribution amount |

Source: [decoder.rs](../../../adapters/market-squawk-adapter-tiingo/src/decoder.rs).

### Mutual-fund NAV uses the same wire object

Official EOD documentation states that supported mutual funds place daily NAV in open/high/low/close. There is no separate JSON nav field or dailyPrices wrapper. These same numeric fields remain traded EOD for exchange-traded securities. NAV identity/currency/availability are supplied separately by governed context. Local metadata and dailyPrices prefixes previously used in notes are **conceptual aliases**, not upstream response property names. The provider does not supply a revision ID or exact publication/finality clock in the audited body.

## Distribution

Prefix `$[]`. Native ex-date and optional payment/record/declaration dates are datetime strings interpreted as economic dates; no midnight publication event is invented. Amount is a source number; currency is not supplied in this reviewed response. Frequency codes: w weekly, bm bimonthly, m monthly, tm trimesterly, q quarterly, sa semiannual, a annual, ir irregular, f final, u unspecified, c cancelled.

| Field | Wire value / local allowance | Meaning |
|---|---|---|
| `permaTicker` | string | Stable provider security identifier preserved across alias changes |
| `ticker` | string | Provider trading alias for this record |
| `exDate` | string | Ex-entitlement/split economic date, distinct from receipt |
| `paymentDate` | string/null | Date cash distribution becomes payable |
| `recordDate` | string/null | Date determining holders of record |
| `declarationDate` | string/null | Date distribution was declared |
| `distribution` | number | Per-share cash distribution amount; currency not provided here |
| `distributionFrequency` | string | Source payment-frequency code; dictionary in section introduction |

Source: [corporate_actions.rs](../../../adapters/market-squawk-adapter-tiingo/src/decoder/corporate_actions.rs).

## Split

Prefix `$[]`. splitFrom/splitTo are original/new share quantities; splitFactor=splitTo/splitFrom. splitStatus a means active and c means cancelled. Native exDate is distinct from receipt time. The batch retains all returned ticker identities.

| Field | Wire value / local allowance | Meaning |
|---|---|---|
| `permaTicker` | string | Stable provider security identifier preserved across alias changes |
| `ticker` | string | Provider trading alias for this record |
| `exDate` | string | Ex-entitlement/split economic date, distinct from receipt |
| `splitFrom` | number | Pre-split share quantity in source ratio |
| `splitTo` | number | Post-split share quantity in source ratio |
| `splitFactor` | number | Split adjustment ratio; separate from distribution amount |
| `splitStatus` | string | Source active/cancelled split state, a/c |

Source: [corporate_actions.rs](../../../adapters/market-squawk-adapter-tiingo/src/decoder/corporate_actions.rs).

## Missing schema evidence

Upstream omission rules beyond explicitly documented null cases, immutable revisions/finality, precise publication time and stable error bodies remain unknown. Corporate-action amount currency is not in the audited wire schema. The current decoder does not establish the separate provider fund-fee, yield, fundamentals or real-time products. [Internal NAV contracts](internal-contracts.md) are not additional Tiingo fields.
