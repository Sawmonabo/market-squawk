# Charles Schwab REST response projection parsed by this application

Contents: [Endpoint map](#endpoint-map) · [QuoteRecord](#quoterecord) · [QuoteComponent dictionary](#quotecomponent-dictionary) · [Reference](#reference) · [Fundamental](#fundamental) · [Instrument](#instrument) · [Option-chain envelope](#option-chain-envelope) · [Option contract](#option-contract) · [Expiration](#expiration) · [Price-history envelope](#price-history-envelope) · [Candle](#candle) · [Market hours](#market-hours) · [Mover](#mover) · [Known missing evidence](#known-missing-evidence)

Base: `https://api.schwabapi.com/marketdata/v1`. Methods below are GET. Scope: the selected market-data routes and current native/semantic decoders, not the whole upstream REST API. [Official portal](https://developer.schwab.com/products/trader-api--individual/details/documentation/Market%20Data%20APIs) did not expose a complete public REST response specification during this audit.

**Evidence:** exact keys below come from [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs). Historical Instruments originals establish five string fields. Other REST families have parser evidence only. The parser preserves absent/null/value states, without proving upstream optionality.

¹ scalar is the local NativeScalar allowance: null/boolean/number/string. It is not a provider-declared type. The quote dictionary is reused for regular, extended and chain-underlying blocks; its accepted keys are not a guarantee of each upstream block's field membership.

## Endpoint map

| Path | Request / response shape | Paging |
|---|---|---|
| /quotes; /{symbol}/quotes | symbols,fields,indicative; symbol map | No cursor decoded |
| /chains | symbol/strategy/expiry/strike filters; call/put maps | Single nested response |
| /expirationchain | symbol; expirationList array | No cursor decoded |
| /pricehistory | period/frequency/date range/extended-hours/previous-close inputs; candles | Date-window requests |
| /markets; /markets/{market} | markets/date; market-type/product map | No cursor decoded |
| /movers/{symbol} | sort/frequency; screeners array | No cursor decoded |
| /instruments; /instruments/{cusip} | symbol/projection or CUSIP; instrument array | No cursor decoded |

Source: [request.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/request.rs). Active quotes request fields=quote; parser support for Fundamental/Reference/Regular does not establish request or receipt. Stable error JSON bodies remain unverified; status and bounded raw body are retained.

## QuoteRecord

Root `{symbol: QuoteRecord}`. A supplied symbol must match its enclosing key. Record blocks: quote, regular, extended, reference, fundamental. Local parsing admits absent/null blocks.

| Field | Parser value type | Meaning |
|---|---|---|
| `symbol` | string | Provider trading identifier; distinct from canonical identity |
| `assetMainType` | string | Provider broad asset classification; complete enum unverified |
| `assetSubType` | string | Provider asset subtype; complete enum unverified |
| `realtime` | boolean | Source real-time flag; not by itself entitlement or freshness evidence |
| `ssid` | unsigned integer | Provider security identifier; identifier namespace not independently documented |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs).

## QuoteComponent dictionary

Mounts: `$.{symbol}.quote`, `$.{symbol}.regular`, `$.{symbol}.extended`, `$.underlying` in chains. This complete local shared dictionary is not four independently verified upstream schemas. REST component clocks remain native scalars; upstream encodings are not proven by the shared dictionary. Exact price/size scalars are retained. Session-specific field membership/types and requiredness remain unverified.

| Field | Parser value type | Meaning |
|---|---|---|
| `52WeekHigh` | scalar¹ | Highest reported price in the 52-week lookback |
| `52WeekLow` | scalar¹ | Lowest reported price in the 52-week lookback |
| `askMICId` | scalar¹ | Market Identifier Code of ask venue |
| `askPrice` | scalar¹ | Offered ask price |
| `askSize` | scalar¹ | Ask-side quoted quantity; REST trading unit unverified |
| `askTime` | scalar¹ | Ask-side update clock; REST encoding unverified |
| `bidMICId` | scalar¹ | Market Identifier Code of bid venue |
| `bidPrice` | scalar¹ | Offered bid price |
| `bidSize` | scalar¹ | Bid-side quoted quantity; REST trading unit unverified |
| `bidTime` | scalar¹ | Bid-side update clock; REST encoding unverified |
| `closePrice` | scalar¹ | Reported closing/reference price; session basis unverified |
| `highPrice` | scalar¹ | Reported session high; session boundary unverified |
| `lastMICId` | scalar¹ | Market Identifier Code of last execution venue |
| `lastPrice` | scalar¹ | Most recent reported execution price |
| `lastSize` | scalar¹ | Quantity of the most recent execution; unit unverified |
| `lowPrice` | scalar¹ | Reported session low; session boundary unverified |
| `mark` | scalar¹ | Provider mark price; computation rule not established here |
| `markChange` | scalar¹ | Reported change of mark relative to provider baseline |
| `markPercentChange` | scalar¹ | Reported percentage change of mark; scale/baseline unverified |
| `netChange` | scalar¹ | Reported absolute price change from provider baseline |
| `netPercentChange` | scalar¹ | Reported percentage price change; scale/baseline unverified |
| `openPrice` | scalar¹ | Reported opening price; session boundary unverified |
| `postMarketChange` | scalar¹ | Post-market absolute change; exact baseline unverified |
| `postMarketPercentChange` | scalar¹ | Post-market percentage change; scale/baseline unverified |
| `quoteTime` | scalar¹ | Quote update clock, independent of trade time; REST encoding unverified |
| `securityStatus` | scalar¹ | Source security/trading state code; complete enum unverified |
| `totalVolume` | scalar¹ | Reported traded quantity; accumulation interval and unit need source context |
| `tradeTime` | scalar¹ | Last execution clock; REST encoding unverified |
| `volatility` | scalar¹ | Provider volatility estimate; model and scaling unverified |

Semantic decoder constraints are narrower than scalar¹: bidPrice/askPrice and bidSize/askSize must be JSON numbers when present for canonical quote mapping; boolean/text values produce a semantic type error. Sizes remain an unresolved unit until separate reference context supplies it. These are application constraints, not evidence of official REST types.
Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Reference

Mount `$.{symbol}.reference`. Listing/security and asset-specific option/bond terms remain explicit source fields.

| Field | Parser value type | Meaning |
|---|---|---|
| `cusip` | scalar¹ | CUSIP security identifier |
| `description` | scalar¹ | Provider security display description |
| `exchange` | scalar¹ | Listing/trading exchange code |
| `exchangeName` | scalar¹ | Exchange display name |
| `isHardToBorrow` | scalar¹ | Source hard-to-borrow eligibility flag; exact REST type/enum unverified |
| `isShortable` | scalar¹ | Source short-sale eligibility flag; not trading authorization |
| `htbQuantity` | scalar¹ | Reported hard-to-borrow availability quantity; unit unverified |
| `htbRate` | scalar¹ | Reported borrow rate; rate period/scaling unverified |
| `contractType` | scalar¹ | Call/put or asset-specific contract code; complete enum unverified |
| `daysToExpiration` | scalar¹ | Remaining time to expiration in days |
| `expirationDay` | scalar¹ | Civil expiration day of month |
| `expirationMonth` | scalar¹ | Civil expiration month |
| `expirationYear` | scalar¹ | Civil expiration year |
| `multiplier` | scalar¹ | Contract economic multiplier; not interchangeable with quoted size |
| `settlementType` | scalar¹ | Source settlement code; complete enum unverified |
| `strikePrice` | scalar¹ | Option exercise price |
| `underlying` | scalar¹ | Underlying security identifier |
| `futureActiveSymbol` | scalar¹ | Provider alias of the active futures contract |
| `futureExpirationDate` | scalar¹ | Futures expiration coordinate; raw encoding unverified |
| `futureIsActive` | scalar¹ | Source active-futures flag; exact REST type/enum unverified |
| `product` | scalar¹ | Source product classification; complete enum unverified |
| `tradingHours` | scalar¹ | Source trading-hours description; format unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [request.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/request.rs).

## Fundamental

Mounts `$.{symbol}.fundamental` and `$.instruments[].fundamental`. EPS/P-E basis, yield scale, market-cap currency/time and provider enum sets are not established by inspected originals. This is the complete local dictionary, not a financial-statement API.

| Field | Parser value type | Meaning |
|---|---|---|
| `avg10DaysVolume` | scalar¹ | Reported average traded volume over ten days; unit/calendar basis unverified |
| `avg1YearVolume` | scalar¹ | Reported average traded volume over one year; unit/calendar basis unverified |
| `declarationDate` | scalar¹ | Dividend declaration date, distinct from ex/payment dates |
| `divAmount` | scalar¹ | Reported dividend amount; currency and annual/per-payment basis unverified |
| `divExDate` | scalar¹ | Reported ex-dividend date |
| `divFreq` | scalar¹ | Dividend payment-frequency code; complete enum unverified |
| `divPayAmount` | scalar¹ | Dividend payment amount; currency unverified |
| `divPayDate` | scalar¹ | Dividend payment date |
| `divYield` | scalar¹ | Reported dividend yield; percentage scaling unverified |
| `eps` | scalar¹ | Reported earnings per share; fiscal/trailing basis and currency unverified |
| `fundLeverageFactor` | scalar¹ | Reported fund leverage target; horizon and sign convention unverified |
| `fundStrategy` | scalar¹ | Provider fund strategy code/description; complete enum unverified |
| `nextDivPayDate` | scalar¹ | Next reported dividend payment date |
| `nextDivExDate` | scalar¹ | Next reported ex-dividend date |
| `peRatio` | scalar¹ | Reported price-to-earnings multiple; price/EPS period basis unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Instrument

Mount `$.instruments[]`; local decoder also admits a bare single record. At least symbol or CUSIP must have a value. Historical originals contain cusip/symbol/description/exchange/assetType strings, omit fundamental, and include classification value ETF. They are quarantined historical evidence, not current accepted data. Receipt proof was rechecked from retained MSJ1 length/CRC, source identifier and original-body SHA-256. Anonymous receipt coordinates: 2026-10-09T04:57:56.954Z / `152d33d35fc8ba4587af8ceba1f71b0bf20623c4f58efa7bf0f5390cdd5af07e`; 2026-10-09T04:57:57.821Z / `2fb9d99150f6b93ba23a50aa8d0cad7742935fd6c7c7771f897c33186fc7e8da`. Hashes identify historical bodies without instrument identifiers or dependencies on local ignored files.

| Field | Parser value type | Meaning |
|---|---|---|
| `cusip` | string | CUSIP security identifier |
| `symbol` | string | Provider trading identifier; distinct from canonical identity |
| `description` | string | Provider security display description |
| `exchange` | string | Listing/trading exchange code |
| `assetType` | string | Source instrument classification; observed historical value ETF |
| `bondFactor` | scalar¹ | Source bond principal factor; exact definition unverified |
| `bondMultiplier` | scalar¹ | Source bond pricing multiplier; exact definition unverified |
| `bondPrice` | scalar¹ | Source bond price; quotation convention unverified |
| `type` | scalar¹ | Additional source instrument-type code; enum unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs).

## Option-chain envelope

Root contains callExpDateMap/putExpDateMap; each is `{expirationGroup: {strikeText: Contract[]}}`. Source group keys are preserved; the parser checks strike text as a decimal. numberOfContracts, when supplied, must match parsed rows. The expiration:days grammar is conventional but not enforced by this parser.

| Field | Parser value type | Meaning |
|---|---|---|
| `symbol` | string | Provider trading identifier; distinct from canonical identity |
| `status` | string | Source operation/status code; complete enum unverified |
| `strategy` | string | Requested/returned option-chain strategy code |
| `underlyingPrice` | number | Reported price of chain underlying |
| `volatility` | number | Provider volatility estimate; model and scaling unverified |
| `interestRate` | number | Model interest-rate input; rate period/scaling unverified |
| `daysToExpiration` | number | Remaining time to expiration in days |
| `numberOfContracts` | unsigned integer | Declared count of contracts across call/put maps |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [option_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/option_publication.rs).

## Option contract

Mounts `$.callExpDateMap.{expirationGroup}.{strikeText}[]` and the analogous put map. Side is also known from the map. Quotes, all five Greeks, IV, open interest, contract terms and dates remain independent fields. Greek, volatility and percentage units remain upstream-unverified where only scalar¹ is established.

| Field | Parser value type | Meaning |
|---|---|---|
| `putCall` | scalar¹ | Contract side; semantic mapper checks against call/put map |
| `symbol` | scalar¹ | Provider trading identifier; distinct from canonical identity |
| `description` | scalar¹ | Provider security display description |
| `exchangeName` | scalar¹ | Exchange display name |
| `bid` | scalar¹ | Offered option bid price |
| `ask` | scalar¹ | Offered option ask price |
| `last` | scalar¹ | Most recent reported option execution price |
| `mark` | scalar¹ | Provider mark price; computation rule not established here |
| `bidSize` | scalar¹ | Bid-side quoted quantity; REST trading unit unverified |
| `askSize` | scalar¹ | Ask-side quoted quantity; REST trading unit unverified |
| `lastSize` | scalar¹ | Quantity of the most recent execution; unit unverified |
| `highPrice` | scalar¹ | Reported session high; session boundary unverified |
| `lowPrice` | scalar¹ | Reported session low; session boundary unverified |
| `openPrice` | scalar¹ | Reported opening price; session boundary unverified |
| `closePrice` | scalar¹ | Reported closing/reference price; session basis unverified |
| `totalVolume` | scalar¹ | Reported traded quantity; accumulation interval and unit need source context |
| `tradeDate` | scalar¹ | Source trade-date coordinate; encoding/zone unverified |
| `quoteTimeInLong` | scalar¹ | Option quote-update Unix milliseconds in semantic mapper |
| `tradeTimeInLong` | scalar¹ | Option execution Unix milliseconds in semantic mapper |
| `netChange` | scalar¹ | Reported absolute price change from provider baseline |
| `volatility` | scalar¹ | Provider volatility estimate; model and scaling unverified |
| `delta` | scalar¹ | Option-value sensitivity to underlying price |
| `gamma` | scalar¹ | Delta sensitivity to underlying price |
| `theta` | scalar¹ | Option-value sensitivity to elapsed time; day/year scale unverified |
| `vega` | scalar¹ | Option-value sensitivity to implied volatility; point scale unverified |
| `rho` | scalar¹ | Option-value sensitivity to interest rates; point scale unverified |
| `openInterest` | scalar¹ | Outstanding option contract count; as-of time not supplied by this field |
| `timeValue` | scalar¹ | Provider time-value component; valuation formula unverified |
| `theoreticalOptionValue` | scalar¹ | Model-estimated option value; model inputs unverified |
| `theoreticalVolatility` | scalar¹ | Model volatility estimate; model/scaling unverified |
| `strikePrice` | scalar¹ | Option exercise price |
| `expirationDate` | scalar¹ | Optional expiration text; leading date must match enclosing expiration map |
| `daysToExpiration` | scalar¹ | Remaining time to expiration in days |
| `expirationType` | scalar¹ | Expiration classification code; complete enum unverified |
| `lastTradingDay` | scalar¹ | Last trading-day coordinate; raw encoding unverified |
| `multiplier` | scalar¹ | Contract economic multiplier; not interchangeable with quoted size |
| `settlementType` | scalar¹ | Source settlement code; complete enum unverified |
| `deliverableNote` | scalar¹ | Text describing contract deliverables; not a structured settlement schema |
| `percentChange` | scalar¹ | Reported option percentage price change; scale/baseline unverified |
| `markChange` | scalar¹ | Reported change of mark relative to provider baseline |
| `markPercentChange` | scalar¹ | Reported percentage change of mark; scale/baseline unverified |
| `inTheMoney` | scalar¹ | Source in-the-money flag; underlying observation basis unverified |
| `mini` | scalar¹ | Mini-contract indicator; type/enum unverified |
| `nonStandard` | scalar¹ | Nonstandard contract indicator; does not enumerate adjustments |
| `bidAskSize` | scalar¹ | Source combined bid/ask size value; format unverified |
| `intrinsicValue` | scalar¹ | Provider intrinsic-value component; underlying observation basis unverified |
| `extrinsicValue` | scalar¹ | Provider value above intrinsic component; formula unverified |
| `optionRoot` | scalar¹ | Option class/root identifier; adjusted roots may differ from underlying |
| `exerciseType` | scalar¹ | Source exercise-style code; complete enum unverified |
| `high52Week` | scalar¹ | Highest reported option price over 52-week lookback |
| `low52Week` | scalar¹ | Lowest reported option price over 52-week lookback |
| `breakEven` | scalar¹ | Source break-even price; cost/position assumptions unverified |
| `ssid` | scalar¹ | Provider security identifier; identifier namespace not independently documented |
| `pennyPilot` | scalar¹ | Penny Program participation indicator; exact REST representation unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [option_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/option_publication.rs).

### Option semantic decoder constraints

These constrain publication after NativeScalar parsing; they do not widen upstream schema evidence.

| Fields | Accepted non-null semantic value |
|---|---|
| symbol, putCall, settlementType | JSON string; side/settlement validation is code-owned |
| bid, ask, last, mark, strikePrice, volatility, delta, gamma, theta, vega, rho | JSON number convertible to exact decimal |
| bidSize, askSize, lastSize, totalVolume, openInterest | Nonnegative integral JSON number |
| multiplier | Positive JSON number |
| quoteTimeInLong, tradeTimeInLong | Positive integral JSON number interpreted as Unix milliseconds, no later than receipt |
| expirationDate | JSON string whose leading YYYY-MM-DD matches expiration map; terms derive from map |

Absent/null values remain distinct; a required economic term can still block publication. Greek meanings use [OIC definitions](https://prd-web.optionseducation.org/advancedconcepts/volatility-the-greeks); Schwab-specific scale conventions remain unverified.
Source: [option_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/option_publication.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Expiration

Prefix `$.expirationList[]`. Date text differs from day count and any event timestamp.

| Field | Parser value type | Meaning |
|---|---|---|
| `expirationDate` | string | Optional expiration text; leading date must match enclosing expiration map |
| `daysToExpiration` | unsigned integer | Remaining time to expiration in days |
| `expirationType` | string | Expiration classification code; complete enum unverified |
| `standard` | boolean | Source standard-expiration flag |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [option_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/option_publication.rs).

## Price-history envelope

Root symbol/empty/candles required locally; empty must match candle array. previousCloseDate is consumed as Unix milliseconds.

| Field | Parser value type | Meaning |
|---|---|---|
| `symbol` | string | Provider trading identifier; distinct from canonical identity |
| `empty` | boolean | Source empty-result flag checked against candle count |
| `previousClose` | number | Prior closing/reference price supplied with history |
| `previousCloseDate` | unsigned integer | Unix-millisecond coordinate of previous close in local mapper |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Candle

Prefix `$.candles[]`. OHLC/volume JSON numbers and unsigned Unix-millisecond datetime. Local parser requires increasing times; volume unit/session completeness need provider evidence.

| Field | Parser value type | Meaning |
|---|---|---|
| `open` | number | First reported price in candle period |
| `high` | number | Highest reported price in candle period |
| `low` | number | Lowest reported price in candle period |
| `close` | number | Last reported price in candle period |
| `volume` | number | Aggregated traded quantity in candle period |
| `datetime` | unsigned integer | Candle time as unsigned Unix milliseconds |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Market hours

Prefix `$.{marketType}.{product}`. sessionHours has separately preserved absent/null/object state; source scalar leaves are flattened locally. Actual session-array nesting and timezone grammar need a documented original; date/isOpen alone do not define a calendar.

| Field | Parser value type | Meaning |
|---|---|---|
| `date` | string | Requested/reported market civil date |
| `isOpen` | boolean | Source market-open flag for reported date |
| `category` | string | Source market category code |
| `sessionHours` | object | Session-name map; ranges retained with original presence state |
| `sessionHours.{session}[].start` | scalar | Start of a reported session range; raw timestamp grammar unverified |
| `sessionHours.{session}[].end` | scalar | End of a reported session range; raw timestamp grammar unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs), [market_hours_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/market_hours_publication.rs).

## Mover

Prefix `$.screeners[]`. Local parser requires an array; ranking records are not timestamp-complete quote snapshots.

| Field | Parser value type | Meaning |
|---|---|---|
| `symbol` | scalar¹ | Provider trading identifier; distinct from canonical identity |
| `description` | scalar¹ | Provider security display description |
| `lastPrice` | scalar¹ | Most recent reported execution price |
| `netChange` | scalar¹ | Reported absolute price change from provider baseline |
| `marketShare` | scalar¹ | Source market-share ranking metric; denominator/scaling unverified |
| `totalVolume` | scalar¹ | Reported traded quantity; accumulation interval and unit need source context |
| `trades` | scalar¹ | Reported number of executions in screener interval |
| `netPercentChange` | scalar¹ | Reported percentage price change; scale/baseline unverified |

Source: [response.rs](../../../adapters/market-squawk-adapter-schwab/src/rest/response.rs).

## Known missing evidence

A full official REST response specification, asset-specific quote variants, Regular/Extended membership, exact fundamental conventions, nested market-hour ranges and error bodies remain unverified. The historical Instruments receipt establishes five primitive types only. [Streamer responses](schwab-streamer.md) use separate numeric field dictionaries.
