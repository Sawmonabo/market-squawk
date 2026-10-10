# Yahoo Finance experimental response projection parsed by this application

Contents: [Endpoints and result envelopes](#endpoints-and-result-envelopes) · [Field presence and wrappers](#field-presence-and-wrappers) · [Quote](#quote) · [Chart metadata](#chart-metadata) · [Chart arrays](#chart-arrays) · [Chart actions](#chart-actions) · [Reference modules](#reference-modules) · [Fund modules](#fund-modules) · [Fund allocations and portfolio metrics](#fund-allocations-and-portfolio-metrics) · [Top holding](#top-holding) · [OptionChain](#optionchain) · [Option contract](#option-contract) · [Search / lookup hint](#search--lookup-hint) · [Nested container shapes](#nested-container-shapes) · [Unverified summary candidates](#unverified-summary-candidates) · [Missing option and financial schema evidence](#missing-option-and-financial-schema-evidence)

Scope: the selected experimental routes and parser projection; not a complete Yahoo response schema. These experimental routes have no reviewed public, versioned Yahoo response specification. Tables describe exact current parser keys and admitted values, with no inspected original Yahoo body. [Yahoo's data-provider/delay table](https://help.yahoo.com/kb/finance/article-exchanges-data-delays-sln2310.html) documents market coverage and delays, not JSON field guarantees.
Source requests: [request.rs](../../../adapters/market-squawk-adapter-yahoo/src/request.rs); parsing: [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs).

## Endpoints and result envelopes

All methods are GET.

| Origin / path | Request parameters | Result shape |
|---|---|---|
| query1.finance.yahoo.com/v7/finance/quote | symbols,formatted=false,lang,region | quoteResponse.result[] Quote |
| query2.finance.yahoo.com/v8/finance/chart/{symbol} | range or period1/period2,interval,includePrePost,events,includeAdjustedClose | chart.result[0] Chart |
| query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol} | modules=quoteType,price,summaryDetail,summaryProfile | quoteSummary.result[0] Reference |
| Same quoteSummary route | modules=quoteType,summaryProfile,topHoldings,fundProfile | quoteSummary.result[0] Fund |
| query2.finance.yahoo.com/v7/finance/options/{symbol} | date when selecting expiration | optionChain.result[0] OptionChain |
| query2.finance.yahoo.com/v1/finance/search | q,quotesCount,newsCount=0 | quotes[] Hint |
| query1.finance.yahoo.com/v1/finance/lookup | query,type,start,count | finance.result[0].documents[] Hint |

For quote/chart/summary/options/lookup, a non-null envelope error is parsed separately. Local error fields are code/description strings when supplied. The parsers require one result for symbol-specific chart/summary/chain requests, except explicit empty-result handling; quote accepts a result array. No general response cursor is decoded. Chart time windows and lookup start/count are request parameters, not evidence of complete history or stable snapshot pagination. Search uses its own root quotes array.

## Field presence and wrappers

Each optional scalar is retained locally as Missing/Null/Value/Invalid. Those are application parsing states; the provider never sends that enum. Tables show admitted value types, not promises of omitted/null wire members. Primitive values and raw wrappers (illustrative shape `{"raw": <value>}`, not an observed example) are accepted in many quote-summary scalars; formatted display text is not a substitute for raw numeric value. Integer parsers accept only exact integral values. Unknown dynamic fund-map keys are retained; no guessed fixed metric dictionary is supplied.

Price fields use source currency when present. Regular/pre/post/last-trade/history/expiration numeric clocks are interpreted as Unix seconds. Native delay values are kept separately as integers; upstream field-unit scaling is not established by this parser. Receipt time cannot upgrade a delayed quote. Volume/open-interest are integral count fields in the parser. Fund weights, ratios and IV are preserved as decimals; upstream scaling/unit conventions remain unverified here.

## Quote

Shared mounts `quoteResponse.result[*]` and `optionChain.result[0].quote`. The option-underlying quote uses the same parser. Pre/post/regular fields retain separate time and price values.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `symbol` | string; YahooSymbol | Provider symbol |
| `quoteType` | string (or {raw:string}) | Provider asset classification |
| `currency` | string (or {raw:string}) | Quote currency |
| `marketState` | string (or {raw:string}) | Provider market session state |
| `regularMarketTime` | integer/string or {raw:integer/string} | Original regular-session market timestamp |
| `regularMarketPrice` | number/string or {raw:number/string} | Regular-session reported last price |
| `bid` | number/string or {raw:number/string} | Bid price |
| `bidSize` | nonnegative integer/string or {raw:integer/string} | Provider bid size |
| `ask` | number/string or {raw:number/string} | Ask price |
| `askSize` | nonnegative integer/string or {raw:integer/string} | Provider ask size |
| `regularMarketOpen` | number/string or {raw:number/string} | Regular-session opening price |
| `regularMarketDayLow` | number/string or {raw:number/string} | Regular-session day low |
| `regularMarketDayHigh` | number/string or {raw:number/string} | Regular-session day high |
| `regularMarketPreviousClose` | number/string or {raw:number/string} | Previous regular-session close |
| `regularMarketVolume` | nonnegative integer/string or {raw:integer/string} | Regular-session reported volume |
| `preMarketPrice` | number/string or {raw:number/string} | Premarket reported price |
| `preMarketTime` | integer/string or {raw:integer/string} | Premarket timestamp |
| `postMarketPrice` | number/string or {raw:number/string} | Postmarket reported price |
| `postMarketTime` | integer/string or {raw:integer/string} | Postmarket timestamp |
| `shortName` | string (or {raw:string}) | Short display name |
| `shortname` | string (or {raw:string}) | Fallback short display name |
| `longName` | string (or {raw:string}) | Long display name |
| `longname` | string (or {raw:string}) | Fallback long display name |
| `exchange` | string (or {raw:string}) | Exchange code |
| `fullExchangeName` | string (or {raw:string}) | Exchange display name |
| `market` | string (or {raw:string}) | Provider market code |
| `country` | string (or {raw:string}) | Provider country |
| `region` | string (or {raw:string}) | Country/region fallback |
| `exchangeTimezoneName` | string (or {raw:string}) | Exchange time-zone identifier; provider naming grammar unverified |
| `exchangeDataDelayedBy` | nonnegative integer/string or {raw:integer/string} | Native integral exchange-delay value; provider unit unverified |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [publication.rs](../../../adapters/market-squawk-adapter-yahoo/src/publication.rs).

## Chart metadata

Mount `chart.result[0].meta`. Provider timezone, interval/range, session and market clocks remain metadata, not per-bar event times.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `exchange` | string (or {raw:string}) | Exchange code |
| `fullExchangeName` | string (or {raw:string}) | Exchange display name |
| `market` | string (or {raw:string}) | Provider market code |
| `country` | string (or {raw:string}) | Provider country |
| `region` | string (or {raw:string}) | Country/region fallback |
| `exchangeTimezoneName` | string (or {raw:string}) | Exchange time-zone identifier; provider naming grammar unverified |
| `exchangeDataDelayedBy` | nonnegative integer/string or {raw:integer/string} | Native integral exchange-delay value; provider unit unverified |
| `exchangeName` | string (or {raw:string}) | Preferred exchange code/name before exchange fallback |
| `symbol` | string; YahooSymbol | Provider symbol |
| `instrumentType` | string (or {raw:string}) | Source chart instrument classification; complete enum unverified |
| `currency` | string (or {raw:string}) | Bar currency |
| `dataGranularity` | string (or {raw:string}) | Returned interval |
| `range` | string (or {raw:string}) | Returned range |
| `firstTradeDate` | integer/string or {raw:integer/string} | First reported trading date/time |
| `regularMarketTime` | integer/string or {raw:integer/string} | Market event timestamp |
| `previousClose` | number/string or {raw:number/string} | Provider previous close |
| `chartPreviousClose` | number/string or {raw:number/string} | Provider chart-specific previous close |
| `validRanges[*]` | string array; Vec<String> | Provider-supported history ranges |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [publication.rs](../../../adapters/market-squawk-adapter-yahoo/src/publication.rs).

## Chart arrays

Timestamp and OHLCV/adjusted-close arrays align by index. `chart.result[0].timestamp[i]` supplies time; `indicators.quote[0]` supplies OHLCV arrays; `indicators.adjclose[0]` supplies adjusted close. Null array members remain missing observations, without invented interpolation. Local mismatched lengths produce an issue.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `timestamp[*]` | integer/string or {raw:integer/string}; i64 Unix seconds | Bar timestamp |
| `indicators.quote[0].open[*]` | number/string or {raw:number/string} | Reported bar open |
| `indicators.quote[0].high[*]` | number/string or {raw:number/string} | Reported bar high |
| `indicators.quote[0].low[*]` | number/string or {raw:number/string} | Reported bar low |
| `indicators.quote[0].close[*]` | number/string or {raw:number/string} | Reported bar close |
| `indicators.quote[0].volume[*]` | nonnegative integer/string or {raw:integer/string} | Reported bar volume |
| `indicators.adjclose[0].adjclose[*]` | number/string or {raw:number/string} | Reported adjusted close |

Source: [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [publication.rs](../../../adapters/market-squawk-adapter-yahoo/src/publication.rs).

## Chart actions

Mount `chart.result[0].events`. The three family maps are parsed independently. There is no inspected original or official response specification for any family here.

| Family mount | Local economic interpretation |
|---|---|
| `dividends.{provider_identity}` | Dividend/cash-distribution event |
| `capitalGains.{provider_identity}` | Capital-gain distribution event |
| `splits.{provider_identity}` | Share split event |

### Shared Action parser allowance

All three maps call the same tolerant parser. The table is its admitted union, **not a claim that dividend responses contain split fields or that split responses contain amount/currency**. Source-specific field membership remains unverified. Identity is an object key, not a field inside each action.

| Relative member | Parser-admitted value | Meaning / applicable interpretation |
|---|---|---|
| `(object key)` | string | Exact provider event identity; numeric key may supply Unix-second fallback |
| `date` | integer/string or raw wrapper | Source event time, interpreted locally as Unix seconds |
| `amount` | number/string or raw wrapper | Cash distribution amount; economic use is cash-action families |
| `currency` | string or raw wrapper | Reported currency of cash amount, if supplied |
| `numerator` | number/string or raw wrapper | New-share ratio component for a split |
| `denominator` | number/string or raw wrapper | Original-share ratio component for a split |
| `splitRatio` | string or raw wrapper | Source split-ratio text; no guessed amount conversion |

A supplied numeric identity/date disagreement is rejected locally. An absent date can fall back to a numeric identity while its original missing/null state remains retained.
Source: [parse_chart_event_family](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [YahooChartEvent](../../../adapters/market-squawk-adapter-yahoo/src/model.rs).

## Reference modules

Mount `quoteSummary.result[0]`. Module objects remain distinct: quoteType identity, price market values, summaryDetail fund hints and summaryProfile company description. No company statement API is implied by these modules.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `quoteType.symbol` | string (or {raw:string}) | Provider symbol |
| `price.symbol` | string (or {raw:string}) | Provider symbol fallback |
| `quoteType.quoteType` | string (or {raw:string}) | Asset classification |
| `price.shortName` | string (or {raw:string}) | Short display name |
| `quoteType.shortName` | string (or {raw:string}) | Short display name fallback |
| `price.longName` | string (or {raw:string}) | Long display name |
| `quoteType.longName` | string (or {raw:string}) | Long display name fallback |
| `quoteType.underlyingSymbol` | string (or {raw:string}) | Underlying symbol hint |
| `price.currency` | string (or {raw:string}) | Currency |
| `summaryDetail.currency` | string (or {raw:string}) | Currency fallback |
| `price.marketState` | string (or {raw:string}) | Market state |
| `price.regularMarketTime` | integer/string or {raw:integer/string} | Regular market timestamp |
| `summaryDetail.regularMarketTime` | integer/string or {raw:integer/string} | Market timestamp fallback |
| `price.regularMarketPrice` | number/string or {raw:number/string} | Regular market price hint |
| `summaryDetail.regularMarketPrice` | number/string or {raw:number/string} | Regular market price fallback |
| `summaryDetail.navPrice` | number/string or {raw:number/string} | Provider-reported NAV hint |
| `summaryDetail.totalAssets` | number/string or {raw:number/string} | Provider-reported total assets |
| `summaryDetail.category` | string (or {raw:string}) | Fund category hint |
| `summaryDetail.fundFamily` | string (or {raw:string}) | Fund family hint |
| `summaryProfile.sector` | string (or {raw:string}) | Company sector |
| `summaryProfile.industry` | string (or {raw:string}) | Company industry |
| `summaryProfile.website` | string (or {raw:string}) | Company/fund website |
| `summaryProfile.longBusinessSummary` | string (or {raw:string}) | Company business description or fund strategy description |
| `price.exchange` | string (or {raw:string}) | Exchange |
| `quoteType.exchange` | string (or {raw:string}) | Exchange fallback |
| `price.exchangeName` | string (or {raw:string}) | Exchange display name |
| `price.fullExchangeName` | string (or {raw:string}) | Exchange display-name fallback |
| `price.market` | string (or {raw:string}) | Market |
| `quoteType.market` | string (or {raw:string}) | Market fallback |
| `summaryProfile.country` | string (or {raw:string}) | Country |
| `price.region` | string (or {raw:string}) | Country fallback |
| `price.exchangeTimezoneName` | string (or {raw:string}) | Timezone |
| `quoteType.timeZoneFullName` | string (or {raw:string}) | Timezone fallback |
| `price.exchangeDataDelayedBy` | nonnegative integer/string or {raw:integer/string} | Native delay value |
| `quoteType.exchangeDataDelayedBy` | nonnegative integer/string or {raw:integer/string} | Native exchange-delay fallback; unit unverified |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [request.rs](../../../adapters/market-squawk-adapter-yahoo/src/request.rs).

## Fund modules

Mount `quoteSummary.result[0]`. fundProfile contains legal type/family/costs; summaryProfile supplies description; topHoldings contains allocation and holdings. Strategy wording is not promoted to a separate numeric analytics field.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `quoteType.symbol` | string (or {raw:string}) | Fund provider symbol |
| `quoteType.quoteType` | string (or {raw:string}) | Fund provider classification |
| `summaryProfile.longBusinessSummary` | string (or {raw:string}) | Fund strategy/description |
| `fundProfile.categoryName` | string (or {raw:string}) | Fund category |
| `fundProfile.family` | string (or {raw:string}) | Fund family |
| `fundProfile.legalType` | string (or {raw:string}) | Fund legal organization type |
| `fundProfile.feesExpensesInvestment.annualReportExpenseRatio` | number/string or {raw:number/string} | Annual-report expense ratio |
| `fundProfile.feesExpensesInvestment.annualHoldingsTurnover` | number/string or {raw:number/string} | Annual holdings turnover |
| `fundProfile.feesExpensesInvestment.totalNetAssets` | number/string or {raw:number/string} | Provider fund net assets |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [request.rs](../../../adapters/market-squawk-adapter-yahoo/src/request.rs).

## Fund allocations and portfolio metrics

Mount `quoteSummary.result[0].topHoldings`. Fixed asset positions are six source keys. Equity/bond metrics are dynamic objects; ratings/sector weights are arrays of keyed objects. Exact returned metric/rating names and units need a real response. Wildcards represent arbitrary provider keys, not enumerated bond ratings.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `cashPosition` | number/string or {raw:number/string} | Allocation to cash; fraction/percent scale unverified |
| `stockPosition` | number/string or {raw:number/string} | Allocation to common stock; fraction/percent scale unverified |
| `bondPosition` | number/string or {raw:number/string} | Allocation to bonds; fraction/percent scale unverified |
| `preferredPosition` | number/string or {raw:number/string} | Allocation to preferred stock; fraction/percent scale unverified |
| `convertiblePosition` | number/string or {raw:number/string} | Allocation to convertible securities; fraction/percent scale unverified |
| `otherPosition` | number/string or {raw:number/string} | Allocation to other assets; fraction/percent scale unverified |
| `equityHoldings.{provider_key}` | number/string or {raw:number/string} | Every returned equity portfolio metric key and its exact decimal value |
| `bondHoldings.{provider_key}` | number/string or {raw:number/string} | Every returned bond portfolio metric key and its exact decimal value |
| `bondRatings[*].{provider_key}` | number/string or {raw:number/string} | Every actual provider rating key |
| `sectorWeightings[*].{provider_key}` | number/string or {raw:number/string} | Every actual provider sector key |

Source: [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs).

## Top holding

Prefix `quoteSummary.result[0].topHoldings.holdings[*]`. Symbol/name/holdingPercent are separate values; position count and full-portfolio coverage are not guaranteed by a top-holdings array.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `symbol` | string (or {raw:string}) | Holding provider symbol |
| `holdingName` | string (or {raw:string}) | Reported holding name |
| `holdingPercent` | number/string or {raw:number/string} | Reported holding weight; provider fraction/percent scale unverified |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs).

## OptionChain

Mount `optionChain.result[0]`. expirationDates and strikes are advertised arrays. options[] holds expiration groups, each with calls/puts arrays. To obtain another expiry, issue a date-specific request; there is no generic cursor. The parser tracks requested/returned expiration separately. Internal underlying_symbol is taken from the requested target, not a response member; it is excluded from this wire table.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `expirationDates[*]` | integer/string array; Vec<i64> Unix seconds | Returned expiry list |
| `strikes[*]` | number/string array; Vec<Decimal> | Returned strike list |
| `options[*].expirationDate` | integer/string or {raw:integer/string} | Returned group expiration date |
| `options[*].hasMiniOptions` | boolean or {raw:boolean} | Provider mini-option flag |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs).

## Option contract

Mounts `optionChain.result[0].options[*].calls[*]` and `…puts[*]`. Side comes from the containing array. contractSize is provider text, not an established numeric multiplier; IV is not a guarantee of Greek availability.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `contractSymbol` | string; YahooSymbol | Exact provider option contract identifier |
| `lastTradeDate` | integer/string or {raw:integer/string} | Last-trade timestamp |
| `strike` | number/string or {raw:number/string} | Contract strike |
| `lastPrice` | number/string or {raw:number/string} | Last traded price |
| `bid` | number/string or {raw:number/string} | Bid price |
| `ask` | number/string or {raw:number/string} | Ask price |
| `change` | number/string or {raw:number/string} | Provider reported price change |
| `percentChange` | number/string or {raw:number/string} | Provider reported percent change |
| `volume` | nonnegative integer/string or {raw:integer/string} | Provider volume |
| `openInterest` | nonnegative integer/string or {raw:integer/string} | Provider open interest |
| `impliedVolatility` | number/string or {raw:number/string} | Provider implied volatility |
| `inTheMoney` | boolean or {raw:boolean} | Provider in-the-money flag |
| `contractSize` | string (or {raw:string}) | Provider contract-size text |
| `currency` | string (or {raw:string}) | Provider currency |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [publication.rs](../../../adapters/market-squawk-adapter-yahoo/src/publication.rs).

## Search / lookup hint

Shared Hint fields mount at `quotes[*]` for search and `finance.result[0].documents[*]` for lookup. Lowercase/camel-case short/long names and score/navScore alternatives are distinct raw keys in the local parser. A hint is not authoritative instrument identity.

| Field | Parser-admitted value | Meaning |
|---|---|---|
| `symbol` | string; YahooSymbol | Provider symbol hint |
| `quoteType` | string (or {raw:string}) | Asset-type hint |
| `exchange` | string (or {raw:string}) | Exchange hint |
| `exchDisp` | string (or {raw:string}) | Exchange display fallback |
| `shortname` | string (or {raw:string}) | Short display name |
| `shortName` | string (or {raw:string}) | Short-name fallback |
| `longname` | string (or {raw:string}) | Long display name |
| `longName` | string (or {raw:string}) | Long-name fallback |
| `sector` | string (or {raw:string}) | Sector hint |
| `industry` | string (or {raw:string}) | Industry hint |
| `score` | number/string or {raw:number/string} | Provider relevance score |
| `navScore` | number/string or {raw:number/string} | Relevance score fallback |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs).

## Nested container shapes

These additional object/array container paths preserve structure and state independently of primitive values. The table is local parsing evidence; omission/null allowance does not establish provider schema requiredness.

| Field | Parser container shape | Meaning |
|---|---|---|
| `quoteResponse.result` | Quote[] | Returned quote objects |
| `chart.result[0].timestamp` | array/null/absent | Original timestamp-container presence/cardinality |
| `chart.result[0].indicators` | object/null/absent | Original indicator-parent presence |
| `chart.result[0].indicators.quote` | array/null/absent | Quote indicator container |
| `chart.result[0].indicators.adjclose` | array/null/absent | Adjusted-close indicator container |
| `chart.result[0].events` | object/null/absent | Original actions-parent presence |
| `quoteSummary.result[0].topHoldings.holdings` | array/null/absent | Top holdings array |
| `quoteSummary.result[0].topHoldings.equityHoldings` | object/null/absent | Equity metric parent |
| `quoteSummary.result[0].topHoldings.bondHoldings` | object/null/absent | Bond metric parent |
| `quoteSummary.result[0].topHoldings.bondRatings` | array of objects/null/absent | Bond-rating allocations |
| `quoteSummary.result[0].topHoldings.sectorWeightings` | array of objects/null/absent | Sector allocations |
| `optionChain.result[0].expirationDates` | array/null/absent | Returned expirations |
| `optionChain.result[0].strikes` | array/null/absent | Returned strike list |
| `optionChain.result[0].options` | expiration-group object[]; null/absent admitted | Returned option groups |
| `optionChain.result[0].options[*].calls` | OptionContract[]; null/absent admitted | Call contracts |
| `optionChain.result[0].options[*].puts` | OptionContract[]; null/absent admitted | Put contracts |

Source: [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs), [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs).

## Unverified summary candidates

These paths were named in prior field discovery but have neither a typed mapper nor an inspected original value in this audit. They are **candidate paths, not response-schema guarantees**. Types and presence remain unknown.

| Field | Unknown upstream type | Meaning |
|---|---|---|
| `quoteResponse.result[*].marketCap` | unknown; no typed mapper or inspected original | Market capitalization |
| `quoteSummary.result[0].price.marketCap` | unknown; no typed mapper or inspected original | Market capitalization |
| `quoteResponse.result[*].trailingPE` | unknown; no typed mapper or inspected original | Trailing P/E |
| `quoteResponse.result[*].forwardPE` | unknown; no typed mapper or inspected original | Forward P/E |
| `quoteSummary.result[0].summaryDetail.trailingPE` | unknown; no typed mapper or inspected original | Trailing P/E |
| `quoteSummary.result[0].summaryDetail.forwardPE` | unknown; no typed mapper or inspected original | Forward P/E |
| `quoteResponse.result[*].epsTrailingTwelveMonths` | unknown; no typed mapper or inspected original | Trailing EPS |
| `quoteResponse.result[*].epsForward` | unknown; no typed mapper or inspected original | Forward EPS |
| `quoteResponse.result[*].fiftyTwoWeekHigh` | unknown; no typed mapper or inspected original | 52-week high |
| `quoteResponse.result[*].fiftyTwoWeekLow` | unknown; no typed mapper or inspected original | 52-week low |
| `quoteSummary.result[0].summaryDetail.fiftyTwoWeekHigh` | unknown; no typed mapper or inspected original | 52-week high |
| `quoteSummary.result[0].summaryDetail.fiftyTwoWeekLow` | unknown; no typed mapper or inspected original | 52-week low |
| `quoteSummary.result[0].summaryDetail.dividendRate` | unknown; no typed mapper or inspected original | Annual dividend/distribution rate |
| `quoteSummary.result[0].summaryDetail.dividendYield` | unknown; no typed mapper or inspected original | Dividend/distribution yield |
| `quoteSummary.result[0].summaryDetail.exDividendDate` | unknown; no typed mapper or inspected original | Ex-dividend date distinct from chart event date |
| `quoteSummary.result[0].summaryDetail.payoutRatio` | unknown; no typed mapper or inspected original | Payout ratio |
| `quoteSummary.result[0].summaryDetail.beta` | unknown; no typed mapper or inspected original | Beta |
| `quoteSummary.result[0].summaryDetail.averageVolume` | unknown; no typed mapper or inspected original | Average volume |
| `quoteSummary.result[0].summaryDetail.averageVolume10days` | unknown; no typed mapper or inspected original | 10-day average volume |
| `quoteSummary.result[0].summaryDetail.yield` | unknown; no typed mapper or inspected original | Provider fund yield |
| `quoteResponse.result[*].regularMarketChange` | unknown; no typed mapper or inspected original | Provider price change |
| `quoteResponse.result[*].regularMarketChangePercent` | unknown; no typed mapper or inspected original | Provider percent change |

Source: [model.rs](../../../adapters/market-squawk-adapter-yahoo/src/model.rs), [parse.rs](../../../adapters/market-squawk-adapter-yahoo/src/parse.rs).

## Missing option and financial schema evidence

No exact upstream key is established here for bidSize/askSize/lastSize/mark or the five Greeks. Those names belonged to canonical option contracts and are excluded from upstream tables. Candidate EPS/P-E/market-cap/dividend/range fields above are explicitly unverified. No complete financial-statement, full fund-holdings, settlement/multiplier, error-body or public WebSocket response specification is established by this parser.
