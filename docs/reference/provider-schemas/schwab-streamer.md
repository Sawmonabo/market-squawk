# Charles Schwab Streamer services parsed by this application

Contents: [Control envelopes](#control-envelopes) · [DataBatch](#databatch) · [Content metadata](#content-metadata) · [Sparse updates](#sparse-updates) · [Service field dictionaries](#service-field-dictionaries) · [LEVELONE_EQUITIES](#levelone_equities) · [LEVELONE_OPTIONS](#levelone_options) · [LEVELONE_FUTURES](#levelone_futures) · [LEVELONE_FUTURES_OPTIONS](#levelone_futures_options) · [LEVELONE_FOREX](#levelone_forex) · [CHART_EQUITY](#chart_equity) · [CHART_FUTURES](#chart_futures) · [NYSE_BOOK / NASDAQ_BOOK / OPTIONS_BOOK](#nyse_book--nasdaq_book--options_book) · [Book level](#book-level) · [Book participant](#book-participant) · [SCREENER_EQUITY / SCREENER_OPTION](#screener_equity--screener_option) · [ScreenerItem](#screeneritem) · [Scope and unknowns](#scope-and-unknowns)

[Official portal](https://developer.schwab.com/products/trader-api--individual/details/documentation/Streamer%20API). Primary field/type evidence: [retained official documentation](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json). Decoder: [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs); reviewed dictionary: [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs).

Scope: the audited service dictionaries and current native decoders, not every upstream Streamer capability. The WebSocket URL comes from private bootstrap metadata; no private value appears here. JSON frames may contain response/data/notify arrays. A command response, quote delta, chart aggregate and book snapshot are different message families.

## Control envelopes

| Path | Type / local validation | Meaning |
|---|---|---|
| $.response[] | object array | Command responses |
| $.response[].service | string | ADMIN or data service |
| $.response[].command | string | LOGIN/LOGOUT/SUBS/ADD/UNSUBS admitted |
| $.response[].requestid | string | Request correlation |
| $.response[].timestamp | unsigned number; absent/null admitted locally | Unix milliseconds |
| $.response[].content.code | integer | Command result code |
| $.response[].content.msg | scalar¹ | Message; local scalar tolerance |
| $.data[] | object array | DataBatch records |
| $.notify[].heartbeat | scalar¹ | Native heartbeat value |

Top-level arrays may be omitted in the local decoder. Explicit null is not equivalent to omitted. scalar¹ means local null/boolean/number/string allowance, not an official declared type.

## DataBatch

Prefix `$.data[]`. Commands admitted locally: SUBS/ADD. content is an array. Each content record requires named key, separately from numeric field 0. Batch timestamp is Unix milliseconds and differs from field-specific quote/trade time.

| Field | Document / parser type | Meaning |
|---|---|---|
| `service` | string | Service name selecting the numeric field dictionary |
| `command` | string | Data delivery command; local SUBS/ADD allowance |
| `timestamp` | unsigned integer | Batch delivery time as unsigned Unix milliseconds; not every field event time |

Source: [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [canonical.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical.rs).

## Content metadata

Prefix `$.data[].content[]`. Named fields accompany numeric string member IDs. An ID has meaning only within its service.

| Field | Document / parser type | Meaning |
|---|---|---|
| `key` | string | Named provider instrument key, separate from numeric member 0 |
| `delayed` | boolean / null | Source delay flag; no delay duration or entitlement guarantee |
| `assetMainType` | string / null | Broad provider asset classification |
| `assetSubType` | string / null | Provider asset subtype |
| `cusip` | string / null | CUSIP source security identifier |

Source: [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs).

## Sparse updates

LEVELONE messages contain requested changed fields. Omitted numeric members do not clear prior values; supplied null remains distinct from zero. Reconstruction is bound to service/key/subscription and connection generation. A reconnect needs a qualified baseline. Books contain nested level/participant arrays; do not impose quote-delta merge rules on book snapshots. CHART messages carry aggregates; SCREENER messages carry rankings.

## Service field dictionaries

Documented types below come from the retained primary document. Native parsing additionally preserves null; that is not proof of official nullability. Fields explicitly described as epoch milliseconds retain that encoding. Calendar date, day-of-month, time-to-expiry and event time must remain distinct. No live Streamer example is claimed. Empty retained descriptions are explicitly treated as missing definitions; REST names do not fill them. Greek meanings use [OIC definitions](https://prd-web.optionseducation.org/advancedconcepts/volatility-the-greeks), without establishing provider-specific scale conventions.

## LEVELONE_EQUITIES

Prefix `$.data[service=LEVELONE_EQUITIES].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | number [double] | Current offered bid price |
| `["2"]` | number [double] | Current offered ask price |
| `["3"]` | number [double] | Price of latest matched execution |
| `["4"]` | integer [int] | Quoted bid quantity in shares |
| `["5"]` | integer [int] | Quoted ask quantity in shares |
| `["6"]` | string [char] | Exchange identifier posting the ask |
| `["7"]` | string [char] | Exchange identifier posting the bid |
| `["8"]` | integer [long] | Source-day aggregated shares including pre/post hours |
| `["9"]` | integer [long] | Last executed quantity in shares |
| `["10"]` | number [double] | Highest execution price of the source day |
| `["11"]` | number [double] | Lowest execution price of the source day |
| `["12"]` | number [double] | Prior source-day closing price |
| `["13"]` | string [char] | Primary listing-exchange identifier |
| `["14"]` | boolean [boolean] | Source margin-collateral eligibility flag |
| `["15"]` | string [String] | Security/product display description |
| `["16"]` | string [char] | Exchange identifier of latest execution |
| `["17"]` | number [double] | Source-day opening price |
| `["18"]` | number [double] | Reported absolute price change; use service-specific baseline |
| `["19"]` | number [double] | Highest traded price during 52-week lookback |
| `["20"]` | number [double] | Lowest traded price during 52-week lookback |
| `["21"]` | number [double] | Price divided by earnings per share; fiscal basis unverified |
| `["22"]` | number [double] | Annualized dividend amount; currency source context required |
| `["23"]` | number [double] | Dividend relative to price; source scaling unverified |
| `["24"]` | number [double] | Mutual-fund net asset value; not ETF NAV |
| `["25"]` | string [String] | Exchange display name |
| `["26"]` | string [String] | Dividend-related date; ex/record/payment role and encoding not defined in retained document |
| `["27"]` | boolean [boolean] | Regular-session quote indicator; definition unavailable in retained document |
| `["28"]` | boolean [boolean] | Regular-session trade indicator; definition unavailable in retained document |
| `["29"]` | number [double] | Most recent regular-session execution price |
| `["30"]` | integer [integer] | Quantity of most recent regular-session execution |
| `["31"]` | number [double] | Regular-session reported absolute price change |
| `["32"]` | string [String] | Trading/security state; closed enum not established here |
| `["33"]` | number [double] | Provider mark; calculation rule unavailable in retained document |
| `["34"]` | integer [Long] | Latest quote-update Unix milliseconds |
| `["35"]` | integer [Long] | Latest execution Unix milliseconds |
| `["36"]` | integer [Long] | Latest regular-session execution Unix milliseconds |
| `["37"]` | integer [long] | Latest bid-update Unix milliseconds |
| `["38"]` | integer [long] | Latest ask-update Unix milliseconds |
| `["39"]` | string [String] | Four-character Market Identifier Code of ask venue |
| `["40"]` | string [String] | Four-character Market Identifier Code of bid venue |
| `["41"]` | string [String] | Four-character Market Identifier Code of last execution venue |
| `["42"]` | number [double] | Reported percentage price change; scaling unverified |
| `["43"]` | number [double] | Regular-session reported percentage price change |
| `["44"]` | number [double] | Reported mark-price change from provider baseline |
| `["45"]` | number [double] | Reported mark percentage change; source scaling unverified |
| `["46"]` | integer [integer] | Borrow-availability quantity; exact definition unavailable in retained document |
| `["47"]` | number [double] | Borrow rate; period/scaling unavailable in retained document |
| `["48"]` | integer [integer] | Source borrow-status integer; code meanings unavailable in retained document |
| `["49"]` | integer [integer] | Source shortability integer; code meanings unavailable in retained document |
| `["50"]` | number [double] | Absolute price change since regular-session end |
| `["51"]` | number [double] | Percentage price change since regular-session end; source scaling unverified |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## LEVELONE_OPTIONS

Prefix `$.data[service=LEVELONE_OPTIONS].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | string [String] | Security/product display description |
| `["2"]` | number [double] | Current offered bid price |
| `["3"]` | number [double] | Current offered ask price |
| `["4"]` | number [double] | Price of latest matched execution |
| `["5"]` | number [double] | Highest execution price of the source day |
| `["6"]` | number [double] | Lowest execution price of the source day |
| `["7"]` | number [double] | Prior source-day closing price |
| `["8"]` | integer [long] | Source-day aggregated contracts including pre/post hours |
| `["9"]` | integer [int] | Outstanding contracts; source as-of coordinate separate |
| `["10"]` | number [double] | Implied volatility estimate; annualization/point scale unverified |
| `["11"]` | number [double] | Option exercise value relative to underlying and strike |
| `["12"]` | integer [int] | Civil expiration year |
| `["13"]` | number [double] | Contract multiplier; independent of quote quantity |
| `["14"]` | integer [int] | Number of displayed decimal places |
| `["15"]` | number [double] | Source-day opening price |
| `["16"]` | integer [int] | Quoted bid quantity in contracts |
| `["17"]` | integer [int] | Quoted ask quantity in contracts |
| `["18"]` | integer [int] | Last executed quantity in contracts |
| `["19"]` | number [double] | Reported absolute price change; use service-specific baseline |
| `["20"]` | number [double] | Option exercise price |
| `["21"]` | string [char] | Source call/put or contract-type character; complete enum unverified |
| `["22"]` | string [String] | Provider alias of underlying instrument |
| `["23"]` | integer [int] | Civil expiration month |
| `["24"]` | string [String] | Source delivered-security/cash description; not a complete settlement model |
| `["25"]` | number [double] | Option time-value component; calculation rule unverified |
| `["26"]` | integer [int] | Civil expiration day of month |
| `["27"]` | integer [int] | Remaining days until expiration |
| `["28"]` | number [double] | Option-value sensitivity to underlying price |
| `["29"]` | number [double] | Delta sensitivity to underlying price |
| `["30"]` | number [double] | Option-value sensitivity to elapsed time; reporting scale unverified |
| `["31"]` | number [double] | Option-value sensitivity to implied volatility; reporting scale unverified |
| `["32"]` | number [double] | Option-value sensitivity to interest rates; reporting scale unverified |
| `["33"]` | string [String] | Trading/security state; closed enum not established here |
| `["34"]` | number [double] | Provider model-estimated option value; model unverified |
| `["35"]` | number [double] | Source price of option underlying |
| `["36"]` | string [char] | Source expiration classification; UV meaning/enum unavailable in retained document |
| `["37"]` | number [double] | Provider mark; calculation rule unavailable in retained document |
| `["38"]` | integer [long] | Latest quote-update Unix milliseconds |
| `["39"]` | integer [long] | Latest execution Unix milliseconds |
| `["40"]` | string [char] | Source exchange character code |
| `["41"]` | string [String] | Exchange display name |
| `["42"]` | integer [long] | Last trading coordinate; exact encoding not defined in retained document |
| `["43"]` | string [char] | Source settlement-method character; complete enum unverified |
| `["44"]` | number [double] | Reported percentage price change; scaling unverified |
| `["45"]` | number [double] | Reported mark-price change from provider baseline |
| `["46"]` | number [double] | Reported mark percentage change; source scaling unverified |
| `["47"]` | number [double] | Source implied-yield metric; definition/scaling unavailable in retained document |
| `["48"]` | boolean [boolean] | Penny Program participation flag |
| `["49"]` | string [String] | Option class/root identifier; adjusted roots may differ from underlying |
| `["50"]` | number [double] | Highest traded price during 52-week lookback |
| `["51"]` | number [double] | Lowest traded price during 52-week lookback |
| `["52"]` | number [double] | Provider indicative ask price |
| `["53"]` | number [double] | Provider indicative bid price |
| `["54"]` | integer [long] | Latest indicative bid/ask update Unix milliseconds |
| `["55"]` | string [char] | Source exercise-style character; complete enum unverified |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## LEVELONE_FUTURES

Prefix `$.data[service=LEVELONE_FUTURES].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | number [double] | Current offered bid price |
| `["2"]` | number [double] | Current offered ask price |
| `["3"]` | number [double] | Price of latest matched execution |
| `["4"]` | integer [long] | Quoted bid quantity in contracts |
| `["5"]` | integer [long] | Quoted ask quantity in contracts |
| `["6"]` | string [char] | Exchange identifier posting the bid |
| `["7"]` | string [char] | Exchange identifier posting the ask |
| `["8"]` | integer [long] | Source-day aggregated contracts including pre/post hours |
| `["9"]` | integer [long] | Last executed quantity in contracts |
| `["10"]` | integer [long] | Latest quote-update Unix milliseconds |
| `["11"]` | integer [long] | Latest execution Unix milliseconds |
| `["12"]` | number [double] | Highest execution price of the source day |
| `["13"]` | number [double] | Lowest execution price of the source day |
| `["14"]` | number [double] | Prior source-day closing price |
| `["15"]` | string [char] | Primary listing-exchange identifier |
| `["16"]` | string [String] | Security/product display description |
| `["17"]` | string [char] | Exchange identifier of latest execution |
| `["18"]` | number [double] | Source-day opening price |
| `["19"]` | number [double] | Reported absolute price change; use service-specific baseline |
| `["20"]` | number [double] | Reported percentage price change; scaling unverified |
| `["21"]` | string [String] | Exchange display name |
| `["22"]` | string [String] | Trading/security state; closed enum not established here |
| `["23"]` | integer [int] | Outstanding contracts; source as-of coordinate separate |
| `["24"]` | number [double] | Provider mark-to-market value; calculation rule not fully established |
| `["25"]` | number [double] | Minimum quoted price increment |
| `["26"]` | number [double] | Source value of minimum price movement; currency context required |
| `["27"]` | string [String] | Product name/classification |
| `["28"]` | string [String] | Decimal/fraction display format: numerator display precision and implied denominator |
| `["29"]` | string [String] | Source trading-hours description; grammar unverified |
| `["30"]` | boolean [boolean] | Provider futures tradability flag |
| `["31"]` | number [double] | Futures monetary point value; currency context required |
| `["32"]` | boolean [boolean] | Provider active-contract flag |
| `["33"]` | number [double] | Provider closing/settlement price |
| `["34"]` | string [String] | Provider alias of active futures contract |
| `["35"]` | integer [long] | Contract expiration coordinate; exact encoding unverified |
| `["36"]` | string [String] | Source expiry-style code; definition unavailable in retained document |
| `["37"]` | integer [long] | Latest ask-update Unix milliseconds |
| `["38"]` | integer [long] | Latest bid-update Unix milliseconds |
| `["39"]` | boolean [boolean] | Whether the contract quoted during current active session |
| `["40"]` | integer [long] | Named settlement-date field; retained description calls it expiration date, ambiguity unresolved |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## LEVELONE_FUTURES_OPTIONS

Prefix `$.data[service=LEVELONE_FUTURES_OPTIONS].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | number [double] | Current offered bid price |
| `["2"]` | number [double] | Current offered ask price |
| `["3"]` | number [double] | Price of latest matched execution |
| `["4"]` | integer [long] | Quoted bid quantity in contracts |
| `["5"]` | integer [long] | Quoted ask quantity in contracts |
| `["6"]` | string [char] | Exchange identifier posting the bid |
| `["7"]` | string [char] | Exchange identifier posting the ask |
| `["8"]` | integer [long] | Source-day aggregated contracts including pre/post hours |
| `["9"]` | integer [long] | Last executed quantity in contracts |
| `["10"]` | integer [long] | Latest quote-update Unix milliseconds |
| `["11"]` | integer [long] | Latest execution Unix milliseconds |
| `["12"]` | number [double] | Highest execution price of the source day |
| `["13"]` | number [double] | Lowest execution price of the source day |
| `["14"]` | number [double] | Prior source-day closing price |
| `["15"]` | string [char] | Exchange identifier of latest execution |
| `["16"]` | string [String] | Security/product display description |
| `["17"]` | number [double] | Source-day opening price |
| `["18"]` | number [double] | Outstanding contracts; source as-of coordinate separate |
| `["19"]` | number [double] | Provider mark-to-market value; calculation rule not fully established |
| `["20"]` | number [double] | Minimum quoted price increment |
| `["21"]` | number [double] | Source value of minimum price movement; currency context required |
| `["22"]` | number [double] | Futures monetary point value; currency context required |
| `["23"]` | number [double] | Provider closing/settlement price |
| `["24"]` | string [String] | Provider alias of underlying instrument |
| `["25"]` | number [double] | Option exercise price |
| `["26"]` | integer [long] | Contract expiration coordinate; exact encoding unverified |
| `["27"]` | string [String] | Source expiry-style code; definition unavailable in retained document |
| `["28"]` | string [Char] | Source call/put or contract-type character; complete enum unverified |
| `["29"]` | string [String] | Trading/security state; closed enum not established here |
| `["30"]` | string [char] | Source exchange character code |
| `["31"]` | string [String] | Exchange display name |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## LEVELONE_FOREX

Prefix `$.data[service=LEVELONE_FOREX].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | number [double] | Current offered bid price |
| `["2"]` | number [double] | Current offered ask price |
| `["3"]` | number [double] | Price of latest matched execution |
| `["4"]` | integer [long] | Quoted bid quantity in currency pairs |
| `["5"]` | integer [long] | Quoted ask quantity in currency pairs |
| `["6"]` | integer [long] | Source-day aggregated currency pairs including pre/post hours |
| `["7"]` | integer [long] | Last executed quantity in currency pairs |
| `["8"]` | integer [long] | Latest quote-update Unix milliseconds |
| `["9"]` | integer [long] | Latest execution Unix milliseconds |
| `["10"]` | number [double] | Highest execution price of the source day |
| `["11"]` | number [double] | Lowest execution price of the source day |
| `["12"]` | number [double] | Prior source-day closing price |
| `["13"]` | string [char] | Source exchange character code |
| `["14"]` | string [String] | Security/product display description |
| `["15"]` | number [double] | Source-day opening price |
| `["16"]` | number [double] | Reported absolute price change; use service-specific baseline |
| `["17"]` | number [double] | Reported percentage price change; scaling unverified |
| `["18"]` | string [String] | Exchange display name |
| `["19"]` | integer [Int] | Number of displayed decimal places |
| `["20"]` | string [String] | Trading/security state; closed enum not established here |
| `["21"]` | number [double] | Minimum quoted price increment |
| `["22"]` | number [double] | Source value of minimum price movement; currency context required |
| `["23"]` | string [String] | Product name/classification |
| `["24"]` | string [String] | Source trading-hours description; grammar unverified |
| `["25"]` | boolean [boolean] | Provider forex tradability flag |
| `["26"]` | string [String] | Source market-maker identifier; definition unavailable in retained document |
| `["27"]` | number [double] | Highest traded price during 52-week lookback |
| `["28"]` | number [double] | Lowest traded price during 52-week lookback |
| `["29"]` | number [double] | Provider mark-to-market value; calculation rule not fully established |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## CHART_EQUITY

Prefix `$.data[service=CHART_EQUITY].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear. The reviewed code dictionary differs from the retained official table at IDs 1–6: 1 sequence, 2 open, 3 high, 4 low, 5 close, 6 volume. Source comments identify earlier supporting evidence; no original chart frame was inspected here. The table describes the reviewed code mapping rather than independently verified upstream IDs.

| Field | Reviewed dictionary type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | integer [long] | Source chart sequence number |
| `["2"]` | number [double] | First price of chart aggregate |
| `["3"]` | number [double] | Highest price of chart aggregate |
| `["4"]` | number [double] | Lowest price of chart aggregate |
| `["5"]` | number [double] | Last price of chart aggregate |
| `["6"]` | number [double] | Aggregated traded quantity for chart period |
| `["7"]` | integer [long] | Chart period time; local Unix-millisecond interpretation |
| `["8"]` | integer [int] | Source chart trading-day coordinate; encoding unverified |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## CHART_FUTURES

Prefix `$.data[service=CHART_FUTURES].content[]`. Numeric member keys are strings in JSON; only requested/present IDs appear.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Provider instrument key |
| `["1"]` | integer [long] | Chart period Unix milliseconds |
| `["2"]` | number [double] | First price of minute aggregate |
| `["3"]` | number [double] | Highest price of minute aggregate |
| `["4"]` | number [double] | Lowest price of minute aggregate |
| `["5"]` | number [double] | Last price of minute aggregate |
| `["6"]` | number [double] | Aggregated traded quantity for chart period |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## NYSE_BOOK / NASDAQ_BOOK / OPTIONS_BOOK

Shared mount `$.data[service={service}].content[]`. All three book services use the same shape.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Uppercase provider symbol; not canonical identity |
| `["1"]` | integer [long] | Book snapshot time; local Unix-millisecond interpretation |
| `["2"]` | BookLevel[] [Array] | Array of bid BookLevel objects |
| `["3"]` | BookLevel[] [Array] | Array of ask BookLevel objects |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## Book level

Mounts content[]["2"][] for bids and content[]["3"][] for asks under each book service. Level field 3 is a participant array. Price, aggregate size and participant count are separate.

| Field | Document / parser type | Meaning |
|---|---|---|
| `["2"][]["0"]` | number | Book-level quoted price |
| `["2"][]["1"]` | number | Quantity aggregated at this price level |
| `["2"][]["2"]` | number | Declared number of participants at this price |
| `["2"][]["3"]` | array | Array of BookParticipant records |
| `["3"][]["0"]` | number | Book-level quoted price |
| `["3"][]["1"]` | number | Quantity aggregated at this price level |
| `["3"][]["2"]` | number | Declared number of participants at this price |
| `["3"][]["3"]` | array | Array of BookParticipant records |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## Book participant

Mounts content[]["2"][]["3"][] and content[]["3"][]["3"][]. Participant quote time differs from batch time.

| Field | Document / parser type | Meaning |
|---|---|---|
| `["2"][]["3"][]["0"]` | string | Participant/venue source identifier |
| `["2"][]["3"][]["1"]` | number | Participant quantity at this price |
| `["2"][]["3"][]["2"]` | number | Participant quote clock; precise upstream encoding unverified |
| `["3"][]["3"][]["0"]` | string | Participant/venue source identifier |
| `["3"][]["3"][]["1"]` | number | Participant quantity at this price |
| `["3"][]["3"][]["2"]` | number | Participant quote clock; precise upstream encoding unverified |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## SCREENER_EQUITY / SCREENER_OPTION

Shared content[] object. Field 4 is a ScreenerItem array; timestamp/sortField/frequency describe ranking basis.

| Field | Document type | Meaning |
|---|---|---|
| `["0"]` | string [String] | Ranking lookup identifier for actives/gainers/losers |
| `["1"]` | integer [long] | Ranking snapshot Unix milliseconds |
| `["2"]` | string [String] | Ranking metric name |
| `["3"]` | integer [Integer] | Source ranking interval/frequency; unit/enum unverified |
| `["4"]` | ScreenerItem[] [Array] | Array of ranked ScreenerItem objects |

Source: [market-data-documentation-20260916.json](../../../adapters/market-squawk-adapter-schwab/resources/market-data-documentation-20260916.json), [streamer_dictionary.rs](../../../adapters/market-squawk-adapter-schwab/src/canonical/streamer_dictionary.rs), [streamer.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## ScreenerItem

Prefix `$.data[service={SCREENER_EQUITY|SCREENER_OPTION}].content[]["4"][]`. These named keys belong to item objects. Local optional/null handling is not proof of upstream requiredness.

| Field | Parser value type | Meaning |
|---|---|---|
| `description` | string | Ranked security display description |
| `lastPrice` | number | Most recent reported execution price |
| `marketShare` | number | Reported ranking market-share metric; denominator/scaling unverified |
| `netChange` | number | Reported absolute price change from ranking baseline |
| `netPercentChange` | number | Reported percentage change; scaling unverified |
| `symbol` | string | Provider symbol of ranked security |
| `totalVolume` | number | Reported total traded quantity; ranking interval context required |
| `trades` | number | Number of executions in ranking interval |
| `volume` | number | Alternate reported traded-quantity member; exact relationship to totalVolume unverified |

Source: [streamer_screener.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_screener.rs), [streamer_family_publication.rs](../../../adapters/market-squawk-adapter-schwab/src/streamer_family_publication.rs).

## Scope and unknowns

All audited services are retained, including futures/options/FX not currently requested. The full field dictionaries exceed current canonical subsets. Entitlement, complete subscription, actual nullability and original live message values are not verified by a static dictionary. LEVELONE_EQUITIES field24 is mutual-fund NAV; it is not ETF NAV.
