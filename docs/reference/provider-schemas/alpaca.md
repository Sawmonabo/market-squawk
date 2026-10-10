# Alpaca market-data and reference response projection

Contents: [Endpoints and envelopes](#endpoints-and-envelopes) · [StockQuote](#stockquote) · [StockTrade](#stocktrade) · [Bar](#bar) · [OptionQuote](#optionquote) · [OptionTrade](#optiontrade) · [OptionSnapshot model fields](#optionsnapshot-model-fields) · [Contract](#contract) · [Deliverable](#deliverable) · [Asset](#asset) · [Corporate-action category union](#corporate-action-category-union) · [Stock movement](#stock-movement) · [WebSocket responses](#websocket-responses) · [Missing schema evidence](#missing-schema-evidence)

Scope: surfaces consumed or parsed by this application, with documented stream variants used to explain related shapes. This is not a complete Alpaca API specification.

REST objects use JSON. Stock streams use JSON message arrays; the indicative option stream uses MessagePack. Component clocks belong to their own trade, quote or bar. These are upstream field schemas, separate from normalized application outputs.

[Snapshots](https://docs.alpaca.markets/us/reference/stocksnapshots-1), [option snapshots](https://docs.alpaca.markets/us/v1.1/reference/optionsnapshots), [contracts](https://docs.alpaca.markets/us/reference/get-options-contracts), [corporate actions](https://docs.alpaca.markets/us/reference/corporateactions-1), [stream schemas](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data).

**Evidence:** retained 2026-10-09 HTTP 200 snapshot bodies contain quote/trade/bar primitives. Observed values below come from those originals, with instrument identifiers omitted. Local evidence: local-only provenance locator `.agents/tmp/v1-first-stock/oct9-exact-field-examples.json`, SHA-256 `292345fdfc54b4f328108da970b12c9d9bec7a260f5e367fe4bee8a3e6aea478`. Contract-reference, actions and history have parser evidence without inspected originals. Rust Option alone does not establish upstream omission or nullability.

## Endpoints and envelopes

| Method and path | Successful response | Continuation |
|---|---|---|
| GET `https://data.alpaca.markets/v2/stocks/snapshots?symbols=…&feed=…` | Root symbol map of Snapshot | No cursor in inspected response |
| GET `https://data.alpaca.markets/v2/stocks/{symbol}/snapshot` | One Snapshot; documented sibling route | No batch symbol map |
| GET `https://data.alpaca.markets/v2/stocks/bars` | `bars: {symbol: Bar[]}`, `next_page_token` | Supply token as `page_token`; preserve feed/timeframe/adjustment |
| GET `https://data.alpaca.markets/v1beta1/options/snapshots/{underlying}` | `snapshots: {contract: OptionSnapshot}`, `next_page_token` | Observed page had continuation; not a complete chain |
| GET `https://data.alpaca.markets/v1beta1/options/snapshots?symbols=…` | Same snapshot map | Documented multi-contract route |
| GET `https://paper-api.alpaca.markets/v2/options/contracts` | `option_contracts: Contract[]`, `next_page_token` | `page_token`; `show_deliverables=true` changes shape |
| GET `https://paper-api.alpaca.markets/v2/assets/{symbol-or-id}` | Asset object | Identity projection below |
| GET `https://data.alpaca.markets/v1/corporate-actions` | `corporate_actions: {category: Action[]}`, `next_page_token` | Process-date interval and economic dates differ |

Paginated local parsers admit token omission/null; the exact upstream terminal form is not independently observed here. Non-success bodies have no inspected stable JSON error schema. The official REST reference describes 400/401/403/429/500; preserve status/body rather than parse failures as snapshots.

### StockSnapshot object

Stock batch prefix: `$[symbol]`. Its five components mount StockQuote, StockTrade and Bar below.

| Field | Shape | Meaning |
|---|---|---|
| `latestQuote` | StockQuote | Most recent quote |
| `latestTrade` | StockTrade | Most recent trade |
| `minuteBar` | Bar | Latest minute aggregate |
| `dailyBar` | Bar | Current daily aggregate |
| `prevDailyBar` | Bar | Previous daily aggregate |

Stock originals contain five components; future presence is not guaranteed.

### OptionSnapshot object

Mount `$.snapshots[contract]`. OptionQuote and OptionTrade are separate variants. Local omission/null allowance does not establish upstream requiredness.

| Field | Parsed or observed shape | Meaning |
|---|---|---|
| `latestQuote` | OptionQuote | Latest quote, subject to selected feed |
| `latestTrade` | OptionTrade | Latest execution, subject to selected feed |
| `greeks` | Greeks | Model sensitivities; absent in inspected original |
| `impliedVolatility` | number | Volatility inferred from option prices; absent in inspected original |
| `minuteBar` | Bar; observed raw | Latest minute aggregate |
| `dailyBar` | Bar; observed raw | Daily aggregate, not current open interest |
| `prevDailyBar` | Bar; observed raw | Prior daily aggregate |

Ancillary bars were observed in one retained option original; the option decoder ignores their contents. They are not guaranteed snapshot members.

## StockQuote

Mount `$[symbol].latestQuote`. This object has no currency member and the request has no currency parameter. Denomination requires separate source/reference context. Times use RFC 3339. Stock stream sizes use round lots; REST conversion is unverified.

| Field | JSON type / local allowance | Meaning | Observed value |
|---|---|---|---|
| `ap` | number | Offered ask price | `778.62` |
| `as` | integer (observed) | Ask quantity; REST trading unit unverified | `1480` |
| `ax` | string | Ask exchange code | `"V"` |
| `bp` | number | Offered bid price | `778.52` |
| `bs` | integer (observed) | Bid quantity; REST trading unit unverified | `1480` |
| `bx` | string | Bid exchange code | `"V"` |
| `c` | string[] | Quote condition codes | `["R"]` |
| `t` | string | Quote event time, RFC 3339 with observed nanoseconds | `"2026-10-09T20:00:49.824864261Z"` |
| `z` | string | Market-data tape code | `"B"` |

Source: [decoder.rs](../../../adapters/market-squawk-adapter-alpaca/src/decoder.rs).

## StockTrade

Mount `$[symbol].latestTrade`. Its execution timestamp is independent of quote/bar clocks.

| Field | JSON type / local allowance | Meaning | Observed value |
|---|---|---|---|
| `c` | string[] | Trade condition codes | `[" ", "T"]` |
| `i` | integer (observed) | Provider trade identifier | `52983964058196` |
| `p` | number | Execution price | `778.6` |
| `s` | integer (observed) | Executed quantity; instrument unit supplied separately | `1000` |
| `t` | string | Execution time, RFC 3339 with observed nanoseconds | `"2026-10-09T20:00:33.066440886Z"` |
| `x` | string | Execution exchange code | `"V"` |
| `z` | string | Market-data tape code | `"B"` |

Source: [decoder.rs](../../../adapters/market-squawk-adapter-alpaca/src/decoder.rs).

## Bar

Mounts: `minuteBar`, `dailyBar`, `prevDailyBar` and `$.bars[symbol][]`. OHLC/VWAP are prices; volume and trade count are separate counts. Time identifies the source period; a daily date does not imply an invented UTC-midnight event.

| Field | JSON type / local allowance | Meaning | Observed value |
|---|---|---|---|
| `c` | number | Last eligible price in period | `778.51` |
| `h` | number | Highest eligible price in period | `779.41` |
| `l` | number | Lowest eligible price in period | `775.195` |
| `n` | integer (observed) | Number of aggregated trades | `13869` |
| `o` | number | First eligible price in period | `776.23` |
| `t` | string | Aggregate period timestamp, RFC 3339 | `"2026-10-09T04:00:00Z"` |
| `v` | integer (observed) | Aggregated traded quantity; stock share volume | `705445` |
| `vw` | number | Volume-weighted mean price of aggregate | `777.706382` |

Source: [historical.rs](../../../adapters/market-squawk-adapter-alpaca/src/historical.rs).

## OptionQuote

Mount `$.snapshots[contract].latestQuote`. The eight quoted price/size/exchange/condition/time values were observed in one indicative original. The additional z key is parser-admitted only. Initial key validation admits ignored JSON values and supplies no type guarantee.

| Field | Observed type | Meaning |
|---|---|---|
| `ap` | number | Offered ask price |
| `as` | integer | Ask quantity; REST trading unit unverified |
| `ax` | string | Ask exchange code |
| `bp` | number | Offered bid price |
| `bs` | integer | Bid quantity; REST trading unit unverified |
| `bx` | string | Bid exchange code |
| `c` | string | Single option quote condition code |
| `t` | string | Quote event time, RFC 3339 |
| `z` | unknown; ignored JSON value admitted locally | No option-specific definition or observed value; stock tape semantics are not assumed |

## OptionTrade

Mount `$.snapshots[contract].latestTrade`. Stock trade `i`/`z` keys are outside this narrower admitted shape.

| Field | Observed type | Meaning |
|---|---|---|
| `c` | string | Single option trade condition code |
| `p` | number | Execution price |
| `s` | integer | Executed quantity; REST trading unit unverified |
| `t` | string | Execution time, RFC 3339 |
| `x` | string | Execution exchange code |

[Option stream specification](https://docs.alpaca.markets/us/docs/real-time-option-data) establishes scalar conditions. Source: [option_chain.rs](../../../adapters/market-squawk-adapter-alpaca/src/option_chain.rs).

## OptionSnapshot model fields

Prefix `$.snapshots[contract]`. The normalizer accepts JSON numbers. IV annualization and Greek volatility-point/time conventions are not established by the inspected original. All five Greeks remain distinct; absent is not zero. Meanings use [OIC Greek definitions](https://prd-web.optionseducation.org/advancedconcepts/volatility-the-greeks), which do not establish Alpaca reporting units.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `greeks.delta` | number/null | Option-value sensitivity to underlying price |
| `greeks.gamma` | number/null | Delta sensitivity to underlying price |
| `greeks.theta` | number/null | Option-value sensitivity to elapsed time; provider day/year scale unverified |
| `greeks.vega` | number/null | Option-value sensitivity to implied volatility; provider point scale unverified |
| `greeks.rho` | number/null | Option-value sensitivity to interest rates; provider point scale unverified |
| `impliedVolatility` | number/null | Volatility inferred from option price; annualization/scaling unverified |

Source: [option_chain.rs](../../../adapters/market-squawk-adapter-alpaca/src/option_chain.rs).


## Contract

Prefix `$.option_contracts[]`. Strike, size, multiplier, dated close and open-interest values are decimal strings in this parser. Dates use YYYY-MM-DD. Local validated enums: type call/put; style american/european; status active for this operation. Deliverables depend on the request and are explicitly required by this identity operation.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `id` | string | Alpaca contract UUID, distinct from symbol |
| `symbol` | string | Provider option identifier validated against terms |
| `name` | string | Contract display name |
| `status` | string | Contract state; active requested/validated locally |
| `tradable` | boolean | Tradability flag; not execution authorization |
| `expiration_date` | string | Civil expiration date, YYYY-MM-DD |
| `root_symbol` | string/null | Option root; may differ from underlying for adjusted contracts |
| `underlying_symbol` | string | Underlying security alias |
| `underlying_asset_id` | string | Alpaca UUID for underlying |
| `type` | string | Call/put kind |
| `style` | string | American/European exercise style |
| `strike_price` | string | Exercise price as decimal text; currency supplied separately |
| `multiplier` | string | Premium multiplier, distinct from size/deliverable quantity |
| `size` | string | Separately reported contract size |
| `deliverables` | array<object> | Delivered asset/cash components requested with show_deliverables |
| `open_interest` | string/null | Outstanding contract count tied to open_interest_date |
| `open_interest_date` | string/null | Civil date of reported open interest |
| `close_price` | string/null | Reported close tied to close_price_date |
| `close_price_date` | string/null | Civil date of reported close |
| `ppind` | boolean/null (local allowance) | Penny Interval Program participation flag |

Source: [options_contract_reference.rs](../../../adapters/market-squawk-adapter-alpaca/src/options_contract_reference.rs); `ppind`: [Alpaca Penny Program explanation](https://alpaca.markets/support/options-pricing-increments-and-options-order-handling).

## Deliverable

Prefix `$.option_contracts[].deliverables[]`. Allocation percentage is parsed in 0–100 percent units. Adapter enum sets: type equity/cash; settlement_type T+0 through T+5; settlement_method BTOB/CADF/CAFX/CCC. These validation sets do not prove future provider enums are closed.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `type` | string | Delivered category: equity/cash |
| `symbol` | string | Delivered asset/currency identifier |
| `asset_id` | string/null | Alpaca asset UUID when supplied |
| `amount` | string | Delivered component amount; equity quantity is not premium multiplier |
| `allocation_percentage` | string | Allocation in percent units; local 0–100 validation |
| `settlement_type` | string | Settlement timing code; local T+0 through T+5 set |
| `settlement_method` | string | Settlement method code; BTOB/CADF/CAFX/CCC admitted, expansions unverified |
| `delayed_settlement` | boolean | Flag indicating delayed settlement |

Source: [options_contract_reference.rs](../../../adapters/market-squawk-adapter-alpaca/src/options_contract_reference.rs).

## Asset

Prefix `$`. The operation projects five identity/status keys, without establishing a complete company-profile schema.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `$.id` | string | Alpaca asset UUID |
| `$.symbol` | string | Trading alias |
| `$.exchange` | string | Listing exchange code |
| `$.class` | string | Asset classification; complete enum unverified |
| `$.status` | string | Asset status; complete enum unverified |

Source: [asset_reference.rs](../../../adapters/market-squawk-adapter-alpaca/src/asset_reference.rs).

## Corporate-action category union

Prefix `$.corporate_actions[category][]`. Common parser fields: id, process_date, currency. Other fields depend on category. Calendar-date strings separately identify process/ex/effective/record/payable/due-bill dates. Numbers represent amounts or share ratios; missing currency remains unknown. This is the parser field union, not one universal action object. The official endpoint documents incomplete records under data_quality=all.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `id` | string | Source action identifier |
| `process_date` | string | Processing date, distinct from economic dates |
| `symbol` | string/null | Affected security alias |
| `cusip` | string/null | Affected security CUSIP |
| `isin` | string/null | Affected security ISIN |
| `currency` | string/null | Currency of cash amounts, if supplied |
| `old_symbol` | string/null | Pre-action security alias; category-specific |
| `old_cusip` | string/null | Pre-action security CUSIP; category-specific |
| `old_isin` | string/null | Pre-action security ISIN; category-specific |
| `new_symbol` | string/null | Post-action security alias; category-specific |
| `new_cusip` | string/null | Post-action security CUSIP; category-specific |
| `new_isin` | string/null | Post-action security ISIN; category-specific |
| `source_symbol` | string/null | Source security alias; category-specific |
| `source_cusip` | string/null | Source security CUSIP; category-specific |
| `source_isin` | string/null | Source security ISIN; category-specific |
| `alternate_symbol` | string/null | Alternate security alias; category-specific |
| `alternate_cusip` | string/null | Alternate security CUSIP; category-specific |
| `alternate_isin` | string/null | Alternate security ISIN; category-specific |
| `acquiree_symbol` | string/null | Acquired security alias; category-specific |
| `acquiree_cusip` | string/null | Acquired security CUSIP; category-specific |
| `acquiree_isin` | string/null | Acquired security ISIN; category-specific |
| `acquirer_symbol` | string/null | Acquiring security alias; category-specific |
| `acquirer_cusip` | string/null | Acquiring security CUSIP; category-specific |
| `acquirer_isin` | string/null | Acquiring security ISIN; category-specific |
| `rate` | number/null | Category-specific distribution amount/share ratio |
| `old_rate` | number/null | Pre-action security quantity/ratio component; category-specific |
| `new_rate` | number/null | Post-action security quantity/ratio component; category-specific |
| `source_rate` | number/null | Source security quantity/ratio component; category-specific |
| `alternate_rate` | number/null | Alternate security quantity/ratio component; category-specific |
| `acquiree_rate` | number/null | Acquired security quantity/ratio component; category-specific |
| `acquirer_rate` | number/null | Acquiring security quantity/ratio component; category-specific |
| `cash_rate` | number/null | Cash consideration component |
| `long_term_rate` | number/null | Long-term capital-gain distribution component |
| `short_term_rate` | number/null | Short-term capital-gain distribution component |
| `price` | number/null | Category-specific amount; interpretation depends on action |
| `dividend_rate` | number/null | Reported dividend component |
| `special` | boolean/null | Special-distribution flag |
| `foreign` | boolean/null | Foreign-distribution flag |
| `sub_type` | string/null | Action subtype; complete enum unverified |
| `ex_date` | string/null | Ex-entitlement civil date |
| `effective_date` | string/null | Date action takes effect |
| `record_date` | string/null | Date determining holders of record |
| `payable_date` | string/null | Date proceeds are payable |
| `due_bill_on_date` | string/null | Start of due-bill interval |
| `due_bill_off_date` | string/null | End of due-bill interval |
| `due_bill_redemption_date` | string/null | Due-bill redemption date |
| `expiration_date` | string/null | Expiry of rights/dated entitlement |
| `lottery_date` | string/null | Partial-call selection lottery date |
| `results_publication_date` | string/null | Date lottery results published |
| `lottery_type` | string/null | Selection-lottery code; enum unverified |
| `stock_movements` | array<object>/null | Delivered-security components of reorganization |

Source: [native.rs](../../../adapters/market-squawk-adapter-alpaca/src/corporate_actions/native.rs).

### Category member sets

Each key maps to an array. These are exact local admitted sets; upstream per-category required/null flags need the complete response specification.

| Category | Additional admitted members |
|---|---|
| `reverse_splits` | `symbol`, `old_cusip`, `new_cusip`, `old_isin`, `new_isin`, `new_symbol`, `new_rate`, `old_rate`, `ex_date`, `record_date`, `payable_date` |
| `forward_splits` | `symbol`, `cusip`, `isin`, `new_rate`, `old_rate`, `ex_date`, `record_date`, `payable_date`, `due_bill_redemption_date` |
| `unit_splits` | `old_symbol`, `old_cusip`, `old_isin`, `old_rate`, `new_symbol`, `new_cusip`, `new_isin`, `new_rate`, `alternate_symbol`, `alternate_cusip`, `alternate_isin`, `alternate_rate`, `effective_date`, `payable_date` |
| `cash_dividends` | `symbol`, `cusip`, `isin`, `rate`, `special`, `foreign`, `ex_date`, `record_date`, `payable_date`, `due_bill_on_date`, `due_bill_off_date`, `sub_type` |
| `stock_dividends` | `symbol`, `cusip`, `isin`, `rate`, `ex_date`, `record_date`, `payable_date` |
| `spin_offs` | `source_symbol`, `source_cusip`, `source_isin`, `source_rate`, `new_symbol`, `new_cusip`, `new_isin`, `new_rate`, `ex_date`, `record_date`, `payable_date`, `due_bill_redemption_date` |
| `cash_mergers` | `acquiree_symbol`, `acquiree_cusip`, `acquiree_isin`, `acquirer_symbol`, `acquirer_cusip`, `acquirer_isin`, `rate`, `effective_date`, `payable_date` |
| `stock_mergers` | `acquiree_symbol`, `acquiree_cusip`, `acquiree_isin`, `acquiree_rate`, `acquirer_symbol`, `acquirer_cusip`, `acquirer_isin`, `acquirer_rate`, `effective_date`, `payable_date` |
| `stock_and_cash_mergers` | `acquiree_symbol`, `acquiree_cusip`, `acquiree_isin`, `acquiree_rate`, `acquirer_symbol`, `acquirer_cusip`, `acquirer_isin`, `acquirer_rate`, `effective_date`, `payable_date`, `cash_rate` |
| `redemptions` | `symbol`, `cusip`, `isin`, `rate`, `payable_date` |
| `name_changes` | `old_symbol`, `old_cusip`, `old_isin`, `new_symbol`, `new_cusip`, `new_isin` |
| `worthless_removals` | `symbol`, `cusip`, `isin` |
| `rights_distributions` | `source_symbol`, `source_cusip`, `source_isin`, `new_symbol`, `new_cusip`, `new_isin`, `rate`, `ex_date`, `record_date`, `payable_date`, `expiration_date` |
| `partial_calls` | `symbol`, `cusip`, `isin`, `price`, `dividend_rate`, `record_date`, `payable_date`, `lottery_type`, `lottery_date`, `results_publication_date` |
| `reorganizations` | `symbol`, `cusip`, `isin`, `cash_rate`, `stock_movements`, `effective_date`, `payable_date` |
| `capital_gains_distributions` | `symbol`, `cusip`, `isin`, `long_term_rate`, `short_term_rate`, `ex_date`, `record_date`, `payable_date` |

## Stock movement

Mount `$.corporate_actions.reorganizations[].stock_movements[]`. Each entry identifies a delivered security and its source/new share ratio.

| Field | JSON type / local allowance | Meaning |
|---|---|---|
| `symbol` | string | Alias of delivered security |
| `cusip` | string | CUSIP of delivered security |
| `isin` | string/null | ISIN of delivered security if supplied |
| `new_rate` | number | Delivered quantity component |
| `source_rate` | number | Original quantity component paired with new_rate |

Source: [native.rs](../../../adapters/market-squawk-adapter-alpaca/src/corporate_actions/native.rs).

## WebSocket responses

`wss://stream.data.alpaca.markets/v2/{feed}` emits independently timestamped JSON messages in arrays. Indicative option data uses MessagePack at `wss://stream.data.alpaca.markets/v1beta1/indicative`. Events use T as message type and S as symbol, separate from REST envelopes.

| T | Additional source fields | Meaning |
|---|---|---|
| q | S + StockQuote | Quote event |
| t | S + StockTrade | Trade event |
| b/d/u | S + Bar | Minute / daily / revised minute aggregate |
| s | S,sc,sm,rc,rm,t,z | Status/reason codes/messages |
| l | S,u,d,i,t,z | LULD upper/lower bands and indicator |
| c | S,x,oi,op,os,oc,ci,cp,cs,cc,t,z | Original/corrected ID, price, size, conditions |
| x | S,i,x,p,s,a,t,z | Cancel/error; a C/E |
| i | S,p,z,t | Documented imbalance; outside current decoder |
| success | msg | Connection/auth acknowledgment |
| error | code,msg | Numeric error code and message |
| subscription | trades,quotes,bars,updatedBars,dailyBars,statuses,lulds,corrections,cancelErrors | Arrays of acknowledged stock subscriptions |

These documented messages exceed the current trade/quote/status decoder subset. Times are RFC 3339, price fields are numbers, IDs/sizes/counts are numbers, and stock conditions are string arrays. Corrections/cancels reference earlier trade identity; an omitted field is not a REST snapshot omission.
Source: [decoder.rs](../../../adapters/market-squawk-adapter-alpaca/src/decoder.rs).

## Missing schema evidence

Snapshot volume, openInterest, markPrice and settlement have no established snapshot wire key here. Dated reference open interest and deliverables are separate objects. No original IV/Greek value, full option pagination, or upstream per-category action requiredness was verified. Bootstrap decoding reads quote/trade; observed ancillary bars are retained raw.
