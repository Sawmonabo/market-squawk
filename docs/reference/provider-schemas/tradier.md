# Tradier market-data schemas — dormant surface

Reviewed **2026-10-10** against current source and primary documentation. [Selected architecture](../../architecture/market-data-provider-architecture.md) explicitly keeps Tradier outside selected import, activation, scheduling, fallback, composition and release gates. This reference describes the retained adapter; it does not activate it or establish entitlement. Only market-data REST and streaming families are covered. No account/trading API, live request or credential inspection was performed.

Sources: [REST requests](../../../adapters/market-squawk-adapter-tradier/src/rest/mod.rs), [REST wire projection](../../../adapters/market-squawk-adapter-tradier/src/rest/normalize.rs), [stream transport/session](../../../adapters/market-squawk-adapter-tradier/src/source.rs), [stream decoder](../../../adapters/market-squawk-adapter-tradier/src/decoder.rs).

## REST requests and envelopes

Source: [official quotes endpoint](https://docs.tradier.com/reference/brokerage-api-markets-get-quotes), [official chain endpoint](https://docs.tradier.com/reference/brokerage-api-markets-get-options-chains), [local encoder](../../../adapters/market-squawk-adapter-tradier/src/rest/mod.rs).

| Request | Query members / types | Response envelope |
|---|---|---|
| `GET https://api.tradier.com/v1/markets/quotes` | `symbols:string`, comma-separated; `greeks:boolean`, encoded as `true`/`false` | Object → `quotes:object` → `quote:Quote \| array<Quote>` |
| `GET https://api.tradier.com/v1/markets/options/chains` | `symbol:string`; `expiration:string`, `YYYY-MM-DD`; `greeks:boolean` | Object → `options:object` → `option:OptionQuote \| array<OptionQuote>` |

Both requests send bearer authorization, JSON Accept and identity encoding. Upstream quote request also offers `includeLotSize:boolean`; the current encoder does not send it. No page cursor is implemented or documented for these two families. They return bounded whole-response snapshots. Chain `options` or `option` absent/null becomes an empty chain locally; quotes require both container members. This parser behavior is not a universal upstream presence guarantee.

## Quote object: admitted REST projection

Sources: [official quote field meanings and examples](https://docs.tradier.com/docs/quotes), [local normalization](../../../adapters/market-squawk-adapter-tradier/src/rest/normalize.rs). Official examples show numeric prices/sizes and integer epoch-millisecond dates; they are examples rather than a comprehensive required/nullability specification.

| Quote member | Documented/example wire type | Meaning / units | Local presence |
|---|---|---|---|
| `symbol` | string | Source security identity | Required |
| `type` | string | `stock`, `option`, `etf`, `index`, `mutual_fund` upstream | Required; configured kind must agree; mutual fund not admitted |
| `last` | number | Last price, USD | Optional/null |
| `trade_date` | integer | Last trade time, Unix milliseconds | Optional/null |
| `bid`, `ask` | number | Side price, USD | Optional/null |
| `bidsize`, `asksize` | integer | Quoted size; official REST field reference says hundreds | Optional/null |
| `bidexch`, `askexch` | string | Side exchange code | Optional/null |
| `bid_date`, `ask_date` | integer | Side time, Unix milliseconds | Optional/null |

All scalar price, size and timestamp members above also accept JSON strings in the local exact-scalar parser. Other JSON types fail. Unknown REST members are ignored by this projection and remain in retained original bytes. Exact requested coverage, duplicate symbols and unexpected symbols/types are checked; an incomplete response is not a valid smaller universe.

Local last/time must be a complete pair, except zero last means unavailable. A present side needs positive price/size and exchange/time; zero price plus zero size means absent, while a mixed zero/nonzero pair fails. Negatives fail. Quote quantities are locally multiplied by 100 for equity/ETF and by 1 for options/derived indexes; this is an adapter interpretation, not an assertion that every instrument's upstream size unit is identical. Trade size below is unscaled. Derived-index REST observations use last/trade time with a modeled quality ceiling, not executable book authority.

### Additional upstream quote members, outside that projection

The [official quote response](https://docs.tradier.com/docs/quotes) describes these members. Wire types below are observed in its published examples; absence, nullability and a complete enum contract remain unverified unless explicitly stated. The selected REST quote normalizer does not expose them.

| Members | Example wire type | Meaning |
|---|---|---|
| `description`, `exch` | string | Display description and listing exchange |
| `change`, `change_percentage` | number | Price change in USD and percent |
| `open`, `high`, `low`, `close` | number; example `close:null` | Session prices |
| `prevclose`, `week_52_high`, `week_52_low` | number | Previous close / yearly extrema |
| `volume`, `average_volume`, `last_volume` | integer | Session, 90-day average and latest-trade volume |
| `root_symbols` | string | Comma-delimited roots for an underlier |
| Option metadata and `greeks` | See below | Applies to option quotes |

## Option-chain row and nested Greeks

Sources: [official chain family](https://docs.tradier.com/reference/brokerage-api-markets-get-options-chains), [official option quote fields](https://docs.tradier.com/docs/quotes), [chain/Greeks parser](../../../adapters/market-squawk-adapter-tradier/src/rest/normalize.rs). Chain rows share quote fields above; the local chain projection selects contract identity, prices, sizes, volume/open interest and optional Greeks. ORATS supplies upstream Greek/IV data.

| OptionQuote member | Documented/example wire type | Meaning | Local presence |
|---|---|---|---|
| `symbol`, `type` | string | Contract identifier; `type:"option"` | Required |
| `underlying`, `root_symbol` | string | Underlier and contract root | Required; underlier must match request |
| `option_type` | string: `call` or `put` | Contract side | Required |
| `strike` | number | Strike price, USD | Required, positive |
| `contract_size` | integer | Contract deliverable size; typically 100, not guaranteed | Required, positive |
| `expiration_date` | string, `YYYY-MM-DD` | Contract expiration | Required; must match request |
| `expiration_type` | string | Provider expiration category | Not projected; complete enum unverified |
| `bid`, `ask`, `last` | number | Option premium in USD | Optional/null; zero becomes unavailable locally |
| `bidsize`, `asksize`, `volume`, `open_interest` | integer | Quote sizes, volume and open contracts | Optional/null; nonnegative locally |
| `greeks` | object | Modeled analytics below | Optional/null |

Required contract fields cannot be null. Numeric fields also accept decimal strings locally. Unknown fields are ignored. Duplicate contracts and mismatched underlying/type/expiration fail; a local 10,000-contract ceiling is not a provider paging limit.

| Greeks member | Example wire type | Meaning / parser treatment |
|---|---|---|
| `delta`, `gamma`, `theta`, `vega`, `rho`, `phi` | number | Corresponding sensitivities; signed exact decimals admitted |
| `bid_iv`, `mid_iv`, `ask_iv` | number | Bid/mid/ask implied volatility; nonnegative locally |
| `smv_vol` | number | ORATS final implied volatility; nonnegative locally |
| `updated_at` | string | Provider analytics update text |

Every projected Greeks member may be missing/null locally; numeric strings are also admitted. Upstream volatility examples use fractional values; authoritative annualization and Greek unit/scaling conventions were not established by the linked field definitions and must not be guessed. `updated_at` is retained bounded ASCII text, not parsed as UTC: the official example has date/time without a timezone. No market freshness or executable quote authority follows from this modeled analytics timestamp.

REST decimal parsing bounds text to 128 bytes, rejects scientific notation and preserves exact decimal values. REST epoch milliseconds are digit-only, positive and checked before conversion to nanoseconds. These are local validation rules rather than upstream type extensions.

## Streaming session and subscription

Sources: [official market session endpoint](https://docs.tradier.com/reference/brokerage-api-streaming-create-market-session), [official session response](https://docs.tradier.com/docs/session), [official WebSocket request](https://docs.tradier.com/reference/websocket-market-data-streaming), [local session/subscription](../../../adapters/market-squawk-adapter-tradier/src/source.rs).

`POST https://api.tradier.com/v1/markets/events/session` has no current request body and uses bearer authorization. Response is object → `stream:object` with `url:string` and `sessionid:string`. Local parser requires both, rejects unknown/null members, admits exactly `https://stream.tradier.com/v1/markets/events` as the returned URL, and bounds session ID to 256 ASCII alphanumeric/hyphen bytes. That returned URL is an HTTP-stream URL; current transport connects separately to `wss://ws.tradier.com/v1/markets/events`.

| WebSocket request member | Wire type | Current value / purpose |
|---|---|---|
| `symbols` | array<string> | Complete active source-symbol selection |
| `sessionid` | string | Short-lived session identity; use immediately |
| `filter` | array<string> | Exactly `quote`, `tradex` |
| `linebreak` | boolean | true; newline-delimited events |
| `validOnly` | boolean | true; valid exchange ticks |
| `advancedDetails` | boolean | false; extended timesale details not requested |

Symbols can be replaced by resending the selection; filters are fixed for the stream. Current transport splits a text message into nonempty trimmed lines, each exactly one JSON event, with a local 1,024-event/message bound. One physical market-data stream per access owner is enforced locally and documented upstream; no per-symbol repeated sessions are implied. [Official session lifecycle](https://docs.tradier.com/docs/streaming-data) requires reconnect handling and a fresh expired session. WebSocket ping/pong is transport liveness, not a timestamped financial heartbeat family.

## Streaming `quote` object

Source: [official streaming response](https://docs.tradier.com/docs/streaming), [strict QuoteWire decoder](../../../adapters/market-squawk-adapter-tradier/src/decoder.rs). The official example shows numeric quote price/size and string timestamp; local scalar allowance accepts either string or number.

| Member | Example wire type | Meaning / local presence |
|---|---|---|
| `type` | string | Required `quote` |
| `symbol` | string | Required configured security |
| `bid`, `ask` | number | USD price; optional/null locally |
| `bidsz`, `asksz` | integer | Quote size; optional/null locally; instrument conversion described above |
| `bidexch`, `askexch` | string | Required exchange codes, including when a side is absent |
| `biddate`, `askdate` | string of integer milliseconds | Side event time; optional/null locally; needed for each present side |

This is a complete quote event, not a price-level book delta. Local parser rejects unknown fields and malformed side pairs. Combined quote event time is the earlier time of its present sides, preserving conservative freshness rather than treating publication as both sides' update time.

## Streaming `tradex` object

Source: [official Tradex definition](https://docs.tradier.com/docs/streaming), [strict TradexWire decoder](../../../adapters/market-squawk-adapter-tradier/src/decoder.rs). Upstream recommends this family for more accurate pre/post-market information than `trade`.

| Member | Example wire type | Meaning / local rule |
|---|---|---|
| `type`, `symbol`, `exch` | string | Required `tradex`, configured security, reporting exchange |
| `price`, `last` | decimal string | USD price; both required, positive and equal locally |
| `size` | integer string | Required positive executed size; no quote multiplier |
| `cvol` | integer string | Required cumulative volume, nonnegative |
| `date` | integer string | Required Unix-millisecond event time |

Numeric JSON substitutes are accepted locally. Unknown/null fields fail. Tradex carries no provider trade ID, aggressor side, sequence or checksum; payload digest supplies local event identity, not a new upstream field. The decoder records sequence/checksum as unsupported and aggressor as unknown. The nonrequested `timesale` family has its own `seq`, cancellation/correction and session semantics; those must not be attributed to `tradex`. Summary/trade/timesale are not current financial projections.

## HTTP rate evidence and errors

Sources: [official rate limits](https://docs.tradier.com/docs/rate-limiting), [rate parser](../../../adapters/market-squawk-adapter-tradier/src/rate_limit.rs), [REST transport](../../../adapters/market-squawk-adapter-tradier/src/rest/transport.rs).

| Header | Wire type | Meaning / local validation |
|---|---|---|
| `X-Ratelimit-Allowed`, `X-Ratelimit-Used`, `X-Ratelimit-Available` | ASCII unsigned integer text | Window allowance/consumed/remaining; unique, required, bounded `u32`; used + available = allowed |
| `X-Ratelimit-Expiry` | ASCII unsigned integer text | Positive Unix-millisecond reset, locally `u64` |

Market-data production documentation currently states 120 requests/minute and sandbox 60 per token; actual response evidence controls local budgeting. REST requires HTTP 200, JSON media type, no non-identity content encoding, complete rate headers and bounded nonempty body. Session creation admits success statuses locally but still requires valid session/rate evidence. 401/403 become unauthorized; 429/server errors apply shared refusal/retry-after evidence; other non-200 REST statuses become unavailable. No internal polling loop or replay continuity guarantee is implied.

The [official stream error](https://docs.tradier.com/docs/streaming-data) is an object with `error:string`. A nonempty bounded error triggers local resynchronization; malformed error text fails schema validation. Other recognized-but-unprojected event types are ignored as extensions, not synthesized into quotes. REST error-body structure is not frozen by this adapter's success parser.

## Coverage and missing evidence

Covered: dormant quote/derived-index REST projection, option-chain/Greeks, market session/subscription, quote/tradex events and rate/error handling. Missing: authoritative full REST required/nullability contract, complete expiration/exchange enumerations, instrument-specific wire size rules beyond the published field description, analytic unit/timezone conventions and live original observations. Official documentation examples establish shapes only; no repository/personal symbols or synthetic provider originals are reproduced. Reference review is not proof of activation, entitlement, connectivity, durable publication or selected-workflow completion.
