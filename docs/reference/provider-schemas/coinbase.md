# Coinbase market-data schemas

Reviewed **2026-10-10** against the current working-tree adapter and primary documentation. [Selected architecture](../../architecture/market-data-provider-architecture.md) distinguishes public Advanced Trade crypto books/trades from optional authenticated Exchange Direct market data. Both are venue-specific. This reference grants no account, order or money-movement capability. No live requests or credential access were performed.

## Public Advanced Trade subscriptions and envelope

Connect to `wss://advanced-trade-ws.coinbase.com`. The current [configuration](../../../adapters/market-squawk-adapter-coinbase/src/config.rs) sends one message per selected channel: `type: "subscribe"`, `channel: "level2" | "market_trades" | "heartbeats"`; market subscriptions include `product_ids: array<string>`, heartbeats omit products. No JWT is sent. `level2` requests receive `channel: "l2_data"`.

Sources: [official endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints), [official AsyncAPI schemas](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json), [selected decoder](../../../adapters/market-squawk-adapter-coinbase/src/decoder.rs).

| Envelope field | Wire type | Meaning / presence |
|---|---|---|
| `channel` | string | `l2_data`, `market_trades`, `heartbeats` or control `subscriptions` |
| `timestamp` | string, RFC3339 | Server publication time |
| `sequence_num` | integer | Message sequence coordinate; locally nonnegative `u64` |
| `events` | array<object> | Channel-specific events below |
| `client_id` | string | Local parser extension: omitted defaults to empty; nonempty rejected |

The current decoder requires the first four members and rejects nulls and unknown members on financial/control envelopes. Upstream AsyncAPI lists their properties without comprehensive `required` arrays; local requiredness is stricter and is not a full upstream presence guarantee. Price and quantity strings preserve decimal values; JSON numeric substitutes are rejected on these channels.

## `l2_data`: snapshot and update

Source: [official L2 AsyncAPI contract](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json), [L2 decoder](../../../adapters/market-squawk-adapter-coinbase/src/decoder.rs).

| `events[]` field | Wire type | Meaning |
|---|---|---|
| `type` | string: `snapshot` or `update` | Initializes a book or changes existing price levels |
| `product_id` | string | Exact source product identifier |
| `updates` | array<LevelUpdate> | Ordered price-level records |

| LevelUpdate field | Wire type | Meaning / units |
|---|---|---|
| `side` | string: `bid` or `offer` | Bid/ask side |
| `event_time` | string, RFC3339 | Trading-engine time of this level change |
| `price_level` | decimal string | Quote currency per base unit |
| `new_quantity` | decimal string | Absolute remaining base quantity at this price |

An update replaces quantity; it is not an arithmetic increment. Zero removes the level. Snapshot quantities must be positive locally, prices positive, and arrays nonempty and bounded. The decoder validates level `event_time` but uses envelope `timestamp` for whole-book observation freshness: snapshot levels may contain epoch-zero or older engine times. Coinbase's L2 contract promises delivery of updates; this does not establish a checksum or repair a reconnect without a fresh snapshot. No checksum field exists in this schema.

## `market_trades`

Source: [official trades channel](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/market-trades), [official nested schema](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json), [trade decoder](../../../adapters/market-squawk-adapter-coinbase/src/decoder.rs).

| `events[]` field | Wire type | Meaning |
|---|---|---|
| `type` | string: `snapshot` or `update` | Trade message kind |
| `trades` | array<Trade> | Trades batched within an event |

| Trade field | Wire type | Meaning / units |
|---|---|---|
| `trade_id` | string | Provider trade identity |
| `product_id` | string | Exact source product |
| `price` | decimal string | Quote currency per base unit |
| `size` | decimal string | Traded base quantity |
| `side` | string: `BUY` or `SELL` | Maker side; aggressor is opposite |
| `time` | string, RFC3339 | Original trade event time |

Updates batch the preceding 250 ms. Array position or batch membership alone does not prove a unique most-recent trade when times tie. Local prices/sizes are positive, duplicate trade IDs within a frame rejected, and original trade time retained independently of envelope publication time. Empty trade work is control flow, not a fabricated zero-volume trade. No account identities, trade pagination, or order authority follow from these public trades.

## Heartbeats, acknowledgement and error

Sources: [official heartbeats](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/heartbeats), [AsyncAPI](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json), [control parser](../../../adapters/market-squawk-adapter-coinbase/src/decoder.rs).

| Object / field | Wire type | Meaning / local allowance |
|---|---|---|
| Heartbeat `events[].current_time` | string | Server heartbeat time text; local check is nonempty/bounded, not full RFC3339 parsing |
| Heartbeat `events[].heartbeat_counter` | integer | Heartbeat counter; local parser also accepts unsigned decimal string |
| Subscription `events[].subscriptions` | object<string,array<string>> | Channel names mapped to admitted product IDs |
| Error `type` | string | `error` discriminator |
| Error `message`, `reason` | string or null/absent locally | Provider diagnostics |

Heartbeats arrive each second and keep idle subscriptions open. The official downloadable schema uses an events array, matching the decoder; the rendered heartbeat example currently shows an object. That example discrepancy is not evidence for accepting both shapes. Local acknowledgement parsing permits exactly one event, bounds map/items, rejects duplicate keys and checks the exact subscription set. Its detailed `subscriptions` control envelope is parser evidence rather than a fully frozen upstream schema. Sequence/counter coordinates are integrity evidence; their presence must not be reported as checksum verification or continuity proof without the runtime's actual validation.

Current public market handoff explicitly records `ProviderCursorUnverified` with the message sequence as its terminal cursor; normalized observations record sequence/checksum as unsupported. Heartbeat parsing retains the last counter as control evidence without checking counter progression, and ignores heartbeat `sequence_num`. Public Advanced Trade sequencing here therefore differs materially from the verified snapshot-plus-full-message continuity of optional Direct below.

## Public product reference

`GET https://api.coinbase.com/api/v3/brokerage/market/products/{product_id}` returns a product object, not an array or `products` wrapper. Source: [official public product](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/public/get-public-product), [selected reference projection](../../../adapters/market-squawk-adapter-coinbase/src/reference.rs).

| Product member | Wire type | Meaning / projection |
|---|---|---|
| `product_id` | string | Requested source identity, must match exactly |
| `product_type` | string enum upstream | Locally admits only `SPOT` |
| `base_currency_id`, `quote_currency_id` | string | Base/quote asset identities |
| `is_disabled`, `trading_disabled` | boolean | Both must be false for local reference admission |

These six members are required by the local parser; unknown upstream fields are ignored and original bytes are digest-bound. The larger REST object also documents price, volume, precision, status and non-spot detail structures; this six-field identity projection does not establish their full schema or admit other asset classes. Disabled/not-spot/mismatched objects are unavailable reference, not empty valid products. No pagination applies to single-product lookup.

The same [official product reference](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/public/get-public-product) documents these additional spot-facing members, outside local validation. Presence/nullability is not inferred from the identity parser.

| Members | Wire type | Meaning |
|---|---|---|
| `price`, `mid_market_price`, `best_bid_price`, `best_ask_price`, `high_24h`, `low_24h` | string | Quote/base prices |
| `volume_24h`, `approximate_quote_24h_volume` | string | Base/approximate quote volume |
| `price_percentage_change_24h`, `volume_percentage_change_24h` | string | Percent text |
| `base_increment`, `base_min_size`, `base_max_size` | string | Base quantity bounds |
| `quote_increment`, `quote_min_size`, `quote_max_size`, `price_increment` | string | Quote/price bounds |
| `base_name`, `quote_name`, `base_display_symbol`, `quote_display_symbol`, `display_name`, `status`, `alias`, `product_venue` | string | Labels/status |
| `alias_to` | array<string> | Aliases |
| `new`, `cancel_only`, `limit_only`, `post_only`, `auction_mode`, `view_only` | boolean | Lifecycle/restrictions |
| `new_at` | string | Launch time |
| `market_cap`, `icon_color`, `icon_url`, `display_name_overwrite`, `about_description` | string | Display/reference metadata |

`watched:boolean` also exists in the generic upstream product schema; this public reference does not interpret it as owner/watchlist state. Generic non-spot `future_product_details`, `equity_product_details` and `fcm_trading_session_details` are outside selected SPOT authority and their nested schemas are not claimed here. Product prices carry no projected observation timestamp and cannot replace the selected book/trade streams.

## Optional Exchange Direct: reference and REST book

Direct connects to `wss://ws-direct.exchange.coinbase.com`; `wss://ws-feed.exchange.coinbase.com` is a distinct public Exchange feed, not the selected Advanced Trade host. The [Direct implementation](../../../adapters/market-squawk-adapter-coinbase/src/direct.rs) signs the market-data subscription and uses the `full` channel. Authentication verification is operational bootstrap and has no financial response family documented here.

Sources: [official Exchange channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels), [official product book](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book), [Direct capture/parser](../../../adapters/market-squawk-adapter-coinbase/src/direct.rs).

| Resource / field | Wire type | Meaning / local presence |
|---|---|---|
| `GET /products/{product_id}` `id` | string | Exact product identity |
| `base_currency`, `quote_currency` | string, parser optional | Currency identities; absent/null is not inferred from product spelling |
| `status` | string | Provider product status |
| `base_increment`, `quote_increment` | decimal string | Base size / quote price increments |
| `trading_disabled`, `cancel_only`, `post_only`, `limit_only`, `auction_mode` | boolean | Provider market restrictions |
| `GET /products/{product_id}/book?level=3` `sequence` | integer | REST snapshot sequence to reconcile with queued full-channel messages |
| `time` | string, RFC3339 | Required by local snapshot metadata parser |
| `bids`, `asks` | array<array> | Level-3 rows: `[price:string, size:string, order_id:string]` |
| `auction_mode` | boolean, optional locally | Auction state |
| `auction` | object, optional locally | Auction details below |

Snapshot auction object: `indicative_open_price`, `indicative_open_size`, `indicative_bid_price`, `indicative_bid_size`, `indicative_ask_price`, `indicative_ask_size` are decimal strings; `auction_status` is string. Prices use quote/base units and quantities base units. Locally auction data must agree with `auction_mode`; active auctions are validated then rejected for ordinary executable-book bootstrap. An inactive or absent auction flag must not carry an auction object. Nullability/requiredness above describes the local parser, not a universal Exchange contract. Book level 2 uses `[price:string,size:string,num_orders:integer]` upstream and is not interchangeable with the selected level-3 bootstrap.

## Optional Exchange Direct: `full` messages

The envelope is one object, with common `type:string`, `product_id:string`, `time:string` (RFC3339) and `sequence:integer` for sequenced book events. Source: [official full-channel messages](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels), [per-type validation](../../../adapters/market-squawk-adapter-coinbase/src/direct.rs). The table is the market-reconstruction projection; optional authenticated decorations are excluded from financial projection.

| `type` | Additional members / wire types | Meaning and conditional presence |
|---|---|---|
| `received` | `order_id:string`, `order_type:string`; optional `price`, `size`, `funds`: decimal strings; `side:string` | Arrival/cursor event; not a resting-book add |
| `open` | `order_id`, `side`: string; `price`, `remaining_size`: decimal strings | Adds a visible resting order |
| `match` | `trade_id:integer`, `maker_order_id`, `taker_order_id`, `side`: string; `price`, `size`: decimal strings | Maker-side fill and public trade |
| `done` | `order_id`, `reason`: string; conditional `side`, `order_type`: string; `price`, `remaining_size`: decimal strings | `reason` filled/canceled; market-order termination lacks resting price/quantity |
| `change` (`reason:modify_order`) | `order_id`, `side`, `reason`: string; `old_price`, `new_price`, `old_size`, `new_size`: decimal strings | Price/size replacement; local parser rejects ambiguous legacy `price` alongside this shape |
| `change` (`reason:STP`) | `order_id`, `side`, `reason`: string; either `price`, `old_size`, `new_size`, or `old_funds`, `new_funds`: decimal strings | Self-trade-prevention quantity/funds change; funds-only is cursor work |

Optional `cancel_reason:string` is only admitted on canceled `done` events. The parser validates exact allowed/required fields and conditional combinations; arbitrary nulls are not substitutes for omitted members. Received orders and funds-only changes advance the cursor without inventing resting liquidity. Visible maker-side fills are market evidence, not the owner's account executions.

Bootstrap queues full messages, obtains REST level 3, discards messages at/before snapshot sequence and applies later messages in sequence. Gaps/unknown sequenced messages invalidate the generation; recovery requires a fresh snapshot. No upstream book checksum is provided. Unsequenced private lifecycle messages cannot enter market continuity or create book changes. Control acknowledgements use `type:"subscriptions"`, `channels:array<{name:string,product_ids:array<string>}>`; heartbeat uses `type:"heartbeat"`, `product_id:string`, `time:string`, `sequence:integer`, `last_trade_id:integer`. These Exchange shapes must not be mixed with Advanced Trade envelopes.

## Coverage and evidence limits

Public financial envelopes, control projection, public product identity, Direct product/book and full-channel reconstruction are covered here. Complete upstream REST product extensions, nonselected candle/ticker/status channels, private user streams and trading APIs are not claimed. No safe live original was verified for examples. Source/schema review is not proof of connectivity, entitlement, continuity, durable publication or product completion. Recheck adapter and official schema after either changes.
