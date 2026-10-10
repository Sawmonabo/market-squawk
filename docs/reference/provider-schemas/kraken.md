# Kraken Spot WebSocket v2 schemas

Reviewed **2026-10-10** against current working-tree source and official contracts. The selected public lane is venue-specific spot crypto `book` and `trade` at `wss://ws.kraken.com/v2`, with a separate `instrument` reference snapshot. Authenticated `level3` is a distinct retained optional implementation, not an implicit upgrade of public depth. No live connections, account data or credentials were accessed.

Sources: [selected architecture](../../architecture/market-data-provider-architecture.md), [public wire types](../../../adapters/market-squawk-adapter-kraken/src/messages.rs), [reference projection](../../../adapters/market-squawk-adapter-kraken/src/reference.rs), [public decoder](../../../adapters/market-squawk-adapter-kraken/src/decoder.rs), [configuration](../../../adapters/market-squawk-adapter-kraken/src/config.rs).

## Subscription and acknowledgement

Sources: [official book subscription](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book), [official trade subscription](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade).

| Request member | Wire type | Meaning / selected behavior |
|---|---|---|
| `method` | string | `subscribe` |
| `params` | object | Channel parameters |
| `params.channel` | string | `book` or `trade` |
| `params.symbol` | array<string> | Exact pair identifiers |
| `params.depth` | integer | Book only: 10, 25, 100, 500 or 1000 levels/side |
| `params.snapshot` | boolean | Public live book true; public live trade false |
| `req_id` | integer, optional upstream | Current public sender uses 1 |

Trade snapshots otherwise contain the last 50 trades. Live `snapshot:false` prevents newly observed pair reference from being backdated to authorize old trades. Neither book nor trade uses page cursors.

| Acknowledgement member | Wire type | Meaning / conditional presence |
|---|---|---|
| `method` | string | `subscribe` |
| `success` | boolean | Provider processing result |
| `result` | object | Successful subscription details; absent/null locally allowed on failure |
| `result.channel`, `result.symbol` | string | Exact channel and pair |
| `result.depth` | integer | Book depth; absent for trade |
| `result.snapshot` | boolean | Snapshot selection |
| `result.warnings` | array<string> | Optional advisory list; no financial authority |
| `error` | string | Failure description; optional/nullable locally |
| `req_id` | integer | Echoed request identifier when supplied |
| `time_in`, `time_out` | string, RFC3339 | Wire receive/send times |

There is one acknowledgement per pair. Local parser matches request/channel/pair/depth/snapshot and validates clocks; successful acknowledgement does not supply market rows. Unknown acknowledgement fields are rejected. Public `warnings` may be omitted, but a present non-array/null warning value is rejected by its bounded list parser.

## Public book envelope and level object

Source: [official book schema](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book), [local messages](../../../adapters/market-squawk-adapter-kraken/src/messages.rs).

| Envelope / data member | Wire type | Meaning / presence |
|---|---|---|
| `channel` | string | `book` |
| `type` | string | `snapshot` or `update` |
| `data` | array<BookData> | Current parser requires exactly one pair row |
| `data[].symbol` | string | Pair identity |
| `data[].bids`, `data[].asks` | array<Level> | Price levels; updates can omit an unchanged side locally |
| `data[].checksum` | integer | Unsigned CRC32 over top ten levels/side |
| `data[].timestamp` | string, RFC3339 | Snapshot/update engine time |

| Level member | Official wire type | Meaning / local allowance |
|---|---|---|
| `price` | number (float in provider documentation) | Quote currency per base unit; local exact-token parser also accepts unescaped decimal string |
| `qty` | number (float in provider documentation) | Absolute base quantity; local exact-token parser also accepts unescaped decimal string |

Both levels' members are required and null is rejected. Local financial envelopes and nested records reject unknown fields. Missing side arrays default empty; null side arrays do not. Snapshot initializes state; update replaces a level's quantity, zero deletes it. Apply repeated updates at a price in message order, then truncate to requested depth; out-of-depth levels need not receive explicit deletion. No provider sequence member exists on the public book envelope.

## Checksum and continuity

Source: [official v2 checksum algorithm](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2), [book decoder](../../../adapters/market-squawk-adapter-kraken/src/decoder.rs).

After all updates, concatenate top-ten asks ascending then bids descending, each price followed by quantity after removing decimal points and leading zeros; compute unsigned CRC32. Preserve provider decimal precision when decoding. This checksum covers top-ten price levels regardless of subscription depth. The local decoder validates snapshot and every accepted update, commits only the valid candidate and quarantines a failing generation until a fresh snapshot. Timestamp ordering and transport order remain distinct from a provider sequence number. No synthetic sequence or checksum may be inserted into the upstream schema.

## Public trade object

Source: [official trade schema](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade), [trade wire record](../../../adapters/market-squawk-adapter-kraken/src/messages.rs).

Envelope: `channel:"trade"`, `type:"snapshot"|"update"`, `data:array<Trade>`.

| Trade member | Wire type | Meaning |
|---|---|---|
| `symbol` | string | Source pair |
| `side` | string: `buy` or `sell` | Taker/aggressor side |
| `price` | number | Average trade price, quote/base units |
| `qty` | number | Base units traded |
| `ord_type` | string: `limit` or `market` | Taker order type |
| `trade_id` | integer | Unique per-book sequential trade identifier |
| `timestamp` | string, RFC3339 | Original event time |

Local decimals also permit unescaped strings, require positive values and retain exact numeric tokens. All members are required locally; null and unknown members are rejected. Trade IDs are parsed as signed `i64` then semantically validated; this is not a book-update sequence. Trade has no CRC32 or book snapshot. Snapshot history must retain identity valid at each original event time; current reference cannot supply historical validity by assumption.

## Heartbeat, status and pong

Sources: [official heartbeat](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat), [official status](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/status), [local controls](../../../adapters/market-squawk-adapter-kraken/src/messages.rs).

| Message / field | Wire type | Meaning |
|---|---|---|
| Heartbeat `channel` | string: `heartbeat` | Idle connection liveness; no counter/time/book payload |
| Status `channel`, `type` | string: `status`, `update` | Connection/system status |
| Status `data` | array<Status> | Current local parser requires one row |
| Status row `system` | string | `online`, `maintenance`, `cancel_only` or `post_only` |
| Status row `api_version`, `version` | string | API/engine version identifiers |
| Status row `connection_id` | integer | Provider connection identity |
| Pong `method` | string: `pong` | Response to application ping |
| Pong `req_id` | integer, optional | Request echo |
| Pong `time_in`, `time_out` | string, RFC3339 | Wire receive/send times |

Heartbeat has no application sequence. Status row advisories are deliberately ignored by the local projection; outer status envelope remains strict. Current official status includes optional paired `upcoming_maintenance` and `emergency` arrays with further nested schedules/incidents. Their full schema is not the local operational projection documented here. Advisory text never creates financial authority or order access.

## Instrument reference snapshot

Request `method:"subscribe"`, `params:{channel:"instrument",snapshot:true}` on public v2. Body: `channel:"instrument"`, `type:"snapshot"|"update"`, `data:{assets:array<Asset>,pairs:array<Pair>}`. The selected [reference parser](../../../adapters/market-squawk-adapter-kraken/src/reference.rs) admits only snapshots and one exact unique `online` pair with nonempty base/quote. Other fields remain in the original rather than being validated by that projection. Source: [official instrument schema](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/instrument).

| Object member | Wire type | Meaning / current projection |
|---|---|---|
| Pair `symbol`, `base`, `quote`, `status` | string | Pair/base/quote identity and status; projected |
| Pair `price_increment`, `qty_increment`, `qty_min`, `cost_min` | number | Price/quantity increments; minimum base quantity and quote cost; original only |
| Pair `price_precision`, `qty_precision`, `cost_precision`, `ws_display_price_precision` | integer | Decimal precision/display recommendations; original only |
| Pair `has_index`, `marginable` | boolean | Pair reference flags; original only |
| Pair `margin_initial` | number, conditional | Marginable-pair reference percentage; original only |
| Pair `position_limit_long`, `position_limit_short` | integer, conditional | Marginable-pair reference limits; original only |
| Pair `tick_size` | number, deprecated | Use `price_increment`; original only |
| Asset `id`, `class`, `status` | string | Asset identity, classification/status; original only |
| Asset `precision`, `precision_display` | integer | Ledger/display precision; original only |
| Asset `borrowable` | boolean | Reference flag; original only |
| Asset `collateral_value`, `margin_rate`, `multiplier` | number | Provider asset reference factors/rate/token conversion; original only |

Official pair status values include `online`, `cancel_only`, `delisted`, `limit_only`, `maintenance`, `post_only`, `reduce_only`, `work_in_progress`. Asset states include `depositonly`, `disabled`, `enabled`, `fundingtemporarilydisabled`, `withdrawalonly`, `workinprogress`. Reference margin fields do not authorize account/margin/trading use. Omission versus null of unused reference fields is not constrained by the selected four-field parser. Current official request also has venue/tokenized-asset switches; the selected spot snapshot request does not opt into those surfaces.

## Separate authenticated level 3

The retained [L3 configuration](../../../adapters/market-squawk-adapter-kraken/src/level3/config.rs) uses `wss://ws-l3.kraken.com/v2`, not the public v2 endpoint. Subscription adds `params.channel:"level3"`, `params.symbol:array<string>`, `params.depth:integer` (10/100/1000), `params.snapshot:boolean` and secret `params.token:string`; `req_id` is integer. This is public visible-order market data through an authenticated transport, not personal orders. Source: [official level3](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/level3), [L3 wire shapes](../../../adapters/market-squawk-adapter-kraken/src/level3/messages.rs).

| Envelope / nested member | Wire type | Meaning |
|---|---|---|
| `channel`, `type` | string | `level3`; `snapshot`/`update` |
| `data` | array<object> | Pair book messages |
| `data[].symbol` | string | Exact pair |
| `data[].timestamp` | string, RFC3339 | Matching-engine message time |
| `data[].checksum` | integer | CRC32 of top ten price levels on each side |
| `data[].bids`, `data[].asks` | array<Order> | Snapshot resting orders or ordered update events |
| Order `order_id` | string | Visible provider order identity |
| Order `limit_price` | number | Quote/base price; local exact-decimal strings also allowed |
| Order `order_qty` | number | Remaining visible base quantity; local strings also allowed |
| Order `timestamp` | string, RFC3339 | Order insertion/amendment time |
| Update Order `event` | string: `add`, `modify`, `delete` | Absent on snapshot rows |

Snapshot side arrays are required locally; update sides may be omitted. L3 update order records locally require all five fields, including delete records. Null numeric/order fields are not accepted. Apply in transport order, truncate by subscribed price depth, preserve order priority and verify the separate order-level checksum; never substitute L2's price/quantity checksum. The provider does not supply an L3 message sequence. Hidden iceberg quantity, in-flight orders, unmatched market orders and untriggered conditional orders are excluded upstream.

The [official L3 checksum algorithm](https://docs.kraken.com/exchange/guides/websockets/l3-checksum-v2) processes every order in each of the ten best price levels: asks ascending, bids descending, preserving each level's queue order. For each order append `limit_price` then `order_qty`, stripping decimal points and leading zeros; concatenate asks then bids and compute unsigned CRC32. Order IDs/timestamps are not checksum input, while order ordering is. Aggregate-level quantities would lose this queue-priority check. The guide permits periodic verification upstream; that allowance does not change the retained decoder's own admission rule.

## Evidence limits

The tables cover selected public market schemas, operational controls, source reference projection and the distinct retained L3 schema. No account/token-response or trading API contract is claimed. Full unused instrument/status extensions are linked, not represented as validated projections. No live payload example, subscription success, durable publication or product completion was verified by this schema review. Recheck source and official specification after either changes.
