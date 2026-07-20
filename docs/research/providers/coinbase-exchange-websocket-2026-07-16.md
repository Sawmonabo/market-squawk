# Coinbase Exchange WebSocket v1 provider decision

- Original decision date: 2026-07-16
- Primary-source refresh: 2026-07-20
- Selected protocol: Coinbase Exchange WebSocket v1
- Production endpoint: `wss://ws-feed.exchange.coinbase.com`

## Selected scope

Market Squawk uses the public Coinbase Exchange Market Data WebSocket endpoint with the `level2`,
`matches`, and `heartbeat` channels for explicitly configured product mappings. Coverage is one
Coinbase Exchange venue and only the subscribed products and channels; it is not consolidated
market coverage.

The adapter's immutable quality ceiling is `DirectUnverified`. The selected `level2` schema starts
with a full price-level snapshot and then supplies absolute price-level sizes, where zero removes a
level. The documented `level2` payload does not carry the per-update sequence evidence needed by
Market Squawk's execution qualification and Coinbase documents no checksum for this channel.
Coinbase also warns that `matches` messages can be dropped. Consequently, neither the order book
nor trade stream can qualify immediate automated action under this profile. See the official
[Exchange channel schemas](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels).

Heartbeats are connection/feed-health evidence only. Their sequence and last-trade fields do not
repair the missing level2 per-update sequence/checksum evidence, and receiving a heartbeat never
refreshes market-price freshness.

## Connection and access policy

The Exchange overview identifies `wss://ws-feed.exchange.coinbase.com` as the production Coinbase
Market Data endpoint and requires a subscription shortly after connection. It separately identifies
`ws-direct` as Coinbase Direct Market Data, so this public-feed adapter does not claim the stronger
delivery relationship of `ws-direct`. See the official
[Exchange WebSocket overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview).

One registry-coordinated provider budget governs connection/subscription attempts. The source opens
one connection generation and returns terminally to the supervisor on refusal, closure, timeout, or
network error. A reconnect requires a new registry session, generation, raw-frame factory, and
capture authority. The implementation contains no account, identity, endpoint, proxy, TLS
fingerprint, CAPTCHA, or quota rotation. Provider refusals use the shared backoff policy documented
by the platform. Coinbase's published limits are treated as shared constraints, not identities to
shard around; see [Exchange WebSocket rate limits](https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits)
and [Exchange WebSocket errors](https://docs.cdp.coinbase.com/exchange/websocket-feed/errors).

All connect, subscription-write, read, pong-write, and close-response operations are cancellable
and deadline-bounded. Incoming message and frame sizes are capped before provider decoding. Text
and binary data frames enter the raw capture sink before any semantic decoder is invoked.

## Protocol separation

Coinbase Advanced Trade is a distinct protocol family using
`wss://advanced-trade-ws.coinbase.com`, per-channel subscription messages, `sequence_num`, and
different event envelopes. Its schemas are deliberately not accepted by this Exchange v1 adapter.
Any Advanced Trade support must use separate metadata, fixtures, decoder rules, and source
composition. See the official [Advanced Trade WebSocket overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)
and [Advanced Trade channel schemas](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels).

## Frozen qualification facts

```text
endpoint: wss://ws-feed.exchange.coinbase.com
channels: level2, matches, heartbeat
coverage: single venue; configured products/channels only
quality ceiling: DirectUnverified
level2 sequence qualification: unsupported by selected payload contract
checksum qualification: unsupported
trade completeness: not guaranteed; matches may be dropped
heartbeat semantics: connection/feed health, never market-price freshness
```
