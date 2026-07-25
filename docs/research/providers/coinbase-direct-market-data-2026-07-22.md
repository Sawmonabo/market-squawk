# Coinbase Direct Market Data execution-quality candidate

- Initial audit date: 2026-07-22 (America/New_York)
- Implementation-blocker review: 2026-07-24 (America/New_York)
- Provider: Coinbase Exchange
- Candidate endpoint: `wss://ws-direct.exchange.coinbase.com`
- Candidate channel: authenticated `full`
- Intended coverage: one Coinbase Exchange venue and explicitly configured products
- Decision status: integrity core integrated; transport, qualification, and release evidence blocked

## Question

Can a zero-mandatory-fee Coinbase interface satisfy Market Squawk's complete
`DirectVerified` contract and drive strategy, risk, and paper execution without weakening the
quality predicates?

## Conclusion

The existing `ws-feed` `level2`/`matches`/`heartbeat` profile cannot. Its selected order-book
payload has no per-update sequence or checksum, and the separate `matches` channel is not a
complete trade stream. Its immutable `DirectUnverified` ceiling remains correct.

The strongest documented candidate is a distinct authenticated Coinbase Direct Market Data
profile using `ws-direct`, the `full` channel, and a level-3 REST snapshot. Coinbase describes
`ws-direct` as having direct access to Exchange servers. The `full` channel carries product
sequence numbers and documents the required snapshot/replay algorithm: queue stream messages,
obtain a level-3 snapshot, discard queued messages at or below the snapshot sequence, and replay
the rest. Sequence gaps can therefore invalidate the generation and require a new snapshot.

The 2026-07-24 blocker review established that the original frame and decoded-payload contracts
could not safely admit this feed. A live official BTC-USD level-3 snapshot was 6,691,816 bytes and
contained 108,522 order rows; the corresponding level-2 snapshot still contained 44,102 aggregate
price levels. Both exceeded the original capture or decoded-item bounds. Coinbase also documents
authentication for `level2` and `full`, a source `time` on REST book snapshots, and sequence
caveats for authenticated-only messages.

The accepted integrity core is integrated at release merge `4cb6e02`. It adds segmented,
digest-bound HTTP snapshot receipts; bounded level-3 order ownership; snapshot and contiguous
replay state; closed public and private sequence domains; source-time freshness; currentness and
provider-consistency evidence; crossed-book quarantine; and explicit separation between owner-only
TPSL lifecycle messages and public sequenced `modify_order` mutations. The final exact-head review
of candidate `6182da0` reported zero Critical, Important, or Minor findings. Focused order-book,
decoder, snapshot-receipt, allocation-accounting, strict affected-package Clippy, formatting, and
diff gates passed on the merged release tree.

This is not yet a runnable authenticated source. Application-owned credential activation,
WebSocket and REST transport supervision, central qualification, strategy/risk/paper composition,
and an authorized unchanged-head trace remain mandatory release blockers. Until those boundaries
are composed and proven, the canonical source remains execution-ineligible.

Qualification still requires an authorized live run at the exact release candidate proving every
source, sequence, snapshot, timestamp, freshness, status, precision, rights, and coverage
predicate.

## 2026-07-24 implementation audit

Items 1–5 below drove the accepted integrity-core implementation. Items 6–7 and the production
transport, composition, and release-proof requirements remain open.

1. Coinbase has required authentication for Exchange `level2`, `level3`, and `full`
   subscriptions since 2023-08-01. The existing unauthenticated `ws-feed` `level2` fallback is
   stale and must either authenticate or select a documented unauthenticated lower-quality
   channel.
2. Coinbase added `time` to REST level-1, level-2, and level-3 book responses on 2023-03-02. That
   source time initializes snapshot freshness; receive time must not replace it.
3. A level-3 snapshot is an unpaginated full order book. A production synchronizer must stream it
   through bounded capture segments into an explicitly bounded order-ID map and derive only the
   configured top price-level depth for the shared live runtime. Increasing the current single
   frame or 20,000-item limits is not an acceptable substitute.
4. Snapshot loading and queued replay must be distinct non-authoritative phases. Only an atomic
   handoff from a fully drained contiguous replay queue to the same live owner may establish a
   healthy generation.
5. `received` and valid unknown-order `done` or `change` messages can advance the exact sequence
   without publishing a market mutation. Unknown sequenced message types, gaps, regressions,
   order-map inconsistencies, or any count/byte overflow quarantine the generation and force a
   completely fresh snapshot.
6. The onboarding catalog currently describes a credential-free Advanced Trade surface while
   granting a `DirectVerified` ceiling, but the shipping Exchange adapter is a different
   `DirectUnverified` surface. The catalog entry must be corrected and a distinct authenticated
   Exchange Direct profile must bind its endpoint, credential, protocol, rights, and coverage
   evidence.
7. Coinbase's Market Data Terms constrain downstream use, including fair-value-related uses.
   Activation must bind an applicable authorization or enforce a technical data-use boundary that
   prevents Coinbase-derived evidence from entering disallowed valuation, export, redistribution,
   or display paths.

## Provider evidence

| Contract | Official evidence | Market Squawk consequence |
| --- | --- | --- |
| Direct delivery | The Exchange WebSocket overview distinguishes traditional `ws-feed` from authenticated Direct Market Data at `ws-direct`, which has direct access to Exchange servers. | Use a distinct source profile and endpoint capability. Never upgrade the existing public profile in place. |
| Sequence continuity | Coinbase documents increasing product sequence numbers and identifies gaps and out-of-order delivery as conditions consumers must handle. | Require exact successor progression within one connection generation. A duplicate, regression, or gap quarantines that generation. |
| Snapshot/update consistency | The `full` channel documentation requires queueing messages while obtaining a level-3 REST snapshot, then replaying only messages after the snapshot sequence. | No book authority exists before the snapshot and complete contiguous replay are committed together. |
| Order-level semantics | `full` publishes received, open, done, match, change, and activation events. | Maintain order-level state first; derive price-level books and online features from the validated owner. |
| Authentication | Coinbase requires authentication for the full channel and Direct Market Data endpoint. | Credentials come only from the local secret capability established by provider onboarding; adapters do not read ambient files or log secret material. |
| Zero-mandatory-fee bound | Coinbase documents a baseline limit of ten WebSocket subscriptions per product/channel and offers paid tiers only for higher limits. | The core profile stays within the baseline limit. Higher paid capacity is optional and never required by the product. |
| Connection topology | Coinbase recommends spreading high-volume full-channel products over separate connections. | Default to one configured product per bounded connection and coordinate the provider-wide connection/subscription budget centrally. |
| Checksum | The selected full-channel contract documents sequences and snapshot replay but no book checksum. | Checksum is `Unsupported` for this profile and is not fabricated; the product requirement applies checksum validation where the venue supplies one. |
| Failover | Coinbase recommends `ws-feed` as fallback when `ws-direct` is primary. | Fallback is a new source generation with a lower quality ceiling. It cannot inherit execution authority or book state from `ws-direct`. |

## Required implementation contract

1. Add a separate authenticated Direct Market Data profile inside the Coinbase adapter package.
   Keep its source identity and generation separate from every public or fallback surface. Correct
   the existing public profile's stale authentication/channel configuration while retaining its
   `DirectUnverified` ceiling.
2. Admit only the exact Direct WebSocket and Exchange REST snapshot endpoints through the source
   endpoint policy.
3. Accept a redacted, bounded signing capability from application composition. Request read-only
   market-data authority; trading authority is unnecessary for data ingestion.
4. Limit the baseline configuration to the documented free subscription allowance and use one
   product per full-channel connection by default.
5. Capture every WebSocket frame before decoding. Queue a count-and-byte-bounded set of sequenced
   events while a bounded, segmented REST level-3 snapshot capture and streaming parse is
   outstanding. Bind the HTTP method, admitted final URL, status, body length, digest, and every
   segment into one snapshot receipt.
6. Commit the snapshot and only the contiguous suffix after its sequence in one instrument-owned
   transition. Overflow, gap, regression, malformed update, generation change, timeout, or
   incomplete replay invalidates the attempt.
7. Treat unsupported or newly introduced sequenced message types as an integrity break unless the
   pinned protocol revision proves they cannot affect the maintained state. Coinbase permits new
   message types, so silently advancing past an unclassified state-changing event is unsafe.
8. Use the required REST snapshot `time` as the initializing source timestamp. When replay has a
   nonempty contiguous suffix, advance freshness from its last applicable source timestamp.
   Connection receipt time cannot substitute for venue event time.
9. Bind current venue status, product identity, tick/lot precision, explicit product coverage,
   source authorization, and current connection generation into qualification.
10. Publish `DirectVerified` authority only after the central qualifier proves the entire
    conjunction. Every failure remains `DirectUnverified` or `Quarantined` and produces no action.
11. Drive only a configured bounded strategy into the existing non-bypassable risk and paper
    dispatcher. Authentication for data must not authorize live order submission.
12. Prove resynchronization, fallback degradation, shutdown, secret redaction, and zero paper
    mutation before current authority in the existing consolidated source/application harnesses.

## Why Advanced Trade is not the primary candidate

Advanced Trade documents a public `level2` channel, `sequence_num`, and delivery of snapshots and
updates. The reviewed official pages do not state the same exact per-product successor and REST
snapshot-replay contract documented for Exchange `full`. The field's presence alone is not enough
to prove Market Squawk's sequence predicate. Advanced Trade remains a possible future adapter or
fallback only after its sequence scope and recovery semantics are established from current
official evidence and a real authorized session.

## Release-proof requirements

The Task 20 provider evidence command must reject qualification unless one unchanged exact-head
run proves all of the following without credentials in its output:

- authenticated connection to the admitted Direct endpoint;
- exact provider, venue, product, internal instrument, and coverage identities;
- bounded snapshot acquisition and contiguous queued replay;
- duplicate, gap, regression, overflow, malformed-message, and reconnect rejection;
- fresh venue timestamps and current trading status;
- exact tick and lot precision;
- safe degradation when the Direct endpoint is unavailable;
- strategy to risk to one-use paper dispatch under current `DirectVerified` authority; and
- no action after authority expiry, quarantine, generation replacement, or shutdown.

Until that evidence passes, the release remains blocked and no README, CLI, MCP, or report may
describe Coinbase data as execution-qualified.

## Primary sources

Retrieved 2026-07-22:

- [Coinbase Exchange WebSocket overview](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)
- [Coinbase Exchange WebSocket channels](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)
- [Coinbase Exchange WebSocket authentication](https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication)
- [Coinbase Exchange WebSocket best practices](https://docs.cdp.coinbase.com/exchange/websocket-feed/best-practices)
- [Coinbase Exchange systems and endpoints](https://docs.cdp.coinbase.com/exchange/introduction/systems-operations)
- [Coinbase Exchange market-data connection limits](https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections)
- [Coinbase Exchange API-key creation](https://help.coinbase.com/en/exchange/managing-my-account/how-to-create-an-api-key)
- [Coinbase Advanced Trade WebSocket channels](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels)

Reviewed 2026-07-24:

- [Coinbase Exchange changelog](https://docs.cdp.coinbase.com/exchange/changes/changelog)
- [Get product book](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-book)
- [Get single product](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-single-product)
- [Coinbase Exchange REST authentication](https://docs.cdp.coinbase.com/exchange/rest-api/authentication)
- [Coinbase Exchange TPSL order messages](https://docs.cdp.coinbase.com/exchange/fix-api/order-entry-messages/tpsl-orders)
- [Coinbase Exchange WebSocket rate limits](https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits)
- [Coinbase Market Data Terms](https://www.coinbase.com/legal/market_data)
- [Live official BTC-USD level-3 endpoint used for the boundedness observation](https://api.exchange.coinbase.com/products/BTC-USD/book?level=3)
