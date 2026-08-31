# Kraken Spot WebSocket v2 public book-and-trade contract

**Researched:** 2026-07-16; refreshed 2026-08-14

**Primary sources:**

- [Kraken Developers — Spot WebSocket v2 Book](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)
- [Kraken Developers — Spot WebSocket v2 Trades](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade)
- [Kraken Developers — Book checksum guide](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)

## Confirmed provider contract

Kraken's Spot WebSocket v2 `book` channel supplies a CRC32 checksum with each update. The provider
defines the checksum over the best ten ask levels followed by the best ten bid levels, independent
of the subscribed book depth. Asks are ordered from lowest to highest price; bids are ordered from
highest to lowest.

For each selected level, preserve the provider's decimal precision, remove the decimal point and
then leading zeros from the price and quantity text, append price then quantity, concatenate asks
before bids, and compute an unsigned CRC32 over the resulting bytes. The published example result
is `3310070434`; retain it as a golden fixture rather than inventing a local-only vector.

Kraken also requires a complete message to be applied before checksum calculation. Quantity zero
deletes the price level. After applying the message, the local book is truncated to subscribed
depth; the provider does not promise a later zero-quantity deletion for a level that merely falls
outside that retained depth.

The public Spot WebSocket v2 `trade` channel is a separate subscription contract. It accepts an
exact symbol list and optional snapshot flag, acknowledges the `trade` channel and each symbol,
and emits snapshot or update batches. Each trade retains symbol, taker side, quantity, price,
order type, provider timestamp, and a provider trade identifier that Kraken documents as a
sequence number unique per book. That trade identifier does not sequence or repair L2 book
updates, and the trade channel supplies no CRC32 book-integrity claim.

## Market Squawk implementation consequences

- Parse and retain price/quantity lexemes without a binary floating-point round trip.
- Apply every update in one provider message to a candidate state transactionally.
- Calculate the checksum from the committed candidate's exact provider representations, not from
  scaled integer display formatting reconstructed after normalization.
- Limit checksum ordering and canonicalization work to the exact provider top-ten scope. Tail
  levels are outside the checksum input and must neither change the result nor create unbounded
  checksum work.
- Require configured retained depth of at least ten for this validator.
- Treat an unknown algorithm/canonicalization/scope revision as unsupported; never substitute a
  best-effort checksum.
- On mismatch, quarantine the current stream generation and require a new generation plus fresh
  snapshot before execution qualification.
- Keep checksum success separate from source authorization, coverage, sequence, freshness,
  precision, trading status, and capture health; it is necessary evidence, not sufficient
  `DirectVerified` authority.
- Configure `book` and `trade` as an exact required pair, but retain separate source metadata,
  channel identities, subscription acknowledgements, capture journals, health, and integrity
  semantics. Never copy book-checksum authority onto trades.
- Require both channel generations to be current before the composite Kraken runtime is healthy.
  A resynchronizing channel withdraws composite currentness without destroying a still-healthy
  sibling; publication and reads resume only after the exact replacement generation is current.
- Preserve the trade identifier as provider-scoped evidence and a stable trade identity. Never use
  it as an L2 sequence, completeness proof, cross-pair identifier, or execution authorization.

## Scope note

The closed canonicalizer and transactional book primitive implement the checksum contract. The
production runtime uses independent public book and trade supervisors under one atomic owner, and
the existing critical vertical must revalidate both channel schemas, acknowledgements, native
state, resynchronization, bounded shutdown, and restart before release qualification.
