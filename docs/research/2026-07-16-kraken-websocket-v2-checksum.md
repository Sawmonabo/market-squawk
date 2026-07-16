# Kraken WebSocket v2 book-checksum contract

**Researched:** 2026-07-16

**Primary source:** [Kraken Developers — Book checksum (WebSocket v2)](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)

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

## Scope note

Task 7 owns the closed canonicalizer, transactional book primitive, and evidence contracts. The
production Kraken transport/decoder and recorded-provider integration fixtures remain Stage 2 work,
where this primary-source contract must be revalidated against the live channel schema and provider
changelog before release qualification.
