# Nasdaq Trader reference contract

Nasdaq Trader is a selected no-key source for current U.S. listing and symbol-reference evidence.
It contributes identity and status context; it is not a quote, trade, order-book, or execution
source.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Admission | Core reference source |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Current repository status | Two-file listed-security extraction and process-local Markets reference search exist; durable lifecycle publication and the option/bond families remain incomplete |

## Role and product workflows

| Data family | Product use | Boundary |
| --- | --- | --- |
| Nasdaq-listed and other-listed securities | Markets search, canonical listing candidates, exchange/round-lot/status context, and provider-symbol resolution | Current reference state only; not historical membership or a market-price feed |
| ETF and fund indicators carried by the selected directories | Asset-kind qualification and search | A directory flag is not complete fund/share-class identity |
| Nasdaq option directory | Root, side, expiry, strike, underlying, and pending/current reference for Options discovery | Target only; the current adapter does not ingest it |
| Nasdaq bond directory | Bond symbol/reference discovery | Target only; an admitted current HTTPS contract and typed bond identity remain open |

**VERIFIED PROVIDER FACT:** Nasdaq publishes separate reference directories rather than one
universal security master. `nasdaqlisted.txt` includes symbol, name, market category, test-issue
state, financial status, round-lot value, ETF state, and a terminal file-creation timestamp.
Other-listed symbols can be longer and use a different schema. The option directory includes root,
put/call, expiration, strike, underlying, issue name, and pending/current state. See the
[Symbol Directory Definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs).

## Authentication and setup

No key or account field is required. The target credential input contains only:

```text
NASDAQ_TRADER_REFERENCE_ENABLED=true
```

**APPLICATION POLICY:** the flag requests the no-key profile and doctor; it does not turn every
Nasdaq directory on, prove freshness, or make a workflow available. Endpoints, parser schemas,
budgets, and file families remain code-owned.

## Exact surfaces and admitted data families

| Surface | Exact reviewed locator | Admission |
| --- | --- | --- |
| Nasdaq-listed securities | `https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt` | Implemented by the current adapter |
| Other-listed securities | `https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt` | Implemented by the current adapter |
| Nasdaq option series | `https://www.nasdaqtrader.com/dynamic/SymDir/options.txt` | Selected target; mapper/publication absent |
| Listed bonds | `ftp://ftp.nasdaqtrader.com/symboldirectory/bondslist.txt` | **UNVERIFIED ENTITLEMENT/ASSUMPTION:** freeze a current supported HTTPS retrieval contract before implementation; the current source client is HTTPS-only |
| Human/current-day discovery | `https://nasdaqtrader.com/trader.aspx?id=symbollookup` | Documentation and manual discovery only; not a scheduled data endpoint |

**VERIFIED PROVIDER FACT:** directory files update “periodically throughout each day,” while the
interactive lookup describes current-trading-day information and states that its MPID population
is incomplete. No exact refresh interval is published. See the
[definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs) and
[Symbol Lookup](https://nasdaqtrader.com/trader.aspx?id=symbollookup).

## Provenance, clocks, and quality

Each acquired directory remains its own provider-native object. Retain:

- directory family, configured URL, final URL, exact bytes, media type, byte count, and SHA-256;
- source `Last-Modified` when supplied, provider file-creation timestamp when defined, local
  first-observed time, receive time, and publication time;
- provider symbol, listing venue/exchange code, status, test/pending flags, round lot, ETF/fund
  flags, root/underlying relationship, and every provider alias;
- parser/schema identity, rejected or unknown rows, requested-versus-returned family closure, and
  the immutable generation that consumed the object.

The file-creation timestamp is reference-file timeliness evidence, not an exchange event time.
Rows from separate directories are not one atomic snapshot. A symbol string or format-valid option
identifier does not mint canonical identity without source and listing evidence.

## Official limits, application admission, and scheduling

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the reviewed first-party pages publish no numeric automated
request ceiling, exact polling interval, pagination contract, stable conditional-request behavior,
retry-header contract, historical-retention policy, or point-in-time archive.

**APPLICATION POLICY:** the current two-file implementation admits at most `8` requests per minute,
`1` concurrent request, `8 MiB` per file, and `32,768` records per file. These are Market Squawk
bounds, not provider limits.

**APPLICATION POLICY:** retrieve each admitted family through one shared reference queue, validate
the complete family-specific schema and terminal control row, reuse identical bytes by digest, and
publish at most one new generation for one provider publication. Do not poll these files as quotes.
If current retrieval fails, historical manifests remain queryable at their original cutoffs, but a
stale generation cannot be presented as current.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** on 2026-08-11, a bounded read returned `5,587` data rows and about
`72 KiB` for `nasdaqlisted.txt`, and `7,541` data rows and about `148 KiB` for
`otherlisted.txt`. Those observations prove the selected files fit the current parser bounds for
that retrieval; they are not future row-count, byte-size, cadence, or availability guarantees.

No runtime receipt in the frozen research package proves `options.txt`, the bond file, a complete
historical lifecycle, or a durable restart-safe publication.

## Canonical storage and point-in-time destination

```text
exact directory bytes
    -> family-specific provider-native rows
    -> market_squawk.instrument_lifecycle
    -> immutable manifest + PIT listing/provider-symbol selector
    -> Markets search and Options identity reads
```

The target `market_squawk.instrument_lifecycle` rows retain stable instrument/security/listing IDs,
provider symbol and typed external identifiers, venue, asset kind, name, currency, validity
interval, status, predecessor/successor event, source snapshot, and revision. Snapshots append; a
new file never overwrites an earlier identity state. Because Nasdaq exposes current reference data,
forward collection cannot reconstruct the complete pre-collection lifecycle.

## Repository integration status and seams

Repository evidence at the audit basis shows:

- [`market-squawk-adapter-nasdaq-symbols`](../../../adapters/market-squawk-adapter-nasdaq-symbols/src/lib.rs)
  implements bounded HTTPS acquisition, exact two-file parsing, provider fields, health, and
  extraction for `nasdaqlisted.txt` and `otherlisted.txt`;
- the parser validates exact headers, terminal file-creation rows, duplicate symbols, file bytes,
  and record counts in
  [`parser.rs`](../../../adapters/market-squawk-adapter-nasdaq-symbols/src/parser.rs);
- [`NasdaqReferenceUniverseService`](../../../apps/market-squawk/src/provider_activation/nasdaq_reference.rs)
  composes a process-local snapshot into bounded Markets reference search and reacquires it after
  restart; and
- [`market_provider_configuration.rs`](../../../apps/market-squawk/src/local_product/market_provider_configuration.rs)
  uses selected listing evidence when constructing governed market-data instrument bindings.

The current path is intentionally narrower than this target contract. It does not consume the
future credential-file flag, persist an immutable `instrument_lifecycle` generation, provide
historical PIT listing state, or ingest option/bond directories. Those are implementation gaps,
not hidden provider capabilities.

## Doctor and end-to-end acceptance gates

Nasdaq becomes Available for an exact workflow only after all applicable gates pass:

1. The no-key profile admits only code-owned HTTPS locators and a shared provider budget.
2. Every requested family returns bounded nonempty bytes with the expected content type, exact
   family header/layout, terminal control row where defined, valid file/source clocks, and no
   unexplained duplicate or trailing records.
3. The doctor records requested and returned families, rows, bytes, digests, latency, response
   status, retry evidence, and the absence or presence of cache validators without exposing a raw
   body to the frontend.
4. Raw objects and canonical lifecycle rows publish atomically under one complete manifest.
5. The typed resolver returns exact, ambiguous, and missing states rather than silently choosing a
   ticker match.
6. Markets search consumes the typed read and shows source time/status; Options uses only admitted
   contract identities.
7. Restart reopens the same manifest and PIT result, and a failed refresh yields stale/degraded or
   unavailable state without corrupting the prior generation.

## Hard gaps

- No complete historical security lifecycle, index membership/weights, or delisting archive is
  established by the selected pages.
- Exact directory publication intervals, automated-request capacity, correction/backfill rules,
  and retention are unpublished.
- The interactive MPID population is explicitly incomplete.
- The current adapter covers only the two equity/ETF listing files; option, bond, fund-network, and
  participant families need separately frozen schemas and bounded implementations.
- Current directory evidence is not a quote, tradability, halt, book, or execution-eligibility
  guarantee.

## First-party sources

- [Nasdaq Trader Symbol Directory Definitions](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)
- [Nasdaq Trader Symbol Lookup](https://nasdaqtrader.com/trader.aspx?id=symbollookup)
- [Nasdaq-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt)
- [Other-listed directory](https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt)
- [Nasdaq option directory](https://www.nasdaqtrader.com/dynamic/SymDir/options.txt)

Related Market Squawk authorities: [provider architecture](../../architecture/market-data-provider-architecture.md),
[canonical schema and evidence contract](../market-data-canonical-schemas.md),
[shipping source coverage](../source-coverage.md), and the
[provider credential template](../market-squawk-provider-credentials.env.example).
