# Cboe option-reference contract

Cboe is a selected no-key source for venue-specific option-series files and symbology mappings. It
supplies reference identity, not consolidated option quotes, OPRA, or a universal option universe.

| Field | Value |
| --- | --- |
| Document type | Selected-provider target and evidence contract |
| Audience | Operators, financial-data engineers, quantitative researchers, application integrators, and reviewers |
| Admission | Core option-reference target |
| Evidence cutoff | 2026-08-11, America/New_York |
| Audit basis | `3a2f24ddbe88a886d9ba6458dd141774e3716a9d` plus the preserved working-tree overlay |
| Current repository status | Selected and configured in the target template; adapter, canonical publication, doctor, and workflow composition are absent |

## Role and product workflows

| Data family | Product use | Boundary |
| --- | --- | --- |
| Venue `All Series` files | Option contract/series discovery and venue-presence evidence | Separate C1, BZX, C2, and EDGX observations; not one consolidated chain |
| Market-maker-registered and underlying reference families | Provider-native root/underlying/venue mapping where selected | Family schema must be frozen independently before admission |
| Cboe Symbol ID, OSI, and suffix mappings | Canonical option/equity alias resolution | A parsed string alone does not create identity |
| Specialist reference families | Targeted option research when explicitly admitted | Not included automatically by enabling Cboe |

**VERIFIED PROVIDER FACT:** Cboe publishes separate `All Series`, `Market Maker Registered`, and
`Underlying` files for C1, BZX Options, C2 Options, and EDGX Options. Additional reference families
exist, but the files remain venue/family specific. See the
[U.S. Options Reference Data page](https://www.cboe.com/markets/us/options/market-statistics/reference-data).

**VERIFIED PROVIDER FACT:** Cboe Options uses a six-character base-62 Cboe Symbol ID mapped to OSI
across the four exchanges. Simple-option reference is available through the website or feeds;
intraday-created complex symbols are feed-only. Options-on-futures names should be sourced from
reference/mapping messages rather than constructed. See the
[Cboe Titanium symbology specification](https://www.cboe.com/document/tech-spec/document/technical-specifications/cboe-titanium-u.s.-equitiesoptionsfutures-symbology-reference).

## Authentication and setup

No credential field is selected. The target input contains only:

```text
CBOE_OPTIONS_REFERENCE_ENABLED=true
```

**APPLICATION POLICY:** enablement requests the bounded no-key reference doctor and selected
families. It does not enable proprietary feeds, assume a CSV schema, or mark Options Available.

## Exact selected surfaces

| Venue | Selected `All Series` locator |
| --- | --- |
| C1 | `https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/cone-all-series.csv` |
| BZX Options | `https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/opt-all-series.csv` |
| C2 Options | `https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/ctwo-all-series.csv` |
| EDGX Options | `https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/exo-all-series.csv` |

The official discovery and contract pages are:

- `https://www.cboe.com/markets/us/options/market-statistics/reference-data`;
- the [2024 URL migration notice](https://cdn.cboe.com/resources/release_notes/2024/Cboe-Updates-Options-Symbol-Reference-and-Equities-Symbols-Traded-File-URLs.pdf); and
- the [Cboe Titanium symbology reference](https://www.cboe.com/document/tech-spec/document/technical-specifications/cboe-titanium-u.s.-equitiesoptionsfutures-symbology-reference).

**VERIFIED PROVIDER FACT:** the migration notice moved legacy query-style references to CDN URLs,
warned that clients unable to follow redirects could fail, and advised using current locators.

## Provenance, clocks, and quality

Each exchange/family file remains a distinct exact object. Retain:

- exchange, reference family, configured and final HTTPS locator, redirect chain, response status,
  media type, bytes, digest, and local first-observed/receive/publish times;
- Cboe Symbol ID, OSI symbol, CQS/SIAC and Exchange/CMS forms where supplied, underlying/root,
  venue, product environment, and every source-native alias;
- the exact layout/schema identity, rejected/unknown rows, row count, completion state, and source
  generation; and
- effective/reference time when the admitted family supplies it, without inventing an exchange
  event time.

The same OSI contract may appear in several venue files; that is venue evidence, not a duplicate to
silently erase. A redirect changes transport lineage and must be recorded without relaxing scheme,
host, byte, deadline, or content-type bounds.

## Official limits, application admission, and scheduling

**UNVERIFIED ENTITLEMENT/ASSUMPTION:** the reviewed first-party pages publish no numeric automated
request ceiling, exact file cadence, complete universal CSV schema, correction/backfill policy,
retention guarantee, rate headers, or per-file availability target.

**APPLICATION POLICY:** retrieve each explicitly admitted venue/family through one shared Cboe
reference queue at most once per applicable publication, pre-admit response and parser bytes, reuse
identical objects by digest, and publish a new canonical generation only after every selected file
is complete. Never poll these files as chains or quotes.

**APPLICATION POLICY:** initial implementation admits the four `All Series` files only. Other
families require their own frozen schema, bounds, product need, and acceptance receipt rather than
sharing a generic Cboe parser.

When a family is unavailable or changed, retain the last manifest for historical PIT queries, mark
current reference coverage stale/partial, and keep unrelated market-price workflows independent.

## Runtime evidence

**RUNTIME-MEASURED VALUE:** bounded acquisitions on 2026-08-11 observed:

| Venue/file | Bytes | Rows |
| --- | ---: | ---: |
| C1 | about `82.96 MiB` | `1,880,416` |
| BZX Options | about `77.31 MiB` | `1,752,372` |
| C2 Options | about `76.53 MiB` | `1,734,778` |
| EDGX Options | about `77.20 MiB` | `1,749,918` |

These dated values establish that Cboe reference ingestion is a large-file, bounded batch workload.
They are not future file maxima, cadence, completeness, capacity, or historical-retention
guarantees.

## Canonical storage and point-in-time destination

```text
venue/family CSV bytes
    -> provider-native Cboe reference rows
    -> market_squawk.instrument_lifecycle
    -> immutable venue-aware manifest
    -> PIT option identity / Options discovery
```

The lifecycle row binds option and underlying `InstrumentId`, Cboe Symbol ID, OSI and provider
aliases, venue, expiry, exact strike, side, multiplier where established, validity/reference
interval, source family/object, schema, and revision. The raw large files are retained by digest;
Arrow validates typed conversion; immutable Parquet generations and manifests publish canonical
rows. A current Cboe snapshot cannot be projected backward as historical listing evidence.

## Repository integration status and seams

Repository inspection found no Cboe adapter, source profile, doctor, raw-object job, large-file
parser, lifecycle publisher, or typed Options read. The future template flag is not consumed while
the thin credential/no-key importer remains unimplemented.

The implementation must reuse:

- endpoint, response-byte, request-budget, cancellation, and raw-capture authorities in
  [`market-squawk-sources`](../../../crates/market-squawk-sources/src/lib.rs);
- provider identity mappings in
  [`provider_identities`](../../../crates/market-squawk-domain/src/instrument/provider_identities.rs);
- immutable Arrow/Parquet publication and PIT selection in the
  [research data plane](../../architecture/research-data-plane.md); and
- the target lifecycle/option schema registry defined by the
  [provider architecture](../../architecture/market-data-provider-architecture.md).

## Doctor and end-to-end acceptance gates

Cboe becomes Available for an exact Options workflow only after:

1. Each selected venue/family locator and current field layout is retrieved, frozen, and bound to
   a separate code-owned parser contract.
2. The doctor validates bounded redirects, approved hosts, status, content type, declared and
   received bytes, digest, row/control structure, latency, and changed/unchanged state.
3. Storage and parser admission cover the complete largest selected response without unbounded
   allocation or a partial-success publication.
4. Cross-file resolution preserves venue multiplicity, base-62-to-OSI mappings, unresolved aliases,
   and requested-versus-returned family closure.
5. Raw objects and canonical lifecycle rows publish atomically under an immutable manifest.
6. The PIT selector and typed Options identity read expose exact, ambiguous, missing, stale, and
   partial states.
7. Options discovery consumes that read and restart reproduces the same manifest-bound result.

## Hard gaps

- Complete CSV layouts, exact publication cadence, correction/backfill behavior, retention,
  checksums, and numeric request capacity are not established by the reviewed pages.
- Complex symbols created intraday are feed-only, so the selected downloadable files cannot claim
  the complete intraday complex-product universe.
- Cboe files are venue-specific reference evidence, not consolidated OPRA quotes, Greeks, open
  interest history, or a universal option chain.
- No current adapter or durable forward archive exists in the repository.
- Current snapshots do not close comprehensive expired-option history.

## First-party sources

- [Cboe U.S. Options Reference Data](https://www.cboe.com/markets/us/options/market-statistics/reference-data)
- [Cboe reference-file URL migration notice](https://cdn.cboe.com/resources/release_notes/2024/Cboe-Updates-Options-Symbol-Reference-and-Equities-Symbols-Traded-File-URLs.pdf)
- [Cboe Titanium U.S. Equities/Options/Futures Symbology Reference](https://www.cboe.com/document/tech-spec/document/technical-specifications/cboe-titanium-u.s.-equitiesoptionsfutures-symbology-reference)

Related Market Squawk authorities: [provider architecture](../../architecture/market-data-provider-architecture.md),
[canonical schema and evidence contract](../market-data-canonical-schemas.md),
[shipping source coverage](../source-coverage.md), and the
[provider credential template](../market-squawk-provider-credentials.env.example).
