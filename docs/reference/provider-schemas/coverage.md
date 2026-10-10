# Audited response projections and missing evidence

These documents cover application-consumed or parsed API surfaces. They are response projections grounded in request/decoder source and selected primary documentation, not a claim to exhaust every upstream API. Shared objects describe repeated mounts once. Internal backend outputs remain in a separate appendix.

| Reference | Object families retained in this lane | Schema boundary |
|---|---|---|
| [Alpaca](alpaca.md) | Stock/option snapshots; stock/option quote/trade variants; bars; contracts/deliverables; asset identity; 16 action categories; stock stream variants | Observed anonymous stock/indicative-option primitives; reference/actions mostly decoder evidence |
| [Schwab REST](schwab.md) | Quote blocks/reference/fundamental; instruments; calls/puts maps and contracts; expirations; candles; hours; movers | Native scalar dictionary separated from stricter semantic decoder requirements |
| [Schwab Streamer](schwab-streamer.md) | All five audited LEVELONE dictionaries; equity/futures charts; three book services with levels/participants; both screeners; control envelopes | Primary retained field dictionaries; sparse updates differ from snapshots; chart-code discrepancy explicit |
| [Yahoo](yahoo.md) | Quote and underlying quote; chart metadata/aligned arrays/actions; company/fund modules and dynamic allocation/metric maps; option chains/contracts; search/lookup | Experimental parser shapes only; shared action union is not provider family membership; unsupported summary candidates remain unknown |
| [SEC company](sec-company.md) | Dynamic CompanyFacts hierarchy/occurrences; 36 mapped concept keys; submissions and companion descriptors; XML/Inline contexts/units/facts/attributes | Numeric/profile filing projection; complete concept metadata/issuer-address schema outside this projection |
| [SEC funds](sec-funds.md) | 77 named core metadata contracts across ten form/table combinations; all 83 closed table names with metadata-defined columns | Exact generation metadata supplies unenumerated columns; lexical/semantic types and source units stated for core fields |
| [Tiingo](tiingo.md) | Metadata; raw/adjusted daily prices and same-shape mutual-fund NAV; distributions/splits | No NAV wrapper, upstream revision/finality or precise publication clock invented |
| [Nasdaq](nasdaq.md) | Both listing directory layouts, control footer, listing/indicator enums and round-lot counts | Listing reference only |
| [OCC](occ.md) | Six-field text/XML root records; memo CSV discovery; separate unverified JSON parser contract | Root references and discovery are not full contract adjustment terms |
| [Cboe](cboe.md) | Four All Series venue files; five-column records and OSI interpretation | Series identity/listing context, without quote or settlement fields |
| [Coinbase](coinbase.md) | Product metadata; public and optional Direct message families, books/trades and heartbeat | Public feed lacks proven counter progression; upstream heartbeat example/schema discrepancy documented |
| [Kraken](kraken.md) | REST asset/pair mapping; WebSocket v2 instrument/trade/book/level3 | Snapshot/delta and checksum rules remain distinct; local projection is not the whole API |
| [OpenFIGI](openfigi.md) | Mapping jobs, positional result/error objects | Conflicting official anonymous batch limits recorded without inventing one guarantee |
| [IEX HIST](iex-hist.md) | Catalog metadata, compressed downloads, transport packets and TOPS/DEEP binary messages | Historical protocol and adapter coverage, not present-day live feed entitlement |
| [FRED](fred.md), [BLS](bls.md), [BEA](bea.md) | Series/observations, period records and dynamic economic datasets | Metadata controls units and dynamic columns; example values do not prove fresh acquisition |
| [Federal Reserve Board](federal-reserve.md), [Census](census.md) | Release CSV/XML and header-driven dataset arrays | Release structure and dynamic metadata preserved; complete Board XSD/live SDMX evidence remains open |
| [EIA](eia.md), [Treasury](treasury.md) | Energy route/facet/observation envelopes; Fiscal Data and daily rate XML | Current string-data policy distinguished from parser allowance; Fiscal docs unavailable during review |
| [Tradier](tradier.md) | Quote/options/Greeks and market-data streaming | Dormant adapter only; no activation, connectivity or entitlement claim |

## Known missing schema evidence

| Family | Missing evidence |
|---|---|
| Alpaca | Complete upstream action/reference required/null flags; original contracts/action/history values; IV/Greeks; terminal option continuation page; option snapshot z definition/value |
| Schwab REST | Complete primary REST response specification and quote/chain/history/hours/mover originals; asset-specific member sets/types; exact percentage/Greek units and mark rules |
| Schwab Streamer | Original sparse/book/screener frames and observed nullability; independent frame verification of code mapping that differs from retained CHART_EQUITY table |
| Yahoo | Public versioned response spec and original bodies; typed candidate summary mappings; dynamic metric/rating units, Greek/settlement/multiplier schema and full holdings |
| SEC company | Complete CompanyFacts concept metadata and additional submissions metadata/columns; original JSON/XML examples; complete dimensional statement presentation |
| SEC funds | Original generation-specific archive headers/values/noncore column declarations; reviewed N-CEN PDF does not declare local REDEEMED_SHARES_PER_CREATION_UNIT |
| Tiingo | Original bodies, omission guarantees beyond documented nulls, revisions/finality/publication clocks and typed errors |
| Nasdaq/OCC/Cboe | Original examples for this rewrite, full upstream optionality/error contracts; verified OCC JSON memo response route and full operative memo-term interpretation |

## Validation boundary

Local checks verify Markdown table structure, heading anchors, repository links, anonymous observed primitive values against retained originals, the SEC typed-core column set, and Streamer field-ID coverage. Results: **13 original pages, 100 Markdown tables, 77 core columns in ten SEC form/table objects, 83 exact SEC catalog members, 309 audited Streamer field paths, and 24 actual stock example values checked; zero link/anchor/table/coverage mismatches.** Option condition/type variants and the two anonymous historical Schwab primitive records were checked against retained safe bodies. All 13 ignored originals matched their prior SHA-256 manifest. These checks do not turn parser allowances into upstream guarantees. Original ignored inventory/evidence artifacts were preserved; no private authentication/account bodies, authenticated requests, runtime mutations, production changes or builds were used in this lane.

The directory contains 22 provider/surface pages plus this coverage index, the directory index and the internal-contract appendix. Macro, crypto and identifier references state their remaining primary-specification, unit, nullability and original-response gaps in each page. No complete upstream API or new live acquisition is implied.
