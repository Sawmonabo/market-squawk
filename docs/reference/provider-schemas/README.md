# Provider response schemas

Reusable response projections organized by endpoint, envelope and nested object. These documents cover audited application surfaces, not complete upstream APIs. Review date: **2026-10-10**. Tables distinguish documented wire facts, observed source values and local parser allowances. They describe reusable protocol shapes.

| Provider | Reference | Coverage |
|---|---|---|
| Alpaca | [REST and streams](alpaca.md) | Snapshot/shared quote-trade-bar objects, history, contracts/deliverables, assets, all 16 action categories, WebSocket messages |
| Charles Schwab | [REST](schwab.md) · [Streamer](schwab-streamer.md) | Quote components/reference/fundamental, chains/expirations/history/hours/movers/instruments; all audited service numeric dictionaries and nested books/screeners |
| Yahoo Finance | [Experimental response shapes](yahoo.md) | Quote/chart/actions/reference/fund/options/search/lookup; all parsed containers/dynamic fund maps and unverified summary candidates |
| SEC | [Company and XBRL](sec-company.md) · [Fund archives](sec-funds.md) | Facts/submissions/companions/XBRL; 77 typed core TSV columns and metadata-defined schemas for 83 closed archive tables |
| Tiingo | [EOD/NAV and actions](tiingo.md) | Metadata, raw/adjusted daily prices, mutual-fund NAV interpretation, distributions and splits |
| Nasdaq Trader | [Directory files](nasdaq.md) | Complete two-file listing layouts, controls and code dictionaries |
| OCC | [DLP and memos](occ.md) | Text/XML root records, CSV memo export and separately unverified JSON parser contract |
| Cboe | [All Series CSV](cboe.md) | Four venue files, five-column row schema and OSI interpretation |
| Coinbase | [REST and streams](coinbase.md) | Product references; public/Direct message envelopes, trades, level-2 books, heartbeat and sequence semantics |
| Kraken | [REST and WebSocket v2](kraken.md) | Assets/pairs; instrument, trade, level-2 and level-3 books, checksums and subscription responses |
| OpenFIGI | [Identifier mapping](openfigi.md) | Mapping request jobs, positional response arrays, result objects and errors |
| IEX HIST | [Catalog and binary feeds](iex-hist.md) | Historical catalog/download, IEX-TP packets, TOPS/DEEP messages and field encodings |
| FRED | [Series and observations](fred.md) | Series metadata, realtime intervals, observation rows and pagination |
| BLS | [Public data](bls.md) | Series responses, period/value rows, footnotes and calculations |
| BEA | [Parameters and datasets](bea.md) | Parameter catalogs, dynamic dataset rows, notes and errors |
| Federal Reserve Board | [Release data](federal-reserve.md) | H.15 CSV and release-aware SDMX/XML keys, observations and units |
| Census | [Dataset rows](census.md) | Header-driven JSON arrays, variables, geography and dynamic measures |
| EIA | [Routes and observations](eia.md) | Route metadata, facets, frequency, data rows, pagination and errors |
| Treasury | [Fiscal data and rates](treasury.md) | Fiscal Data envelopes/metadata and daily XML rate families |
| Tradier — dormant | [Retained adapter schemas](tradier.md) | Quotes/options/Greeks, stream session and quote/tradex events; excluded from selected activation |

[Internal contracts](internal-contracts.md) are a separate appendix. Forecasts, valuation, probabilities, backtests, patterns, selected app pages and normalized fund projections are not provider response fields.

## Reading the schemas

A field table is relative to its stated object mount. Reused objects replace repeated field rows. Dynamic keys use explicit placeholders; neither missing keys nor enum members are invented. Sparse stream updates differ from REST snapshots.

“Parser-admitted” describes current code, not upstream requiredness. Omitted, null, invalid and zero are different states. Presence claims are made only where a primary response specification or inspected original establishes them. Parser-only scalar unions do not declare official numeric types.

Observed examples come only from retained safe public market-data originals. Identifiers are omitted to keep examples reusable; no illustrative value is presented as an observed payload. Historical quarantined receipts establish primitive shape only, not current freshness or availability. No private authentication/account body is included.

## Evidence and gaps

Source fields are organized into object tables and shared mounts. Parser-only keys, unknown candidate paths and internal fields are separated from documented response guarantees. [Coverage and validation](coverage.md) records the projection boundary and missing schema evidence. The reference distinguishes documentation and parser review from live acquisition and installed-workflow verification.
