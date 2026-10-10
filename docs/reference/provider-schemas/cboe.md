# Cboe All Series CSV schema

Contents: [GET files](#get-files) · [CSV row](#csv-row) · [Missing schema evidence](#missing-schema-evidence)

[Official locator update](https://cdn.cboe.com/resources/release_notes/2024/Cboe-Updates-Options-Symbol-Reference-and-Equities-Symbols-Traded-File-URLs.pdf). These are venue-specific complete CSV files, not option quote APIs. No original CSV row is used as a fresh example here.

## GET files

Base directory: `https://cdn.cboe.com/data/us/options/market_statistics/symbol_reference/`.

| Venue | Exact file |
|---|---|
| C1 | cone-all-series.csv |
| BZX | opt-all-series.csv |
| C2 | ctwo-all-series.csv |
| EDGX | exo-all-series.csv |

Files have independent publication context; there is no response cursor. Parser expects the exact header below and rejects extra/missing columns. HTTP Last-Modified, when supplied, is file publication context rather than option quote time.

## CSV row

Every source cell is text. OSI Symbol decodes root, expiration date, C/P side and strike at the standard scale of 1/1000; those decoded values are not extra CSV columns. Matching Unit is a venue routing identifier. Closing Only retains the source flag. Format-valid symbols do not supply settlement/multiplier/deliverables.

| Field | Source encoding | Meaning |
|---|---|---|
| `Cboe Symbol` | CSV string | Venue-native series identifier |
| `OSI Symbol` | CSV string | Standard option identity text carrying root/date/side/strike |
| `Underlying` | CSV string | Provider alias of underlying instrument |
| `Matching Unit` | positive integer text | Positive venue matching-engine routing unit; local nonzero u16 validation |
| `Closing Only` | True/False text | Venue restriction flag; local True means closing-only, False means normal |

Source: [cboe.rs](../../../adapters/market-squawk-adapter-options-reference/src/cboe.rs).

## Missing schema evidence

Original wire examples, future header/enum changes, per-cell upstream nullable/required flags and stable error bodies remain unverified. Quotes, IV/Greeks, volume/open interest and adjustment terms are absent from this file schema.
