# OCC reference response schemas

Contents: [GET download surfaces](#get-download-surfaces) · [DLP root record](#dlp-root-record) · [Memo CSV index](#memo-csv-index) · [Memo JSON parser contract — upstream response unverified](#memo-json-parser-contract--upstream-response-unverified) · [MemoDiscovery JSON record](#memodiscovery-json-record) · [Missing schema evidence](#missing-schema-evidence)

[OCC batch-processing documentation](https://beta-md.theocc.com/market-data/market-data-reports/other-market-data-info/batch-processing). Wire parsing: [occ.rs](../../../adapters/market-squawk-adapter-options-reference/src/occ.rs); exact request dispatch: [transport.rs](../../../adapters/market-squawk-adapter-options-reference/src/transport.rs).

No original response example was inspected here. DLP root identity, memo discovery and full operative memo content are separate families.

## GET download surfaces

| URL | Body |
|---|---|
| `https://marketdata.theocc.com/delo-download?prodType=ALL&downloadFields=OS;US;SN;EXCH;PL;ONN&format=txt` | Selected six-field text, fixed-width layout with terminal empty field |
| `https://marketdata.theocc.com/daily-delo-download?reportDate={YYYYMMDD}&format=txt` | Dated headerless six-column text |
| Same dated route, format=xml | results/record XML |
| `https://infomemo.theocc.com/infomemo/exportmemo` | CSV memo index export |
| `https://infomemo.theocc.com/infomemos?number={memo-number}` | Operative memo document; content retained without a complete financial-terms mapper |

Downloads are complete objects, not cursor pages. reportDate is publication selection and does not create an option-expiration field. HTTP errors/HTML are not valid DLP or memo records.

## DLP root record

Text fields OS/US/SN/EXCH/PL/ONN map to XML children under `results/record`. Each wire value is text; product/root identity does not provide option expiry/strike/call-put/multiplier/settlement.

| Field | Source encoding | Meaning |
|---|---|---|
| `DLP selected-text field OS; XML results/record/optionSymbol` | text | product/root symbol |
| `DLP selected-text field US; XML results/record/underlyingSymbol` | text | underlying alias |
| `DLP selected-text field SN; XML results/record/symbolName` | text | source security name |
| `DLP selected-text field EXCH; XML results/record/exchanges` | text | exchange codes |
| `DLP selected-text field PL; XML results/record/positionLimit` | text | position-limit text |
| `DLP selected-text field ONN; XML results/record/onnProductType` | text | product-type code |

Source: [occ.rs](../../../adapters/market-squawk-adapter-options-reference/src/occ.rs).

### DLP code and unit interpretation

PL is a position-limit count, not a price. EXCH contains source exchange codes. Current product codes admitted locally: EU/EB/EL/EF, CU/CL/CM/CF, IU/IL/IF, GF/SF/FC/FP, TU/TL. Current meanings from [OccProductType](../../../adapters/market-squawk-adapter-options-reference/src/occ.rs): EU equity underlying, EB equity bounds, EL equity long-term, EF equity FLEX; CU currency underlying, CL currency long-term, CM currency month-end, CF currency FLEX; IU index underlying, IL index long-term, IF index FLEX; GF interest-rate futures, SF stock futures, FC futures cash index, FP futures physical index; TU Treasury underlying, TL Treasury long-term. These are code-owned validation sets, not an inferred open-ended upstream guarantee.

## Memo CSV index

Exact header variants:
`Number,Post Date,Effective Date,Title,Category` or the final plural label `Categories`. Fields are CSV text. Number parses as positive integer; post/effective dates use MM/DD/YYYY; blank effective date is retained missing. Memo URL is constructed from number, not supplied as a CSV field. Categories are discovery metadata; the title/index is not operative adjustment terms.

## Memo JSON parser contract — upstream response unverified

The local parser additionally admits an object with page, total_pages, next_cursor and results[]. No official live locator/response matching this JSON envelope was verified. It must not be presented as the actual CSV export shape.

| Envelope member | Local type | Meaning |
|---|---|---|
| page | unsigned integer | Page ordinal |
| total_pages | unsigned integer | Declared page count |
| next_cursor | string/null, member required locally | Continuation marker |
| results | MemoDiscovery[] | MemoDiscovery records |

## MemoDiscovery JSON record

Prefix `$.results[]` for the unverified JSON contract above. post_date/effective_date use MM/DD/YYYY in local parsing; title/categories locate documents without interpreting economic terms.

| Field | Local parser value type | Meaning |
|---|---|---|
| `number` | number(integer) | Positive OCC memo identifier |
| `post_date` | string | Memo posting civil date, MM/DD/YYYY |
| `effective_date` | string/null | Adjustment/announcement effective civil date, MM/DD/YYYY if supplied |
| `title` | string | Memo title for document discovery, not operative economic terms |
| `categories` | array<string> | Source discovery-category labels |
| `memo_url` | string | Locator of operative memo document |

Source: [occ.rs](../../../adapters/market-squawk-adapter-options-reference/src/occ.rs).

## Missing schema evidence

The JSON memo envelope has parser evidence, not upstream receipt. Full memo contents need a separately reviewed interpretation schema; they cannot supply adjustment multipliers/settlement by guessing from titles. Text/XML per-field upstream optionality, complete exchange-code semantics and error bodies remain unverified here.
