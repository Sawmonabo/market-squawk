# Federal Reserve Board response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). The Board adapter accepts DDP series-column CSV, SDMX Compact XML and bounded SDMX ZIP packages. H.15 and G.17 are the closed release selectors. Tables below describe **local contract requirements**; they do not guarantee every Board download has this shape. No retained live example was inspected for this page, so example values are unverified.

## Requests and formats

| GET route on `https://www.federalreserve.gov` | Selection/encoding | Consumed response |
|---|---|---|
| `/datadownload/Download.aspx` | `rel`, `series`, `filetype=csv`, `label=include`, `layout=seriescolumn`, `type=package`; full-history H.15 has empty `lastObs` | CSV metadata matrix followed by dated values |
| `/datadownload/Output.aspx` | Same CSV selectors, `lastobs=100` dashboard or `lastobs=10` doctor | Same CSV schema with bounded recent periods |
| DDP `Download.aspx`/`Output.aspx` under a frozen dataset contract | `rel=H15` or `G17`, `filetype=xml`/`sdmx` or `zip`, contract-bound selectors | CompactData XML or package members |
| Contract-bound Board release `.xml`/`.zip` files | Board-hosted release path validated by contract | CompactData XML or ZIP; candidate support is not a verified live route |

The H.15 CSV package selector is code-owned `bf17364827e38702b42a58cf8eaa3f78`; it is a package identity, not one instrument. There is no cursor envelope. Full and rolling requests are distinct datasets. An admitted contract binds exact URLs, namespaces, release, frequency, series scope and package artifacts; the parser never discovers arbitrary URLs from the response.

Sources: [request/format contracts](../../../adapters/market-squawk-adapter-federal-reserve/src/contract.rs), [transport](../../../adapters/market-squawk-adapter-federal-reserve/src/transport.rs), official [DDP help](https://www.federalreserve.gov/DataDownload/help/default.htm), [H.15 downloads](https://www.federalreserve.gov/datadownload/choose.aspx?rel=h15). DDP route lifecycle is separate from schema acceptance; a FRED substitute has different provenance.

## CSV matrix

All CSV cells are text on the wire. The first column contains metadata labels or a period. Each subsequent column represents one series and must preserve its position through all rows. The parser requires these six metadata records in this exact order, including punctuation/whitespace.

| First-column label | Remaining cells | Meaning |
|---|---|---|
| `Series Description` | Nonempty text | Economic description of each series |
| `Unit:` | Text | Published unit, e.g. rate rather than currency amount |
| `Multiplier:` | Decimal text | Published scale factor; compared exactly to the frozen contract |
| `Currency:` | Text | Provider currency code or not-applicable marker |
| `Unique Identifier: ` | Text | Exact release/dataset/series identity; trailing label space is significant |
| `Time Period` | Text | Series names aligned to subsequent value columns |

| Data-record position | Wire form | Local requirement/meaning |
|---|---|---|
| Column 0 | Period text | Reference period interpreted using contract frequency |
| Columns 1…N | Decimal text, `ND`, or empty text | One value per aligned series; `ND`/empty are missing, never zero |

Width is exactly N+1 in every record. The local parser requires at least one observation period, unique periods, and metadata matching the expected series order, units, multiplier and currency. Commas/quotes are ordinary CSV escaping, not JSON structures. The H.15 dashboard unit is percent per year; it is not a fractional rate until an explicit downstream transformation says so.

Source: [CSV parser](../../../adapters/market-squawk-adapter-federal-reserve/src/parse/csv.rs), [H.15 contract units](../../../adapters/market-squawk-adapter-federal-reserve/src/contract.rs).

## SDMX CompactData and Header

The message namespace is `http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message`; the release-specific DataSet namespace is frozen in the package contract. Prefix spellings are irrelevant, namespace URIs are not. XML declaration and contract-bound structure/schema artifacts are required locally.

| XML path | Wire form | Local presence | Meaning |
|---|---|---|---|
| `CompactData/Header` | Element | Required before DataSet | Message provenance header |
| `Header/ID` | Text | Required | Message identity with contract-bound release prefix |
| `Header/Test` | Text | Required, `false` | Production/test message flag |
| `Header/Prepared` | Text | Required | Package preparation timestamp; not observation effective date |
| `Header/Sender/@id` or `@ID` | Attribute text | Exactly one required | Publisher identity |
| `CompactData/DataSet` | Element | Required | Collection of series in release namespace |
| `DataSet/Series` | Repeated element | Nonempty series scope locally | One series and its observations |

`Prepared` accepts RFC 3339 with zone, retained as an instant. A valid zone-less `YYYY-MM-DDTHH:MM:SS[.fraction]` is retained without inventing an instant. Package preparation is not proof of first historical public availability.

Source: [SDMX hierarchy parser](../../../adapters/market-squawk-adapter-federal-reserve/src/parse/sdmx.rs), [header model](../../../adapters/market-squawk-adapter-federal-reserve/src/model.rs).

## Series attributes

Every attribute is XML text, even numeric metadata. Local requiredness below comes from `SeriesBuilder`.

| Attribute | Local presence | Meaning |
|---|---|---|
| `SERIES_NAME` | Required | Native series selector within the release |
| `FREQ` | Required | Cadence code; local D/W/M/Q/A must match contract |
| `UNIT` | Required | Published measurement unit |
| `UNIT_MULT` | Required decimal text | Native scale metadata, retained and compared to contract |
| `CURRENCY` | Required | Currency or not-applicable code |
| `SERIES_DESCRIPTION` or `DESCRIPTION` | One description required locally | Human explanation of the measure |
| `UNIQUE_IDENTIFIER` | Optional locally | Native identifier; when absent a release/series coordinate is generated locally, not claimed returned |
| Additional release attributes | Allowed within bounded map | Dataset-specific dimensions/qualifications retained by exact name |

Additional attributes do not authorize new economic semantics without the frozen dataset contract and package evidence. Exact-scope series must match its selected unit/scale/currency/description; complete-release mode requires structure-bound scope instead.

## Obs attributes

| Attribute | Wire form | Local presence | Meaning |
|---|---|---|---|
| `TIME_PERIOD` | Text | Required | Economic reference period |
| `OBS_VALUE` | Decimal text or `ND` | Optional locally | Exact measure; absent/empty/`ND` remains missing |
| `OBS_STATUS` | Text | Required | Provider observation-status code; `ND` indicates missing locally |
| Additional attributes | Text | Allowed within bounded map | Observation-level release dimensions/status evidence |

Values with surrounding whitespace are rejected. A nonmissing decimal paired with `OBS_STATUS=ND` is inconsistent and rejected. Observations within each XML series must increase strictly by period; duplicate coordinates are rejected. The parser does not define a universal closed set of status codes beyond missing-value treatment.

| Contract frequency | Locally parsed period text |
|---|---|
| Business daily/daily | `YYYY-MM-DD` |
| Weekly | `YYYY-W01`…`YYYY-W53`, valid ISO week |
| Monthly | `YYYY-MM` |
| Quarterly | `YYYY-Q01`…`YYYY-Q04` |
| Annual | `YYYY` |

Sources: [SDMX series/observation parser](../../../adapters/market-squawk-adapter-federal-reserve/src/parse/sdmx.rs), [period and missing-value model](../../../adapters/market-squawk-adapter-federal-reserve/src/model.rs).

## ZIP artifacts, errors and evidence gaps

The package contract names the exact data XML, FRB common schema, release structure and dataset schema members with expected identities. ZIP membership, duplicates, member sizes, digests and XML namespaces are validated; this is an archive envelope rather than a JSON response object. Files are not recursively accepted because they have an `.xml` suffix.

Sources: [package decoder](../../../adapters/market-squawk-adapter-federal-reserve/src/parse/sdmx.rs), [artifact contracts](../../../adapters/market-squawk-adapter-federal-reserve/src/contract.rs), official [SDMX overview](https://www.federalreserve.gov/DataDownload/help/default.htm).

Transport status and malformed CSV/XML/archive errors remain failures; no stable upstream structured HTTP-error body was verified. The full release-specific upstream XSD vocabulary, formal requiredness of each optional/native attribute, raw SDMX examples and current live availability of candidate release-file routes were not independently verified here. Local supported formats are not proof of all-release integration or complete historical vintages.
