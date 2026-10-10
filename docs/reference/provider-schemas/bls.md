# BLS response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). This page covers both time-series operations consumed by the adapter. Presence means **required or admitted by the local parser**; it does not make upstream example fields formally required. The retained fixture is a structural reduction with test values and a preliminary-footnote addition, not an untouched live response. No observed values are reproduced.

## Endpoints and request mode

| Operation | Request | Success shape |
|---|---|---|
| POST `https://api.bls.gov/publicAPI/v1/timeseries/data/` | `seriesid: string[]`, `startyear: string`, `endyear: string` | Response → Results → Series[] → Observation[] |
| POST `https://api.bls.gov/publicAPI/v2/timeseries/data/` | Same selection plus registration key; `catalog=false`, `calculations=false`, `annualaverage=false`, `aspects=false` | Same narrow response contract |

The adapter explicitly disables v2 expansions. Catalog, calculations and aspects objects from other request modes are outside its closed parser. Latest GET, popular series and survey-discovery endpoints are upstream documented capabilities, **not requests made by this adapter**. The local discovery flow uses configured, application-owned series metadata; that internal metadata JSON is not a BLS wire response.

Sources: [requests](../../../adapters/market-squawk-adapter-bls/src/client.rs), [discovery](../../../adapters/market-squawk-adapter-bls/src/discovery.rs), [metadata authority](../../../adapters/market-squawk-adapter-bls/src/series_metadata.rs), official [v1 signatures](https://www.bls.gov/developers/api_signature.htm), [v2 signatures](https://www.bls.gov/developers/api_signature_v2.htm).

## Response and Results

| Object | Field | Wire type | Local presence | Meaning |
|---|---|---|---|---|
| Response | `status` | string | Required | Semantic request outcome; success accepted only as `REQUEST_SUCCEEDED` |
| Response | `responseTime` | integer number | Required | Provider processing duration in milliseconds; not a publication timestamp |
| Response | `message` | string[] | Required | Provider warnings/errors, including invalid-series reports |
| Response | `Results` | object | Required | Result collection |
| Results | `series` | Series[] | Required | Returned series, including potentially empty results |

HTTP 200 or `REQUEST_SUCCEEDED` does not establish complete usable observations. The adapter marks empty series, empty result collections or any message as partial and requires exact returned/requested series membership and years before acquisition can proceed. Unknown fields in each decoded object are rejected.

Source: [response parser](../../../adapters/market-squawk-adapter-bls/src/observations.rs), official [v2 response examples](https://www.bls.gov/developers/api_signature_v2.htm).

## Series and Observation

| Object | Field | Wire type | Local presence | Meaning |
|---|---|---|---|---|
| Series | `seriesID` | string | Required | Provider series identity; not an instrument/ticker |
| Series | `data` | Observation[] | Required | Period observations, in provider order |
| Observation | `year` | string | Required | Calendar/reference year; locally numeric and at least 1900 |
| Observation | `period` | string | Required | Survey period coordinate, including annual-average periods |
| Observation | `periodName` | string | Required | Human period label |
| Observation | `latest` | string | Omission/null accepted locally | `"true"` or `"false"`; omitted/null means no latest marker locally, never JSON boolean |
| Observation | `value` | string | Required | Exact decimal or local missing marker `-` |
| Observation | `footnotes` | Footnote[] | Required | Observation qualifications; may include an empty object |

Units, measure and seasonal adjustment come from admitted series metadata, not a universal field inside Observation. Value null, absent and `-` are different wire states; only `-` is admitted as missing by this parser. Period does not carry a release instant or historical revision interval.

| Local accepted period grammar | Meaning |
|---|---|
| `M01`–`M12`; `M13` | Monthly periods; annual average |
| `Q01`–`Q04`; `Q05` | Quarterly periods; annual average |
| `S01`–`S02`; `S03` | Semiannual periods; annual average |
| `A01` | Annual period |

This grammar is local acceptance, not a promise every survey uses every code. Duplicate `(year,period)` pairs within a series are rejected; display order is preserved.

Source: [observations and period parser](../../../adapters/market-squawk-adapter-bls/src/observations.rs); [BLS API FAQ](https://www.bls.gov/developers/api_faqs.htm).

## Footnote

| Field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `code` | string | Omission/null accepted locally | Compact qualification identifier; `P` marks preliminary locally |
| `text` | string | Omission/null accepted locally | Explanation of the qualification |

An empty footnote object carries no preliminary signal. Footnotes are retained separately from value and period; preliminary is not a revision timestamp. The API does not supply ALFRED-style vintages through these endpoints.

Source: [FootnoteWire and preliminary handling](../../../adapters/market-squawk-adapter-bls/src/observations.rs), official [preliminary example](https://www.bls.gov/developers/api_signature_v2.htm).

## Pagination, errors and missing evidence

There is no cursor/offset page envelope here. The adapter splits selections into bounded series/year request chunks; chunk completion is not a provider cursor. It validates each request and combines its exact results. Current configured chunk ceilings are application policy, distinct from the upstream v1/v2 query limits.

Provider failures use the same `status`, `responseTime`, `message`, `Results` envelope in documented examples; the parser rejects non-success status and retains success messages as partial evidence. HTTP refusals/throttling are transport failures. No separate universal JSON HTTP-error object was verified. Formal upstream per-field omission/null guarantees, enabled expansion schemas and immutable historical release clocks remain outside the evidence established here.

Sources: [chunking](../../../adapters/market-squawk-adapter-bls/src/chunks.rs), [transport/status handling](../../../adapters/market-squawk-adapter-bls/src/client.rs), [fixture evidence classification](../../../adapters/market-squawk-adapter-bls/fixtures/manifest.json).
