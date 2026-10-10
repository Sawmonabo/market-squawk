# FRED and ALFRED response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). This covers the adapter's v1 series, observation and vintage endpoints and v2 release observations. Field presence below describes the **local parser contract**, not a formal upstream required/null guarantee. The v1 fixtures are documented-schema test data, not captured observations; no live example values were verified for this page.

## Requests and envelopes

All responses below are JSON selected explicitly by the request. V1 uses query-key authentication; v2 uses a bearer header. Credentials are excluded from retained request locators.

| GET path on `https://api.stlouisfed.org` | Selection | Response root | Pagination |
|---|---|---|---|
| `/fred/series` | `series_id`, real-time bounds, `file_type=json` | SeriesEnvelope | None; `seriess` can contain metadata revisions |
| `/fred/series/observations` | Same series/bounds, observation bounds, `output_type=1`, `units=lin`, `order_by=observation_date`, `sort_order=asc`, `file_type=json`, `limit`, `offset` | ObservationPage | Zero-based offset; next offset = offset + returned rows until `count` |
| `/fred/series/vintagedates` | Same series/bounds, `sort_order=asc`, `file_type=json`, `limit`, `offset` | VintagePage | Zero-based offset until `count` |
| `/fred/v2/release/observations` | `release_id`, `format=json`, `limit`, optional `next_cursor` | ReleasePage | `has_more` and opaque returned `next_cursor`; never v1 offset |

Sources: [v1 request implementation](../../../adapters/market-squawk-adapter-fred/src/client.rs), [metadata request](../../../adapters/market-squawk-adapter-fred/src/client/metadata.rs), [vintage/release requests](../../../adapters/market-squawk-adapter-fred/src/client/macro_pages.rs); official [observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html), [vintages](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html), [v2 release](https://fred.stlouisfed.org/docs/api/fred/v2/release_observations.html).

## SeriesEnvelope and SeriesMetadata

SeriesEnvelope has three required members: `realtime_start: string`, `realtime_end: string` (requested inclusive civil-date envelope), and `seriess: SeriesMetadata[]` (the spelling is upstream). Dates are `YYYY-MM-DD`, not instants. The parser requires a nonempty array, matching series identity and ordered, nonoverlapping metadata intervals after semantic sorting.

| SeriesMetadata field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `id` | string | Required | Provider's stable series selector |
| `realtime_start`, `realtime_end` | string | Required | Inclusive dates for which these metadata semantics apply |
| `title` | string | Required | Human description of the measured series |
| `observation_start`, `observation_end` | string | Required | Earliest/latest observation reference dates |
| `frequency`, `frequency_short` | string | Required | Full and abbreviated sampling-frequency labels |
| `units`, `units_short` | string | Required | Full and abbreviated measurement-unit labels |
| `seasonal_adjustment`, `seasonal_adjustment_short` | string | Required | Full and abbreviated adjustment labels |
| `last_updated` | string | Required | Provider update timestamp; parser accepts `YYYY-MM-DD HH:MM:SS±HH` |
| `popularity` | integer number | Required | Provider popularity ranking measure; local `u32` |
| `notes` | string | Omission/null accepted locally | Explanatory, methodology or attribution text |

Source: [metadata parser](../../../adapters/market-squawk-adapter-fred/src/client/metadata.rs), official [series response example](https://fred.stlouisfed.org/docs/api/fred/series.html). The official page provides examples rather than a closed versioned requiredness schema. Local `notes` null acceptance does not establish upstream null emission.

## ObservationPage and Observation

| Page field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `realtime_start`, `realtime_end` | string | Required | Requested knowledge-date interval |
| `observation_start`, `observation_end` | string | Required | Requested reference-date interval |
| `units` | string | Required, `lin` only locally | Transformation mode; linear leaves source values untransformed |
| `output_type` | integer number | Required, `1` only locally | Observations represented by real-time periods |
| `file_type` | string | Required, `json` | Selected response encoding |
| `order_by`, `sort_order` | string | Required, `observation_date`, `asc` | Page ordering contract |
| `count` | integer number | Required | Total matching observations, including later pages |
| `offset` | integer number | Required | Number of observations skipped before this page |
| `limit` | integer number | Required | Requested page ceiling |
| `observations` | Observation[] | Required | Rows in this page |

| Observation field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `realtime_start`, `realtime_end` | string | Required | Inclusive knowledge-date interval of this value revision |
| `date` | string | Required | Reference date of the economic observation |
| `value` | string | Required | Exact decimal text, or `.` for missing data; never JSON null locally |

The unit comes from series metadata; page `units=lin` is a transformation code, not percent/dollars. Official transformations include `lin`, `chg`, `ch1`, `pch`, `pc1`, `pca`, `cch`, `cca`, `log`; output modes 1–4 differ. These broader modes are outside this local observation parser. It rejects unknown page/row fields, out-of-order duplicate revisions, impossible intervals and count/offset inconsistency.

Source: [series parser](../../../adapters/market-squawk-adapter-fred/src/series.rs), official [response and parameters](https://fred.stlouisfed.org/docs/api/fred/series_observations.html).

## VintagePage

| Field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `realtime_start`, `realtime_end` | string | Required | Knowledge-date search interval |
| `order_by`, `sort_order` | string | Required, `vintage_date`, `asc` | Revision-event date order |
| `count`, `offset`, `limit` | integer number | Required | Matching dates, skipped dates, page ceiling |
| `vintage_dates` | string[] | Required | Strictly increasing `YYYY-MM-DD` dates of new/revised values |

A vintage date marks a series change, not every possible as-of date and not its observation period. V1 observations permit up to 100,000 rows/page; vintage dates up to 10,000. Local request guards and configured whole-chain bounds can be narrower.

Source: [vintage parser](../../../adapters/market-squawk-adapter-fred/src/vintages.rs), official [vintage dates](https://fred.stlouisfed.org/docs/api/fred/series_vintagedates.html).

## ReleasePage, Release and ReleaseSource

| ReleasePage field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `has_more` | boolean | Required | Whether another page is needed |
| `next_cursor` | string | Required when continuing; omission/null accepted at terminal | Returned continuation coordinate, parsed locally as `series_id,YYYY-MM-DD` |
| `release` | Release | Required | Release identity and publisher attribution |
| `series` | ReleaseSeries[] | Required | Ordered series segments; one series may span pages |

Official JSON examples use boolean `has_more`; descriptive prose calls it a string. This local JSON parser accepts boolean only. The official terminal example omits `next_cursor`; local null acceptance is broader. The limit counts **observations across series**, up to 500,000, not series objects. Each continuation must begin at the requested cursor; local checks require increasing coordinates and stable split-series metadata.

| Object | Field | Wire type | Local presence | Meaning |
|---|---|---|---|---|
| Release | `release_id` | integer number | Required, positive | Requested release identifier |
| Release | `name` | string | Required | Release title |
| Release | `url` | string | Required | Originating release URL |
| Release | `sources` | ReleaseSource[] | Required, nonempty locally | Agencies publishing the release |
| ReleaseSource | `name` | string | Required | Publishing agency name |
| ReleaseSource | `url` | string | Required | Agency's originating URL |
| ReleaseSource | `notes` | string | Omission/null accepted locally | Source explanatory notes; official docs say supplied only when recorded |

## ReleaseSeries and ReleaseObservation

| ReleaseSeries field | Wire type | Local presence | Meaning |
|---|---|---|---|
| `series_id` | string | Required | Series identity within the release |
| `title` | string | Required | Economic measure description |
| `frequency` | string | Required | Sampling cadence |
| `units` | string | Required | Measurement units |
| `seasonal_adjustment` | string | Required | Adjustment convention |
| `last_updated` | string | Required | RFC 3339 UTC update instant; local parser requires trailing `Z` |
| `copyright_id` | string | Required | Provider's attribution/restriction notice |
| `notes` | string | Required; may be empty | Series explanation |
| `observations` | ReleaseObservation[] | Required, nonempty locally | Increasing observation-date/value pairs |

ReleaseObservation has required `date: string` (`YYYY-MM-DD`) and `value: string` (exact decimal or `.`). V2 does **not** supply v1 row real-time revision intervals in this schema. Series update time is not automatically a historical public-release time. Updates during traversal can produce mixed generations; retain metadata and reacquire rather than claiming snapshot isolation.

Sources for all v2 objects: [release parser](../../../adapters/market-squawk-adapter-fred/src/release.rs), official [release response contract](https://fred.stlouisfed.org/docs/api/fred/v2/release_observations.html).

## Errors and evidence limits

V1 documents JSON `error_code: integer` and `error_message: string` for failures. The transport maps status separately: 401/403 unauthorized; 429/503 cooldown using `Retry-After`; other non-200 statuses network failure. It does not parse that error object as successful data. See [official v1 errors](https://fred.stlouisfed.org/docs/api/fred/errors.html) and [transport](../../../adapters/market-squawk-adapter-fred/src/client/http.rs). A distinct complete v2 error-body schema was not verified here. No formal upstream JSON Schema, general future nullability guarantee or live page-completeness proof is established by this reference.
