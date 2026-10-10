# OpenFIGI V3 mapping schemas

Reviewed **2026-10-10** against the current working-tree adapter and primary documentation. This is reference-identity enrichment for source-qualified listing symbol/MIC jobs, not quotes, historical membership or trading authority. The selected implementation uses only `POST https://api.openfigi.com/v3/mapping`; search/filter/value-discovery are separate upstream APIs and are not called by this adapter. No provider request or credential access was performed for this review.

Source: [request/response parser](../../../adapters/market-squawk-adapter-openfigi/src/parser.rs), [transport](../../../adapters/market-squawk-adapter-openfigi/src/client.rs), [typed outcomes](../../../adapters/market-squawk-adapter-openfigi/src/model.rs). Upstream authority: [OpenFIGI documentation](https://www.openfigi.com/api/documentation). The tables describe this endpoint's reusable schema; parser constraints are identified separately.

## Request array

Body: `array<MappingJob>` with positional responses. Headers: `Content-Type: application/json`, `Accept: application/json`, and optional secret `X-OPENFIGI-APIKEY`. Mapping has no page cursor. Batch splitting is by jobs, preserving each original listing identity.

Source: [official mapping contract](https://www.openfigi.com/api/documentation#v3-mapping), [encoder](../../../adapters/market-squawk-adapter-openfigi/src/parser.rs).

| MappingJob field | Wire type | Presence and meaning | Current request |
|---|---|---|---|
| `idType` | string | Required identifier-kind code | Always `TICKER` |
| `idValue` | string or number upstream | Required identifier value | Listing symbol as string |
| `micCode` | string | Optional ISO MIC; mutually exclusive with `exchCode` | Required by the listing-job model |
| `includeUnlistedEquities` | boolean | Optional inclusion of unlisted equity candidates | Always `false` |

Other upstream mapping filters (`exchCode`, `currency`, `marketSecDes`, `securityType`, `securityType2`, option/range/date filters) are not emitted here. Their presence in the full API does not broaden the listing-mapping request contract.

## Result array and candidate object

Body: `array<MappingResult>`; result at index `i` belongs to request job `i`. A job returns candidate data, a no-match warning, or an error. These are job results within an HTTP success, not transport success/failure substitutions.

Source: [official response format](https://www.openfigi.com/api/documentation#v3-mapping), [parser classification](../../../adapters/market-squawk-adapter-openfigi/src/parser.rs).

| MappingResult field | Wire type | Presence and meaning | Parser behavior |
|---|---|---|---|
| `data` | array<Candidate> | Present when mappings exist | Missing/null is absent; empty array is conflict |
| `warning` | string | Present for no match | Missing/null absent; valid text becomes `NoMatch` |
| `error` | string | Present for failed job | Missing/null absent; valid text becomes `ProviderError` with message digest |

Exactly one non-null outcome is required locally. No outcome or multiple outcomes produces a typed conflict; one bad job does not silently remove other jobs. Result-array length must equal request-array length.

| Candidate field | Wire type | Meaning | Current projection |
|---|---|---|---|
| `figi` | string | Exchange-level instrument FIGI | Required valid FIGI; absent/null becomes conflict |
| `compositeFIGI` | string or null | Composite identity across venues within the applicable composite | Optional validated FIGI |
| `shareClassFIGI` | string or null | Share-class identity across applicable composites | Optional validated FIGI |
| `ticker` | string or null | Provider's display/mapping ticker | Retained in original only |
| `name` | string or null | Provider's descriptive instrument name | Retained in original only |
| `exchCode` | string or null | Provider exchange/composite code | Retained in original only |
| `marketSector` | string or null | Provider market-sector classification | Retained in original only |
| `securityType` | string or null | Detailed security classification | Retained in original only |
| `securityType2` | string or null | Broader alternate security classification | Retained in original only |
| `securityDescription` | string or null | Provider descriptive identifier text | Retained in original only |
| `metadata` | string: `Metadata N/A` | Attributes unavailable/not displayable | Retained in original only |

Descriptive members above are documented upstream, not validated by the local three-FIGI projection. The official table allows null attribute/relationship values and a metadata-unavailable marker. Unknown fields are skipped by the projection, while the complete bounded original remains receipt-bound. One candidate becomes `Exact`; multiple valid candidates become `Ambiguous`. Neither implies canonical listing acceptance. Duplicate candidates, contradictory FIGI relationships, invalid FIGIs and candidate overflow are conflicts.

## Headers, limits and errors

Source: [official rate/status tables](https://www.openfigi.com/api/documentation), [client status handling](../../../adapters/market-squawk-adapter-openfigi/src/client.rs).

| Response header | Wire type | Meaning |
|---|---|---|
| `ratelimit-limit` | ASCII unsigned integer | Requests in current window |
| `ratelimit-remaining` | ASCII unsigned integer | Remaining requests |
| `ratelimit-reset` | ASCII unsigned integer | Seconds until window reset; not an epoch timestamp |
| `Retry-After` | HTTP header text | Provider retry instruction when supplied |

The official general rate table lists 10 jobs/request without a key and 100 with a key, with 25 requests/minute and 25 requests/6 seconds respectively; the same page's mapping-specific Limits table instead lists 5 public jobs/request. This primary-documentation conflict remains unresolved. Current local access constants admit 10/100 jobs; that does not establish a reliable public upstream maximum. Current source admission remains shared and cannot expand from observed headers. The parser bounds request bytes at 64 KiB, response bytes at 2 MiB, and candidates at 256/job. These are local bounds, not provider guarantees.

The client requires HTTP 200, JSON content type, identity content encoding and one complete, unique rate-header set. It handles 429/503 through shared retry admission, 401/403 as authorization failure, 500 as provider unavailable and other non-200 statuses as rejected. It does not retry or poll independently. HTTP error bodies are not parsed into the successful mapping schema. Official status descriptions also include 400/404/405/406/413/415; no complete error-object contract is established here.

## Evidence boundary

Mapping results contain no provider event timestamp or historical effective interval. Request/receive times are local receipt evidence; they must not backdate an identity into historical universes. No live response example was verified in this review. Official examples demonstrate shape only. Search/filter pagination (`next`/`start`) belongs to those nonselected APIs and must never be invented on `/v3/mapping`.
