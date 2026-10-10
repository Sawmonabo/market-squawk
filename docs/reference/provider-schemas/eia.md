# EIA response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). EIA v2 has a shared JSON envelope and route-specific metadata/data objects. This covers route discovery, facet discovery and data acquisition consumed by the adapter. **Local presence and parser alternatives are separate from the provider wire examples.** No untouched retained live response was inspected; example values are unverified.

## Endpoints and shared envelope

| GET endpoint | Selected response |
|---|---|
| `https://api.eia.gov/v2/{route}` | RouteMetadata |
| `https://api.eia.gov/v2/{route}/facet/{facet}` | FacetCatalog |
| `https://api.eia.gov/v2/{route}/data` | DataPage; query selects `data[]`, `facets[facet][]`, `frequency`, bounds, ordered sorts, `offset`, `length`, `out=json` |

Every local success parser requires this root; metadata/data below mounts at `$.response`.

| Root field | Wire form | Local presence | Meaning |
|---|---|---|---|
| `apiVersion` | string | Required | Serving API version; data must match discovered metadata |
| `request` | RequestEcho | Required | Provider's interpretation of the request |
| `response` | object | Required | Route/facet/data result |
| `error` | string in official error example | Presence rejected | Provider failure, never a success response |

RequestEcho requires `command: string` (path identifying the operation) and `params: object` (query interpretation). An empty `[]` is also accepted only for metadata with no nonsecret parameters. The object must have exactly those two keys; command is matched after stripping trailing slashes. API-key echoes are validated/redacted before retained evidence; omitted credential echoes are also accepted. Other root fields are bounded and retained rather than made canonical fields.

Sources: [requests](../../../adapters/market-squawk-adapter-eia/src/request.rs), [shared envelope parser](../../../adapters/market-squawk-adapter-eia/src/wire.rs), official [EIA API technical documentation](https://www.eia.gov/opendata/documentation.php).

## RequestEcho.params for a data query

These are JSON echo members, distinct from bracketed URL query keys. The parser requires exactly the nonsecret parameters it sent.

| Echo member | JSON form locally expected | Meaning |
|---|---|---|
| `data` | string[] | Requested measured columns |
| `facets` | `{facetName: string[]}` when selected | Exact categorical filters |
| `frequency` | string | Selected observation cadence |
| `start`, `end` | string when selected | Reference-period bounds |
| `sort` | Sort[] | Stable ordered row coordinates |
| `offset`, `length` | string | Skipped rows and page length, echoed as text |
| `out` | string, `json` | Response encoding |
| `api_key` | string if echoed | Secret; replaced before retention |

Sort has `column: string` (row key) and `direction: string` (`asc`/`desc`). An observation's returned value must not be inferred from echoed selectors.

Source: [expected echo construction](../../../adapters/market-squawk-adapter-eia/src/request.rs).

## RouteMetadata

| Response member | Wire form | Local presence | Meaning |
|---|---|---|---|
| `id` | string | Optional; null rejected | Route identity |
| `name` | string | Optional; null rejected | Route display title |
| `description` | string | Optional; null rejected | Coverage/methodology explanation |
| `routes` | ChildRoute[] | Optional; null rejected | Navigable child routes |
| `frequency` | Frequency[] | Optional; null rejected | Supported cadence/period format definitions |
| `facets` | Facet[] | Optional; null rejected | Categorical dimensions available for filtering |
| `data` | `{dataField: DataColumn}` | Optional; null rejected | Measured-column dictionary keyed dynamically |
| `startPeriod`, `endPeriod` | string | Optional; null rejected | Available reference-period bounds |
| `defaultDateFormat` | string | Optional; null rejected | Default period grammar |
| `defaultFrequency` | string | Optional; null rejected | Default cadence |
| `sources` or `Sources` | string | Optional locally | Source attribution text, retained raw |
| Additional response fields | Bounded JSON | Retained as unmapped schema evidence | Potential drift, not arbitrary canonical properties |

## Metadata child objects

| Object | Field | Wire form / local presence | Meaning |
|---|---|---|---|
| ChildRoute | `id` | Required string | Next route path segment |
| ChildRoute | `name` | Optional string | Child-route title |
| ChildRoute | `description` | Optional string | Child-route coverage explanation |
| Frequency | `id` | Required string | Query frequency selector |
| Frequency | `format` | Required string | Grammar of `period` values |
| Frequency | `description` | Optional string | Cadence explanation |
| Frequency | `query` | Optional string | Provider's cadence query code |
| Facet | `id` | Required string | Categorical dimension key |
| Facet | `description` | Optional string | Dimension meaning |
| DataColumn | `alias` | Optional string | Alternative descriptive field label |
| DataColumn | `units` | Optional string | Column's measurement-unit metadata |

Nulls and undeclared members inside these local child schemas are rejected. Sibling IDs are deduplicated; route metadata is compared across generations so changed units/frequencies/facets/schema are not silently treated as unchanged data.

Source for route and child objects: [metadata parser](../../../adapters/market-squawk-adapter-eia/src/metadata.rs), official [metadata navigation documentation](https://www.eia.gov/opendata/documentation.php).

## FacetCatalog and FacetValue

| Object | Field | Provider form / local allowance | Meaning |
|---|---|---|---|
| FacetCatalog | `totalFacets` | Integer string in official example; integer number also accepted locally | Required total available facet members |
| FacetCatalog | `facets` | Required FacetValue[] | Categorical member catalog |
| FacetValue | `id` | Required string | Exact member token used in filters |
| FacetValue | `name` | Optional string; null rejected | Human member label |
| FacetValue | `alias` | Optional string; null rejected | Provider alternate member label |

The local catalog parser permits no other result/value members and requires `totalFacets == facets.length`, unique member IDs. There is no locally consumed facet continuation coordinate; an incomplete facet list is rejected.

Source: [facet parsing](../../../adapters/market-squawk-adapter-eia/src/metadata.rs).

## DataPage

| Field | Documented wire form | Local presence/allowance | Meaning |
|---|---|---|---|
| `total` | Integer string in official examples | Required; integer number also accepted | All matching **rows**, not scalar observations |
| `dateFormat` | string | Required, must match discovered frequency | Grammar of row `period` |
| `frequency` | string | Required, must match request | Observation cadence |
| `description` | string | Optional; null rejected | Result explanation |
| `data` | Row[] | Required | Returned data rows |

The local data response rejects extra members. JSON pages are bounded to at most 5,000 provider rows; offset is zero-based. Next offset is requested offset + returned rows; it is complete when equal to `total`. Nonterminal local pages must be full, and totals, API version, schema and stable ordering must agree across the chain. One row with several measured columns becomes several scalar observations, so those counts differ.

Source: [page parser/tracker](../../../adapters/market-squawk-adapter-eia/src/data.rs), official [pagination/data documentation](https://www.eia.gov/opendata/documentation.php).

## Dynamic Row object

The exact key set is frozen by route metadata plus the admitted dataset contract. Every expected key is required locally and unknown keys are rejected. Different routes have different field names; there is no universal `value` member.

| Key rule | Wire form | Meaning |
|---|---|---|
| `period` | string | Reference period, interpreted with `dateFormat` |
| Each requested data-field name | String in current documented v2 data returns; number accepted locally | Measured value; exact decimal or string according to field contract |
| `{dataField}-units` when row units selected | string | Unit of that measured value in this row |
| Each selected facet name | string | Exact categorical coordinate, validated against request and facet catalog |
| Each admitted descriptor key | string | Route-specific explanatory coordinate/label |
| Each explicitly mapped clock key | RFC 3339 string | Release, update or availability instant only if its actual field is admitted |

Missing-value policy is field-specific: null is missing only when explicitly allowed, and lexical markers are only the admitted marker set. Empty, zero and absent are not interchangeable. Units come from row units or a frozen fixed unit checked against metadata, not a guessed common energy unit.

Locally typed period grammars are `YYYY`, `YYYY-MM`, `YYYY-Q#` variants and `YYYY-MM-DD`; other declared formats remain provider text. A period is not a publication timestamp. Without mapped provider clocks, availability is first local receipt. Mapped clocks must not exceed receipt or contradict release/update order.

Source: [row fields, periods, values, units and clocks](../../../adapters/market-squawk-adapter-eia/src/data.rs).

## Errors and missing evidence

The official technical guide illustrates these additional root-level fields. They are provider examples, not a complete universal error schema.

| Response kind | Field | Documented JSON type | Meaning/local handling |
|---|---|---|---|
| Error | `error` | string | Explanation of invalid request; the success parser rejects its presence |
| Error | `code` | integer number | HTTP-style failure code, e.g. 400; transport handles HTTP status independently |
| Warning with data | `warning` | string | Qualification such as a page-length bound; retained as extra root evidence |
| Warning with data | `description` | string | Longer explanation; retained as extra root evidence |

Source: [official error/debugging examples and data-type changelog](https://www.eia.gov/opendata/documentation.php). Since v2.1.6 the guide says data values are strings; numeric decoding remains local compatibility breadth.

Transport handles HTTP failures/cooldown separately. No complete stable error schema, universal upstream required/null guarantee, all-route unit/marker enum, or historical vintage contract is established here. A discovered route alone does not prove its dataset contract is configured and producing.

Source: [transport](../../../adapters/market-squawk-adapter-eia/src/transport.rs), [error types](../../../adapters/market-squawk-adapter-eia/src/error.rs).
