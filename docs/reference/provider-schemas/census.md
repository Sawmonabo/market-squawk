# Census response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). Census observations are header-indexed JSON matrices; discovery objects define the dataset-specific columns. This covers every discovery and data response family consumed by the adapter. **Presence and alternate scalar forms below are local parser rules**, not provider-wide required/null guarantees. No untouched retained live response was inspected; example values are unverified.

## Endpoints and envelopes

`base` below means `https://api.census.gov/data/{year}/{dataset}` or `https://api.census.gov/data/timeseries/{dataset}`. Dataset path segments and geography/time predicates remain exact query coordinates.

| GET endpoint | Response shape | Purpose |
|---|---|---|
| `https://api.census.gov/data.json` | `{dataset: Dataset[]}` | Dataset catalog across vintages |
| `https://api.census.gov/data/{year}.json` | Same Dataset[] envelope | One vintage catalog |
| `{base}/variables.json` | `{variables: {variableName: Variable}}` | Variable dictionary |
| `{base}/groups.json` | `{groups: Group[]}` | Variable group dictionary |
| `{base}/groups/{group}.json` | Same variable-map envelope | Detailed group variable dictionary |
| `{base}/geography.json` | `{fips: Geography[]}` | Allowed geography hierarchy/predicates |
| `{base}?get=…&for=…&in=…` or `ucgid=…`, time/dataset predicates | Matrix `[[header…],[cell…],…]` | Selected observations |

Sources: [request builders](../../../adapters/market-squawk-adapter-census/src/query.rs), [discovery parser](../../../adapters/market-squawk-adapter-census/src/discovery.rs), official [API user guide](https://www.census.gov/data/developers/guidance/api-user-guide.html), [core concepts](https://www.census.gov/data/developers/guidance/api-user-guide.Core_Concepts.html).

## Dataset and Distribution

Catalog roots/entries may contain further federal catalog metadata; the parser projects the following members, not a closed universal catalog object.

| Dataset field | Typical wire type | Local presence/allowance | Meaning |
|---|---|---|---|
| `c_dataset` | string[] | Required | Ordered dataset path components |
| `c_vintage` | integer number | Year string or `timeseries` also accepted | Dataset vintage; omitted only for supported time-series/timeless catalog cases |
| `title` | string | Required for admitted datasets | Published dataset title |
| `description` | string | Required for admitted datasets | Subject/coverage explanation |
| `c_variablesLink` | string | Optional/null accepted | Official variable dictionary URL |
| `c_groupsLink` | string | Optional/null accepted | Official group dictionary URL |
| `c_geographyLink` | string | Optional/null accepted | Official geography dictionary URL |
| `c_isAvailable` | boolean | Optional/null accepted locally | Provider catalog availability flag, not local publication status |
| `c_isAggregate` | boolean | Optional/null accepted locally | Whether dataset reports aggregate geography |
| `c_isTimeseries` | boolean | Optional/null accepted locally | Whether dataset is a time series |
| `distribution` | Distribution[] | Optional; if present must be array | Access formats/locations |

Distribution projects `format: string` (optional/null accepted, with `API` selecting the API distribution) and `accessURL: string` (optional/null accepted, access location). Other distribution/catalog fields remain raw metadata, not observation columns. Unvintaged timeless entries can be retained as catalog evidence without minting an analytical dataset.

Source: [dataset parsing](../../../adapters/market-squawk-adapter-census/src/discovery.rs).

## Variable

Prefix: `$.variables[variableName]`; the dynamic key is the exact column/predicate identifier, not a user-specific selected value.

| Field | Typical wire type | Local presence/allowance | Meaning |
|---|---|---|---|
| `label` | string | Required | Detailed variable description |
| `concept` | string | Optional/null accepted | Survey concept grouping |
| `group` | string | Optional/null accepted; empty/`N/A` means no group locally | Variable-group selector |
| `predicateType` | string | Optional; unknown strings retained | Logical query/value type, not physical matrix-cell type |
| `required` | string or boolean | Optional/null accepted | Selection/predicate requirement; values described below |
| `predicateOnly` | boolean | Optional, interpreted when true | Restricts variable to filtering rather than `get` |
| `attributes` | comma-separated string | Array/null/omission also accepted locally | Companion annotation/attribute variable identifiers |
| `limit` | integer number | Optional; unsigned integer string/null also accepted locally | Provider's variable predicate limit |

Known predicate types are `string`, `int`, `float`, `fips-for`, `fips-in`, `ucgid`, `time`/`datetime`; absent, empty and `not a predicate` mean no predicate locally, while null is rejected. Other strings remain provider text. Known `required` strings are `predicate-only`, `required, predicate-only`, `default displayed`, `true`, `false`; booleans are also admitted. These describe **request rules**, not whether every output row has a nonnull value. An attribute dictionary is joined by metadata-declared relationship, not by guessing suffixes.

Source: [variable/required/attribute parsers](../../../adapters/market-squawk-adapter-census/src/discovery.rs), official [variables guidance](https://www.census.gov/data/developers/guidance/api-user-guide.Core_Concepts.html).

## Group and Geography

| Group field | Local type/presence | Meaning |
|---|---|---|
| `name` | Required string | Group selector |
| `description` | Required string | Group's survey/table description |
| `variables` | Required string | URL of group-specific variable definitions |

| Geography field | Typical wire type | Local presence/allowance | Meaning |
|---|---|---|---|
| `name` | string | Required | Geography level used in predicates/returned headers |
| `geoLevelDisplay` or `geoLevelId` | string | At least one; both must agree locally | Provider geography-level code |
| `requires` | string[] | Optional | Parent levels required to identify this level |
| `wildcard` | string[] or boolean | Optional | Array names permitted wildcard parents; boolean controls wildcard use for this level |
| `optionalWithWCFor` | string[] | Scalar string/null also accepted locally; optional | Parents that can be omitted with wildcard `for` |
| `referenceDate` | string | Optional/null accepted | Geography-definition reference `YYYY` or `YYYY-MM-DD` |

The reference date describes geography validity, not economic observation publication. Parent references must match discovered levels; wildcard permission does not imply a parent wildcard is valid everywhere.

Source: [group/geography parser](../../../adapters/market-squawk-adapter-census/src/discovery.rs).

## Data matrix and dynamic columns

The root has no `data` wrapper or pagination object. Row 0 is a nonempty array of unique string column names. Each following row has exactly that width; cell i belongs to header i. Geography and predicate context columns can be appended by the provider, so decode by header rather than positional guesses from the `get` request alone.

| Header/member category | Wire form | Meaning and local interpretation |
|---|---|---|
| Selected variable | Usually string; null supported for missing | Economic measure or category described by the Variable dictionary |
| Metadata-declared attribute column | Usually string/null | Annotation/qualification for a selected measure; preserve independently |
| Geography level, e.g. state/county | String | Geography code; retain leading zeros and hierarchy |
| `GEO_ID` | String | Fully qualified geographic identity |
| `NAME` | String | Geography display name |
| `time` | String | Provider reference period for time-series selection |
| Echoed predicate/default context | Dataset-specific scalar | Coordinates needed to distinguish observations |

The cell decoder also accepts finite numbers and booleans; that is parser breadth, not a provider guarantee that numeric measures arrive as JSON numbers. `predicateType=int` converts lexical values to integer; `float` converts to exact decimal; other logical types retain text. Units are dataset/variable specific and must not be inferred from JSON scalar type or all negative numbers.

Null, empty string, annotated values, provider-annotated missing, missing annotation columns and invalid typed values remain distinct local states. Sentinel/annotation meanings are dataset-specific; no universal Census null-code list is claimed. The parser checks header coverage, exact geography/predicate/time selection and unique family coordinates. One wire row may produce multiple variable observations.

Source: [streaming matrix, header and value parser](../../../adapters/market-squawk-adapter-census/src/response.rs), official [API format guidance](https://www.census.gov/data/developers/guidance/api-user-guide.Core_Concepts.html).

## Pagination, errors and missing evidence

These endpoints expose no consumed continuation token. Broad acquisitions partition explicit variable/geography/time selections and retain those request identities. A bounded matrix or wildcard query is not automatically proof of the complete upstream universe.

The HTTP client records status and `x-datawebapi-keyerror`; data decoding requires the successful matrix/discovery shape. Census failures can be textual/HTML rather than a universal JSON object. No stable complete HTTP-error schema was verified here. This reference establishes neither formal required/null guarantees across every catalog/dataset, provider release timestamps, historical vintages nor the meaning of each dataset's special-value annotations.

Source: [HTTP response handling](../../../adapters/market-squawk-adapter-census/src/http.rs), [source validation](../../../adapters/market-squawk-adapter-census/src/source.rs).
