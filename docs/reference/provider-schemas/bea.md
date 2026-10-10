# BEA response schemas

Reviewed 2026-10-09 America/New_York (2026-10-10 UTC). BEA is a metadata-driven API: dataset dimensions define observation keys. This reference describes every method consumed by the adapter without copying the selected dataset inventory. **Local presence and parser allowances are not upstream wire guarantees.** No untouched retained live example was inspected; example values remain unverified.

## Methods and envelope

All operations are GET `https://apps.bea.gov/api/data`, with `ResultFormat=JSON`, a protected `UserID`, `method`, and method-specific selectors. Response requests echo the credential; the adapter validates then redacts it before retention.

| Method | Selector | `BEAAPI.Results` payload |
|---|---|---|
| `GetDatasetList` | No dataset selector | `Dataset: DatasetDefinition[]` |
| `GetParameterList` | `DatasetName` | `Parameter: ParameterDefinition[]` |
| `GetParameterValues` | Dataset and `ParameterName` | `ParamValue: ParameterValue[]` |
| `GetParameterValuesFiltered` | `DatasetName`, `TargetParameter`, supplied filtering parameters | `ParamValue: ParameterValue[]` |
| `GetData` | Dataset and metadata-admitted selection parameters | Dimensions, data, notes and result attributes |

Sources: [query methods](../../../adapters/market-squawk-adapter-bea/src/query.rs), [parser](../../../adapters/market-squawk-adapter-bea/src/parser.rs), official [BEA API guide, April 20, 2026](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf), pp. 2–21. The guide describes the methods and gives JSON examples; the presence rules below are the exact local acceptance boundary, not a complete upstream required/null schema.

| Object/path | Field | Local type/presence | Meaning |
|---|---|---|---|
| `$` | `BEAAPI` | Required object | API response container |
| `BEAAPI` | `Request` | Required object | Echo of parameters actually processed |
| `BEAAPI` | `Results` | Required object; singleton object-array also accepted | Method-specific results or error |
| `Request` | `RequestParam` | Required ParameterEcho[] | Echoed parameter collection |
| ParameterEcho | `ParameterName` | Required string | Parameter name; local comparison ignores ASCII case |
| ParameterEcho | `ParameterValue` | Required string | Parameter text; `USERID` is secret and redacted |

Outer envelope/echo objects reject extra fields. Inner method result keys are matched case-insensitively and reject duplicate case variants. `Results` singleton-array support is a parser allowance, not a universal documented provider representation.

## Metadata objects

| DatasetDefinition field | Local type/presence | Meaning |
|---|---|---|
| `DatasetName` | Required string | Dataset selector used in subsequent requests |
| `DatasetDescription` | Required string | Subject and coverage description |

| ParameterDefinition field | Local type/presence | Meaning |
|---|---|---|
| `ParameterName` | Required string | Dataset-specific selector name |
| `ParameterDataType` | Required string; `string`/`integer` | Declared input value class |
| `ParameterDescription` | Required string | Selector's economic or structural meaning |
| `ParameterIsRequiredFlag` or `ParameterIsRequired` | Exactly one required; `0`/`1` string or number locally | Whether selection must supply this parameter |
| `MultipleAcceptedFlag` or `MultipleAccepted` | Exactly one required; `0`/`1` string or number locally | Whether multiple selected values are accepted |
| `ParameterDefaultValue` | Optional string; null rejected locally | Default selector text when omitted |
| `AllValue` | Optional string; null rejected locally | Provider's token for all values |

Official JSON examples serialize parameter flags as strings. The local flag parser converts JSON scalars to text, then accepts only `0` or `1`; that does not imply BEA sends JSON booleans. Dataset and parameter definitions reject undeclared fields.

| ParameterValue field | Local type/presence | Meaning |
|---|---|---|
| `Key` | Required scalar; ordinarily string, number also accepted | Selectable parameter coordinate |
| `Desc` or `Description` | Optional string, at most one | Label explaining that coordinate |
| Dataset-specific additional keys | Scalar only locally | Further metadata attributes, retained by name; arrays/objects rejected |

Additional scalar allowances include boolean and null converted to lexical text; this is **parser breadth**, not established provider wire typing. Actual selector meaning belongs to the returned dataset metadata.

Source for these objects: [metadata parsing](../../../adapters/market-squawk-adapter-bea/src/parser.rs), [official method descriptions/examples](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf), pp. 6–15. The guide specifies both `Key` and `Desc` for filtered parameter results; this parser also accepts unlabelled parameter values.

## GetData results, Dimension and Note

| Result field | Local type/presence | Meaning |
|---|---|---|
| `Dimensions` | Required nonempty Dimension[] | Schema of each observation's coordinates and value |
| `Data` | Required Observation[] | Economic observations for the selection |
| `Notes` | Optional Note[]; null rejected | Reference-indexed explanations |
| `NoteRef` | Optional string; null rejected | Result-wide note references separated by commas/whitespace |
| `UTCProductionTime` | Optional string; null rejected | UTC response-production time; locally `YYYY-MM-DDTHH:MM:SS[.fraction]` without zone suffix |
| Additional result attributes | Scalar only locally | Dataset-specific result metadata |

| Dimension field | Local type/presence | Meaning |
|---|---|---|
| `Name` | Required string | Exact corresponding observation member name |
| `Ordinal` | Optional positive integer/string locally | Ordering position; all dimensions must either supply it or omit it |
| `DataType` | Required string; `string`/`numeric` | Declared dimension/value class |
| `IsValue` | Required `0`/`1` string or number locally | Marks the single measured-value dimension |

Exactly one dimension must have `IsValue=1`. The local macro mapper additionally requires non-value `TimePeriod` and `CL_UNIT` string dimensions and numeric `UNIT_MULT`. Dimension definitions reject extra fields. Notes have required `NoteRef: string` (reference key) and `NoteText: string` (nonempty explanation); every result/row note reference must resolve to a unique returned Note.

Source: [dimensions, notes and data parser](../../../adapters/market-squawk-adapter-bea/src/parser.rs).

## Dynamic Observation object

There is no single universal list of BEA observation keys. Official examples use string cells, including numeric dimensions and values. Locally, every non-value `Dimensions[].Name` is required; the measured-value member can be missing under the rule below. Row members are matched case-insensitively with duplicate case variants rejected; optional `NoteRef` is the only additional admitted member. Geography, table, line and series dimensions remain native coordinates.

| Member rule | Provider wire meaning | Local acceptance |
|---|---|---|
| Dimension with `IsValue=1` | Measured amount, commonly `DataValue` numeric text | Decimal string or number; absent/null = missing, blank string = blank missing; Regional `L` = suppressed |
| `TimePeriod` dimension | Reference year, quarter or month | String `YYYY`, `YYYYQ1`–`YYYYQ4`, `YYYYM1`–`YYYYM12` |
| `CL_UNIT` dimension | Measurement unit description/classification | Required nonblank scalar text locally |
| `UNIT_MULT` dimension | Decimal power-of-ten scale for the measure | Required scalar text parseable as signed integer; interpret with `CL_UNIT` |
| Other non-value dimensions | Dataset-specific identity/context | Required scalar; string/number/boolean/null are lexically retained locally |
| `NoteRef` | Row qualifications | Optional string; each token must resolve |

Grouped numeric strings are parsed exactly after validating comma grouping. Suppression is not zero. The adapter does not universally support every BEA dataset merely because discovery lists it: its data mapper requires the dimensions above and supported period grammar. Production time is response generation, not necessarily historical first public availability.

## Continuation and errors

BEA returns one result per selection here; no provider cursor, offset or page token is consumed. Adapter page-number/count evidence describes application-planned query partitions and optional expected-row counts, not BEA pagination fields.

`Results.Error` contains `APIErrorCode` (string in official examples; integer/string accepted locally, parsed as `u32`) and `APIErrorDescription: string`. It must be the only result field; extra error members are rejected. Code 34 on filtered parameter lookup is classified as unsupported filtered lookup. The official guide documents throttling as HTTP 429 with `Retry-After` seconds and this same error envelope. HTTP failures are separately handled by [transport](../../../adapters/market-squawk-adapter-bea/src/transport.rs). Source: [official errors](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf), pp. 4–5, 15.

Missing evidence: a formal complete method/dataset-specific required/null/type schema, raw successful/error captures for every admitted dataset, and a universal vintage/publication-clock contract. The inspected guide's examples establish native response forms, not immutable live values or universal scalar/null emission guarantees.
