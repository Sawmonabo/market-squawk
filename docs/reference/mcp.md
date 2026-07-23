# Local stdio MCP reference

This page specifies the production Model Context Protocol surface exposed by `market-squawk`: its
stdio transport, lifecycle, complete tool inventory, schemas, result publication, limits,
authorization, audit behavior, and error mapping.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Audience | MCP client authors, operators, security reviewers, and maintainers |
| Status | Current |
| Last substantive review | 2026-07-23 |
| Reviewed commit | `836aae662dfbbc3cf40e94e6da6c5c37cd3b57bd` |

## Contents

- [Scope](#scope)
- [Starting the server and framing](#starting-the-server-and-framing)
- [Initialization and advertised capabilities](#initialization-and-advertised-capabilities)
- [Requests, cancellation, and progress](#requests-cancellation-and-progress)
- [Tool contract](#tool-contract)
- [Complete tool inventory](#complete-tool-inventory)
- [Results and controlled artifacts](#results-and-controlled-artifacts)
- [Production limits](#production-limits)
- [Authority and durable audit](#authority-and-durable-audit)
- [Errors and session termination](#errors-and-session-termination)
- [Relationship to the CLI](#relationship-to-the-cli)
- [Related documentation and code](#related-documentation-and-code)
- [External sources](#external-sources)

## Scope

This reference covers the sole production MCP composition reached by `market-squawk mcp serve` and
the bare `market-squawk mcp` compatibility form. It does not describe the test-only diagnostic
service registry.

The reviewed server:

- uses inherited standard input and standard output only;
- advertises the MCP tools capability and exactly 59 application operations;
- does not advertise or serve MCP resources or prompts;
- forbids MCP task execution for every tool;
- does not expose an HTTP, WebSocket, or other network transport; and
- does not expose arbitrary SQL. The bounded DataFusion SQL surface is
  [CLI-only](cli.md#research-datasets-and-features).

The server is a local protocol boundary, not an identity provider. Inherited stdio is recorded as
an unverified process identity, and `confirm: true` records explicit mutation intent without
authenticating a person.

## Starting the server and framing

```text
market-squawk mcp serve
```

`market-squawk mcp` starts the same server for compatibility. The process reserves stdout for MCP
frames and uses the configured local control and artifact roots. A normal end of input initiates
bounded server shutdown, application shutdown, and audit flush.

The transport accepts one JSON-RPC message per line. The newline delimiter is not part of the
frame-size count. Whitespace-only lines are ignored. Production framing admits at most 1 MiB for
both the outer frame and the parsed JSON body. A frame larger than the outer limit ends the session
after a null-identity resource-limit response. A body that exceeds the body limit while still
fitting the outer frame receives a bounded resource-limit error and does not by itself require the
next valid frame to be discarded.

Only JSON-RPC integer request identifiers representable as `i64`, or string identifiers no longer
than 1,024 UTF-8 bytes, are admitted. The audit records a SHA-256 digest of the request identifier,
not its raw value.

## Initialization and advertised capabilities

The session has three states:

```text
await initialize -> await notifications/initialized -> ready
```

| Event | Behavior |
| --- | --- |
| `initialize` request | Accepted once; returns protocol version `2025-11-25`, server name `market-squawk`, package version, instructions, and the tools capability |
| Repeated `initialize` | Invalid request, `-32600` |
| `notifications/initialized` | Completes the handshake and makes tool operations ready |
| `tools/list` or `tools/call` before ready | Server initialization incomplete, `-32002` |
| `ping` after initialization | Returns the standard empty ping result |
| Unknown method | Method not found, `-32601` |

The negotiated capability surface is intentionally narrow:

| MCP feature | Reviewed-head behavior |
| --- | --- |
| Tools | Advertised; one complete, deterministic 59-tool list |
| Tool-list pagination | Unsupported; a non-null cursor is invalid parameters |
| Tool-list change notifications | Not advertised; the list is immutable for the process |
| Resources | Not advertised and no resource handlers are registered |
| Prompts | Not advertised and no prompt handlers are registered |
| Tool tasks | Explicitly `forbidden` on every tool |

The server instruction string also states that inherited stdio does not establish the peer's
identity. Clients must complete the standard MCP lifecycle before calling a tool.

## Requests, cancellation, and progress

At most eight request identities may be active. Reusing an active identity returns `-32009`;
exhausting the active-request or other retained-resource ceiling returns `-32010`. Each admitted
request receives a 30-second execution deadline.

A client cancels an active request with `notifications/cancelled` and its exact `requestId`. The
server marks the request cancelled, cancels its child token, and returns `-32800` if the call has
not already completed. Application and artifact publication work receive the same cancellation
token and stop at their next safe interruption point. Cancellation is not a rollback mechanism:
an authoritative mutation outcome is committed or rolled back according to the domain service's
atomicity, then audited independently from whether its response was delivered.

A tool call may carry `_meta.progressToken`. Integer tokens remain exact; string tokens are bounded
to 1,024 UTF-8 bytes. One request may emit at most 1,024 progress updates, and one progress message
may contain at most 1,024 UTF-8 bytes. Progress publication is queue- and deadline-bounded, closes
before the final response, and does not turn the call into an MCP task.

## Tool contract

### Closed request objects

Every tool has contract version `"1"` and an object input schema with
`additionalProperties: false`. The descriptor admits the same object again at runtime; JSON Schema
metadata is not the sole authority check.

Four scope profiles determine the common fields:

| Scope | `instrumentIds` | `timeRange` | `resultLimits` | `sourceCoverage` |
| --- | --- | --- | --- | --- |
| Local | Not accepted | Not accepted | Required | Not accepted |
| Source | Not accepted | Not accepted | Required | Optional |
| Data | Optional | Optional | Required | Optional |
| Portfolio | Optional | Optional | Required | Optional |

The common field types are:

| Field | Exact contract |
| --- | --- |
| `instrumentIds` | Nonempty unique array of at most 256 UUID strings |
| `timeRange` | Closed object with required RFC 3339 `start` and `end`; `start` must not be later than `end` |
| `resultLimits` | Closed object with required integer `maximumItems` in `1..=100000` and `maximumBytes` in `1..=268435456` |
| `sourceCoverage` | Nonempty unique array of at most 32 identifiers |
| Identifier | 1–256 ASCII bytes; only letters, digits, `-`, `_`, `.`, `:`, and `/` |
| Text | String whose trimmed UTF-8 value is nonempty and at most 4 KiB |
| Decimal | String of at most 128 bytes accepted by `rust_decimal::Decimal` |
| Timestamp | RFC 3339 string whose instant is representable at nanosecond precision |
| Object | Nonempty JSON object; the transport-wide structure limits still apply |

All mutations additionally require `"confirm": true`. Read tools reject `confirm`, because their
closed schemas do not contain it. Caller-supplied `resultLimits` are clamped to the MCP hard result
ceilings, so requesting 256 MiB does not raise the production MCP maximum above 64 MiB.

The outer MCP request object is independently bounded to depth 32, 64 KiB per JSON string or key,
10,000 items per array, 2,000 entries per map, and 1 MiB encoded body size.

### Descriptor metadata and effect annotations

Each `tools/list` entry includes:

- `inputSchema`, the closed schema described here;
- standard effect annotations;
- execution metadata declaring task support `forbidden`; and
- `_meta["org.market-squawk/tool-contract"]`, containing schema version, service domain,
  authorization class, scope requirements, source-evidence policy, and artifact policy.

Read tools are annotated read-only, non-destructive, idempotent, and closed-world. Mutations are
annotated destructive and closed-world. `FairValue.Measure`, `FairValue.Classify`,
`FairValue.Approve`, and `FairValue.ApproveMarketAccess` are the four idempotent mutations; the
other mutations are not idempotent. Annotations are client hints. The typed descriptor and domain
service enforce actual authority.

### Tool-specific argument types

The inventory tables use these compact argument forms in addition to the scope fields:

| Form | Fields |
| --- | --- |
| Dataset | Required `dataset` identifier |
| Optional dataset | Optional `dataset` identifier |
| Provider | Required `provider` identifier |
| Account | Required `accountId` identifier |
| Model input | Required `modelId` identifier and nonempty `input` object |
| Measurement | Required `measurementId` identifier |
| Confirmed mutation | Every listed field plus `confirm: true` |

`FairValue.Measure` uses the dedicated `measurement` object below. Its top level is closed and
requires:

| Field | Type |
| --- | --- |
| `accountId`, `instrumentId` | Non-nil UUIDs |
| `amount` | Decimal string, at most 128 bytes |
| `currency` | Exactly three ASCII letters |
| `scale` | Integer `0..=28` |
| `measurementAt`, `preparedAt` | Nanosecond-representable RFC 3339 timestamps |
| `preparedBy` | 1–128 UTF-8 bytes with no ASCII whitespace or control byte |
| `method` | `quoted_market_price`, `market_approach`, `income_approach`, or `cost_approach` |

At least one of `producerReceipts` or `producerSelections` must be present and nonempty; their
combined item count may not exceed 4,096.

- A receipt is a closed object with `producer` (`research`, `analytics`, or `portfolio`),
  `receiptId`, and `significance` (`significant` or `not_significant`).
- A live selection has `producer: "live"`, `venueId`, `selection` (`trade`, `bid`, or `ask`), and
  `significance`.
- A research or analytics selection has its producer, `datasetId`, integer `row` in
  `0..=999999`, and `significance`.
- A portfolio selection has only `producer: "portfolio"` and `significance`.

## Complete tool inventory

The production registry contains exactly 59 tools. “Read” means `read_only` authorization and
opaque artifact fallback on overflow. “Confirm” means local confirmation and inline-only result.
“Risk” means risk-mediated authorization, still with `confirm: true` and inline-only result.

### Source — 5 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Source.Register` | `provider` | Confirm | Register one code-supported provider capability in the local catalog |
| `Source.GetStatus` | None | Read | Return bounded configured and onboarding state for local providers |
| `Source.GetCoverage` | None | Read | Return explicit provider, venue, instrument, and delay coverage |
| `Source.GetHealth` | None | Read | Return bounded connection, integrity, and freshness health |
| `Source.Setup` | `provider` | Confirm | Start or resume capability-gated local provider onboarding |

All five use Source scope. `sourceCoverage`, when supplied to a read, filters the code-owned profile
surface identifiers and active runtime source identifiers.

### Market — 6 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Market.GetSnapshot` | None | Read | Return current bounded market state with explicit coverage and quality evidence |
| `Market.GetTrades` | Optional `dataset` | Read | Return bounded trade observations |
| `Market.GetQuotes` | Optional `dataset` | Read | Return bounded quote observations |
| `Market.GetBooks` | Optional `dataset` | Read | Return bounded order-book observations |
| `Market.GetQuality` | None | Read | Return source and instrument data-quality state |
| `Market.GetComparisons` | Optional `dataset` | Read | Compare bounded observations across requested sources |

All six use Data scope and require source-derived evidence in their result metadata.

### Research — 5 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Research.ListDatasets` | Optional `afterDataset` | Read | List immutable analytical dataset generations |
| `Research.GetManifest` | `dataset` | Read | Return one immutable analytical dataset manifest |
| `Research.GetHistory` | `dataset` | Read | Return bounded point-in-time observations and revisions |
| `Research.GetAlternativeData` | `dataset` | Read | Return bounded alternative-data observations from an immutable dataset |
| `Research.IngestSource` | `provider`, `object`, `dataset` | Confirm | Extract and ingest one configured provider object under retained rights authority |

The first two use Local scope and not-applicable source evidence. History and alternative data use
Data scope and require source evidence. Ingestion uses Source scope.

### Fundamental — 4 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Fundamental.GetFilings` | `dataset` | Read | Return bounded filing observations |
| `Fundamental.GetFacts` | `dataset` | Read | Return bounded reported fundamental facts |
| `Fundamental.GetStatements` | `dataset` | Read | Return bounded normalized financial-statement observations |
| `Fundamental.GetRatios` | `dataset` | Read | Return bounded point-in-time fundamental ratios |

All four use Data scope and require source evidence.

### Macro — 4 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Macro.ListSeries` | `dataset` | Read | List bounded macroeconomic series represented by a dataset |
| `Macro.GetObservations` | `dataset` | Read | Return bounded macroeconomic observations |
| `Macro.GetVintages` | `dataset` | Read | Return bounded point-in-time macroeconomic vintages |
| `Macro.GetRevisions` | `dataset` | Read | Return bounded macroeconomic revision history |

All four use Data scope and require source evidence.

### Portfolio — 6 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Portfolio.Import` | `accountId`, `artifactId` | Confirm | Import one controlled portfolio artifact and preserve reconciliation evidence |
| `Portfolio.GetHoldings` | `accountId` | Read | Return bounded current holdings under an exact revision |
| `Portfolio.GetTransactions` | `accountId` | Read | Return bounded normalized portfolio transactions |
| `Portfolio.GetPerformance` | `accountId` | Read | Return point-in-time portfolio performance |
| `Portfolio.GetExposure` | `accountId` | Read | Return point-in-time instrument, sector, factor, and currency exposure |
| `Portfolio.GetRisk` | `accountId` | Read | Return point-in-time portfolio risk and scenarios |

All six use Portfolio scope. The five reads require source evidence.

### Analysis — 7 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Analysis.GetReturns` | `dataset` | Read | Return bounded price and total returns |
| `Analysis.GetFactors` | `dataset` | Read | Return bounded factor estimates |
| `Analysis.GetValuation` | `dataset` | Read | Return bounded analytical valuation measures |
| `Analysis.GetScenarios` | `dataset` | Read | Return bounded scenario and stress-analysis outputs |
| `Analysis.GetFeatureDatasets` | Optional `dataset` | Read | Return registered feature contracts and immutable feature datasets |
| `Analysis.GetBacktests` | `runId` | Read | Return governed backtest experiment metadata and results |
| `Analysis.RunBacktest` | Nonempty `registration` object | Confirm | Run one governed point-in-time backtest experiment |

The first five use Data scope and require source evidence. Backtest lookup and execution use Local
scope and not-applicable source evidence.

### Model — 4 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Model.GetMetadata` | `modelId` | Read | Return complete admitted model metadata and validation evidence |
| `Model.ListBundles` | None | Read | List admitted immutable model bundle generations |
| `Model.Evaluate` | `modelId`, nonempty `input` object | Confirm | Evaluate an admitted local model and retain bounded evaluation evidence |
| `Model.Predict` | `modelId`, nonempty `input` object | Read | Run bounded local inference; every failure produces no automated action |

All four use Local scope and not-applicable source evidence.

### Fair value — 10 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `FairValue.ListMeasurements` | None | Read | List bounded immutable fair-value measurements |
| `FairValue.GetClassification` | `measurementId` | Read | Return one measurement classification and ruleset identity |
| `FairValue.Explain` | `measurementId` | Read | Explain one evidence-bound hierarchy classification |
| `FairValue.GetEvidence` | `measurementId` | Read | Return bounded evidence linked to one measurement |
| `FairValue.GetApprovalStatus` | `measurementId`, `at` | Read | Return approval and revocation status at the requested instant |
| `FairValue.Measure` | Dedicated `measurement` object | Confirm, idempotent | Create one immutable measurement from admitted evidence |
| `FairValue.Classify` | `measurementId` | Confirm, idempotent | Classify one immutable measurement with the code-owned hierarchy ruleset |
| `FairValue.Approve` | `measurementId`, `decisionId`, `approvedBy`, `approvedAt`, `expiresAt` | Confirm, idempotent | Approve an eligible measurement through controlled review |
| `FairValue.ApproveMarketAccess` | Fields below | Confirm, idempotent | Create or supersede a dual-approved account, venue, and instrument access assessment |
| `FairValue.GetMarketAccess` | `assessmentId` | Read | Return one immutable dual-approved market-access assessment |

`FairValue.ApproveMarketAccess` requires `accountId`, `venueId`, and `instrumentId` identifiers;
`conclusion` equal to `accessible` or `inaccessible`; `effectiveFrom`, `effectiveUntil`,
`preparedAt`, and `approvedAt` timestamps; `rationale` text; and `preparedBy` and `approvedBy`
identifiers. All fair-value tools use Local scope and not-applicable source evidence.

### Bot — 3 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Bot.GetStatus` | None | Read | Return controlled paper-operation lifecycle and risk status |
| `Bot.Start` | `provider`, `initialCash`, `feeBasisPoints` | Confirm | Start an explicitly configured local paper operation |
| `Bot.Stop` | `reason` | Confirm | Stop the current local paper operation and durably reconcile it |

`Bot.Start.provider` is `coinbase` or `kraken`; `initialCash` is a decimal string; and
`feeBasisPoints` passes the current application descriptor when it is an integer in `0..=100000`,
but the paper runtime has the stricter effective limit `0..=10000`. A larger admitted value cannot
start the runtime and is reported as service unavailable. This contract mismatch is release-
blocking and tracked in the [delivery ledger](../plans/delivery-ledger.md); callers must use
`0..=10000`. All bot tools use Local scope.

### Execution — 4 tools

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Execution.GetOrders` | None | Read | Return bounded paper orders and state transitions |
| `Execution.GetFills` | None | Read | Return bounded paper fills |
| `Execution.Cancel` | `orderId` | Risk | Cancel one tracked paper order through dispatcher-owned authority |
| `Execution.Reconcile` | None | Risk | Reconcile paper orders, fills, balances, and positions through the dispatcher |

All four use Local scope. The two reads have not-applicable source evidence.

### Risk — 1 tool

| Tool | Specific arguments | Effect | Purpose |
| --- | --- | --- | --- |
| `Risk.TriggerKillSwitch` | `reason` | Confirm | Stop only the current local paper operation with an explicit reason |

This operation uses Local scope. Its descriptor belongs to the Bot service domain even though its
public operation name has the `Risk` prefix.

## Results and controlled artifacts

Successful tool calls place no text blocks in `content`; their machine result is
`structuredContent`. An inline result has this exact envelope:

```json
{
  "data": null,
  "metadata": {
    "completeness": "complete",
    "returnedItems": 0,
    "availableItems": 0,
    "sourceCoverage": {"status": "not_applicable"},
    "dataQuality": {"status": "not_applicable"}
  }
}
```

The example illustrates shape, not a specific operation's value. `completeness` is `complete` or
`truncated`. A truncated result must report `availableItems` greater than `returnedItems`.
Source-derived operations return bounded nonempty coverage and quality evidence; local operations
return the explicit not-applicable objects shown above. Each evidence value is limited to 8 KiB,
depth 8, 4 KiB strings, and 256 array items or object entries.

Read descriptors use `opaque_on_overflow`. If a valid read result exceeds either 64 KiB inline
bytes or 1,000 inline logical items, but remains within the 64 MiB and 1,000,000-item hard MCP
ceilings, the server publishes the complete canonical JSON envelope as a controlled artifact and
returns:

```json
{
  "artifact": {
    "id": "mcp-<64-lowercase-hex-digest>",
    "sha256": "<64-lowercase-hex-digest>",
    "byteCount": 1234,
    "mediaType": "application/json"
  },
  "metadata": {
    "completeness": "complete",
    "returnedItems": 1,
    "availableItems": 1,
    "sourceCoverage": {"status": "not_applicable"},
    "dataQuality": {"status": "not_applicable"}
  }
}
```

The artifact is SHA-256 content-addressed, capped at 64 MiB, staged through private no-follow files,
durably synchronized, re-read, and digest-verified before publication. The example illustrates a
valid local-result shape; a source-derived artifact carries actual coverage and quality evidence.
The reference ID begins with an ASCII alphanumeric byte, contains only ASCII alphanumerics, `_`,
and `-`, and is at most 160 bytes. The digest is exactly 64 lowercase hexadecimal characters,
`byteCount` is positive, and the media type is a bounded 1–128-byte token. The reference is
path-free and must be treated as opaque. The reviewed server exposes no MCP resource or tool that
retrieves an artifact by this reference; clients cannot translate it into a filesystem path or
`resources/read` URI.

Mutation descriptors are `inline_only`. A mutation result that cannot fit inline fails with
`-32010`; it is never silently replaced by an artifact reference.

## Production limits

The production composition constructs `McpLimitSpec::default()`; there is no public MCP command-line
option that changes these values.

| Resource | Production value |
| --- | ---: |
| Newline-delimited frame, excluding delimiter | 1 MiB |
| Parsed JSON body | 1 MiB |
| JSON nesting depth | 32 |
| One JSON string or key | 64 KiB |
| Items in one JSON array | 10,000 |
| Entries in one JSON object | 2,000 |
| Active request identities | 8 |
| Queued writer messages | 64 |
| Queued writer bytes | 8 MiB |
| Inline result bytes | 64 KiB |
| Inline logical result items | 1,000 |
| Hard result or artifact-bound bytes | 64 MiB |
| Hard logical result items | 1,000,000 |
| Progress updates per request | 1,024 |
| Progress message | 1,024 bytes |
| String progress token | 1,024 bytes |
| Request execution deadline | 30 seconds |
| Queue/write deadline | 5 seconds |
| SDK, writer, and MCP shutdown deadline | 5 seconds |

The limits type also refuses configurations above implementation ceilings: 64 MiB frame/body,
depth 64, 16 MiB string, 1,000,000 array items, 100,000 map entries, 4,096 active requests and
writer messages, 256 MiB queued bytes, 16 MiB/100,000 inline result, 256 MiB/10,000,000 hard result,
100,000 progress updates, 64 KiB progress message/token, and 24-hour timeouts. Cross-field checks
require body no larger than frame, the largest response and progress frame to fit the writer
budget, and aggregate active body and result reservations each to remain at or below 512 MiB.
These are constructor safety bounds, not production operator settings.

## Authority and durable audit

The MCP adapter receives the same lifecycle-owned `Application` instance as the local product. A
tool name resolves to one immutable descriptor; the descriptor admits scope, schema,
authorization class, result policy, and source-evidence policy before a domain service executes.
Application services retain source, rights, model, valuation, portfolio, risk, and execution
authority. MCP does not mint an alternate route around those checks.

Every mutation requires `confirm: true`. `Execution.Cancel` and `Execution.Reconcile` additionally
use risk-mediated authorization. Confirmation, tool annotations, and an inherited process
relationship do not prove a human identity or satisfy downstream financial authority.

The production audit file is `mcp-audit.jsonl` in the control root. It is opened exclusively as a
private, one-link, no-follow regular file. Recovery may truncate only an incomplete final record;
a corrupt complete record, unsafe file identity, or unavailable audit sink fails closed.

Audit records are payload-free. They retain bounded phase and result classifications, a timestamp,
the honest unverified peer class, a known bounded operation name, admitted service limits,
SHA-256 digests of the request identity and request/response content, and no raw arguments or
response payload.

The phases are:

| Phase | Meaning |
| --- | --- |
| `admitted` | The outer request passed framing, identity, and resource admission |
| `mutation_admitted` | Durable intent was recorded before dispatching a mutation |
| `mutation_service_completed` | The authoritative mutation outcome was recorded independently of response delivery |
| `completed` | Protocol delivery reached its terminal classified outcome |

Result classes are `succeeded`, `artifact_published`, `protocol_rejected`, `service_rejected`,
`cancelled`, `deadline_exceeded`, `resource_exhausted`, and `output_unavailable`. Mutations reserve
three audit records so admission, authoritative service completion, and response delivery remain
distinct. A session may append at most 16,384 records; each encoded record is at most 8 KiB, and
the file ceiling is 134,234,112 bytes. Audit exhaustion or write failure prevents unrecorded work
from proceeding.

## Errors and session termination

| Code | Classification and stable message |
| ---: | --- |
| `-32700` | JSON parse error: `parse error` |
| `-32600` | Invalid JSON-RPC request, repeated initialization, or invalid request identifier |
| `-32601` | Unknown method, unavailable capability, or service operation not found |
| `-32602` | Invalid tool/list parameters: `service request is invalid` or the bounded-list cursor diagnostic |
| `-32001` | Service unavailable: `service is unavailable` |
| `-32002` | Lifecycle gate: `server initialization is not complete` |
| `-32003` | Authorization failure: `service request is not authorized` |
| `-32008` | Deadline: `request deadline exceeded` |
| `-32009` | Duplicate active request identity: `duplicate active request identifier` |
| `-32010` | Frame, body, concurrency, progress, writer, or service-result resource ceiling |
| `-32800` | Cancellation: `request was cancelled` |
| `-32603` | Internal failure; artifact admission/publication reports `artifact publication failed` |

Error data does not disclose provider secrets, internal paths, or dynamic authority detail.

End of input, explicit process cancellation, peer closure, bounded write timeout, rejected input,
or audit failure drives supervised session shutdown. The application then receives its own bounded
shutdown and the audit is flushed. A failure in server termination, application termination, or
audit finalization makes the command fail rather than reporting a clean exit.

## Relationship to the CLI

MCP and mapped CLI commands call the same immutable operation descriptors and the same
lifecycle-owned application services. They therefore share the domain schemas and authority
checks described above. They are different transports: CLI result ceilings, filesystem admission,
provider activation, dataset building, initialization, and bounded SQL remain CLI-owned surfaces
where documented.

CLI subcommands and MCP tools are not one-to-one. In particular, MCP has no arbitrary SQL tool, no
filesystem-path import tool, no provider activation request-file command, and no direct
artifact-read resource.

## Related documentation and code

- [CLI reference](cli.md)
- [Configuration reference](configuration.md)
- [Source coverage reference](source-coverage.md)
- [Data quality reference](data-quality.md)
- [Control-plane architecture](../architecture/control-plane.md)
- [Security and trust boundaries](../architecture/security-and-trust-boundaries.md)
- [Local deployment architecture](../architecture/deployment.md)
- [Installation and bootstrap operations](../operations/installation-and-bootstrap.md)
- [Configuration and secrets operations](../operations/configuration-and-secrets.md)
- [Troubleshooting](../operations/troubleshooting.md)
- [Production MCP composition](../../apps/market-squawk/src/mcp.rs)
- [Application descriptor registry](../../apps/market-squawk/src/application/contracts.rs)
- [Shared application composition](../../apps/market-squawk/src/application.rs)
- [MCP server](../../crates/market-squawk-mcp/src/server.rs)
- [MCP framing](../../crates/market-squawk-mcp/src/framing.rs)
- [MCP limits](../../crates/market-squawk-mcp/src/limits.rs)
- [Controlled artifact publication](../../crates/market-squawk-mcp/src/artifact.rs)
- [Durable MCP audit](../../apps/market-squawk/src/mcp/audit.rs)
- [Accepted-head delivery evidence](../plans/delivery-ledger.md)

## External sources

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [MCP lifecycle specification, 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle) | Initialize negotiation and initialized-notification ordering | 2026-07-23 |
| [MCP stdio transport specification, 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#stdio) | Newline-delimited JSON-RPC over inherited stdin/stdout | 2026-07-23 |
| [MCP tools specification, 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) | Tool listing, invocation, schemas, annotations, structured content, and task-support fields | 2026-07-23 |
| [MCP cancellation utility, 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation) | Standard cancellation notification and race semantics | 2026-07-23 |
| [MCP progress utility, 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/progress) | Progress-token and notification semantics | 2026-07-23 |

The MCP specification defines interoperable protocol semantics. The reviewed Market Squawk code
head remains authoritative for the advertised capability subset, exact tool schemas, limits,
artifact behavior, audit phases, and domain authority.
