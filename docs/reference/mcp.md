# Shared MCP reference

Market Squawk provides one authenticated, stateless Streamable HTTP MCP service in the installed
per-user runtime. Claude Code and Codex use distinct credentials and isolated request/handle state
through the installed stdio relay; they do not start their own product server or share prompt
history. The Desktop is a separate `/app/v1` client and does not use MCP as its internal API.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Status | Current implementation contract |
| MCP version | `2026-07-28` only |
| Last substantive review | 2026-08-03 |
| Authority | `crates/market-squawk-mcp` and installed service composition |

## Endpoint, registration, and lifecycle

The service binds only to `127.0.0.1` and mounts MCP at `/mcp`. Its authenticated owner-only
rendezvous records the current endpoint, service/installation/workspace identities, supported
application protocol range, and process-start identity. It contains no credential. A stale,
retired, unauthenticated, or conflicting rendezvous is a repair/startup condition; clients must not
guess a port or construct a replacement endpoint.

The service accepts only `POST`. It rejects standalone `GET`/`DELETE`, `Mcp-Session-Id`, and
`Last-Event-ID`; it never mints a session ID or retains session state. It requires one exact Host
matching the published loopback authority, rejects an unknown/present browser Origin, accepts no
cookie authority, and requires one `Authorization: Bearer <token>` header. Missing Origin is valid
for native clients. A bearer token is at most 4 KiB and cannot contain controls. The service accepts
JSON request bodies and serves JSON responses; the relay advertises `application/json,
text/event-stream` as acceptable response media.

Do not manually place the endpoint or token in Claude Code/Codex configuration. Setup/repair
discovers each supported client and creates at most one owned user-level registration named
`market-squawk`; it preserves unrelated configuration and refuses an unowned same-name conflict.
The normal registered command is:

```text
market-squawk-mcp-relay --client <claude|codex>
```

The relay obtains the named client credential through protected native storage, resolves the live
rendezvous, applies Host/Origin/bearer headers internally, and forwards bounded stdio JSON-RPC. It
does no product work and exits without stopping the service. The compatibility command
`market-squawk mcp serve --client <claude-code|codex>` runs the same relay. Bare `market-squawk mcp`
is not a server and fails because `--client` is required.

## Initialization, concurrency, cancellation, and limits

Every HTTP request is independently authenticated and gets a fresh stateless handler. The handler
advertises the admitted `2026-07-28` protocol, tools, resources, and resource templates. It accepts
the normal MCP initialize/initialized lifecycle for each relay process, but no HTTP connection,
prior request, or client conversation conveys product authority.

The service has a global ceiling of eight concurrent MCP requests and each named client has a
separate ceiling of four. Global saturation returns HTTP `503`; named-client saturation returns
HTTP `429`. A request deadline is 30 seconds. Cancellation propagates to the service request and
publication work, but it does not roll back an already atomically committed mutation. Disconnect
and response completion release the applicable permits.

| Resource | Production value |
| --- | ---: |
| HTTP JSON body / relay JSON-RPC frame | 1 MiB |
| JSON depth | 32 |
| String or key | 64 KiB |
| Array items / object entries | 10,000 / 2,000 |
| Global active requests | 8 |
| Per Claude Code or Codex client | 4 |
| Inline result | 64 KiB or 1,000 logical items |
| Complete result/artifact | 64 MiB or 1,000,000 logical items |
| Artifact chunk | 32 KiB |
| Request deadline | 30 seconds |
| Progress updates per request | 1,024 |
| Progress message / string token | 1,024 UTF-8 bytes |
| Relay writer queue | 64 messages / 8 MiB |
| Relay queue/write/shutdown deadline | 5 seconds |

Tool-list, resource-list, and resource-template pagination reject a non-null cursor. A client may
send a standard cancellation notification with its exact request identity. Invalid, cancelled,
expired, over-limit, unavailable, or unauthorised operations fail closed and do not reveal paths,
credentials, or raw provider state.

## Closed tool schemas

Every advertised tool has contract version `"1"`, an object input schema with
`additionalProperties: false`, an output schema, effect annotations, and
`_meta["org.market-squawk/tool-contract"]`. The descriptor is runtime admission authority as well
as discovery metadata. `tools/list` is therefore the machine-readable exact schema for every
individual operation on the installed version; clients must use it rather than inventing optional
fields or carrying schemas from another release.

The shared admitted scalar forms are:

| Form | Exact admission |
| --- | --- |
| Identifier | `1..=256` ASCII bytes from letters, digits, `-`, `_`, `.`, `:`, `/`. |
| Text | Trimmed nonempty UTF-8, at most 4 KiB. |
| SHA-256 | Exactly 64 lowercase hexadecimal characters. |
| Decimal | `rust_decimal::Decimal` string, at most 128 bytes. |
| Timestamp | RFC 3339 instant representable at nanosecond precision. |
| UUID | Non-nil UUID string. |
| Object | Nonempty JSON object subject to transport structure limits. |
| `instrumentIds` | Optional nonempty unique UUID array, at most 256. |
| `timeRange` | Optional closed `{start,end}` RFC 3339 object with `start <= end`. |
| `sourceCoverage` | Optional/required by tool scope; unique nonempty identifier array, at most 32. |
| `resultLimits` | Required where the descriptor scope says so: closed `{maximumItems,maximumBytes}` with each in `1..=100000` and `1..=268435456`. MCP clamps it to the 1,000,000-item / 64-MiB MCP ceiling. |

Read-only descriptors do not accept `confirm`. Every mutation has a required `confirm: true` field;
confirmation is explicit local intent, not human authentication or financial approval. Risk-mediated
`Execution.Cancel` and `Execution.Reconcile` remain subject to dispatcher/risk authority.

Tool request scopes are closed: Local has required `resultLimits`; Source additionally permits
`sourceCoverage`; Source discovery requires `sourceCoverage`; Data and Portfolio permit optional
instrument/time/source fields and require `resultLimits`; Job has no common result scope. Tool
specific required fields and enums are emitted in the descriptor schema and are enforced again at
runtime.

## Complete tool inventory

The following names are the complete current registry. A `Start*` tool creates a durable job and
returns its typed receipt; `Research.IngestSource` and `Analysis.RunBacktest` are the bounded
compatibility wait forms. No inventory entry grants raw SQL, raw filesystem, generic network,
arbitrary shell, credential, audit-deletion, remote-code, direct-order, or risk-bypass authority.

| Domain | Exact tool names |
| --- | --- |
| Job | `Job.List`, `Job.Get`, `Job.Watch`, `Job.Cancel`, `Job.Confirm`, `Job.Retry` |
| Source | `Source.Register`, `Source.GetStatus`, `Source.GetCoverage`, `Source.GetHealth`, `Source.Setup`, `Source.ListObjects`, `Source.Inspect`, `Source.Discover`, `Source.Start`, `Source.Stop`, `Source.Retry`, `Source.Resynchronize`, `Source.Verify`, `Source.Reconfigure`, `Source.Remove` |
| Market | `Market.GetSnapshot`, `Market.GetTrades`, `Market.GetQuotes`, `Market.GetBooks`, `Market.GetQuality`, `Market.GetComparisons` |
| Research | `Research.ListDatasets`, `Research.GetManifest`, `Research.GetHistory`, `Research.GetAlternativeData`, `Research.StartIngestSource`, `Research.IngestSource`, `Research.StartDatasetBuild`, `Research.StartExport` |
| Fundamental | `Fundamental.GetFilings`, `Fundamental.GetFacts`, `Fundamental.GetStatements`, `Fundamental.GetRatios` |
| Macro | `Macro.ListSeries`, `Macro.GetObservations`, `Macro.GetVintages`, `Macro.GetRevisions` |
| Portfolio | `Portfolio.Import`, `Portfolio.PreviewStagedImport`, `Portfolio.ApproveStagedImport`, `Portfolio.CommitStagedImport`, `Portfolio.DiscardStagedImport`, `Portfolio.ListAccounts`, `Portfolio.ListRevisions`, `Portfolio.GetHoldings`, `Portfolio.GetTransactions`, `Portfolio.GetPerformance`, `Portfolio.GetExposure`, `Portfolio.GetRisk`, `Portfolio.GetAttribution`, `Portfolio.EvaluateScenario`, `Portfolio.EvaluateScenarioBatch`, `Portfolio.ProposeRebalance`, `Portfolio.EvaluateCandidateImpact` |
| Analysis | `Analysis.GetReturns`, `Analysis.Lookup`, `Analysis.GetDecisionOverview`, `Analysis.GetFactors`, `Analysis.GetValuation`, `Analysis.GetScenarios`, `Analysis.StartScenarioBatch`, `Analysis.GetFeatureDatasets`, `Analysis.StartFeatureDatasetBuild`, `Analysis.GetBacktests`, `Analysis.StartBacktest`, `Analysis.RunBacktest`, `Analysis.ReadArtifact` |
| Model | `Model.GetMetadata`, `Model.ListBundles`, `Model.Evaluate`, `Model.Predict`, `Model.StartTraining`, `Model.StartForecast`, `Model.GenerateForecast`, `Model.GetForecast`, `Model.ListForecasts`, `Model.GetForecastOutcomes` |
| Decision | `Decision.SaveScreen`, `Decision.RunScreen`, `Decision.ListScreens`, `Decision.ListScreenRuns`, `Decision.GetCandidates`, `Decision.GetDossierPreparation`, `Decision.PrepareDossier`, `Decision.CreateDossier`, `Decision.GetDossier`, `Decision.ListCandidateDossiers`, `Decision.GetTargetPreparation`, `Decision.PrepareTargetSet`, `Decision.CreateTargetSet`, `Decision.GetTargetSet`, `Decision.ListTargetSets`, `Decision.ListTargetIndex`, `Decision.ReviewTargetSet`, `Decision.ReevaluateTargetSet`, `Decision.GetTargetSetStatus` |
| Operations | `Operations.ListBackups`, `Operations.GetBackup`, `Operations.StartBackup`, `Operations.StartBackupVerification`, `Operations.PreviewBackupRetention`, `Operations.StartBackupRetention`, `Operations.PreviewRestore`, `Operations.StartRestore`, `Operations.ListWorkspaces`, `Operations.PreviewWorkspaceSwitch`, `Operations.StartWorkspaceSwitch`, `Operations.GetUpdateStatus`, `Operations.CheckForUpdates`, `Operations.PreviewUpdate`, `Operations.StartUpdate`, `Operations.PreviewProgramRollback`, `Operations.StartProgramRollback`, `Operations.QueryLogs`, `Operations.ExportLogs`, `Operations.GetSettings`, `Operations.PreviewSettingsChange`, `Operations.ApplySettingsChange`, `Operations.PreviewSettingsRollback`, `Operations.RollbackSettings` |
| Setup | `Setup.GetStatus`, `Setup.PreviewPlan`, `Setup.ApplyPlan` |
| Fair value | `FairValue.ListMeasurements`, `FairValue.GetClassification`, `FairValue.Explain`, `FairValue.GetEvidence`, `FairValue.GetApprovalStatus`, `FairValue.Measure`, `FairValue.Classify`, `FairValue.Approve`, `FairValue.ProposeOverride`, `FairValue.RevokeApproval`, `FairValue.ListAuditEvents`, `FairValue.ApproveMarketAccess`, `FairValue.GetMarketAccess` |
| Bot, execution, risk | `Bot.GetStatus`, `Bot.Start`, `Bot.Stop`, `Execution.GetOrders`, `Execution.GetFills`, `Execution.Cancel`, `Execution.Reconcile`, `Risk.TriggerKillSwitch` |

Important operation-specific forms include the following code-owned bounds:

- Source lifecycle actions require `provider`, `expectedStateRevision >= 1`, optional positive
  `expectedGeneration`, and only the schema-permitted onboarding/public-configuration/reason fields.
  `Source.Inspect` requires provider, onboarding UUID, dataset identifier, `pageIndex 0..63`, and
  `maxRecords 1..1024`. `Source.Discover` produces the receipt consumed by source ingestion.
- Job list is a bounded latest-generation page; watch/cancel/retry require exact job UUID,
  positive generation, and exact observed sequence. Job confirmation also needs bounded `identity`
  and a lowercase `digest`.
- Portfolio staged import consumes native staged-input tickets and server-owned preview/approval
  identities. It does not accept a path. Scenario, proposal, candidate, screen, target, forecast,
  training, dataset, and backtest values are closed descriptor objects, not raw operation payloads.
- `Analysis.ReadArtifact` requires `artifactId`, digest, `byteCount 1..67108864`, media type
  `application/json`, `application/vnd.apache.parquet`, or `application/x-ndjson`, offset,
  and `maximumBytes 1..32768`.
- `Bot.Start.provider` is `coinbase`, `coinbase-direct`, or `kraken`; `feeBasisPoints` is
  `0..=10000`. Fair-value market-access conclusion is `accessible` or `inaccessible`; proposed
  hierarchy override is only `level_2` or `level_3`.
- Operations preview-bound mutations use the exact `previewId` UUID and `previewDigest` SHA-256.
  Backup pages limit to 64; workspace pages limit to 64; retention is `keepLatest 1..128`.
  Settings and setup are revision-fenced. The full type/schema is in the corresponding advertised
  descriptor, rather than a user-editable generic JSON schema.

## Resources and controlled artifacts

Resources are advertised. There are two fixed JSON resources:

| URI | Result |
| --- | --- |
| `market-squawk://service` | Authenticated installation ID, service generation, and `ready` status. |
| `market-squawk://workspace` | Authenticated active workspace ID and `active` status. |

Four JSON templates are also advertised:

```text
market-squawk://sources/{source_id}
market-squawk://models/{model_id}
market-squawk://jobs/{job_id}/generations/{generation}
market-squawk://artifacts/{artifact_id}
```

The source/model/artifact path component is opaque, nonempty, at most 1,024 bytes, has no `/` or
control bytes; `job_id` is a non-nil UUID. Resources resolve through the same application/job/artifact
authorities, return at most the MCP result ceiling, and never turn an artifact identity into a local
filesystem path.

Successful tools return structured content with `data` and metadata (`completeness`, returned and
available item counts, source coverage, and data quality). A read that exceeds the 64-KiB/1,000-item
inline ceiling but remains inside the 64-MiB/1,000,000-item ceiling is published as a controlled,
path-free artifact. Mutations are inline-only: an oversized mutation result fails rather than being
replaced with an artifact. Read one artifact only through `Analysis.ReadArtifact` using the complete
identity returned by the service. A resource reads artifact metadata/integrity; it is not a byte
download escape hatch.

## Authority, audit, and errors

Authentication maps each request to a separate Claude Code or Codex client identity, credential
generation, and client request budget. The underlying selected workspace, durable artifacts, and
application services are shared once; request cancellation, prompt/conversation state, handles,
subscriptions, credentials, and audit origin remain isolated.

Every admitted call is audited without raw payloads: request identity/content digests, bounded
operation/version, limits, authenticated installed-client class, phase, and result class. Mutations
separately record admission, authoritative service completion, and response completion. Audit write
failure prevents unrecorded work. Tool-level invalid/not-found/unauthorized/resource/unavailable
outcomes are returned as bounded `isError: true` text; cancellation, deadline, invalid output, and
internal failure are protocol errors. The stable server error codes include `-32008` deadline,
`-32010` resource exhaustion, `-32800` cancellation, `-32003` authorization, and `-32001`
unavailable.

## Related references

- [CLI reference](cli.md)
- [Configuration reference](configuration.md)
- [Installed-service composition](../../apps/market-squawk/src/service/mod.rs)
- [HTTP MCP boundary](../../crates/market-squawk-mcp/src/http.rs)
- [MCP handler and schema projection](../../crates/market-squawk-mcp/src/handler.rs)
- [Application operation registry](../../apps/market-squawk/src/application/contracts.rs)
