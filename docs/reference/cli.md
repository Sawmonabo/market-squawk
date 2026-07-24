# Command-line interface reference

This page is the factual reference for the shipping `market-squawk` command hierarchy, its
arguments, local authority boundaries, result envelopes, and exit behavior.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Audience | Operators, integrators, automation authors, and maintainers |
| Status | Current |
| Last substantive review | 2026-07-24 |
| Reviewed commit | `3ef05dc8724ec2be808f98543e0bc695f2ae0937` |

## Contents

- [Scope](#scope)
- [Invocation and global options](#invocation-and-global-options)
- [Command hierarchy](#command-hierarchy)
- [Confirmation and input admission](#confirmation-and-input-admission)
- [Request and result limits](#request-and-result-limits)
- [Output and exit behavior](#output-and-exit-behavior)
- [Compatibility commands](#compatibility-commands)
- [Authority mapping](#authority-mapping)
- [Related documentation and code](#related-documentation-and-code)
- [External sources](#external-sources)

## Scope

The public CLI is a local transport over the same application services used by MCP, except where a
row below explicitly identifies a CLI-owned boundary such as initialization, bounded read-only
DataFusion SQL, provider activation, or an immutable input-file admission path.

This page does not define the JSON schema inside every request file, provide an operating tutorial,
or claim that the complete release gate has passed. Request schemas are code-owned contracts;
operator procedures belong in the [operations guide](../operations/README.md), and current release
blockers remain in the [delivery ledger](../plans/delivery-ledger.md).

## Invocation and global options

```text
market-squawk [GLOBAL OPTIONS] <COMMAND> [COMMAND OPTIONS]
```

Clap marks the options below as global, so they may appear with any public subcommand.

| Option | Value | Default | Effect |
| --- | --- | --- | --- |
| `--data-dir <PATH>` | Local path | `.market-squawk` through configuration | Highest-precedence local data-root override |
| `--config <PATH>` | Local TOML file | None | Reads one explicit configuration file; there is no implicit file discovery |
| `--log <FILTER>` | Tracing filter | `info`; may use `MARKET_SQUAWK_LOG` | Configures local stderr tracing; it is not part of `AppConfig` |
| `--json-logs` | Flag | Off | Emits structured tracing to stderr |
| `--output <human|json>` | Enum | `human` | Selects human plus pretty JSON or compact JSON for supported commands |
| `--source-shutdown-ms <U64>` | Milliseconds | Configuration value, initially `5000` | Overrides the source-supervisor shutdown deadline; valid range is `1..=60000` |
| `--training-release-root <PATH>` | Absolute path | None | Selects the installed signed training release; the running application and sibling ONNX worker must be the exact files from that release |
| `--capture-queue-capacity <USIZE>` | Count | Configuration value, initially `16384` | Overrides the fixed raw-capture queue capacity; valid range is `1..=1048576` |
| `--capture-memory-ceiling-bytes <USIZE>` | Bytes | Configuration value, initially `67108864` | Overrides the per-channel capture memory ceiling; valid range is `1..=4294967295` |
| `--capture-destination-registry-memory-ceiling-bytes <USIZE>` | Bytes | Configuration value, initially `1048576` | Overrides the process-wide destination-registry ceiling; valid range is `1..=67108864` |
| `-h`, `--help` | Flag | — | Prints help and exits successfully |
| `-V`, `--version` | Flag | — | Prints the package version and exits successfully |

Configuration precedence, every environment mapping, and the provider-profile contracts are in
[Configuration reference](configuration.md).

## Command hierarchy

### Bootstrap and control

| Command | Arguments | Authority and result |
| --- | --- | --- |
| `init` | None | Prepares the configured local paths and current Coinbase diagnostic journal; prints the initialized root |
| `config show` | None | Loads validated configuration and emits its redacted effective values |
| `config validate` | None | Performs the same load/validation and emits `valid: true` plus the redacted effective values |
| `doctor` | None | Composes the local product, performs bounded shutdown, and reports readiness plus current release blockers |
| `mcp` | Optional `serve` | Runs the sole production MCP server over stdio; bare `mcp` is the v0.1 compatibility form for `mcp serve` |

`mcp` reserves stdout for protocol frames. It does not render a normal CLI result envelope.

### Sources and capture

| Command | Arguments and defaults | Application boundary |
| --- | --- | --- |
| `source register <PROVIDER> --confirm` | Provider is a code-owned profile identifier | `Source.Register` |
| `source status [PROVIDER]` | Provider filter is optional | `Source.GetStatus` |
| `source coverage [PROVIDER]` | Provider filter is optional | `Source.GetCoverage` |
| `source health [PROVIDER]` | Provider filter is optional | `Source.GetHealth` |
| `source setup <PROVIDER> --confirm` | Starts or resumes local onboarding; keeps the bounded loopback portal alive until Ctrl-C or expiry | `Source.Setup` plus the local portal owner |
| `source discover <PROVIDER> --dataset <DATASET>` | Provider and exact dataset namespace are required | `Source.ListObjects`; bounded listing only, with no ingestion receipt |
| `source activate <REQUEST> --confirm` | Versioned activation request file, at most 1 MiB | Evidence-bound provider activation and durable restart authority |
| `capture` | `--products <CSV>` defaults to `BTC-USD`; optional `--seconds <U64>` and `--paper-bot` | Diagnostic Coinbase capture composition, not the production application-service path |

`source setup` accepts only the loopback URL returned by the product: scheme `http`, host
`127.0.0.1`, an explicit port, no credentials, query, or fragment, and a lifetime from 30 seconds
through one hour. A browser-launch failure does not terminate the portal; the URL remains in the
command result and local log.

### Research, datasets, and features

| Command | Arguments and defaults | Application boundary |
| --- | --- | --- |
| `ingest file <MANIFEST> --object <ID> --dataset <ID> --confirm` | Confined file-adapter manifest and exact object/dataset identities | File adapter, research-ingestion authority, then immutable publication |
| `ingest source <PROVIDER> <OBJECT> --dataset <DATASET> --confirm` | Provider and object are positional; dataset is a required named option | `Research.IngestSource` |
| `dataset list` | Optional `--after-dataset <DATASET>` cursor from the preceding bounded page | `Research.ListDatasets` |
| `dataset manifest <DATASET>` | Dataset identity is positional | `Research.GetManifest` |
| `dataset build <REQUEST> --confirm` | Closed point-in-time build request, at most 8 MiB | Research dataset builder and immutable publication |
| `query dataset <DATASET>` | `--maximum-rows <USIZE>` defaults to `1000` | `Research.GetHistory` with the requested result-count ceiling |
| `query artifact --artifact-id <ID> --sha256 <HEX> --byte-count <N>` | Optional `--media-type application/json`, `--offset 0`, and `--maximum-bytes 32768` | `Analysis.ReadArtifact` over the shared path-free controlled-artifact authority |
| `query sql --dataset <DATASET> <STATEMENT>` | `--maximum-rows <USIZE>` defaults to `1000` | CLI-only bounded read-only DataFusion over the latest pinned immutable generation |
| `feature list` | Optional `--after-dataset <DATASET>` stable cursor | `Analysis.GetFeatureDatasets` |
| `feature build <REQUEST> --confirm` | Same closed point-in-time build contract as `dataset build` | Research dataset builder and immutable publication |

The SQL command is deliberately absent from MCP. Its fixed query ceilings are 64 KiB SQL text,
256 KiB Arrow result bytes, 64 MiB admitted query memory, four partitions, 2,048 AST nodes, 4,096
plan nodes, and 60 seconds. The JSON-rendered result must also remain within the CLI's 16 MiB result
ceiling.

### Models and backtests

| Command | Arguments | Application boundary |
| --- | --- | --- |
| `model list` | None | `Model.ListBundles` |
| `model admit <REQUEST> --confirm` | Closed schema-v1 request, at most 8 MiB; requires a configured verified training release | Model runtime admission and immutable registry publication |
| `model metadata <MODEL>` | Model identity is positional | `Model.GetMetadata` |
| `model evaluate <REQUEST> --confirm` | Confined JSON object | `Model.Evaluate` |
| `model predict <REQUEST>` | Confined JSON object | `Model.Predict` |
| `backtest run <REQUEST> --confirm` | Closed governed-input registration request | Registration followed by `Analysis.RunBacktest` |
| `backtest show <RUN>` | Experiment or run identity | `Analysis.GetBacktests` |

Inference errors and model-admission failures return no order authority. Backtest results remain
research evidence and cannot create execution authority.

### Portfolio, paper operation, and execution

| Command | Arguments and defaults | Application boundary |
| --- | --- | --- |
| `portfolio import <PATH> --account <ID> --confirm` | Versioned holdings/transaction manifest, at most 8 MiB | Portfolio adapter, research ingest, immutable artifact, then `Portfolio.Import` |
| `portfolio holdings --account <ID>` | Exact account identity | `Portfolio.GetHoldings` |
| `portfolio transactions --account <ID>` | Exact account identity | `Portfolio.GetTransactions` |
| `portfolio performance <REQUEST>` | Confined JSON object | `Portfolio.GetPerformance` |
| `portfolio exposure <REQUEST>` | Confined JSON object | `Portfolio.GetExposure` |
| `portfolio risk <REQUEST>` | Confined JSON object | `Portfolio.GetRisk` |
| `bot status` | None | `Bot.GetStatus` |
| `bot start --confirm` | `--provider <coinbase|kraken>` defaults to `coinbase`; optional `--seconds`; `--initial-cash` defaults to `100000`; `--fee-basis-points` defaults to `100` | `Bot.Start`, wait for duration/Ctrl-C, then confirmed `Bot.Stop` |
| `bot stop --reason <TEXT> --confirm` | Audit reason is required | `Bot.Stop` |
| `execution orders` | None | `Execution.GetOrders` |
| `execution fills` | None | `Execution.GetFills` |
| `execution cancel <ORDER> --confirm` | Existing paper order identity | `Execution.Cancel` |
| `execution reconcile --confirm` | None beyond confirmation | `Execution.Reconcile` |

The current Coinbase and Kraken profiles remain `DirectUnverified`. Consequently, the bot and
execution commands operate the risk-enforced paper system but current provider observations cannot
satisfy the default `DirectVerified` automated-action gate.

### Fair value

| Command | Arguments | Application boundary |
| --- | --- | --- |
| `fair-value list` | None | `FairValue.ListMeasurements` |
| `fair-value measure <REQUEST> --confirm` | Confined genuine-producer selection and measurement JSON | `FairValue.Measure` |
| `fair-value classify <MEASUREMENT> --confirm` | Measurement identity | `FairValue.Classify` |
| `fair-value explain <MEASUREMENT>` | Measurement identity | `FairValue.Explain` |
| `fair-value evidence <MEASUREMENT>` | Measurement identity | `FairValue.GetEvidence` |
| `fair-value approval-status <MEASUREMENT> --at <RFC3339>` | Exact status instant | `FairValue.GetApprovalStatus` |
| `fair-value approve <MEASUREMENT> --decision <ID> --reviewer <ID> --approved-at <RFC3339> --expires-at <RFC3339> --confirm` | Exact decision, distinct reviewer, approval time, and expiry | `FairValue.Approve` |

Fair-value classification never changes market-data quality or creates live execution authority.

## Confirmation and input admission

Commands marked `--confirm` require the flag at the CLI boundary or the typed application
descriptor. Omitting it fails before the requested durable mutation. Confirmation does not bypass
provider rights, source qualification, point-in-time, model, portfolio, fair-value, risk, or
execution authority.

Ordinary JSON request-file commands enforce all of the following:

- the path is made absolute without accepting a parent-directory component;
- the parent becomes a user-authorized capability root;
- the input is an unchanged, no-follow, bounded regular file;
- the default file ceiling is 8 MiB;
- the top-level JSON value is an object; and
- the operation's closed descriptor or dedicated versioned decoder performs the final validation.

Provider activation uses a 1 MiB request ceiling and additional provider-evidence bounds. Governed
backtest, dataset, model-admission, portfolio, and file-ingestion paths use their dedicated closed
decoders rather than treating arbitrary JSON as authority.

## Request and result limits

Shared product commands receive a five-minute monotonic request deadline and these fixed transport
limits:

| Limit | Value |
| --- | ---: |
| Default result items | 10,000 |
| Default result bytes | 16 MiB |
| Hard result bytes | 64 MiB |
| Maximum JSON depth | 32 |
| Maximum bytes in one JSON string or key | 1 MiB |
| Maximum items in one JSON array | 100,000 |
| Maximum entries in one JSON object | 10,000 |

`query dataset` replaces the default item ceiling with `--maximum-rows`. `portfolio import` uses an
8 MiB result ceiling. Individual application descriptors may impose narrower instrument, time,
source-coverage, schema, work, or retained-memory limits.

The standard local-product result envelope is:

```json
{
  "data": {},
  "metadata": {
    "completeness": "complete",
    "returnedItems": 0,
    "availableItems": 0,
    "sourceCoverage": null,
    "dataQuality": null,
    "sourceEvidence": null
  },
  "encodedBytes": 0
}
```

The values above illustrate the shape, not the result of a particular operation.

## Output and exit behavior

| Condition | Exit status | Output behavior |
| --- | ---: | --- |
| Help, version, or successful command | `0` | Command result on stdout; local tracing on stderr |
| Clap syntax, enum, or required-argument error | `2` | Clap diagnostic and usage on stderr |
| Configuration, admission, service, I/O, lifecycle, or shutdown failure | `1` | Error chain on stderr; no success envelope |

For `--output human`, supported control/product commands print one summary line followed by
pretty-printed JSON. For `--output json`, they print one compact JSON value. `init`, diagnostic
`capture`, and the hidden compatibility commands retain their fixed v0.1 rendering and currently do
not change shape with `--output`. MCP stdout is protocol-only.

JSON errors are not currently emitted as a separate stable machine-readable envelope. Automation
must use the exit status and treat stderr text as diagnostic rather than a versioned API.

## Compatibility commands

The following commands are hidden from normal help and are not the primary product interface:

| Command | Purpose and boundary |
| --- | --- |
| `mock --product <ID> --events <N> [--paper-bot]` | Deterministic diagnostic source; defaults are `TEST-USD` and `100` events |
| `paper-bot [--provider <coinbase|kraken>] [--seconds <U64>] [--initial-cash <DECIMAL>] [--fee-basis-points <U32>]` | v0.1 production-composition compatibility command; defaults match `bot start` but it is not an unchecked order path |
| `replay [--source coinbase-exchange] [--journal-format <current|legacy>]` | Validates and reconstructs the diagnostic Coinbase journal; other decoded sources are rejected |

Replay is diagnostic tooling. It is not a source of current execution authority and is not a core
historical-data requirement.

## Authority mapping

The CLI parser creates no business authority. It either calls a local lifecycle boundary or maps a
command to a code-owned operation descriptor. The descriptor validates its closed schema and
authorization class; the domain service then owns financial, source, model, portfolio, valuation,
or execution invariants. Risk-mediated actions still cross central risk and one-use dispatch.

```text
Clap command
  -> confined CLI input and fixed request limits
  -> code-owned application descriptor or dedicated local authority
  -> product-domain service
  -> bounded result envelope
```

CLI-only DataFusion SQL receives only a pinned dataset generation and a read-only query engine. The
CLI confines file inputs to declared capability roots; credential resolution, approval authority,
database publication, and order dispatch remain with their dedicated application services.

## Related documentation and code

- [Control-plane architecture](../architecture/control-plane.md)
- [Configuration reference](configuration.md)
- [MCP reference](mcp.md)
- [Installation and bootstrap](../operations/installation-and-bootstrap.md)
- [CLI contract](../../apps/market-squawk/src/cli.rs)
- [Process dispatch and output](../../apps/market-squawk/src/main.rs)
- [Shared CLI transport](../../apps/market-squawk/src/local_product/cli_transport.rs)
- [Application capability registry](../../apps/market-squawk/src/application/contracts.rs)
- [Accepted-head delivery evidence](../plans/delivery-ledger.md)

## External sources

| Source | Applied fact | Reviewed |
| --- | --- | --- |
| [Clap derive tutorial 4.6.2](https://docs.rs/clap/4.6.2/clap/_derive/_tutorial/index.html) | Derive-based parser, subcommand, argument, help, and version behavior used by the shipping CLI | 2026-07-23 |
| [DataFusion SQL reference](https://datafusion.apache.org/user-guide/sql/index.html) | SQL dialect reference for the separately bounded CLI-only analytical query | 2026-07-23 |

External documentation explains upstream parser and query syntax. The reviewed Market Squawk code
head remains the authority for which commands, options, and limits actually ship.
