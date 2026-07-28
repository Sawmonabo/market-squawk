# Command-line interface reference

This page is the factual reference for the shipping `market-squawk` command hierarchy, its
arguments, local authority boundaries, result envelopes, and exit behavior.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Audience | Operators, integrators, automation authors, and maintainers |
| Status | Current |
| Last substantive review | 2026-07-26 |
| Implementation review base | `50912c18271a0389fb5ac8817555230930dd0506` |

## Contents

- [Scope](#scope)
- [Invocation and global options](#invocation-and-global-options)
- [Command hierarchy](#command-hierarchy)
- [Release evidence](#release-evidence)
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
| `--source-shutdown-ms <U64>` | Milliseconds | Configuration value, initially `15000` | Overrides the source-supervisor shutdown deadline; it must be at least `2 × capture_shutdown_ms + 1000` and no greater than `121000` |
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
| `init` | None | Explicitly prepares/opens the full local product, initializes or migrates durable authorities, creates the current Coinbase diagnostic journal, completes bounded shutdown, and prints the initialized root |
| `config show` | None | Loads validated configuration and emits the shared redacted `{value, origin}` view |
| `config validate` | None | Performs the same load/validation and emits `valid: true` plus the shared provenance-bearing redacted view |
| `doctor` | None | Performs a bounded query-only inspection of an existing layout/catalog plus compiled application/MCP contracts and provider facts; it does not initialize storage, acquire application/MCP writer authority, start adapters, or call remote endpoints |
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
| `source inspect <PROVIDER> --onboarding-session-id <UUID> --dataset-identifier <DATASET>` | Optional `--page-index` defaults to `0` in `0..=63`; optional `--max-records` defaults to `256` in `1..=1024` | `Source.Inspect`; bounded FRED/ALFRED canonical page and exact evidence, with no research-dataset persistence |
| `source activate <REQUEST> --confirm` | Versioned activation request file, at most 1 MiB | Evidence-bound provider activation and durable restart authority |
| `capture` | `--products <CSV>` defaults to `BTC-USD`; optional `--seconds <U64>` and `--paper-bot` | Diagnostic Coinbase capture composition, not the production application-service path |

`source setup` accepts only the loopback URL returned by the product: scheme `http`, host
`127.0.0.1`, an explicit port, no credentials, query, or fragment, and a lifetime from 30 seconds
through one hour. A browser-launch failure does not terminate the portal; the URL remains in the
command result and local log. The portal commits source-only sessions for public Coinbase,
Coinbase Direct, and Kraken. Treasury daily XML uses a provider-specific research form that selects
an inclusive year range and activates all five official families; Treasury Fiscal has its own
date/page form. The Coinbase Direct form creates the exact version-1 credential envelope from
separate write-only API-key, passphrase, and signing-secret fields. Buttons remain disabled when
the code-owned profile is `refresh_required` or `rights_blocked`.

### Research, datasets, and features

| Command | Arguments and defaults | Application boundary |
| --- | --- | --- |
| `ingest file <MANIFEST> --object <ID> --dataset <ID> --confirm` | Confined file-adapter manifest and exact object/dataset identities | File adapter, research-ingestion authority, then immutable publication |
| `ingest source <PROVIDER> <OBJECT> --dataset <DATASET> --confirm` | Provider and object are positional; dataset is a required named option | `Research.IngestSource` |
| `dataset list` | Optional `--after-dataset <DATASET>` cursor from the preceding bounded page | `Research.ListDatasets` |
| `dataset manifest <DATASET>` | Dataset identity is positional | `Research.GetManifest` |
| `dataset build <REQUEST> --confirm` | Closed point-in-time build request, at most 8 MiB | Research dataset builder and immutable publication |
| `query dataset <DATASET>` | `--maximum-rows <USIZE>` defaults to `1000` | `Research.GetHistory` with the requested result-count ceiling |
| `query artifact --artifact-id <ID> --sha256 <HEX> --byte-count <N>` | Optional `--media-type application/json`, `--offset 0`, and `--maximum-bytes 32768`; pass the returned `application/vnd.apache.parquet` media type for query overflow | `Analysis.ReadArtifact` over the shared path-free controlled-artifact authority |
| `query sql --dataset <DATASET> <STATEMENT>` | `--maximum-rows <USIZE>` defaults to `1000` | CLI-only bounded read-only DataFusion over the latest pinned immutable generation |
| `feature list` | Optional `--after-dataset <DATASET>` stable cursor | `Analysis.GetFeatureDatasets` |
| `feature build <REQUEST> --confirm` | Same closed point-in-time build contract as `dataset build` | Research dataset builder and immutable publication |

The SQL command is deliberately absent from MCP. Its fixed query ceilings are 64 KiB SQL text,
256 KiB inline Arrow IPC, 64 MiB for the complete result, 256 MiB of admitted query memory, four
partitions, 2,048 AST nodes, 4,096 plan nodes, and 60 seconds. A result above the inline ceiling and
within the complete-result ceiling becomes one opaque durable content-addressed Parquet reference.
Its exact fields are `artifactId`, `sha256`, `byteCount`, `mediaType`, and `rowCount`, with
`mediaType: "application/vnd.apache.parquet"`. Retrieve it through `query artifact`; the reference
has no public owner, expiry, or path.

Fixed-template application queries use a different limit source: their inline and complete-result
ceilings are the caller's admitted `ServiceLimits`, their query-memory ceiling is four times the
complete-result ceiling within the code-owned clamp, and the same partition/node/at-most-60-second
bounds apply. The CLI `query dataset` request admits 16 MiB for both inline and complete result, so
that command returns inline or fails at its complete ceiling; production MCP can use the wider
caller-admitted band described in the [MCP reference](mcp.md).

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
| `bot start --confirm` | `--provider <coinbase|coinbase-direct|kraken>` defaults to `coinbase`; Direct requires `--provider-session-id <UUID>`; optional `--seconds`; `--initial-cash` defaults to `100000`; `--fee-basis-points` defaults to `100` | `Bot.Start`, wait for duration/Ctrl-C, then confirmed `Bot.Stop` |
| `bot stop --reason <TEXT> --confirm` | Audit reason is required | `Bot.Stop` |
| `execution orders` | None | `Execution.GetOrders` |
| `execution fills` | None | `Execution.GetFills` |
| `execution cancel <ORDER> --confirm` | Existing paper order identity | `Execution.Cancel` |
| `execution reconcile --confirm` | None beyond confirmation | `Execution.Reconcile` |

Public Coinbase and Kraken remain `DirectUnverified`. Authenticated `coinbase-direct` binds the
exact active onboarding session and can derive `DirectVerified` authority only while every
sequence, snapshot, status, timestamp, freshness, precision, coverage, and generation check
remains current. Any failure cancels the paper run and denies further operations until stop
completes.

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

## Release evidence

| Command | Purpose and primary output |
| --- | --- |
| `release evidence fuzz` | Runs the six closed parser/protocol/model fuzz targets and atomically publishes `fuzz.json` |
| `release evidence benchmark` | Supervises the production live/storage measurement worker and atomically publishes `performance.json` |
| `release evidence providers` | Exercises the selected authorized production provider surfaces and creates `providers/provider-evidence.json` |
| `release demonstrate --offline` | Composes the complete local product against the exact provider/Python evidence and atomically publishes `demo.json` |
| `release evidence gate` | Supervises the exact checked-in full gate and binds its repository, executable, script, log, process limits, and target usage in `full-gate.json` |
| `release evidence close` | Validates the complete exact-HEAD directory and atomically publishes its terminal `manifest.json` |

Exact-head producers accept `--head` and `--tree`, reject a dirty or changing repository, and use
no-clobber outputs. The fuzz and benchmark commands permit omitted identities only for provisional
diagnosis; their reports cannot close a release. The demonstration and closer require exact
identities. Benchmark and demonstration execution require a binary built with the
`release-evidence` feature.

### Provider acceptance

`release evidence providers` is the production provider-acceptance producer. It is intentionally
separate from ordinary source setup and requires:

- exact `--head` and `--tree` identities for a clean, unchanged repository;
- `MARKET_SQUAWK_EXTERNAL_NETWORK=1` and
  `MARKET_SQUAWK_PROVIDER_TERMS_ACCEPTED=1`;
- a nonempty, duplicate-free list of exact built-in surface identifiers;
- `--sec-cik <CIK>` containing the exact nonzero ten-digit registrant selected during SEC setup;
- `--fred-dataset <PROVIDER_DATASET>` containing one bounded
  `fred:series-observations:<SERIES>:<START>:<END>` or `alfred:` selector;
- `--fred-training-request <REQUEST_FILE>` containing the existing bounded typed PIT dataset-build
  contract;
- `--bls-dataset <PROVIDER_DATASET>` containing the exact
  `bls:timeseries:public-v1:<PLAN_SHA256>` identity returned by BLS setup/status;
- `--bls-training-request <REQUEST_FILE>` containing the bounded typed PIT dataset-build contract
  for the exact published BLS manifest;
- an existing parent for `--output-directory`, while the output directory itself must not exist;
  and
- portal-prepared active sessions and callable research runtimes for surfaces that require
  contacts, credentials, series/query configuration, or admitted durable-use rights.

The exact terminal-closing surface set is:

```text
coinbase.public-market-data
coinbase.exchange-direct-market-data
kraken.spot-public-market-data
sec.edgar-public
fred-alfred.api-v1-v2
bls.v1-unregistered
treasury.daily-rates-xml
treasury.fiscal-data
```

The producer also recognizes `bls.v2-registered` for bounded provisional diagnostics. It does not
replace the mandatory public-v1 path and cannot appear as an extra surface in a terminal provider
report.

The producer can establish no-credential onboarding for public Coinbase and public Kraken. It
requires a portal-prepared Treasury daily research runtime with all five official families and an
inclusive configured range; it then retrieves, ingests, queries, and recovers one configured
common year across those families. It does not invent SEC contact data, BLS series semantics,
Treasury query bounds, provider credentials, or FRED/ALFRED rights. Every selected surface must
recover an exact active lease; durable research surfaces must also recover the same callable
runtime generation after a clean application shutdown and restart.

FRED/ALFRED acceptance is a working-data gate. The producer discovers every page for the exact
provider selector, persists it under the separate dotted analytical `DatasetId`, queries
observations and vintages, and repeats those exact queries after restart. It then runs the supplied
PIT build request through the same production builder used by `dataset build`. That request must
name the exact published FRED manifest and genuine historical-universe evidence; it must produce
nonempty train, validation, and test splits plus a durable nonzero Python-export digest. Missing,
synthetic, zero-row, mismatched-parent, or non-recoverable evidence fails the command. This path
cannot begin durable publication unless exact current terms, written St. Louis Fed service
permission with a hash-bound local review, and independent exact-series authority are all present.

Live surfaces are exercised one at a time through the production application. Public Coinbase and
Kraken must remain `DirectUnverified` and must produce no automated paper order. Coinbase Direct
must reach `DirectVerified`; `--require-direct-verified-action` additionally requires at least one
strategy-originated, centrally risk-approved paper order. `--require-fred-alfred-rights` requires
both persistence and model-training admission for the exact FRED/ALFRED surface. Profile revision
4 is rights-limited: those operations pass only when the active lease binds both the exact Bank
service-permission gate and the exact-series gate for the same scope and validity interval. An API
key, contact receipt, or public-domain series alone cannot satisfy this predicate.

Only after collection, shutdown, restart recovery, executable hashing, and a second repository
identity barrier does the command create
`<OUTPUT-DIRECTORY>/provider-evidence.json` through atomic no-clobber publication. The final
`release evidence close` command rejects a provider report unless it contains every mandatory
Coinbase, Kraken, SEC, FRED/ALFRED, BLS v1, Treasury XML, and Treasury Fiscal surface plus the
required Direct action, FRED/ALFRED rights, restart, and exact-binary evidence.
For FRED/ALFRED, closure also requires nonempty real publications, observation/vintage queries,
the exact derived-parent relationship, all three dataset splits, a nonzero Python-export digest,
and restart recovery of that same derived generation and complete parent set.

### Demonstration and closure

`release demonstrate --offline` requires:

- exact `--head` and `--tree`;
- `--provider-evidence <HEAD-ROOT>/providers`;
- `--python-evidence <HEAD-ROOT>/python/market-squawk-release.json`; and
- `--output-file <HEAD-ROOT>/demo.json`, which must not exist.

It revalidates the provider report, current executable, signed CPython 3.12 and 3.13 environments,
repository identity, and directory topology at admission and publication. It runs production
live/model/risk/paper, storage/PIT/Python/backtest, portfolio/fair-value, CLI/doctor, and stdio MCP
paths. Public-source fixtures remain `DirectUnverified`; the local product starts with the paper bot
stopped and proves execution operations fail closed until a running source owns authority.

`release evidence gate` requires exact `--head`, `--tree`, `--binary`, absent
`--gate-log <HEAD-ROOT>/full-gate.log`, and absent
`--output-file <HEAD-ROOT>/full-gate.json`. The running selected release executable
parent-supervises the exact checked-in `scripts/verify.sh` with an eight-hour deadline, a 16 GiB
process-tree RSS ceiling, a log-only 64 MiB file-size ceiling, and no-clobber log creation. It binds
the script and completed log by SHA-256 and byte count, records observed process evidence,
revalidates its immutable inputs, and rejects target usage above 20 GiB.

`release evidence close` accepts only the exact HEAD-keyed root containing `fuzz.json`,
`performance.json`, `providers/`, `python/`, `demo.json`, `full-gate.log`, and `full-gate.json`. Its
`--output-file` must be the absent `<HEAD-ROOT>/manifest.json`. The closer rejects missing or extra
root entries, credentials, symlinks, parent traversal, cross-HEAD artifacts, binary mismatches,
failed semantic fuzz/performance/gate predicates, incomplete product predicates, or incomplete
provider rights/action evidence. Python release evidence must bind the same selected application
binary. The final artifact inventory and every external immutable input are revalidated on both
sides of pending-manifest preparation.

The reproducible sequence and current blockers are in the
[exact-head release gate](../verification/usable-release-gate.md).

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

`query dataset` replaces the default item ceiling with `--maximum-rows` and retains the 16 MiB
default byte ceiling for both inline and complete fixed-template query output. `query sql` instead
uses its dedicated 256 KiB inline and 64 MiB complete-result ceilings and returns only its small
terminal reference when it republishes Parquet. `portfolio import` uses an 8 MiB result ceiling.
Individual application descriptors may impose narrower instrument, time, source-coverage, schema,
work, or retained-memory limits.

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
| `paper-bot [--provider <coinbase|coinbase-direct|kraken>] [--seconds <U64>] [--initial-cash <DECIMAL>] [--fee-basis-points <U32>]` | v0.1 public-source compatibility command; `coinbase-direct` is rejected with an instruction to use `bot start`, which retains the exact onboarding session and application authority |
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

CLI-only DataFusion SQL receives only a pinned dataset generation, a read-only query engine, and
bounded transient query-publication authority. Verified overflow is transferred into the shared
terminal repository before a path-free reference is returned. The CLI confines file inputs to
declared capability roots; credential resolution, approval authority, unrelated database
publication, and order dispatch remain with their dedicated application services.

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
