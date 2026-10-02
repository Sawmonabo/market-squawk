# Command-line interface reference

This is the installed `market-squawk` command contract. The CLI is a separately authenticated
client of the one per-user service for product commands; it does not start a second catalog, MCP
server, job authority, or paper runtime. Use [MCP](mcp.md) when an automation needs the complete
typed operation registry rather than this operator-oriented command projection.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Status | Current implementation contract |
| Last substantive review | 2026-10-02 |
| Authority | `apps/market-squawk/src/cli.rs` and `src/main.rs` |

## Invocation and global options

```text
market-squawk [GLOBAL OPTIONS] <COMMAND> [COMMAND OPTIONS]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--data-dir <PATH>` | Configuration default | Local workspace root passed to configuration. It is not a service endpoint selector. |
| `--config <PATH>` | None | The sole explicit TOML configuration file. |
| `--log <FILTER>` | `info` or `MARKET_SQUAWK_LOG` | Stderr tracing filter. |
| `--json-logs` | Off | Render local tracing as JSON on stderr. |
| `--output <human|json>` | `human` | Command-result rendering mode; MCP reserves stdout for protocol frames. |
| `--source-shutdown-ms <U64>` | Configuration value | Source-supervisor shutdown override; whole-configuration validation still applies. |
| `--training-release-root <PATH>` | Installed release root when resolvable | Absolute release root used to verify admitted model artifacts. |
| `--capture-queue-capacity <USIZE>` | Configuration value | Diagnostic capture override. |
| `--capture-memory-ceiling-bytes <USIZE>` | Configuration value | Diagnostic per-channel capture-memory override. |
| `--capture-destination-registry-memory-ceiling-bytes <USIZE>` | Configuration value | Diagnostic capture registry-memory override. |

The exact startup configuration semantics and ranges are in the [configuration reference](configuration.md).

## Service lifecycle and routing

`service status` authenticates the owner-only rendezvous, probes readiness, and returns a
non-secret bootstrap snapshot. `service start` first probes; if no ready service is found it starts
the verified packaged `market-squawk-service` sibling and waits up to 15 seconds for authenticated
readiness. It never accepts a caller-provided port, URL, bearer token, or service executable.

The commands in the following table connect as the CLI client and require that installed service:
`source`, `ingest`, `dataset`, `query`, `feature`, `model`, `analysis`, `portfolio`, `backtest`, `bot`,
`execution`, `fair-value`, `job`, `operations`, and `setup`. `init`, `config`, `capture`,
`doctor`, `release`, and the named-client MCP relay have their documented dedicated compositions.

| Command | Exact subcommands / admission |
| --- | --- |
| `init` | Initializes controlled local state and the Coinbase diagnostic journal, then performs bounded shutdown. |
| `config show`, `config validate` | Redacted effective startup configuration; validate returns `valid: true` only after whole-object validation. |
| `service status`, `service start` | Authenticated readiness or verified sibling start as above. |
| `doctor` | Query-only existing-layout/configuration/readiness inspection; does not start adapters or make provider calls. |
| `capture` | Diagnostic Coinbase capture; `--products` is CSV and defaults to `BTC-USD`; optional `--seconds` and `--paper-bot`. It is not the production paper-service path. |
| `release evidence <fuzz|benchmark|providers|gate|close>`; `release demonstrate` | Exact-head release-evidence producer/closure commands. Their required arguments are Clap-defined evidence paths and identities; they make no release approval claim by themselves. |

## Product command hierarchy

All mutations require `--confirm` unless a row explicitly says it is a read. Confirmation records
local mutation intent; it is not an identity, risk approval, source qualification, or an execution
bypass. Request files are admitted as bounded, confined JSON objects only at the command boundaries
that name one; MCP never receives a filesystem path.

### Sources, ingestion, datasets, and analysis

| Command | Exact arguments and effect |
| --- | --- |
| `source register <provider> --confirm` | Register a code-supported profile. Configure connections in Desktop Settings → Connections → Set up connections. |
| `source status [provider]`; `source coverage [provider]`; `source health [provider]` | Bounded provider status, explicit coverage, or connection/integrity/freshness facts. |
| `source discover <provider> --dataset <dataset> --confirm` | Bounded exact objects with single-use ingestion receipts. |
| `source inspect <provider> --onboarding-session-id <UUID> --dataset-identifier <dataset> [--page-index 0..63] [--max-records 1..1024]` | One non-persisting provider page; defaults are `0` and `256`. |
| `source verify <provider> --confirm` | Verify the saved connection without starting its runtime. |
| `source start <provider> --confirm` | Start the verified saved source configuration through the installed service. |
| `source retry <provider> --confirm` | Resume the retained source transition after correcting its reported failure, using the observed revision and saved configuration. |
| `source stop <provider> --confirm`; `source remove <provider> --confirm` | Stop activity while retaining configuration, or remove the connection through its normal credential cleanup contract. Both use the observed revision; stored market data and audit history remain. |
| `source activate <request> --confirm` | One-shot, bounded installed-service request using `market-squawk.provider-setup.v1`. Supports `start` to prepare/reuse a session and `activate`, `verifySaved`, `restoreSaved`, and `resumePublication` for an existing session; see [the exact envelope](../operations/source-operations.md#understand-the-source-activate-boundary). |
| `ingest source <provider> <object> --dataset <dataset> --discovery-receipt <receipt> --confirm` | Consumes the original receipt for that exact object without rediscovery. |
| `ingest file <manifest> --object <id> --dataset <id> --confirm` | CLI-owned confined local-file manifest admission. |
| `dataset list [--after-dataset <id>]`; `dataset manifest <dataset>` | Bounded immutable dataset inventory or one manifest. |
| `dataset build <request> --confirm`; `feature build <request> --confirm` | CLI-owned confined typed point-in-time dataset request and immutable publication. |
| `feature list [--after-dataset <id>]` | Registered feature contracts and immutable feature datasets. |
| `query dataset <dataset> [--maximum-rows <n>]` | Bounded dataset-history read; default row request is `1000`. |
| `query sql --dataset <dataset> <statement> [--maximum-rows <n>]` | CLI-only bounded, read-only DataFusion SQL. It does not exist as an MCP tool. |
| `query artifact --artifact-id <id> --sha256 <digest> --byte-count <n> [--media-type <type>] [--offset <n>] [--maximum-bytes <n>]` | Digest-verified artifact chunk; defaults are `application/json`, `0`, and `32768` bytes. |
| `analysis results [--after <UUID>] [--limit 1..1000]` | Read saved generated, no-action, and unavailable analyses in creation order; default `100`. Each entry includes its original `actionToken`; pass `nextAfterActionToken` as `--after` for the next page. |
| `analysis show --action-token <UUID>` | Reopen one exact saved analysis. This read reports chart availability; retrieve chart evidence separately with `analysis chart`. |
| `analysis chart --action-token <UUID> [--start-unix-nanos <i64>] [--end-unix-nanos <i64>] [--point-limit <u16>] [--layer <NAME>]` | Read the selected display window and layer of the saved analysis through `Decision.GetInvestmentChart`; no confirmation is required. |

For chart reads, either time bound may be omitted independently; supplied bounds are inclusive,
and a start after the end is rejected. Times are exact signed Unix nanoseconds and are serialized
as JSON strings. The service defaults to layer `all` and `1000` display points, admits point limits
from `8` through `4096`, and supports `all`, `history`, `forecast`, `benchmark`, `price_pattern`,
and `action_ranges`. The selected window and point limit affect display only, preserving the
complete analytical inputs and original saved evidence. These reads do not run a new analysis or
establish current trading eligibility.

Use `--output json` to retrieve the structured service result, including available series,
original evidence, viewport metadata, and explanations of unavailable layers:

```text
market-squawk --output json analysis results
market-squawk --output json analysis show --action-token <UUID>
market-squawk --output json analysis chart --action-token <UUID> --layer history --point-limit 1000
```

CLI SQL has fixed limits: 64 KiB statement text, 1,000 default requested rows, 256 KiB inline
Arrow IPC, 64 MiB complete result, 256 MiB query memory, four partitions, 2,048 syntax-tree nodes,
4,096 plan nodes, and 60 seconds. A result above inline and within the complete ceiling is a
path-free Parquet artifact reference with `artifactId`, `sha256`, `byteCount`, `mediaType`, and
`rowCount`; retrieve it through `query artifact`.

### Selected investment details

Use the exact `selectionToken` returned by `market search --query <ticker>` or `market overview`;
do not construct a token from a ticker. These commands share the same selected-detail operations
as Desktop and MCP:

| Command | Effect |
| --- | --- |
| `market profile --selection-token <token>` | Read the selected investment's reference and listing profile independently of price availability. |
| `market financials --selection-token <token> --section <facts\|statements\|ratios\|filings> [--cursor <cursor>] [--limit 1..100]` | Read one retained financial section without acquiring new provider data; the default page size is 32. |
| `market close-financials --selection-token <token> --read-token <UUID>` | Release an open financial read without deleting its stored source evidence or cancelling a separate durable job. |
| `market prepare-financials --selection-token <token> --confirm` | Acquire company financial evidence as an independent durable job; ordinary financial reads stay available. |
| `market financial-preparation --selection-token <token> --job-id <UUID> --generation <positive>` | Read the selected financial job’s exact generation, progress and outcome. |
| `market cancel-financial-preparation --selection-token <token> --job-id <UUID> --generation <positive> --expected-sequence <n> --confirm` | Cancel against the observed sequence; valid retained intermediate evidence remains available. |
| `market reconcile-financial-preparation --request-id <original> --arguments-sha256 <digest>` | Resolve an uncertain financial start using its original request identity; never starts another job. |
| `market prepare-history --history-token <token> --lookback-days <days> --confirm` | Start selected adjusted daily-history acquisition; returns the durable job receipt. |
| `market history-preparation --history-token <token> --job-id <UUID> --generation <positive>` | Read the selected job's exact generation, progress, outcome and publication result. |
| `market cancel-history-preparation --history-token <token> --job-id <UUID> --generation <positive> --expected-sequence <n> --confirm` | Cancel against the exact observed sequence; already committed evidence remains retained. |
| `market reconcile-history-preparation --request-id <original> --arguments-sha256 <digest>` | Resolve an uncertain start using its original request identity and digest; does not start another job. |

History preparation uses the returned `historyToken`, not the investment's `selectionToken`.
`lookback-days` requests 30–3650 calendar days ending at admission; the result reports actual
coverage and gaps. These are acquisition parameters, not limits on retained history. Desktop uses
the same `Market.StartHistoryPreparation`, `Market.GetHistoryPreparation` and
`Market.CancelHistoryPreparation` operations available to ordinary MCP clients. Read the completed
publication with `market history`; a queued receipt does not establish that chart data is ready.
An interrupted process does not silently restart provider acquisition. Reconcile an uncertain start
before attempting another; the CLI prints the original request identity and digest on that failure.

Financial pages return `currentCursor`, `nextCursor`, `readToken`, frozen `knowledgeAt` and
`effectiveOn`, per-family availability, and explicit omissions/limitations. Pass cursor strings
unchanged to the same selection and section. Statements and ratios use complete reporting contexts;
page boundaries do not combine facts from different filings to calculate a ratio. Use
`--output json` for exact decimals, contexts and evidence dates.

Read handles are temporary, process-owned views. Closing, expiry or service restart requires a new
first page; it does not erase published evidence. A fresh first page selects current evidence and
is not a promise to reproduce an earlier cutoff. Saved analysis retains its own durable evidence.

### Models, portfolio, backtests, paper, and fair value

| Command | Exact arguments and effect |
| --- | --- |
| `model list`; `model metadata <model>` | Admitted immutable model bundles or one bundle's validation metadata. |
| `model admit <request> --confirm` | CLI-owned verified model-admission request. |
| `model evaluate <request> --confirm`; `model predict <request>` | Confined model-input object. Prediction failure produces no automatic action. |
| `portfolio accounts [--cursor <cursor>] [--limit <count>]` | Named portfolio directory and opaque account tokens; default page size 25. |
| `portfolio import preview <path> --account <id> --confirm` | Review a selected file and its required interpretation choices. |
| `portfolio import approve --review-token <token> --interpretations <path> --confirm` | Approve explicit interpretations from that review; returns an approval token. |
| `portfolio import commit --approval-token <token> --confirm`; `portfolio import discard --review-token <token> --confirm` | Save the approved import, or discard an unsaved review. |
| `portfolio holdings --account <token> [--cursor <cursor>] [--limit <count>]` | Exact positions from the selected saved snapshot, with reported prices, cost basis and available historical investment names. Get the token from `portfolio accounts`; continuation remains on the original snapshot, while a fresh request reads the current one. Default page size 25. |
| `portfolio transactions --account <token> [--cursor <cursor>] [--limit <count>]` | Exact recorded activity for the selected opaque account token. Cursor pages preserve the saved observation across later imports and restart; amounts, quantities, dates and available investment names remain source observations, not inferred performance. |
| `portfolio performance <request>`; `portfolio risk <request>` | Confined typed point-in-time request object using `accountToken` returned by `portfolio accounts`. Performance preserves optional instrument/time filters and returns exact cash, reported value, returns and reconciliation from one saved snapshot. |
| `portfolio exposure <request>` | Selected `accountToken` with optional `cursor`/`limit` and instrument/time filters. Returns whole-snapshot position net/gross totals, currency totals including cash and receivables, explicit classification gaps, and a page of exact positions from that same saved snapshot. |
| `portfolio revisions --account <token> [--cursor <cursor>] [--limit <count>]` | List saved portfolio observations, newest first. `selectedSnapshotToken` anchors the listing; `pageCursor` and `nextCursor` preserve that selection across later imports and restart. |
| `portfolio attribution <request>` | Compare explicit `selectedSnapshotToken` and earlier `baselineSnapshotToken` for `accountToken`, obtained from `portfolio revisions`. Optional `cursor`/`limit` and instrument/time filters page all position changes while retaining the full total. Exact opening, closing and change amounts cover added, closed and short positions. This is reported position-value change before cash-flow/corporate-action adjustments, not investment return. |
| `portfolio scenario <request>`; `portfolio scenario-batch <request>` | Calculate explicit hypothetical position-value changes against `accountToken` and `snapshotToken` from holdings or revision history. Supply `scenario` (or `scenarios`) with `id`, `composition` (`additive` or `compounded`), and `shocks:[{"instrumentId":"…","percentChange":"-10"}]`. Percentages are exact strings; repeated shocks combine under the selected rule. Results retain the submitted assumptions and original snapshot clocks. Cash and unshocked positions stay unchanged; no forecasts, fees or trades are implied. Successful planning calculations return `calculationToken` and `calculatedAtUnixNanos`; saving is a separate action. |
| `portfolio rebalance <request>` | Selected `accountToken`, `snapshotToken` and `proposal` with explicit `targets:[{"instrumentId":"…","targetPercent":"25"}]`, `maxTurnoverPercent`, `minimumCash:{"amount":"50","currency":"USD"}` and `allowShort`. Percentages/amounts are strings; every held investment needs one target and targets total 100% of portfolio value including cash. The backend applies exact cash/turnover constraints and returns proposed value changes, projected cash and original assumptions. Nonterminating adjustments use conservative decimal rounding; `constrained` includes precision effects. `allowShort` permits retaining existing shorts, never opening new shorts through rounding. No executable quantities, fees or trade authority are inferred. |
| `portfolio position-impact <request>` | Explicit `accountToken`, canonical `instrumentId`, `proposedQuantity` and `scenarioShockPercent`. Quantity and percentage are exact strings; quantity is the desired total position, with zero meaning exit, and `-10` means a hypothetical 10% price decline. Uses the chosen account's current observation and fresh selected price without changing recommendation settings. Returns original assumptions, observation dates and evidence identity. Only the chosen investment is revalued; other holdings retain reported values. Cash transfer is assumed before costs; settlement capacity and trade permission are not inferred. |
| `portfolio save-planning-result --account <token> --calculation-token <uuid> --confirm` | Save a completed scenario, scenario batch, rebalance or position comparison without recalculating it. Repeating Save returns the original saved marker and time. |
| `portfolio planning-results --account <token> [--cursor <cursor>] [--limit <count>]` | List saved calculations for that account, default 25 per page. Continuation retains the original listing fence. |
| `portfolio planning-result --account <token> --saved-result-token <uuid>` | Reopen the original assumptions, calculation and observation times after restart or later imports. Original prices remain historical evidence, not a fresh trading instruction. |
| `backtest run <request> --confirm`; `backtest show <run>` | CLI-owned governed-input registration followed by a bounded backtest request, or one result. |
| `bot status`; `bot preparation` | Current paper state or available market, virtual-cash, trading-cost and practice-mode choices. Choices come from the service; none is silently selected. |
| `bot prepare --market-choice <token> --cash-choice <token> --cost-choice <token> --mode-choice <token>` | Prepare the exact selected session and return its short-lived confirmation token. |
| `bot start --confirmation-token <token> --confirm [--seconds <n>]`; `bot stop --reason <text> --confirm` | Start the reviewed session or stop it. A timed/interactive start remains attached and stops through `Bot.Stop` when its time expires or it is interrupted. |
| `execution targets [--analysis-action-token <UUID>]` | List eligible active plans, or open one original saved recommendation for paper practice. Expired or unavailable evidence cannot authorize an order. |
| `execution prepare-manual <request>`; `execution submit-manual --confirmation-token <token> --confirm` | Prepare explicit trade choices, review their original recommendation and safeguards, then submit the exact one-use draft. |
| `execution orders`; `execution fills`; `execution cancel <action-token> --confirm` | Paper order/fill reads and risk-mediated cancellation using the returned action token. |
| `fair-value list`; `fair-value measure <request> --confirm`; `fair-value classify <measurement> --confirm`; `fair-value explain <measurement>`; `fair-value evidence <measurement>` | Bounded evidence-bound fair-value workflow. |
| `fair-value approval-status <measurement> --at <RFC3339>` | Approval/revocation state at one exact time. |
| `fair-value approve <measurement> --decision <id> --reviewer <id> --approved-at <RFC3339> --expires-at <RFC3339> --confirm` | Controlled review approval. |

### Durable jobs, operational lifecycle, and guided setup

| Command | Exact arguments and effect |
| --- | --- |
| `job list [--after-job-id <UUID>] [--limit 1..1000]` | Latest job-generation page; default `100`. |
| `job get <UUID>` | Latest sanitized generation. |
| `job watch <UUID> --generation <positive> [--after-sequence <n>] [--limit 1..1000]` | Ordered event page; defaults `0`, `100`. |
| `job cancel <UUID> --generation <positive> --expected-sequence <n> --confirm`; `job retry ... --confirm` | Exact-observation fenced mutation. |
| `job confirm <UUID> --generation <positive> --expected-sequence <n> --confirmation-identity <id> --evidence-sha256 <lowercase digest> --confirm` | Exact generation/sequence confirmation. |
| `operations backup list [--after-backup-id <digest>] [--limit 1..64]`; `get <digest>`; `create --confirm`; `verify <digest> --confirm` | Backup inventory/get and durable create/verify. |
| `operations backup retention preview --keep-latest 1..128`; `apply --preview-id <UUID> --preview-digest <digest> --confirm` | Preview-bound retention only. |
| `operations backup restore preview <digest>`; `start --preview-id <UUID> --preview-digest <digest> --confirm` | Fenced fresh-workspace restore only. |
| `operations workspace list [--after-workspace-id <UUID>] [--limit 1..64]`; `switch preview <UUID>`; `switch start <preview args>` | List and preview-bound service-owned switch. |
| `operations update status`; `check --confirm`; `preview`; `start <preview args>` | Trusted update state, staged check, and preview-bound activation. |
| `operations update program-rollback preview`; `start <preview args>` | Program-file rollback only; it is not data restore. |
| `operations logs query` / `export --confirm` | Closed filters `--from`, `--through`, `--minimum-severity`, `--domain`, `--source-id`, `--job-id`, `--correlation-id`, `--search`, `--after-sequence`, and `--limit 1..1000` (default `250`). Export publishes a controlled redacted artifact. |
| `operations settings get`; `change preview --expected-revision <positive> <typed fields>`; `change apply <preview args>`; `rollback preview --expected-revision <positive> --target-revision <positive>`; `rollback apply <preview args>` | Typed settings only. Fields are log retention `1..365`, severity, update channel, automatic checks, storage `1073741824..17592186044416`, default query rows `100..1000000`, concurrent jobs `1..64`, freshness `250..600000`, and backup retention `1..64`. |
| `setup status`; `preview [--expected-revision <n>] [--goal <csv/repeated>] [--starter-plan <value>]`; `apply --preview-id <UUID> --preview-sha256 <digest> --confirm` | Closed, workspace-bound guided plan. Goals and starters are Clap enums; defaults are `everything-recommended`. Preview/acceptance do not claim all steps are complete. |

Every preview-bound operation requires the exact non-nil preview UUID, lowercase SHA-256 digest,
and `--confirm`; stale previews fail rather than being reapplied. Jobs, workspace switches, updates,
backups, restores, and logs return typed receipts or controlled artifacts rather than shell paths.

## MCP relay and client registration

The installed registration target is the package relay, not `market-squawk mcp` directly:

```text
market-squawk-mcp-relay --client <claude|codex> [--data-dir <PATH>] [--config <PATH>]
```

It resolves the authenticated service rendezvous and that named client's credential through native
secret authority, then relays bounded stdio JSON-RPC to the service's local `/mcp`. It does no
catalog, model, source, job, or application work and never puts a bearer credential in client
configuration or argv. The public compatibility command is:

```text
market-squawk mcp serve --client <claude-code|codex>
```

Bare `market-squawk mcp` now fails with the same requirement; it is not a standalone server. Setup
and repair own official Claude Code/Codex registration, use the logical name `market-squawk`, and
refuse to overwrite an unrelated same-name registration. See [MCP reference](mcp.md).

## Output, authority, and hidden compatibility commands

Normal command results use human output or JSON according to `--output`; errors are non-successful
process exits and do not disclose secrets or uncontrolled paths. MCP stdio reserves stdout for
frames. `mock`, `paper-bot`, and `replay` remain hidden diagnostic/v0.1 compatibility commands and
are intentionally not an installed-product automation interface.

The CLI has no raw SQL outside `query sql`, raw configuration editor, arbitrary shell/filesystem
authority, raw service port/token option, unrestricted database query, direct order submit, or
risk bypass.

## Related references

- [Configuration reference](configuration.md)
- [MCP reference](mcp.md)
- [Installation and bootstrap](../operations/installation-and-bootstrap.md)
- [CLI definition](../../apps/market-squawk/src/cli.rs)
- [CLI transport](../../apps/market-squawk/src/local_product/cli_transport.rs)
