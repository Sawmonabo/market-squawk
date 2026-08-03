# Command-line interface reference

This is the installed `market-squawk` command contract. The CLI is a separately authenticated
client of the one per-user service for product commands; it does not start a second catalog, MCP
server, job authority, or paper runtime. Use [MCP](mcp.md) when an automation needs the complete
typed operation registry rather than this operator-oriented command projection.

| Field | Value |
| --- | --- |
| Document type | Reference |
| Status | Current implementation contract |
| Last substantive review | 2026-08-03 |
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
`source`, `ingest`, `dataset`, `query`, `feature`, `model`, `portfolio`, `backtest`, `bot`,
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
| `source register <provider> --confirm`; `source setup <provider> --confirm` | Register a code-supported profile or start/resume its bounded onboarding flow. |
| `source status [provider]`; `source coverage [provider]`; `source health [provider]` | Bounded provider status, explicit coverage, or connection/integrity/freshness facts. |
| `source discover <provider> --dataset <dataset>` | Bounded object list without ingestion authority. |
| `source inspect <provider> --onboarding-session-id <UUID> --dataset-identifier <dataset> [--page-index 0..63] [--max-records 1..1024]` | One non-persisting provider page; defaults are `0` and `256`. |
| `source activate <request> --confirm` | CLI-owned, confined, versioned provider activation request. |
| `ingest source <provider> <object> --dataset <dataset> --confirm` | Mints the exact discovery receipt then uses it for source ingestion. |
| `ingest file <manifest> --object <id> --dataset <id> --confirm` | CLI-owned confined local-file manifest admission. |
| `dataset list [--after-dataset <id>]`; `dataset manifest <dataset>` | Bounded immutable dataset inventory or one manifest. |
| `dataset build <request> --confirm`; `feature build <request> --confirm` | CLI-owned confined typed point-in-time dataset request and immutable publication. |
| `feature list [--after-dataset <id>]` | Registered feature contracts and immutable feature datasets. |
| `query dataset <dataset> [--maximum-rows <n>]` | Bounded dataset-history read; default row request is `1000`. |
| `query sql --dataset <dataset> <statement> [--maximum-rows <n>]` | CLI-only bounded, read-only DataFusion SQL. It does not exist as an MCP tool. |
| `query artifact --artifact-id <id> --sha256 <digest> --byte-count <n> [--media-type <type>] [--offset <n>] [--maximum-bytes <n>]` | Digest-verified artifact chunk; defaults are `application/json`, `0`, and `32768` bytes. |

CLI SQL has fixed limits: 64 KiB statement text, 1,000 default requested rows, 256 KiB inline
Arrow IPC, 64 MiB complete result, 256 MiB query memory, four partitions, 2,048 syntax-tree nodes,
4,096 plan nodes, and 60 seconds. A result above inline and within the complete ceiling is a
path-free Parquet artifact reference with `artifactId`, `sha256`, `byteCount`, `mediaType`, and
`rowCount`; retrieve it through `query artifact`.

### Models, portfolio, backtests, paper, and fair value

| Command | Exact arguments and effect |
| --- | --- |
| `model list`; `model metadata <model>` | Admitted immutable model bundles or one bundle's validation metadata. |
| `model admit <request> --confirm` | CLI-owned verified model-admission request. |
| `model evaluate <request> --confirm`; `model predict <request>` | Confined model-input object. Prediction failure produces no automatic action. |
| `portfolio import <path> --account <id> --confirm` | CLI-owned confined portfolio import. |
| `portfolio holdings --account <id>`; `portfolio transactions --account <id>` | Bounded current holdings or normalized transactions. |
| `portfolio performance <request>`; `portfolio exposure <request>`; `portfolio risk <request>` | Confined typed point-in-time request object. |
| `backtest run <request> --confirm`; `backtest show <run>` | CLI-owned governed-input registration followed by a bounded backtest request, or one result. |
| `bot status`; `bot start --confirm [--provider <coinbase|coinbase-direct|kraken>] [--provider-session-id <UUID>] [--seconds <n>] [--initial-cash <decimal>] [--fee-basis-points <n>]`; `bot stop --reason <text> --confirm` | Paper lifecycle. The provider defaults to `coinbase`, cash to `100000`, and fee basis points to `100`; `coinbase-direct` requires its exact active session. A timed/interactive start stops through the typed `Bot.Stop` path. |
| `execution orders`; `execution fills`; `execution cancel <order> --confirm`; `execution reconcile --confirm` | Paper order/fill reads and risk-mediated cancel/reconciliation. |
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
