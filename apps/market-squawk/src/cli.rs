//! Typed command-line contract for the local control plane.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use market_squawk_platform::JournalFileFormat;
use uuid::Uuid;

/// Market Squawk's complete local command-line surface.
#[derive(Debug, Parser)]
#[command(name = "market-squawk")]
#[command(about = "Self-hosted market data, research, analytics, and paper execution")]
#[command(version)]
pub struct Cli {
    /// Local Market Squawk data root.
    #[arg(long, global = true, hide = true)]
    pub data_dir: Option<PathBuf>,

    /// Explicit installed-service authority root for isolated verification.
    #[arg(long, global = true, hide = true)]
    pub installation_data_root: Option<PathBuf>,

    /// Explicit local configuration file.
    #[arg(long, global = true, hide = true)]
    pub config: Option<PathBuf>,

    /// Local tracing filter.
    #[arg(
        long,
        env = "MARKET_SQUAWK_LOG",
        default_value = "info",
        global = true,
        hide = true
    )]
    pub log: String,

    /// Render local tracing as JSON.
    #[arg(long, global = true, hide = true)]
    pub json_logs: bool,

    /// Render command results for people or structured consumers.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    pub output: OutputFormat,

    /// Source-task cancellation deadline in milliseconds.
    #[arg(long, global = true, hide = true)]
    pub source_shutdown_ms: Option<u64>,

    /// Absolute installed Python training-release root used to verify admitted models.
    #[arg(long, global = true, hide = true)]
    pub training_release_root: Option<PathBuf>,

    /// Fixed raw-capture queue depth.
    #[arg(long, global = true, hide = true)]
    pub capture_queue_capacity: Option<usize>,

    /// Unified per-channel capture memory ceiling in bytes.
    #[arg(long, global = true, hide = true)]
    pub capture_memory_ceiling_bytes: Option<usize>,

    /// Process-wide capture destination-registry memory ceiling in bytes.
    #[arg(long, global = true, hide = true)]
    pub capture_destination_registry_memory_ceiling_bytes: Option<usize>,

    /// Selected operation.
    #[command(subcommand)]
    pub command: Command,
}

/// Result rendering mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputFormat {
    /// Concise operator-oriented output.
    Human,
    /// Stable structured JSON output.
    Json,
}

/// Top-level local control-plane command.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Initialize controlled local state.
    #[command(hide = true)]
    Init,

    /// Inspect and validate effective configuration.
    #[command(hide = true)]
    Config {
        /// Configuration operation.
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// Register, inspect, and configure provider sources.
    #[command(hide = true)]
    Source {
        /// Source operation.
        #[command(subcommand)]
        command: SourceCommand,
    },

    /// Read the bounded unified market view.
    Market {
        /// Market operation.
        #[command(subcommand)]
        command: MarketCommand,
    },

    /// Show the economic and interest-rate backdrop at one optional point in time.
    EconomicContext {
        /// RFC 3339 instant defining what information was known.
        #[arg(long, requires = "effective_date_cutoff")]
        knowledge_cutoff: Option<String>,
        /// Latest effective date admitted into the context, in YYYY-MM-DD form.
        #[arg(long, requires = "knowledge_cutoff")]
        effective_date_cutoff: Option<String>,
    },

    /// Capture direct Coinbase Exchange data into the local journal.
    #[command(hide = true)]
    Capture(CaptureArguments),

    /// Ingest user-authorized local or provider data.
    #[command(hide = true)]
    Ingest {
        /// Ingestion operation.
        #[command(subcommand)]
        command: IngestCommand,
    },

    /// Build and inspect immutable analytical datasets.
    #[command(hide = true)]
    Dataset {
        /// Dataset operation.
        #[command(subcommand)]
        command: DatasetCommand,
    },

    /// Query controlled analytical datasets.
    #[command(hide = true)]
    Query {
        /// Query operation.
        #[command(subcommand)]
        command: QueryCommand,
    },

    /// Inspect and build registered features.
    #[command(hide = true)]
    Feature {
        /// Feature operation.
        #[command(subcommand)]
        command: FeatureCommand,
    },

    /// Inspect and operate low-level model evidence for diagnostics.
    #[command(name = "diagnostics-model", hide = true)]
    Model {
        /// Diagnostic model operation.
        #[command(subcommand)]
        command: ModelCommand,
    },

    /// Prepare, start, and inspect investment forecasts.
    Forecast {
        /// Forecast operation.
        #[command(subcommand)]
        command: ForecastCommand,
    },

    /// Import, inspect, and analyze portfolios.
    Portfolio {
        /// Portfolio operation.
        #[command(subcommand)]
        command: PortfolioCommand,
    },

    /// Run and inspect governed research backtests.
    Backtest {
        /// Backtest operation.
        #[command(subcommand)]
        command: BacktestCommand,
    },

    /// Control local paper-operation lifecycle.
    Bot {
        /// Bot operation.
        #[command(subcommand)]
        command: BotCommand,
    },

    /// Inspect and control risk-approved paper execution.
    Execution {
        /// Execution operation.
        #[command(subcommand)]
        command: ExecutionCommand,
    },

    /// Inspect and operate low-level valuation evidence for diagnostics.
    #[command(name = "diagnostics-fair-value", hide = true)]
    FairValue {
        /// Diagnostic valuation operation.
        #[command(subcommand)]
        command: FairValueCommand,
    },

    /// Inspect or start the shared installed application service.
    #[command(hide = true)]
    Service {
        /// Installed-service operation.
        #[command(subcommand)]
        command: ServiceCommand,
    },

    /// Inspect and control durable work owned by the installed service.
    #[command(hide = true)]
    Job {
        /// Durable-job operation.
        #[command(subcommand)]
        command: JobCommand,
    },

    /// Back up, restore, update, inspect logs, and manage typed product settings.
    #[command(hide = true)]
    Operations {
        /// Installed-product operation.
        #[command(subcommand)]
        command: OperationsCommand,
    },

    /// Inspect, preview, and accept the guided first-run setup plan.
    #[command(hide = true)]
    Setup {
        /// Guided setup operation.
        #[command(subcommand)]
        command: SetupCommand,
    },

    /// Produce and close exact-head release evidence.
    #[command(hide = true)]
    Release {
        /// Release operation.
        #[command(subcommand)]
        command: ReleaseCommand,
    },

    /// Run the local stdio MCP server.
    #[command(hide = true)]
    Mcp {
        /// MCP operation.
        #[command(subcommand)]
        command: McpCommand,
    },

    /// Report bounded local readiness, configuration provenance, and release blockers.
    #[command(hide = true)]
    Doctor,

    /// Run a deterministic diagnostic feed.
    #[command(hide = true)]
    Mock(MockArguments),

    /// Validate the v0.1 immutable diagnostic journal.
    #[command(hide = true)]
    Replay(ReplayArguments),
}

/// Effective-configuration operation.
#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Show the effective redacted configuration and value provenance.
    Show,
    /// Validate configuration, paths, endpoint policy, and artifact confinement.
    Validate,
}

/// Provider-source operation.
#[derive(Debug, Subcommand)]
pub enum SourceCommand {
    /// Import the exact provider credential bundle through the protected installed service.
    ImportCredentials {
        /// Filled copy of `market-squawk-provider-credentials.env.example`.
        bundle: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Register a code-supported provider profile in the local catalog.
    Register {
        /// Code-owned provider identifier.
        provider: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Report configured source state.
    Status {
        /// Optional provider filter.
        provider: Option<String>,
    },
    /// Report explicit provider and instrument coverage.
    Coverage {
        /// Optional provider filter.
        provider: Option<String>,
    },
    /// Report bounded source connection and data health.
    Health {
        /// Optional provider filter.
        provider: Option<String>,
    },
    /// Start or resume evidence-bound local provider onboarding.
    Setup {
        /// Code-owned provider identifier.
        provider: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Activate one evidence-bound provider adapter after onboarding verification.
    Activate {
        /// Confined versioned provider-activation request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// List exact provider objects without minting ingestion authority.
    Discover {
        /// Active configured provider identifier.
        provider: String,
        /// Exact provider dataset namespace.
        #[arg(long)]
        dataset: String,
    },
    /// Inspect one bounded provider page without persisting it as a research dataset.
    Inspect {
        /// Active configured provider identifier.
        provider: String,
        /// Exact active onboarding session that owns the supplied credential.
        #[arg(long)]
        onboarding_session_id: Uuid,
        /// Exact provider dataset identifier.
        #[arg(long)]
        dataset_identifier: String,
        /// Zero-based provider page index.
        #[arg(
            long,
            default_value_t = 0,
            value_parser = clap::value_parser!(u8).range(..=63)
        )]
        page_index: u8,
        /// Maximum provider observations returned from the selected page.
        #[arg(
            long,
            default_value_t = 256,
            value_parser = clap::value_parser!(u16).range(1..=1024)
        )]
        max_records: u16,
    },
}

/// Unified market-data operation.
#[derive(Debug, Subcommand)]
pub enum MarketCommand {
    /// Return provider-neutral current-market summaries selected by Market Squawk.
    Overview {
        /// Continue from an opaque Market page token.
        #[arg(long)]
        page_token: Option<String>,
    },
    /// Find investments by canonical product name.
    Search {
        #[arg(long)]
        query: String,
        #[arg(long)]
        page_token: Option<String>,
    },
    /// Return current information for one opaque investment selection.
    Select {
        #[arg(long)]
        selection_token: String,
    },
    /// Return immutable daily history for one opaque investment selection.
    History {
        #[arg(long)]
        history_token: String,
    },
}

/// Direct capture arguments.
#[derive(Debug, Args)]
pub struct CaptureArguments {
    /// Coinbase products to capture.
    #[arg(long, value_delimiter = ',', default_value = "BTC-USD")]
    pub products: Vec<String>,
    /// Stop after this many seconds; omit to run until interrupted.
    #[arg(long)]
    pub seconds: Option<u64>,
    /// Enable local paper simulation.
    #[arg(long)]
    pub paper_bot: bool,
}

/// Research-ingestion operation.
#[derive(Debug, Subcommand)]
pub enum IngestCommand {
    /// Ingest one object from a confined user-authorized local-file manifest.
    File {
        /// Versioned file-adapter manifest; its parent is the authorized input root.
        manifest: PathBuf,
        /// Exact object identity declared by the manifest.
        #[arg(long)]
        object: String,
        /// Exact dataset identity declared by the manifest.
        #[arg(long)]
        dataset: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Extract and ingest one object from a configured provider.
    Source {
        /// Configured provider identifier.
        provider: String,
        /// Provider object or series identifier.
        object: String,
        /// Destination dataset identity.
        #[arg(long)]
        dataset: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Analytical-dataset operation.
#[derive(Debug, Subcommand)]
pub enum DatasetCommand {
    /// List bounded immutable datasets.
    List {
        /// Continue after this dataset identity from the preceding bounded page.
        #[arg(long)]
        after_dataset: Option<String>,
    },
    /// Inspect one exact dataset manifest.
    Manifest {
        /// Dataset identity.
        dataset: String,
    },
    /// Build a point-in-time feature/label generation from a typed request file.
    Build {
        /// Confined JSON request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Analytical-query operation.
#[derive(Debug, Subcommand)]
pub enum QueryCommand {
    /// Read one verified bounded chunk from an opaque local analytical artifact.
    Artifact {
        /// Opaque artifact identifier returned by Market Squawk.
        #[arg(long)]
        artifact_id: String,
        /// Lowercase SHA-256 digest returned with the artifact reference.
        #[arg(long)]
        sha256: String,
        /// Exact complete artifact size returned with the artifact reference.
        #[arg(long)]
        byte_count: usize,
        /// Registered artifact media type.
        #[arg(long, default_value = "application/json")]
        media_type: String,
        /// Zero-based byte offset for this chunk.
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Maximum raw bytes returned in this chunk.
        #[arg(long, default_value_t = 32 * 1024)]
        maximum_bytes: usize,
    },
    /// Run bounded read-only DataFusion SQL. This operation is CLI-only.
    Sql {
        /// Exact immutable dataset generation to expose as the query relation.
        #[arg(long)]
        dataset: String,
        /// Read-only SQL statement.
        statement: String,
        /// Maximum returned rows.
        #[arg(long, default_value_t = 1_000)]
        maximum_rows: usize,
    },
    /// Read one dataset through its immutable manifest authority.
    Dataset {
        /// Dataset identity.
        dataset: String,
        /// Maximum returned rows.
        #[arg(long, default_value_t = 1_000)]
        maximum_rows: usize,
    },
    /// Report whether the exact desired FRED/ALFRED generation is ready for local reads.
    FredAlfredStatus,
    /// Read one latest-known FRED/ALFRED observation from an exact immutable generation.
    FredAlfredLatestKnown {
        /// Exact immutable manifest version returned by `fred-alfred-status`.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        manifest_version: u64,
        /// Exact immutable schema name returned by `fred-alfred-status`.
        #[arg(long)]
        schema_name: String,
        /// Exact immutable schema version returned by `fred-alfred-status`.
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
        schema_version: u16,
        /// Exact lowercase schema SHA-256 returned by `fred-alfred-status`.
        #[arg(long)]
        schema_fingerprint: String,
        /// Exact lowercase manifest-content SHA-256 returned by `fred-alfred-status`.
        #[arg(long)]
        content_hash: String,
        /// RFC 3339 knowledge cutoff for the point-in-time read.
        #[arg(long)]
        knowledge_cutoff: String,
        /// Effective-date cutoff in YYYY-MM-DD form.
        #[arg(long)]
        effective_date_cutoff: String,
    },
}

/// Feature-registry operation.
#[derive(Debug, Subcommand)]
pub enum FeatureCommand {
    /// List registered versioned feature definitions.
    List {
        /// Continue after this durable feature-dataset identity.
        #[arg(long)]
        after_dataset: Option<String>,
    },
    /// Build features from a confined typed request file.
    Build {
        /// Confined JSON request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Low-level model diagnostic operation.
#[derive(Debug, Subcommand)]
pub enum ModelCommand {
    /// List model evidence and its usable analytical limits.
    List,
    /// List current model-training and forecasting activity.
    Activity,
    /// Admit one verified immutable model bundle through a closed request file.
    Admit {
        /// Confined JSON admission request file.
        request: PathBuf,
        /// Explicit durable-admission confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Inspect one bundle's complete metadata and admission state.
    Metadata {
        /// Model bundle identity.
        model: String,
    },
    /// Evaluate a model through a confined typed request file.
    Evaluate {
        /// Confined JSON request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Run bounded local prediction through a confined typed request file.
    Predict {
        /// Confined JSON request file.
        request: PathBuf,
    },
}

/// Investment-forecast operation.
#[derive(Debug, Subcommand)]
pub enum ForecastCommand {
    /// Show the currently available forecast choices.
    Options,
    /// Preview one forecast and receive its one-use confirmation token.
    Preview {
        /// Opaque model choice returned by `forecast options`.
        #[arg(long)]
        model_token: Uuid,
        /// Opaque history choice returned by `forecast options`.
        #[arg(long)]
        history_token: Uuid,
        /// Opaque investment choice returned by `forecast options`.
        #[arg(long)]
        investment_token: Uuid,
        /// Opaque horizon choice returned by `forecast options`.
        #[arg(long)]
        horizon_token: Uuid,
    },
    /// Start the forecast accepted in one preparation preview.
    Start {
        /// One-use confirmation token returned by `forecast preview`.
        #[arg(long)]
        confirmation_token: Uuid,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// List current forecasts.
    List,
    /// Show one forecast.
    Show {
        /// Opaque forecast token returned by `forecast list`.
        forecast_token: Uuid,
    },
    /// Show the realized outcomes for one forecast.
    Outcomes {
        /// Opaque forecast token returned by `forecast list`.
        forecast_token: Uuid,
    },
}

/// Portfolio operation.
#[derive(Debug, Subcommand)]
pub enum PortfolioCommand {
    /// List named portfolios and their opaque product tokens.
    Accounts {
        /// Continue after one opaque portfolio token.
        #[arg(long)]
        after_account_token: Option<String>,
    },
    /// Import a selected portfolio file through review and approval.
    #[command(name = "import")]
    ImportFlow {
        /// Portfolio import step.
        #[command(subcommand)]
        command: PortfolioImportCommand,
    },
    /// Run the low-level portfolio manifest importer for diagnostics.
    #[command(name = "diagnostics-import", hide = true)]
    Import {
        /// Confined diagnostic manifest.
        path: PathBuf,
        /// Destination portfolio identity.
        #[arg(long)]
        account: String,
        /// Explicit diagnostic mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Report current holdings.
    Holdings {
        /// Exact account identity.
        #[arg(long)]
        account: String,
    },
    /// Report normalized transactions.
    Transactions {
        /// Exact account identity.
        #[arg(long)]
        account: String,
    },
    /// Measure point-in-time portfolio performance.
    Performance {
        /// Confined JSON request file.
        request: PathBuf,
    },
    /// Measure point-in-time portfolio exposure.
    Exposure {
        /// Confined JSON request file.
        request: PathBuf,
    },
    /// Measure point-in-time portfolio risk.
    Risk {
        /// Confined JSON request file.
        request: PathBuf,
    },
}

/// Reviewed portfolio-import operation.
#[derive(Debug, Subcommand)]
pub enum PortfolioImportCommand {
    /// Review how one selected file will be interpreted before saving it.
    Preview {
        /// Selected portfolio file.
        path: PathBuf,
        /// Destination portfolio identity.
        #[arg(long)]
        account: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Approve the selected interpretations from one import review.
    Approve {
        /// Opaque review token returned by `portfolio import preview`.
        #[arg(long)]
        review_token: Uuid,
        /// Confined JSON array selecting an interpretation for each reviewed record.
        #[arg(long)]
        interpretations: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Save one approved portfolio import.
    Commit {
        /// Opaque approval token returned by `portfolio import approve`.
        #[arg(long)]
        approval_token: Uuid,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Discard an import review without saving it.
    Discard {
        /// Opaque review token returned by `portfolio import preview`.
        #[arg(long)]
        review_token: Uuid,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Governed-backtest operation.
#[derive(Debug, Subcommand)]
pub enum BacktestCommand {
    /// Show the currently available investment-test choices.
    Options,
    /// Preview one investment test and receive its one-use confirmation token.
    Preview {
        /// Opaque history choice returned by `backtest options`.
        #[arg(long)]
        history_token: Uuid,
        /// Opaque period choice returned by `backtest options`.
        #[arg(long)]
        period_token: Uuid,
        /// Opaque method choice returned by `backtest options`.
        #[arg(long)]
        method_token: Uuid,
        /// Opaque trading-cost choice returned by `backtest options`.
        #[arg(long)]
        cost_token: Uuid,
        /// Opaque portfolio choice returned by `backtest options`.
        #[arg(long)]
        portfolio_token: Uuid,
        /// Opaque comparison choice returned by `backtest options`.
        #[arg(long)]
        comparison_token: Uuid,
    },
    /// Start the investment test accepted in one preparation preview.
    Start {
        /// One-use confirmation token returned by `backtest preview`.
        #[arg(long)]
        confirmation_token: Uuid,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// List backtest activity and available results.
    List,
    /// Inspect one completed backtest result.
    Show {
        /// Opaque backtest result token returned by `backtest list`.
        backtest_token: uuid::Uuid,
    },
}

/// Paper-bot lifecycle operation.
#[derive(Debug, Subcommand)]
pub enum BotCommand {
    /// Report lifecycle, source qualification, risk, and paper state.
    Status,
    /// List the explicit virtual-cash, cost, and mode choices for a paper session.
    Preparation,
    /// Prepare one short-lived paper-session confirmation.
    Prepare {
        /// Opaque virtual-cash choice returned by `bot preparation`.
        #[arg(long)]
        cash_choice: String,
        /// Opaque trading-cost choice returned by `bot preparation`.
        #[arg(long)]
        cost_choice: String,
        /// Opaque practice-mode choice returned by `bot preparation`.
        #[arg(long)]
        mode_choice: String,
    },
    /// Start the exact server-prepared paper session.
    Start {
        /// One-use confirmation token returned by `bot prepare`.
        #[arg(long)]
        confirmation_token: String,
        /// Stop after this many seconds; omit to run until interrupted.
        #[arg(long)]
        seconds: Option<u64>,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Stop controlled local paper operation.
    Stop {
        /// Required audit reason.
        #[arg(long)]
        reason: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Paper execution operation.
#[derive(Debug, Subcommand)]
pub enum ExecutionCommand {
    /// List bounded paper orders and transitions.
    Orders,
    /// List bounded paper fills.
    Fills,
    /// List the active investment plans eligible for a manual virtual order.
    Targets,
    /// Prepare a manual virtual order from an explicit JSON request.
    PrepareManual {
        /// Confined request containing the selected target and every order choice.
        request: PathBuf,
    },
    /// Submit one exact prepared manual virtual order.
    SubmitManual {
        /// One-use confirmation token returned by `execution prepare-manual`.
        #[arg(long)]
        confirmation_token: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Cancel one existing paper order through risk-controlled dispatch.
    Cancel {
        /// Paper order identity.
        action_token: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Run low-level paper-state reconciliation for diagnostics.
    #[command(name = "diagnostics-reconcile", hide = true)]
    Reconcile {
        /// Explicit diagnostic mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Low-level valuation diagnostic operation.
#[derive(Debug, Subcommand)]
pub enum FairValueCommand {
    /// List bounded immutable measurements.
    List,
    /// Create an immutable evidence-bound measurement.
    Measure {
        /// Confined JSON request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Classify an existing immutable measurement.
    Classify {
        /// Measurement identity.
        measurement: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Explain the complete classification decision table.
    Explain {
        /// Measurement identity.
        measurement: String,
    },
    /// Return bounded measurement evidence.
    Evidence {
        /// Measurement identity.
        measurement: String,
    },
    /// Return approval and revocation status at one exact instant.
    ApprovalStatus {
        /// Measurement identity.
        measurement: String,
        /// RFC 3339 status instant.
        #[arg(long)]
        at: String,
    },
    /// Approve an eligible measurement through the controlled workflow.
    Approve {
        /// Measurement identity.
        measurement: String,
        /// Exact classification decision identity.
        #[arg(long)]
        decision: String,
        /// Distinct reviewer identity.
        #[arg(long)]
        reviewer: String,
        /// RFC 3339 approval instant.
        #[arg(long)]
        approved_at: String,
        /// RFC 3339 approval expiry.
        #[arg(long)]
        expires_at: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// MCP operation.
#[derive(Debug, Subcommand)]
pub enum McpCommand {
    /// Serve the bounded local stdio protocol.
    Serve {
        /// Installer-owned MCP client credential used by this stateless relay.
        #[arg(long, value_enum)]
        client: McpClientArgument,
    },
}

/// Named MCP client registration used by the stateless stdio relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum McpClientArgument {
    /// Claude Code user-level MCP registration.
    ClaudeCode,
    /// Codex user-level MCP registration.
    Codex,
}

impl From<McpClientArgument> for market_squawk_runtime::NamedClient {
    fn from(value: McpClientArgument) -> Self {
        match value {
            McpClientArgument::ClaudeCode => Self::ClaudeCode,
            McpClientArgument::Codex => Self::Codex,
        }
    }
}

/// Installed-service operation.
#[derive(Debug, Subcommand)]
pub enum ServiceCommand {
    /// Prove authenticated readiness and show the non-secret bootstrap snapshot.
    Status,
    /// Start the verified installed service sibling and wait for authenticated readiness.
    Start,
    /// Complete the short-lived owner-authenticated credential bootstrap.
    Bootstrap {
        /// Read one bounded unlock from standard input instead of a no-echo terminal prompt.
        #[arg(long, conflicts_with = "complete_foreground_keyring")]
        stdin: bool,
        /// Complete the protected-keyring handoff from this foreground process.
        #[arg(long)]
        complete_foreground_keyring: bool,
    },
}

/// Durable-job operation.
#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List one bounded page of jobs in stable identity order.
    List {
        /// Resume strictly after this job identity.
        #[arg(long)]
        after_job_id: Option<Uuid>,
        /// Maximum jobs returned.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=1000))]
        limit: u16,
    },
    /// Read one exact generation of a job.
    Get {
        /// Durable job identity.
        job_id: Uuid,
        /// Exact one-based execution generation.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
    },
    /// Read one bounded event page after an exact generation cursor.
    Watch {
        /// Durable job identity.
        job_id: Uuid,
        /// Exact one-based execution generation.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        /// Resume strictly after this event sequence; zero begins at the first event.
        #[arg(long, default_value_t = 0)]
        after_sequence: u64,
        /// Maximum events returned.
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=1000))]
        limit: u16,
    },
    /// Request cancellation at the exact observed generation and sequence.
    Cancel {
        /// Durable job identity.
        job_id: Uuid,
        /// Exact one-based execution generation.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        /// Exact latest event sequence observed by the operator.
        #[arg(long)]
        expected_sequence: u64,
        /// Explicitly authorize this mutation.
        #[arg(long)]
        confirm: bool,
    },
    /// Release a confirmation-gated job at the exact observed generation and sequence.
    Confirm {
        /// Durable job identity.
        job_id: Uuid,
        /// Exact one-based execution generation.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        /// Exact latest event sequence observed by the operator.
        #[arg(long)]
        expected_sequence: u64,
        /// Bounded reviewer or approval identity.
        #[arg(long)]
        confirmation_identity: String,
        /// Lowercase SHA-256 of the exact confirmation evidence.
        #[arg(long)]
        evidence_sha256: String,
        /// Explicitly authorize this mutation.
        #[arg(long)]
        confirm: bool,
    },
    /// Start the next admitted retry generation from the exact terminal observation.
    Retry {
        /// Durable job identity.
        job_id: Uuid,
        /// Exact one-based execution generation.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        generation: u64,
        /// Exact latest event sequence observed by the operator.
        #[arg(long)]
        expected_sequence: u64,
        /// Explicitly authorize this mutation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Installed-product operational hierarchy.
#[derive(Debug, Subcommand)]
pub enum OperationsCommand {
    /// Create, inspect, verify, retain, and restore product backups.
    Backup {
        /// Backup operation.
        #[command(subcommand)]
        command: BackupOperationsCommand,
    },
    /// List and switch between local workspaces through the service-owned fence.
    Workspace {
        /// Workspace operation.
        #[command(subcommand)]
        command: WorkspaceOperationsCommand,
    },
    /// Check, preview, activate, and roll back immutable program releases.
    Update {
        /// Update operation.
        #[command(subcommand)]
        command: UpdateOperationsCommand,
    },
    /// Query and export bounded redacted structured logs.
    Logs {
        /// Log operation.
        #[command(subcommand)]
        command: LogOperationsCommand,
    },
    /// Inspect, preview, apply, and roll back typed product settings.
    Settings {
        /// Settings operation.
        #[command(subcommand)]
        command: SettingsOperationsCommand,
    },
}

/// Product-backup operation.
#[derive(Debug, Subcommand)]
pub enum BackupOperationsCommand {
    /// List one bounded page of retained product backups.
    List {
        /// Continue strictly after this lowercase backup SHA-256.
        #[arg(long)]
        after_backup_id: Option<String>,
        /// Maximum backup manifests returned.
        #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u8).range(1..=64))]
        limit: u8,
    },
    /// Return one exact retained product-backup manifest.
    Get {
        /// Exact lowercase backup SHA-256.
        backup_id: String,
    },
    /// Start one durable complete product-backup job.
    Create {
        /// Explicitly authorize this mutation.
        #[arg(long)]
        confirm: bool,
    },
    /// Start durable verification of one exact retained backup.
    Verify {
        /// Exact lowercase backup SHA-256.
        backup_id: String,
        /// Explicitly authorize this mutation.
        #[arg(long)]
        confirm: bool,
    },
    /// Preview and apply bounded backup retention.
    Retention {
        /// Backup-retention operation.
        #[command(subcommand)]
        command: BackupRetentionCommand,
    },
    /// Preview and start a fenced restore into a fresh workspace.
    Restore {
        /// Restore operation.
        #[command(subcommand)]
        command: RestoreCommand,
    },
}

/// Backup-retention preview and application.
#[derive(Debug, Subcommand)]
pub enum BackupRetentionCommand {
    /// Preview the exact backups retained and removed by the policy.
    Preview {
        /// Number of newest verified backups to retain.
        #[arg(long, value_parser = clap::value_parser!(u16).range(1..=128))]
        keep_latest: u16,
    },
    /// Start the durable retention job bound to one exact preview.
    Apply(OperationsPreviewConfirmationArguments),
}

/// Restore preview and start.
#[derive(Debug, Subcommand)]
pub enum RestoreCommand {
    /// Preview restoring one exact backup into a fresh fenced workspace.
    Preview {
        /// Exact lowercase backup SHA-256.
        backup_id: String,
    },
    /// Start the durable restore bound to one exact preview.
    Start(OperationsPreviewConfirmationArguments),
}

/// Local-workspace operation.
#[derive(Debug, Subcommand)]
pub enum WorkspaceOperationsCommand {
    /// List one bounded page of known workspaces and active-generation evidence.
    List {
        /// Continue strictly after this workspace identity.
        #[arg(long)]
        after_workspace_id: Option<Uuid>,
        /// Maximum workspace descriptors returned.
        #[arg(long, default_value_t = 64, value_parser = clap::value_parser!(u8).range(1..=64))]
        limit: u8,
    },
    /// Preview and start a service-owned workspace switch.
    Switch {
        /// Workspace-switch operation.
        #[command(subcommand)]
        command: WorkspaceSwitchCommand,
    },
}

/// Workspace-switch preview and start.
#[derive(Debug, Subcommand)]
pub enum WorkspaceSwitchCommand {
    /// Preview fencing, blockers, reconciliation, and client resynchronization.
    Preview {
        /// Exact target workspace identity.
        workspace_id: Uuid,
    },
    /// Start the durable switch bound to one exact preview.
    Start(OperationsPreviewConfirmationArguments),
}

/// Immutable-program update and rollback operation.
#[derive(Debug, Subcommand)]
pub enum UpdateOperationsCommand {
    /// Return trusted update, known-good generation, and recovery status.
    Status,
    /// Check trusted metadata and stage only an admitted candidate.
    Check {
        /// Explicitly authorize provider contact and candidate staging.
        #[arg(long)]
        confirm: bool,
    },
    /// Preview activation of the currently staged trusted candidate.
    Preview,
    /// Start update activation bound to one exact preview.
    Start(OperationsPreviewConfirmationArguments),
    /// Preview or start rollback of program files without restoring data.
    ProgramRollback {
        /// Program-rollback operation.
        #[command(subcommand)]
        command: ProgramRollbackCommand,
    },
}

/// Program-generation rollback preview and start.
#[derive(Debug, Subcommand)]
pub enum ProgramRollbackCommand {
    /// Preview the known-good program generation and data compatibility.
    Preview,
    /// Start program rollback bound to one exact preview.
    Start(OperationsPreviewConfirmationArguments),
}

/// Exact one-use Operations preview confirmation.
#[derive(Debug, Args)]
pub struct OperationsPreviewConfirmationArguments {
    /// Exact preview UUID returned by the corresponding preview operation.
    #[arg(long)]
    pub preview_id: Uuid,
    /// Lowercase SHA-256 returned with the exact preview.
    #[arg(long)]
    pub preview_digest: String,
    /// Explicitly authorize this preview-bound mutation.
    #[arg(long)]
    pub confirm: bool,
}

/// Structured-log operation.
#[derive(Debug, Subcommand)]
pub enum LogOperationsCommand {
    /// Query one bounded page of redacted structured logs.
    Query(LogQueryArguments),
    /// Publish a bounded redacted log export as a controlled artifact.
    Export {
        /// Exact bounded log selection.
        #[command(flatten)]
        query: LogQueryArguments,
        /// Explicitly authorize controlled artifact publication.
        #[arg(long)]
        confirm: bool,
    },
}

/// Typed filters shared by structured-log query and controlled export.
#[derive(Debug, Args)]
pub struct LogQueryArguments {
    /// Inclusive RFC 3339 lower time bound.
    #[arg(long)]
    pub from: Option<String>,
    /// Inclusive RFC 3339 upper time bound.
    #[arg(long)]
    pub through: Option<String>,
    /// Minimum retained severity.
    #[arg(long, value_enum)]
    pub minimum_severity: Option<LogSeverityArgument>,
    /// Exact product domain filter.
    #[arg(long, value_enum)]
    pub domain: Option<LogDomainArgument>,
    /// Exact source identity filter, bounded by the application contract.
    #[arg(long)]
    pub source_id: Option<String>,
    /// Exact durable-job identity filter, bounded by the application contract.
    #[arg(long)]
    pub job_id: Option<String>,
    /// Exact correlation identity filter, bounded by the application contract.
    #[arg(long)]
    pub correlation_id: Option<String>,
    /// Bounded redacted message search text.
    #[arg(long)]
    pub search: Option<String>,
    /// Continue strictly after this monotonic local log sequence.
    #[arg(long)]
    pub after_sequence: Option<u64>,
    /// Maximum records returned or exported.
    #[arg(long, default_value_t = 250, value_parser = clap::value_parser!(u16).range(1..=1000))]
    pub limit: u16,
}

/// Closed structured-log severity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogSeverityArgument {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Closed structured-log product domain.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogDomainArgument {
    Application,
    Source,
    Market,
    Research,
    Portfolio,
    Model,
    Backtest,
    Execution,
    Risk,
    FairValue,
    Mcp,
    Lifecycle,
}

/// Typed-settings operation.
#[derive(Debug, Subcommand)]
pub enum SettingsOperationsCommand {
    /// Return all effective typed settings, origins, and restart impacts.
    Get,
    /// Preview or apply a typed settings change.
    Change {
        /// Settings-change operation.
        #[command(subcommand)]
        command: SettingsChangeCommand,
    },
    /// Preview or apply restoration of a retained settings revision.
    Rollback {
        /// Settings-rollback operation.
        #[command(subcommand)]
        command: SettingsRollbackCommand,
    },
}

/// Typed settings-change preview and application.
#[derive(Debug, Subcommand)]
pub enum SettingsChangeCommand {
    /// Preview one or more closed typed settings at an exact revision.
    Preview(SettingsChangeArguments),
    /// Apply one exact settings-change preview.
    Apply(OperationsPreviewConfirmationArguments),
}

/// Settings-rollback preview and application.
#[derive(Debug, Subcommand)]
pub enum SettingsRollbackCommand {
    /// Preview restoring a retained revision as a new monotonic revision.
    Preview {
        /// Exact currently observed settings revision.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        expected_revision: u64,
        /// Exact retained settings revision to restore.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        target_revision: u64,
    },
    /// Apply one exact settings-rollback preview.
    Apply(OperationsPreviewConfirmationArguments),
}

/// Closed settings values accepted by `operations settings change preview`.
#[derive(Debug, Args)]
pub struct SettingsChangeArguments {
    /// Exact currently observed settings revision.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub expected_revision: u64,
    /// Structured-log retention in days.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=365))]
    pub log_retention_days: Option<u16>,
    /// Minimum structured-log severity.
    #[arg(long, value_enum)]
    pub log_minimum_severity: Option<LogSeverityArgument>,
    /// Product-owned update stream.
    #[arg(long, value_enum)]
    pub update_channel: Option<UpdateChannelArgument>,
    /// Whether the service performs disclosed bounded automatic update checks.
    #[arg(long)]
    pub automatic_update_checks: Option<bool>,
    /// Workspace soft storage limit in bytes.
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1_073_741_824..=17_592_186_044_416)
    )]
    pub storage_soft_limit_bytes: Option<u64>,
    /// Default bounded analytical-query row limit.
    #[arg(long, value_parser = clap::value_parser!(u32).range(100..=1_000_000))]
    pub default_query_row_limit: Option<u32>,
    /// Maximum concurrently running durable jobs.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=64))]
    pub maximum_concurrent_jobs: Option<u16>,
    /// Market-data freshness threshold in milliseconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(250..=600_000))]
    pub market_freshness_millis: Option<u64>,
    /// Number of verified backups retained by default.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=64))]
    pub backup_retention_count: Option<u16>,
}

/// Closed product-owned update stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum UpdateChannelArgument {
    Stable,
    Preview,
}

/// Guided first-run setup operation.
#[derive(Debug, Subcommand)]
pub enum SetupCommand {
    /// Return the closed setup catalog and exact accepted plan, if any.
    Status,
    /// Preview a complete workspace-bound setup plan without completing any step.
    Preview(SetupPreviewArguments),
    /// Accept one exact one-use setup-plan preview without completing any step.
    Apply(SetupApplyArguments),
}

/// Closed setup-plan selection.
#[derive(Debug, Args)]
pub struct SetupPreviewArguments {
    /// Exact current setup-plan revision; zero selects an unconfigured workspace.
    #[arg(long, default_value_t = 0)]
    pub expected_revision: u64,
    /// One or more closed goals; repeat the option or pass a comma-separated list.
    #[arg(
        long = "goal",
        value_enum,
        value_delimiter = ',',
        default_value = "everything-recommended"
    )]
    pub goals: Vec<SetupGoalArgument>,
    /// One code-owned starter plan compatible with the selected goals.
    #[arg(long, value_enum, default_value = "everything-recommended")]
    pub starter_plan: SetupStarterPlanArgument,
}

/// Exact one-use setup-plan confirmation.
#[derive(Debug, Args)]
pub struct SetupApplyArguments {
    /// Exact non-nil preview UUID returned by `setup preview`.
    #[arg(long)]
    pub preview_id: Uuid,
    /// Exact lowercase SHA-256 returned by `setup preview`.
    #[arg(long)]
    pub preview_sha256: String,
    /// Explicitly authorize acceptance of this exact setup plan.
    #[arg(long)]
    pub confirm: bool,
}

/// Closed setup goal projected into the public application setup contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SetupGoalArgument {
    EverythingRecommended,
    ExplorePublicMarkets,
    ResearchInvestments,
    ManagePortfolio,
    BuildAndEvaluateModels,
    PracticePaperExecution,
    UseClaudeCode,
    UseCodex,
}

/// Closed setup starter plan projected into the public application setup contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SetupStarterPlanArgument {
    EverythingRecommended,
    PublicMarkets,
    Research,
    Portfolio,
    Models,
    PaperPractice,
    AiClients,
}

/// Exact-head release operation.
#[derive(Debug, Subcommand)]
pub enum ReleaseCommand {
    /// Produce or close one class of machine-readable release evidence.
    Evidence {
        /// Evidence operation.
        #[command(subcommand)]
        command: ReleaseEvidenceCommand,
    },
    /// Run the deterministic local all-vertical demonstration.
    Demonstrate(ReleaseDemonstrateArguments),
}

/// Release-evidence operation.
#[derive(Debug, Subcommand)]
pub enum ReleaseEvidenceCommand {
    /// Run the closed parser, protocol, model, and MCP fuzz campaign.
    Fuzz(ReleaseFuzzArguments),
    /// Measure the production live and analytical-storage paths.
    Benchmark(ReleaseBenchmarkArguments),
    /// Execute one parent-supervised benchmark worker without publishing evidence.
    #[command(hide = true)]
    BenchmarkWorker(ReleaseBenchmarkArguments),
    /// Collect authorized evidence from configured provider interfaces.
    Providers(ReleaseProviderArguments),
    /// Bind one successful full verification gate to its exact inputs.
    Gate(ReleaseGateArguments),
    /// Validate and seal a complete exact-head evidence directory.
    Close(ReleaseCloseArguments),
}

/// Repository identity optionally asserted by a provisional producer.
#[derive(Debug, Args)]
pub struct ReleaseRepositoryArguments {
    /// Exact Git commit expected at command start and completion.
    #[arg(long)]
    pub head: Option<String>,
    /// Exact Git tree expected at command start and completion.
    #[arg(long)]
    pub tree: Option<String>,
}

/// Fuzz-campaign arguments.
#[derive(Debug, Args)]
pub struct ReleaseFuzzArguments {
    /// Repository identity asserted for exact-head evidence.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Pinned fuzz-only Rust toolchain.
    #[arg(long, default_value = "nightly-2026-07-15")]
    pub toolchain: String,
    /// Maximum campaign duration for each target.
    #[arg(long, default_value_t = 120)]
    pub seconds_per_target: u64,
    /// Maximum resident memory for each complete target process tree.
    #[arg(long, default_value_t = 2_048)]
    pub rss_limit_mib: u64,
    /// New no-clobber JSON evidence file.
    #[arg(
        id = "fuzz_output_file",
        long = "output-file",
        value_name = "OUTPUT_FILE"
    )]
    pub output: PathBuf,
}

/// Performance-evidence arguments.
#[derive(Debug, Args)]
pub struct ReleaseBenchmarkArguments {
    /// Repository identity asserted for exact-head evidence.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Untimed live-path warm-up event count.
    #[arg(long, default_value_t = 1_000_000)]
    pub warm_up_events: u64,
    /// Measured live-path event count.
    #[arg(long, default_value_t = 60_000_000)]
    pub events: u64,
    /// Measured analytical-storage row count.
    #[arg(long, default_value_t = 10_000_000)]
    pub storage_rows: u64,
    /// Maximum post-warm-up resident-memory growth.
    #[arg(long, default_value_t = 32)]
    pub max_tail_growth_mib: u64,
    /// Maximum post-warm-up resident-memory growth as an integer percentage.
    #[arg(long, default_value_t = 1)]
    pub max_tail_growth_percent: u64,
    /// Minimum accepted complete live-path throughput.
    #[arg(long, default_value_t = 100_000)]
    pub min_events_per_second: u64,
    /// Strict upper bound for warmed complete live-path p99 latency.
    #[arg(long, default_value_t = 999_999)]
    pub max_warmed_p99_ns: u64,
    /// New no-clobber JSON evidence file.
    #[arg(
        id = "benchmark_output_file",
        long = "output-file",
        value_name = "OUTPUT_FILE"
    )]
    pub output: PathBuf,
}

/// Authorized provider-evidence arguments.
#[derive(Debug, Args)]
pub struct ReleaseProviderArguments {
    /// Repository identity asserted for exact-head evidence.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Closed provider set to exercise.
    #[arg(long, value_delimiter = ',', required = true)]
    pub providers: Vec<String>,
    /// Require one authorized source to drive a verified risk-approved paper action.
    #[arg(long)]
    pub require_direct_verified_action: bool,
    /// Require admitted FRED and ALFRED persistence and training rights.
    #[arg(long)]
    pub require_fred_alfred_rights: bool,
    /// Exact zero-padded ten-digit CIK exercised through SEC filings and Company Facts.
    #[arg(long, value_name = "CIK")]
    pub sec_cik: Option<String>,
    /// Exact FRED or ALFRED provider dataset exercised through durable release acceptance.
    #[arg(long, value_name = "PROVIDER_DATASET")]
    pub fred_dataset: Option<String>,
    /// Exact BLS request-plan dataset exercised through durable release acceptance.
    #[arg(long, value_name = "PROVIDER_DATASET")]
    pub bls_dataset: Option<String>,
    /// New empty directory that will own provider evidence.
    #[arg(
        id = "provider_output_directory",
        long = "output-directory",
        value_name = "OUTPUT_DIRECTORY"
    )]
    pub output: PathBuf,
}

/// Full verification-gate receipt arguments.
#[derive(Debug, Args)]
pub struct ReleaseGateArguments {
    /// Exact clean repository identity represented by the completed gate.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Exact release executable represented by the completed gate.
    #[arg(long)]
    pub binary: PathBuf,
    /// Absent no-clobber destination for supervised full-gate output.
    #[arg(long)]
    pub gate_log: PathBuf,
    /// New no-clobber full-gate.json beside the finalized log.
    #[arg(
        id = "full_gate_output_file",
        long = "output-file",
        value_name = "OUTPUT_FILE"
    )]
    pub output: PathBuf,
}

/// Evidence-closure arguments.
#[derive(Debug, Args)]
pub struct ReleaseCloseArguments {
    /// Exact clean repository identity all evidence must bind.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Complete HEAD-keyed release-evidence directory.
    #[arg(long)]
    pub evidence_dir: PathBuf,
    /// Exact release executable represented by the evidence.
    #[arg(long)]
    pub binary: PathBuf,
    /// New no-clobber closed-manifest file inside the evidence directory.
    #[arg(
        id = "closed_manifest_output_file",
        long = "output-file",
        value_name = "OUTPUT_FILE"
    )]
    pub output: PathBuf,
}

/// Deterministic local all-vertical demonstration arguments.
#[derive(Debug, Args)]
pub struct ReleaseDemonstrateArguments {
    /// Repository identity asserted for exact-head evidence.
    #[command(flatten)]
    pub repository: ReleaseRepositoryArguments,
    /// Require the demonstration to deny all external networking.
    #[arg(long)]
    pub offline: bool,
    /// Authorized provider-evidence directory.
    #[arg(long)]
    pub provider_evidence: PathBuf,
    /// Sealed Python release-manifest evidence.
    #[arg(long)]
    pub python_evidence: PathBuf,
    /// New no-clobber JSON evidence file.
    #[arg(
        id = "demonstration_output_file",
        long = "output-file",
        value_name = "OUTPUT_FILE"
    )]
    pub output: PathBuf,
}

/// Diagnostic mock-feed arguments.
#[derive(Debug, Args)]
pub struct MockArguments {
    /// Diagnostic product.
    #[arg(long, default_value = "TEST-USD")]
    pub product: String,
    /// Number of deterministic events.
    #[arg(long, default_value_t = 100)]
    pub events: usize,
    /// Enable local paper simulation.
    #[arg(long)]
    pub paper_bot: bool,
}

/// Diagnostic replay arguments.
#[derive(Debug, Args)]
pub struct ReplayArguments {
    /// Captured source identity.
    #[arg(long, default_value = "coinbase-exchange")]
    pub source: String,
    /// Select a journal when current and legacy formats both exist.
    #[arg(long, value_enum)]
    pub journal_format: Option<JournalFormatArgument>,
}

/// Supported immutable journal format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum JournalFormatArgument {
    /// Current checksummed raw-record format.
    Current,
    /// Read-only legacy journal format.
    Legacy,
}

impl From<JournalFormatArgument> for JournalFileFormat {
    fn from(value: JournalFormatArgument) -> Self {
        match value {
            JournalFormatArgument::Current => Self::Current,
            JournalFormatArgument::Legacy => Self::Legacy,
        }
    }
}
