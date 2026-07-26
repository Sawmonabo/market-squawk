//! Typed command-line contract for the local control plane.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use market_squawk_platform::JournalFileFormat;
use rust_decimal::Decimal;
use uuid::Uuid;

/// Market Squawk's complete local command-line surface.
#[derive(Debug, Parser)]
#[command(name = "market-squawk")]
#[command(about = "Local-first market data, research, analytics, and paper execution")]
#[command(version)]
pub struct Cli {
    /// Local Market Squawk data root.
    #[arg(long, global = true)]
    pub data_dir: Option<PathBuf>,

    /// Explicit local configuration file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Local tracing filter.
    #[arg(long, env = "MARKET_SQUAWK_LOG", default_value = "info", global = true)]
    pub log: String,

    /// Render local tracing as JSON.
    #[arg(long, global = true)]
    pub json_logs: bool,

    /// Render command results for people or structured consumers.
    #[arg(long, value_enum, default_value_t = OutputFormat::Human, global = true)]
    pub output: OutputFormat,

    /// Source-task cancellation deadline in milliseconds.
    #[arg(long, global = true)]
    pub source_shutdown_ms: Option<u64>,

    /// Absolute installed Python training-release root used to verify admitted models.
    #[arg(long, global = true)]
    pub training_release_root: Option<PathBuf>,

    /// Fixed raw-capture queue depth.
    #[arg(long, global = true)]
    pub capture_queue_capacity: Option<usize>,

    /// Unified per-channel capture memory ceiling in bytes.
    #[arg(long, global = true)]
    pub capture_memory_ceiling_bytes: Option<usize>,

    /// Process-wide capture destination-registry memory ceiling in bytes.
    #[arg(long, global = true)]
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
    Init,

    /// Inspect and validate effective configuration.
    Config {
        /// Configuration operation.
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// Register, inspect, and configure provider sources.
    Source {
        /// Source operation.
        #[command(subcommand)]
        command: SourceCommand,
    },

    /// Capture direct Coinbase Exchange data into the local journal.
    Capture(CaptureArguments),

    /// Ingest user-authorized local or provider data.
    Ingest {
        /// Ingestion operation.
        #[command(subcommand)]
        command: IngestCommand,
    },

    /// Build and inspect immutable analytical datasets.
    Dataset {
        /// Dataset operation.
        #[command(subcommand)]
        command: DatasetCommand,
    },

    /// Query controlled analytical datasets.
    Query {
        /// Query operation.
        #[command(subcommand)]
        command: QueryCommand,
    },

    /// Inspect and build registered features.
    Feature {
        /// Feature operation.
        #[command(subcommand)]
        command: FeatureCommand,
    },

    /// Inspect, evaluate, and run admitted local models.
    Model {
        /// Model operation.
        #[command(subcommand)]
        command: ModelCommand,
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

    /// Create and inspect evidence-bound fair-value measurements.
    FairValue {
        /// Fair-value operation.
        #[command(subcommand)]
        command: FairValueCommand,
    },

    /// Produce and close exact-head release evidence.
    Release {
        /// Release operation.
        #[command(subcommand)]
        command: ReleaseCommand,
    },

    /// Run the local stdio MCP server.
    Mcp {
        /// MCP operation. Omitting it retains the v0.1 `mcp` compatibility form.
        #[command(subcommand)]
        command: Option<McpCommand>,
    },

    /// Report bounded local readiness, configuration provenance, and release blockers.
    Doctor,

    /// Run a deterministic diagnostic feed.
    #[command(hide = true)]
    Mock(MockArguments),

    /// Run the v0.1 paper-bot compatibility command.
    #[command(hide = true)]
    PaperBot(PaperBotArguments),

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

/// Model-registry and inference operation.
#[derive(Debug, Subcommand)]
pub enum ModelCommand {
    /// List admitted immutable model bundles.
    List,
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

/// Portfolio operation.
#[derive(Debug, Subcommand)]
pub enum PortfolioCommand {
    /// Import and reconcile a confined holdings or transactions export.
    Import {
        /// Confined provider export.
        path: PathBuf,
        /// Destination account identity.
        #[arg(long)]
        account: String,
        /// Explicit local mutation confirmation.
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

/// Governed-backtest operation.
#[derive(Debug, Subcommand)]
pub enum BacktestCommand {
    /// Run one admitted point-in-time experiment.
    Run {
        /// Confined JSON request file.
        request: PathBuf,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Inspect one immutable experiment result.
    Show {
        /// Experiment or run identity.
        run: String,
    },
}

/// Paper-bot lifecycle operation.
#[derive(Debug, Subcommand)]
pub enum BotCommand {
    /// Report lifecycle, source qualification, risk, and paper state.
    Status,
    /// Start controlled local paper operation.
    Start {
        /// Controlled paper-run parameters.
        #[command(flatten)]
        paper: PaperBotArguments,
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
    /// Cancel one existing paper order through risk-controlled dispatch.
    Cancel {
        /// Paper order identity.
        order: String,
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
    /// Reconcile paper orders, fills, balances, and positions.
    Reconcile {
        /// Explicit local mutation confirmation.
        #[arg(long)]
        confirm: bool,
    },
}

/// Fair-value operation.
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
    Serve,
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
    /// New empty directory that will own provider evidence.
    #[arg(
        id = "provider_output_directory",
        long = "output-directory",
        value_name = "OUTPUT_DIRECTORY"
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

/// Production paper-composition arguments.
#[derive(Debug, Args)]
pub struct PaperBotArguments {
    /// Configured direct source.
    #[arg(long, value_enum, default_value_t = ProductionSourceArgument::Coinbase)]
    pub provider: ProductionSourceArgument,
    /// Exact active provider-onboarding session; required only for Coinbase Direct.
    #[arg(long, required_if_eq("provider", "coinbase-direct"))]
    pub provider_session_id: Option<Uuid>,
    /// Stop after this many seconds; omit to run until interrupted.
    #[arg(long)]
    pub seconds: Option<u64>,
    /// Virtual starting cash in the configured common quote currency.
    #[arg(long, default_value = "100000")]
    pub initial_cash: Decimal,
    /// Maker and taker fee assumption for local paper execution.
    #[arg(long, default_value_t = 100)]
    pub fee_basis_points: u32,
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

/// Direct source selectable by controlled local paper operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ProductionSourceArgument {
    /// Coinbase Exchange.
    Coinbase,
    /// Authenticated Coinbase Exchange Direct Market Data.
    CoinbaseDirect,
    /// Kraken book-v2.
    Kraken,
}
