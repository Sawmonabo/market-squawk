//! Complete local product composition shared by CLI and MCP transports.

mod cli_dataset;
mod cli_model;
mod cli_portfolio;
mod cli_transport;
mod executable;

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureMetadataError,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_backtesting::{ExperimentLimits, ExperimentLimitsInput};
use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
use market_squawk_domain::RoundingPolicy;
use market_squawk_modeling::{TrainingEnvironmentError, verify_validator_training_environment};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths, PreferredSecretStore};
use market_squawk_sources::AuthoritativeSourceRegistry;
use market_squawk_valuation::{FairValueLimitInput, FairValueLimits, FairValueService};
use thiserror::Error;

pub use self::cli_dataset::CliDatasetError;
pub use self::cli_model::CliModelAdmissionError;
pub use self::cli_portfolio::CliPortfolioImportError;
pub use self::cli_transport::{CliProductError, CliProductResult, execute_cli_command};
use self::executable::{
    ExecutableIdentityError, admit_installed_onnx_worker, current_executable_sha256,
};
use crate::application::analysis::{
    AnalysisCatalog, AnalysisDomainService, GovernedBacktestAuthority,
    GovernedBacktestInputAuthorityLimits, GovernedBacktestInputResolver,
    GovernedBacktestRepository, GovernedBacktestRepositoryLimits, ProductionBacktestAuthority,
    ProductionGovernedBacktestInputAuthority, ProductionGovernedBacktestRepository,
};
use crate::application::model::runtime::{
    ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
};
use crate::application::model::{ModelDomainService, ModelDomainServiceError};
use crate::application::{
    Application, ApplicationCompositionError, ApplicationDomainService, FairValueDomainService,
    FairValueInputAuthorityError, FairValueInputAuthorityLimits, PaperApplicationServices,
    ProductionFairValueInputAuthority, ProductionResearchIngestCoordinator,
    ResearchApplicationServices, ResearchExtractionLimits, SourceDomainService,
};
use crate::backtest_service::{ProductionBacktestService, ProductionBacktestServiceError};
use crate::backtest_strategy::{
    BacktestStrategyCompositionError, production_backtest_strategy_registry,
};
use crate::{
    AppConfig, PortfolioApplicationLimits, PortfolioApplicationService,
    PortfolioApplicationServiceError, ProviderAdapterActivation, ProviderOnboardingError,
    ProviderOnboardingService, ResearchService, ResearchServiceError,
};

const SOURCE_AUTHORITY_DIRECTORY: &str = "sources/research-runtime";
const CATALOG_BUSY_TIMEOUT: Duration = Duration::from_millis(750);
const CATALOG_MAXIMUM_ROWS: usize = 10_000;
const CATALOG_MAXIMUM_RECORD_BYTES: usize = 1024 * 1024;
const CATALOG_MAXIMUM_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_DATASET_GENERATIONS: usize = 4_096;
const MAXIMUM_STAGING_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_ROW_GROUP_ROWS: usize = 65_536;
const ORPHAN_GRACE: Duration = Duration::from_secs(60);
const MODEL_EVALUATION_RECORDS: usize = 4_096;
const BATCH_FEATURE_REVISION: &str = "market-squawk-batch-features-v1";

/// Lifecycle owner for every production local authority required by the product surface.
pub struct LocalProduct {
    paths: LocalPaths,
    application: Arc<Application>,
    research: Arc<ResearchService>,
    research_ingest: Arc<ProductionResearchIngestCoordinator>,
    provider_onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    portfolio: Arc<PortfolioApplicationService>,
    backtest_inputs: Arc<ProductionGovernedBacktestInputAuthority>,
    model_runtime: Option<Arc<ProductionModelRuntime>>,
    fair_value_inputs: ProductionFairValueInputAuthority,
}

impl LocalProduct {
    /// Opens or initializes every required local product domain under one application authority.
    ///
    /// Existing model admissions are never represented as an empty registry. If durable models
    /// exist, the configured signed training release and any required sibling ONNX worker must be
    /// available and verified before the application is published.
    pub fn try_new(config: AppConfig) -> Result<Self, LocalProductError> {
        let paths = LocalPaths::prepare(config.data_dir())?;
        let research = Arc::new(open_research(&paths)?);

        let source_store = LocalAuthorityStateStore::try_open(
            paths
                .control_root()?
                .root()
                .join(SOURCE_AUTHORITY_DIRECTORY),
        )?;
        let source_registry = AuthoritativeSourceRegistry::try_new_durable(source_store)?;
        let research_ingest = Arc::new(ProductionResearchIngestCoordinator::new(
            source_registry,
            Arc::clone(&research),
            ResearchExtractionLimits::standard(),
        ));

        let secrets = Arc::new(PreferredSecretStore::try_new("market-squawk", None)?);
        let onboarding = Arc::new(ProviderOnboardingService::try_new(
            research.onboarding_catalog(),
            secrets,
        )?);
        let provider_activation = Arc::new(ProviderAdapterActivation::new(
            Arc::clone(&onboarding),
            Arc::clone(&research_ingest),
            config.clone(),
        ));

        let paper = PaperApplicationServices::new(config.clone());
        let source: Arc<dyn ApplicationDomainService> = Arc::new(SourceDomainService::try_new(
            Arc::clone(&onboarding),
            paper.source_runtime_view(),
        )?);
        let research_domains = ResearchApplicationServices::new(
            Arc::clone(&research),
            Arc::clone(&research_ingest) as Arc<_>,
        );

        let portfolio = Arc::new(PortfolioApplicationService::try_new(
            &paths,
            PortfolioApplicationLimits::standard(),
        )?);
        let executable_sha256 = current_executable_sha256()?;
        let strategies = production_backtest_strategy_registry(executable_sha256)?;
        let backtest_service = Arc::new(ProductionBacktestService::initialize(
            &paths,
            experiment_limits()?,
            strategies,
        )?);
        let backtest_inputs = Arc::new(ProductionGovernedBacktestInputAuthority::try_new(
            &paths,
            Arc::clone(&research),
            GovernedBacktestInputAuthorityLimits::standard(),
        )?);
        let resolver: Arc<dyn GovernedBacktestInputResolver> = backtest_inputs.clone();
        let backtest_repository = Arc::new(ProductionGovernedBacktestRepository::try_new(
            &paths,
            resolver,
            GovernedBacktestRepositoryLimits::standard(),
        )?);
        let repository: Arc<dyn GovernedBacktestRepository> = backtest_repository;
        let backtests: Arc<dyn GovernedBacktestAuthority> = Arc::new(
            ProductionBacktestAuthority::new(backtest_service, repository),
        );
        let analysis = Arc::new(AnalysisDomainService::new(
            Arc::new(analysis_catalog()?),
            backtests,
        ));

        let model_limits = ProductionModelRuntimeLimits::standard()?;
        let (model_runtime, model) = open_model_domain(&paths, &config, model_limits)?;

        let fair_value_inputs =
            ProductionFairValueInputAuthority::try_new(FairValueInputAuthorityLimits::standard())?;
        let fair_value_limits = fair_value_limits()?;
        let fair_value_service =
            FairValueService::open(research.fair_value_catalog(), fair_value_limits)?;
        let maximum_quote_age_nanos = u64::try_from(config.stale_after().as_nanos())
            .map_err(|_| LocalProductError::InvalidCodeOwnedLimit)?;
        let fair_value = Arc::new(FairValueDomainService::try_new(
            fair_value_service,
            fair_value_inputs.resolver(),
            maximum_quote_age_nanos,
        )?);

        let application = Arc::new(Application::try_from_product_services(
            source,
            &research_domains,
            portfolio.clone(),
            analysis,
            model,
            fair_value,
            &paper,
        )?);
        Ok(Self {
            paths,
            application,
            research,
            research_ingest,
            provider_onboarding: onboarding,
            provider_activation,
            portfolio,
            backtest_inputs,
            model_runtime,
            fair_value_inputs,
        })
    }

    /// Returns the sole transport-neutral application authority.
    pub fn application(&self) -> Arc<Application> {
        Arc::clone(&self.application)
    }

    /// Returns the controlled local paths used by MCP and CLI artifact boundaries.
    pub const fn paths(&self) -> &LocalPaths {
        &self.paths
    }

    /// Returns the analytical publication and point-in-time read authority.
    pub fn research(&self) -> Arc<ResearchService> {
        Arc::clone(&self.research)
    }

    /// Returns the sole registered extraction coordinator.
    pub fn research_ingest(&self) -> Arc<ProductionResearchIngestCoordinator> {
        Arc::clone(&self.research_ingest)
    }

    /// Returns activation authority for onboarding-ready adapters.
    pub fn provider_activation(&self) -> Arc<ProviderAdapterActivation> {
        Arc::clone(&self.provider_activation)
    }

    /// Returns provider onboarding authority for explicit CLI adapter activation boundaries.
    pub fn provider_onboarding(&self) -> Arc<ProviderOnboardingService> {
        Arc::clone(&self.provider_onboarding)
    }

    /// Returns the durable portfolio service used by direct CLI publication boundaries.
    pub fn portfolio(&self) -> Arc<PortfolioApplicationService> {
        Arc::clone(&self.portfolio)
    }

    /// Returns the durable governed-input registration authority.
    pub fn backtest_inputs(&self) -> Arc<ProductionGovernedBacktestInputAuthority> {
        Arc::clone(&self.backtest_inputs)
    }

    /// Returns model-admission authority when a signed training release was configured.
    pub fn model_runtime(&self) -> Option<Arc<ProductionModelRuntime>> {
        self.model_runtime.as_ref().map(Arc::clone)
    }

    /// Returns separated genuine-producer fair-value publication handles.
    pub const fn fair_value_inputs(&self) -> &ProductionFairValueInputAuthority {
        &self.fair_value_inputs
    }
}

impl std::fmt::Debug for LocalProduct {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalProduct")
            .field("paths", &"[LOCAL CAPABILITIES]")
            .field("application", &self.application)
            .field("research", &"[ANALYTICAL AUTHORITY]")
            .field("provider_onboarding", &"[ONBOARDING AUTHORITY]")
            .field("provider_activation", &"[ADAPTER ACTIVATION AUTHORITY]")
            .field("portfolio", &"[PORTFOLIO AUTHORITY]")
            .field("backtest_inputs", &"[BACKTEST INPUT AUTHORITY]")
            .field("model_runtime_configured", &self.model_runtime.is_some())
            .field("fair_value_inputs", &self.fair_value_inputs)
            .finish()
    }
}

fn open_research(paths: &LocalPaths) -> Result<ResearchService, LocalProductError> {
    let catalog = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        CATALOG_BUSY_TIMEOUT,
        CatalogLimit::new(CATALOG_MAXIMUM_ROWS)?,
        CatalogResultLimits::try_new(CATALOG_MAXIMUM_RECORD_BYTES, CATALOG_MAXIMUM_RESULT_BYTES)?,
    )?;
    let objects =
        ObjectStoreConfig::try_new(MAXIMUM_STAGING_BYTES, MAXIMUM_ROW_GROUP_ROWS, ORPHAN_GRACE)?;
    ResearchService::open_or_initialize(paths, catalog, MAXIMUM_DATASET_GENERATIONS, objects)
        .map_err(Into::into)
}

fn analysis_catalog() -> Result<AnalysisCatalog, LocalProductError> {
    let config = BatchFeatureCatalogConfig::try_new(
        NonZeroU32::new(252).ok_or(LocalProductError::InvalidCodeOwnedLimit)?,
        NonZeroU32::new(950_000).ok_or(LocalProductError::InvalidCodeOwnedLimit)?,
        6,
        BatchFeaturePolicies::new(
            VarianceConvention::Sample,
            MissingValuePolicy::Reject,
            WeightPolicy::PositiveNormalized,
            RoundingPolicy::NearestEven,
            ShockComposition::Compounded,
        ),
    )?;
    let features = BatchFeatureCatalog::try_new(config, BATCH_FEATURE_REVISION)?;
    AnalysisCatalog::try_new(Vec::new(), features, Vec::new()).map_err(Into::into)
}

fn experiment_limits() -> Result<ExperimentLimits, LocalProductError> {
    ExperimentLimits::try_new(ExperimentLimitsInput {
        max_trials: 10_000,
        max_record_bytes: 1024 * 1024,
        max_artifact_bytes: 256 * 1024 * 1024,
        max_metrics: 512,
    })
    .map_err(Into::into)
}

fn fair_value_limits() -> Result<FairValueLimits, LocalProductError> {
    FairValueLimits::try_new(FairValueLimitInput {
        max_measurements: 4_096,
        max_inputs_per_measurement: 64,
        max_records_per_family: 512,
        max_query_results: 10_000,
        max_retained_bytes: 64 * 1024 * 1024,
    })
    .map_err(Into::into)
}

fn open_model_domain(
    paths: &LocalPaths,
    config: &AppConfig,
    limits: ProductionModelRuntimeLimits,
) -> Result<(Option<Arc<ProductionModelRuntime>>, Arc<ModelDomainService>), LocalProductError> {
    let durable = ProductionModelRuntime::has_durable_admissions(paths, limits)?;
    let (runtime, snapshot) = match config.training_release_root() {
        None if durable => return Err(LocalProductError::TrainingReleaseRequired),
        None => (None, ProductionModelRuntime::empty_snapshot(limits)?),
        Some(root) => {
            let validator = root.join("bin").join(format!(
                "market-squawk-model-validator{}",
                std::env::consts::EXE_SUFFIX
            ));
            let training = verify_validator_training_environment(root, &validator)?;
            let onnx_worker = admit_installed_onnx_worker()?;
            let runtime = Arc::new(ProductionModelRuntime::try_open(
                paths,
                training,
                onnx_worker,
                limits,
            )?);
            let snapshot = match runtime.snapshot() {
                Ok(snapshot) => snapshot,
                Err(ProductionModelRuntimeError::EmptyRuntime) => {
                    ProductionModelRuntime::empty_snapshot(limits)?
                }
                Err(error) => return Err(error.into()),
            };
            (Some(runtime), snapshot)
        }
    };
    let (registry, backends) = snapshot.into_parts();
    let evaluation_records = NonZeroUsize::new(MODEL_EVALUATION_RECORDS)
        .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
    let model = Arc::new(ModelDomainService::try_new(
        registry,
        backends,
        evaluation_records,
    )?);
    Ok((runtime, model))
}

/// Production local composition failed before any transport was published.
#[derive(Debug, Error)]
pub enum LocalProductError {
    /// A code-owned nonzero or duration conversion was invalid.
    #[error("local product code-owned limit is invalid")]
    InvalidCodeOwnedLimit,
    /// Existing durable model generations require their signed training release.
    #[error("durable model admissions require the configured signed training release")]
    TrainingReleaseRequired,
    /// Controlled local paths could not be prepared.
    #[error(transparent)]
    Path(#[from] market_squawk_platform::PathError),
    /// The analytical catalog policy was invalid.
    #[error(transparent)]
    Catalog(#[from] market_squawk_data::CatalogError),
    /// The Parquet object-store policy was invalid.
    #[error(transparent)]
    ObjectStore(#[from] market_squawk_data::ParquetStoreError),
    /// Analytical storage composition failed.
    #[error(transparent)]
    Research(#[from] ResearchServiceError),
    /// Durable authority-state storage failed.
    #[error(transparent)]
    AuthorityState(#[from] market_squawk_platform::LocalAuthorityStateStoreError),
    /// Durable source-registry recovery failed.
    #[error(transparent)]
    SourceRegistry(#[from] market_squawk_sources::RegistryError),
    /// Preferred local secret-store construction failed.
    #[error(transparent)]
    Secrets(#[from] market_squawk_platform::LocalSecretStoreError),
    /// Provider onboarding construction failed.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// Source-domain lifecycle construction failed.
    #[error(transparent)]
    Source(#[from] crate::application::SourceApplicationError),
    /// Portfolio authority recovery failed.
    #[error(transparent)]
    Portfolio(#[from] PortfolioApplicationServiceError),
    /// Executable identity or ONNX sibling admission failed.
    #[error(transparent)]
    Executable(#[from] ExecutableIdentityError),
    /// Code-owned baseline strategy registration failed.
    #[error(transparent)]
    BacktestStrategy(#[from] BacktestStrategyCompositionError),
    /// Backtest experiment limits were invalid.
    #[error(transparent)]
    Experiment(#[from] market_squawk_backtesting::ExperimentError),
    /// Backtest service construction failed.
    #[error(transparent)]
    Backtest(#[from] ProductionBacktestServiceError),
    /// Durable backtest-input authority recovery failed.
    #[error(transparent)]
    BacktestInputs(
        #[from] crate::application::analysis::ProductionGovernedBacktestInputAuthorityError,
    ),
    /// Durable backtest terminal authority recovery failed.
    #[error(transparent)]
    BacktestRepository(
        #[from] crate::application::analysis::ProductionGovernedBacktestRepositoryError,
    ),
    /// Canonical batch-feature metadata was invalid.
    #[error(transparent)]
    FeatureMetadata(#[from] FeatureMetadataError),
    /// Immutable analysis catalog construction failed.
    #[error(transparent)]
    AnalysisCatalog(#[from] crate::application::analysis::AnalysisCatalogError),
    /// Signed training-release verification failed.
    #[error(transparent)]
    TrainingEnvironment(#[from] TrainingEnvironmentError),
    /// Durable model runtime construction failed.
    #[error(transparent)]
    ModelRuntime(#[from] ProductionModelRuntimeError),
    /// Model application service construction failed.
    #[error(transparent)]
    ModelDomain(#[from] ModelDomainServiceError),
    /// Fair-value input authority construction failed.
    #[error(transparent)]
    FairValueInput(#[from] FairValueInputAuthorityError),
    /// Fair-value catalog, limits, or ruleset construction failed.
    #[error(transparent)]
    FairValue(#[from] market_squawk_valuation::FairValueError),
    /// Complete application composition failed.
    #[error(transparent)]
    Application(#[from] ApplicationCompositionError),
}
