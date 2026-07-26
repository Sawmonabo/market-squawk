//! Complete local product composition shared by CLI and MCP transports.

mod cli_backtest;
mod cli_dataset;
mod cli_model;
mod cli_portfolio;
mod cli_provider;
mod cli_transport;
mod executable;
mod fair_value_producer;
mod provider_activation_state;

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
use market_squawk_modeling::{TrainingEnvironmentError, verify_application_training_environment};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths, PreferredSecretStore};
use market_squawk_services::{ArtifactError, ArtifactRepository};
use market_squawk_sources::{AuthoritativeSourceRegistry, AuthorizationSubjectResolver};
use market_squawk_valuation::{FairValueLimitInput, FairValueLimits, FairValueService};
use thiserror::Error;

pub use self::cli_backtest::CliBacktestRegistrationError;
pub use self::cli_dataset::CliDatasetError;
pub use self::cli_model::CliModelAdmissionError;
pub use self::cli_portfolio::CliPortfolioImportError;
pub use self::cli_provider::CliProviderActivationError;
pub use self::cli_transport::{CliProductError, CliProductResult, execute_cli_command};
use self::executable::{
    ExecutableIdentityError, admit_installed_onnx_worker, current_executable_sha256,
    installed_release_programs,
};
use self::fair_value_producer::ProductionFairValueProducerSelectionAuthority;
use self::provider_activation_state::DurableProviderActivationState;
use crate::application::analysis::{
    AnalysisCatalog, AnalysisDomainService, GovernedBacktestAuthority,
    GovernedBacktestInputAuthorityLimits, GovernedBacktestInputRegistrar,
    GovernedBacktestInputResolver, GovernedBacktestRepository, GovernedBacktestRepositoryLimits,
    ProductionBacktestAuthority, ProductionGovernedBacktestInputAuthority,
    ProductionGovernedBacktestRepository,
};
use crate::application::model::runtime::{
    ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
};
use crate::application::model::{ModelDomainService, ModelDomainServiceError};
use crate::application::{
    Application, ApplicationCompositionError, ApplicationDomainService, FairValueDomainService,
    FairValueInputAuthorityError, FairValueInputAuthorityLimits,
    FairValueProducerSelectionAuthority, LiveFairValueObservationBuffer,
    LiveFairValueObservationBufferError, PaperApplicationServices,
    PrepublishedResearchSourceRegistration, ProductionFairValueInputAuthority,
    ProductionResearchIngestCoordinator, ResearchApplicationServices, ResearchExtractionLimits,
    ResearchIngestCompositionError, ResearchSourceDiscoveryCoordinator, SourceDomainService,
};
use crate::artifact_repository::controlled_artifact_repository;
use crate::backtest_service::{ProductionBacktestService, ProductionBacktestServiceError};
use crate::backtest_strategy::{
    BacktestStrategyCompositionError, production_backtest_strategy_registry,
};
use crate::provider_rate::open_provider_rate_authority;
use crate::{
    AppConfig, PortfolioApplicationLimits, PortfolioApplicationService,
    PortfolioApplicationServiceError, ProviderAdapterActivation, ProviderOnboardingError,
    ProviderOnboardingService, ResearchService, ResearchServiceError,
};

const SOURCE_AUTHORITY_DIRECTORY: &str = "sources/research-runtime";
const PROVIDER_SECRET_DIRECTORY: &str = "secrets/provider-credentials";
const CATALOG_BUSY_TIMEOUT: Duration = Duration::from_millis(750);
const CATALOG_MAXIMUM_ROWS: usize = 10_000;
const CATALOG_MAXIMUM_RECORD_BYTES: usize = 1024 * 1024;
const CATALOG_MAXIMUM_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAXIMUM_OBJECTS_PER_DATASET_GENERATION: usize = 1_024;
const MAXIMUM_STAGING_BYTES: u64 = 256 * 1024 * 1024;
const MAXIMUM_ROW_GROUP_ROWS: usize = 65_536;
const ORPHAN_GRACE: Duration = Duration::from_secs(60);
const MODEL_EVALUATION_RECORDS: usize = 4_096;
const BATCH_FEATURE_REVISION: &str = "market-squawk-batch-features-v1";
const LOCAL_MAXIMUM_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

/// Lifecycle owner for every production local authority required by the product surface.
pub struct LocalProduct {
    paths: LocalPaths,
    artifacts: Arc<dyn ArtifactRepository>,
    application: Arc<Application>,
    research: Arc<ResearchService>,
    research_ingest: Arc<ProductionResearchIngestCoordinator>,
    provider_onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    provider_activation_state: DurableProviderActivationState,
    portfolio: Arc<PortfolioApplicationService>,
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
        Self::try_new_with_prepublished_research_sources(
            config,
            std::iter::empty::<PrepublishedResearchSourceRegistration>(),
        )
    }

    /// Opens the local product with a bounded static research-adapter composition.
    ///
    /// Registrations are consumed before the coordinator or application is published. Code-owned
    /// provider profiles remain restricted to exact onboarding and adapter activation.
    ///
    /// # Errors
    ///
    /// Returns the same closed composition failures as [`Self::try_new`], plus invalid static
    /// research registrations.
    pub fn try_new_with_prepublished_research_sources<I>(
        config: AppConfig,
        registrations: I,
    ) -> Result<Self, LocalProductError>
    where
        I: IntoIterator<Item = PrepublishedResearchSourceRegistration>,
    {
        let paths = LocalPaths::prepare(config.data_dir())?;
        let research = Arc::new(open_research(&paths)?);
        let maximum_artifact_bytes = NonZeroUsize::new(LOCAL_MAXIMUM_ARTIFACT_BYTES)
            .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
        let artifacts =
            controlled_artifact_repository(paths.artifacts()?.clone(), maximum_artifact_bytes)?;
        let provider_rate = open_provider_rate_authority(paths.control_root()?.root())?;

        let source_store = LocalAuthorityStateStore::try_open(
            paths
                .control_root()?
                .root()
                .join(SOURCE_AUTHORITY_DIRECTORY),
        )?;
        let authorization_subject_resolver: Arc<dyn AuthorizationSubjectResolver> =
            Arc::new(provider_rate.clone());
        let source_registry =
            AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
                source_store,
                authorization_subject_resolver,
                provider_rate.clone(),
            )?;
        let (research_ingest, provider_runtime_mutation) =
            ProductionResearchIngestCoordinator::try_new_with_provider_runtime_authority(
                source_registry,
                Arc::clone(&research),
                ResearchExtractionLimits::standard(),
                registrations,
            )?;

        let secrets = Arc::new(
            PreferredSecretStore::try_new_with_locked_encrypted_file_fallback(
                "market-squawk",
                paths.control_root()?.root().join(PROVIDER_SECRET_DIRECTORY),
            )?,
        );
        let provider_activation_state =
            DurableProviderActivationState::new(paths.control_root()?.root().to_path_buf());
        let runtime_admissions = provider_activation_state.startup_runtime_admissions()?;
        let onboarding = Arc::new(
            ProviderOnboardingService::try_new_with_provider_rate_and_runtime_admissions(
                research.onboarding_catalog(),
                secrets,
                provider_rate.clone(),
                runtime_admissions,
            )?,
        );
        let provider_activation = Arc::new(ProviderAdapterActivation::new(
            Arc::clone(&onboarding),
            Arc::clone(&research_ingest),
            provider_runtime_mutation,
            config.clone(),
            provider_rate.clone(),
        ));
        cli_provider::restore_research_providers(
            &paths,
            &onboarding,
            &provider_activation,
            &provider_activation_state,
        );
        let portal_activation = Arc::new(cli_provider::ProviderResearchActivationService::new(
            paths.clone(),
            Arc::clone(&onboarding),
            Arc::clone(&provider_activation),
            provider_activation_state.clone(),
        ));

        let live_fair_value = Arc::new(LiveFairValueObservationBuffer::try_new(
            maximum_live_route_count(&config)?,
        )?);
        let paper = PaperApplicationServices::new(
            config.clone(),
            Arc::clone(&live_fair_value),
            provider_rate,
            Arc::clone(&provider_activation),
        );
        let source_discovery: Arc<dyn ResearchSourceDiscoveryCoordinator> =
            Arc::clone(&research_ingest) as Arc<_>;
        let source: Arc<dyn ApplicationDomainService> = Arc::new(SourceDomainService::try_new(
            Arc::clone(&onboarding),
            paper.source_runtime_view(),
            source_discovery,
            portal_activation,
        )?);
        let research_domains = ResearchApplicationServices::new_with_artifacts(
            Arc::clone(&research),
            Arc::clone(&research_ingest) as Arc<_>,
            Arc::clone(&artifacts),
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
        let backtest_registrar: Arc<dyn GovernedBacktestInputRegistrar> = backtest_inputs.clone();
        let analysis = Arc::new(
            AnalysisDomainService::new_with_feature_reader_and_artifacts(
                Arc::new(analysis_catalog()?),
                research.analytical_reader(),
                Arc::clone(&artifacts),
                backtest_registrar,
                backtests,
            ),
        );

        let model_limits = ProductionModelRuntimeLimits::standard()?;
        let (model_runtime, model) = open_model_domain(&paths, &config, model_limits)?;

        let fair_value_inputs =
            ProductionFairValueInputAuthority::try_new(FairValueInputAuthorityLimits::standard())?;
        let fair_value_limits = fair_value_limits()?;
        let fair_value_service =
            FairValueService::open(research.fair_value_catalog(), fair_value_limits)?;
        let selection_authority: Arc<dyn FairValueProducerSelectionAuthority> =
            Arc::new(ProductionFairValueProducerSelectionAuthority::new(
                research.analytical_reader(),
                portfolio.fair_value_reader(),
                live_fair_value,
                fair_value_inputs.live_publisher(),
                fair_value_inputs.research_publisher(),
                fair_value_inputs.analytics_publisher(),
                fair_value_inputs.portfolio_publisher(),
            ));
        let maximum_quote_age_nanos = u64::try_from(config.stale_after().as_nanos())
            .map_err(|_| LocalProductError::InvalidCodeOwnedLimit)?;
        let fair_value = Arc::new(FairValueDomainService::try_new(
            fair_value_service,
            fair_value_inputs.resolver(),
            selection_authority,
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
            artifacts,
            application,
            research,
            research_ingest,
            provider_onboarding: onboarding,
            provider_activation,
            provider_activation_state,
            portfolio,
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

    /// Returns the sole controlled path-free artifact authority shared by local transports.
    pub fn artifacts(&self) -> Arc<dyn ArtifactRepository> {
        Arc::clone(&self.artifacts)
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

    pub(in crate::local_product) const fn provider_activation_state(
        &self,
    ) -> &DurableProviderActivationState {
        &self.provider_activation_state
    }

    /// Returns the durable portfolio service used by direct CLI publication boundaries.
    pub fn portfolio(&self) -> Arc<PortfolioApplicationService> {
        Arc::clone(&self.portfolio)
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
            .field("artifacts", &"[CONTROLLED ARTIFACT AUTHORITY]")
            .field("application", &self.application)
            .field("research", &"[ANALYTICAL AUTHORITY]")
            .field("provider_onboarding", &"[ONBOARDING AUTHORITY]")
            .field("provider_activation", &"[ADAPTER ACTIVATION AUTHORITY]")
            .field("provider_activation_state", &"[DURABLE ACTIVATION RECIPES]")
            .field("portfolio", &"[PORTFOLIO AUTHORITY]")
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
    ResearchService::open_or_initialize(
        paths,
        catalog,
        MAXIMUM_OBJECTS_PER_DATASET_GENERATION,
        objects,
    )
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

fn maximum_live_route_count(config: &AppConfig) -> Result<NonZeroUsize, LocalProductError> {
    let coinbase = config
        .coinbase()
        .map_or(0, |source| source.instruments().len());
    let kraken = usize::from(config.kraken().is_some());
    NonZeroUsize::new(coinbase.max(kraken).max(1)).ok_or(LocalProductError::InvalidCodeOwnedLimit)
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
            let (application, onnx_worker_path) = installed_release_programs()?;
            let training =
                verify_application_training_environment(root, &application, &onnx_worker_path)?;
            let onnx_worker = Some(admit_installed_onnx_worker(training.onnx_worker_sha256())?);
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
    /// Controlled local artifact authority could not be established.
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
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
    /// Static research-adapter composition failed.
    #[error(transparent)]
    ResearchComposition(#[from] ResearchIngestCompositionError),
    /// Product-wide provider-rate authority could not be opened or reconciled.
    #[error(transparent)]
    ProviderRate(#[from] market_squawk_sources::ProviderRateStoreError),
    /// Preferred local secret-store construction failed.
    #[error(transparent)]
    Secrets(#[from] market_squawk_platform::LocalSecretStoreError),
    /// Provider onboarding construction failed.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// Restart recovery of a durable research-provider activation failed.
    #[error(transparent)]
    ProviderActivationRecovery(#[from] CliProviderActivationError),
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
    /// Live fair-value observation handoff construction failed.
    #[error(transparent)]
    LiveFairValue(#[from] LiveFairValueObservationBufferError),
    /// Fair-value catalog, limits, or ruleset construction failed.
    #[error(transparent)]
    FairValue(#[from] market_squawk_valuation::FairValueError),
    /// Complete application composition failed.
    #[error(transparent)]
    Application(#[from] ApplicationCompositionError),
}
