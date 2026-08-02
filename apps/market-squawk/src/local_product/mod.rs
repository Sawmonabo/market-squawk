//! Complete local product composition shared by CLI and MCP transports.

mod cli_backtest;
pub(crate) mod cli_dataset;
mod cli_model;
mod cli_portfolio;
mod cli_provider;
mod cli_transport;
mod executable;
mod fair_value_producer;
mod governance;
mod operations;
mod provider_activation_state;
mod source_lifecycle;

use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_adapter_treasury::TreasuryFiscalQuery;
use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureMetadataError,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_backtesting::{ExperimentLimits, ExperimentLimitsInput};
use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
use market_squawk_decisions::DecisionRepositoryLimits;
use market_squawk_domain::{RoundingPolicy, SourceIdentifier};
use market_squawk_mcp::{McpLimitSpec, McpLimits, validate_service_capabilities};
use market_squawk_modeling::{TrainingEnvironmentError, verify_application_training_environment};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths, PreferredSecretStore};
use market_squawk_services::{ArtifactAuthority, ArtifactError, ArtifactRepository};
use market_squawk_sources::{AuthoritativeSourceRegistry, AuthorizationSubjectResolver};
use market_squawk_valuation::{FairValueLimitInput, FairValueLimits, FairValueService};
use thiserror::Error;

pub use self::cli_backtest::CliBacktestRegistrationError;
pub use self::cli_dataset::CliDatasetError;
pub use self::cli_model::CliModelAdmissionError;
pub use self::cli_portfolio::CliPortfolioImportError;
pub use self::cli_provider::CliProviderActivationError;
pub use self::cli_transport::{
    CliProductError, CliProductResult, execute_cli_command, execute_installed_cli_command,
};
use self::executable::{
    ExecutableIdentityError, admit_installed_onnx_worker, current_executable_sha256,
    installed_application_program, installed_release_programs, installed_service_program,
};
use self::fair_value_producer::ProductionFairValueProducerSelectionAuthority;
use self::governance::{DecisionGovernanceAdapter, ProductionFairValueGovernanceActionFactory};
use self::provider_activation_state::DurableProviderActivationState;
use self::source_lifecycle::ProductionSourceLifecycleAuthority;
use crate::application::analysis::{
    AnalysisCatalog, AnalysisDomainService, GovernedBacktestAuthority,
    GovernedBacktestInputAuthorityLimits, GovernedBacktestInputRegistrar,
    GovernedBacktestInputResolver, GovernedBacktestRepository, GovernedBacktestRepositoryLimits,
    ProductionBacktestAuthority, ProductionGovernedBacktestInputAuthority,
    ProductionGovernedBacktestRepository,
};
use crate::application::decision::{DecisionApplication, DecisionApplicationError};
use crate::application::governance::{
    DecisionGovernanceActionFactory, FairValueGovernanceActionFactory,
};
use crate::application::model::runtime::{
    ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
};
use crate::application::model::{
    ForecastApplicationError, ForecastApplicationLimits, ForecastApplicationService,
    ModelDomainService, ModelDomainServiceError,
};
use crate::application::{
    Application, ApplicationCompositionError, ApplicationDomainService, FairValueDomainService,
    FairValueInputAuthorityError, FairValueInputAuthorityLimits,
    FairValueProducerSelectionAuthority, LiveFairValueObservationBuffer,
    LiveFairValueObservationBufferError, PaperApplicationServices,
    PrepublishedResearchSourceRegistration, ProductionFairValueInputAuthority,
    ProductionResearchIngestCoordinator, ResearchApplicationServices, ResearchExtractionLimits,
    ResearchIngestCompositionError, ResearchSourceDiscoveryCoordinator, SourceDomainService,
    SourceLifecycleAuthority,
};
use crate::artifact_repository::{ControlledArtifactRepository, controlled_artifact_repository};
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
const FORECAST_VINTAGES: usize = 4_096;
const FORECAST_OUTCOMES: usize = 65_536;
const FORECAST_INDEX_BYTES: usize = LocalAuthorityStateStore::maximum_payload_bytes();
const FORECAST_AUTHORITY_DIRECTORY: &str = "model/forecasts";
const BATCH_FEATURE_REVISION: &str = "market-squawk-batch-features-v1";
const LOCAL_MAXIMUM_ARTIFACT_BYTES: usize = 64 * 1024 * 1024;

/// Returns the installed CLI path after stable-file and permission verification.
///
/// This narrow native-launch boundary does not initialize the application, validate the MCP tool
/// contract, or start a process.
///
/// # Errors
///
/// Returns [`LocalMcpAvailabilityError::InstalledCli`] when the packaged CLI sibling is absent,
/// unsafe, unreadable, or changes during inspection.
pub fn verified_installed_cli_program() -> Result<PathBuf, LocalMcpAvailabilityError> {
    installed_application_program().map_err(|_error| LocalMcpAvailabilityError::InstalledCli)
}

/// Returns the installed service sibling after stable-file and permission verification.
///
/// This is a native process-launch capability. It does not start the service or expose a path to
/// WebView code.
///
/// # Errors
///
/// Returns [`LocalServiceAvailabilityError`] when the packaged service is absent, unsafe,
/// unreadable, or changes during inspection.
pub fn verified_installed_service_program() -> Result<PathBuf, LocalServiceAvailabilityError> {
    installed_service_program().map_err(|_error| LocalServiceAvailabilityError::InstalledService)
}

/// Lifecycle owner for every production local authority required by the product surface.
pub struct LocalProduct {
    paths: LocalPaths,
    artifacts: Arc<ControlledArtifactRepository>,
    application: Arc<Application>,
    research: Arc<ResearchService>,
    research_ingest: Arc<ProductionResearchIngestCoordinator>,
    provider_onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    provider_portal_activation: Arc<dyn crate::ProviderPortalActivationAuthority>,
    provider_activation_state: DurableProviderActivationState,
    portfolio: Arc<PortfolioApplicationService>,
    decisions: Arc<DecisionApplication>,
    decision_governance: Arc<dyn DecisionGovernanceActionFactory>,
    fair_value_governance: Arc<dyn FairValueGovernanceActionFactory>,
    research_domain: Arc<dyn ApplicationDomainService>,
    analysis_domain: Arc<dyn ApplicationDomainService>,
    model_domain: Arc<dyn ApplicationDomainService>,
    backtest_registrar: Arc<dyn GovernedBacktestInputRegistrar>,
    backtests: Arc<dyn GovernedBacktestAuthority>,
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
        let artifact_repository: Arc<dyn ArtifactRepository> = artifacts.clone();
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
        let provider_portal_activation: Arc<dyn crate::ProviderPortalActivationAuthority> =
            portal_activation.clone();

        let live_fair_value = Arc::new(LiveFairValueObservationBuffer::try_new(
            maximum_live_route_count(&config)?,
        )?);
        let paper = PaperApplicationServices::new(
            config.clone(),
            Arc::clone(&live_fair_value),
            provider_rate,
            Arc::clone(&provider_activation),
        );
        let source_lifecycle: Arc<dyn SourceLifecycleAuthority> =
            Arc::new(ProductionSourceLifecycleAuthority::new(
                paths.clone(),
                Arc::clone(&onboarding),
                Arc::clone(&provider_activation),
                Arc::clone(&provider_portal_activation),
                provider_activation_state.clone(),
                paper.source_lifecycle_control(),
            ));
        let source_discovery: Arc<dyn ResearchSourceDiscoveryCoordinator> =
            Arc::clone(&research_ingest) as Arc<_>;
        let source: Arc<dyn ApplicationDomainService> = Arc::new(SourceDomainService::try_new(
            Arc::clone(&onboarding),
            paper.source_runtime_view(),
            source_discovery,
            portal_activation.clone(),
            portal_activation,
            source_lifecycle,
        )?);
        let research_domains = ResearchApplicationServices::new_with_artifacts(
            Arc::clone(&research),
            Arc::clone(&research_ingest) as Arc<_>,
            Arc::clone(&artifact_repository),
        );

        let portfolio = Arc::new(PortfolioApplicationService::try_new(
            &paths,
            PortfolioApplicationLimits::standard(),
        )?);
        let decisions = Arc::new(DecisionApplication::open(
            paths.control_root()?.decision_database_location(),
            decision_repository_limits()?,
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
                Arc::clone(&artifact_repository),
                Arc::clone(&backtest_registrar),
                Arc::clone(&backtests),
            ),
        );

        let model_limits = ProductionModelRuntimeLimits::standard()?;
        let forecast_limits = ForecastApplicationLimits::try_new(
            NonZeroUsize::new(FORECAST_VINTAGES).ok_or(LocalProductError::InvalidCodeOwnedLimit)?,
            NonZeroUsize::new(FORECAST_OUTCOMES).ok_or(LocalProductError::InvalidCodeOwnedLimit)?,
            NonZeroUsize::new(FORECAST_INDEX_BYTES)
                .ok_or(LocalProductError::InvalidCodeOwnedLimit)?,
        )?;
        let forecasts = Arc::new(ForecastApplicationService::try_open(
            paths
                .control_root()?
                .root()
                .join(FORECAST_AUTHORITY_DIRECTORY),
            Arc::clone(&artifact_repository),
            forecast_limits,
        )?);
        let (model_runtime, model) = open_model_domain(&paths, &config, model_limits, forecasts)?;

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
        let decision_governance: Arc<dyn DecisionGovernanceActionFactory> =
            Arc::new(DecisionGovernanceAdapter::new(Arc::clone(&decisions)));
        let fair_value_governance: Arc<dyn FairValueGovernanceActionFactory> = Arc::new(
            ProductionFairValueGovernanceActionFactory::new(Arc::clone(&fair_value)),
        );

        let application = Arc::new(Application::try_from_product_services(
            source,
            &research_domains,
            portfolio.clone(),
            analysis.clone(),
            model.clone(),
            fair_value,
            &paper,
            config.source_shutdown(),
        )?);
        Ok(Self {
            paths,
            artifacts,
            application,
            research,
            research_ingest,
            provider_onboarding: onboarding,
            provider_activation,
            provider_portal_activation,
            provider_activation_state,
            portfolio,
            decisions,
            decision_governance,
            fair_value_governance,
            research_domain: research_domains.research(),
            analysis_domain: analysis,
            model_domain: model,
            backtest_registrar,
            backtests,
            model_runtime,
            fair_value_inputs,
        })
    }

    /// Returns the sole transport-neutral application authority.
    pub fn application(&self) -> Arc<Application> {
        Arc::clone(&self.application)
    }

    /// Returns the installed CLI path after verifying the CLI and bounded MCP tool contract.
    ///
    /// This inspection does not start a protocol session or claim peer identity. Desktop packages
    /// use it to distinguish an installed, validated on-demand MCP capability from a static UI
    /// claim.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the installed CLI is not a stable regular file, the code-owned
    /// MCP limits are invalid, or the complete tool advertisement exceeds those limits.
    pub fn verified_local_mcp_program(&self) -> Result<PathBuf, LocalMcpAvailabilityError> {
        let program = verified_installed_cli_program()?;
        let limits = McpLimits::try_from(McpLimitSpec::default())
            .map_err(|_error| LocalMcpAvailabilityError::Limits)?;
        validate_service_capabilities(&self.application.capabilities(), limits)
            .map_err(|_error| LocalMcpAvailabilityError::ToolContract)?;
        Ok(program)
    }

    /// Returns the controlled local paths used by MCP and CLI artifact boundaries.
    pub const fn paths(&self) -> &LocalPaths {
        &self.paths
    }

    /// Returns the sole controlled path-free artifact authority shared by local transports.
    pub fn artifacts(&self) -> Arc<dyn ArtifactRepository> {
        Arc::clone(&self.artifacts) as Arc<dyn ArtifactRepository>
    }

    /// Returns one authority that resolves and reads its own verified artifact references.
    pub fn artifact_authority(&self) -> Arc<dyn ArtifactAuthority> {
        Arc::clone(&self.artifacts) as Arc<dyn ArtifactAuthority>
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

    /// Returns the shared durable provider activation boundary used by local presentation modes.
    pub fn provider_portal_activation(&self) -> Arc<dyn crate::ProviderPortalActivationAuthority> {
        Arc::clone(&self.provider_portal_activation)
    }

    pub(in crate::local_product) const fn provider_activation_state(
        &self,
    ) -> &DurableProviderActivationState {
        &self.provider_activation_state
    }

    /// Returns one configured year covered by all five active Treasury daily-rate families.
    pub(crate) fn treasury_daily_rate_release_year(
        &self,
    ) -> Result<u16, CliProviderActivationError> {
        cli_provider::treasury_daily_rate_release_year(&self.provider_activation_state)
    }

    /// Returns the exact Fiscal Data query owned by the desired, currently published runtime.
    pub(crate) fn treasury_fiscal_release_query(
        &self,
    ) -> Result<TreasuryFiscalQuery, CliProviderActivationError> {
        let (query, expected_runtime) =
            cli_provider::treasury_fiscal_release_query(&self.provider_activation_state)?;
        let profile = SourceIdentifier::try_from("treasury.fiscal-data")
            .map_err(|_| CliProviderActivationError::StateUnavailable)?;
        let runtime = self
            .provider_activation
            .research_runtime_generation(&profile)
            .map_err(CliProviderActivationError::Activation)?
            .ok_or(CliProviderActivationError::StateUnavailable)?;
        let actual_runtime = runtime
            .generation_digest()
            .map_err(|_| CliProviderActivationError::StateUnavailable)?;
        if actual_runtime != expected_runtime {
            return Err(CliProviderActivationError::StateUnavailable);
        }
        Ok(query)
    }

    /// Returns the durable portfolio service used by direct CLI publication boundaries.
    pub fn portfolio(&self) -> Arc<PortfolioApplicationService> {
        Arc::clone(&self.portfolio)
    }

    /// Returns the sole durable decision workflow authority.
    pub fn decisions(&self) -> Arc<DecisionApplication> {
        Arc::clone(&self.decisions)
    }

    pub(crate) fn decision_governance(&self) -> Arc<dyn DecisionGovernanceActionFactory> {
        Arc::clone(&self.decision_governance)
    }

    pub(crate) fn fair_value_governance(&self) -> Arc<dyn FairValueGovernanceActionFactory> {
        Arc::clone(&self.fair_value_governance)
    }

    pub(crate) fn research_domain(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::clone(&self.research_domain)
    }

    pub(crate) fn analysis_domain(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::clone(&self.analysis_domain)
    }

    pub(crate) fn model_domain(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::clone(&self.model_domain)
    }

    pub(crate) fn backtest_registrar(&self) -> Arc<dyn GovernedBacktestInputRegistrar> {
        Arc::clone(&self.backtest_registrar)
    }

    pub(crate) fn backtests(&self) -> Arc<dyn GovernedBacktestAuthority> {
        Arc::clone(&self.backtests)
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
            .field(
                "provider_portal_activation",
                &"[PORTAL ACTIVATION AUTHORITY]",
            )
            .field("provider_activation_state", &"[DURABLE ACTIVATION RECIPES]")
            .field("portfolio", &"[PORTFOLIO AUTHORITY]")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field(
                "job_domain_authorities",
                &"[SHARED APPLICATION AUTHORITIES]",
            )
            .field("model_runtime_configured", &self.model_runtime.is_some())
            .field("fair_value_inputs", &self.fair_value_inputs)
            .finish()
    }
}

fn open_research(paths: &LocalPaths) -> Result<ResearchService, LocalProductError> {
    let catalog = local_catalog_config(paths)?;
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

pub(crate) fn local_catalog_config(paths: &LocalPaths) -> Result<CatalogConfig, LocalProductError> {
    CatalogConfig::try_new(
        paths.catalog()?.clone(),
        CATALOG_BUSY_TIMEOUT,
        CatalogLimit::new(CATALOG_MAXIMUM_ROWS)?,
        CatalogResultLimits::try_new(CATALOG_MAXIMUM_RECORD_BYTES, CATALOG_MAXIMUM_RESULT_BYTES)?,
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

fn decision_repository_limits() -> Result<DecisionRepositoryLimits, LocalProductError> {
    DecisionRepositoryLimits::try_new(4_096, 8_192, 64, 8_192, 8_192, 16_384, 8_192)
        .map_err(|_error| LocalProductError::InvalidCodeOwnedLimit)
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
    forecasts: Arc<ForecastApplicationService>,
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
    let evaluation_records = NonZeroUsize::new(MODEL_EVALUATION_RECORDS)
        .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
    let model = Arc::new(
        ModelDomainService::try_from_runtime_snapshot_with_forecasts(
            snapshot,
            evaluation_records,
            forecasts,
        )?,
    );
    Ok((runtime, model))
}

/// Installed service process availability could not be established.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalServiceAvailabilityError {
    /// The packaged service sibling was absent, unsafe, unreadable, or changed during inspection.
    #[error("installed Market Squawk service is unavailable")]
    InstalledService,
}

/// Installed local MCP availability could not be established.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum LocalMcpAvailabilityError {
    /// The packaged CLI sibling was absent, unsafe, unreadable, or changed during inspection.
    #[error("installed Market Squawk CLI is unavailable")]
    InstalledCli,
    /// Code-owned MCP resource limits were invalid.
    #[error("local MCP resource limits are invalid")]
    Limits,
    /// The complete application tool contract could not fit the bounded MCP advertisement.
    #[error("local MCP tool contract is invalid")]
    ToolContract,
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
    /// Durable decision authority recovery failed.
    #[error(transparent)]
    Decision(#[from] DecisionApplicationError),
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
    /// Durable forecast authority recovery or publication configuration failed.
    #[error(transparent)]
    Forecast(#[from] ForecastApplicationError),
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
