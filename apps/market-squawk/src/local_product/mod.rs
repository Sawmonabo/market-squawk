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
mod market_provider_configuration;
pub(crate) mod operations;
mod provider_activation_state;
mod source_lifecycle;

use std::num::{NonZeroU32, NonZeroUsize};
#[cfg(debug_assertions)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use market_squawk_adapter_treasury::TreasuryFiscalQuery;
use market_squawk_analytics::{
    BatchFeatureCatalog, BatchFeatureCatalogConfig, BatchFeaturePolicies, FeatureMetadataError,
    MissingValuePolicy, ShockComposition, VarianceConvention, WeightPolicy,
};
use market_squawk_backtesting::{ExperimentLimits, ExperimentLimitsInput};
use market_squawk_data::{CatalogConfig, CatalogLimit, CatalogResultLimits, ObjectStoreConfig};
use market_squawk_decisions::DecisionRepositoryLimits;
use market_squawk_domain::{InstrumentDefinition, RoundingPolicy, SourceIdentifier, Timestamp};
use market_squawk_mcp::{McpLimitSpec, McpLimits, validate_service_capabilities};
use market_squawk_modeling::{TrainingEnvironmentError, verify_application_training_environment};
use market_squawk_platform::{
    InstalledServiceSelectedWorkspaceGuard, LocalAuthorityStateStore, LocalPaths,
    PreferredSecretStore,
};
use market_squawk_services::{ArtifactAuthority, ArtifactError, ArtifactRepository};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, RESEARCH_SOURCE_AUTHORITY_DIRECTORY,
};
use market_squawk_valuation::{FairValueLimitInput, FairValueLimits, FairValueService};
use thiserror::Error;

pub use self::cli_backtest::CliBacktestRegistrationError;
pub use self::cli_dataset::CliDatasetError;
pub use self::cli_model::CliModelAdmissionError;
pub use self::cli_portfolio::CliPortfolioImportError;
pub use self::cli_provider::CliProviderActivationError;
pub(crate) use self::cli_provider::{
    ControlledLocalFileRequest, ProviderResearchActivationService,
};
pub use self::cli_transport::{
    CliProductError, CliProductResult, execute_cli_command, execute_installed_cli_command,
};
use self::executable::{
    ExecutableIdentityError, current_executable_sha256, installed_application_program,
    installed_service_program,
};
#[cfg(debug_assertions)]
use self::executable::{
    admit_development_onnx_worker, development_mcp_relay_program, development_service_program,
    development_training_release_programs,
};
#[cfg(not(debug_assertions))]
use self::executable::{admit_installed_onnx_worker, installed_release_programs};
use self::fair_value_producer::ProductionFairValueProducerSelectionAuthority;
use self::governance::{DecisionGovernanceAdapter, ProductionFairValueGovernanceActionFactory};
use self::market_provider_configuration::ProductionMarketProviderConfigurationResolver;
use self::provider_activation_state::{
    DurableProviderActivationState, ProviderMetadataBackupAuthority,
};
use self::source_lifecycle::ProductionSourceLifecycleAuthority;
#[cfg(all(feature = "board-installed-fixture", debug_assertions))]
use crate::BoardInstalledFixtureBundle;
use crate::application::analysis::{
    AnalysisCatalog, AnalysisDomainService, GovernedBacktestAuthority,
    GovernedBacktestInputAuthorityLimits, GovernedBacktestInputRegistrar,
    GovernedBacktestInputResolver, GovernedBacktestRepository, GovernedBacktestRepositoryLimits,
    ProductionBacktestAuthority, ProductionGovernedBacktestInputAuthority,
    ProductionGovernedBacktestRepository,
};
use crate::application::company_security_resolution::CompanySecurityResolutionAuthority;
use crate::application::decision::{DecisionApplication, DecisionApplicationError};
use crate::application::governance::{
    DecisionGovernanceActionFactory, FairValueGovernanceActionFactory,
};
use crate::application::model::backup::{
    ModelBackupAuthority, ModelBackupError, ModelBackupLimits,
};
use crate::application::model::runtime::{
    ProductionModelRuntime, ProductionModelRuntimeError, ProductionModelRuntimeLimits,
};
use crate::application::model::{
    ForecastApplicationError, ForecastApplicationLimits, ForecastApplicationService,
    ModelDomainService, ModelDomainServiceError,
};
use crate::application::settings::SettingsSeed;
use crate::application::{
    Application, ApplicationCompositionError, ApplicationDomainService, FairValueDomainService,
    FairValueInputAuthorityError, FairValueInputAuthorityLimits,
    FairValueProducerSelectionAuthority, LiveFairValueObservationBuffer,
    LiveFairValueObservationBufferError, MarketReferenceSearchAuthority, MarketRuntimeRegistry,
    PaperApplicationServices, PaperRuntimeActivityAuthority, PortfolioCandidateResolutionFactory,
    PrepublishedResearchSourceRegistration, ProductionFairValueInputAuthority,
    ProductionResearchIngestCoordinator, ResearchApplicationServices, ResearchExtractionLimits,
    ResearchIngestCompositionError, ResearchSourceDiscoveryCoordinator, SourceDomainService,
    SourceLifecycleAuthority, backup::ProductBackupError,
};
use crate::artifact_repository::{ControlledArtifactRepository, controlled_artifact_repository};
use crate::backtest_service::{ProductionBacktestService, ProductionBacktestServiceError};
use crate::backtest_strategy::{
    BacktestStrategyCompositionError, production_backtest_strategy_registry,
};
use crate::local_product::operations::{SettingsLifecycleAuthority, WorkspaceRestorePolicy};
use crate::provider_activation::nasdaq_reference::NasdaqReferenceUniverseService;
use crate::provider_rate::open_provider_rate_authority;
use crate::{
    AppConfig, PortfolioApplicationLimits, PortfolioApplicationService,
    PortfolioApplicationServiceError, ProviderAdapterActivation, ProviderOnboardingError,
    ProviderOnboardingService, ResearchService, ResearchServiceError,
};

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
const MAXIMUM_CONFIGURED_LIVE_INSTRUMENTS: usize = 101;
const COINBASE_LIVE_AUTHORITY_KEY: &str = "coinbase-exchange-public";
const KRAKEN_LIVE_AUTHORITY_KEY: &str = "kraken-public-book-v2";

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

/// Returns an explicit development service after stable-file and permission verification.
///
/// This boundary exists only in debug builds. Production binaries can launch only their verified
/// installed sibling.
#[cfg(debug_assertions)]
pub fn verified_development_service_program(
    program: &Path,
) -> Result<PathBuf, LocalServiceAvailabilityError> {
    development_service_program(program)
        .map_err(|_error| LocalServiceAvailabilityError::InstalledService)
}

/// Returns an explicit development MCP relay after stable-file and permission verification.
///
/// This boundary exists only in debug builds. Production binaries can select only the managed
/// installed relay.
#[cfg(debug_assertions)]
pub fn verified_development_mcp_relay_program(
    program: &Path,
) -> Result<PathBuf, LocalMcpAvailabilityError> {
    development_mcp_relay_program(program).map_err(|_error| LocalMcpAvailabilityError::InstalledCli)
}

/// Lifecycle owner for every production local authority required by the product surface.
pub struct LocalProduct {
    paths: LocalPaths,
    artifacts: Arc<ControlledArtifactRepository>,
    application: Arc<Application>,
    research: Arc<ResearchService>,
    company_security_resolution: Arc<CompanySecurityResolutionAuthority>,
    research_ingest: Arc<ProductionResearchIngestCoordinator>,
    source_lifecycle: Arc<ProductionSourceLifecycleAuthority>,
    paper_activity: Arc<dyn PaperRuntimeActivityAuthority>,
    portfolio_candidate_resolution: PortfolioCandidateResolutionFactory,
    provider_onboarding: Arc<ProviderOnboardingService>,
    provider_activation: Arc<ProviderAdapterActivation>,
    provider_research_activation: Arc<cli_provider::ProviderResearchActivationService>,
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
    model_runtime_limits: ProductionModelRuntimeLimits,
    forecasts: Arc<ForecastApplicationService>,
    fair_value: Arc<FairValueDomainService>,
    fair_value_inputs: ProductionFairValueInputAuthority,
}

#[derive(Debug)]
enum SourceAuthorityStartupPolicy<'guard> {
    RejectUncleanPredecessor,
    ExclusiveInstalledReplacement(&'guard InstalledServiceSelectedWorkspaceGuard),
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

    /// Opens the product through an already selected workspace path capability.
    pub(crate) fn try_new_at_selected_workspace(
        config: AppConfig,
        selected_workspace: &InstalledServiceSelectedWorkspaceGuard,
    ) -> Result<Self, LocalProductError> {
        Self::try_new_with_paths_and_prepublished_research_sources(
            config,
            selected_workspace.workspace_paths().clone(),
            std::iter::empty::<PrepublishedResearchSourceRegistration>(),
            SourceAuthorityStartupPolicy::ExclusiveInstalledReplacement(selected_workspace),
            #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
            None,
        )
    }

    #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
    pub(crate) fn try_new_at_selected_workspace_with_board_fixture(
        config: AppConfig,
        selected_workspace: &InstalledServiceSelectedWorkspaceGuard,
        board_fixture: BoardInstalledFixtureBundle,
    ) -> Result<Self, LocalProductError> {
        Self::try_new_with_paths_and_prepublished_research_sources(
            config,
            selected_workspace.workspace_paths().clone(),
            std::iter::empty::<PrepublishedResearchSourceRegistration>(),
            SourceAuthorityStartupPolicy::ExclusiveInstalledReplacement(selected_workspace),
            Some(board_fixture),
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
        Self::try_new_with_paths_and_prepublished_research_sources(
            config,
            paths,
            registrations,
            SourceAuthorityStartupPolicy::RejectUncleanPredecessor,
            #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
            None,
        )
    }

    fn try_new_with_paths_and_prepublished_research_sources<I>(
        config: AppConfig,
        paths: LocalPaths,
        registrations: I,
        source_authority_startup_policy: SourceAuthorityStartupPolicy<'_>,
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))] board_fixture: Option<
            BoardInstalledFixtureBundle,
        >,
    ) -> Result<Self, LocalProductError>
    where
        I: IntoIterator<Item = PrepublishedResearchSourceRegistration>,
    {
        let (research, onboarding_catalog) = open_research(&paths)?;
        let research = Arc::new(research);
        let company_security_resolution = Arc::new(CompanySecurityResolutionAuthority::new(
            research.company_identities(),
            research.market_data_instruments(),
            research.company_security_link_publication(),
        ));
        let configured_instruments = configured_live_instruments(&config)?;
        if !configured_instruments.is_empty() {
            research.synchronize_configured_instruments(
                &configured_instruments,
                local_product_timestamp()?,
                CatalogLimit::new(MAXIMUM_CONFIGURED_LIVE_INSTRUMENTS)?,
            )?;
        }
        let maximum_artifact_bytes = NonZeroUsize::new(LOCAL_MAXIMUM_ARTIFACT_BYTES)
            .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
        let artifacts =
            controlled_artifact_repository(paths.artifacts()?.clone(), maximum_artifact_bytes)?;
        let artifact_repository: Arc<dyn ArtifactRepository> = artifacts.clone();
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        let provider_rate = match &board_fixture {
            Some(fixture) => fixture.bind_provider_rate(paths.control_root()?.root())?,
            None => open_provider_rate_authority(paths.control_root()?.root())?,
        };
        #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
        let provider_rate = open_provider_rate_authority(paths.control_root()?.root())?;

        // The installation-global guard proves that no predecessor service can still own this
        // selected workspace. Reconcile each configured live authority once, before any runtime
        // can be constructed. Ordinary in-process source starts retain the strict predecessor
        // rejection path.
        if let SourceAuthorityStartupPolicy::ExclusiveInstalledReplacement(selected_workspace) =
            &source_authority_startup_policy
        {
            if config.coinbase().is_some() {
                AuthoritativeSourceRegistry::reconcile_live_authority_for_exclusive_installed_service_replacement(
                    selected_workspace,
                    COINBASE_LIVE_AUTHORITY_KEY,
                )?;
            }
            if config.kraken().is_some() {
                AuthoritativeSourceRegistry::reconcile_live_authority_for_exclusive_installed_service_replacement(
                    selected_workspace,
                    KRAKEN_LIVE_AUTHORITY_KEY,
                )?;
            }
        }

        let authorization_subject_resolver: Arc<dyn AuthorizationSubjectResolver> =
            Arc::new(provider_rate.clone());
        let source_registry = match source_authority_startup_policy {
            SourceAuthorityStartupPolicy::RejectUncleanPredecessor => {
                let source_store = LocalAuthorityStateStore::try_open(
                    paths
                        .control_root()?
                        .root()
                        .join(RESEARCH_SOURCE_AUTHORITY_DIRECTORY),
                )?;
                AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
                    source_store,
                    authorization_subject_resolver,
                    provider_rate.clone(),
                )
            }
            SourceAuthorityStartupPolicy::ExclusiveInstalledReplacement(selected_workspace) => {
                AuthoritativeSourceRegistry::try_new_durable_for_exclusive_installed_service_replacement(
                    selected_workspace,
                    authorization_subject_resolver,
                    provider_rate.clone(),
                )
            }
        }?;
        let (research_ingest, provider_runtime_mutation, alpaca_historical_source) =
            ProductionResearchIngestCoordinator::try_new_with_runtime_authorities(
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
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        let onboarding = Arc::new(match &board_fixture {
            Some(fixture) => ProviderOnboardingService::try_new_with_provider_rate_runtime_admissions_and_board_fixture(
                onboarding_catalog,
                secrets,
                provider_rate.clone(),
                runtime_admissions,
                fixture.doctor_executor(),
            )?,
            None => ProviderOnboardingService::try_new_with_provider_rate_and_runtime_admissions(
                onboarding_catalog,
                secrets,
                provider_rate.clone(),
                runtime_admissions,
            )?,
        });
        #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
        let onboarding = Arc::new(
            ProviderOnboardingService::try_new_with_provider_rate_and_runtime_admissions(
                onboarding_catalog,
                secrets,
                provider_rate.clone(),
                runtime_admissions,
            )?,
        );
        #[cfg(all(feature = "board-installed-fixture", debug_assertions))]
        let provider_activation = Arc::new(match &board_fixture {
            Some(fixture) => ProviderAdapterActivation::new_with_board_fixture(
                Arc::clone(&onboarding),
                Arc::clone(&research_ingest),
                provider_runtime_mutation,
                config.clone(),
                provider_rate.clone(),
                fixture.production_source_factory(),
            ),
            None => ProviderAdapterActivation::new(
                Arc::clone(&onboarding),
                Arc::clone(&research_ingest),
                provider_runtime_mutation,
                config.clone(),
                provider_rate.clone(),
            ),
        });
        #[cfg(not(all(feature = "board-installed-fixture", debug_assertions)))]
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

        let decisions = Arc::new(DecisionApplication::open(
            paths.control_root()?.decision_database_location(),
            decision_repository_limits()?,
        )?);
        let live_fair_value = Arc::new(LiveFairValueObservationBuffer::try_new(
            maximum_live_route_count(&config)?,
        )?);
        let nasdaq_reference = Arc::new(
            NasdaqReferenceUniverseService::try_new(provider_rate.clone()).map_err(|error| {
                tracing::error!(%error, "Nasdaq reference-universe startup failed");
                LocalProductError::NasdaqReference
            })?,
        );
        let prepared_market_configuration = ProductionMarketProviderConfigurationResolver::try_new(
            config.clone(),
            Arc::clone(&onboarding),
            Arc::clone(&provider_activation),
            Arc::clone(&nasdaq_reference),
            research.as_ref(),
            provider_rate.clone(),
        )?;
        let market_runtime = MarketRuntimeRegistry::try_new(
            config.clone(),
            provider_rate.clone(),
            Arc::clone(&provider_activation),
            Arc::clone(&research),
            alpaca_historical_source,
            prepared_market_configuration,
            Arc::clone(&live_fair_value),
        )?;
        let reference_search: Arc<dyn MarketReferenceSearchAuthority> = nasdaq_reference;
        let paper = PaperApplicationServices::new(
            config.clone(),
            Arc::clone(&decisions),
            Arc::clone(&market_runtime),
            research.instrument_definitions(),
            research.market_data_instruments(),
            reference_search,
        );
        let portfolio_candidate_resolution = paper.candidate_resolution_factory()?;
        let source_lifecycle = Arc::new(ProductionSourceLifecycleAuthority::new(
            paths.clone(),
            Arc::clone(&onboarding),
            Arc::clone(&provider_activation),
            Arc::clone(&provider_portal_activation),
            provider_activation_state.clone(),
            market_runtime,
        ));
        let source_lifecycle_service: Arc<dyn SourceLifecycleAuthority> = source_lifecycle.clone();
        let paper_activity = paper.runtime_activity_authority();
        let source_discovery: Arc<dyn ResearchSourceDiscoveryCoordinator> =
            Arc::clone(&research_ingest) as Arc<_>;
        let source: Arc<dyn ApplicationDomainService> = Arc::new(SourceDomainService::try_new(
            Arc::clone(&onboarding),
            paper.source_runtime_view(),
            source_discovery,
            portal_activation.clone(),
            portal_activation.clone(),
            source_lifecycle_service,
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
        let (model_runtime, model) =
            open_model_domain(&paths, &config, model_limits, Arc::clone(&forecasts))?;

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
            fair_value.clone(),
            &paper,
            config.source_shutdown(),
        )?);
        Ok(Self {
            paths,
            artifacts,
            application,
            research,
            company_security_resolution,
            research_ingest,
            source_lifecycle,
            paper_activity,
            portfolio_candidate_resolution,
            provider_onboarding: onboarding,
            provider_activation,
            provider_research_activation: portal_activation,
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
            model_runtime_limits: model_limits,
            forecasts,
            fair_value,
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

    pub(crate) fn controlled_artifacts(&self) -> Arc<ControlledArtifactRepository> {
        Arc::clone(&self.artifacts)
    }

    /// Returns one authority that resolves and reads its own verified artifact references.
    pub fn artifact_authority(&self) -> Arc<dyn ArtifactAuthority> {
        Arc::clone(&self.artifacts) as Arc<dyn ArtifactAuthority>
    }

    /// Returns the analytical publication and point-in-time read authority.
    pub fn research(&self) -> Arc<ResearchService> {
        Arc::clone(&self.research)
    }

    /// Returns the explicit, evidence-bound company/security resolution workflow.
    pub(crate) fn company_security_resolution(&self) -> Arc<CompanySecurityResolutionAuthority> {
        Arc::clone(&self.company_security_resolution)
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

    pub(crate) fn controlled_file_activation(
        &self,
    ) -> Arc<cli_provider::ProviderResearchActivationService> {
        Arc::clone(&self.provider_research_activation)
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

    /// Installs the sole workspace-bound resolver before the installed service becomes ready.
    pub(crate) fn register_portfolio_candidate_resolution(
        &self,
        setup: Arc<crate::application::recommendation::RecommendationSetupAuthority>,
    ) -> Result<(), PortfolioApplicationServiceError> {
        let authority = self
            .portfolio_candidate_resolution
            .bind(setup, self.portfolio.account_catalog_reader());
        self.portfolio
            .register_candidate_resolution_authority(authority)
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

    pub(crate) fn model_backup_authority(
        &self,
    ) -> Result<Arc<ModelBackupAuthority>, LocalProductError> {
        Ok(ModelBackupAuthority::new(
            self.model_runtime.as_ref().map(Arc::clone),
            self.model_runtime_limits,
            Arc::clone(&self.forecasts),
            ModelBackupLimits::standard()?,
        ))
    }

    pub(crate) fn fair_value_service(&self) -> Arc<FairValueDomainService> {
        Arc::clone(&self.fair_value)
    }

    pub(in crate::local_product) fn provider_metadata_backup_authority(
        &self,
    ) -> ProviderMetadataBackupAuthority {
        ProviderMetadataBackupAuthority::new(
            self.provider_activation_state.clone(),
            Arc::clone(&self.provider_onboarding),
            Arc::clone(&self.research_ingest),
        )
    }

    pub(crate) fn source_lifecycle_authority(&self) -> Arc<dyn SourceLifecycleAuthority> {
        self.source_lifecycle.clone()
    }

    pub(crate) async fn restore_active_live_sources(
        &self,
        deadline: std::time::Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<
        source_lifecycle::LiveSourceRestoreReport,
        crate::application::source::SourceLifecycleError,
    > {
        self.source_lifecycle
            .restore_active_live_sources(deadline, cancellation)
            .await
    }

    pub(crate) fn paper_runtime_activity_authority(
        &self,
    ) -> Arc<dyn PaperRuntimeActivityAuthority> {
        Arc::clone(&self.paper_activity)
    }

    pub(crate) fn workspace_restore_policy(
        &self,
        settings_seed: SettingsSeed,
        settings_lifecycle: SettingsLifecycleAuthority,
        jobs: market_squawk_jobs::JobRepositoryConfig,
    ) -> Result<Arc<WorkspaceRestorePolicy>, LocalProductError> {
        let objects = ObjectStoreConfig::try_new(
            MAXIMUM_STAGING_BYTES,
            MAXIMUM_ROW_GROUP_ROWS,
            ORPHAN_GRACE,
        )?;
        let maximum_controlled_artifact_bytes = NonZeroUsize::new(LOCAL_MAXIMUM_ARTIFACT_BYTES)
            .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
        let maximum_buffered_component_bytes =
            NonZeroUsize::new(512 * 1024 * 1024).ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
        WorkspaceRestorePolicy::try_new(
            settings_seed,
            settings_lifecycle,
            PortfolioApplicationLimits::standard(),
            self.model_backup_authority()?,
            decision_repository_limits()?,
            jobs,
            fair_value_limits()?,
            objects,
            MAXIMUM_OBJECTS_PER_DATASET_GENERATION,
            maximum_controlled_artifact_bytes,
            maximum_buffered_component_bytes,
        )
        .map(Arc::new)
        .map_err(LocalProductError::ProductBackup)
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

fn open_research(
    paths: &LocalPaths,
) -> Result<
    (
        ResearchService,
        market_squawk_data::OnboardingCatalogCapability,
    ),
    LocalProductError,
> {
    let catalog = local_catalog_config(paths)?;
    let objects =
        ObjectStoreConfig::try_new(MAXIMUM_STAGING_BYTES, MAXIMUM_ROW_GROUP_ROWS, ORPHAN_GRACE)?;
    ResearchService::open_or_initialize_with_provider_onboarding(
        paths,
        catalog,
        MAXIMUM_OBJECTS_PER_DATASET_GENERATION,
        objects,
    )
    .map_err(Into::into)
}

fn configured_live_instruments(
    config: &AppConfig,
) -> Result<Vec<InstrumentDefinition>, LocalProductError> {
    let coinbase_count = config
        .coinbase()
        .map_or(0, |source| source.instruments().len());
    let total = coinbase_count
        .checked_add(usize::from(config.kraken().is_some()))
        .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
    if total > MAXIMUM_CONFIGURED_LIVE_INSTRUMENTS {
        return Err(LocalProductError::InvalidCodeOwnedLimit);
    }
    let mut definitions = Vec::new();
    definitions
        .try_reserve_exact(total)
        .map_err(|_error| LocalProductError::ConfiguredInstrumentAllocation)?;
    if let Some(source) = config.coinbase() {
        definitions.extend(
            source
                .instruments()
                .iter()
                .map(|mapping| mapping.definition().clone()),
        );
    }
    if let Some(source) = config.kraken() {
        definitions.push(source.definition().clone());
    }
    definitions.sort_by_key(InstrumentDefinition::instrument_id);

    let mut canonical = Vec::<InstrumentDefinition>::new();
    canonical
        .try_reserve_exact(definitions.len())
        .map_err(|_error| LocalProductError::ConfiguredInstrumentAllocation)?;
    for definition in definitions {
        match canonical.last() {
            Some(previous)
                if previous.instrument_id() == definition.instrument_id()
                    && previous != &definition =>
            {
                return Err(LocalProductError::ConfiguredInstrumentConflict);
            }
            Some(previous) if previous.instrument_id() == definition.instrument_id() => continue,
            _ => canonical.push(definition),
        }
    }
    Ok(canonical)
}

fn local_product_timestamp() -> Result<Timestamp, LocalProductError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_error| LocalProductError::ClockRange)?;
    let nanos =
        i64::try_from(elapsed.as_nanos()).map_err(|_error| LocalProductError::ClockRange)?;
    Ok(Timestamp::from_unix_nanos(nanos))
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
    AnalysisCatalog::try_new(Vec::new(), features).map_err(Into::into)
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
    DecisionRepositoryLimits::try_new(4_096, 8_192, 64, 8_192, 8_192, 16_384, 8_192, 4_096)
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
    let public_routes = coinbase
        .checked_add(kraken)
        .ok_or(LocalProductError::InvalidCodeOwnedLimit)?;
    let direct_routes = coinbase;
    NonZeroUsize::new(
        public_routes
            .checked_add(direct_routes)
            .ok_or(LocalProductError::InvalidCodeOwnedLimit)?
            .max(1),
    )
    .ok_or(LocalProductError::InvalidCodeOwnedLimit)
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
            #[cfg(debug_assertions)]
            let (application, onnx_worker_path) = development_training_release_programs(root)?;
            #[cfg(not(debug_assertions))]
            let (application, onnx_worker_path) = installed_release_programs()?;
            let training =
                verify_application_training_environment(root, &application, &onnx_worker_path)?;
            #[cfg(debug_assertions)]
            let onnx_worker = Some(admit_development_onnx_worker(
                &onnx_worker_path,
                training.onnx_worker_sha256(),
            )?);
            #[cfg(not(debug_assertions))]
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
    /// Configured live definitions exceeded bounded allocation capacity.
    #[error("configured live instrument publication allocation failed")]
    ConfiguredInstrumentAllocation,
    /// Two configured live providers supplied incompatible definitions for one stable identity.
    #[error("configured live providers disagree on one canonical instrument definition")]
    ConfiguredInstrumentConflict,
    /// System wall-clock time cannot be represented by the domain timestamp.
    #[error("local product wall clock is outside the supported timestamp range")]
    ClockRange,
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
    /// Model and forecast backup authority construction failed.
    #[error(transparent)]
    ModelBackup(#[from] ModelBackupError),
    /// Fresh workspace restore policy construction failed.
    #[error(transparent)]
    ProductBackup(#[from] ProductBackupError),
    /// Fair-value input authority construction failed.
    #[error(transparent)]
    FairValueInput(#[from] FairValueInputAuthorityError),
    /// Live fair-value observation handoff construction failed.
    #[error(transparent)]
    LiveFairValue(#[from] LiveFairValueObservationBufferError),
    /// Multi-provider market runtime construction failed.
    #[error(transparent)]
    MarketRuntime(#[from] market_squawk_services::ServiceError),
    /// Session-only official U.S. listing-reference composition failed.
    #[error("session-only official U.S. listing-reference composition failed")]
    NasdaqReference,
    /// Fair-value catalog, limits, or ruleset construction failed.
    #[error(transparent)]
    FairValue(#[from] market_squawk_valuation::FairValueError),
    /// Complete application composition failed.
    #[error(transparent)]
    Application(#[from] ApplicationCompositionError),
}
