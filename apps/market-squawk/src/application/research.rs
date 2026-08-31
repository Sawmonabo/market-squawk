//! Manifest-pinned research, fundamental, and macro application services.

use std::{
    fmt,
    io::{self, Write},
    num::NonZeroU16,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::json::ArrayWriter;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use market_squawk_adapter_federal_reserve::{
    BOARD_DDP_SOURCE_ID, BoardDatasetFamily, BoardDatasetProfile, BoardFrequency,
    BoardH15DashboardSeriesDescriptor, BoardRelease,
    h15_treasury_constant_maturities_canonical_unit_identifier,
    h15_treasury_constant_maturities_dashboard_series,
};
use market_squawk_data::{
    AnalyticalFundNavOutput, AnalyticalFundNavReadRequest, AnalyticalGeneration,
    AnalyticalMacroLatestKnownOutput, AnalyticalMacroLatestKnownRequest,
    AnalyticalMacroSeriesAllowlist, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadCapability, AnalyticalReadError,
    AnalyticalReadLimit, DatasetId, DatasetManifestRef, GenerationKind, GenerationParentRelation,
    IngestPrecommitAuthority, ListingReferenceReadCapability, ManifestCatalogError,
    PinnedArtifactQueryRequest, PinnedQueryOutput, QueryError, QueryLimits, QueryResult,
};
use market_squawk_domain::{
    CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, MacroObservation,
    PayloadReference, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactRepository,
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};

use super::{ApplicationDomainService, domain_support::DomainLifecycle, effective_service_limits};
use crate::ResearchService;

mod company_product;
mod company_research;
mod dataset_preparation;
mod forecast_evidence;
mod fred;
mod fund_product;
mod ingest;
mod instrument_context;
mod macro_context;
mod macro_features;
mod market_history;
mod options_context;
mod sec_fund_job;
mod sec_fund_product;
mod sec_fundamentals;
mod treasury;

pub(crate) use company_product::CompanyProductResult;
pub(crate) use company_research::{
    CompanyProductRead, CompanyResearchReadCapability, FundProductRead,
    ResearchProductReadCapability, ResearchProductReadError,
};
pub(crate) use dataset_preparation::{
    DatasetPreparationAuthority, DatasetPreparationError, DatasetPreparationOptions,
    DatasetPreparationPreview, DatasetPreparationPreviewRequest, DatasetPreparationReceipt,
    DatasetPreparationSelection, FeatureDatasetProductionFinalizer, PreparedFeatureDatasetBuild,
};
pub(crate) use forecast_evidence::AnalyticalForecastEvidenceReader;
pub(crate) use fred::{
    FredLatestKnownAvailability, FredLatestKnownCompositionError, FredLatestKnownOperation,
};
pub(crate) use fund_product::FundProductResult;
pub(crate) use ingest::{
    AlpacaHistoricalAuthorizedPlan, AlpacaHistoricalPlanAdmissionError,
    AlpacaHistoricalPlanReceipt, AlpacaHistoricalSourceMutationAuthority,
    BEA_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION, BLS_PROVIDER_PERIOD_LATEST_KNOWN_OPERATION,
    BeaDoctorActivationState, BeaLivePublicationError, BeaMacroApplicationClosure,
    BeaMacroApplicationError, BeaMacroCapabilityState, BeaMacroPlanPublication,
    BeaProviderPeriodLatestKnownDto, BeaProviderPeriodLatestKnownRequest,
    BeaRegionalLiveComposition, BeaRegionalLiveOutcome, BeaRegionalLiveRequest,
    BeaRegionalLiveRuntime, BeaSetupRequiredDto, BeaSetupRequiredKind, BeaUnavailableDto,
    BeaUnavailableReason, BlsMacroApplicationClosure, BlsMacroApplicationError,
    BlsMacroCapabilityState, BlsMacroPlanPublication, BlsMacroUnavailableReason,
    BlsPreparedMacroPlan, BlsProviderPeriodLatestKnownDto, BlsProviderPeriodLatestKnownRequest,
    BlsSealFirstExtractionLimits, BlsWholePlanApplicationHandoff, CoinbaseMarketApplicationOutcome,
    CryptoCommittedRowIngress, CryptoMarketPublicationAuthority, CryptoMarketPublicationClosure,
    CryptoMarketPublicationError, CryptoMarketSurface, CryptoPendingFrameIngress,
    CryptoPublicationRendezvousLimits, FredPublishedGenerationHandoff, IexHistApplicationError,
    IexHistApplicationLane, IexHistCaptureSealHandoff, IexHistCaptureSealRequirements,
    IexHistCatalogSealHandoff, IexHistClockStatus, IexHistExactJobPreview,
    IexHistExplicitJobRequest, IexHistInstrumentIdentityBlocker, IexHistInstrumentIdentityStatus,
    IexHistJobAuthority, IexHistJobStatus, IexHistPhysicalArtifact, IexHistPhysicalSealRequirement,
    IexHistPublicationAvailability, IexHistPublicationBlocker, IexHistPublicationBlockers,
    IexHistResearchJobLeaf, IexHistSelectionStatus, KrakenMarketApplicationOutcome,
    KrakenSealedRawCanonicalUnavailable, MarketEventDurableRead, MarketEventDurableReadWriter,
    MarketEventPointInTimeReceipt, MarketEventPointInTimeSelector, MarketEventPublicationReceipt,
    MarketEventReadError, MarketEventRestartReceipt, MarketEventRestartSelector,
    MarketEventSealedReceiptEvidence, ResearchProviderPublicationOperation,
    ResearchProviderRuntimeMutationAuthority, ResearchProviderRuntimeReplacement,
    SchwabMarketPublicationError, SchwabRestQuoteGenerationAuthority,
    SchwabRestQuotePostSealFailure, SchwabRestQuotePublicationPackage,
    SchwabRestQuoteSourceHealthOutcome, SecFundPublicationReceipt, SecLiveFundApplicationError,
    SecLiveFundRequest, SecLiveFundSource, TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION,
    TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION, TreasuryApplicationClosure,
    TreasuryMacroPublicationReceipt, TreasurySelectedObjectRequest,
};
pub(crate) use ingest::{
    BlsLiveComposition, BlsLiveOutcome, BlsLivePublicationError, BlsLiveRequest, BlsLiveRuntime,
};
pub use ingest::{
    ManagedResearchExtractionSource, PrepublishedResearchSourceRegistration,
    ProductionResearchIngestCoordinator, ResearchExtractionLimits, ResearchIngestCompositionError,
    ResearchProviderRuntimeGeneration, ResearchRevisionPlanError, ResearchRightsAuthority,
    ResearchSourceDiscovery, ResearchSourceDiscoveryObject, ResearchSourceDiscoveryRights,
    ResearchSourceObjectListing,
};
pub(crate) use instrument_context::{
    InstrumentContext, InstrumentContextOutcome, InstrumentContextReadCapability,
    InstrumentContextRequest,
};
pub(crate) use macro_context::{
    MACRO_GET_CONTEXT, MacroContextOperation, MacroContextReadCapability,
};
pub(crate) use macro_features::{MacroFeatureVector, read_macro_feature_vector};
pub(crate) use market_history::{
    LatestMarketHistoryReadRequest, MAX_MARKET_HISTORY_BARS, MarketHistoryAdjustmentPolicy,
    MarketHistoryBar, MarketHistoryCoverage, MarketHistoryMissingReason,
    MarketHistoryPartialReason, MarketHistoryQuality, MarketHistoryReadCapability,
    MarketHistoryReadLimit, MarketHistoryReadOutcome, MarketHistorySeries,
    MarketHistorySessionPolicy, MarketHistoryTimeframe, MarketHistoryUnavailableReason,
};
pub(crate) use options_context::{
    OptionsContextReadCapability, OptionsObservationReadAvailability,
    OptionsReferenceReadAvailability,
};
pub(crate) use sec_fund_job::{
    SecFundJobCommitAuthority, SecFundJobExecutionError, SecFundJobRunner, SecFundJobRunnerError,
};
pub(crate) use sec_fund_product::{
    SEC_FUND_START_PUBLICATION_OPERATION, SecAdmittedFundProductRequest,
    SecFundProductBoundaryError, SecFundProductCoordinate, SecFundProductFamily,
    SecFundProductProjection, SecFundProductRequest, SecFundProductRequestFactory,
    SecFundPublicationProjection,
};
pub(crate) use sec_fundamentals::{
    SEC_FUNDAMENTALS_RESEARCH_STATUS_OPERATION, SecAvailableResearchFamily,
    SecCombinedIdentityStatus, SecConflictResearchFamily, SecFamilyResearchStatus,
    SecFamilyUnavailableReason, SecFourClockStatus, SecFundamentalsResearchError,
    SecFundamentalsResearchOperation, SecFundamentalsResearchRequest,
    SecFundamentalsResearchStatus, SecRatioResearchStatus, SecRatioUnavailableReason,
    SecResearchFamilyBinding, SecUnavailableResearchFamily,
};
pub(crate) use treasury::TreasuryLatestKnownOperation;

const RESEARCH_LIST_DATASETS: &str = "Research.ListDatasets";
const RESEARCH_GET_MANIFEST: &str = "Research.GetManifest";
const RESEARCH_GET_HISTORY: &str = "Research.GetHistory";
const RESEARCH_GET_ALTERNATIVE_DATA: &str = "Research.GetAlternativeData";
const RESEARCH_INGEST_SOURCE: &str = "Research.IngestSource";

const FUNDAMENTAL_GET_FILINGS: &str = "Fundamental.GetFilings";
const FUNDAMENTAL_GET_FACTS: &str = "Fundamental.GetFacts";
const FUNDAMENTAL_GET_STATEMENTS: &str = "Fundamental.GetStatements";
const FUNDAMENTAL_GET_RATIOS: &str = "Fundamental.GetRatios";

const MACRO_LIST_SERIES: &str = "Macro.ListSeries";
const MACRO_GET_DASHBOARD: &str = "Macro.GetDashboard";
const MACRO_GET_OBSERVATIONS: &str = "Macro.GetObservations";
const MACRO_GET_VINTAGES: &str = "Macro.GetVintages";
const MACRO_GET_REVISIONS: &str = "Macro.GetRevisions";

const BOARD_DDP_SURFACE_ID: &str = "federal-reserve-board.data-download-program";
const H15_REQUEST_RELEASE: &str = "h15";
const MACRO_DASHBOARD_SCHEMA_IDENTITY: &str = "market-squawk-macro-dashboard/v1";
const MACRO_DASHBOARD_SELECTION_POLICY: &str = "latest_known_by_series_as_of_cutoff_v1";
const MACRO_DASHBOARD_SERIES_COUNT: usize = 11;
const MACRO_DASHBOARD_QUERY_BYTES: u64 = 256 * 1024;
const MACRO_DASHBOARD_QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

const MAX_ANALYTICAL_PAGE: usize = 64;
const QUERY_ARTIFACT_TTL: Duration = Duration::from_secs(60 * 60);
const QUERY_ARTIFACT_OWNER: &str = "market-squawk.research-query";
const MINIMUM_ANALYTICAL_QUERY_MEMORY_BYTES: u64 = 64 * 1024 * 1024;

/// Provider extraction and rights-admitted ingestion used by the Research domain.
///
/// Implementations own the concrete extraction adapters and must be cancellation-aware. Shutdown
/// operations are idempotent because Source and Research may share the same coordinator.
#[async_trait]
pub trait ResearchIngestCoordinator: Send + Sync + 'static {
    /// Executes one descriptor-admitted provider extraction and durable ingest.
    async fn ingest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError>;

    /// Executes ingestion while composing one additional process-local commit authority.
    ///
    /// The implementation must validate its ordinary provider-generation authority first and
    /// invoke `additional` only at the exact durable catalog/manifest commit boundary.
    async fn ingest_with_precommit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        additional: Arc<dyn ResearchIngestCommitAuthority>,
    ) -> Result<TypedToolResult, ServiceError>;

    /// Rejects new extraction work and cancels owned background activity without blocking.
    fn begin_shutdown(&self);

    /// Completes bounded adapter and persistence shutdown.
    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Additional application authority spanning the exact ingest commit boundary.
pub trait ResearchIngestCommitAuthority: IngestPrecommitAuthority {
    /// Seals the already-claimed authority immediately after durable ingest publication succeeds.
    fn commit_succeeded(&self);
}

/// Receipt-minting discovery authority shared with the Source domain.
///
/// This narrow trait exposes registered adapter discovery and exact-batch rollback without
/// granting source registration, raw adapter access, credential access, extraction, or analytical
/// publication authority.
#[async_trait]
pub trait ResearchSourceDiscoveryCoordinator: Send + Sync + 'static {
    /// Hard object ceiling configured on this exact coordinator.
    fn maximum_discovery_objects(&self) -> NonZeroU16;

    /// Returns the exact discovery dataset carried by one currently admitted provider runtime.
    ///
    /// # Errors
    ///
    /// Returns a bounded service error when current runtime authority cannot be inspected.
    fn registered_discovery_dataset(
        &self,
        profile: &SourceIdentifier,
    ) -> Result<Option<SourceIdentifier>, ServiceError>;

    /// Revokes exactly one discovery batch that could not be published to its caller.
    ///
    /// Implementations must leave every receipt outside `discovery` unchanged. This operation is
    /// idempotent so shutdown may clear the same receipts before application rollback runs.
    fn revoke_discovery_receipts(
        &self,
        discovery: &ResearchSourceDiscovery,
    ) -> Result<(), ServiceError>;

    /// Lists bounded exact objects without allocating ingestion receipts or retained capacity.
    async fn list_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceObjectListing, ServiceError>;

    /// Discovers bounded exact objects for one active registered provider profile.
    async fn discover_registered_objects(
        &self,
        profile: &SourceIdentifier,
        dataset: &SourceIdentifier,
        effective_at: Option<Timestamp>,
        max_results: NonZeroU16,
        context: &RequestContext,
    ) -> Result<ResearchSourceDiscovery, ServiceError>;
}

/// Shared analytical authority exposed as separate Research, Fundamental, and Macro services.
pub struct ResearchApplicationServices {
    controller: Arc<ResearchController>,
}

impl ResearchApplicationServices {
    /// Binds one application-owned analytical service and one concrete extraction coordinator.
    #[must_use]
    pub fn new(service: Arc<ResearchService>, ingest: Arc<dyn ResearchIngestCoordinator>) -> Self {
        Self::compose(
            service,
            ingest,
            None,
            None,
            FredLatestKnownOperation::setup_required(),
            TreasuryLatestKnownOperation::fiscal_setup_required(),
            TreasuryLatestKnownOperation::daily_setup_required(),
        )
    }

    /// Binds the public application path to the shared controlled opaque-artifact repository.
    #[must_use]
    pub fn new_with_artifacts(
        service: Arc<ResearchService>,
        ingest: Arc<dyn ResearchIngestCoordinator>,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> Self {
        Self::compose(
            service,
            ingest,
            Some(artifacts),
            None,
            FredLatestKnownOperation::setup_required(),
            TreasuryLatestKnownOperation::fiscal_setup_required(),
            TreasuryLatestKnownOperation::daily_setup_required(),
        )
    }

    /// Binds artifacts and exact startup-composed canonical research reads.
    pub(crate) fn new_with_artifacts_and_composed_reads(
        service: Arc<ResearchService>,
        ingest: Arc<dyn ResearchIngestCoordinator>,
        artifacts: Arc<dyn ArtifactRepository>,
        listing_reference: Option<ListingReferenceReadCapability>,
        fred_latest_known: FredLatestKnownOperation,
    ) -> Self {
        Self::compose(
            service,
            ingest,
            Some(artifacts),
            listing_reference,
            fred_latest_known,
            TreasuryLatestKnownOperation::fiscal_setup_required(),
            TreasuryLatestKnownOperation::daily_setup_required(),
        )
    }

    fn compose(
        service: Arc<ResearchService>,
        ingest: Arc<dyn ResearchIngestCoordinator>,
        artifacts: Option<Arc<dyn ArtifactRepository>>,
        listing_reference: Option<ListingReferenceReadCapability>,
        fred_latest_known: FredLatestKnownOperation,
        treasury_fiscal_latest_known: TreasuryLatestKnownOperation,
        treasury_daily_latest_known: TreasuryLatestKnownOperation,
    ) -> Self {
        let reader = service.analytical_reader();
        let macro_context = MacroContextOperation::with_treasury(
            reader.clone(),
            fred_latest_known.clone(),
            treasury_fiscal_latest_known.clone(),
            treasury_daily_latest_known.clone(),
        );
        let company_research = CompanyResearchReadCapability::new(Arc::clone(&service));
        let product_identity = listing_reference.as_ref().cloned().map(|listings| {
            Arc::new(InstrumentContextReadCapability::new(
                service.market_data_instruments(),
                listings,
            ))
        });
        let product_research =
            ResearchProductReadCapability::new(company_research.clone(), product_identity);
        let options_context = OptionsContextReadCapability::new(
            Arc::clone(&service),
            OptionsReferenceReadAvailability::Ready(
                service
                    .analytical()
                    .official_options_reference_catalog_reader(),
            ),
            OptionsObservationReadAvailability::SetupRequired,
        );
        let market_history = MarketHistoryReadCapability::new(reader.clone());
        Self {
            controller: Arc::new(ResearchController {
                authority: service,
                reader,
                ingest,
                artifacts,
                listing_reference,
                company_research,
                product_research,
                fred_latest_known,
                macro_context,
                options_context,
                market_history,
                treasury_fiscal_latest_known,
                treasury_daily_latest_known,
                lifecycle: DomainLifecycle::new(),
            }),
        }
    }

    /// Returns the Research-domain implementation.
    pub fn research(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(ResearchDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Fundamental-domain implementation.
    pub fn fundamental(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(FundamentalDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the Macro-domain implementation.
    pub fn macroeconomics(&self) -> Arc<dyn ApplicationDomainService> {
        Arc::new(MacroDomainService {
            controller: Arc::clone(&self.controller),
        })
    }

    /// Returns the provider-neutral point-in-time Macro read for analytical consumers.
    pub(crate) fn macro_context_read_capability(&self) -> MacroContextReadCapability {
        self.controller.macro_context.read_capability()
    }

    /// Returns the canonical identity join only when an official-directory reader was composed.
    pub(crate) fn instrument_context_read_capability(
        &self,
    ) -> Option<InstrumentContextReadCapability> {
        self.controller
            .listing_reference
            .as_ref()
            .cloned()
            .map(|listings| {
                InstrumentContextReadCapability::new(
                    self.controller.authority.market_data_instruments(),
                    listings,
                )
            })
    }

    /// Returns canonical company and fund research reads over the rich local store.
    pub(crate) fn company_research_read_capability(&self) -> CompanyResearchReadCapability {
        self.controller.company_research.clone()
    }

    /// Returns fixed provider-neutral company and fund product research operations.
    pub(crate) fn product_research_read_capability(&self) -> ResearchProductReadCapability {
        self.controller.product_research.clone()
    }

    /// Returns provider-neutral option context with truthful startup availability.
    pub(crate) fn options_context_read_capability(&self) -> OptionsContextReadCapability {
        self.controller.options_context.clone()
    }

    /// Returns canonical, provider-neutral market-bar history reads.
    pub(crate) fn market_history_read_capability(&self) -> MarketHistoryReadCapability {
        self.controller.market_history.clone()
    }

    /// Reads bounded, manifest-pinned Fund NAV history through the application lifecycle.
    ///
    /// This is an application-core seam only. It neither selects a provider nor grants ingestion,
    /// publication, network, or operation-registration authority.
    pub async fn read_fund_nav_history(
        &self,
        request: AnalyticalFundNavReadRequest,
        limits: ServiceLimits,
        context: &RequestContext,
    ) -> Result<AnalyticalFundNavOutput, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, context)?;
        self.controller
            .reader
            .read_fund_nav_history(
                request,
                query_limits(limits, context)?,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_read_error)
    }

    /// Installs the exact restart-verified FRED publication into the replaceable Macro route.
    pub(crate) fn install_fred_published(
        &self,
        handoff: FredPublishedGenerationHandoff,
    ) -> Result<(), FredLatestKnownCompositionError> {
        self.controller
            .fred_latest_known
            .install_published(self.controller.authority.as_ref(), handoff)
    }

    /// Marks configured Fiscal Data unavailable until an exact publication is installed.
    pub(crate) fn configure_treasury_fiscal_unavailable(&self) -> Result<(), ServiceError> {
        self.controller
            .treasury_fiscal_latest_known
            .configured_unavailable()
    }

    /// Marks configured daily rates unavailable until an exact publication is installed.
    pub(crate) fn configure_treasury_daily_unavailable(&self) -> Result<(), ServiceError> {
        self.controller
            .treasury_daily_latest_known
            .configured_unavailable()
    }

    /// Installs restart-verified Fiscal Data publication receipts.
    pub(crate) fn install_treasury_fiscal_published(
        &self,
        closure: Arc<TreasuryApplicationClosure>,
        receipts: Vec<TreasuryMacroPublicationReceipt>,
    ) -> Result<(), ServiceError> {
        self.controller
            .treasury_fiscal_latest_known
            .install_published(closure, receipts)
    }

    /// Installs restart-verified daily-rate publication receipts.
    pub(crate) fn install_treasury_daily_published(
        &self,
        closure: Arc<TreasuryApplicationClosure>,
        receipts: Vec<TreasuryMacroPublicationReceipt>,
    ) -> Result<(), ServiceError> {
        self.controller
            .treasury_daily_latest_known
            .install_published(closure, receipts)
    }
}

impl fmt::Debug for ResearchApplicationServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchApplicationServices")
            .field("controller", &self.controller)
            .finish()
    }
}

struct ResearchDomainService {
    controller: Arc<ResearchController>,
}

struct FundamentalDomainService {
    controller: Arc<ResearchController>,
}

struct MacroDomainService {
    controller: Arc<ResearchController>,
}

#[async_trait]
impl ApplicationDomainService for ResearchDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Research
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        let limits = effective_service_limits(&request, &context)?;
        match request.name() {
            RESEARCH_LIST_DATASETS => self.controller.datasets(&request, &context, limits),
            RESEARCH_GET_MANIFEST => self.controller.manifest(&request, &context, limits),
            RESEARCH_GET_HISTORY => {
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::All,
                    )
                    .await
            }
            RESEARCH_GET_ALTERNATIVE_DATA => {
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::AlternativeData,
                    )
                    .await
            }
            RESEARCH_INGEST_SOURCE => {
                self.controller
                    .ingest
                    .ingest(&request, &context, limits)
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl ApplicationDomainService for FundamentalDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Fundamental
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        match request.name() {
            FUNDAMENTAL_GET_FILINGS => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::Filing,
                    )
                    .await
            }
            FUNDAMENTAL_GET_FACTS | FUNDAMENTAL_GET_STATEMENTS | FUNDAMENTAL_GET_RATIOS => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::Fundamental,
                    )
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

#[async_trait]
impl ApplicationDomainService for MacroDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Macro
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let _call = DomainLifecycle::enter(&self.controller.lifecycle, &context)?;
        match request.name() {
            MACRO_GET_CONTEXT => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .macro_context
                    .call(&request, &context, limits)
                    .await
            }
            crate::provider_activation::FRED_ALFRED_READ_OPERATION => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .fred_latest_known
                    .call(&request, &context, limits)
                    .await
            }
            TREASURY_FISCAL_DATA_LATEST_KNOWN_OPERATION => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .treasury_fiscal_latest_known
                    .call(&request, &context, limits)
                    .await
            }
            TREASURY_DAILY_RATES_LATEST_KNOWN_OPERATION => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .treasury_daily_latest_known
                    .call(&request, &context, limits)
                    .await
            }
            MACRO_GET_DASHBOARD => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .macro_dashboard(&request, &context, limits)
                    .await
            }
            MACRO_LIST_SERIES
            | MACRO_GET_OBSERVATIONS
            | MACRO_GET_VINTAGES
            | MACRO_GET_REVISIONS => {
                let limits = effective_service_limits(&request, &context)?;
                self.controller
                    .observations(
                        &request,
                        &context,
                        limits,
                        AnalyticalObservationTemplate::Macro,
                    )
                    .await
            }
            _ => Err(ServiceError::NotFound),
        }
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

struct ResearchController {
    authority: Arc<ResearchService>,
    reader: AnalyticalReadCapability,
    ingest: Arc<dyn ResearchIngestCoordinator>,
    artifacts: Option<Arc<dyn ArtifactRepository>>,
    listing_reference: Option<ListingReferenceReadCapability>,
    company_research: CompanyResearchReadCapability,
    product_research: ResearchProductReadCapability,
    fred_latest_known: FredLatestKnownOperation,
    macro_context: MacroContextOperation,
    options_context: OptionsContextReadCapability,
    market_history: MarketHistoryReadCapability,
    treasury_fiscal_latest_known: TreasuryLatestKnownOperation,
    treasury_daily_latest_known: TreasuryLatestKnownOperation,
    lifecycle: Arc<DomainLifecycle>,
}

impl ResearchController {
    fn datasets(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let after = optional_dataset(request, "afterDataset")?;
        let page_limit = limits.maximum_result_items().min(MAX_ANALYTICAL_PAGE);
        let page = self
            .reader
            .datasets(
                after.as_ref(),
                AnalyticalReadLimit::try_new(page_limit).map_err(map_read_error)?,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(map_read_error)?;
        let returned = page.generations().len();
        if returned == 0 {
            return TypedToolResult::try_new(
                Value::Null,
                0,
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )
            .map_err(Into::into);
        }
        let next_after = page
            .generations()
            .last()
            .filter(|_| page.has_more())
            .map(|generation| generation.manifest().dataset_id().as_str());
        let content = json!({
            "items": page
                .generations()
                .iter()
                .map(generation_value)
                .collect::<Vec<_>>(),
            "hasMore": page.has_more(),
            "nextAfterDataset": next_after,
        });
        TypedToolResult::try_new(
            content,
            returned,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    fn manifest(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let dataset = required_dataset(request)?;
        let generation = self
            .reader
            .latest(&dataset, context.deadline(), context.cancellation())
            .map_err(map_read_error)?
            .ok_or(ServiceError::NotFound)?;
        TypedToolResult::try_new(
            generation_value(&generation),
            1,
            ToolResultMetadata::complete_not_applicable(),
            limits,
        )
        .map_err(Into::into)
    }

    async fn observations(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
        template: AnalyticalObservationTemplate,
    ) -> Result<TypedToolResult, ServiceError> {
        let dataset = required_dataset(request)?;
        let generation = self
            .reader
            .latest(&dataset, context.deadline(), context.cancellation())
            .map_err(map_read_error)?
            .ok_or(ServiceError::NotFound)?;
        let instruments = requested_instruments(request)?;
        let knowledge_range = requested_knowledge_range(request)?;
        let read = AnalyticalObservationReadRequest::try_new(
            generation.manifest().clone(),
            template,
            instruments,
            knowledge_range,
        )
        .map_err(map_read_error)?;
        let query_limits = query_limits(limits, context)?;
        let query = read.query_request().map_err(map_query_error)?;
        let pinned = self
            .authority
            .analytical()
            .pinned(generation.manifest())
            .map_err(|_error| ServiceError::Unavailable)?;
        let owner = SourceIdentifier::try_from(QUERY_ARTIFACT_OWNER)
            .map_err(|_error| ServiceError::Unavailable)?;
        let query = PinnedArtifactQueryRequest::try_new(
            pinned,
            "observations",
            query,
            query_limits,
            owner,
            QUERY_ARTIFACT_TTL,
            context.cancellation().clone(),
        )
        .map_err(map_query_error)?;
        let output = self
            .authority
            .analytical()
            .query_pinned_with_artifact_publication(query)
            .await
            .map_err(map_query_error)?;
        observation_result(
            generation.source_id(),
            output,
            limits,
            self.artifacts.as_ref(),
            self.authority.as_ref(),
            context,
        )
        .await
    }

    async fn macro_dashboard(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        limits: ServiceLimits,
    ) -> Result<TypedToolResult, ServiceError> {
        let (knowledge_cutoff, effective_date_cutoff, evaluated_at) = macro_dashboard_cutoff()?;
        validate_macro_dashboard_request(request)?;
        if limits.maximum_result_items() < MACRO_DASHBOARD_SERIES_COUNT {
            return Err(ServiceError::ResourceExhausted);
        }

        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_rolling_dashboard()
            .map_err(|_error| ServiceError::Unavailable)?;
        validate_macro_dashboard_profile(&profile)?;

        let dataset = DatasetId::try_from(profile.analytical_dataset().as_str())
            .map_err(|_error| ServiceError::Unavailable)?;
        let generation = self
            .reader
            .latest(&dataset, context.deadline(), context.cancellation())
            .map_err(map_read_error)?
            .ok_or(ServiceError::NotFound)?;
        let descriptors = h15_treasury_constant_maturities_dashboard_series();
        let mut series = Vec::new();
        series
            .try_reserve_exact(MACRO_DASHBOARD_SERIES_COUNT)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for descriptor in descriptors {
            series.push(
                descriptor
                    .canonical_macro_series_identifier()
                    .map_err(|_error| ServiceError::Unavailable)?,
            );
        }
        let allowlist = AnalyticalMacroSeriesAllowlist::try_from_code_owned_identifiers(series)
            .map_err(map_read_error)?;
        let source_id =
            SourceId::try_from(BOARD_DDP_SOURCE_ID).map_err(|_error| ServiceError::Unavailable)?;
        let read = AnalyticalMacroLatestKnownRequest::try_new(
            generation.manifest().clone(),
            source_id.clone(),
            knowledge_cutoff,
            effective_date_cutoff,
            allowlist,
        )
        .map_err(map_read_error)?;
        let query_limits = macro_dashboard_query_limits(&read, context)?;
        let output = self
            .reader
            .read_macro_latest_known_snapshot(
                read,
                query_limits,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_read_error)?;
        if output.source_id() != &source_id
            || output.output().manifest() != generation.manifest()
            || output.output().manifest().dataset_id().as_str()
                != profile.analytical_dataset().as_str()
        {
            return Err(ServiceError::InvalidResult);
        }

        macro_dashboard_result(
            &profile,
            &source_id,
            knowledge_cutoff,
            effective_date_cutoff,
            &evaluated_at,
            &output,
            limits,
        )
    }

    fn begin_shutdown(&self) {
        self.lifecycle.begin_shutdown();
        self.ingest.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let drained = self.lifecycle.finish_shutdown(deadline).await;
        let ingest = self.ingest.finish_shutdown(deadline).await;
        drained.and(ingest)
    }
}

impl fmt::Debug for ResearchController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResearchController")
            .field("authority", &"[ANALYTICAL AUTHORITY]")
            .field("reader", &self.reader)
            .field("ingest", &"[EXTRACTION COORDINATOR]")
            .field(
                "artifacts",
                &self.artifacts.as_ref().map(|_| "[OPAQUE REPOSITORY]"),
            )
            .field(
                "listing_reference",
                &self
                    .listing_reference
                    .as_ref()
                    .map(|_| "[OFFICIAL DIRECTORY READ AUTHORITY]"),
            )
            .field("company_research", &self.company_research)
            .field("fred_latest_known", &self.fred_latest_known)
            .field("macro_context", &self.macro_context)
            .field("options_context", &self.options_context)
            .field("market_history", &self.market_history)
            .field("lifecycle", &self.lifecycle)
            .finish()
    }
}

async fn observation_result(
    source_id: &market_squawk_domain::SourceId,
    output: PinnedQueryOutput,
    limits: ServiceLimits,
    artifacts: Option<&Arc<dyn ArtifactRepository>>,
    authority: &ResearchService,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let pinned = &output;
    let manifest = pinned.manifest();
    let coverage = json!({
        "sourceId": source_id,
        "manifest": manifest_value(manifest),
        "objectGraphDigest": encode_hex(pinned.object_graph_digest().bytes()),
        "queryIdentity": encode_hex(pinned.query_identity().bytes()),
        "resultDigest": encode_hex(pinned.result_digest().bytes()),
    });
    let quality = json!({
        "classification": "record_level_provenance",
        "qualityRetainedPerRow": true,
        "executionEligible": false,
    });
    let metadata = ToolResultMetadata::try_complete(coverage, quality)
        .map_err(|_error| ServiceError::InvalidResult)?;
    match pinned.result() {
        QueryResult::Inline {
            batches,
            byte_count,
        } => {
            let returned = batches.iter().try_fold(0_usize, |total, batch| {
                total
                    .checked_add(batch.num_rows())
                    .ok_or(ServiceError::ResourceExhausted)
            })?;
            if returned == 0 {
                return TypedToolResult::try_new(Value::Null, 0, metadata, limits)
                    .map_err(Into::into);
            }
            let manifest_content = manifest_value(manifest);
            let empty_content = json!({
                "manifest": manifest_content.clone(),
                "arrowIpcBytes": byte_count,
                "rows": [],
            });
            let empty_result =
                TypedToolResult::try_new(empty_content, returned, metadata.clone(), limits)
                    .map_err(ServiceError::from)?;
            let maximum_json_bytes = limits
                .maximum_result_bytes()
                .checked_sub(empty_result.encoded_bytes())
                .and_then(|remaining| remaining.checked_add(2))
                .ok_or(ServiceError::ResourceExhausted)?;
            let rows = arrow_rows(batches, maximum_json_bytes)?;
            let content = json!({
                "manifest": manifest_content,
                "arrowIpcBytes": byte_count,
                "rows": rows,
            });
            TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
        }
        QueryResult::Artifact {
            object,
            artifact,
            ownership,
        } => {
            let artifacts = artifacts.ok_or(ServiceError::ResourceExhausted)?;
            let publication = authority.analytical().query_artifact_publication();
            let bytes = publication
                .read_verified_bytes(
                    object,
                    artifact,
                    ownership,
                    limits.maximum_result_bytes(),
                    tokio::time::Instant::from_std(context.deadline()),
                    context.cancellation(),
                )
                .await
                .map_err(map_query_error)?;
            let publication =
                ArtifactPublication::try_parquet(bytes).map_err(map_artifact_error)?;
            let reference = artifacts
                .publish(
                    publication.clone(),
                    ArtifactPublicationContext::new(
                        context.cancellation().clone(),
                        context.deadline(),
                    ),
                )
                .await
                .map_err(map_artifact_error)?;
            if !reference.matches(&publication) {
                return Err(ServiceError::InvalidResult);
            }
            let returned = usize::try_from(object.row_count())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            let content = json!({
                "manifest": manifest_value(manifest),
                "artifact": {
                    "artifactId": reference.id(),
                    "sha256": reference.sha256(),
                    "byteCount": reference.byte_count(),
                    "mediaType": reference.media_type(),
                    "rowCount": object.row_count(),
                },
            });
            TypedToolResult::try_new(content, returned, metadata, limits).map_err(Into::into)
        }
    }
}

fn arrow_rows(
    batches: &[arrow::record_batch::RecordBatch],
    maximum_bytes: usize,
) -> Result<Value, ServiceError> {
    if maximum_bytes == 0 {
        return Err(ServiceError::ResourceExhausted);
    }
    let buffer = BoundedJsonBuffer::new(maximum_bytes);
    let mut writer = ArrayWriter::new(buffer);
    let references = batches.iter().collect::<Vec<_>>();
    writer
        .write_batches(&references)
        .and_then(|()| writer.finish())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let bytes = writer.into_inner().bytes;
    serde_json::from_slice(&bytes).map_err(|_error| ServiceError::InvalidResult)
}

struct BoundedJsonBuffer {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonBuffer {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }
}

impl Write for BoundedJsonBuffer {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let required = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .filter(|required| *required <= self.maximum)
            .ok_or_else(|| io::Error::other("bounded analytical JSON result exceeded"))?;
        self.bytes
            .try_reserve(required.saturating_sub(self.bytes.len()))
            .map_err(|_error| io::Error::other("bounded analytical JSON allocation failed"))?;
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn validate_macro_dashboard_request(request: &TypedToolRequest) -> Result<(), ServiceError> {
    let arguments = request.arguments();
    let provider = arguments
        .get("provider")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)?;
    let release = arguments
        .get("release")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)?;
    if provider != BOARD_DDP_SURFACE_ID
        || release != H15_REQUEST_RELEASE
        || !arguments
            .keys()
            .all(|field| matches!(field.as_str(), "provider" | "release" | "resultLimits"))
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(())
}

fn validate_macro_dashboard_profile(profile: &BoardDatasetProfile) -> Result<(), ServiceError> {
    let contract = profile.contract();
    if !contract.is_h15_treasury_constant_maturities_rolling_dashboard()
        || contract.release() != BoardRelease::H15SelectedInterestRates
        || contract.family() != BoardDatasetFamily::H15TreasuryConstantMaturities
        || contract.frequency() != BoardFrequency::BusinessDaily
        || h15_treasury_constant_maturities_dashboard_series().len() != MACRO_DASHBOARD_SERIES_COUNT
    {
        return Err(ServiceError::Unavailable);
    }
    Ok(())
}

fn macro_dashboard_cutoff() -> Result<(Timestamp, CalendarDate, String), ServiceError> {
    let evaluated_at = Utc::now();
    let unix_nanos = evaluated_at
        .timestamp_nanos_opt()
        .ok_or(ServiceError::Unavailable)?;
    let year = u16::try_from(evaluated_at.year()).map_err(|_error| ServiceError::Unavailable)?;
    let month = u8::try_from(evaluated_at.month()).map_err(|_error| ServiceError::Unavailable)?;
    let day = u8::try_from(evaluated_at.day()).map_err(|_error| ServiceError::Unavailable)?;
    let effective_date =
        CalendarDate::new(year, month, day).map_err(|_error| ServiceError::Unavailable)?;
    Ok((
        Timestamp::from_unix_nanos(unix_nanos),
        effective_date,
        evaluated_at.to_rfc3339_opts(SecondsFormat::Nanos, true),
    ))
}

fn macro_dashboard_query_limits(
    request: &AnalyticalMacroLatestKnownRequest,
    context: &RequestContext,
) -> Result<QueryLimits, ServiceError> {
    let now = Instant::now();
    if now >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let deadline = context
        .deadline()
        .saturating_duration_since(now)
        .min(Duration::from_secs(60));
    QueryLimits::try_new_with_inline_bytes(
        request.required_query_rows(),
        MACRO_DASHBOARD_QUERY_BYTES,
        MACRO_DASHBOARD_QUERY_BYTES,
        MACRO_DASHBOARD_QUERY_MEMORY_BYTES,
        4,
        2_048,
        4_096,
        deadline,
    )
    .map_err(map_query_error)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the immutable dashboard result binds every independent selection authority"
)]
fn macro_dashboard_result(
    profile: &BoardDatasetProfile,
    source_id: &SourceId,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
    evaluated_at: &str,
    output: &AnalyticalMacroLatestKnownOutput,
    limits: ServiceLimits,
) -> Result<TypedToolResult, ServiceError> {
    let expected_unit = h15_treasury_constant_maturities_canonical_unit_identifier()
        .map_err(|_error| ServiceError::Unavailable)?;
    let actual = output.observations();
    if actual.len() != MACRO_DASHBOARD_SERIES_COUNT {
        return Err(ServiceError::InvalidResult);
    }

    let mut selected = [false; MACRO_DASHBOARD_SERIES_COUNT];
    let mut observations = Vec::new();
    observations
        .try_reserve_exact(MACRO_DASHBOARD_SERIES_COUNT)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut available_series = 0_usize;
    let mut missing_series = 0_usize;
    for descriptor in h15_treasury_constant_maturities_dashboard_series() {
        let expected_series = descriptor
            .canonical_macro_series_identifier()
            .map_err(|_error| ServiceError::Unavailable)?;
        let mut matching = actual
            .iter()
            .enumerate()
            .filter(|(_, observation)| observation.series() == &expected_series);
        let (index, observation) = matching.next().ok_or(ServiceError::InvalidResult)?;
        if matching.next().is_some() || selected[index] {
            return Err(ServiceError::InvalidResult);
        }
        selected[index] = true;
        let (value, observed) = macro_dashboard_observation_value(
            *descriptor,
            observation,
            source_id,
            &expected_series,
            &expected_unit,
            knowledge_cutoff,
            effective_date_cutoff,
        )?;
        if observed {
            available_series = available_series
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
        } else {
            missing_series = missing_series
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
        }
        observations.push(value);
    }
    if selected.iter().any(|matched| !matched)
        || available_series
            .checked_add(missing_series)
            .filter(|total| *total == MACRO_DASHBOARD_SERIES_COUNT)
            .is_none()
    {
        return Err(ServiceError::InvalidResult);
    }

    let pinned = output.output();
    let object_graph_digest = sha256_digest_hex(pinned.object_graph_digest())?;
    let query_identity = sha256_digest_hex(pinned.query_identity())?;
    let result_digest = sha256_digest_hex(pinned.result_digest())?;
    let selection_digest = sha256_digest_hex(output.selection_digest())?;
    let manifest = macro_dashboard_manifest_value(pinned.manifest());
    let content = json!({
        "schemaIdentity": MACRO_DASHBOARD_SCHEMA_IDENTITY,
        "binding": {
            "surfaceId": BOARD_DDP_SURFACE_ID,
            "sourceId": source_id.as_str(),
            "providerDatasetId": profile.dataset().as_str(),
            "analyticalDatasetId": profile.analytical_dataset().as_str(),
            "manifest": manifest.clone(),
            "objectGraphDigest": object_graph_digest,
            "queryIdentity": query_identity,
            "resultDigest": result_digest,
        },
        "release": {
            "code": profile.contract().release().code(),
            "title": profile.contract().release().title(),
            "family": profile.contract().family().as_str(),
            "frequency": "business_daily",
            "quality": "official_delayed",
        },
        "selection": {
            "policy": MACRO_DASHBOARD_SELECTION_POLICY,
            "evaluatedAt": evaluated_at,
            "selectionDigest": selection_digest,
            "returnedSeries": MACRO_DASHBOARD_SERIES_COUNT,
            "availableSeries": available_series,
            "missingSeries": missing_series,
            "complete": true,
        },
        "observations": observations,
    });
    let source_coverage = json!({
        "sourceId": source_id.as_str(),
        "manifest": manifest,
        "objectGraphDigest": content["binding"]["objectGraphDigest"].clone(),
        "queryIdentity": content["binding"]["queryIdentity"].clone(),
        "resultDigest": content["binding"]["resultDigest"].clone(),
        "selectionDigest": content["selection"]["selectionDigest"].clone(),
    });
    let data_quality = json!({
        "classification": "official_delayed",
        "recordLevelProvenance": true,
        "observedSeries": available_series,
        "missingSeries": missing_series,
        "executionEligible": false,
        "executionEligibility": "research_only_execution_ineligible",
    });
    let metadata = ToolResultMetadata::try_complete(source_coverage, data_quality)
        .map_err(|_error| ServiceError::InvalidResult)?;
    TypedToolResult::try_new(content, MACRO_DASHBOARD_SERIES_COUNT, metadata, limits)
        .map_err(Into::into)
}

#[allow(
    clippy::too_many_arguments,
    reason = "every independent observation invariant must remain explicit"
)]
fn macro_dashboard_observation_value(
    descriptor: BoardH15DashboardSeriesDescriptor,
    observation: &MacroObservation,
    source_id: &SourceId,
    expected_series: &SourceIdentifier,
    expected_unit: &SourceIdentifier,
    knowledge_cutoff: Timestamp,
    effective_date_cutoff: CalendarDate,
) -> Result<(Value, bool), ServiceError> {
    let context = observation.context();
    let provenance = context.provenance();
    let time = context.time();
    let effective_date = time
        .effective()
        .calendar_date_value()
        .filter(|date| *date <= effective_date_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    let available_at = provenance
        .availability()
        .conservative_available_at()
        .filter(|available_at| *available_at <= knowledge_cutoff)
        .ok_or(ServiceError::InvalidResult)?;
    if observation.series() != expected_series
        || observation.unit() != expected_unit
        || provenance.source_id() != source_id
        || provenance.instrument_id().is_some()
        || provenance.venue_id().is_some()
        || provenance.quality() != DataQuality::OfficialDelayed
        || provenance.received_at() > knowledge_cutoff
        || provenance.ingested_at() > knowledge_cutoff
    {
        return Err(ServiceError::InvalidResult);
    }
    let payload_digest = match provenance.payload_reference() {
        PayloadReference::ContentHash(payload)
            if payload.algorithm() == DigestAlgorithm::Sha256 =>
        {
            encode_hex(payload.digest())
        }
        PayloadReference::ContentHash(_) | PayloadReference::SourceReference(_) => {
            return Err(ServiceError::InvalidResult);
        }
    };
    let (value, observed) = match (
        observation.value().observed_value(),
        observation.value().missing_value(),
    ) {
        (Some(value), None) => (
            json!({
                "state": "observed",
                "decimal": value.normalize().to_string(),
            }),
            true,
        ),
        (None, Some(missing)) => (
            json!({
                "state": "missing",
                "marker": missing.marker().as_str(),
                "reason": missing.reason().map(SourceIdentifier::as_str),
            }),
            false,
        ),
        (Some(_), Some(_)) | (None, None) => return Err(ServiceError::InvalidResult),
    };
    Ok((
        json!({
            "slot": descriptor.slot(),
            "label": descriptor.label(),
            "maturityOrder": descriptor.maturity_order(),
            "seriesId": observation.series().as_str(),
            "unitId": observation.unit().as_str(),
            "unitPresentation": descriptor.unit_presentation(),
            "effectiveDate": effective_date.to_string(),
            "availableAt": timestamp_rfc3339(available_at)?,
            "revision": time.revision().get(),
            "observation": value,
            "sourceIdentifier": provenance.source_identifier().as_str(),
            "sourcePayloadDigest": payload_digest,
        }),
        observed,
    ))
}

fn timestamp_rfc3339(timestamp: Timestamp) -> Result<String, ServiceError> {
    let unix_nanos = timestamp.unix_nanos();
    let seconds = unix_nanos.div_euclid(1_000_000_000);
    let nanoseconds = u32::try_from(unix_nanos.rem_euclid(1_000_000_000))
        .map_err(|_error| ServiceError::InvalidResult)?;
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds)
        .map(|value| value.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .ok_or(ServiceError::InvalidResult)
}

fn sha256_digest_hex(digest: EvidenceDigest) -> Result<String, ServiceError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ServiceError::InvalidResult);
    }
    Ok(encode_hex(digest.bytes()))
}

fn macro_dashboard_manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "datasetId": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version().to_string(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint()),
        },
        "contentHash": encode_hex(manifest.content_hash().bytes()),
    })
}

fn query_limits(
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<QueryLimits, ServiceError> {
    let now = Instant::now();
    if now >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let deadline = context
        .deadline()
        .saturating_duration_since(now)
        .min(Duration::from_secs(60));
    let rows = u64::try_from(limits.maximum_result_items())
        .map_err(|_error| ServiceError::InvalidRequest)?;
    let inline_bytes = u64::try_from(limits.maximum_inline_bytes())
        .map_err(|_error| ServiceError::InvalidRequest)?;
    let bytes = limits.maximum_result_bytes();
    let bytes = u64::try_from(bytes).map_err(|_error| ServiceError::InvalidRequest)?;
    // The result envelope is intentionally small for desktop and MCP callers, but DataFusion's
    // bounded planning and Parquet publication receipts are independent working-set costs. Keep
    // that internal budget finite without making a small response limit impossible to execute.
    let memory = bytes
        .saturating_mul(4)
        .clamp(MINIMUM_ANALYTICAL_QUERY_MEMORY_BYTES, 1024 * 1024 * 1024);
    QueryLimits::try_new_with_inline_bytes(
        rows,
        inline_bytes,
        bytes,
        memory,
        4,
        2_048,
        4_096,
        deadline,
    )
    .map_err(map_query_error)
}

fn map_artifact_error(error: ArtifactError) -> ServiceError {
    match error {
        ArtifactError::Cancelled => ServiceError::Cancelled,
        ArtifactError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ArtifactError::ReadLimitExceeded => ServiceError::ResourceExhausted,
        ArtifactError::InvalidPublication
        | ArtifactError::InvalidReference
        | ArtifactError::NotFound
        | ArtifactError::Unavailable => ServiceError::Unavailable,
    }
}

fn required_dataset(request: &TypedToolRequest) -> Result<DatasetId, ServiceError> {
    optional_dataset(request, "dataset")?.ok_or(ServiceError::InvalidRequest)
}

fn optional_dataset(
    request: &TypedToolRequest,
    field: &str,
) -> Result<Option<DatasetId>, ServiceError> {
    request
        .arguments()
        .get(field)
        .map(|value| {
            value
                .as_str()
                .ok_or(ServiceError::InvalidRequest)
                .and_then(|value| {
                    DatasetId::try_from(value).map_err(|_error| ServiceError::InvalidRequest)
                })
        })
        .transpose()
}

fn requested_instruments(request: &TypedToolRequest) -> Result<Vec<InstrumentId>, ServiceError> {
    request
        .arguments()
        .get("instrumentIds")
        .map(|value| {
            value
                .as_array()
                .ok_or(ServiceError::InvalidRequest)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .ok_or(ServiceError::InvalidRequest)
                        .and_then(|value| {
                            InstrumentId::from_str(value)
                                .map_err(|_error| ServiceError::InvalidRequest)
                        })
                })
                .collect()
        })
        .transpose()
        .map(Option::unwrap_or_default)
}

fn requested_knowledge_range(
    request: &TypedToolRequest,
) -> Result<Option<market_squawk_data::ObservationKnowledgeRange>, ServiceError> {
    let Some(range) = request.arguments().get("timeRange") else {
        return Ok(None);
    };
    let range = range.as_object().ok_or(ServiceError::InvalidRequest)?;
    let start = range
        .get("start")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_timestamp)?;
    let end = range
        .get("end")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
        .and_then(parse_timestamp)?;
    market_squawk_data::ObservationKnowledgeRange::try_new(start, end)
        .map(Some)
        .map_err(map_read_error)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, ServiceError> {
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_error| ServiceError::InvalidRequest)?;
    parsed
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::InvalidRequest)
}

fn generation_value(generation: &AnalyticalGeneration) -> Value {
    let mut value = json!({
        "manifest": manifest_value(generation.manifest()),
        "sourceId": generation.source_id(),
        "generationKind": generation_kind(generation.generation_kind()),
        "buildSpecDigest": generation
            .build_spec_digest()
            .map(|digest| encode_hex(digest.digest().bytes())),
        "parents": generation
            .parents()
            .iter()
            .map(|parent| json!({
                "relation": parent_relation(parent.relation()),
                "manifest": manifest_value(parent.manifest()),
            }))
            .collect::<Vec<_>>(),
        "rowCount": generation.row_count(),
        "totalBytes": generation.total_bytes(),
        "lineageDigest": encode_hex(generation.lineage_digest().bytes()),
        "objectCount": generation.object_count(),
    });
    if generation.generation_kind() == GenerationKind::Derived
        && let Value::Object(fields) = &mut value
    {
        fields.insert(
            "publicationStage".to_owned(),
            Value::String("phase_one_derived_generation".to_owned()),
        );
        fields.insert(
            "phaseOneDescriptorSha256".to_owned(),
            generation
                .python_export_sha256()
                .map(|digest| Value::String(encode_hex(digest.bytes())))
                .unwrap_or(Value::Null),
        );
        // This analytical-generation projection does not establish product-admission state.
        // Receipt-backed admission remains available only through Analysis.GetFeatureDatasets.
        fields.insert(
            "productAdmission".to_owned(),
            Value::String("not_established_on_this_surface".to_owned()),
        );
    }
    value
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "datasetId": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema_version().get(),
            "fingerprint": encode_hex(manifest.schema().fingerprint()),
        },
        "contentHash": encode_hex(manifest.content_hash().bytes()),
    })
}

const fn generation_kind(kind: GenerationKind) -> &'static str {
    match kind {
        GenerationKind::Ingest => "ingest",
        GenerationKind::Compaction => "compaction",
        GenerationKind::Derived => "derived",
    }
}

const fn parent_relation(relation: GenerationParentRelation) -> &'static str {
    match relation {
        GenerationParentRelation::AppendPredecessor => "append_predecessor",
        GenerationParentRelation::CompactionPredecessor => "compaction_predecessor",
        GenerationParentRelation::DerivedInput => "derived_input",
    }
}

fn encode_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn map_read_error(error: AnalyticalReadError) -> ServiceError {
    match error {
        AnalyticalReadError::InvalidLimit
        | AnalyticalReadError::InstrumentLimitExceeded
        | AnalyticalReadError::InvalidKnowledgeRange
        | AnalyticalReadError::UniverseMembershipReadMustBeExhaustive
        | AnalyticalReadError::InvalidMarketBarLimit
        | AnalyticalReadError::InvalidMarketBarEffectiveRange
        | AnalyticalReadError::InvalidFundNavLimit
        | AnalyticalReadError::InvalidFundNavDateRange
        | AnalyticalReadError::InvalidMacroSeriesAllowlist
        | AnalyticalReadError::MacroSnapshotSourceOwnerMismatch
        | AnalyticalReadError::InvalidOutcomeMarketBarWindow
        | AnalyticalReadError::InvalidObservationSchema => ServiceError::InvalidRequest,
        AnalyticalReadError::MarketBarResultRequiresInline
        | AnalyticalReadError::InvalidMarketBarResult
        | AnalyticalReadError::FundNavResultRequiresInline
        | AnalyticalReadError::InvalidFundNavResult
        | AnalyticalReadError::MacroSnapshotResultRequiresInline
        | AnalyticalReadError::MacroSnapshotCandidateSetSaturated
        | AnalyticalReadError::MacroSnapshotRevisionConflict
        | AnalyticalReadError::MacroSnapshotIncomplete
        | AnalyticalReadError::InvalidMacroSnapshotResult => ServiceError::InvalidResult,
        AnalyticalReadError::ForecastDatasetUnavailable => ServiceError::NotFound,
        AnalyticalReadError::Parquet(_) | AnalyticalReadError::PythonDataset(_) => {
            ServiceError::Unavailable
        }
        AnalyticalReadError::Manifest(error) => map_manifest_error(error),
        AnalyticalReadError::Query(error) => map_query_error(error),
    }
}

fn map_manifest_error(error: ManifestCatalogError) -> ServiceError {
    match error {
        ManifestCatalogError::Cancelled => ServiceError::Cancelled,
        ManifestCatalogError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ManifestCatalogError::ObjectLimitExceeded { .. }
        | ManifestCatalogError::ReferenceWorkLimitExceeded { .. }
        | ManifestCatalogError::FeatureDatasetCandidateLimitExceeded { .. }
        | ManifestCatalogError::CountOverflow
        | ManifestCatalogError::AllocationContract => ServiceError::ResourceExhausted,
        ManifestCatalogError::InvalidConfiguration
        | ManifestCatalogError::MigrationMissing
        | ManifestCatalogError::AnchorMismatch
        | ManifestCatalogError::GenerationConflict
        | ManifestCatalogError::SourceMismatch
        | ManifestCatalogError::SchemaMismatch
        | ManifestCatalogError::SchemaIdentity(_)
        | ManifestCatalogError::CorruptCatalog
        | ManifestCatalogError::CaptureInputLimitExceeded { .. }
        | ManifestCatalogError::MarketBarHistoryMismatch
        | ManifestCatalogError::MarketBarHistoryInputLimitExceeded { .. }
        | ManifestCatalogError::ProviderMacroPlanMismatch
        | ManifestCatalogError::LockPoisoned
        | ManifestCatalogError::Plan(_)
        | ManifestCatalogError::Path(_)
        | ManifestCatalogError::Sqlite(_)
        | ManifestCatalogError::CatalogAuthority(_) => ServiceError::Unavailable,
    }
}

fn map_query_error(error: QueryError) -> ServiceError {
    match error {
        QueryError::Cancelled => ServiceError::Cancelled,
        QueryError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        QueryError::InvalidLimits
        | QueryError::RowLimitExceeded { .. }
        | QueryError::ByteLimitExceeded { .. }
        | QueryError::MemoryLimitExceeded { .. }
        | QueryError::SizeOverflow
        | QueryError::DependencyAllocationContract
        | QueryError::BlockingTaskLimitExceeded
        | QueryError::ReaderMemoryBoundExceeded
        | QueryError::ArtifactStoreRequired
        | QueryError::ArtifactAuthorityRequired => ServiceError::ResourceExhausted,
        QueryError::InvalidSql
        | QueryError::Parse(_)
        | QueryError::ForbiddenStatement
        | QueryError::ForbiddenTableFunction
        | QueryError::ForbiddenFunction
        | QueryError::ForbiddenRelation
        | QueryError::InvalidSource
        | QueryError::ManifestPinMismatch
        | QueryError::PinnedQuerySourceRequired
        | QueryError::MonetaryValueRequiresInlineResult
        | QueryError::MonetaryCellOutOfBounds
        | QueryError::InvalidMonetaryCell
        | QueryError::UnsupportedMonetaryScale
        | QueryError::AstLimitExceeded
        | QueryError::PlanLimitExceeded
        | QueryError::PartitionLimitExceeded
        | QueryError::UnsupportedSourceSchema
        | QueryError::ArtifactReservationMismatch
        | QueryError::ArtifactRootMismatch
        | QueryError::DataFusion(_)
        | QueryError::Arrow(_)
        | QueryError::Artifact(_)
        | QueryError::ArrowConversion(_)
        | QueryError::Catalog(_)
        | QueryError::Io(_)
        | QueryError::ObjectStore(_) => ServiceError::Unavailable,
    }
}
