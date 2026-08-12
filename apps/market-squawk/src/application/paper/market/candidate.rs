//! Exact live-market evidence for imported-portfolio candidate impact.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{
    CatalogError, InstrumentDefinitionReadCapability, MarketDataInstrumentCatalogError,
    MarketDataInstrumentReadCapability,
};
use market_squawk_domain::{
    AssetClass, DataQuality, InstrumentDefinition, InstrumentId, Timestamp,
};
use market_squawk_services::ServiceError;
use tokio_util::sync::CancellationToken;

use super::{
    MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT, build_surface_policies,
    collect_candidate_streams,
    unified::{
        UnifiedInstrumentDefinition, build_candidates, read_selected_market_investment,
        validate_inputs,
    },
};
use crate::{
    application::{
        market_runtime::MarketRuntimeRegistry,
        market_selection::{
            DowngradePolicy, FreshnessBasis, FreshnessRequirement, MarketCoverage,
            MarketInvestmentRead, MarketOperation, MarketOperationSet, MarketSelectionPolicy,
            MarketSelectionRequest, ObservationTiming, RequestPriority, select_market_source,
        },
        recommendation::{
            RecommendationSetupAuthority, RecommendationSetupError, RecommendationSetupResolution,
            ResolvedRecommendationSetup,
        },
    },
    portfolio_application::{
        PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
        PortfolioAccountCatalogSnapshot, PortfolioApplicationServiceError,
        PortfolioCandidateAvailability, PortfolioCandidateResolution,
        PortfolioCandidateResolutionAuthority, PortfolioCandidateUnavailableReason,
    },
};

const MAXIMUM_PORTFOLIO_MARK_CANDIDATES: usize = 256;

/// Market-only factory retained by composition until the workspace setup authority is opened.
#[derive(Clone)]
pub(in crate::application::paper) struct ProductionPortfolioCandidateResolutionFactory {
    registry: Arc<MarketRuntimeRegistry>,
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
    maximum_mark_age_nanos: u64,
}

impl ProductionPortfolioCandidateResolutionFactory {
    pub(in crate::application::paper) fn try_new(
        registry: Arc<MarketRuntimeRegistry>,
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        if maximum_mark_age_nanos == 0 || maximum_mark_age_nanos > i64::MAX as u64 {
            return Err(ServiceError::Internal);
        }
        Ok(Self {
            registry,
            instrument_definitions,
            market_data_instruments,
            maximum_mark_age_nanos,
        })
    }

    /// Binds only durable setup and immutable imported-portfolio read capabilities.
    pub(in crate::application::paper) fn bind(
        &self,
        setup: Arc<RecommendationSetupAuthority>,
        catalog: PortfolioAccountCatalogReadCapability,
    ) -> Arc<dyn PortfolioCandidateResolutionAuthority> {
        Arc::new(ProductionPortfolioCandidateResolutionAuthority {
            registry: Arc::clone(&self.registry),
            instrument_definitions: self.instrument_definitions.clone(),
            market_data_instruments: self.market_data_instruments.clone(),
            maximum_mark_age_nanos: self.maximum_mark_age_nanos,
            setup,
            catalog,
        })
    }
}

impl fmt::Debug for ProductionPortfolioCandidateResolutionFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProductionPortfolioCandidateResolutionFactory")
            .field("market", &"[CURRENT MARKET READ AUTHORITIES]")
            .field("maximum_mark_age_nanos", &self.maximum_mark_age_nanos)
            .finish()
    }
}

struct ProductionPortfolioCandidateResolutionAuthority {
    registry: Arc<MarketRuntimeRegistry>,
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
    maximum_mark_age_nanos: u64,
    setup: Arc<RecommendationSetupAuthority>,
    catalog: PortfolioAccountCatalogReadCapability,
}

#[async_trait]
impl PortfolioCandidateResolutionAuthority for ProductionPortfolioCandidateResolutionAuthority {
    async fn resolve(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioCandidateResolution, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        let catalog = self
            .catalog
            .snapshot_current(deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let setup = match self
            .setup
            .resolve(&catalog, as_of)
            .map_err(map_setup_error)?
        {
            RecommendationSetupResolution::Ready(setup) => setup,
            RecommendationSetupResolution::SetupRequired(_evidence) => {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
        };
        let resolution = self
            .resolve_market(
                instrument_id,
                as_of,
                deadline,
                &cancellation,
                setup,
                catalog,
            )
            .await?;
        self.recheck_setup(&resolution, as_of, deadline, &cancellation)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(resolution)
    }

    async fn recheck(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if expected.setup().as_of() != as_of
            || expected.market().observation().instrument_id()
                != expected.market().selection().instrument_id()
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.recheck_setup(expected, as_of, deadline, &cancellation)?;
        let current = self
            .resolve(
                expected.market().observation().instrument_id(),
                as_of,
                deadline,
                cancellation.clone(),
            )
            .await?;
        if &current != expected {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        self.recheck_setup(expected, as_of, deadline, &cancellation)?;
        ensure_before(as_of, deadline, &cancellation)
    }
}

impl ProductionPortfolioCandidateResolutionAuthority {
    async fn resolve_market(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        setup: ResolvedRecommendationSetup,
        catalog: PortfolioAccountCatalogSnapshot,
    ) -> Result<PortfolioCandidateResolution, PortfolioApplicationServiceError> {
        let snapshots = self
            .registry
            .snapshots(deadline, cancellation)
            .await
            .map_err(map_service_error)?;
        if !snapshots.failures().is_empty() {
            return Err(PortfolioApplicationServiceError::Authority);
        }
        let streams = collect_candidate_streams(&snapshots, instrument_id, deadline, cancellation)
            .map_err(map_service_error)?;
        let maximum_display_sources =
            NonZeroUsize::new(MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT)
                .ok_or(PortfolioApplicationServiceError::Authority)?;
        let display = self
            .registry
            .display_snapshots_for_instrument(
                instrument_id,
                maximum_display_sources,
                as_of,
                deadline,
                cancellation,
            )
            .await
            .map_err(map_service_error)?;
        let display_snapshots = display.snapshots().iter().collect::<Vec<_>>();
        let kraken = self
            .registry
            .kraken_price_projection(instrument_id, deadline, cancellation)
            .await
            .map_err(map_service_error)?;
        let kraken_projections = kraken.iter().collect::<Vec<_>>();

        let definition = self.load_execution_definition(instrument_id, deadline, cancellation)?;
        let market_data_definition = if display_snapshots.is_empty() {
            None
        } else {
            Some(
                self.market_data_instruments
                    .latest(instrument_id, deadline, cancellation)
                    .map_err(map_market_data_catalog_error)?
                    .ok_or(PortfolioApplicationServiceError::Authority)?
                    .definition()
                    .clone(),
            )
        };
        let market_data_definitions = market_data_definition
            .as_ref()
            .map(std::slice::from_ref)
            .unwrap_or(&[]);
        let definition_view = UnifiedInstrumentDefinition::try_new(
            instrument_id,
            Some(&definition),
            market_data_definition.as_ref(),
        )
        .map_err(map_service_error)?;
        let operations = MarketOperationSet::try_new(&[MarketOperation::PortfolioMark])
            .map_err(|_error| PortfolioApplicationServiceError::Authority)?;
        let policies = build_surface_policies(
            &snapshots,
            &display_snapshots,
            &kraken_projections,
            as_of,
            operations,
        )
        .map_err(map_service_error)?;
        validate_inputs(
            &streams,
            std::slice::from_ref(&definition),
            market_data_definitions,
            &display_snapshots,
            &kraken_projections,
            &policies,
            &[],
        )
        .map_err(map_service_error)?;
        let candidates = build_candidates(
            &streams,
            definition_view,
            &display_snapshots,
            &kraken_projections,
            &policies,
            &[],
            as_of,
        )
        .map_err(map_service_error)?;
        let request = portfolio_mark_request(
            definition_view.asset_class(),
            as_of,
            self.maximum_mark_age_nanos,
        )?;
        let policy = MarketSelectionPolicy::v1(MAXIMUM_PORTFOLIO_MARK_CANDIDATES)
            .map_err(|_error| PortfolioApplicationServiceError::Authority)?;
        let receipt =
            select_market_source(policy, request, candidates).map_err(map_selection_error)?;
        let observation = match read_selected_market_investment(
            definition_view,
            &streams,
            &display_snapshots,
            &kraken_projections,
            &receipt,
        )
        .map_err(map_service_error)?
        {
            MarketInvestmentRead::Available(observation) => observation,
            MarketInvestmentRead::Unavailable(_reason) => {
                return Err(PortfolioApplicationServiceError::Authority);
            }
        };
        let execution_terms = definition_view
            .execution_terms()
            .map_err(map_service_error)?;
        PortfolioCandidateResolution::try_from_authorities(
            setup,
            catalog,
            &receipt,
            observation,
            execution_terms,
            PortfolioCandidateAvailability::Unavailable(PortfolioCandidateUnavailableReason::Fees),
            PortfolioCandidateAvailability::Unavailable(
                PortfolioCandidateUnavailableReason::Slippage,
            ),
        )
    }

    fn load_execution_definition(
        &self,
        instrument_id: InstrumentId,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<InstrumentDefinition, PortfolioApplicationServiceError> {
        let mut definitions = self
            .instrument_definitions
            .latest(&[instrument_id], 1, deadline, cancellation)
            .map_err(map_catalog_read_error)?;
        if definitions.len() != 1 {
            return Err(PortfolioApplicationServiceError::NotFound);
        }
        let definition = definitions
            .pop()
            .ok_or(PortfolioApplicationServiceError::NotFound)?;
        if definition.instrument_id() != instrument_id {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(definition)
    }

    fn recheck_setup(
        &self,
        expected: &PortfolioCandidateResolution,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        self.catalog
            .recheck(expected.catalog(), deadline, cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(expected.setup(), expected.catalog(), as_of)
            .map_err(map_setup_error)
    }
}

fn portfolio_mark_request(
    asset_class: AssetClass,
    as_of: Timestamp,
    maximum_mark_age_nanos: u64,
) -> Result<MarketSelectionRequest, PortfolioApplicationServiceError> {
    let (coverage, coverage_downgrades): (MarketCoverage, &[MarketCoverage]) =
        if asset_class == AssetClass::Index {
            (MarketCoverage::Benchmark, &[])
        } else {
            (
                MarketCoverage::Consolidated,
                &[
                    MarketCoverage::MultiVenuePartial,
                    MarketCoverage::SingleVenue,
                ],
            )
        };
    let downgrade = DowngradePolicy::try_new(
        &[],
        &[],
        &[DataQuality::DirectUnverified],
        coverage_downgrades,
        None,
    )
    .map_err(map_selection_error)?;
    MarketSelectionRequest::try_new(
        asset_class,
        MarketOperation::PortfolioMark,
        ObservationTiming::RealTime,
        None,
        DataQuality::DirectVerified,
        coverage,
        FreshnessRequirement::try_new(as_of, FreshnessBasis::Received, maximum_mark_age_nanos)
            .map_err(map_selection_error)?,
        RequestPriority::Interactive,
        downgrade,
    )
    .map_err(map_selection_error)
}

fn ensure_before(
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), PortfolioApplicationServiceError> {
    if as_of.unix_nanos() <= 0 {
        Err(PortfolioApplicationServiceError::InvalidRequest)
    } else if cancellation.is_cancelled() {
        Err(PortfolioApplicationServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(PortfolioApplicationServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_catalog_error(error: PortfolioAccountCatalogError) -> PortfolioApplicationServiceError {
    match error {
        PortfolioAccountCatalogError::Portfolio(error) => error,
        PortfolioAccountCatalogError::CorruptPublication => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        PortfolioAccountCatalogError::ResourceExhausted => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        PortfolioAccountCatalogError::CatalogChanged => {
            PortfolioApplicationServiceError::StateChanged
        }
    }
}

fn map_setup_error(error: RecommendationSetupError) -> PortfolioApplicationServiceError {
    match error {
        RecommendationSetupError::InvalidProfile
        | RecommendationSetupError::AccountUnavailable
        | RecommendationSetupError::CurrencyMismatch
        | RecommendationSetupError::InvalidAsOf => PortfolioApplicationServiceError::InvalidRequest,
        RecommendationSetupError::CapacityExceeded => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        RecommendationSetupError::StateChanged
        | RecommendationSetupError::StaleRevision
        | RecommendationSetupError::StaleCatalog => PortfolioApplicationServiceError::StateChanged,
        RecommendationSetupError::CorruptState | RecommendationSetupError::Encoding => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        RecommendationSetupError::Unavailable
        | RecommendationSetupError::RecoveryRequired
        | RecommendationSetupError::TimeUnavailable
        | RecommendationSetupError::Persistence(_) => PortfolioApplicationServiceError::Authority,
        RecommendationSetupError::PreviewUnavailable
        | RecommendationSetupError::PreviewExpired
        | RecommendationSetupError::InvalidConfirmation
        | RecommendationSetupError::CrossWorkspacePreview
        | RecommendationSetupError::RevisionExhausted
        | RecommendationSetupError::InvalidBackup
        | RecommendationSetupError::RestoreTargetOccupied => {
            PortfolioApplicationServiceError::Authority
        }
    }
}

fn map_catalog_read_error(error: CatalogError) -> PortfolioApplicationServiceError {
    match error {
        CatalogError::InstrumentDefinitionReadCancelled => {
            PortfolioApplicationServiceError::Cancelled
        }
        CatalogError::InstrumentDefinitionReadDeadlineExceeded => {
            PortfolioApplicationServiceError::DeadlineExceeded
        }
        CatalogError::ResultByteLimitExceeded
        | CatalogError::ResultRowLimitExceeded
        | CatalogError::Allocation => PortfolioApplicationServiceError::ResourceExhausted,
        CatalogError::CorruptCatalog | CatalogError::InvalidRecord => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        _ => PortfolioApplicationServiceError::Authority,
    }
}

fn map_market_data_catalog_error(
    error: MarketDataInstrumentCatalogError,
) -> PortfolioApplicationServiceError {
    match error {
        MarketDataInstrumentCatalogError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        MarketDataInstrumentCatalogError::DeadlineExceeded => {
            PortfolioApplicationServiceError::DeadlineExceeded
        }
        MarketDataInstrumentCatalogError::ResultByteLimitExceeded => {
            PortfolioApplicationServiceError::ResourceExhausted
        }
        MarketDataInstrumentCatalogError::CorruptCatalog => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        _ => PortfolioApplicationServiceError::Authority,
    }
}

fn map_selection_error(
    error: crate::application::market_selection::MarketSelectionError,
) -> PortfolioApplicationServiceError {
    match error {
        crate::application::market_selection::MarketSelectionError::Allocation
        | crate::application::market_selection::MarketSelectionError::TooManyCandidates {
            ..
        } => PortfolioApplicationServiceError::ResourceExhausted,
        _ => PortfolioApplicationServiceError::Authority,
    }
}

const fn map_service_error(error: ServiceError) -> PortfolioApplicationServiceError {
    match error {
        ServiceError::InvalidRequest => PortfolioApplicationServiceError::InvalidRequest,
        ServiceError::NotFound => PortfolioApplicationServiceError::NotFound,
        ServiceError::ResourceExhausted => PortfolioApplicationServiceError::ResourceExhausted,
        ServiceError::Cancelled => PortfolioApplicationServiceError::Cancelled,
        ServiceError::DeadlineExceeded => PortfolioApplicationServiceError::DeadlineExceeded,
        ServiceError::InvalidResult | ServiceError::Internal => {
            PortfolioApplicationServiceError::CorruptPublication
        }
        ServiceError::Unauthorized | ServiceError::Unavailable => {
            PortfolioApplicationServiceError::Authority
        }
    }
}
