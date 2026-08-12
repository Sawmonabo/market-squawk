//! Exact live-market evidence for imported-portfolio candidate impact.

use std::{fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_data::{
    CatalogError, InstrumentDefinitionReadCapability, MarketDataInstrumentCatalogError,
    MarketDataInstrumentReadCapability,
};
use market_squawk_domain::{
    AssetClass, DataQuality, InstrumentDefinition, InstrumentExecutionTerms, InstrumentId,
    MarketDepth, PriceTicks, QuantityLots, Timestamp,
};
use market_squawk_live::{SnapshotCompleteness, StreamPhaseSnapshot};
use market_squawk_portfolio::PortfolioRevisionToken;
use market_squawk_services::ServiceError;
use market_squawk_sources::MarketFreshness;
use tokio_util::sync::CancellationToken;

use super::{
    MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT, StreamView, build_surface_policies,
    collect_candidate_streams,
    unified::{
        UnifiedInstrumentDefinition, build_candidates, read_selected_market_investment,
        validate_inputs,
    },
};
use crate::live_source::display_market::{DisplayMarketAvailability, DisplayMarketPayload};
use crate::{
    application::{
        market_runtime::{
            MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease, MarketRuntimeRegistry,
        },
        market_selection::{
            DowngradePolicy, FreshnessBasis, FreshnessRequirement, MarketCoverage,
            MarketInvestmentObservation, MarketInvestmentRead, MarketInvestmentUnavailableReason,
            MarketOperation, MarketOperationSet, MarketSelectionPolicy, MarketSelectionReceipt,
            MarketSelectionRequest, ObservationTiming, RequestPriority, SelectedMarketSource,
            select_market_source, selected_generation_matches,
        },
        recommendation::{RecommendationSetupAuthority, RecommendationSetupError},
    },
    portfolio_application::{
        PortfolioAccountCatalogError, PortfolioAccountCatalogReadCapability,
        PortfolioAnalysisDepthLevelsInput, PortfolioAnalysisDepthUnavailableReason,
        PortfolioAnalysisLiquidityEvidence, PortfolioAnalysisMarketAvailability,
        PortfolioAnalysisMarketEntry, PortfolioAnalysisMarketSet,
        PortfolioAnalysisMarketUnavailableReason, PortfolioAnalysisSetupResolution,
        PortfolioAnalysisSetupSnapshot, PortfolioApplicationServiceError,
        PortfolioCandidateAvailability, PortfolioCandidateMarketEvidence,
        PortfolioCandidateResolution, PortfolioCandidateResolutionAuthority,
        PortfolioCandidateUnavailableReason,
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
        let setup = match self
            .resolve_analysis_setup(as_of, deadline, cancellation.clone())
            .await?
        {
            PortfolioAnalysisSetupResolution::Ready(setup) => setup,
            PortfolioAnalysisSetupResolution::SetupRequired { .. } => {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
        };
        let markets = self
            .resolve_analysis_markets(
                &setup,
                &[instrument_id],
                as_of,
                deadline,
                cancellation.clone(),
            )
            .await?;
        let entry = markets
            .entry(instrument_id)
            .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
        let market = match entry.availability() {
            PortfolioAnalysisMarketAvailability::Available { market, .. } => market.clone(),
            PortfolioAnalysisMarketAvailability::Unavailable(_) => {
                return Err(PortfolioApplicationServiceError::Authority);
            }
        };
        let resolution = PortfolioCandidateResolution::try_from_parts(setup, market)?;
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

    async fn resolve_analysis_setup(
        &self,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioAnalysisSetupResolution, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        let catalog = self
            .catalog
            .snapshot_current(deadline, &cancellation)
            .map_err(map_catalog_error)?;
        let resolution = self
            .setup
            .resolve(&catalog, as_of)
            .map_err(map_setup_error)?;
        let resolution =
            PortfolioAnalysisSetupResolution::try_from_resolution(resolution, catalog)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(resolution)
    }

    async fn resolve_analysis_markets(
        &self,
        setup: &PortfolioAnalysisSetupSnapshot,
        instrument_ids: &[InstrumentId],
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PortfolioAnalysisMarketSet, PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if setup.setup().as_of() != as_of
            || instrument_ids.is_empty()
            || instrument_ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.catalog
            .recheck(setup.catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(setup.setup(), setup.catalog(), as_of)
            .map_err(map_setup_error)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(instrument_ids.len())
            .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
        for instrument_id in instrument_ids {
            ensure_before(as_of, deadline, &cancellation)?;
            entries.push(
                self.resolve_analysis_market(
                    *instrument_id,
                    as_of,
                    deadline,
                    &cancellation,
                    setup.setup().current_head().revision().clone(),
                )
                .await?,
            );
        }
        let markets = PortfolioAnalysisMarketSet::try_new(setup.clone(), entries, as_of)?;
        self.catalog
            .recheck(setup.catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(setup.setup(), setup.catalog(), as_of)
            .map_err(map_setup_error)?;
        ensure_before(as_of, deadline, &cancellation)?;
        Ok(markets)
    }

    async fn recheck_analysis_markets(
        &self,
        expected: &PortfolioAnalysisMarketSet,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), PortfolioApplicationServiceError> {
        ensure_before(as_of, deadline, &cancellation)?;
        if expected.evaluated_at() != as_of {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        self.catalog
            .recheck(expected.setup().catalog(), deadline, &cancellation)
            .map_err(map_catalog_error)?;
        self.setup
            .recheck(expected.setup().setup(), expected.setup().catalog(), as_of)
            .map_err(map_setup_error)?;
        let instrument_ids = expected
            .entries()
            .iter()
            .map(PortfolioAnalysisMarketEntry::instrument_id)
            .collect::<Vec<_>>();
        let current = self
            .resolve_analysis_markets(
                expected.setup(),
                &instrument_ids,
                as_of,
                deadline,
                cancellation.clone(),
            )
            .await?;
        if &current != expected {
            return Err(PortfolioApplicationServiceError::StateChanged);
        }
        ensure_before(as_of, deadline, &cancellation)
    }
}

impl ProductionPortfolioCandidateResolutionAuthority {
    async fn resolve_analysis_market(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        portfolio_revision: PortfolioRevisionToken,
    ) -> Result<PortfolioAnalysisMarketEntry, PortfolioApplicationServiceError> {
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

        let definition = match self.load_execution_definition(instrument_id, deadline, cancellation)
        {
            Ok(definition) => definition,
            Err(PortfolioApplicationServiceError::NotFound) => {
                return Ok(PortfolioAnalysisMarketEntry::unavailable(
                    instrument_id,
                    PortfolioAnalysisMarketUnavailableReason::InstrumentDefinitionUnavailable,
                ));
            }
            Err(error) => return Err(error),
        };
        let market_data_definition = if display_snapshots.is_empty() {
            None
        } else {
            let Some(definition) = self
                .market_data_instruments
                .latest(instrument_id, deadline, cancellation)
                .map_err(map_market_data_catalog_error)?
            else {
                return Ok(PortfolioAnalysisMarketEntry::unavailable(
                    instrument_id,
                    PortfolioAnalysisMarketUnavailableReason::InstrumentDefinitionUnavailable,
                ));
            };
            Some(definition.definition().clone())
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
            MarketInvestmentRead::Unavailable(reason) => {
                let reason = match reason {
                    MarketInvestmentUnavailableReason::NoEligibleSource => {
                        PortfolioAnalysisMarketUnavailableReason::NoEligibleSelectedSource
                    }
                    MarketInvestmentUnavailableReason::NoFreshLastTradeOrMidpoint => {
                        PortfolioAnalysisMarketUnavailableReason::NoFreshSelectedMark
                    }
                };
                return Ok(PortfolioAnalysisMarketEntry::unavailable(
                    instrument_id,
                    reason,
                ));
            }
        };
        if observation.mark().fresh_until().is_none() {
            return Ok(PortfolioAnalysisMarketEntry::unavailable(
                instrument_id,
                PortfolioAnalysisMarketUnavailableReason::SourceFreshnessDeadlineUnavailable,
            ));
        }
        let execution_terms = definition_view
            .execution_terms()
            .map_err(map_service_error)?;
        let market = PortfolioCandidateMarketEvidence::try_from_market_selection(
            &receipt,
            observation,
            execution_terms,
            PortfolioCandidateAvailability::Unavailable(PortfolioCandidateUnavailableReason::Fees),
            PortfolioCandidateAvailability::Unavailable(
                PortfolioCandidateUnavailableReason::Slippage,
            ),
            portfolio_revision,
        )?;
        let liquidity = selected_liquidity(
            &market,
            &receipt,
            observation,
            &streams,
            &display_snapshots,
            &kraken_projections,
            execution_terms,
        )?;
        Ok(PortfolioAnalysisMarketEntry::available(
            instrument_id,
            market,
            liquidity,
        ))
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

#[derive(Clone, Copy)]
enum SelectedDepthView<'source> {
    Live(StreamView<'source>),
    Display(&'source MarketDisplaySnapshotLease),
    Kraken(&'source MarketKrakenPriceProjectionLease),
}

#[allow(
    clippy::too_many_arguments,
    reason = "the exact receipt, observation, retained leases, and execution terms stay explicit"
)]
fn selected_liquidity(
    market: &PortfolioCandidateMarketEvidence,
    receipt: &MarketSelectionReceipt,
    observation: MarketInvestmentObservation<'_, '_>,
    streams: &[StreamView<'_>],
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    terms: InstrumentExecutionTerms,
) -> Result<PortfolioAnalysisLiquidityEvidence, PortfolioApplicationServiceError> {
    let selected = receipt
        .selected()
        .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
    if receipt.selection_digest() != observation.selection_digest()
        || market.selection().receipt_digest() != receipt.selection_digest()
        || market.observation().instrument_id() != observation.instrument_id()
        || market.execution_terms() != terms
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let view = exact_selected_depth_view(selected, streams, display_snapshots, kraken_projections)?;
    match view {
        SelectedDepthView::Live(view) => live_selected_liquidity(market, observation, view, terms),
        SelectedDepthView::Display(snapshot) => {
            display_selected_liquidity(market, observation, snapshot, terms)
        }
        SelectedDepthView::Kraken(snapshot) => {
            kraken_selected_liquidity(market, observation, snapshot, terms)
        }
    }
}

fn exact_selected_depth_view<'source>(
    selected: SelectedMarketSource<'_>,
    streams: &[StreamView<'source>],
    display_snapshots: &[&'source MarketDisplaySnapshotLease],
    kraken_projections: &[&'source MarketKrakenPriceProjectionLease],
) -> Result<SelectedDepthView<'source>, PortfolioApplicationServiceError> {
    let identity = selected.candidate().identity();
    let generation = selected.candidate().admission().integrity().generation();
    let mut live = streams.iter().copied().filter(|view| {
        view.surface_id == identity.observation_id()
            && view.metadata.provider() == identity.provider()
            && view.stream.source() == identity.source_id()
            && view.stream.provider_product() == identity.product()
            && view.stream.provider_channel() == identity.feed()
            && Some(view.route.route().venue()) == identity.venue_id()
            && view.route.route().instrument() == identity.instrument_id()
            && selected_generation_matches(selected, view.stream.connection_generation())
    });
    let live = unique_match(live.next(), live.next())?;
    let mut display = display_snapshots.iter().copied().filter(|snapshot| {
        let actor = snapshot.lease();
        let Some(selected_observation) = actor.selection_observation() else {
            return false;
        };
        let provenance = selected_observation.observation().provenance();
        snapshot.metadata().provider() == identity.provider()
            && provenance.coverage().provider_product() == identity.product().as_source_identifier()
            && provenance.coverage().provider_channel() == identity.feed().as_source_identifier()
            && actor.key().source_id() == identity.source_id()
            && Some(actor.key().venue_id()) == identity.venue_id()
            && actor.key().instrument_id() == identity.instrument_id()
            && snapshot.surface_id() == identity.observation_id()
            && Some(actor.key().generation()) == generation
    });
    let display = unique_match(display.next(), display.next())?;
    let mut kraken = kraken_projections.iter().copied().filter(|snapshot| {
        let projection = snapshot.projection();
        let live = snapshot.metadata().coverage().live();
        snapshot.metadata().provider() == identity.provider()
            && live.is_some_and(|live| {
                live.provider_product() == identity.product()
                    && live.provider_channel() == identity.feed()
            })
            && snapshot.key().source_id() == identity.source_id()
            && Some(snapshot.key().venue_id()) == identity.venue_id()
            && snapshot.key().instrument_id() == identity.instrument_id()
            && snapshot.surface_id() == identity.observation_id()
            && Some(snapshot.key().generation()) == generation
            && projection.route().generation() == snapshot.key().generation()
    });
    let kraken = unique_match(kraken.next(), kraken.next())?;
    match (live, display, kraken) {
        (Some(live), None, None) => Ok(SelectedDepthView::Live(live)),
        (None, Some(display), None) => Ok(SelectedDepthView::Display(display)),
        (None, None, Some(kraken)) => Ok(SelectedDepthView::Kraken(kraken)),
        _ => Err(PortfolioApplicationServiceError::CorruptPublication),
    }
}

fn unique_match<T: Copy>(
    first: Option<T>,
    second: Option<T>,
) -> Result<Option<T>, PortfolioApplicationServiceError> {
    if second.is_some() {
        Err(PortfolioApplicationServiceError::CorruptPublication)
    } else {
        Ok(first)
    }
}

fn live_selected_liquidity(
    market: &PortfolioCandidateMarketEvidence,
    observation: MarketInvestmentObservation<'_, '_>,
    view: StreamView<'_>,
    _terms: InstrumentExecutionTerms,
) -> Result<PortfolioAnalysisLiquidityEvidence, PortfolioApplicationServiceError> {
    let stream = view.stream;
    let Some(depth) = observation.depth() else {
        return Ok(PortfolioAnalysisLiquidityEvidence::unavailable(
            market,
            PortfolioAnalysisDepthUnavailableReason::SourceDoesNotSupplyDepth,
        ));
    };
    if depth == MarketDepth::OrderLevel
        || stream.phase() != StreamPhaseSnapshot::Healthy
        || !stream.generation_current()
        || observation.selected_at() > stream.source_valid_until()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let bid = snapshot_depth_input(
        depth,
        stream.bid_dimension().completeness(),
        stream
            .bids()
            .iter()
            .map(|level| (level.price(), level.quantity())),
    );
    let ask = snapshot_depth_input(
        depth,
        stream.ask_dimension().completeness(),
        stream
            .asks()
            .iter()
            .map(|level| (level.price(), level.quantity())),
    );
    let fresh_until = stream
        .source_valid_until()
        .checked_add_nanos(1)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    PortfolioAnalysisLiquidityEvidence::try_from_selected_depth(
        market,
        depth,
        stream.state_revision(),
        stream.source_timestamp().unwrap_or(stream.received_at()),
        view.shard.published_at(),
        fresh_until,
        bid,
        ask,
    )
}

fn snapshot_depth_input(
    depth: MarketDepth,
    completeness: SnapshotCompleteness,
    levels: impl Iterator<Item = (PriceTicks, QuantityLots)>,
) -> PortfolioAnalysisDepthLevelsInput {
    match (depth, completeness) {
        (MarketDepth::TopOfBook, SnapshotCompleteness::Complete) => {
            PortfolioAnalysisDepthLevelsInput::Available(levels.take(1).collect())
        }
        (MarketDepth::TopOfBook, SnapshotCompleteness::Truncated) => {
            PortfolioAnalysisDepthLevelsInput::Unavailable(
                PortfolioAnalysisDepthUnavailableReason::SideIncomplete,
            )
        }
        (MarketDepth::TopOfBook, SnapshotCompleteness::Unavailable) => {
            PortfolioAnalysisDepthLevelsInput::Unavailable(
                PortfolioAnalysisDepthUnavailableReason::SideUnavailable,
            )
        }
        (MarketDepth::PriceLevel, SnapshotCompleteness::Complete) => {
            PortfolioAnalysisDepthLevelsInput::Available(levels.collect())
        }
        (MarketDepth::PriceLevel, SnapshotCompleteness::Truncated) => {
            PortfolioAnalysisDepthLevelsInput::Unavailable(
                PortfolioAnalysisDepthUnavailableReason::SideIncomplete,
            )
        }
        (MarketDepth::PriceLevel, SnapshotCompleteness::Unavailable) => {
            PortfolioAnalysisDepthLevelsInput::Unavailable(
                PortfolioAnalysisDepthUnavailableReason::SideUnavailable,
            )
        }
        (MarketDepth::OrderLevel, _) => PortfolioAnalysisDepthLevelsInput::Unavailable(
            PortfolioAnalysisDepthUnavailableReason::SourceDoesNotSupplyDepth,
        ),
    }
}

fn display_selected_liquidity(
    market: &PortfolioCandidateMarketEvidence,
    observation: MarketInvestmentObservation<'_, '_>,
    snapshot: &MarketDisplaySnapshotLease,
    terms: InstrumentExecutionTerms,
) -> Result<PortfolioAnalysisLiquidityEvidence, PortfolioApplicationServiceError> {
    let actor = snapshot.lease();
    let Some(quote) = actor.quote() else {
        return Ok(PortfolioAnalysisLiquidityEvidence::unavailable(
            market,
            PortfolioAnalysisDepthUnavailableReason::SourceDoesNotSupplyDepth,
        ));
    };
    let (stale_after, _expires_after) = match quote.availability() {
        DisplayMarketAvailability::Fresh {
            stale_after,
            expires_after,
        } if observation.selected_at() <= stale_after && stale_after <= expires_after => {
            (stale_after, expires_after)
        }
        DisplayMarketAvailability::Fresh { .. }
        | DisplayMarketAvailability::Stale { .. }
        | DisplayMarketAvailability::Expired { .. }
        | DisplayMarketAvailability::Quarantined { .. } => {
            return Ok(PortfolioAnalysisLiquidityEvidence::unavailable(
                market,
                PortfolioAnalysisDepthUnavailableReason::SourceFreshnessDeadlineUnavailable,
            ));
        }
    };
    let provenance = quote.observation().provenance();
    if provenance.generation() != actor.key().generation()
        || provenance.available_at() > observation.selected_at()
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    let DisplayMarketPayload::Quote(quote) = quote.observation().payload() else {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    };
    let bid = display_side_input(quote.bid(), terms)?;
    let ask = display_side_input(quote.ask(), terms)?;
    let fresh_until = stale_after
        .checked_add_nanos(1)
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    PortfolioAnalysisLiquidityEvidence::try_from_selected_depth(
        market,
        MarketDepth::TopOfBook,
        actor.revision(),
        provenance.effective_at(),
        provenance.available_at(),
        fresh_until,
        bid,
        ask,
    )
}

fn display_side_input(
    side: Option<&crate::live_source::display_market::DisplayQuoteSide>,
    terms: InstrumentExecutionTerms,
) -> Result<PortfolioAnalysisDepthLevelsInput, PortfolioApplicationServiceError> {
    let Some(side) = side else {
        return Ok(PortfolioAnalysisDepthLevelsInput::Unavailable(
            PortfolioAnalysisDepthUnavailableReason::SideUnavailable,
        ));
    };
    let price = PriceTicks::try_from_decimal(side.price().value(), terms.price_tick())
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    let quantity = QuantityLots::try_from_decimal(side.quantity().value(), terms.lot_size())
        .map_err(|_| PortfolioApplicationServiceError::Analytics)?;
    Ok(PortfolioAnalysisDepthLevelsInput::Available(vec![(
        price, quantity,
    )]))
}

fn kraken_selected_liquidity(
    market: &PortfolioCandidateMarketEvidence,
    observation: MarketInvestmentObservation<'_, '_>,
    snapshot: &MarketKrakenPriceProjectionLease,
    _terms: InstrumentExecutionTerms,
) -> Result<PortfolioAnalysisLiquidityEvidence, PortfolioApplicationServiceError> {
    let projection = snapshot.projection();
    if projection.available_at() > observation.selected_at()
        || !matches!(projection.freshness(), MarketFreshness::Fresh { .. })
    {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    }
    // The Kraken projection currently retains freshness classification but no exact deadline.
    // Deadline-consuming analysis must not derive one from a local policy age.
    Ok(PortfolioAnalysisLiquidityEvidence::unavailable(
        market,
        PortfolioAnalysisDepthUnavailableReason::SourceFreshnessDeadlineUnavailable,
    ))
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
