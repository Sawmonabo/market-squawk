//! Bounded current-state Market domain over the paper runtime's live owner.

mod candidate;
mod durable_product;
mod history;
mod product;
mod results;
mod serialization;
mod unified;

use std::{cmp::Ordering, fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use market_squawk_data::{
    InstrumentDefinitionReadCapability, MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS,
    MarketDataInstrumentPopulationDisposition, MarketDataInstrumentPopulationQuery,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
    ProviderMarketEventSelectedCandidate, ProviderMarketEventSelectionCompleteness,
};
use market_squawk_domain::{
    AssetClass, CoverageDelay, DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentDefinition,
    InstrumentId, LiveEventClass, MarketDepth, MarketEvent, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};
use market_squawk_live::{
    RouteSnapshot, ShardSnapshot, SnapshotCompleteness, SnapshotDimension, StreamSnapshot,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use market_squawk_sources::SourceMetadata;
use serde_json::Value;

use super::ensure_live;
use crate::application::market_runtime::{
    MarketDisplaySnapshotBatch, MarketDisplaySnapshotLease, MarketEventDurableRouteRead,
    MarketKrakenPriceProjectionLease, MarketOrderLevelSnapshot, MarketRuntimeRegistry,
    MarketRuntimeSnapshotBatch,
};
use crate::application::market_selection::{MarketOperation, MarketOperationSet};
use crate::application::research::MarketEventPointInTimeReceipt;
use crate::application::research::MarketHistoryReadCapability;
use crate::application::{ApplicationDomainService, effective_service_limits};
use crate::live_source::display_market::DisplayMarketReadTime;
pub(super) use candidate::ProductionPortfolioCandidateResolutionFactory;
use results::{
    build_book_result, build_comparison_result, build_quality_result, build_quote_result,
    build_snapshot_result, build_trade_result,
};
use serialization::{source_coverage_value, timestamp_value};
use unified::{
    MarketSurfaceRightsPolicy, MarketSurfaceSelectionPolicy, build_market_overview_result,
    build_unified_market_result, market_event_provenance,
};

const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_TRADES: &str = "Market.GetTrades";
const MARKET_GET_QUOTES: &str = "Market.GetQuotes";
const MARKET_GET_BOOKS: &str = "Market.GetBooks";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const MARKET_GET_COMPARISONS: &str = "Market.GetComparisons";
const MARKET_GET_UNIFIED_FEED: &str = "Market.GetUnifiedFeed";
const MARKET_GET_OVERVIEW: &str = "Market.GetOverview";
const MARKET_GET_INSTRUMENT: &str = "Market.GetInstrument";
const MARKET_GET_HISTORY: &str = "Market.GetHistory";
const MARKET_SEARCH_UNIVERSE: &str = "Market.SearchUniverse";
const MAXIMUM_UNIFIED_MARKET_INSTRUMENTS: usize = 4_096;
const MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT: usize = 256;
const MAXIMUM_UNIFIED_ORDER_SAMPLE: usize = 64;
const MAXIMUM_DURABLE_EVENT_CANDIDATES: usize = 32;
const DURABLE_CURRENT_EVENT_KINDS: [LiveEventClass; 4] = [
    LiveEventClass::Trade,
    LiveEventClass::Quote,
    LiveEventClass::BookSnapshot,
    LiveEventClass::BookDelta,
];

/// Provider-neutral proof that one current runtime coordinate also exists in the immutable store.
///
/// Source identity remains internal because it is required to prevent cross-source substitution.
/// Ordinary product results receive only the selected canonical market state.
#[derive(Debug, Default)]
struct DurableMarketEvidenceSet {
    bound_sources: Vec<SourceId>,
    routes: Vec<DurableMarketRouteEvidence>,
    expected_route_count: usize,
}

impl DurableMarketEvidenceSet {
    fn try_new(
        mut bound_sources: Vec<SourceId>,
        routes: Vec<DurableMarketRouteEvidence>,
        expected_route_count: usize,
    ) -> Result<Self, ServiceError> {
        bound_sources.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        bound_sources.dedup();
        if routes.len() > expected_route_count
            || routes.iter().any(|route| {
                bound_sources
                    .binary_search_by(|source| source.as_str().cmp(route.source_id.as_str()))
                    .is_err()
            })
            || routes.iter().enumerate().any(|(index, route)| {
                routes.iter().skip(index + 1).any(|candidate| {
                    candidate.source_id == route.source_id
                        && candidate.instrument_id == route.instrument_id
                        && candidate.venue_id == route.venue_id
                })
            })
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            bound_sources,
            routes,
            expected_route_count,
        })
    }

    fn source_requires_durable_evidence(&self, source_id: &SourceId) -> bool {
        self.bound_sources
            .binary_search_by(|source| source.as_str().cmp(source_id.as_str()))
            .is_ok()
    }

    fn complete_for(&self, streams: &[StreamView<'_>]) -> bool {
        self.routes.len() == self.expected_route_count
            && streams.iter().all(|view| {
                !self.source_requires_durable_evidence(view.stream.source())
                    || self.route_for(view).is_some()
            })
    }

    fn route_for(&self, view: &StreamView<'_>) -> Option<&DurableMarketRouteEvidence> {
        self.routes.iter().find(|route| {
            &route.source_id == view.stream.source()
                && route.instrument_id == view.route.route().instrument()
                && &route.venue_id == view.route.route().venue()
        })
    }
}

#[derive(Debug)]
struct DurableMarketRouteEvidence {
    surface_id: SourceIdentifier,
    metadata: SourceMetadata,
    source_id: SourceId,
    instrument_id: InstrumentId,
    venue_id: VenueId,
    selections: Vec<MarketEventPointInTimeReceipt>,
    trade_status: TradeStatus,
    // Only retained Display assembly may combine independently acquired event families.
    // Each revision remains the original acquisition authority, never a renewal of its clocks.
    retained_metadata: Option<Vec<SourceMetadata>>,
    display_authorizations: Vec<Arc<market_squawk_data::AuthorizedMarketEventUse>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TradeStatus {
    Available,
    Ambiguous,
    Unavailable,
}

impl TradeStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Ambiguous => "ambiguous",
            Self::Unavailable => "unavailable",
        }
    }
}

/// A source/receive-time tie can contain distinct trades without establishing a last trade.
/// Conflicting evidence for the same native event remains invalid, as do non-trade ties.
fn retained_event_status<'event>(
    event_kind: LiveEventClass,
    events: impl Iterator<Item = (&'event MarketEvent, EvidenceDigest)> + Clone,
) -> Result<TradeStatus, ServiceError> {
    let Some((first, _)) = events.clone().next() else {
        return Ok(TradeStatus::Unavailable);
    };
    let first_provenance = market_event_provenance(first);
    let mut status = TradeStatus::Available;
    for (event, digest) in events.clone() {
        let provenance = market_event_provenance(event);
        if market_event_class(event) != event_kind
            || !same_durable_cohort(provenance.binding(), first_provenance.binding())
            || provenance.source_timestamp() != first_provenance.source_timestamp()
            || provenance.received_at() != first_provenance.received_at()
        {
            return Err(ServiceError::InvalidResult);
        }
        for (other, other_digest) in events.clone() {
            let event_id = |event: &'event MarketEvent| match event {
                MarketEvent::MarketDataTrade(trade) => trade.provider_trade_id(),
                _ => market_event_provenance(event).source_identifier(),
            };
            if event_id(event) == event_id(other) || event_kind != LiveEventClass::Trade {
                if digest != other_digest || event != other {
                    return Err(ServiceError::InvalidResult);
                }
            } else {
                status = TradeStatus::Ambiguous;
            }
        }
    }
    Ok(status)
}

impl DurableMarketRouteEvidence {
    fn try_new(
        surface_id: SourceIdentifier,
        metadata: SourceMetadata,
        source_id: SourceId,
        instrument_id: InstrumentId,
        venue_id: VenueId,
        mut selections: Vec<MarketEventPointInTimeReceipt>,
        retained_metadata: Option<Vec<SourceMetadata>>,
    ) -> Result<Option<Self>, ServiceError> {
        let invalid = |stage: &'static str| {
            tracing::warn!(
                %source_id,
                %instrument_id,
                %venue_id,
                stage,
                "durable market evidence rejected"
            );
            ServiceError::InvalidResult
        };
        let mut seen_event_kinds = Vec::new();
        seen_event_kinds
            .try_reserve_exact(selections.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for receipt in &selections {
            let selection = receipt.selection();
            let request = selection.request();
            if receipt.source_surface() != &source_id
                || metadata.source_id() != &source_id
                || request.instrument_id() != Some(instrument_id)
                || request.venue_id() != &venue_id
                || selection.completeness() != ProviderMarketEventSelectionCompleteness::Complete
                || seen_event_kinds.contains(&request.event_kind())
            {
                return Err(invalid("selection_identity_or_completeness"));
            }
            seen_event_kinds.push(request.event_kind());
            // A complete empty selection proves this event family has no eligible evidence.
            // Validate its route identity and uniqueness before omitting it from presentation.
            if selection.sources().is_empty() {
                continue;
            }
            let source = selection
                .sources()
                .first()
                .ok_or_else(|| invalid("selected_source_missing"))?;
            if selection.sources().len() != 1 || source.source_surface() != &source_id {
                return Err(invalid("ambiguous_source"));
            }
            if source.tied_candidates().is_empty() {
                return Err(invalid("selected_candidate_missing"));
            }
            for candidate in source.tied_candidates() {
                let coordinate = candidate.coordinate();
                let provenance = market_event_provenance(candidate.event());
                if coordinate.source_surface() != &source_id
                    || coordinate.instrument_id() != Some(instrument_id)
                    || coordinate.venue_id() != &venue_id
                    || coordinate.event_kind() != request.event_kind()
                    || market_event_class(candidate.event()) != request.event_kind()
                    || provenance.source_id() != &source_id
                    || provenance.instrument_id() != Some(instrument_id)
                    || provenance.venue_id() != Some(&venue_id)
                    || provenance.source_identifier() != coordinate.provider_event_id()
                    || provenance.source_timestamp() != coordinate.source_timestamp()
                    || provenance.received_at() != coordinate.received_at()
                    || provenance.available_at() != coordinate.available_at()
                    || provenance.ingested_at() != coordinate.ingested_at()
                    || provenance.connection_generation().get()
                        != coordinate.connection_generation()
                {
                    return Err(invalid("candidate_identity_or_provenance"));
                }
                if let Some(retained_metadata) = &retained_metadata {
                    let original = retained_metadata
                        .iter()
                        .find(|metadata| {
                            metadata.revision() == provenance.binding().metadata_revision()
                        })
                        .ok_or_else(|| invalid("component_metadata_missing"))?;
                    let live = original
                        .coverage()
                        .live()
                        .ok_or_else(|| invalid("component_live_coverage_missing"))?;
                    if original.source_id() != &source_id
                        || original.provider() != metadata.provider()
                        || original.authorization().basis()
                            != provenance.binding().authorization_basis()
                        || live.provider_product() != provenance.binding().provider_product()
                        || live.provider_channel() != provenance.binding().provider_channel()
                        || !original.is_effective_at(provenance.received_at())
                    {
                        return Err(invalid("component_acquisition_authority"));
                    }
                }
            }
            retained_event_status(
                request.event_kind(),
                source.tied_candidates().iter().map(|candidate| {
                    (
                        candidate.event(),
                        candidate.coordinate().canonical_event_digest(),
                    )
                }),
            )
            .map_err(|_| invalid("conflicting_tied_evidence"))?;
        }
        selections.retain(|receipt| !receipt.selection().sources().is_empty());
        let mut selected_cohort: Option<(
            (Timestamp, Timestamp, Timestamp, u64, Timestamp),
            market_squawk_domain::LiveEvidenceBinding,
        )> = None;
        for candidate in selections
            .iter()
            .flat_map(|receipt| receipt.selection().sources())
            .flat_map(|source| source.tied_candidates())
        {
            let key = durable_cohort_recency_key(candidate);
            let binding = market_event_provenance(candidate.event()).binding();
            match selected_cohort.as_ref() {
                None => selected_cohort = Some((key, binding.clone())),
                Some((selected_key, _selected_binding)) if key > *selected_key => {
                    selected_cohort = Some((key, binding.clone()));
                }
                Some((selected_key, selected_binding))
                    if key == *selected_key && !same_durable_cohort(binding, selected_binding) =>
                {
                    return Err(invalid("ambiguous_cohort"));
                }
                Some(_) => {}
            }
        }
        let Some((_cohort_key, cohort_binding)) = selected_cohort else {
            return Ok(None);
        };
        let live = metadata
            .coverage()
            .live()
            .ok_or_else(|| invalid("live_coverage_missing"))?;
        for (stage, matches) in [
            ("cohort_source", cohort_binding.source_id() == &source_id),
            (
                "cohort_instrument",
                cohort_binding.instrument_id() == Some(instrument_id),
            ),
            ("cohort_venue", cohort_binding.venue_id() == &venue_id),
            (
                "cohort_metadata_revision",
                cohort_binding.metadata_revision() == metadata.revision(),
            ),
            (
                "cohort_product",
                cohort_binding.provider_product() == live.provider_product(),
            ),
            (
                "cohort_channel",
                cohort_binding.provider_channel() == live.provider_channel(),
            ),
        ] {
            if !matches {
                return Err(invalid(stage));
            }
        }
        selections.retain(|receipt| {
            (retained_metadata.is_some()
                && matches!(
                    receipt.selection().request().event_kind(),
                    LiveEventClass::Quote | LiveEventClass::Trade
                ))
                || receipt.selection().sources()[0]
                    .tied_candidates()
                    .iter()
                    .all(|candidate| {
                        same_durable_cohort(
                            market_event_provenance(candidate.event()).binding(),
                            &cohort_binding,
                        )
                    })
        });
        let trade_status = selections
            .iter()
            .find(|receipt| receipt.selection().request().event_kind() == LiveEventClass::Trade)
            .map(|receipt| {
                retained_event_status(
                    LiveEventClass::Trade,
                    receipt.selection().sources()[0]
                        .tied_candidates()
                        .iter()
                        .map(|candidate| {
                            (
                                candidate.event(),
                                candidate.coordinate().canonical_event_digest(),
                            )
                        }),
                )
            })
            .transpose()?
            .unwrap_or(TradeStatus::Unavailable);
        let route = Self {
            surface_id,
            metadata,
            source_id,
            instrument_id,
            venue_id,
            selections,
            trade_status,
            retained_metadata,
            display_authorizations: Vec::new(),
        };
        if route.evidence_candidate().is_some() {
            Ok(Some(route))
        } else {
            Ok(None)
        }
    }

    fn candidate(
        &self,
        event_kind: LiveEventClass,
    ) -> Option<&ProviderMarketEventSelectedCandidate> {
        if event_kind == LiveEventClass::Trade && self.trade_status == TradeStatus::Ambiguous {
            return None;
        }
        self.selections
            .iter()
            .find(|receipt| receipt.selection().request().event_kind() == event_kind)
            .map(|receipt| &receipt.selection().sources()[0].tied_candidates()[0])
    }

    fn presentation_candidate(&self) -> Option<&ProviderMarketEventSelectedCandidate> {
        self.candidate(LiveEventClass::Trade)
            .into_iter()
            .chain(self.candidate(LiveEventClass::Quote))
            .chain(self.safe_book_snapshot_candidate())
            .max_by_key(|candidate| durable_candidate_effective_at(candidate))
    }

    fn primary_effective_at(&self) -> Option<Timestamp> {
        self.evidence_candidate()
            .map(durable_candidate_effective_at)
    }

    /// Supplies route identity and clocks only, never an arbitrary last-trade price or size.
    fn evidence_candidate(&self) -> Option<&ProviderMarketEventSelectedCandidate> {
        self.presentation_candidate().or_else(|| {
            self.selections
                .iter()
                .find(|receipt| receipt.selection().request().event_kind() == LiveEventClass::Trade)
                .and_then(|receipt| {
                    receipt.selection().sources()[0]
                        .tied_candidates()
                        .iter()
                        .max_by_key(|candidate| durable_cohort_recency_key(candidate))
                })
        })
    }

    fn event(&self, event_kind: LiveEventClass) -> Option<&MarketEvent> {
        self.candidate(event_kind)
            .map(ProviderMarketEventSelectedCandidate::event)
    }

    fn safe_book_snapshot_candidate(&self) -> Option<&ProviderMarketEventSelectedCandidate> {
        let snapshot = self.candidate(LiveEventClass::BookSnapshot)?;
        let Some(delta) = self.candidate(LiveEventClass::BookDelta) else {
            return Some(snapshot);
        };
        if matches!(
            (
                snapshot.coordinate().source_sequence(),
                delta.coordinate().source_sequence(),
            ),
            (Some(snapshot_sequence), Some(delta_sequence))
                if snapshot_sequence >= delta_sequence
        ) {
            Some(snapshot)
        } else {
            None
        }
    }

    fn best_quote_candidate(&self) -> Option<&ProviderMarketEventSelectedCandidate> {
        self.candidate(LiveEventClass::Quote)
            .into_iter()
            .chain(self.safe_book_snapshot_candidate())
            .max_by_key(|candidate| durable_candidate_effective_at(candidate))
    }

    fn display_fresh_until(
        &self,
        candidate: &ProviderMarketEventSelectedCandidate,
    ) -> Option<Timestamp> {
        let authorization = self.display_authorization(candidate)?;
        let provenance = market_event_provenance(candidate.event());
        if matches!(
            provenance.recorded_quality(),
            DataQuality::Modeled
                | DataQuality::Estimated
                | DataQuality::Stale
                | DataQuality::Quarantined
        ) {
            return None;
        }
        let metadata = self.component_metadata(candidate)?;
        let policy = metadata.freshness_policy();
        let mut until = provenance
            .source_timestamp()?
            .checked_add_nanos(i64::try_from(policy.max_source_age_nanos()).ok()?)
            .ok()?
            .min(
                provenance
                    .received_at()
                    .checked_add_nanos(i64::try_from(policy.max_market_age_nanos()).ok()?)
                    .ok()?,
            );
        for deadline in [
            metadata.authorization().inclusive_authorization_deadline(),
            metadata.coverage().inclusive_coverage_deadline(),
        ]
        .into_iter()
        .flatten()
        {
            until = until.min(deadline);
        }
        until = until.min(authorization.expires_at().checked_sub_nanos(1).ok()?);
        (candidate.coordinate().origin_committed_at() <= until).then_some(until)
    }

    fn component_metadata(
        &self,
        candidate: &ProviderMarketEventSelectedCandidate,
    ) -> Option<&SourceMetadata> {
        let revision = market_event_provenance(candidate.event())
            .binding()
            .metadata_revision();
        match &self.retained_metadata {
            Some(metadata) => metadata
                .iter()
                .find(|metadata| metadata.revision() == revision),
            None => (self.metadata.revision() == revision).then_some(&self.metadata),
        }
    }

    fn display_authorization(
        &self,
        candidate: &ProviderMarketEventSelectedCandidate,
    ) -> Option<&market_squawk_data::AuthorizedMarketEventUse> {
        let receipt = self.selections.iter().find(|receipt| {
            receipt.selection().request().event_kind() == candidate.coordinate().event_kind()
        })?;
        let coordinate = candidate.coordinate();
        self.display_authorizations
            .iter()
            .find(|authorization| {
                authorization.research_use() == market_squawk_data::ResearchUse::Display
                    && authorization.admits_event(
                        receipt.selection().commit(),
                        coordinate.publication().digest(),
                        coordinate.publication_row_ordinal(),
                        coordinate.canonical_event_digest(),
                    )
            })
            .map(Arc::as_ref)
    }

    /// Saved presentation is governed by current exact-row Display permits. Original live
    /// authorization still bounds freshness, but its expiry does not revoke retained use.
    fn display_rights(
        &self,
        operations: MarketOperationSet,
        reference_at: Timestamp,
    ) -> Result<MarketSurfaceRightsPolicy, ServiceError> {
        if self.retained_metadata.is_none() {
            return surface_rights(&self.metadata, operations, reference_at);
        }
        if operations != presentation_surface_operations()? {
            return Err(ServiceError::Unauthorized);
        }
        let mut decided_at = None;
        let mut expires_at = None;
        for candidate in self
            .selections
            .iter()
            .flat_map(|receipt| receipt.selection().sources())
            .flat_map(|source| source.tied_candidates())
        {
            let authorization = self
                .display_authorization(candidate)
                .ok_or(ServiceError::Unauthorized)?;
            decided_at = Some(
                decided_at.map_or(authorization.evaluated_at(), |at: Timestamp| {
                    at.max(authorization.evaluated_at())
                }),
            );
            expires_at = minimum_optional_timestamp(expires_at, Some(authorization.expires_at()));
        }
        let decided_at = decided_at.ok_or(ServiceError::Unauthorized)?;
        let expires_at = expires_at.ok_or(ServiceError::Unauthorized)?;
        if reference_at < decided_at || reference_at >= expires_at {
            return Err(ServiceError::Unauthorized);
        }
        MarketSurfaceRightsPolicy::try_admitted(
            self.metadata.revision().as_source_identifier().clone(),
            operations,
            decided_at,
            decided_at,
            Some(
                expires_at
                    .checked_sub_nanos(1)
                    .map_err(|_| ServiceError::InvalidResult)?,
            ),
        )
        .map_err(|_| ServiceError::InvalidResult)
    }

    fn display_current_through(&self) -> Option<Timestamp> {
        self.candidate(LiveEventClass::Trade)
            .into_iter()
            .chain(self.best_quote_candidate())
            .filter_map(|candidate| self.display_fresh_until(candidate))
            .max()
    }
}

fn durable_candidate_effective_at(candidate: &ProviderMarketEventSelectedCandidate) -> Timestamp {
    candidate
        .coordinate()
        .source_timestamp()
        .unwrap_or(candidate.coordinate().received_at())
}

fn durable_cohort_recency_key(
    candidate: &ProviderMarketEventSelectedCandidate,
) -> (Timestamp, Timestamp, Timestamp, u64, Timestamp) {
    (
        candidate.coordinate().origin_committed_at(),
        candidate.coordinate().available_at(),
        candidate.coordinate().received_at(),
        candidate.coordinate().connection_generation(),
        durable_candidate_effective_at(candidate),
    )
}

fn same_durable_cohort(
    left: &market_squawk_domain::LiveEvidenceBinding,
    right: &market_squawk_domain::LiveEvidenceBinding,
) -> bool {
    left.source_id() == right.source_id()
        && left.session_id() == right.session_id()
        && left.metadata_revision() == right.metadata_revision()
        && left.authorization_basis() == right.authorization_basis()
        && left.venue_id() == right.venue_id()
        && left.instrument_id() == right.instrument_id()
        && left.connection_generation() == right.connection_generation()
        && left.provider_product() == right.provider_product()
        && left.provider_channel() == right.provider_channel()
}

const fn market_event_class(event: &MarketEvent) -> LiveEventClass {
    match event {
        MarketEvent::Trade(_) | MarketEvent::MarketDataTrade(_) => LiveEventClass::Trade,
        MarketEvent::Quote(_) | MarketEvent::MarketDataQuote(_) => LiveEventClass::Quote,
        MarketEvent::BookSnapshot(_) | MarketEvent::MarketDataBook(_) => {
            LiveEventClass::BookSnapshot
        }
        MarketEvent::MarketDataChart(_) => LiveEventClass::Chart,
        MarketEvent::MarketDataScreener(_) => LiveEventClass::Screener,
        MarketEvent::BookDelta(_) => LiveEventClass::BookDelta,
        MarketEvent::Auction(_) => LiveEventClass::Auction,
        MarketEvent::TradingHalt(_) => LiveEventClass::TradingHalt,
        MarketEvent::InstrumentStatus(_) => LiveEventClass::InstrumentStatus,
        MarketEvent::CorporateAction(_) => LiveEventClass::CorporateAction,
    }
}

/// Why one official reference record matched the user's bounded search.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketReferenceMatchKind {
    DefaultOverview,
    ExactSymbol,
    SymbolPrefix,
    SymbolContains,
    SecurityNamePrefix,
    SecurityNameContains,
}

impl MarketReferenceMatchKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::DefaultOverview => "default_overview",
            Self::ExactSymbol => "exact_symbol",
            Self::SymbolPrefix => "symbol_prefix",
            Self::SymbolContains => "symbol_contains",
            Self::SecurityNamePrefix => "security_name_prefix",
            Self::SecurityNameContains => "security_name_contains",
        }
    }
}

/// One non-tradable current-directory identity with exact provider provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketReferenceRecord {
    reference_id: SourceIdentifier,
    symbol: String,
    security_name: String,
    venue_id: VenueId,
    asset_class: AssetClass,
    is_etf: bool,
    round_lot_size: u32,
    quality: DataQuality,
    effective_at: Timestamp,
    available_at: Timestamp,
    source_id: SourceId,
    provider_id: SourceIdentifier,
    source_payload_digest: EvidenceDigest,
    match_kind: MarketReferenceMatchKind,
}

impl MarketReferenceRecord {
    #[allow(
        clippy::too_many_arguments,
        reason = "reference identity, classification, time, source, and evidence remain explicit"
    )]
    pub(crate) fn try_new(
        reference_id: SourceIdentifier,
        symbol: String,
        security_name: String,
        venue_id: VenueId,
        asset_class: AssetClass,
        is_etf: bool,
        round_lot_size: u32,
        quality: DataQuality,
        effective_at: Timestamp,
        available_at: Timestamp,
        source_id: SourceId,
        provider_id: SourceIdentifier,
        source_payload_digest: EvidenceDigest,
        match_kind: MarketReferenceMatchKind,
    ) -> Result<Self, ServiceError> {
        if symbol.is_empty()
            || symbol.len() > 64
            || security_name.trim().is_empty()
            || security_name.len() > 512
            || !matches!(asset_class, AssetClass::Equity | AssetClass::Fund)
            || effective_at > available_at
            || source_payload_digest.bytes() == [0; 32]
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            reference_id,
            symbol,
            security_name,
            venue_id,
            asset_class,
            is_etf,
            round_lot_size,
            quality,
            effective_at,
            available_at,
            source_id,
            provider_id,
            source_payload_digest,
            match_kind,
        })
    }

    pub(crate) fn with_match_kind(mut self, match_kind: MarketReferenceMatchKind) -> Self {
        self.match_kind = match_kind;
        self
    }
}

/// One bounded current reference-universe page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketReferenceSearchPage {
    records: Box<[MarketReferenceRecord]>,
    available: usize,
    has_more: bool,
}

impl MarketReferenceSearchPage {
    pub(crate) fn try_new(
        records: Vec<MarketReferenceRecord>,
        available: usize,
        has_more: bool,
    ) -> Result<Self, ServiceError> {
        if records.len() > available || has_more != (available > records.len()) {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            records: records.into_boxed_slice(),
            available,
            has_more,
        })
    }
}

/// Session-owned, non-persistent reference lookup shared by every Market presentation.
#[async_trait]
pub(crate) trait MarketReferenceSearchAuthority: fmt::Debug + Send + Sync + 'static {
    async fn search(
        &self,
        query: &str,
        maximum_rows: usize,
        deadline: Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<MarketReferenceSearchPage, ServiceError>;

    fn begin_shutdown(&self);

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError>;
}

/// Current-state Market service over every healthy provider runtime.
pub(super) struct MarketDomainService {
    registry: Arc<MarketRuntimeRegistry>,
    instrument_definitions: InstrumentDefinitionReadCapability,
    market_data_instruments: MarketDataInstrumentReadCapability,
    reference_search: Arc<dyn MarketReferenceSearchAuthority>,
    market_history: MarketHistoryReadCapability,
    market_collection: Arc<crate::application::market_collection::MarketCollectionAuthority>,
    product_research: Arc<crate::ResearchService>,
}

impl MarketDomainService {
    pub(super) fn new(
        registry: Arc<MarketRuntimeRegistry>,
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
        reference_search: Arc<dyn MarketReferenceSearchAuthority>,
        market_history: MarketHistoryReadCapability,
        market_collection: Arc<crate::application::market_collection::MarketCollectionAuthority>,
        product_research: Arc<crate::ResearchService>,
    ) -> Self {
        Self {
            registry,
            instrument_definitions,
            market_data_instruments,
            reference_search,
            market_history,
            market_collection,
            product_research,
        }
    }
}

impl fmt::Debug for MarketDomainService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MarketDomainService")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ApplicationDomainService for MarketDomainService {
    fn domain(&self) -> ServiceDomain {
        ServiceDomain::Market
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(&context)?;
        if request.arguments().contains_key("dataset") {
            // No historical authority is injected into this current-state service.
            return Err(ServiceError::Unavailable);
        }
        if request.name() == "Market.SetCollectionChoice" {
            let revision = request
                .arguments()
                .get("expectedRevision")
                .and_then(Value::as_str)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(ServiceError::InvalidRequest)?;
            let symbol = request
                .arguments()
                .get("symbol")
                .and_then(Value::as_str)
                .ok_or(ServiceError::InvalidRequest)?;
            let kept = request
                .arguments()
                .get("kept")
                .and_then(Value::as_bool)
                .ok_or(ServiceError::InvalidRequest)?;
            let snapshot = self.market_collection.set_choice(revision, symbol, kept)
                .map_err(|error| match error {
                    crate::application::market_collection::MarketCollectionError::UnknownSymbol
                    | crate::application::market_collection::MarketCollectionError::StaleRevision => ServiceError::InvalidRequest,
                    _ => ServiceError::Unavailable,
                })?;
            return TypedToolResult::try_new(
                serde_json::json!({"revision": snapshot.revision.to_string(), "choices": snapshot.choices}),
                snapshot.choices.len(), ToolResultMetadata::complete_not_applicable(), context.limits(),
            ).map_err(|_| ServiceError::ResourceExhausted);
        }
        let reference_at = system_timestamp()?;
        if matches!(
            request.name(),
            MARKET_GET_OVERVIEW
                | "Market.GetCollection"
                | MARKET_GET_INSTRUMENT
                | MARKET_GET_HISTORY
                | MARKET_SEARCH_UNIVERSE
        ) {
            return self
                .call_product(&request, reference_at, context.limits(), &context)
                .await;
        }
        let limits = effective_service_limits(&request, &context)?;
        let filters = MarketFilters::parse(&request)?;
        let snapshots = self
            .registry
            .snapshots(context.deadline(), context.cancellation())
            .await?;
        let streams = collect_streams(&snapshots, &filters, &context)?;

        let source_coverage =
            source_coverage_value(&streams, snapshots.failures(), &filters, &[], &[]);
        let output = match request.name() {
            MARKET_GET_SNAPSHOT => build_snapshot_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_TRADES => build_trade_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_QUOTES => build_quote_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_BOOKS => build_book_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_QUALITY => build_quality_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_COMPARISONS => build_comparison_result(
                &streams,
                &filters,
                reference_at,
                source_coverage,
                limits,
                &context,
            ),
            MARKET_GET_UNIFIED_FEED => {
                let durable_market = DurableMarketEvidenceSet::default();
                let display_instrument_ids =
                    load_display_instrument_ids(self.registry.as_ref(), &filters, &context).await?;
                let market_instrument_ids =
                    load_market_instrument_ids(self.registry.as_ref(), &filters, &context).await?;
                let kraken_price_projections = load_kraken_price_projections(
                    self.registry.as_ref(),
                    &market_instrument_ids,
                    &filters,
                    &context,
                )
                .await?;
                let kraken_projection_refs = kraken_projection_refs(&kraken_price_projections)?;
                let definitions = load_instrument_definitions(
                    &self.instrument_definitions,
                    &streams,
                    &kraken_price_projections,
                    &durable_market,
                    &context,
                )?;
                let order_level = load_order_level_snapshots(
                    self.registry.as_ref(),
                    &streams,
                    &kraken_price_projections,
                    &context,
                )
                .await?;
                let display_batches = load_display_snapshots(
                    self.registry.as_ref(),
                    &display_instrument_ids,
                    DisplayMarketReadTime::LatestDisplay,
                    &context,
                )
                .await?;
                let display_snapshots = display_snapshot_refs(&display_batches, &filters)?;
                let reference_at = system_timestamp()?;
                let market_data_records = load_market_data_instrument_records(
                    &self.market_data_instruments,
                    &definitions,
                    &display_instrument_ids,
                    &display_batches,
                    reference_at,
                    &context,
                )?;
                let surface_policies = build_surface_policies(
                    &snapshots,
                    &display_snapshots,
                    &kraken_projection_refs,
                    &durable_market,
                    reference_at,
                    presentation_surface_operations()?,
                )?;
                let source_coverage = source_coverage_value(
                    &streams,
                    snapshots.failures(),
                    &filters,
                    &display_snapshots,
                    &kraken_projection_refs,
                );
                build_unified_market_result(
                    &streams,
                    &filters,
                    &definitions,
                    &market_data_records,
                    &display_snapshots,
                    &kraken_projection_refs,
                    &surface_policies,
                    &order_level,
                    reference_at,
                    source_coverage,
                    limits,
                    &context,
                )
            }
            _ => Err(ServiceError::NotFound),
        }?;
        ensure_live(&context)?;
        Ok(output)
    }

    fn begin_shutdown(&self) {
        self.reference_search.begin_shutdown();
        self.registry.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        let market = self.registry.finish_shutdown(deadline).await;
        let reference = self.reference_search.finish_shutdown(deadline).await;
        market.and(reference)
    }
}

fn build_reference_search_result(
    page: MarketReferenceSearchPage,
    limits: market_squawk_services::ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let MarketReferenceSearchPage {
        records,
        available,
        has_more,
    } = page;
    let mut values = Vec::new();
    values
        .try_reserve_exact(records.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for record in records {
        ensure_live(context)?;
        match record.quality {
            DataQuality::OfficialDelayed => {}
            _ => return Err(ServiceError::InvalidResult),
        }
        values.push(serde_json::json!({
            "referenceId": record.reference_id.as_str(),
            "symbol": record.symbol,
            "name": record.security_name.trim(),
            "assetClass": match record.asset_class {
                AssetClass::Equity => "equity",
                AssetClass::Fund => "fund",
                _ => return Err(ServiceError::InvalidResult),
            },
            "isEtf": record.is_etf,
            "effectiveAt": timestamp_value(record.effective_at),
            "availableAt": timestamp_value(record.available_at),
        }));
    }
    results::bounded_result(
        &values,
        available,
        serde_json::json!({
            "complete": !has_more,
            "availability": if values.is_empty() { "unavailable" } else { "available" },
        }),
        serde_json::json!({
            "quality": "official_delayed",
            "executionEligible": false,
        }),
        limits,
        context,
    )
}

fn load_instrument_definitions(
    reader: &InstrumentDefinitionReadCapability,
    streams: &[StreamView<'_>],
    kraken: &[MarketKrakenPriceProjectionLease],
    durable_market: &DurableMarketEvidenceSet,
    context: &RequestContext,
) -> Result<Vec<InstrumentDefinition>, ServiceError> {
    let mut instrument_ids = Vec::new();
    instrument_ids
        .try_reserve_exact(
            streams
                .len()
                .checked_add(kraken.len())
                .and_then(|count| count.checked_add(durable_market.routes.len()))
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    instrument_ids.extend(streams.iter().map(|view| view.route.route().instrument()));
    instrument_ids.extend(kraken.iter().map(|snapshot| snapshot.key().instrument_id()));
    instrument_ids.extend(
        durable_market
            .routes
            .iter()
            .map(|route| route.instrument_id),
    );
    instrument_ids.sort_unstable();
    instrument_ids.dedup();
    if instrument_ids.len() > MAXIMUM_UNIFIED_MARKET_INSTRUMENTS {
        return Err(ServiceError::ResourceExhausted);
    }
    if instrument_ids.is_empty() {
        return Ok(Vec::new());
    }
    let definitions = reader
        .latest(
            &instrument_ids,
            MAXIMUM_UNIFIED_MARKET_INSTRUMENTS,
            context.deadline(),
            context.cancellation(),
        )
        .map_err(|error| {
            tracing::error!(%error, "unified Markets instrument-definition read failed");
            ServiceError::Unavailable
        })?;
    if definitions.len() != instrument_ids.len() {
        return Err(ServiceError::Unavailable);
    }
    Ok(definitions)
}

async fn load_market_instrument_ids(
    registry: &MarketRuntimeRegistry,
    filters: &MarketFilters<'_>,
    context: &RequestContext,
) -> Result<Vec<InstrumentId>, ServiceError> {
    let maximum =
        NonZeroUsize::new(MAXIMUM_UNIFIED_MARKET_INSTRUMENTS).ok_or(ServiceError::Internal)?;
    let mut instrument_ids = registry
        .market_instrument_ids(maximum, context.deadline(), context.cancellation())
        .await?;
    instrument_ids.retain(|instrument_id| matches_instrument_filter(filters, *instrument_id));
    Ok(instrument_ids)
}

async fn load_display_instrument_ids(
    registry: &MarketRuntimeRegistry,
    filters: &MarketFilters<'_>,
    context: &RequestContext,
) -> Result<Vec<InstrumentId>, ServiceError> {
    let maximum =
        NonZeroUsize::new(MAXIMUM_UNIFIED_MARKET_INSTRUMENTS).ok_or(ServiceError::Internal)?;
    let mut instrument_ids = registry
        .display_instrument_ids(maximum, context.deadline(), context.cancellation())
        .await?;
    instrument_ids.retain(|instrument_id| matches_instrument_filter(filters, *instrument_id));
    Ok(instrument_ids)
}

async fn load_display_snapshots(
    registry: &MarketRuntimeRegistry,
    instrument_ids: &[InstrumentId],
    read_time: DisplayMarketReadTime,
    context: &RequestContext,
) -> Result<Vec<MarketDisplaySnapshotBatch>, ServiceError> {
    let maximum_sources = NonZeroUsize::new(MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT)
        .ok_or(ServiceError::Internal)?;
    let mut batches = Vec::new();
    batches
        .try_reserve_exact(instrument_ids.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in instrument_ids {
        ensure_live(context)?;
        let batch = registry
            .display_snapshots_for_instrument(
                *instrument_id,
                maximum_sources,
                read_time,
                context.deadline(),
                context.cancellation(),
            )
            .await?;
        if batch.snapshots().is_empty() {
            return Err(ServiceError::Unavailable);
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn display_snapshot_refs<'batch>(
    batches: &'batch [MarketDisplaySnapshotBatch],
    filters: &MarketFilters<'_>,
) -> Result<Vec<&'batch MarketDisplaySnapshotLease>, ServiceError> {
    let count = batches.iter().try_fold(0_usize, |count, batch| {
        count.checked_add(batch.snapshots().len())
    });
    let mut snapshots = Vec::new();
    snapshots
        .try_reserve_exact(count.ok_or(ServiceError::ResourceExhausted)?)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for snapshot in batches
        .iter()
        .flat_map(MarketDisplaySnapshotBatch::snapshots)
    {
        if filters.matches_display_identity(snapshot) {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

async fn load_durable_market_evidence(
    registry: &MarketRuntimeRegistry,
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    context: &RequestContext,
) -> Result<DurableMarketEvidenceSet, ServiceError> {
    let mut bindings = registry
        .market_event_durable_route_reads(context.deadline(), context.cancellation())
        .await?;
    bindings.retain(|binding| filters.matches_durable_identity(binding));
    let mut bound_sources = Vec::new();
    bound_sources
        .try_reserve_exact(bindings.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let maximum_selection_count = bindings
        .len()
        .checked_mul(DURABLE_CURRENT_EVENT_KINDS.len())
        .ok_or(ServiceError::ResourceExhausted)?;
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(maximum_selection_count / DURABLE_CURRENT_EVENT_KINDS.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;

    for binding in &bindings {
        ensure_live(context)?;
        let source_id = binding.read().point_in_time_selector().source_surface();
        bound_sources.push(source_id.clone());
        if let Some(route) = load_durable_route_evidence(binding, reference_at, context).await?
            && route
                .primary_effective_at()
                .is_some_and(|observed_at| filters.matches_time(observed_at))
        {
            routes.push(route);
        }
    }
    DurableMarketEvidenceSet::try_new(bound_sources, routes, bindings.len())
}

/// Product fallback discovers original publications even when their source runtime is absent.
/// It reuses the page's canonical definitions and grants Display use only.
async fn load_retained_display_evidence(
    research: &Arc<crate::ResearchService>,
    records: &[MarketDataInstrumentRecord],
    instruments: &[InstrumentId],
    reference_at: Timestamp,
    context: &RequestContext,
) -> Result<DurableMarketEvidenceSet, ServiceError> {
    use crate::application::research::{
        MarketEventPointInTimeSelector, map_durable_market_ingest_error,
    };
    use market_squawk_data::{MarketEventUseRequest, ResearchUse, ResearchUseLimits};
    let mut routes = Vec::new();
    let mut sources = Vec::new();
    let mut expected = 0usize;
    let mut pending = Vec::new();
    let mut reads = Vec::new();
    for instrument in instruments {
        ensure_live(context)?;
        let record = records
            .binary_search_by_key(instrument, |record| record.definition().instrument_id())
            .ok()
            .and_then(|index| records.get(index))
            .ok_or(ServiceError::InvalidResult)?;
        let kinds: &[LiveEventClass] = if record.definition().asset_class() == AssetClass::Crypto {
            &DURABLE_CURRENT_EVENT_KINDS
        } else {
            &[LiveEventClass::Quote, LiveEventClass::Trade]
        };
        let mut after = None;
        loop {
            let page = research
                .analytical()
                .provider_market_event_durable_routes(
                    *instrument,
                    kinds,
                    reference_at,
                    reference_at,
                    after.as_ref(),
                    MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(map_durable_market_ingest_error)?;
            let exhausted = page.len() < MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT;
            after = page.last().cloned();
            for route in page {
                expected = expected
                    .checked_add(1)
                    .ok_or(ServiceError::ResourceExhausted)?;
                sources.push(route.source_surface().clone());
                let selector = MarketEventPointInTimeSelector::new(
                    Arc::clone(research),
                    route.dataset().clone(),
                    route.source_surface().clone(),
                );
                for kind in kinds {
                    reads.push((
                        selector.clone(),
                        *instrument,
                        route.venue_id().clone(),
                        *kind,
                    ));
                }
                pending.push((route, instrument, record, kinds.len()));
            }
            if exhausted {
                break;
            }
        }
    }
    let mut results = MarketEventPointInTimeSelector::select_current_batch(
        research,
        &reads,
        reference_at,
        reference_at,
        MAXIMUM_DURABLE_EVENT_CANDIDATES,
        context.deadline(),
        context.cancellation().clone(),
    )
    .await
    .map_err(crate::application::market_selection::map_market_event_read_error)?
    .into_iter();
    // Group only exact selected coordinates with the same horizon, source and original
    // publication. One shared Display permit covers those inputs; it never widens selection.
    struct DisplayUseGroup {
        commit: market_squawk_data::MarketEventCommitRef,
        publication: EvidenceDigest,
        source: SourceId,
        coordinates: Vec<market_squawk_data::ProviderMarketEventSelectionCoordinate>,
        components: Vec<(usize, LiveEventClass)>,
    }
    let mut use_groups: Vec<DisplayUseGroup> = Vec::new();
    for (route, instrument, record, count) in pending {
        ensure_live(context)?;
        let mut selections = Vec::new();
        let mut unavailable = false;
        for _ in 0..count {
            match results.next().ok_or(ServiceError::InvalidResult)? {
                Ok(Some(selection)) => selections.push(selection),
                Ok(None) => {}
                Err(error) => {
                    match crate::application::market_selection::map_market_event_read_error(error) {
                        ServiceError::Unavailable | ServiceError::Unauthorized => {
                            unavailable = true
                        }
                        error => return Err(error),
                    }
                }
            }
        }
        if unavailable {
            continue;
        }
        let mut retained_metadata: Vec<SourceMetadata> = Vec::new();
        let mut admitted_selections = Vec::new();
        let mut denied_book = false;
        for receipt in selections {
            let mut denied = false;
            for candidate in receipt
                .selection()
                .sources()
                .iter()
                .flat_map(|source| source.tied_candidates())
            {
                let provenance = market_event_provenance(candidate.event());
                let revision = provenance.binding().metadata_revision();
                if !retained_metadata
                    .iter()
                    .any(|metadata| metadata.revision() == revision)
                {
                    if let Some(metadata) = research
                        .analytical()
                        .retained_source_metadata(
                            provenance.binding().source_id(),
                            revision,
                            reference_at,
                            context.deadline(),
                            context.cancellation(),
                        )
                        .map_err(map_durable_market_ingest_error)?
                    {
                        retained_metadata.push(metadata);
                    } else {
                        tracing::warn!(source_id = %route.source_surface(), %instrument,
                            event_kind = ?candidate.coordinate().event_kind(),
                            "retained market component original metadata is unavailable");
                        denied = true;
                        break;
                    }
                }
                // Retrieval authority applies at receipt; a retained closing quote may
                // describe an observation from before this authorization began.
                if !retained_metadata.iter().any(|metadata| {
                    metadata.revision() == revision
                        && metadata.is_effective_at(provenance.received_at())
                }) {
                    tracing::warn!(source_id = %route.source_surface(), %instrument,
                        event_kind = ?candidate.coordinate().event_kind(),
                        "retained market component was received outside its original authority");
                    denied = true;
                    break;
                }
                let native_reference = match candidate.event() {
                    MarketEvent::MarketDataQuote(quote) => Some(quote.reference()),
                    MarketEvent::MarketDataTrade(trade) => Some(trade.reference()),
                    _ => None,
                };
                if let Some(reference) = native_reference {
                    if reference.definition_digest() != record.revision_digest() {
                        tracing::warn!(source_id = %route.source_surface(), %instrument,
                            event_kind = ?candidate.coordinate().event_kind(),
                            "retained market component reference revision does not match the selected instrument");
                        denied = true;
                        break;
                    }
                    crate::application::market_selection::validate_native_reference(
                        reference,
                        record,
                        provenance,
                        reference_at,
                        crate::application::market_selection::NativeReferenceUse::RetainedDisplay,
                    )?;
                }
            }
            if !denied {
                admitted_selections.push(receipt);
            } else if matches!(
                receipt.selection().request().event_kind(),
                LiveEventClass::BookSnapshot | LiveEventClass::BookDelta
            ) {
                denied_book = true;
            }
        }
        if denied_book {
            admitted_selections.retain(|receipt| {
                !matches!(
                    receipt.selection().request().event_kind(),
                    LiveEventClass::BookSnapshot | LiveEventClass::BookDelta
                )
            });
        }
        let Some(latest) = admitted_selections
            .iter()
            .flat_map(|receipt| receipt.selection().sources())
            .flat_map(|source| source.tied_candidates())
            .max_by_key(|candidate| durable_cohort_recency_key(candidate))
        else {
            continue;
        };
        let latest_revision = market_event_provenance(latest.event())
            .binding()
            .metadata_revision();
        let metadata = retained_metadata
            .iter()
            .find(|metadata| metadata.revision() == latest_revision)
            .cloned()
            .ok_or(ServiceError::InvalidResult)?;
        let surface = SourceIdentifier::try_from(route.source_surface().as_str())
            .map_err(|_| ServiceError::InvalidResult)?;
        let Some(evidence) = DurableMarketRouteEvidence::try_new(
            surface,
            metadata,
            route.source_surface().clone(),
            *instrument,
            route.venue_id().clone(),
            admitted_selections,
            Some(retained_metadata),
        )?
        else {
            continue;
        };
        let route_index = routes.len();
        for receipt in &evidence.selections {
            for candidate in receipt.selection().sources()[0].tied_candidates() {
                let coordinate = candidate.coordinate();
                let commit = receipt.selection().commit();
                let publication = coordinate.publication().digest();
                let source = coordinate.source_surface();
                // Bound a permit by the existing authorization input limit; a large exact
                // publication may produce several permits without dropping any selected row.
                let group_index = use_groups.iter().position(|group| {
                    &group.commit == commit
                        && group.publication == publication
                        && &group.source == source
                        && (group.coordinates.len() < market_squawk_data::MAX_RESEARCH_USE_SOURCES
                            || group.coordinates.contains(coordinate))
                });
                let group_index = match group_index {
                    Some(index) => index,
                    None => {
                        use_groups.push(DisplayUseGroup {
                            commit: commit.clone(),
                            publication,
                            source: source.clone(),
                            coordinates: Vec::new(),
                            components: Vec::new(),
                        });
                        use_groups.len() - 1
                    }
                };
                let group = &mut use_groups[group_index];
                if !group.coordinates.contains(coordinate) {
                    group.coordinates.push(coordinate.clone());
                }
                let component = (route_index, receipt.selection().request().event_kind());
                if !group.components.contains(&component) {
                    group.components.push(component);
                }
            }
        }
        routes.push(evidence);
    }
    if results.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let mut denied = vec![Vec::new(); routes.len()];
    for DisplayUseGroup {
        commit,
        coordinates,
        components: members,
        ..
    } in use_groups
    {
        ensure_live(context)?;
        let remaining = context.deadline().saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let use_limits = ResearchUseLimits::try_new(
            1,
            market_squawk_data::MAX_RESEARCH_USE_GRAPH_NODES,
            market_squawk_data::MAX_RESEARCH_USE_EDGES,
            market_squawk_data::MAX_RESEARCH_USE_SOURCES,
            market_squawk_data::MAX_RESEARCH_USE_RETAINED_BYTES,
            remaining.min(std::time::Duration::from_secs(
                market_squawk_data::MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
            )),
            std::time::Duration::from_secs(
                market_squawk_data::MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS,
            ),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let authorization = research
            .authorize_market_event_use(
                MarketEventUseRequest::try_new(
                    commit.clone(),
                    coordinates,
                    ResearchUse::Display,
                    use_limits,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(crate::application::research::corporate_actions::map_research_error)?
            .map_err(crate::application::research::map_research_use_error);
        match authorization {
            Ok(authorization)
                if authorization.research_use() == ResearchUse::Display
                    && authorization.commit() == &commit
                    && system_timestamp()? < authorization.expires_at() =>
            {
                let authorization = Arc::new(authorization);
                for (index, _) in members {
                    if !routes[index]
                        .display_authorizations
                        .iter()
                        .any(|existing| Arc::ptr_eq(existing, &authorization))
                    {
                        routes[index]
                            .display_authorizations
                            .push(Arc::clone(&authorization));
                    }
                }
            }
            Ok(_) | Err(ServiceError::Unauthorized) => {
                for (index, event_kind) in members {
                    tracing::warn!(source_id = %routes[index].source_id,
                        instrument_id = %routes[index].instrument_id, ?event_kind,
                        "retained market component has no current Display-use permit");
                    denied[index].push(event_kind);
                    if matches!(
                        event_kind,
                        LiveEventClass::BookSnapshot | LiveEventClass::BookDelta
                    ) {
                        denied[index]
                            .extend([LiveEventClass::BookSnapshot, LiveEventClass::BookDelta]);
                    }
                }
            }
            Err(error) => return Err(error),
        }
    }
    let routes = routes
        .into_iter()
        .zip(denied)
        .filter_map(|(mut route, denied)| {
            route
                .selections
                .retain(|receipt| !denied.contains(&receipt.selection().request().event_kind()));
            if denied.contains(&LiveEventClass::Trade) {
                route.trade_status = TradeStatus::Unavailable;
            }
            // A rejected component must not withhold another family's independent permission.
            route.display_authorizations.retain(|authorization| {
                route.selections.iter().any(|receipt| {
                    receipt
                        .selection()
                        .sources()
                        .iter()
                        .flat_map(|source| source.tied_candidates())
                        .any(|candidate| {
                            let coordinate = candidate.coordinate();
                            authorization.admits_event(
                                receipt.selection().commit(),
                                coordinate.publication().digest(),
                                coordinate.publication_row_ordinal(),
                                coordinate.canonical_event_digest(),
                            )
                        })
                })
            });
            route.evidence_candidate().is_some().then_some(route)
        })
        .collect();
    DurableMarketEvidenceSet::try_new(sources, routes, expected)
}

async fn load_durable_route_evidence(
    binding: &MarketEventDurableRouteRead,
    reference_at: Timestamp,
    context: &RequestContext,
) -> Result<Option<DurableMarketRouteEvidence>, ServiceError> {
    let read = binding.read();
    let route = binding.route();
    if binding.metadata().source_id() != read.point_in_time_selector().source_surface() {
        return Err(ServiceError::InvalidResult);
    }
    let mut selections = Vec::new();
    selections
        .try_reserve_exact(DURABLE_CURRENT_EVENT_KINDS.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for event_kind in DURABLE_CURRENT_EVENT_KINDS {
        ensure_live(context)?;
        match read
            .point_in_time_selector()
            .select_current(
                route.instrument(),
                route.venue().clone(),
                event_kind,
                reference_at,
                reference_at,
                MAXIMUM_DURABLE_EVENT_CANDIDATES,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
        {
            Ok(Some(receipt)) => selections.push(receipt),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    source_id = %binding.metadata().source_id(),
                    instrument_id = %route.instrument(),
                    venue_id = %route.venue(),
                    ?event_kind,
                    %error,
                    "durable current-market evidence is unavailable for this runtime route"
                );
                return match super::super::market_selection::map_market_event_read_error(error) {
                    ServiceError::Unavailable | ServiceError::Unauthorized => Ok(None),
                    error => Err(error),
                };
            }
        }
    }
    ensure_live(context)?;
    match DurableMarketRouteEvidence::try_new(
        binding.surface_id().clone(),
        binding.metadata().clone(),
        binding.metadata().source_id().clone(),
        route.instrument(),
        route.venue().clone(),
        selections,
        None,
    ) {
        Ok(route) => Ok(route),
        Err(ServiceError::ResourceExhausted) => Err(ServiceError::ResourceExhausted),
        Err(error) => {
            tracing::warn!(
                source_id = %binding.metadata().source_id(),
                instrument_id = %route.instrument(),
                venue_id = %route.venue(),
                %error,
                "durable current-market route failed closed"
            );
            Ok(None)
        }
    }
}

fn kraken_projection_refs(
    projections: &[MarketKrakenPriceProjectionLease],
) -> Result<Vec<&MarketKrakenPriceProjectionLease>, ServiceError> {
    let mut references = Vec::new();
    references
        .try_reserve_exact(projections.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    references.extend(projections.iter());
    Ok(references)
}

fn load_market_data_instrument_records(
    reader: &MarketDataInstrumentReadCapability,
    execution_definitions: &[InstrumentDefinition],
    display_instrument_ids: &[InstrumentId],
    display_batches: &[MarketDisplaySnapshotBatch],
    reference_at: Timestamp,
    context: &RequestContext,
) -> Result<Vec<MarketDataInstrumentRecord>, ServiceError> {
    if display_batches.len() != display_instrument_ids.len() {
        return Err(ServiceError::Unavailable);
    }
    if display_instrument_ids
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(ServiceError::InvalidResult);
    }
    // Live crypto routes carry execution definitions independently of display feeds, but their
    // official reference revisions are stored in the same canonical market-data catalog.
    let mut instrument_ids = Vec::new();
    instrument_ids
        .try_reserve_exact(
            display_instrument_ids
                .len()
                .checked_add(execution_definitions.len())
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    instrument_ids.extend_from_slice(display_instrument_ids);
    instrument_ids.extend(
        execution_definitions
            .iter()
            .filter(|definition| definition.asset_class() == AssetClass::Crypto)
            .map(InstrumentDefinition::instrument_id),
    );
    instrument_ids.sort_unstable();
    instrument_ids.dedup();
    if instrument_ids.len() > MAXIMUM_UNIFIED_MARKET_INSTRUMENTS {
        return Err(ServiceError::ResourceExhausted);
    }
    let mut records = Vec::new();
    records
        .try_reserve_exact(instrument_ids.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_chunk in instrument_ids.chunks(MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS) {
        ensure_live(context)?;
        let mut query_instrument_ids = Vec::new();
        query_instrument_ids
            .try_reserve_exact(instrument_chunk.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        query_instrument_ids.extend_from_slice(instrument_chunk);
        let query = MarketDataInstrumentPopulationQuery::try_new(
            query_instrument_ids,
            reference_at,
            reference_at,
        )
        .map_err(|error| {
            tracing::error!(%error, "unified Markets market-data definition query failed");
            ServiceError::InvalidResult
        })?;
        let selection = reader
            .pin_population_as_of(query, context.deadline(), context.cancellation())
            .map_err(|error| {
                tracing::error!(%error, "unified Markets market-data definition PIT read failed");
                ServiceError::Unavailable
            })?;
        if selection.disposition() != MarketDataInstrumentPopulationDisposition::Complete {
            return Err(ServiceError::Unavailable);
        }
        if selection.query().knowledge_at() != reference_at
            || selection.query().effective_at() != reference_at
            || selection.query().instrument_ids() != instrument_chunk
            || !selection.exclusions().is_empty()
            || selection.records().len() != instrument_chunk.len()
            || selection
                .records()
                .iter()
                .zip(instrument_chunk)
                .any(|(record, expected)| record.definition().instrument_id() != *expected)
        {
            return Err(ServiceError::InvalidResult);
        }
        records.extend(selection.records().iter().cloned());
    }
    if records.len() != instrument_ids.len() {
        return Err(ServiceError::InvalidResult);
    }
    for (record, instrument_id) in records.iter().zip(&instrument_ids) {
        ensure_live(context)?;
        let definition = record.definition();
        let interval = definition.effective_interval();
        if definition.instrument_id() != *instrument_id
            || record.published_at() > reference_at
            || interval.starts_at() > reference_at
            || interval.ends_at().is_some_and(|end| reference_at >= end)
            || record.revision_digest().algorithm() != DigestAlgorithm::Sha256
            || record.revision_digest().bytes() == [0; 32]
        {
            return Err(ServiceError::Unavailable);
        }
    }
    for (instrument_id, batch) in display_instrument_ids.iter().zip(display_batches) {
        ensure_live(context)?;
        let index = records
            .binary_search_by_key(instrument_id, |record| record.definition().instrument_id())
            .map_err(|_error| ServiceError::Unavailable)?;
        let record = &records[index];
        if batch.snapshots().is_empty()
            || batch.snapshots().iter().any(|snapshot| {
                snapshot.lease().key().instrument_id() != *instrument_id
                    || !snapshot.matches_definition_record(record)
            })
        {
            return Err(ServiceError::Unavailable);
        }
    }
    Ok(records)
}

async fn load_kraken_price_projections(
    registry: &MarketRuntimeRegistry,
    instrument_ids: &[InstrumentId],
    filters: &MarketFilters<'_>,
    context: &RequestContext,
) -> Result<Vec<MarketKrakenPriceProjectionLease>, ServiceError> {
    let mut snapshots = Vec::new();
    snapshots
        .try_reserve_exact(instrument_ids.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in instrument_ids {
        ensure_live(context)?;
        if let Some(snapshot) = registry
            .kraken_price_projection(*instrument_id, context.deadline(), context.cancellation())
            .await?
            && filters.matches_kraken_identity(&snapshot)
        {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

async fn load_order_level_snapshots(
    registry: &MarketRuntimeRegistry,
    streams: &[StreamView<'_>],
    kraken: &[MarketKrakenPriceProjectionLease],
    context: &RequestContext,
) -> Result<Vec<MarketOrderLevelSnapshot>, ServiceError> {
    let maximum_orders =
        NonZeroUsize::new(MAXIMUM_UNIFIED_ORDER_SAMPLE).ok_or(ServiceError::Internal)?;
    let mut snapshots = Vec::new();
    snapshots
        .try_reserve_exact(
            streams
                .len()
                .checked_add(kraken.len())
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for view in streams {
        ensure_live(context)?;
        if !supports_order_level(view.metadata) {
            continue;
        }
        if snapshots
            .iter()
            .any(|existing| exact_order_level_identity(existing, view))
        {
            continue;
        }
        if let Some(snapshot) = registry
            .scalar_order_level_snapshot(
                view.surface_id,
                view.stream.source(),
                view.route.route().venue(),
                view.route.route().instrument(),
                view.stream.connection_generation(),
                maximum_orders,
                context.deadline(),
                context.cancellation(),
            )
            .await?
        {
            snapshots.push(snapshot);
        }
    }
    for projection in kraken {
        ensure_live(context)?;
        let key = projection.key();
        if snapshots.iter().any(|existing| {
            existing.source_id() == key.source_id()
                && existing.venue_id() == key.venue_id()
                && existing.instrument_id() == key.instrument_id()
                && existing.generation() == key.generation()
        }) {
            continue;
        }
        if let Some(snapshot) = registry
            .kraken_order_level_snapshot(
                projection,
                maximum_orders,
                context.deadline(),
                context.cancellation(),
            )
            .await?
        {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

fn exact_order_level_identity(snapshot: &MarketOrderLevelSnapshot, view: &StreamView<'_>) -> bool {
    snapshot.source_id() == view.stream.source()
        && snapshot.venue_id() == view.route.route().venue()
        && snapshot.instrument_id() == view.route.route().instrument()
        && snapshot.generation() == view.stream.connection_generation()
}

fn supports_order_level(metadata: &SourceMetadata) -> bool {
    metadata.coverage().live().is_some_and(|coverage| {
        coverage
            .rules()
            .iter()
            .any(|rule| rule.depth() == Some(MarketDepth::OrderLevel))
    })
}

fn build_surface_policies(
    snapshots: &MarketRuntimeSnapshotBatch,
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    durable_market: &DurableMarketEvidenceSet,
    reference_at: Timestamp,
    operations: MarketOperationSet,
) -> Result<Vec<MarketSurfaceSelectionPolicy>, ServiceError> {
    let policy_count = snapshots
        .sources()
        .iter()
        .try_fold(0_usize, |count, source| {
            source.metadata().iter().try_fold(count, |count, metadata| {
                count.checked_add(metadata.coverage().asset_classes().len())
            })
        });
    let policy_count = display_snapshots.iter().try_fold(
        policy_count.ok_or(ServiceError::ResourceExhausted)?,
        |count, snapshot| count.checked_add(snapshot.metadata().coverage().asset_classes().len()),
    );
    let policy_count = kraken_projections.iter().try_fold(
        policy_count.ok_or(ServiceError::ResourceExhausted)?,
        |count, snapshot| count.checked_add(snapshot.metadata().coverage().asset_classes().len()),
    );
    let policy_count = durable_market.routes.iter().try_fold(
        policy_count.ok_or(ServiceError::ResourceExhausted)?,
        |count, route| count.checked_add(route.metadata.coverage().asset_classes().len()),
    );
    let policy_count = policy_count.ok_or(ServiceError::ResourceExhausted)?;
    let mut policies = Vec::new();
    policies
        .try_reserve_exact(policy_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for source in snapshots.sources() {
        for metadata in source.metadata().iter() {
            for asset_class in metadata.coverage().asset_classes() {
                let rights = surface_rights(metadata, operations, reference_at)?;
                push_surface_policy(
                    &mut policies,
                    source.surface_id(),
                    metadata,
                    *asset_class,
                    operations,
                    rights,
                )?;
            }
        }
    }
    for snapshot in display_snapshots {
        let metadata = snapshot.metadata();
        for asset_class in metadata.coverage().asset_classes() {
            let rights = surface_rights(metadata, operations, reference_at)?;
            push_surface_policy(
                &mut policies,
                snapshot.surface_id(),
                metadata,
                *asset_class,
                operations,
                rights,
            )?;
        }
    }
    for snapshot in kraken_projections {
        let metadata = snapshot.metadata();
        for asset_class in metadata.coverage().asset_classes() {
            let rights = surface_rights(metadata, operations, reference_at)?;
            push_surface_policy(
                &mut policies,
                snapshot.surface_id(),
                metadata,
                *asset_class,
                operations,
                rights,
            )?;
        }
    }
    for route in &durable_market.routes {
        for asset_class in route.metadata.coverage().asset_classes() {
            let rights = surface_rights(&route.metadata, operations, reference_at)?;
            push_surface_policy(
                &mut policies,
                &route.surface_id,
                &route.metadata,
                *asset_class,
                operations,
                rights,
            )?;
        }
    }
    Ok(policies)
}

fn presentation_surface_operations() -> Result<MarketOperationSet, ServiceError> {
    MarketOperationSet::try_new(&[
        MarketOperation::SnapshotDisplay,
        MarketOperation::StreamDisplay,
    ])
    .map_err(|_error| ServiceError::Internal)
}

fn push_surface_policy(
    policies: &mut Vec<MarketSurfaceSelectionPolicy>,
    surface_id: &SourceIdentifier,
    metadata: &SourceMetadata,
    asset_class: AssetClass,
    operations: crate::application::market_selection::MarketOperationSet,
    rights: MarketSurfaceRightsPolicy,
) -> Result<(), ServiceError> {
    if policies
        .iter()
        .any(|policy| policy.matches_identity(surface_id, metadata.source_id(), asset_class))
    {
        return Ok(());
    }
    policies.push(MarketSurfaceSelectionPolicy::try_new(
        surface_id.clone(),
        metadata.source_id().clone(),
        metadata.provider().clone(),
        asset_class,
        operations,
        observation_timing(metadata),
        presentation_depth(metadata, asset_class),
        market_coverage(metadata, asset_class),
        rights,
    )?);
    Ok(())
}

fn surface_rights(
    metadata: &SourceMetadata,
    operations: crate::application::market_selection::MarketOperationSet,
    reference_at: Timestamp,
) -> Result<MarketSurfaceRightsPolicy, ServiceError> {
    let decision_id = metadata.revision().as_source_identifier().clone();
    if !metadata.is_effective_at(reference_at) {
        return MarketSurfaceRightsPolicy::unavailable(
            decision_id,
            crate::application::market_selection::RightsState::Unknown,
            reference_at,
        )
        .map_err(|_error| ServiceError::InvalidResult);
    }
    let authorization = metadata.authorization().effective_interval();
    let coverage = metadata.coverage().effective_interval();
    let effective_from = authorization.starts_at().max(coverage.starts_at());
    let effective_until = minimum_optional_timestamp(
        metadata.authorization().inclusive_authorization_deadline(),
        metadata.coverage().inclusive_coverage_deadline(),
    );
    MarketSurfaceRightsPolicy::try_admitted(
        decision_id,
        operations,
        effective_from,
        effective_from,
        effective_until,
    )
    .map_err(|_error| ServiceError::InvalidResult)
}

const fn minimum_optional_timestamp(
    left: Option<Timestamp>,
    right: Option<Timestamp>,
) -> Option<Timestamp> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if left.unix_nanos() <= right.unix_nanos() {
            left
        } else {
            right
        }),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

const fn observation_timing(
    metadata: &SourceMetadata,
) -> crate::application::market_selection::ObservationTiming {
    match metadata.coverage().delay() {
        CoverageDelay::NotApplicable => {
            crate::application::market_selection::ObservationTiming::Stored
        }
        CoverageDelay::Unknown => crate::application::market_selection::ObservationTiming::Unknown,
        CoverageDelay::RealTime => {
            crate::application::market_selection::ObservationTiming::RealTime
        }
        CoverageDelay::Delayed(_) => {
            crate::application::market_selection::ObservationTiming::Delayed
        }
    }
}

fn presentation_depth(metadata: &SourceMetadata, asset_class: AssetClass) -> Option<MarketDepth> {
    if matches!(asset_class, AssetClass::Index | AssetClass::Cash) {
        return None;
    }
    let Some(live) = metadata.coverage().live() else {
        return None;
    };
    let mut top_of_book = false;
    for rule in live.rules() {
        match rule.depth() {
            Some(MarketDepth::OrderLevel | MarketDepth::PriceLevel) => {
                // The joined `StreamSnapshot` is aggregated. The exact order-level directory is
                // exposed separately and must not be inferred from this representation.
                return Some(MarketDepth::PriceLevel);
            }
            Some(MarketDepth::TopOfBook) => top_of_book = true,
            None if rule.event_class() == LiveEventClass::Quote => top_of_book = true,
            None => {}
        }
    }
    top_of_book.then_some(MarketDepth::TopOfBook)
}

fn market_coverage(
    metadata: &SourceMetadata,
    asset_class: AssetClass,
) -> crate::application::market_selection::MarketCoverage {
    use crate::application::market_selection::MarketCoverage;
    if asset_class == AssetClass::Index {
        return MarketCoverage::Benchmark;
    }
    let topology = metadata.coverage().topology();
    if topology.is_consolidated() {
        MarketCoverage::Consolidated
    } else if topology.is_partial() {
        MarketCoverage::MultiVenuePartial
    } else {
        MarketCoverage::SingleVenue
    }
}

#[derive(Debug)]
struct MarketFilters<'request> {
    instruments: Vec<InstrumentId>,
    sources: Vec<&'request str>,
    time_range: Option<(Timestamp, Timestamp)>,
}

impl<'request> MarketFilters<'request> {
    fn parse(request: &'request TypedToolRequest) -> Result<Self, ServiceError> {
        let mut instruments = Vec::new();
        if let Some(values) = request
            .arguments()
            .get("instrumentIds")
            .and_then(Value::as_array)
        {
            instruments
                .try_reserve_exact(values.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for value in values {
                instruments.push(
                    value
                        .as_str()
                        .ok_or(ServiceError::InvalidRequest)?
                        .parse()
                        .map_err(|_error| ServiceError::InvalidRequest)?,
                );
            }
            instruments.sort_unstable();
        }

        let mut sources = Vec::new();
        if let Some(values) = request
            .arguments()
            .get("sourceCoverage")
            .and_then(Value::as_array)
        {
            sources
                .try_reserve_exact(values.len())
                .map_err(|_error| ServiceError::ResourceExhausted)?;
            for value in values {
                sources.push(value.as_str().ok_or(ServiceError::InvalidRequest)?);
            }
            sources.sort_unstable();
        }

        Ok(Self {
            instruments,
            sources,
            time_range: request
                .arguments()
                .get("timeRange")
                .map(parse_time_range)
                .transpose()?,
        })
    }

    fn matches_identity(&self, stream: &StreamView<'_>) -> bool {
        (self.instruments.is_empty()
            || self
                .instruments
                .binary_search(&stream.route.route().instrument())
                .is_ok())
            && (self.sources.is_empty()
                || self
                    .sources
                    .binary_search(&stream.stream.source().as_str())
                    .is_ok()
                || self
                    .sources
                    .binary_search(&stream.surface_id.as_str())
                    .is_ok())
    }

    fn matches_display_identity(&self, snapshot: &MarketDisplaySnapshotLease) -> bool {
        matches_instrument_filter(self, snapshot.lease().key().instrument_id())
            && (self.sources.is_empty()
                || self
                    .sources
                    .binary_search(&snapshot.metadata().source_id().as_str())
                    .is_ok()
                || self
                    .sources
                    .binary_search(&snapshot.surface_id().as_str())
                    .is_ok())
    }

    fn matches_kraken_identity(&self, snapshot: &MarketKrakenPriceProjectionLease) -> bool {
        matches_instrument_filter(self, snapshot.key().instrument_id())
            && (self.sources.is_empty()
                || self
                    .sources
                    .binary_search(&snapshot.metadata().source_id().as_str())
                    .is_ok()
                || self
                    .sources
                    .binary_search(&snapshot.surface_id().as_str())
                    .is_ok())
    }

    fn matches_durable_identity(&self, binding: &MarketEventDurableRouteRead) -> bool {
        matches_instrument_filter(self, binding.route().instrument())
            && (self.sources.is_empty()
                || self
                    .sources
                    .binary_search(&binding.metadata().source_id().as_str())
                    .is_ok()
                || self
                    .sources
                    .binary_search(&binding.surface_id().as_str())
                    .is_ok())
    }

    fn matches_time(&self, timestamp: Timestamp) -> bool {
        self.time_range
            .is_none_or(|(start, end)| timestamp >= start && timestamp <= end)
    }
}

fn matches_instrument_filter(filters: &MarketFilters<'_>, instrument_id: InstrumentId) -> bool {
    filters.instruments.is_empty() || filters.instruments.binary_search(&instrument_id).is_ok()
}

fn parse_time_range(value: &Value) -> Result<(Timestamp, Timestamp), ServiceError> {
    let range = value.as_object().ok_or(ServiceError::InvalidRequest)?;
    let parse = |name: &str| {
        range
            .get(name)
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .and_then(|value| value.timestamp_nanos_opt())
            .map(Timestamp::from_unix_nanos)
            .ok_or(ServiceError::InvalidRequest)
    };
    let start = parse("start")?;
    let end = parse("end")?;
    if start > end {
        Err(ServiceError::InvalidRequest)
    } else {
        Ok((start, end))
    }
}

#[derive(Clone, Copy)]
struct StreamView<'snapshot> {
    surface_id: &'snapshot SourceIdentifier,
    metadata: &'snapshot SourceMetadata,
    shard: &'snapshot ShardSnapshot,
    route: &'snapshot RouteSnapshot,
    stream: &'snapshot StreamSnapshot,
}

fn collect_streams<'snapshot>(
    snapshots: &'snapshot MarketRuntimeSnapshotBatch,
    filters: &MarketFilters<'_>,
    context: &RequestContext,
) -> Result<Vec<StreamView<'snapshot>>, ServiceError> {
    let mut count = 0_usize;
    for source in snapshots.sources() {
        for shard in source.lease().snapshots() {
            require_complete(shard.route_dimension())?;
            for route in shard.routes() {
                require_complete(route.stream_dimension())?;
                count = count
                    .checked_add(route.streams().len())
                    .ok_or(ServiceError::ResourceExhausted)?;
            }
        }
    }
    let mut streams = Vec::new();
    streams
        .try_reserve_exact(count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for source in snapshots.sources() {
        for shard in source.lease().snapshots() {
            ensure_live(context)?;
            for route in shard.routes() {
                for stream in route.streams() {
                    let metadata = exact_stream_metadata(source.metadata(), stream.source())?;
                    let view = StreamView {
                        surface_id: source.surface_id(),
                        metadata,
                        shard,
                        route,
                        stream,
                    };
                    if filters.matches_identity(&view) {
                        streams.push(view);
                    }
                }
            }
        }
    }
    streams.sort_unstable_by(compare_streams);
    Ok(streams)
}

/// Collects only one exact instrument's complete scalar stream set for a non-presentation read.
fn collect_candidate_streams<'snapshot>(
    snapshots: &'snapshot MarketRuntimeSnapshotBatch,
    instrument_id: InstrumentId,
    deadline: Instant,
    cancellation: &tokio_util::sync::CancellationToken,
) -> Result<Vec<StreamView<'snapshot>>, ServiceError> {
    let mut count = 0_usize;
    for source in snapshots.sources() {
        for shard in source.lease().snapshots() {
            require_complete(shard.route_dimension())?;
            for route in shard.routes() {
                require_complete(route.stream_dimension())?;
                if route.route().instrument() == instrument_id {
                    count = count
                        .checked_add(route.streams().len())
                        .ok_or(ServiceError::ResourceExhausted)?;
                }
            }
        }
    }
    let mut streams = Vec::new();
    streams
        .try_reserve_exact(count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for source in snapshots.sources() {
        for shard in source.lease().snapshots() {
            super::ensure_before(deadline, cancellation)?;
            for route in shard.routes() {
                if route.route().instrument() != instrument_id {
                    continue;
                }
                for stream in route.streams() {
                    let metadata = exact_stream_metadata(source.metadata(), stream.source())?;
                    streams.push(StreamView {
                        surface_id: source.surface_id(),
                        metadata,
                        shard,
                        route,
                        stream,
                    });
                }
            }
        }
    }
    if streams.len() != count {
        return Err(ServiceError::InvalidResult);
    }
    streams.sort_unstable_by(compare_streams);
    Ok(streams)
}

fn exact_stream_metadata<'metadata>(
    metadata: &'metadata [SourceMetadata],
    source_id: &SourceId,
) -> Result<&'metadata SourceMetadata, ServiceError> {
    let mut matches = metadata
        .iter()
        .filter(|candidate| candidate.source_id() == source_id);
    let selected = matches.next().ok_or(ServiceError::Unavailable)?;
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn require_complete(dimension: &SnapshotDimension) -> Result<(), ServiceError> {
    if dimension.completeness() == SnapshotCompleteness::Complete {
        Ok(())
    } else {
        Err(ServiceError::Unavailable)
    }
}

fn compare_streams(left: &StreamView<'_>, right: &StreamView<'_>) -> Ordering {
    left.route
        .route()
        .instrument()
        .cmp(&right.route.route().instrument())
        .then_with(|| {
            left.route
                .route()
                .venue()
                .as_str()
                .cmp(right.route.route().venue().as_str())
        })
        .then_with(|| {
            left.stream
                .source()
                .as_str()
                .cmp(right.stream.source().as_str())
        })
        .then_with(|| {
            left.stream
                .provider_product()
                .as_source_identifier()
                .as_str()
                .cmp(
                    right
                        .stream
                        .provider_product()
                        .as_source_identifier()
                        .as_str(),
                )
        })
        .then_with(|| {
            left.stream
                .provider_channel()
                .as_source_identifier()
                .as_str()
                .cmp(
                    right
                        .stream
                        .provider_channel()
                        .as_source_identifier()
                        .as_str(),
                )
        })
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}

/// Extends the real Alpaca capture/publication fixture through retained product assembly.
#[cfg(test)]
pub(crate) async fn assert_retained_quote_trade_components(
    research: &Arc<crate::ResearchService>,
    record: &MarketDataInstrumentRecord,
    trade_metadata: &SourceMetadata,
    quote_metadata: &SourceMetadata,
    trade_source_at: Timestamp,
    quote_source_at: Timestamp,
) -> Result<(), Box<dyn std::error::Error>> {
    let instrument = record.definition().instrument_id();
    let context = RequestContext::new(
        market_squawk_services::RequestId::try_string("retained-component-fixture")?,
        tokio_util::sync::CancellationToken::new(),
        Instant::now() + std::time::Duration::from_secs(30),
        market_squawk_services::ServiceLimits::try_new(
            4096,
            8,
            4096,
            8,
            market_squawk_services::JsonStructureLimits::try_new(16, 4096, 64, 64)?,
        )?,
    );
    // The explicit read horizon crosses original acquisition expiry without sleeping or
    // changing any stored clock. Independently admitted retained permits cover this horizon.
    let reference_at = trade_metadata
        .authorization()
        .effective_interval()
        .ends_at()
        .ok_or("fixture must exercise expired acquisition authority")?;
    assert!(!trade_metadata.is_effective_at(reference_at));
    let evidence = load_retained_display_evidence(
        research,
        std::slice::from_ref(record),
        &[instrument],
        reference_at,
        &context,
    )
    .await?;
    assert_eq!(evidence.routes.len(), 1);
    let route = &evidence.routes[0];
    assert_eq!(route.trade_status, TradeStatus::Available);
    let trade = route
        .candidate(LiveEventClass::Trade)
        .ok_or("missing independently retained trade")?;
    let quote = route
        .candidate(LiveEventClass::Quote)
        .ok_or("missing independently retained quote")?;
    assert_eq!(durable_candidate_effective_at(trade), trade_source_at);
    assert_eq!(durable_candidate_effective_at(quote), quote_source_at);
    assert!(trade_source_at > quote_source_at);
    assert!(durable_cohort_recency_key(quote) > durable_cohort_recency_key(trade));
    assert_eq!(route.component_metadata(trade), Some(trade_metadata));
    assert_eq!(route.component_metadata(quote), Some(quote_metadata));
    assert!(!quote_metadata.is_effective_at(market_event_provenance(trade.event()).received_at()));
    for candidate in [trade, quote] {
        assert!(route.display_authorization(candidate).is_some());
        assert!(
            route
                .display_fresh_until(candidate)
                .is_none_or(|until| until < reference_at)
        );
    }
    assert!(matches!(trade.event(), MarketEvent::MarketDataTrade(trade)
        if trade.price().amount() == rust_decimal::Decimal::new(51271, 2)
            && trade.quantity() == rust_decimal::Decimal::new(2, 0)));
    route.display_rights(presentation_surface_operations()?, reference_at)?;
    assert!(
        route
            .display_rights(
                MarketOperationSet::try_new(&[MarketOperation::PaperDecision])?,
                reference_at
            )
            .is_err()
    );
    let runtime = DurableMarketRouteEvidence::try_new(
        route.surface_id.clone(),
        quote_metadata.clone(),
        route.source_id.clone(),
        instrument,
        route.venue_id.clone(),
        route.selections.clone(),
        None,
    )?
    .ok_or("missing runtime quote cohort")?;
    assert!(runtime.candidate(LiveEventClass::Quote).is_some());
    assert!(runtime.candidate(LiveEventClass::Trade).is_none());
    assert!(runtime.display_authorizations.is_empty());
    Ok(())
}
