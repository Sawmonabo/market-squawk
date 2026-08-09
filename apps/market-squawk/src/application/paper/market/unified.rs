//! Source-preserving unified Markets presentation rows.

use market_squawk_domain::{
    AssetClass, CaptureIntegrityState, ChecksumIntegrity, CoverageConsolidation, CoverageDelay,
    CoverageStatus, Currency, DataQuality, DeliveryEvidence, ExecutionEligibility,
    InstrumentDefinition, InstrumentExecutionTerms, InstrumentId, LiveEventClass,
    MarketDataInstrumentDefinition, MarketDepth, ProviderChannel, ProviderProduct,
    SequenceIntegrity, SourceId, SourceIdentifier, StreamIntegrityState, Timestamp,
};
use market_squawk_live::{
    BookSide, OrderLevelBatchKind, OrderLevelPhase, OrderLevelPriceProjection,
    OrderLevelQuarantineReason, StreamPhaseSnapshot, StreamSnapshot,
};
use market_squawk_services::{RequestContext, ServiceError, ServiceLimits, TypedToolResult};
use market_squawk_sources::{InstrumentCoverageMembership, MarketFreshness, SourceMetadata};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::results::bounded_result;
use super::serialization::{QualitySummary, timestamp_value, with_availability};
use super::{MarketFilters, StreamView, ensure_live};
use crate::application::domain_support::encode_hex;
use crate::application::market_runtime::{
    MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease, MarketOrderLevelSnapshot,
};
use crate::application::market_selection::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, DowngradeDimension,
    DowngradePolicy, FreshnessBasis, FreshnessRequirement, HealthState, IntegrityState,
    MarketCoverage, MarketOperation, MarketOperationSet, MarketSelectionError,
    MarketSelectionPolicy, MarketSelectionReceipt, MarketSelectionRequest, ObservationTiming,
    ProviderBudgetSnapshot, RequestPriority, RightsAdmission, RightsState, SelectedMarketSource,
    SelectionClass, SourceCandidate, select_market_source,
};
use crate::live_source::display_market::{
    DisplayDecimal, DisplayEffectiveTimeBasis, DisplayMarketAvailability, DisplayMarketPayload,
    DisplayMarketProvenance, DisplayMarketReadObservation, DisplayMarketSnapshotLease,
    DisplayStatus,
};

const MAXIMUM_CANDIDATES_PER_INSTRUMENT: usize = 256;
const MAXIMUM_ALTERNATIVES_PER_INSTRUMENT: usize = 8;

/// Exact operation-rights decision attached to one provider surface and asset class.
#[derive(Clone, Debug)]
pub(super) struct MarketSurfaceRightsPolicy {
    decision_id: SourceIdentifier,
    state: RightsState,
    permitted_operations: Option<MarketOperationSet>,
    decided_at: Timestamp,
    effective_from: Option<Timestamp>,
    effective_until: Option<Timestamp>,
}

impl MarketSurfaceRightsPolicy {
    /// Constructs an admitted, time-bounded rights decision.
    pub(super) fn try_admitted(
        decision_id: SourceIdentifier,
        permitted_operations: MarketOperationSet,
        decided_at: Timestamp,
        effective_from: Timestamp,
        effective_until: Option<Timestamp>,
    ) -> Result<Self, MarketSelectionError> {
        RightsAdmission::try_admitted(
            decision_id.clone(),
            permitted_operations,
            decided_at,
            effective_from,
            effective_until,
        )?;
        Ok(Self {
            decision_id,
            state: RightsState::Admitted,
            permitted_operations: Some(permitted_operations),
            decided_at,
            effective_from: Some(effective_from),
            effective_until,
        })
    }

    /// Constructs an exact denied or unresolved rights observation.
    pub(super) fn unavailable(
        decision_id: SourceIdentifier,
        state: RightsState,
        observed_at: Timestamp,
    ) -> Result<Self, MarketSelectionError> {
        RightsAdmission::unavailable(decision_id.clone(), state, observed_at)?;
        Ok(Self {
            decision_id,
            state,
            permitted_operations: None,
            decided_at: observed_at,
            effective_from: None,
            effective_until: None,
        })
    }

    fn admission(&self) -> Result<RightsAdmission, MarketSelectionError> {
        match (self.state, self.permitted_operations, self.effective_from) {
            (RightsState::Admitted, Some(operations), Some(effective_from)) => {
                RightsAdmission::try_admitted(
                    self.decision_id.clone(),
                    operations,
                    self.decided_at,
                    effective_from,
                    self.effective_until,
                )
            }
            (state @ (RightsState::Unknown | RightsState::Denied), None, None) => {
                RightsAdmission::unavailable(self.decision_id.clone(), state, self.decided_at)
            }
            _ => Err(MarketSelectionError::InvalidRightsState),
        }
    }
}

/// Immutable provider-surface facts supplied by the application composition owner.
///
/// One surface may retain several independently governed source records, and each source may have
/// different coverage by asset class. The exact surface, source, and asset class therefore form
/// the key. Runtime quality, health, timestamps, and generation come from each retained stream.
#[derive(Clone, Debug)]
pub(super) struct MarketSurfaceSelectionPolicy {
    surface_id: SourceIdentifier,
    source_id: SourceId,
    provider_id: SourceIdentifier,
    asset_class: AssetClass,
    operations: MarketOperationSet,
    timing: ObservationTiming,
    depth: Option<MarketDepth>,
    coverage: MarketCoverage,
    rights: MarketSurfaceRightsPolicy,
}

impl MarketSurfaceSelectionPolicy {
    pub(super) fn matches_identity(
        &self,
        surface_id: &SourceIdentifier,
        source_id: &SourceId,
        asset_class: AssetClass,
    ) -> bool {
        &self.surface_id == surface_id
            && &self.source_id == source_id
            && self.asset_class == asset_class
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "provider identity and independently classified market facts form one closed surface policy"
    )]
    pub(super) fn try_new(
        surface_id: SourceIdentifier,
        source_id: SourceId,
        provider_id: SourceIdentifier,
        asset_class: AssetClass,
        operations: MarketOperationSet,
        timing: ObservationTiming,
        depth: Option<MarketDepth>,
        coverage: MarketCoverage,
        rights: MarketSurfaceRightsPolicy,
    ) -> Result<Self, ServiceError> {
        // This value describes the representation joined into the read model, not a provider's
        // maximum capability. `StreamSnapshot` contains aggregated price levels, so an OrderLevel
        // claim is rejected until a separate retained order-level authority is joined here.
        if depth == Some(MarketDepth::OrderLevel) {
            return Err(ServiceError::InvalidResult);
        }
        CandidateCapabilities::try_new(
            asset_class,
            operations,
            timing,
            depth,
            DataQuality::DirectUnverified,
            coverage,
        )
        .map_err(selection_error)?;
        Ok(Self {
            surface_id,
            source_id,
            provider_id,
            asset_class,
            operations,
            timing,
            depth,
            coverage,
            rights,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct UnifiedInstrumentDefinition<'definition> {
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    quote_currency: Currency,
    executable: Option<&'definition InstrumentDefinition>,
    market_data: Option<&'definition MarketDataInstrumentDefinition>,
}

impl<'definition> UnifiedInstrumentDefinition<'definition> {
    fn try_new(
        instrument_id: InstrumentId,
        executable: Option<&'definition InstrumentDefinition>,
        market_data: Option<&'definition MarketDataInstrumentDefinition>,
    ) -> Result<Self, ServiceError> {
        let (asset_class, quote_currency) = match (executable, market_data) {
            (Some(definition), _) => (definition.asset_class(), definition.quote_currency()),
            (None, Some(definition)) => (definition.asset_class(), definition.quote_currency()),
            (None, None) => return Err(ServiceError::InvalidResult),
        };
        if executable.is_some_and(|definition| definition.instrument_id() != instrument_id)
            || market_data.is_some_and(|definition| definition.instrument_id() != instrument_id)
        {
            return Err(ServiceError::InvalidResult);
        }
        if let (Some(executable), Some(market_data)) = (executable, market_data)
            && (executable.asset_class() != market_data.asset_class()
                || executable.quote_currency() != market_data.quote_currency())
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(Self {
            instrument_id,
            asset_class,
            quote_currency,
            executable,
            market_data,
        })
    }

    const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    const fn asset_class(self) -> AssetClass {
        self.asset_class
    }

    const fn quote_currency(self) -> Currency {
        self.quote_currency
    }
}

/// Builds one bounded presentation row per supplied stable instrument identity.
#[expect(
    clippy::too_many_arguments,
    reason = "the result, selection, source-evidence, and cancellation contracts remain explicit"
)]
pub(super) fn build_unified_market_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    definitions: &[InstrumentDefinition],
    market_data_definitions: &[MarketDataInstrumentDefinition],
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    validate_inputs(
        streams,
        definitions,
        market_data_definitions,
        display_snapshots,
        kraken_projections,
        surface_policies,
        order_level,
    )?;
    let selection_policy =
        MarketSelectionPolicy::v1(MAXIMUM_CANDIDATES_PER_INSTRUMENT).map_err(selection_error)?;
    let instrument_ids = unified_instrument_ids(definitions, market_data_definitions)?;
    let available = instrument_ids
        .iter()
        .filter(|instrument_id| matches_instrument(filters, **instrument_id))
        .count();
    let build_count = available.min(limits.maximum_result_items());
    let mut rows = Vec::new();
    rows.try_reserve_exact(build_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in instrument_ids {
        ensure_live(context)?;
        if !matches_instrument(filters, instrument_id) {
            continue;
        }
        if rows.len() == build_count {
            break;
        }
        let definition = unified_definition(instrument_id, definitions, market_data_definitions)?;
        let instrument_streams = streams
            .iter()
            .copied()
            .filter(|view| {
                view.route.route().instrument() == instrument_id
                    && filters.matches_time(view.stream.evaluated_at())
            })
            .collect::<Vec<_>>();
        let instrument_display = display_snapshots
            .iter()
            .copied()
            .filter(|snapshot| {
                snapshot.lease().key().instrument_id() == instrument_id
                    && display_selection_observation(snapshot.lease()).is_some_and(|observation| {
                        filters.matches_time(observation.observation().provenance().received_at())
                    })
            })
            .collect::<Vec<_>>();
        let instrument_kraken = kraken_projections
            .iter()
            .copied()
            .filter(|snapshot| {
                snapshot.key().instrument_id() == instrument_id
                    && filters.matches_time(snapshot.projection().received_at())
            })
            .collect::<Vec<_>>();
        let candidates = build_candidates(
            &instrument_streams,
            definition,
            &instrument_display,
            &instrument_kraken,
            surface_policies,
            order_level,
            reference_at,
        )?;
        let request = presentation_request(definition.asset_class(), reference_at)?;
        let receipt =
            select_market_source(selection_policy, request, candidates).map_err(selection_error)?;
        rows.push(instrument_row(
            definition,
            &instrument_streams,
            &instrument_display,
            &instrument_kraken,
            order_level,
            &receipt,
        )?);
    }

    let observed_streams = streams
        .iter()
        .filter(|view| filters.matches_time(view.stream.evaluated_at()))
        .count();
    let observed_display = display_snapshots
        .iter()
        .copied()
        .filter(|snapshot| {
            display_selection_observation(snapshot.lease()).is_some_and(|observation| {
                filters.matches_time(observation.observation().provenance().received_at())
            })
        })
        .count();
    let observed_kraken = kraken_projections
        .iter()
        .copied()
        .filter(|snapshot| filters.matches_time(snapshot.projection().received_at()))
        .count();
    let observed = observed_streams
        .checked_add(observed_display)
        .and_then(|count| count.checked_add(observed_kraken))
        .ok_or(ServiceError::ResourceExhausted)?;
    let mut quality = QualitySummary::new(reference_at);
    for view in streams
        .iter()
        .filter(|view| filters.matches_time(view.stream.evaluated_at()))
    {
        quality.observe_stream(view.stream);
    }
    let mut quality = quality.into_value();
    quality["summaryScope"] = Value::String("live_price_level_streams".to_owned());
    quality["summarizedObservationCount"] = Value::from(observed_streams);
    quality["displayObservationCount"] = Value::from(observed_display);
    quality["krakenOrderLevelProjectionCount"] = Value::from(observed_kraken);
    bounded_result(
        &rows,
        available,
        with_availability(source_coverage, observed),
        quality,
        limits,
        context,
    )
}

fn validate_inputs(
    streams: &[StreamView<'_>],
    definitions: &[InstrumentDefinition],
    market_data_definitions: &[MarketDataInstrumentDefinition],
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
) -> Result<(), ServiceError> {
    if definitions
        .windows(2)
        .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
    {
        return Err(ServiceError::InvalidResult);
    }
    if market_data_definitions
        .windows(2)
        .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
    {
        return Err(ServiceError::InvalidResult);
    }
    for (index, policy) in surface_policies.iter().enumerate() {
        if surface_policies.iter().skip(index + 1).any(|candidate| {
            candidate.surface_id == policy.surface_id
                && candidate.source_id == policy.source_id
                && candidate.asset_class == policy.asset_class
        }) {
            return Err(ServiceError::InvalidResult);
        }
    }
    if streams.iter().any(|view| {
        definitions
            .binary_search_by_key(&view.route.route().instrument(), |definition| {
                definition.instrument_id()
            })
            .is_err()
    }) {
        return Err(ServiceError::Unavailable);
    }
    for snapshot in display_snapshots {
        let actor = snapshot.lease();
        let key = actor.key();
        let definition = market_data_definitions
            .binary_search_by_key(&key.instrument_id(), |definition| {
                definition.instrument_id()
            })
            .ok()
            .and_then(|index| market_data_definitions.get(index))
            .ok_or(ServiceError::Unavailable)?;
        if snapshot.metadata().source_id() != key.source_id()
            || definition.instrument_id() != key.instrument_id()
            || snapshot.provider_symbol().as_str().is_empty()
        {
            return Err(ServiceError::InvalidResult);
        }
        validate_display_observations(actor)?;
    }
    for (index, snapshot) in display_snapshots.iter().enumerate() {
        if display_snapshots
            .iter()
            .skip(index + 1)
            .any(|candidate| same_display_candidate_identity(snapshot, candidate))
        {
            return Err(ServiceError::InvalidResult);
        }
    }
    for snapshot in kraken_projections {
        let key = snapshot.key();
        let definition = definitions
            .binary_search_by_key(&key.instrument_id(), InstrumentDefinition::instrument_id)
            .ok()
            .and_then(|index| definitions.get(index))
            .ok_or(ServiceError::Unavailable)?;
        validate_kraken_projection(snapshot, definition)?;
    }
    for (index, snapshot) in kraken_projections.iter().enumerate() {
        if kraken_projections
            .iter()
            .skip(index + 1)
            .any(|candidate| same_kraken_candidate_identity(snapshot, candidate))
        {
            return Err(ServiceError::InvalidResult);
        }
    }
    for (index, snapshot) in order_level.iter().enumerate() {
        if order_level.iter().skip(index + 1).any(|candidate| {
            candidate.source_id() == snapshot.source_id()
                && candidate.venue_id() == snapshot.venue_id()
                && candidate.instrument_id() == snapshot.instrument_id()
                && candidate.generation() == snapshot.generation()
        }) {
            return Err(ServiceError::InvalidResult);
        }
    }
    Ok(())
}

fn matches_instrument(filters: &MarketFilters<'_>, instrument_id: InstrumentId) -> bool {
    filters.instruments.is_empty() || filters.instruments.binary_search(&instrument_id).is_ok()
}

fn unified_instrument_ids(
    definitions: &[InstrumentDefinition],
    market_data_definitions: &[MarketDataInstrumentDefinition],
) -> Result<Vec<InstrumentId>, ServiceError> {
    let capacity = definitions
        .len()
        .checked_add(market_data_definitions.len())
        .ok_or(ServiceError::ResourceExhausted)?;
    let mut instrument_ids = Vec::new();
    instrument_ids
        .try_reserve_exact(capacity)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    instrument_ids.extend(definitions.iter().map(InstrumentDefinition::instrument_id));
    instrument_ids.extend(
        market_data_definitions
            .iter()
            .map(MarketDataInstrumentDefinition::instrument_id),
    );
    instrument_ids.sort_unstable();
    instrument_ids.dedup();
    Ok(instrument_ids)
}

fn unified_definition<'definition>(
    instrument_id: InstrumentId,
    definitions: &'definition [InstrumentDefinition],
    market_data_definitions: &'definition [MarketDataInstrumentDefinition],
) -> Result<UnifiedInstrumentDefinition<'definition>, ServiceError> {
    let executable = definitions
        .binary_search_by_key(&instrument_id, InstrumentDefinition::instrument_id)
        .ok()
        .and_then(|index| definitions.get(index));
    let market_data = market_data_definitions
        .binary_search_by_key(
            &instrument_id,
            MarketDataInstrumentDefinition::instrument_id,
        )
        .ok()
        .and_then(|index| market_data_definitions.get(index));
    UnifiedInstrumentDefinition::try_new(instrument_id, executable, market_data)
}

fn build_candidates(
    streams: &[StreamView<'_>],
    definition: UnifiedInstrumentDefinition<'_>,
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
    reference_at: Timestamp,
) -> Result<Vec<SourceCandidate>, ServiceError> {
    let candidate_count = streams
        .len()
        .checked_add(display_snapshots.len())
        .and_then(|count| count.checked_add(kraken_projections.len()))
        .ok_or(ServiceError::ResourceExhausted)?;
    if candidate_count > MAXIMUM_CANDIDATES_PER_INSTRUMENT {
        return Err(ServiceError::ResourceExhausted);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(candidate_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for view in streams {
        let executable = definition.executable.ok_or(ServiceError::Unavailable)?;
        let policy = exact_surface_policy(surface_policies, view, definition.asset_class())?;
        candidates.push(source_candidate(
            view,
            executable,
            policy,
            exact_order_level_snapshot(order_level, view)?,
            reference_at,
        )?);
    }
    for snapshot in display_snapshots {
        let market_data = definition.market_data.ok_or(ServiceError::Unavailable)?;
        let policy =
            exact_display_surface_policy(surface_policies, snapshot, definition.asset_class())?;
        candidates.push(display_source_candidate(
            snapshot,
            market_data,
            policy,
            reference_at,
        )?);
    }
    for snapshot in kraken_projections {
        let executable = definition.executable.ok_or(ServiceError::Unavailable)?;
        let policy =
            exact_kraken_surface_policy(surface_policies, snapshot, definition.asset_class())?;
        candidates.push(kraken_source_candidate(
            snapshot,
            executable,
            policy,
            reference_at,
        )?);
    }
    Ok(candidates)
}

fn exact_surface_policy<'policy>(
    policies: &'policy [MarketSurfaceSelectionPolicy],
    view: &StreamView<'_>,
    asset_class: AssetClass,
) -> Result<&'policy MarketSurfaceSelectionPolicy, ServiceError> {
    let mut matching = policies.iter().filter(|policy| {
        policy.surface_id == *view.surface_id
            && policy.source_id == *view.stream.source()
            && policy.provider_id == *view.metadata.provider()
            && policy.asset_class == asset_class
    });
    let policy = matching.next().ok_or(ServiceError::Unavailable)?;
    if matching.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(policy)
}

fn exact_display_surface_policy<'policy>(
    policies: &'policy [MarketSurfaceSelectionPolicy],
    snapshot: &MarketDisplaySnapshotLease,
    asset_class: AssetClass,
) -> Result<&'policy MarketSurfaceSelectionPolicy, ServiceError> {
    let mut matching = policies.iter().filter(|policy| {
        policy.surface_id == *snapshot.surface_id()
            && policy.source_id == *snapshot.metadata().source_id()
            && policy.provider_id == *snapshot.metadata().provider()
            && policy.asset_class == asset_class
    });
    let policy = matching.next().ok_or(ServiceError::Unavailable)?;
    if matching.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(policy)
}

fn exact_kraken_surface_policy<'policy>(
    policies: &'policy [MarketSurfaceSelectionPolicy],
    snapshot: &MarketKrakenPriceProjectionLease,
    asset_class: AssetClass,
) -> Result<&'policy MarketSurfaceSelectionPolicy, ServiceError> {
    let mut matching = policies.iter().filter(|policy| {
        policy.surface_id == *snapshot.surface_id()
            && policy.source_id == *snapshot.metadata().source_id()
            && policy.provider_id == *snapshot.metadata().provider()
            && policy.asset_class == asset_class
    });
    let policy = matching.next().ok_or(ServiceError::Unavailable)?;
    if matching.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(policy)
}

fn same_kraken_candidate_identity(
    left: &MarketKrakenPriceProjectionLease,
    right: &MarketKrakenPriceProjectionLease,
) -> bool {
    left.surface_id() == right.surface_id()
        && left.metadata().provider() == right.metadata().provider()
        && left.key().source_id() == right.key().source_id()
        && left.key().venue_id() == right.key().venue_id()
        && left.key().instrument_id() == right.key().instrument_id()
}

fn validate_kraken_projection(
    snapshot: &MarketKrakenPriceProjectionLease,
    definition: &InstrumentDefinition,
) -> Result<(), ServiceError> {
    let key = snapshot.key();
    let execution_terms = snapshot.execution_terms();
    let projection = snapshot.projection();
    let route = projection.route();
    let metadata = snapshot.metadata();
    let coverage = metadata.coverage();
    let live = coverage.live().ok_or(ServiceError::InvalidResult)?;
    if definition.instrument_id() != key.instrument_id()
        || execution_terms.instrument_id() != key.instrument_id()
        || definition.execution_terms() != execution_terms
        || snapshot.source_depth() != MarketDepth::OrderLevel
        || projection.market_depth() != MarketDepth::PriceLevel
        || metadata.quality_ceiling() != DataQuality::DirectUnverified
        || coverage.delay() != CoverageDelay::RealTime
        || coverage.delivery() != DeliveryEvidence::DirectVenue
        || metadata.source_id() != key.source_id()
        || key.source_id() != route.source_id()
        || key.venue_id() != route.venue_id()
        || key.instrument_id() != route.instrument_id()
        || key.generation() != route.generation()
        || snapshot.provider_symbol() != route.provider_instrument()
        || projection.sequence_evidence().connection_generation() != key.generation()
        || projection.checksum_evidence().connection_generation() != key.generation()
        || projection.received_at() > projection.available_at()
        || !coverage.asset_classes().contains(&definition.asset_class())
        || coverage.instruments().membership(key.instrument_id())
            != InstrumentCoverageMembership::Enumerated
        || !coverage.topology().is_single_venue()
        || !coverage.topology().contains_venue(key.venue_id())
        || !coverage.is_effective_at(projection.available_at())
        || live
            .rule_for(LiveEventClass::BookSnapshot, Some(MarketDepth::OrderLevel))
            .is_none()
        || live
            .rule_for(LiveEventClass::BookDelta, Some(MarketDepth::OrderLevel))
            .is_none()
        || projection.quality() != expected_kraken_quality(projection)
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

const fn expected_kraken_quality(projection: &OrderLevelPriceProjection) -> DataQuality {
    match projection.phase() {
        OrderLevelPhase::Quarantined(_) => DataQuality::Quarantined,
        OrderLevelPhase::AwaitingSnapshot => DataQuality::DirectUnverified,
        OrderLevelPhase::Healthy => match projection.freshness() {
            MarketFreshness::Stale { .. } => DataQuality::Stale,
            MarketFreshness::Uninitialized | MarketFreshness::Fresh { .. } => {
                DataQuality::DirectUnverified
            }
        },
    }
}

fn same_display_candidate_identity(
    left: &MarketDisplaySnapshotLease,
    right: &MarketDisplaySnapshotLease,
) -> bool {
    let left_key = left.lease().key();
    let right_key = right.lease().key();
    let Some(left_observation) = display_selection_observation(left.lease()) else {
        return false;
    };
    let Some(right_observation) = display_selection_observation(right.lease()) else {
        return false;
    };
    let left_provenance = left_observation.observation().provenance();
    let right_provenance = right_observation.observation().provenance();
    left.surface_id() == right.surface_id()
        && left.metadata().provider() == right.metadata().provider()
        && left_key.source_id() == right_key.source_id()
        && left_key.venue_id() == right_key.venue_id()
        && left_key.instrument_id() == right_key.instrument_id()
        && left_provenance.coverage().provider_product()
            == right_provenance.coverage().provider_product()
        && left_provenance.coverage().provider_channel()
            == right_provenance.coverage().provider_channel()
}

fn validate_display_observations(
    snapshot: &DisplayMarketSnapshotLease,
) -> Result<(), ServiceError> {
    let key = snapshot.key();
    for observation in [snapshot.quote(), snapshot.trade(), snapshot.status()]
        .into_iter()
        .flatten()
    {
        let provenance = observation.observation().provenance();
        if provenance.generation() != key.generation() {
            return Err(ServiceError::InvalidResult);
        }
    }
    Ok(())
}

fn display_selection_observation(
    snapshot: &DisplayMarketSnapshotLease,
) -> Option<&DisplayMarketReadObservation> {
    let mut selected = None;
    for candidate in [snapshot.quote(), snapshot.trade()].into_iter().flatten() {
        if selected.is_none_or(|current| display_observation_is_better(candidate, current)) {
            selected = Some(candidate);
        }
    }
    selected.or(snapshot.status())
}

fn display_observation_is_better(
    candidate: &DisplayMarketReadObservation,
    current: &DisplayMarketReadObservation,
) -> bool {
    let candidate_provenance = candidate.observation().provenance();
    let current_provenance = current.observation().provenance();
    display_availability_rank(candidate.availability())
        .cmp(&display_availability_rank(current.availability()))
        .then_with(|| {
            display_depth_rank(candidate_provenance.display_depth())
                .cmp(&display_depth_rank(current_provenance.display_depth()))
        })
        .then_with(|| {
            display_quality_rank(display_current_quality(candidate))
                .cmp(&display_quality_rank(display_current_quality(current)))
        })
        .then_with(|| {
            candidate_provenance
                .received_at()
                .cmp(&current_provenance.received_at())
        })
        .is_gt()
}

const fn display_availability_rank(availability: DisplayMarketAvailability) -> u8 {
    match availability {
        DisplayMarketAvailability::Fresh { .. } => 4,
        DisplayMarketAvailability::Stale { .. } => 3,
        DisplayMarketAvailability::Expired { .. } => 2,
        DisplayMarketAvailability::Quarantined { .. } => 1,
    }
}

const fn display_depth_rank(depth: Option<MarketDepth>) -> u8 {
    match depth {
        Some(MarketDepth::OrderLevel) => 3,
        Some(MarketDepth::PriceLevel) => 2,
        Some(MarketDepth::TopOfBook) => 1,
        None => 0,
    }
}

const fn display_quality_rank(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 9,
        DataQuality::DirectUnverified => 8,
        DataQuality::OfficialDelayed => 7,
        DataQuality::Aggregated => 6,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 4,
        DataQuality::Estimated => 3,
        DataQuality::Stale => 2,
        DataQuality::Quarantined => 1,
    }
}

fn exact_order_level_snapshot<'snapshot>(
    snapshots: &'snapshot [MarketOrderLevelSnapshot],
    view: &StreamView<'_>,
) -> Result<Option<&'snapshot MarketOrderLevelSnapshot>, ServiceError> {
    let mut matches = snapshots.iter().filter(|snapshot| {
        snapshot.source_id() == view.stream.source()
            && snapshot.venue_id() == view.route.route().venue()
            && snapshot.instrument_id() == view.route.route().instrument()
            && snapshot.generation() == view.stream.connection_generation()
    });
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn order_level_snapshot_for_kraken<'snapshot>(
    snapshots: &'snapshot [MarketOrderLevelSnapshot],
    selected: &MarketKrakenPriceProjectionLease,
) -> Result<Option<&'snapshot MarketOrderLevelSnapshot>, ServiceError> {
    let key = selected.key();
    let mut matches = snapshots.iter().filter(|snapshot| {
        snapshot.source_id() == key.source_id()
            && snapshot.venue_id() == key.venue_id()
            && snapshot.instrument_id() == key.instrument_id()
            && snapshot.generation() == key.generation()
    });
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn order_level_is_usable(snapshot: &MarketOrderLevelSnapshot) -> bool {
    snapshot.orders().phase() == OrderLevelPhase::Healthy
        && snapshot.orders().quality() == DataQuality::DirectUnverified
        && matches!(snapshot.orders().freshness(), MarketFreshness::Fresh { .. })
}

fn source_candidate(
    view: &StreamView<'_>,
    definition: &InstrumentDefinition,
    policy: &MarketSurfaceSelectionPolicy,
    order_level: Option<&MarketOrderLevelSnapshot>,
    reference_at: Timestamp,
) -> Result<SourceCandidate, ServiceError> {
    let stream = view.stream;
    let quality = super::serialization::current_display_quality(stream, reference_at);
    let source_timestamp = stream.source_timestamp();
    let timestamps = CandidateTimestamps::try_new(
        source_timestamp.unwrap_or(stream.received_at()),
        source_timestamp,
        stream.received_at(),
        stream.evaluated_at(),
        view.shard.published_at(),
    )
    .map_err(selection_error)?;
    let integrity = candidate_integrity(stream);
    let admission = CandidateAdmissionState::new(
        CandidateHealth::new(
            candidate_health(stream, reference_at),
            health_observed_at(stream),
        ),
        ProviderBudgetSnapshot::try_new(BudgetAvailability::NotRequired, None, None, reference_at)
            .map_err(selection_error)?,
        policy.rights.admission().map_err(selection_error)?,
        integrity,
        // A presentation read owns no execution capability. It must not infer one from quality.
        ExecutionEligibility::Ineligible,
    );
    SourceCandidate::try_new(
        CandidateIdentity::new(
            policy.provider_id.clone(),
            stream.provider_product().clone(),
            stream.provider_channel().clone(),
            stream.source().clone(),
            Some(view.route.route().venue().clone()),
            definition.instrument_id(),
            view.surface_id.to_owned(),
        ),
        CandidateCapabilities::try_new(
            definition.asset_class(),
            policy.operations,
            policy.timing,
            if order_level.is_some_and(order_level_is_usable) {
                Some(MarketDepth::OrderLevel)
            } else {
                policy.depth
            },
            quality,
            policy.coverage,
        )
        .map_err(selection_error)?,
        timestamps,
        admission,
    )
    .map_err(selection_error)
}

fn display_source_candidate(
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    policy: &MarketSurfaceSelectionPolicy,
    reference_at: Timestamp,
) -> Result<SourceCandidate, ServiceError> {
    let actor = snapshot.lease();
    let observation = display_selection_observation(actor).ok_or(ServiceError::Unavailable)?;
    let provenance = observation.observation().provenance();
    let coverage = provenance.coverage();
    if definition.instrument_id() != actor.key().instrument_id()
        || definition.asset_class() != policy.asset_class
        || snapshot.metadata().source_id() != actor.key().source_id()
        || snapshot.metadata().provider() != &policy.provider_id
        || snapshot.surface_id() != &policy.surface_id
    {
        return Err(ServiceError::InvalidResult);
    }
    let depth = display_candidate_depth(definition.asset_class(), provenance)?;
    let timing = display_timing(coverage.delay());
    let market_coverage = display_coverage(definition.asset_class(), coverage.consolidation());
    if policy.timing != timing || policy.depth != depth || policy.coverage != market_coverage {
        return Err(ServiceError::InvalidResult);
    }
    let timestamps = CandidateTimestamps::try_new(
        provenance.effective_at(),
        provenance.source_at(),
        provenance.received_at(),
        provenance.available_at(),
        provenance.available_at(),
    )
    .map_err(selection_error)?;
    let admission = CandidateAdmissionState::new(
        CandidateHealth::new(display_health(observation), reference_at),
        ProviderBudgetSnapshot::try_new(BudgetAvailability::NotRequired, None, None, reference_at)
            .map_err(selection_error)?,
        policy.rights.admission().map_err(selection_error)?,
        CandidateIntegrity::new(
            display_integrity(observation),
            Some(actor.key().generation()),
            provenance.available_at(),
        ),
        ExecutionEligibility::Ineligible,
    );
    SourceCandidate::try_new(
        CandidateIdentity::new(
            snapshot.metadata().provider().clone(),
            ProviderProduct::new(coverage.provider_product().clone()),
            ProviderChannel::new(coverage.provider_channel().clone()),
            actor.key().source_id().clone(),
            Some(actor.key().venue_id().clone()),
            definition.instrument_id(),
            snapshot.surface_id().clone(),
        ),
        CandidateCapabilities::try_new(
            definition.asset_class(),
            policy.operations,
            timing,
            depth,
            display_current_quality(observation),
            market_coverage,
        )
        .map_err(selection_error)?,
        timestamps,
        admission,
    )
    .map_err(selection_error)
}

fn kraken_source_candidate(
    snapshot: &MarketKrakenPriceProjectionLease,
    definition: &InstrumentDefinition,
    policy: &MarketSurfaceSelectionPolicy,
    reference_at: Timestamp,
) -> Result<SourceCandidate, ServiceError> {
    validate_kraken_projection(snapshot, definition)?;
    let projection = snapshot.projection();
    let coverage = snapshot.metadata().coverage();
    let live = coverage.live().ok_or(ServiceError::InvalidResult)?;
    let timing = display_timing(coverage.delay());
    if policy.timing != timing
        || policy.depth != Some(projection.market_depth())
        || policy.coverage != MarketCoverage::SingleVenue
    {
        return Err(ServiceError::InvalidResult);
    }
    let timestamps = CandidateTimestamps::try_new(
        projection.source_timestamp(),
        Some(projection.source_timestamp()),
        projection.received_at(),
        projection.available_at(),
        projection.available_at(),
    )
    .map_err(selection_error)?;
    let admission = CandidateAdmissionState::new(
        CandidateHealth::new(kraken_health(projection), projection.available_at()),
        ProviderBudgetSnapshot::try_new(BudgetAvailability::NotRequired, None, None, reference_at)
            .map_err(selection_error)?,
        policy.rights.admission().map_err(selection_error)?,
        CandidateIntegrity::new(
            kraken_integrity(projection),
            Some(snapshot.key().generation()),
            projection.available_at(),
        ),
        ExecutionEligibility::Ineligible,
    );
    SourceCandidate::try_new(
        CandidateIdentity::new(
            snapshot.metadata().provider().clone(),
            live.provider_product().clone(),
            live.provider_channel().clone(),
            snapshot.key().source_id().clone(),
            Some(snapshot.key().venue_id().clone()),
            definition.instrument_id(),
            snapshot.surface_id().clone(),
        ),
        CandidateCapabilities::try_new(
            definition.asset_class(),
            policy.operations,
            timing,
            Some(projection.market_depth()),
            projection.quality(),
            MarketCoverage::SingleVenue,
        )
        .map_err(selection_error)?,
        timestamps,
        admission,
    )
    .map_err(selection_error)
}

const fn kraken_health(projection: &OrderLevelPriceProjection) -> HealthState {
    match (projection.phase(), projection.freshness()) {
        (OrderLevelPhase::Quarantined(_), _) => HealthState::Quarantined,
        (OrderLevelPhase::AwaitingSnapshot, _) | (_, MarketFreshness::Uninitialized) => {
            HealthState::Unavailable
        }
        (OrderLevelPhase::Healthy, MarketFreshness::Fresh { .. }) => HealthState::Healthy,
        (OrderLevelPhase::Healthy, MarketFreshness::Stale { .. }) => HealthState::Degraded,
    }
}

const fn kraken_integrity(projection: &OrderLevelPriceProjection) -> IntegrityState {
    if matches!(projection.phase(), OrderLevelPhase::Quarantined(_)) {
        return IntegrityState::Quarantined;
    }
    match (
        projection.sequence_evidence().integrity(),
        projection.checksum_evidence().integrity(),
    ) {
        (SequenceIntegrity::Invalid, _) | (_, ChecksumIntegrity::Failed) => IntegrityState::Failed,
        (SequenceIntegrity::Uninitialized, _) | (_, ChecksumIntegrity::Unchecked) => {
            IntegrityState::Unverified
        }
        (
            SequenceIntegrity::Valid | SequenceIntegrity::NotSupported,
            ChecksumIntegrity::Valid | ChecksumIntegrity::NotSupported,
        ) => IntegrityState::Verified,
    }
}

fn display_candidate_depth(
    asset_class: AssetClass,
    provenance: &DisplayMarketProvenance,
) -> Result<Option<MarketDepth>, ServiceError> {
    if asset_class == AssetClass::Index {
        if provenance.display_depth().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        Ok(None)
    } else {
        match provenance.display_depth() {
            Some(MarketDepth::OrderLevel | MarketDepth::PriceLevel) => {
                Err(ServiceError::InvalidResult)
            }
            depth => Ok(depth),
        }
    }
}

const fn display_timing(delay: CoverageDelay) -> ObservationTiming {
    match delay {
        CoverageDelay::RealTime => ObservationTiming::RealTime,
        CoverageDelay::Delayed(_) => ObservationTiming::Delayed,
    }
}

fn display_coverage(
    asset_class: AssetClass,
    consolidation: CoverageConsolidation,
) -> MarketCoverage {
    if asset_class == AssetClass::Index {
        return MarketCoverage::Benchmark;
    }
    match consolidation {
        CoverageConsolidation::Consolidated => MarketCoverage::Consolidated,
        CoverageConsolidation::Partial => MarketCoverage::MultiVenuePartial,
        CoverageConsolidation::SingleVenue => MarketCoverage::SingleVenue,
    }
}

fn display_current_quality(observation: &DisplayMarketReadObservation) -> DataQuality {
    match observation.availability() {
        DisplayMarketAvailability::Fresh { .. } => observation.observation().provenance().quality(),
        DisplayMarketAvailability::Stale { .. } | DisplayMarketAvailability::Expired { .. } => {
            DataQuality::Stale
        }
        DisplayMarketAvailability::Quarantined { .. } => DataQuality::Quarantined,
    }
}

fn display_health(observation: &DisplayMarketReadObservation) -> HealthState {
    match observation.availability() {
        DisplayMarketAvailability::Fresh { .. } => {
            if observation.observation().provenance().coverage().status()
                == CoverageStatus::Sufficient
            {
                HealthState::Healthy
            } else {
                HealthState::Degraded
            }
        }
        DisplayMarketAvailability::Stale { .. } => HealthState::Degraded,
        DisplayMarketAvailability::Expired { .. } => HealthState::Unavailable,
        DisplayMarketAvailability::Quarantined { .. } => HealthState::Quarantined,
    }
}

fn display_integrity(observation: &DisplayMarketReadObservation) -> IntegrityState {
    if matches!(
        observation.availability(),
        DisplayMarketAvailability::Quarantined { .. }
    ) {
        return IntegrityState::Quarantined;
    }
    match observation.observation().provenance().capture_integrity() {
        CaptureIntegrityState::Healthy => IntegrityState::Verified,
        CaptureIntegrityState::Disabled => IntegrityState::NotApplicable,
        CaptureIntegrityState::Incomplete => IntegrityState::Failed,
    }
}

fn candidate_health(stream: &StreamSnapshot, reference_at: Timestamp) -> HealthState {
    if !stream.generation_current() || stream.phase() == StreamPhaseSnapshot::Disconnected {
        HealthState::Unavailable
    } else if stream.phase() == StreamPhaseSnapshot::Quarantined
        || stream.quality() == DataQuality::Quarantined
    {
        HealthState::Quarantined
    } else if stream.phase() == StreamPhaseSnapshot::Healthy
        && reference_at <= stream.source_valid_until()
    {
        HealthState::Healthy
    } else {
        HealthState::Degraded
    }
}

fn health_observed_at(stream: &StreamSnapshot) -> Timestamp {
    stream
        .runtime_evidence()
        .filter(|evidence| evidence.matches_stream(stream))
        .map_or(stream.evaluated_at(), |evidence| {
            evidence.health_observed_at()
        })
}

fn candidate_integrity(stream: &StreamSnapshot) -> CandidateIntegrity {
    let Some(evidence) = stream
        .runtime_evidence()
        .filter(|evidence| evidence.matches_stream(stream))
    else {
        return CandidateIntegrity::new(
            if stream.phase() == StreamPhaseSnapshot::Quarantined {
                IntegrityState::Quarantined
            } else {
                IntegrityState::Unverified
            },
            Some(stream.connection_generation()),
            stream.evaluated_at(),
        );
    };
    let state = match evidence.stream_integrity() {
        StreamIntegrityState::Healthy => IntegrityState::Verified,
        StreamIntegrityState::GapDetected
        | StreamIntegrityState::ChecksumFailed
        | StreamIntegrityState::Divergent => IntegrityState::Failed,
        StreamIntegrityState::Quarantined => IntegrityState::Quarantined,
        StreamIntegrityState::Initializing
        | StreamIntegrityState::Synchronizing
        | StreamIntegrityState::Validating
        | StreamIntegrityState::Stale => IntegrityState::Unverified,
    };
    CandidateIntegrity::new(
        state,
        Some(evidence.connection_generation()),
        evidence.qualification_evaluated_at(),
    )
}

fn presentation_request(
    asset_class: AssetClass,
    reference_at: Timestamp,
) -> Result<MarketSelectionRequest, ServiceError> {
    let depth = if matches!(asset_class, AssetClass::Index | AssetClass::Cash) {
        None
    } else {
        Some(MarketDepth::OrderLevel)
    };
    let coverage = if asset_class == AssetClass::Index {
        MarketCoverage::Benchmark
    } else {
        MarketCoverage::Consolidated
    };
    let downgrade = DowngradePolicy::try_new(
        &[
            ObservationTiming::Delayed,
            ObservationTiming::EndOfDay,
            ObservationTiming::Historical,
            ObservationTiming::Stored,
        ],
        &[
            Some(MarketDepth::PriceLevel),
            Some(MarketDepth::TopOfBook),
            None,
        ],
        &[
            DataQuality::DirectUnverified,
            DataQuality::OfficialDelayed,
            DataQuality::Aggregated,
            DataQuality::Indicative,
            DataQuality::Modeled,
            DataQuality::Estimated,
            DataQuality::Stale,
        ],
        &[
            MarketCoverage::Consolidated,
            MarketCoverage::MultiVenuePartial,
            MarketCoverage::SingleVenue,
            MarketCoverage::Benchmark,
            MarketCoverage::Reference,
            MarketCoverage::UserOwned,
        ],
        None,
    )
    .map_err(selection_error)?;
    MarketSelectionRequest::try_new(
        asset_class,
        MarketOperation::SnapshotDisplay,
        ObservationTiming::RealTime,
        depth,
        DataQuality::DirectVerified,
        coverage,
        FreshnessRequirement::try_new(reference_at, FreshnessBasis::Received, i64::MAX as u64)
            .map_err(selection_error)?,
        RequestPriority::Interactive,
        downgrade,
    )
    .map_err(selection_error)
}

fn instrument_row(
    definition: UnifiedInstrumentDefinition<'_>,
    streams: &[StreamView<'_>],
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
    order_level: &[MarketOrderLevelSnapshot],
    receipt: &MarketSelectionReceipt,
) -> Result<Value, ServiceError> {
    let selected = receipt.selected();
    let selected_view = selected
        .map(|selected| {
            exact_selected_view(streams, display_snapshots, kraken_projections, selected)
        })
        .transpose()?;
    let symbol = unified_symbol(
        definition,
        selected_view,
        display_snapshots,
        kraken_projections,
    )?;
    let quote = selected_view
        .map(|view| unified_quote_value(view, definition, receipt.selected_at()))
        .transpose()?
        .unwrap_or_else(empty_quote);
    let alternatives = receipt
        .eligible()
        .iter()
        .skip(1)
        .take(MAXIMUM_ALTERNATIVES_PER_INSTRUMENT)
        .map(|eligible| {
            source_summary(
                eligible.candidate(),
                eligible.freshness_age_nanos(),
                eligible.downgrade(),
            )
        })
        .collect::<Vec<_>>();
    let selected_source = match selected.zip(selected_view) {
        Some((selected, UnifiedSelectedView::Live(view))) => {
            selected_source_value(selected, view, receipt.selected_at())
        }
        Some((selected, UnifiedSelectedView::Display(snapshot))) => {
            display_selected_source_value(selected, snapshot, receipt.selected_at())?
        }
        Some((selected, UnifiedSelectedView::Kraken(snapshot))) => {
            kraken_selected_source_value(selected, snapshot, receipt.selected_at())?
        }
        None => Value::Null,
    };
    let order_book = match (selected_view, definition.executable) {
        (Some(UnifiedSelectedView::Live(view)), Some(executable)) => {
            exact_order_level_snapshot(order_level, &view)?
                .map(|snapshot| order_level_value(snapshot, executable))
                .transpose()?
                .unwrap_or(Value::Null)
        }
        (Some(UnifiedSelectedView::Kraken(snapshot)), Some(executable)) => {
            validate_kraken_projection(snapshot, executable)?;
            order_level_snapshot_for_kraken(order_level, snapshot)?
                .map(|orders| order_level_value_with_terms(orders, snapshot.execution_terms()))
                .transpose()?
                .ok_or(ServiceError::Unavailable)?
        }
        (Some(UnifiedSelectedView::Display(_)) | None, _) | (_, None) => Value::Null,
    };
    let selected_downgrades = selected
        .and_then(SelectedMarketSource::downgrade)
        .map(|downgrade| downgrade.dimensions())
        .unwrap_or(&[]);

    Ok(json!({
        "instrumentId": definition.instrument_id().to_string(),
        "symbol": symbol.value,
        "symbolKind": symbol.kind,
        "symbolVenueId": symbol.venue_id,
        "assetClass": definition.asset_class(),
        "quoteCurrency": definition.quote_currency().as_str(),
        "definitionKind": definition_kind(definition),
        "definitionRevision": definition.executable.map(|value| value.definition_revision().get()),
        "referenceRevision": definition.market_data.map(|value| value.reference_revision().as_source_identifier().as_str()),
        "permanentFigi": definition.market_data.map(|value| value.permanent_figi().as_str()),
        "displayName": definition.market_data.and_then(MarketDataInstrumentDefinition::display_name).map(|value| value.as_str()),
        "tickSize": definition.executable.map(|value| value.tick_size().as_decimal().normalize().to_string()),
        "lotSize": definition.executable.map(|value| value.lot_size().as_decimal().normalize().to_string()),
        "executionTermsAvailable": definition.executable.is_some(),
        "referenceEvidence": market_data_definition_evidence(definition.market_data),
        "availability": selected.map(availability_label).unwrap_or("Unavailable"),
        "confidence": selected.map(confidence_label).unwrap_or("No eligible source"),
        "quote": quote,
        "orderBook": order_book,
        "selectedSource": selected_source,
        "alternatives": alternatives,
        "selectionReceipt": {
            "policyRevision": receipt.policy_revision(),
            "policyDigest": {
                "algorithm": receipt.policy_digest().algorithm(),
                "bytes": encode_hex(receipt.policy_digest().bytes())
            },
            "selectedAt": timestamp_value(receipt.selected_at()),
            "eligibleCount": receipt.eligible().len(),
            "rejectedCount": receipt.rejected().len(),
            "availableAlternativeCount": receipt.eligible().len().saturating_sub(1),
            "returnedAlternativeCount": alternatives.len(),
            "alternativesComplete": receipt.eligible().len().saturating_sub(1) == alternatives.len(),
            "selectionClass": selected.map(|value| selection_class(value.class())),
            "downgradeDimensions": selected_downgrades.iter().map(downgrade_value).collect::<Vec<_>>()
        }
    }))
}

#[derive(Clone, Copy)]
enum UnifiedSelectedView<'snapshot> {
    Live(StreamView<'snapshot>),
    Display(&'snapshot MarketDisplaySnapshotLease),
    Kraken(&'snapshot MarketKrakenPriceProjectionLease),
}

struct UnifiedSymbol {
    value: String,
    venue_id: Option<String>,
    kind: &'static str,
}

fn exact_selected_view<'snapshot>(
    streams: &[StreamView<'snapshot>],
    display_snapshots: &[&'snapshot MarketDisplaySnapshotLease],
    kraken_projections: &[&'snapshot MarketKrakenPriceProjectionLease],
    selected: SelectedMarketSource<'_>,
) -> Result<UnifiedSelectedView<'snapshot>, ServiceError> {
    let live = exact_selected_stream(streams, selected)?;
    let display = exact_selected_display(display_snapshots, selected)?;
    let kraken = exact_selected_kraken(kraken_projections, selected)?;
    match (live, display, kraken) {
        (Some(view), None, None) => Ok(UnifiedSelectedView::Live(view)),
        (None, Some(snapshot), None) => Ok(UnifiedSelectedView::Display(snapshot)),
        (None, None, Some(snapshot)) => Ok(UnifiedSelectedView::Kraken(snapshot)),
        _ => Err(ServiceError::InvalidResult),
    }
}

fn unified_symbol(
    definition: UnifiedInstrumentDefinition<'_>,
    selected: Option<UnifiedSelectedView<'_>>,
    display_snapshots: &[&MarketDisplaySnapshotLease],
    kraken_projections: &[&MarketKrakenPriceProjectionLease],
) -> Result<UnifiedSymbol, ServiceError> {
    match selected {
        Some(UnifiedSelectedView::Live(view)) => {
            let executable = definition.executable.ok_or(ServiceError::InvalidResult)?;
            let mapping = display_mapping(executable, Some(view))?;
            return Ok(UnifiedSymbol {
                value: mapping.venue_symbol().as_str().to_owned(),
                venue_id: Some(mapping.venue_id().as_str().to_owned()),
                kind: "venue_symbol",
            });
        }
        Some(UnifiedSelectedView::Display(snapshot)) => {
            return Ok(UnifiedSymbol {
                value: snapshot.provider_symbol().as_str().to_owned(),
                venue_id: Some(snapshot.lease().key().venue_id().as_str().to_owned()),
                kind: "provider_subscription_symbol",
            });
        }
        Some(UnifiedSelectedView::Kraken(snapshot)) => {
            return Ok(UnifiedSymbol {
                value: snapshot.provider_symbol().as_str().to_owned(),
                venue_id: Some(snapshot.key().venue_id().as_str().to_owned()),
                kind: "provider_subscription_symbol",
            });
        }
        None => {}
    }
    if let Some(snapshot) = display_snapshots.first() {
        return Ok(UnifiedSymbol {
            value: snapshot.provider_symbol().as_str().to_owned(),
            venue_id: Some(snapshot.lease().key().venue_id().as_str().to_owned()),
            kind: "provider_subscription_symbol",
        });
    }
    if let Some(snapshot) = kraken_projections.first() {
        return Ok(UnifiedSymbol {
            value: snapshot.provider_symbol().as_str().to_owned(),
            venue_id: Some(snapshot.key().venue_id().as_str().to_owned()),
            kind: "provider_subscription_symbol",
        });
    }
    if let Some(executable) = definition.executable {
        let mapping = display_mapping(executable, None)?;
        return Ok(UnifiedSymbol {
            value: mapping.venue_symbol().as_str().to_owned(),
            venue_id: Some(mapping.venue_id().as_str().to_owned()),
            kind: "venue_symbol",
        });
    }
    let market_data = definition.market_data.ok_or(ServiceError::InvalidResult)?;
    if let Some(mapping) = market_data.venue_mappings().first() {
        return Ok(UnifiedSymbol {
            value: mapping.venue_symbol().as_str().to_owned(),
            venue_id: Some(mapping.venue_id().as_str().to_owned()),
            kind: "venue_symbol",
        });
    }
    Ok(UnifiedSymbol {
        value: market_data.permanent_figi().as_str().to_owned(),
        venue_id: None,
        kind: "permanent_figi",
    })
}

const fn definition_kind(definition: UnifiedInstrumentDefinition<'_>) -> &'static str {
    match (definition.executable, definition.market_data) {
        (Some(_), Some(_)) => "execution_and_market_data",
        (Some(_), None) => "execution",
        (None, Some(_)) => "market_data",
        (None, None) => "invalid",
    }
}

fn market_data_definition_evidence(definition: Option<&MarketDataInstrumentDefinition>) -> Value {
    definition.map_or(Value::Null, |definition| {
        let reference = definition.reference_payload_evidence().content_digest();
        let currency = definition.quote_currency_evidence().content_digest();
        json!({
            "referenceRevision": definition.reference_revision().as_source_identifier().as_str(),
            "referencePayloadDigest": {
                "algorithm": reference.algorithm(),
                "bytes": encode_hex(reference.bytes())
            },
            "quoteCurrencyPayloadDigest": {
                "algorithm": currency.algorithm(),
                "bytes": encode_hex(currency.bytes())
            },
            "referencePayloadLocator": payload_locator(definition.reference_payload_evidence()),
            "quoteCurrencyPayloadLocator": payload_locator(definition.quote_currency_evidence()),
            "effectiveFrom": timestamp_value(definition.effective_interval().starts_at()),
            "effectiveUntil": definition.effective_interval().ends_at().map(timestamp_value),
            "permanentFigi": definition.permanent_figi().as_str()
        })
    })
}

fn payload_locator(evidence: &market_squawk_domain::ExactPayloadEvidence) -> Value {
    evidence
        .version_pinned_locator()
        .map_or(Value::Null, |locator| {
            json!({
                "reference": locator.reference().as_str(),
                "version": locator.version().as_str()
            })
        })
}

fn display_mapping<'definition>(
    definition: &'definition InstrumentDefinition,
    selected: Option<StreamView<'_>>,
) -> Result<&'definition market_squawk_domain::VenueMapping, ServiceError> {
    if let Some(view) = selected {
        if let Some(mapping) = definition
            .venue_mappings()
            .iter()
            .find(|mapping| mapping.venue_id() == view.route.route().venue())
        {
            return Ok(mapping);
        }
    }
    definition
        .venue_mappings()
        .iter()
        .min_by(|left, right| {
            left.venue_id()
                .cmp(right.venue_id())
                .then_with(|| left.venue_symbol().cmp(right.venue_symbol()))
        })
        .ok_or(ServiceError::Unavailable)
}

fn exact_selected_stream<'snapshot>(
    streams: &[StreamView<'snapshot>],
    selected: SelectedMarketSource<'_>,
) -> Result<Option<StreamView<'snapshot>>, ServiceError> {
    let identity = selected.candidate().identity();
    let mut matches = streams.iter().copied().filter(|view| {
        view.surface_id == identity.observation_id()
            && view.stream.source() == identity.source_id()
            && view.stream.provider_product() == identity.product()
            && view.stream.provider_channel() == identity.feed()
            && Some(view.route.route().venue()) == identity.venue_id()
            && view.route.route().instrument() == identity.instrument_id()
    });
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn exact_selected_display<'snapshot>(
    snapshots: &[&'snapshot MarketDisplaySnapshotLease],
    selected: SelectedMarketSource<'_>,
) -> Result<Option<&'snapshot MarketDisplaySnapshotLease>, ServiceError> {
    let identity = selected.candidate().identity();
    let generation = selected.candidate().admission().integrity().generation();
    let mut matches = snapshots.iter().copied().filter(|snapshot| {
        let actor = snapshot.lease();
        let Some(observation) = display_selection_observation(actor) else {
            return false;
        };
        let provenance = observation.observation().provenance();
        snapshot.metadata().provider() == identity.provider()
            && provenance.coverage().provider_product() == identity.product().as_source_identifier()
            && provenance.coverage().provider_channel() == identity.feed().as_source_identifier()
            && actor.key().source_id() == identity.source_id()
            && Some(actor.key().venue_id()) == identity.venue_id()
            && actor.key().instrument_id() == identity.instrument_id()
            && snapshot.surface_id() == identity.observation_id()
            && Some(actor.key().generation()) == generation
    });
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn exact_selected_kraken<'snapshot>(
    snapshots: &[&'snapshot MarketKrakenPriceProjectionLease],
    selected: SelectedMarketSource<'_>,
) -> Result<Option<&'snapshot MarketKrakenPriceProjectionLease>, ServiceError> {
    let identity = selected.candidate().identity();
    let generation = selected.candidate().admission().integrity().generation();
    let mut matches = snapshots.iter().copied().filter(|snapshot| {
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
    let selected = matches.next();
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
}

fn unified_quote_value(
    selected: UnifiedSelectedView<'_>,
    definition: UnifiedInstrumentDefinition<'_>,
    selected_at: Timestamp,
) -> Result<Value, ServiceError> {
    match selected {
        UnifiedSelectedView::Live(view) => quote_value(
            view.stream,
            definition.executable.ok_or(ServiceError::InvalidResult)?,
            selected_at,
        ),
        UnifiedSelectedView::Display(snapshot) => display_quote_value(snapshot.lease()),
        UnifiedSelectedView::Kraken(snapshot) => kraken_quote_value(snapshot, definition),
    }
}

fn display_quote_value(snapshot: &DisplayMarketSnapshotLease) -> Result<Value, ServiceError> {
    let quote_observation = snapshot.quote();
    let quote = match quote_observation.map(|value| value.observation().payload()) {
        Some(DisplayMarketPayload::Quote(quote)) => Some(quote),
        None => None,
        Some(_) => return Err(ServiceError::InvalidResult),
    };
    let trade_observation = snapshot.trade();
    let trade = match trade_observation.map(|value| value.observation().payload()) {
        Some(DisplayMarketPayload::Trade(trade)) => Some(trade),
        None => None,
        Some(_) => return Err(ServiceError::InvalidResult),
    };
    let bid = quote.and_then(|value| value.bid());
    let ask = quote.and_then(|value| value.ask());
    let midpoint = bid
        .zip(ask)
        .map(|(bid, ask)| {
            bid.price()
                .value()
                .checked_add(ask.price().value())
                .and_then(|sum| sum.checked_div(Decimal::from(2_u8)))
                .map(|value| value.normalize().to_string())
                .ok_or(ServiceError::InvalidResult)
        })
        .transpose()?;
    Ok(json!({
        "bidPrice": bid.map(|value| display_decimal_string(value.price())),
        "bidPriceProviderLexeme": bid.map(|value| value.price().provider_lexeme()),
        "bidSize": bid.map(|value| display_decimal_string(value.quantity())),
        "bidSizeProviderLexeme": bid.map(|value| value.quantity().provider_lexeme()),
        "askPrice": ask.map(|value| display_decimal_string(value.price())),
        "askPriceProviderLexeme": ask.map(|value| value.price().provider_lexeme()),
        "askSize": ask.map(|value| display_decimal_string(value.quantity())),
        "askSizeProviderLexeme": ask.map(|value| value.quantity().provider_lexeme()),
        "midPrice": midpoint,
        "midPriceBasis": bid.zip(ask).map(|_| "calculated_from_selected_bid_and_ask"),
        "lastPrice": trade.map(|value| display_decimal_string(value.price())),
        "lastPriceProviderLexeme": trade.map(|value| value.price().provider_lexeme()),
        "lastSize": trade.map(|value| display_decimal_string(value.quantity())),
        "lastSizeProviderLexeme": trade.map(|value| value.quantity().provider_lexeme()),
        "lastSourceTimestamp": trade_observation.and_then(|value| value.observation().provenance().source_at()).map(timestamp_value),
        "lastReceivedAt": trade_observation.map(|value| timestamp_value(value.observation().provenance().received_at())),
        "lastAvailableAt": trade_observation.map(|value| timestamp_value(value.observation().provenance().available_at())),
        "lastQuality": trade_observation.map(display_current_quality),
        "lastFreshAtSelection": trade_observation.map(|value| matches!(value.availability(), DisplayMarketAvailability::Fresh { .. })),
        "quoteEvidence": quote_observation.map(display_observation_evidence).unwrap_or(Value::Null),
        "tradeEvidence": trade_observation.map(display_observation_evidence).unwrap_or(Value::Null)
    }))
}

fn kraken_quote_value(
    snapshot: &MarketKrakenPriceProjectionLease,
    definition: UnifiedInstrumentDefinition<'_>,
) -> Result<Value, ServiceError> {
    let executable = definition.executable.ok_or(ServiceError::InvalidResult)?;
    validate_kraken_projection(snapshot, executable)?;
    let execution_terms = snapshot.execution_terms();
    let projection = snapshot.projection();
    let bid = projection.bids().first().copied();
    let ask = projection.asks().first().copied();
    let bid_price = bid
        .map(|level| decimal_price_with_terms(level.price(), execution_terms))
        .transpose()?;
    let ask_price = ask
        .map(|level| decimal_price_with_terms(level.price(), execution_terms))
        .transpose()?;
    let midpoint = bid_price
        .zip(ask_price)
        .map(|(bid, ask)| {
            bid.checked_add(ask)
                .and_then(|sum| sum.checked_div(Decimal::from(2_u8)))
                .map(|value| value.normalize().to_string())
                .ok_or(ServiceError::InvalidResult)
        })
        .transpose()?;
    Ok(json!({
        "bidPrice": bid_price.map(|value| value.normalize().to_string()),
        "bidPriceProviderLexeme": Value::Null,
        "bidSize": bid.map(|level| decimal_quantity_with_terms(level.quantity(), execution_terms)).transpose()?,
        "bidSizeProviderLexeme": Value::Null,
        "askPrice": ask_price.map(|value| value.normalize().to_string()),
        "askPriceProviderLexeme": Value::Null,
        "askSize": ask.map(|level| decimal_quantity_with_terms(level.quantity(), execution_terms)).transpose()?,
        "askSizeProviderLexeme": Value::Null,
        "midPrice": midpoint,
        "midPriceBasis": bid.zip(ask).map(|_| "calculated_from_selected_bid_and_ask"),
        "lastPrice": Value::Null,
        "lastPriceProviderLexeme": Value::Null,
        "lastSize": Value::Null,
        "lastSizeProviderLexeme": Value::Null,
        "lastSourceTimestamp": Value::Null,
        "lastReceivedAt": Value::Null,
        "lastAvailableAt": Value::Null,
        "lastQuality": Value::Null,
        "lastFreshAtSelection": Value::Null,
        "quoteEvidence": kraken_projection_evidence(snapshot),
        "tradeEvidence": Value::Null
    }))
}

fn display_decimal_string(value: &DisplayDecimal) -> String {
    value.value().normalize().to_string()
}

fn display_observation_evidence(observation: &DisplayMarketReadObservation) -> Value {
    let provenance = observation.observation().provenance();
    let coverage = provenance.coverage();
    json!({
        "sourceIdentifier": provenance.source_identifier().as_str(),
        "sourceTimestamp": provenance.source_at().map(timestamp_value),
        "effectiveAt": timestamp_value(provenance.effective_at()),
        "effectiveTimeBasis": display_effective_time_basis(provenance.effective_time_basis()),
        "receivedAt": timestamp_value(provenance.received_at()),
        "availableAt": timestamp_value(provenance.available_at()),
        "metadataRevision": provenance.metadata_revision().as_source_identifier().as_str(),
        "recordedQuality": provenance.quality(),
        "currentDisplayQuality": display_current_quality(observation),
        "displayDepth": provenance.display_depth().map(depth_name),
        "connectionGeneration": provenance.generation().get(),
        "sessionId": provenance.session_id().as_str(),
        "frameId": provenance.frame_id().get(),
        "payloadDigest": {
            "algorithm": provenance.payload_digest().algorithm(),
            "bytes": encode_hex(provenance.payload_digest().bytes())
        },
        "captureIntegrity": provenance.capture_integrity(),
        "decoderRule": provenance.decoder_rule().as_str(),
        "decoderRuleVersion": provenance.decoder_rule_version().get(),
        "timestampRule": provenance.timestamp_rule().as_str(),
        "timestampRuleVersion": provenance.timestamp_rule_version().get(),
        "availability": display_availability_value(observation.availability()),
        "coverage": {
            "providerProduct": coverage.provider_product().as_str(),
            "providerChannel": coverage.provider_channel().as_str(),
            "eventClass": coverage.event_class(),
            "declaredDepth": coverage.declared_depth().map(depth_name),
            "delay": coverage.delay(),
            "consolidation": coverage.consolidation(),
            "delivery": coverage.delivery(),
            "status": coverage.status(),
            "staticEvidenceDigest": {
                "algorithm": coverage.static_evidence_digest().algorithm(),
                "bytes": encode_hex(coverage.static_evidence_digest().bytes())
            },
            "runtimeEvidenceDigest": coverage.runtime_evidence_digest().map(|digest| json!({
                "algorithm": digest.algorithm(),
                "bytes": encode_hex(digest.bytes())
            })),
            "effectiveFrom": timestamp_value(coverage.effective_from()),
            "effectiveUntil": coverage.effective_until().map(timestamp_value)
        }
    })
}

fn kraken_projection_evidence(snapshot: &MarketKrakenPriceProjectionLease) -> Value {
    let execution_terms = snapshot.execution_terms();
    let projection = snapshot.projection();
    let route = projection.route();
    let (freshness, last_market_at) = market_freshness(projection.freshness());
    json!({
        "surfaceId": snapshot.surface_id().as_str(),
        "providerId": snapshot.metadata().provider().as_str(),
        "providerSymbol": snapshot.provider_symbol().as_str(),
        "sourceId": route.source_id().as_str(),
        "venueId": route.venue_id().as_str(),
        "instrumentId": route.instrument_id().to_string(),
        "providerInstrument": route.provider_instrument().as_str(),
        "connectionGeneration": route.generation().get(),
        "batchIdentifier": projection.batch_identifier().as_str(),
        "revision": projection.revision().to_string(),
        "phase": order_level_phase(projection.phase()),
        "quarantineReason": order_level_quarantine_reason(projection.phase()),
        "quality": projection.quality(),
        "sourceDepth": "order_level",
        "projectionDepth": depth_name(projection.market_depth()),
        "executionTerms": {
            "instrumentId": execution_terms.instrument_id().to_string(),
            "definitionRevision": execution_terms.definition_revision().get(),
            "priceTick": execution_terms.price_tick().as_decimal().normalize().to_string(),
            "lotSize": execution_terms.lot_size().as_decimal().normalize().to_string(),
            "quoteCurrency": execution_terms.quote_currency().as_str(),
            "settlementDenomination": execution_terms.settlement_denomination(),
            "contractMultiplier": execution_terms.contract_multiplier().normalize().to_string()
        },
        "freshness": freshness,
        "lastMarketAt": last_market_at.map(timestamp_value),
        "sourceTimestamp": timestamp_value(projection.source_timestamp()),
        "receivedAt": timestamp_value(projection.received_at()),
        "availableAt": timestamp_value(projection.available_at()),
        "providerSequence": projection.provider_sequence().map(|sequence| sequence.get()),
        "diagnosticOrdinal": projection.diagnostic_ordinal().map(|ordinal| ordinal.to_string()),
        "sequenceEvidence": projection.sequence_evidence(),
        "checksumEvidence": projection.checksum_evidence(),
        "bidLevelCount": projection.bids().len(),
        "askLevelCount": projection.asks().len()
    })
}

const fn display_effective_time_basis(value: DisplayEffectiveTimeBasis) -> &'static str {
    match value {
        DisplayEffectiveTimeBasis::Provider => "provider",
        DisplayEffectiveTimeBasis::Received => "received",
    }
}

fn display_availability_value(value: DisplayMarketAvailability) -> Value {
    match value {
        DisplayMarketAvailability::Fresh {
            stale_after,
            expires_after,
        } => json!({
            "state": "fresh",
            "staleAfter": timestamp_value(stale_after),
            "expiresAfter": timestamp_value(expires_after)
        }),
        DisplayMarketAvailability::Stale {
            stale_after,
            expires_after,
        } => json!({
            "state": "stale",
            "staleAfter": timestamp_value(stale_after),
            "expiresAfter": timestamp_value(expires_after)
        }),
        DisplayMarketAvailability::Expired { expired_after } => json!({
            "state": "expired",
            "expiredAfter": timestamp_value(expired_after)
        }),
        DisplayMarketAvailability::Quarantined { failure } => json!({
            "state": "quarantined",
            "failure": failure.to_string()
        }),
    }
}

fn quote_value(
    stream: &StreamSnapshot,
    definition: &InstrumentDefinition,
    selected_at: Timestamp,
) -> Result<Value, ServiceError> {
    let bid = stream.bids().first().copied();
    let ask = stream.asks().first().copied();
    let bid_price = bid
        .map(|level| decimal_price(level.price(), definition))
        .transpose()?;
    let ask_price = ask
        .map(|level| decimal_price(level.price(), definition))
        .transpose()?;
    let midpoint = bid_price
        .zip(ask_price)
        .map(|(bid, ask)| {
            bid.checked_add(ask)
                .and_then(|sum| sum.checked_div(Decimal::from(2_u8)))
                .map(|value| value.normalize().to_string())
                .ok_or(ServiceError::InvalidResult)
        })
        .transpose()?;
    let trade = stream.last_trade();
    let last_quality = trade.map(|trade| {
        if selected_at <= trade.qualification_valid_until() {
            trade.recorded_quality()
        } else {
            DataQuality::Stale
        }
    });
    Ok(json!({
        "bidPrice": bid_price.map(|value| value.normalize().to_string()),
        "bidPriceProviderLexeme": Value::Null,
        "bidSize": bid.map(|level| decimal_quantity(level.quantity(), definition)).transpose()?,
        "bidSizeProviderLexeme": Value::Null,
        "askPrice": ask_price.map(|value| value.normalize().to_string()),
        "askPriceProviderLexeme": Value::Null,
        "askSize": ask.map(|level| decimal_quantity(level.quantity(), definition)).transpose()?,
        "askSizeProviderLexeme": Value::Null,
        "midPrice": midpoint,
        "midPriceBasis": bid.zip(ask).map(|_| "calculated_from_selected_bid_and_ask"),
        "lastPrice": trade.map(|value| decimal_price(value.price(), definition)).transpose()?.map(|value| value.normalize().to_string()),
        "lastPriceProviderLexeme": Value::Null,
        "lastSize": trade.map(|value| decimal_quantity(value.quantity(), definition)).transpose()?,
        "lastSizeProviderLexeme": Value::Null,
        "lastSourceTimestamp": trade.and_then(|value| value.source_timestamp()).map(timestamp_value),
        "lastReceivedAt": trade.map(|value| timestamp_value(value.received_at())),
        "lastAvailableAt": trade.map(|value| timestamp_value(value.available_at())),
        "lastQuality": last_quality,
        "lastFreshAtSelection": trade.map(|value| selected_at <= value.qualification_valid_until()),
        "quoteEvidence": Value::Null,
        "tradeEvidence": Value::Null
    }))
}

fn empty_quote() -> Value {
    json!({
        "bidPrice": Value::Null,
        "bidPriceProviderLexeme": Value::Null,
        "bidSize": Value::Null,
        "bidSizeProviderLexeme": Value::Null,
        "askPrice": Value::Null,
        "askPriceProviderLexeme": Value::Null,
        "askSize": Value::Null,
        "askSizeProviderLexeme": Value::Null,
        "midPrice": Value::Null,
        "midPriceBasis": Value::Null,
        "lastPrice": Value::Null,
        "lastPriceProviderLexeme": Value::Null,
        "lastSize": Value::Null,
        "lastSizeProviderLexeme": Value::Null,
        "lastSourceTimestamp": Value::Null,
        "lastReceivedAt": Value::Null,
        "lastAvailableAt": Value::Null,
        "lastQuality": Value::Null,
        "lastFreshAtSelection": Value::Null,
        "quoteEvidence": Value::Null,
        "tradeEvidence": Value::Null
    })
}

fn order_level_value(
    snapshot: &MarketOrderLevelSnapshot,
    definition: &InstrumentDefinition,
) -> Result<Value, ServiceError> {
    order_level_value_with_terms(snapshot, definition.execution_terms())
}

fn order_level_value_with_terms(
    snapshot: &MarketOrderLevelSnapshot,
    execution_terms: InstrumentExecutionTerms,
) -> Result<Value, ServiceError> {
    let read = snapshot.orders();
    let mut orders = Vec::new();
    orders
        .try_reserve_exact(read.orders().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for order in read.orders() {
        orders.push(json!({
            "orderId": order.order_id().as_str(),
            "side": book_side_name(order.side()),
            "price": decimal_price_with_terms(order.price(), execution_terms)?.normalize().to_string(),
            "priceTicks": order.price().get().to_string(),
            "quantity": decimal_quantity_with_terms(order.quantity(), execution_terms)?,
            "quantityLots": order.quantity().get().to_string(),
            "providerOrderTimestamp": order.provider_order_timestamp().map(timestamp_value),
            "providerPriority": order.provider_priority().map(|priority| json!({
                "value": priority.value().to_string(),
                "rule": priority.rule().as_str(),
            })),
            "firstSeenIn": order_level_batch_kind(order.first_seen_in()),
            "lastUpdatedIn": order_level_batch_kind(order.last_updated_in()),
            "lastSourceTimestamp": timestamp_value(order.last_source_timestamp()),
            "lastReceivedAt": timestamp_value(order.last_received_at()),
            "arrivalOrdinal": order.arrival_ordinal().to_string(),
        }));
    }
    let (freshness, last_market_at) = market_freshness(read.freshness());
    Ok(json!({
        "depth": "order_level",
        "revision": read.revision().to_string(),
        "phase": order_level_phase(read.phase()),
        "quarantineReason": order_level_quarantine_reason(read.phase()),
        "quality": read.quality(),
        "freshness": freshness,
        "lastMarketAt": last_market_at.map(timestamp_value),
        "usableForSelection": order_level_is_usable(snapshot),
        "totalOrderCount": read.total_order_count(),
        "returnedOrderCount": orders.len(),
        "sampleTruncated": read.is_truncated(),
        "samplePolicy": "stable_provider_order_id_prefix",
        "orders": orders,
    }))
}

const fn book_side_name(side: BookSide) -> &'static str {
    match side {
        BookSide::Bid => "bid",
        BookSide::Ask => "ask",
    }
}

const fn order_level_batch_kind(kind: OrderLevelBatchKind) -> &'static str {
    match kind {
        OrderLevelBatchKind::Snapshot => "snapshot",
        OrderLevelBatchKind::Update => "update",
    }
}

const fn order_level_phase(phase: OrderLevelPhase) -> &'static str {
    match phase {
        OrderLevelPhase::AwaitingSnapshot => "awaiting_snapshot",
        OrderLevelPhase::Healthy => "healthy",
        OrderLevelPhase::Quarantined(_) => "quarantined",
    }
}

const fn order_level_quarantine_reason(phase: OrderLevelPhase) -> Option<&'static str> {
    match phase {
        OrderLevelPhase::AwaitingSnapshot | OrderLevelPhase::Healthy => None,
        OrderLevelPhase::Quarantined(reason) => Some(match reason {
            OrderLevelQuarantineReason::RouteMismatch => "route_mismatch",
            OrderLevelQuarantineReason::Sequence => "sequence",
            OrderLevelQuarantineReason::Checksum => "checksum",
            OrderLevelQuarantineReason::Snapshot => "snapshot",
            OrderLevelQuarantineReason::Mutation => "mutation",
            OrderLevelQuarantineReason::Book => "book",
            OrderLevelQuarantineReason::Resource => "resource",
        }),
    }
}

const fn market_freshness(value: MarketFreshness) -> (&'static str, Option<Timestamp>) {
    match value {
        MarketFreshness::Uninitialized => ("uninitialized", None),
        MarketFreshness::Fresh { last_market_at } => ("fresh", Some(last_market_at)),
        MarketFreshness::Stale { last_market_at } => ("stale", Some(last_market_at)),
    }
}

fn decimal_price(
    price: market_squawk_domain::PriceTicks,
    definition: &InstrumentDefinition,
) -> Result<Decimal, ServiceError> {
    decimal_price_with_terms(price, definition.execution_terms())
}

fn decimal_price_with_terms(
    price: market_squawk_domain::PriceTicks,
    execution_terms: InstrumentExecutionTerms,
) -> Result<Decimal, ServiceError> {
    price
        .checked_to_decimal(execution_terms.price_tick())
        .map_err(|_error| ServiceError::InvalidResult)
}

fn decimal_quantity(
    quantity: market_squawk_domain::QuantityLots,
    definition: &InstrumentDefinition,
) -> Result<String, ServiceError> {
    decimal_quantity_with_terms(quantity, definition.execution_terms())
}

fn decimal_quantity_with_terms(
    quantity: market_squawk_domain::QuantityLots,
    execution_terms: InstrumentExecutionTerms,
) -> Result<String, ServiceError> {
    quantity
        .checked_to_decimal(execution_terms.lot_size())
        .map(|value| value.normalize().to_string())
        .map_err(|_error| ServiceError::InvalidResult)
}

fn selected_source_value(
    selected: SelectedMarketSource<'_>,
    view: StreamView<'_>,
    selected_at: Timestamp,
) -> Value {
    let candidate = selected.candidate();
    let timestamps = candidate.timestamps();
    let capabilities = candidate.capabilities();
    let admission = candidate.admission();
    let rights = admission.rights();
    let integrity = admission.integrity();
    json!({
        "surfaceId": view.surface_id.as_str(),
        "providerId": candidate.identity().provider().as_str(),
        "sourceId": candidate.identity().source_id().as_str(),
        "venueId": candidate.identity().venue_id().map(|venue| venue.as_str()),
        "providerProduct": candidate.identity().product().as_source_identifier().as_str(),
        "providerChannel": candidate.identity().feed().as_source_identifier().as_str(),
        "timing": timing_name(capabilities.timing()),
        "depth": capabilities.depth().map(depth_name),
        "depthLabel": depth_label(capabilities.depth(), capabilities.asset_class()),
        "quality": capabilities.quality(),
        "coverage": coverage_name(capabilities.coverage()),
        "health": health_name(admission.health().state()),
        "healthObservedAt": timestamp_value(admission.health().observed_at()),
        "stateRevision": view.stream.state_revision(),
        "shardId": view.shard.shard_id().to_string(),
        "shardSnapshotRevision": view.shard.snapshot_revision().get(),
        "snapshotPublishedAt": timestamp_value(view.shard.published_at()),
        "providerBudget": {
            "availability": budget_name(admission.budget().availability()),
            "observedAt": timestamp_value(admission.budget().observed_at())
        },
        "rights": {
            "decisionId": rights.decision_id().as_str(),
            "state": rights_name(rights.state()),
            "decidedAt": timestamp_value(rights.decided_at()),
            "effectiveFrom": rights.effective_from().map(timestamp_value),
            "effectiveUntil": rights.effective_until().map(timestamp_value),
            "snapshotDisplayPermitted": rights.permitted_operations().contains(MarketOperation::SnapshotDisplay)
        },
        "freshness": {
            "ageNanos": selected.freshness_age_nanos(),
            "sourceTimestamp": timestamps.source_timestamp().map(timestamp_value),
            "receivedAt": timestamp_value(timestamps.received_at()),
            "availableAt": timestamp_value(timestamps.available_at()),
            "ingestedAt": timestamp_value(timestamps.ingested_at()),
            "sourceValidUntil": timestamp_value(view.stream.source_valid_until()),
            "freshAtSelection": selected_at <= view.stream.source_valid_until()
        },
        "integrity": {
            "state": integrity_name(integrity.state()),
            "assessedAt": timestamp_value(integrity.assessed_at()),
            "connectionGeneration": integrity.generation().map(|generation| generation.get()),
            "phase": view.stream.phase(),
            "generationCurrent": view.stream.generation_current(),
            "snapshotInitialized": view.stream.snapshot_initialized(),
            "lastSequence": view.stream.last_sequence().map(|sequence| sequence.get()),
            "runtimeEvidence": runtime_evidence_value(view.stream)
        }
    })
}

fn display_selected_source_value(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketDisplaySnapshotLease,
    selected_at: Timestamp,
) -> Result<Value, ServiceError> {
    let actor = snapshot.lease();
    let observation = display_selection_observation(actor).ok_or(ServiceError::InvalidResult)?;
    let provenance = observation.observation().provenance();
    let coverage = provenance.coverage();
    let candidate = selected.candidate();
    let capabilities = candidate.capabilities();
    let admission = candidate.admission();
    let rights = admission.rights();
    let integrity = admission.integrity();
    let expires_at = display_expires_at(observation.availability());
    Ok(json!({
        "surfaceId": snapshot.surface_id().as_str(),
        "providerId": snapshot.metadata().provider().as_str(),
        "providerSymbol": snapshot.provider_symbol().as_str(),
        "sourceId": actor.key().source_id().as_str(),
        "venueId": actor.key().venue_id().as_str(),
        "providerProduct": coverage.provider_product().as_str(),
        "providerChannel": coverage.provider_channel().as_str(),
        "timing": timing_name(capabilities.timing()),
        "depth": capabilities.depth().map(depth_name),
        "depthLabel": depth_label(capabilities.depth(), capabilities.asset_class()),
        "quality": capabilities.quality(),
        "coverage": coverage_name(capabilities.coverage()),
        "coverageStatus": coverage.status(),
        "health": health_name(admission.health().state()),
        "healthObservedAt": timestamp_value(admission.health().observed_at()),
        "stateRevision": actor.revision(),
        "snapshotPublishedAt": timestamp_value(provenance.available_at()),
        "providerBudget": {
            "availability": budget_name(admission.budget().availability()),
            "observedAt": timestamp_value(admission.budget().observed_at())
        },
        "rights": {
            "decisionId": rights.decision_id().as_str(),
            "state": rights_name(rights.state()),
            "decidedAt": timestamp_value(rights.decided_at()),
            "effectiveFrom": rights.effective_from().map(timestamp_value),
            "effectiveUntil": rights.effective_until().map(timestamp_value),
            "snapshotDisplayPermitted": rights.permitted_operations().contains(MarketOperation::SnapshotDisplay)
        },
        "freshness": {
            "ageNanos": selected.freshness_age_nanos(),
            "sourceTimestamp": provenance.source_at().map(timestamp_value),
            "effectiveAt": timestamp_value(provenance.effective_at()),
            "receivedAt": timestamp_value(provenance.received_at()),
            "availableAt": timestamp_value(provenance.available_at()),
            "ingestedAt": timestamp_value(provenance.available_at()),
            "sourceValidUntil": expires_at.map(timestamp_value),
            "freshAtSelection": matches!(observation.availability(), DisplayMarketAvailability::Fresh { .. }),
            "selectedAt": timestamp_value(selected_at),
            "availability": display_availability_value(observation.availability())
        },
        "integrity": {
            "state": integrity_name(integrity.state()),
            "assessedAt": timestamp_value(integrity.assessed_at()),
            "connectionGeneration": actor.key().generation().get(),
            "phase": display_phase(observation.availability()),
            "generationCurrent": Value::Null,
            "snapshotInitialized": actor.trade().is_some() || actor.quote().is_some() || actor.status().is_some(),
            "lastSequence": Value::Null,
            "terminalFailure": actor.terminal_failure().map(|failure| failure.to_string()),
            "runtimeEvidence": display_observation_evidence(observation)
        },
        "status": display_status_value(actor.status())?
    }))
}

fn kraken_selected_source_value(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketKrakenPriceProjectionLease,
    selected_at: Timestamp,
) -> Result<Value, ServiceError> {
    let projection = snapshot.projection();
    let live = snapshot
        .metadata()
        .coverage()
        .live()
        .ok_or(ServiceError::InvalidResult)?;
    let candidate = selected.candidate();
    let capabilities = candidate.capabilities();
    let admission = candidate.admission();
    let rights = admission.rights();
    let integrity = admission.integrity();
    let (freshness, last_market_at) = market_freshness(projection.freshness());
    Ok(json!({
        "surfaceId": snapshot.surface_id().as_str(),
        "providerId": snapshot.metadata().provider().as_str(),
        "providerSymbol": snapshot.provider_symbol().as_str(),
        "sourceId": snapshot.key().source_id().as_str(),
        "venueId": snapshot.key().venue_id().as_str(),
        "providerProduct": live.provider_product().as_source_identifier().as_str(),
        "providerChannel": live.provider_channel().as_source_identifier().as_str(),
        "timing": timing_name(capabilities.timing()),
        "depth": capabilities.depth().map(depth_name),
        "depthLabel": depth_label(capabilities.depth(), capabilities.asset_class()),
        "sourceDepth": depth_name(snapshot.source_depth()),
        "projectionDepth": depth_name(projection.market_depth()),
        "quality": capabilities.quality(),
        "qualityCeiling": snapshot.metadata().quality_ceiling(),
        "coverage": coverage_name(capabilities.coverage()),
        "health": health_name(admission.health().state()),
        "healthObservedAt": timestamp_value(admission.health().observed_at()),
        "stateRevision": projection.revision(),
        "snapshotPublishedAt": timestamp_value(projection.available_at()),
        "executionEligible": false,
        "providerBudget": {
            "availability": budget_name(admission.budget().availability()),
            "observedAt": timestamp_value(admission.budget().observed_at())
        },
        "rights": {
            "decisionId": rights.decision_id().as_str(),
            "state": rights_name(rights.state()),
            "decidedAt": timestamp_value(rights.decided_at()),
            "effectiveFrom": rights.effective_from().map(timestamp_value),
            "effectiveUntil": rights.effective_until().map(timestamp_value),
            "snapshotDisplayPermitted": rights.permitted_operations().contains(MarketOperation::SnapshotDisplay)
        },
        "freshness": {
            "ageNanos": selected.freshness_age_nanos(),
            "state": freshness,
            "lastMarketAt": last_market_at.map(timestamp_value),
            "sourceTimestamp": timestamp_value(projection.source_timestamp()),
            "effectiveAt": timestamp_value(projection.source_timestamp()),
            "receivedAt": timestamp_value(projection.received_at()),
            "availableAt": timestamp_value(projection.available_at()),
            "ingestedAt": timestamp_value(projection.available_at()),
            "sourceValidUntil": Value::Null,
            "freshAtSelection": matches!(projection.freshness(), MarketFreshness::Fresh { .. }),
            "selectedAt": timestamp_value(selected_at)
        },
        "integrity": {
            "state": integrity_name(integrity.state()),
            "assessedAt": timestamp_value(integrity.assessed_at()),
            "connectionGeneration": snapshot.key().generation().get(),
            "phase": order_level_phase(projection.phase()),
            "generationCurrent": true,
            "snapshotInitialized": projection.phase() != OrderLevelPhase::AwaitingSnapshot,
            "lastSequence": projection.provider_sequence().map(|sequence| sequence.get()),
            "runtimeEvidence": kraken_projection_evidence(snapshot)
        },
        "sourceMetadataEvidence": source_metadata_evidence(snapshot.metadata())
    }))
}

fn source_metadata_evidence(metadata: &SourceMetadata) -> Value {
    let revision = metadata
        .revision_evidence()
        .payload_evidence()
        .content_digest();
    let coverage = metadata.coverage();
    let coverage_digest = coverage.evidence().content_digest();
    json!({
        "schemaVersion": metadata.schema_version(),
        "sourceId": metadata.source_id().as_str(),
        "providerId": metadata.provider().as_str(),
        "sourceClass": metadata.source_class(),
        "metadataRevision": metadata.revision().as_source_identifier().as_str(),
        "metadataPayloadDigest": {
            "algorithm": revision.algorithm(),
            "bytes": encode_hex(revision.bytes())
        },
        "metadataPayloadLocator": payload_locator(metadata.revision_evidence().payload_evidence()),
        "qualityCeiling": metadata.quality_ceiling(),
        "coverage": {
            "payloadDigest": {
                "algorithm": coverage_digest.algorithm(),
                "bytes": encode_hex(coverage_digest.bytes())
            },
            "payloadLocator": payload_locator(coverage.evidence()),
            "effectiveFrom": timestamp_value(coverage.effective_interval().starts_at()),
            "effectiveUntil": coverage.effective_interval().ends_at().map(timestamp_value),
            "assetClasses": coverage.asset_classes(),
            "topology": coverage.topology(),
            "instruments": coverage.instruments(),
            "live": coverage.live(),
            "delay": coverage.delay(),
            "delivery": coverage.delivery()
        }
    })
}

const fn display_expires_at(value: DisplayMarketAvailability) -> Option<Timestamp> {
    match value {
        DisplayMarketAvailability::Fresh { expires_after, .. }
        | DisplayMarketAvailability::Stale { expires_after, .. } => Some(expires_after),
        DisplayMarketAvailability::Expired { expired_after } => Some(expired_after),
        DisplayMarketAvailability::Quarantined { .. } => None,
    }
}

const fn display_phase(value: DisplayMarketAvailability) -> &'static str {
    match value {
        DisplayMarketAvailability::Fresh { .. } => "healthy",
        DisplayMarketAvailability::Stale { .. } => "stale",
        DisplayMarketAvailability::Expired { .. } => "expired",
        DisplayMarketAvailability::Quarantined { .. } => "quarantined",
    }
}

fn display_status_value(
    observation: Option<&DisplayMarketReadObservation>,
) -> Result<Value, ServiceError> {
    let Some(observation) = observation else {
        return Ok(Value::Null);
    };
    let status = match observation.observation().payload() {
        DisplayMarketPayload::Status(status) => status,
        _ => return Err(ServiceError::InvalidResult),
    };
    let payload = match status {
        DisplayStatus::TradingHalt {
            provider_status,
            transition,
            reason,
        } => json!({
            "kind": "trading_halt",
            "providerStatus": provider_status.as_str(),
            "transition": transition,
            "reason": reason.as_str()
        }),
        DisplayStatus::Instrument {
            provider_status,
            trading_status,
        } => json!({
            "kind": "instrument",
            "providerStatus": provider_status.as_str(),
            "tradingStatus": trading_status
        }),
    };
    Ok(json!({
        "payload": payload,
        "evidence": display_observation_evidence(observation)
    }))
}

fn runtime_evidence_value(stream: &StreamSnapshot) -> Value {
    stream
        .runtime_evidence()
        .filter(|evidence| evidence.matches_stream(stream))
        .map(|evidence| {
            json!({
                "sessionId": evidence.session_id().as_str(),
                "assessmentId": evidence.assessment_id().as_source_identifier().as_str(),
                "bindingDigest": encode_hex(evidence.binding_digest()),
                "connection": evidence.connection(),
                "transportFreshness": evidence.transport_freshness(),
                "marketFreshness": evidence.market_freshness(),
                "sourceFreshness": evidence.source_freshness(),
                "streamIntegrity": evidence.stream_integrity(),
                "captureIntegrity": evidence.capture_integrity(),
                "coverageStatus": evidence.coverage_status(),
                "healthObservedAt": timestamp_value(evidence.health_observed_at()),
                "qualificationEvaluatedAt": timestamp_value(evidence.qualification_evaluated_at()),
                "qualificationValidUntil": timestamp_value(evidence.qualification_valid_until())
            })
        })
        .unwrap_or(Value::Null)
}

fn source_summary(
    candidate: &SourceCandidate,
    freshness_age_nanos: u64,
    downgrade: Option<&crate::application::market_selection::AdmittedDowngrade>,
) -> Value {
    let capabilities = candidate.capabilities();
    json!({
        "surfaceId": candidate.identity().observation_id().as_str(),
        "providerId": candidate.identity().provider().as_str(),
        "sourceId": candidate.identity().source_id().as_str(),
        "venueId": candidate.identity().venue_id().map(|venue| venue.as_str()),
        "providerProduct": candidate.identity().product().as_source_identifier().as_str(),
        "providerChannel": candidate.identity().feed().as_source_identifier().as_str(),
        "timing": timing_name(capabilities.timing()),
        "depth": capabilities.depth().map(depth_name),
        "quality": capabilities.quality(),
        "coverage": coverage_name(capabilities.coverage()),
        "freshnessAgeNanos": freshness_age_nanos,
        "downgradeDimensions": downgrade
            .map(|value| value.dimensions())
            .unwrap_or(&[])
            .iter()
            .map(downgrade_value)
            .collect::<Vec<_>>()
    })
}

fn downgrade_value(downgrade: &DowngradeDimension) -> Value {
    match *downgrade {
        DowngradeDimension::Timing { required, selected } => json!({
            "dimension": "timing",
            "required": timing_name(required),
            "selected": timing_name(selected)
        }),
        DowngradeDimension::Depth { minimum, selected } => json!({
            "dimension": "depth",
            "minimum": depth_name(minimum),
            "selected": selected.map(depth_name)
        }),
        DowngradeDimension::Quality { minimum, selected } => json!({
            "dimension": "quality",
            "minimum": minimum,
            "selected": selected
        }),
        DowngradeDimension::Coverage { required, selected } => json!({
            "dimension": "coverage",
            "required": coverage_name(required),
            "selected": coverage_name(selected)
        }),
        DowngradeDimension::Freshness {
            maximum_age_nanos,
            selected_age_nanos,
        } => json!({
            "dimension": "freshness",
            "maximumAgeNanos": maximum_age_nanos,
            "selectedAgeNanos": selected_age_nanos
        }),
    }
}

fn availability_label(selected: SelectedMarketSource<'_>) -> &'static str {
    if selected.candidate().capabilities().quality() == DataQuality::Stale {
        return "Stale";
    }
    match selected.candidate().capabilities().timing() {
        ObservationTiming::RealTime => "Live",
        ObservationTiming::Delayed => "Delayed",
        ObservationTiming::EndOfDay => "End of day",
        ObservationTiming::Historical | ObservationTiming::Stored => "Stored data",
    }
}

fn confidence_label(selected: SelectedMarketSource<'_>) -> &'static str {
    match selected.candidate().capabilities().quality() {
        DataQuality::DirectVerified => "Verified",
        DataQuality::DirectUnverified => "Direct, unverified",
        DataQuality::OfficialDelayed => "Official delayed",
        DataQuality::Aggregated => "Aggregated",
        DataQuality::Indicative => "Indicative",
        DataQuality::Modeled => "Modeled",
        DataQuality::Estimated => "Estimated",
        DataQuality::Stale => "Stale",
        DataQuality::Quarantined => "Unavailable",
    }
}

fn depth_label(depth: Option<MarketDepth>, asset_class: AssetClass) -> &'static str {
    match (depth, asset_class) {
        (Some(MarketDepth::TopOfBook), _) => "Best quote",
        (Some(MarketDepth::PriceLevel), _) => "Price-level book",
        (Some(MarketDepth::OrderLevel), _) => "Order-level book",
        (None, AssetClass::Index) => "Benchmark",
        (None, _) => "No market book",
    }
}

const fn selection_class(class: SelectionClass) -> &'static str {
    match class {
        SelectionClass::ExactRequirements => "exact_requirements",
        SelectionClass::AdmittedDowngrade => "admitted_downgrade",
    }
}

const fn timing_name(timing: ObservationTiming) -> &'static str {
    match timing {
        ObservationTiming::RealTime => "real_time",
        ObservationTiming::Delayed => "delayed",
        ObservationTiming::EndOfDay => "end_of_day",
        ObservationTiming::Historical => "historical",
        ObservationTiming::Stored => "stored",
    }
}

const fn depth_name(depth: MarketDepth) -> &'static str {
    match depth {
        MarketDepth::TopOfBook => "top_of_book",
        MarketDepth::PriceLevel => "price_level",
        MarketDepth::OrderLevel => "order_level",
    }
}

const fn coverage_name(coverage: MarketCoverage) -> &'static str {
    match coverage {
        MarketCoverage::Consolidated => "consolidated",
        MarketCoverage::MultiVenuePartial => "multi_venue_partial",
        MarketCoverage::SingleVenue => "single_venue",
        MarketCoverage::Benchmark => "benchmark",
        MarketCoverage::Reference => "reference",
        MarketCoverage::UserOwned => "user_owned",
    }
}

const fn health_name(health: HealthState) -> &'static str {
    match health {
        HealthState::Healthy => "healthy",
        HealthState::Degraded => "degraded",
        HealthState::Unavailable => "unavailable",
        HealthState::Quarantined => "quarantined",
    }
}

const fn integrity_name(integrity: IntegrityState) -> &'static str {
    match integrity {
        IntegrityState::Verified => "verified",
        IntegrityState::Unverified => "unverified",
        IntegrityState::NotApplicable => "not_applicable",
        IntegrityState::Failed => "failed",
        IntegrityState::Quarantined => "quarantined",
    }
}

const fn budget_name(budget: BudgetAvailability) -> &'static str {
    match budget {
        BudgetAvailability::NotRequired => "not_required",
        BudgetAvailability::Open => "open",
        BudgetAvailability::InteractiveOnly => "interactive_only",
        BudgetAvailability::Exhausted => "exhausted",
        BudgetAvailability::Unknown => "unknown",
    }
}

const fn rights_name(rights: RightsState) -> &'static str {
    match rights {
        RightsState::Admitted => "admitted",
        RightsState::Unknown => "unknown",
        RightsState::Denied => "denied",
    }
}

fn selection_error(error: MarketSelectionError) -> ServiceError {
    match error {
        MarketSelectionError::Allocation | MarketSelectionError::TooManyCandidates { .. } => {
            ServiceError::ResourceExhausted
        }
        _ => ServiceError::InvalidResult,
    }
}
