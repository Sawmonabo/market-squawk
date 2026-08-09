//! Source-preserving unified Markets presentation rows.

use market_squawk_domain::{
    AssetClass, DataQuality, ExecutionEligibility, InstrumentDefinition, MarketDepth, SourceId,
    SourceIdentifier, StreamIntegrityState, Timestamp,
};
use market_squawk_live::{
    BookSide, OrderLevelBatchKind, OrderLevelPhase, OrderLevelQuarantineReason,
    StreamPhaseSnapshot, StreamSnapshot,
};
use market_squawk_services::{RequestContext, ServiceError, ServiceLimits, TypedToolResult};
use market_squawk_sources::MarketFreshness;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::results::bounded_result;
use super::serialization::{QualitySummary, timestamp_value, with_availability};
use super::{MarketFilters, StreamView, ensure_live};
use crate::application::domain_support::encode_hex;
use crate::application::market_runtime::MarketOrderLevelSnapshot;
use crate::application::market_selection::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, DowngradeDimension,
    DowngradePolicy, FreshnessBasis, FreshnessRequirement, HealthState, IntegrityState,
    MarketCoverage, MarketOperation, MarketOperationSet, MarketSelectionError,
    MarketSelectionPolicy, MarketSelectionReceipt, MarketSelectionRequest, ObservationTiming,
    ProviderBudgetSnapshot, RequestPriority, RightsAdmission, RightsState, SelectedMarketSource,
    SelectionClass, SourceCandidate, select_market_source,
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

/// Builds one bounded presentation row per supplied exact instrument definition.
#[expect(
    clippy::too_many_arguments,
    reason = "the result, selection, source-evidence, and cancellation contracts remain explicit"
)]
pub(super) fn build_unified_market_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    definitions: &[InstrumentDefinition],
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    validate_inputs(streams, definitions, surface_policies, order_level)?;
    let selection_policy =
        MarketSelectionPolicy::v1(MAXIMUM_CANDIDATES_PER_INSTRUMENT).map_err(selection_error)?;
    let available = definitions
        .iter()
        .filter(|definition| matches_definition(filters, definition))
        .count();
    let build_count = available.min(limits.maximum_result_items());
    let mut rows = Vec::new();
    rows.try_reserve_exact(build_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for definition in definitions {
        ensure_live(context)?;
        if !matches_definition(filters, definition) {
            continue;
        }
        if rows.len() == build_count {
            break;
        }
        let instrument_streams = streams
            .iter()
            .copied()
            .filter(|view| {
                view.route.route().instrument() == definition.instrument_id()
                    && filters.matches_time(view.stream.evaluated_at())
            })
            .collect::<Vec<_>>();
        let candidates = build_candidates(
            &instrument_streams,
            definition,
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
            order_level,
            &receipt,
        )?);
    }

    let observed_streams = streams
        .iter()
        .filter(|view| filters.matches_time(view.stream.evaluated_at()))
        .count();
    let mut quality = QualitySummary::new(reference_at);
    for view in streams
        .iter()
        .filter(|view| filters.matches_time(view.stream.evaluated_at()))
    {
        quality.observe_stream(view.stream);
    }
    bounded_result(
        &rows,
        available,
        with_availability(source_coverage, observed_streams),
        quality.into_value(),
        limits,
        context,
    )
}

fn validate_inputs(
    streams: &[StreamView<'_>],
    definitions: &[InstrumentDefinition],
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
) -> Result<(), ServiceError> {
    if definitions
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

fn matches_definition(filters: &MarketFilters<'_>, definition: &InstrumentDefinition) -> bool {
    filters.instruments.is_empty()
        || filters
            .instruments
            .binary_search(&definition.instrument_id())
            .is_ok()
}

fn build_candidates(
    streams: &[StreamView<'_>],
    definition: &InstrumentDefinition,
    surface_policies: &[MarketSurfaceSelectionPolicy],
    order_level: &[MarketOrderLevelSnapshot],
    reference_at: Timestamp,
) -> Result<Vec<SourceCandidate>, ServiceError> {
    if streams.len() > MAXIMUM_CANDIDATES_PER_INSTRUMENT {
        return Err(ServiceError::ResourceExhausted);
    }
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(streams.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for view in streams {
        let policy = exact_surface_policy(surface_policies, view, definition.asset_class())?;
        candidates.push(source_candidate(
            view,
            definition,
            policy,
            exact_order_level_snapshot(order_level, view)?,
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
            && policy.asset_class == asset_class
    });
    let policy = matching.next().ok_or(ServiceError::Unavailable)?;
    if matching.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(policy)
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
    definition: &InstrumentDefinition,
    streams: &[StreamView<'_>],
    order_level: &[MarketOrderLevelSnapshot],
    receipt: &MarketSelectionReceipt,
) -> Result<Value, ServiceError> {
    let selected = receipt.selected();
    let selected_view = selected
        .map(|selected| exact_selected_stream(streams, selected))
        .transpose()?;
    let mapping = display_mapping(definition, selected_view)?;
    let quote = selected_view
        .map(|view| quote_value(view.stream, definition, receipt.selected_at()))
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
    let selected_source = selected
        .zip(selected_view)
        .map(|(selected, view)| selected_source_value(selected, view, receipt.selected_at()))
        .unwrap_or(Value::Null);
    let order_book = selected_view
        .map(|view| exact_order_level_snapshot(order_level, &view))
        .transpose()?
        .flatten()
        .map(|snapshot| order_level_value(snapshot, definition))
        .transpose()?
        .unwrap_or(Value::Null);
    let selected_downgrades = selected
        .and_then(SelectedMarketSource::downgrade)
        .map(|downgrade| downgrade.dimensions())
        .unwrap_or(&[]);

    Ok(json!({
        "instrumentId": definition.instrument_id().to_string(),
        "symbol": mapping.venue_symbol().as_str(),
        "symbolVenueId": mapping.venue_id().as_str(),
        "assetClass": definition.asset_class(),
        "quoteCurrency": definition.quote_currency().as_str(),
        "definitionRevision": definition.definition_revision().get(),
        "tickSize": definition.tick_size().as_decimal().normalize().to_string(),
        "lotSize": definition.lot_size().as_decimal().normalize().to_string(),
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
) -> Result<StreamView<'snapshot>, ServiceError> {
    let identity = selected.candidate().identity();
    let mut matches = streams.iter().copied().filter(|view| {
        view.surface_id == identity.observation_id()
            && view.stream.source() == identity.source_id()
            && view.stream.provider_product() == identity.product()
            && view.stream.provider_channel() == identity.feed()
            && Some(view.route.route().venue()) == identity.venue_id()
            && view.route.route().instrument() == identity.instrument_id()
    });
    let selected = matches.next().ok_or(ServiceError::InvalidResult)?;
    if matches.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(selected)
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
        "bidSize": bid.map(|level| decimal_quantity(level.quantity(), definition)).transpose()?,
        "askPrice": ask_price.map(|value| value.normalize().to_string()),
        "askSize": ask.map(|level| decimal_quantity(level.quantity(), definition)).transpose()?,
        "midPrice": midpoint,
        "lastPrice": trade.map(|value| decimal_price(value.price(), definition)).transpose()?.map(|value| value.normalize().to_string()),
        "lastSize": trade.map(|value| decimal_quantity(value.quantity(), definition)).transpose()?,
        "lastSourceTimestamp": trade.and_then(|value| value.source_timestamp()).map(timestamp_value),
        "lastReceivedAt": trade.map(|value| timestamp_value(value.received_at())),
        "lastAvailableAt": trade.map(|value| timestamp_value(value.available_at())),
        "lastQuality": last_quality,
        "lastFreshAtSelection": trade.map(|value| selected_at <= value.qualification_valid_until())
    }))
}

fn empty_quote() -> Value {
    json!({
        "bidPrice": Value::Null,
        "bidSize": Value::Null,
        "askPrice": Value::Null,
        "askSize": Value::Null,
        "midPrice": Value::Null,
        "lastPrice": Value::Null,
        "lastSize": Value::Null,
        "lastSourceTimestamp": Value::Null,
        "lastReceivedAt": Value::Null,
        "lastAvailableAt": Value::Null,
        "lastQuality": Value::Null,
        "lastFreshAtSelection": Value::Null
    })
}

fn order_level_value(
    snapshot: &MarketOrderLevelSnapshot,
    definition: &InstrumentDefinition,
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
            "price": decimal_price(order.price(), definition)?.normalize().to_string(),
            "priceTicks": order.price().get().to_string(),
            "quantity": decimal_quantity(order.quantity(), definition)?,
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
    price
        .checked_to_decimal(definition.tick_size())
        .map_err(|_error| ServiceError::InvalidResult)
}

fn decimal_quantity(
    quantity: market_squawk_domain::QuantityLots,
    definition: &InstrumentDefinition,
) -> Result<String, ServiceError> {
    quantity
        .checked_to_decimal(definition.lot_size())
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
