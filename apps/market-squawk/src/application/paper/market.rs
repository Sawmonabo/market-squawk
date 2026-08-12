//! Bounded current-state Market domain over the paper runtime's live owner.

mod candidate;
mod results;
mod serialization;
mod unified;

use std::{cmp::Ordering, fmt, num::NonZeroUsize, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use market_squawk_data::{InstrumentDefinitionReadCapability, MarketDataInstrumentReadCapability};
use market_squawk_domain::{
    AssetClass, CoverageDelay, DataQuality, EvidenceDigest, InstrumentDefinition, InstrumentId,
    LiveEventClass, MarketDataInstrumentDefinition, MarketDepth, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_live::{
    RouteSnapshot, ShardSnapshot, SnapshotCompleteness, SnapshotDimension, StreamSnapshot,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, TypedToolRequest, TypedToolResult,
};
use market_squawk_sources::SourceMetadata;
use serde_json::Value;

use super::ensure_live;
use crate::application::market_runtime::{
    MarketDisplaySnapshotBatch, MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease,
    MarketOrderLevelSnapshot, MarketRuntimeRegistry, MarketRuntimeSnapshotBatch,
};
use crate::application::market_selection::{MarketOperation, MarketOperationSet};
use crate::application::{ApplicationDomainService, effective_service_limits};
pub(super) use candidate::ProductionPortfolioCandidateResolutionFactory;
use results::{
    build_book_result, build_comparison_result, build_quality_result, build_quote_result,
    build_snapshot_result, build_trade_result,
};
use serialization::{source_coverage_value, timestamp_value};
use unified::{
    MarketSurfaceRightsPolicy, MarketSurfaceSelectionPolicy, build_unified_market_result,
};

const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_TRADES: &str = "Market.GetTrades";
const MARKET_GET_QUOTES: &str = "Market.GetQuotes";
const MARKET_GET_BOOKS: &str = "Market.GetBooks";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const MARKET_GET_COMPARISONS: &str = "Market.GetComparisons";
const MARKET_GET_UNIFIED_FEED: &str = "Market.GetUnifiedFeed";
const MARKET_SEARCH_UNIVERSE: &str = "Market.SearchUniverse";
const MAXIMUM_UNIFIED_MARKET_INSTRUMENTS: usize = 4_096;
const MAXIMUM_UNIFIED_DISPLAY_SOURCES_PER_INSTRUMENT: usize = 256;
const MAXIMUM_UNIFIED_ORDER_SAMPLE: usize = 64;
const MAXIMUM_REFERENCE_SEARCH_ROWS: usize = 100;

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
}

impl MarketDomainService {
    pub(super) fn new(
        registry: Arc<MarketRuntimeRegistry>,
        instrument_definitions: InstrumentDefinitionReadCapability,
        market_data_instruments: MarketDataInstrumentReadCapability,
        reference_search: Arc<dyn MarketReferenceSearchAuthority>,
    ) -> Self {
        Self {
            registry,
            instrument_definitions,
            market_data_instruments,
            reference_search,
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
        let limits = effective_service_limits(&request, &context)?;
        if request.name() == MARKET_SEARCH_UNIVERSE {
            let query = request
                .arguments()
                .get("query")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let maximum_rows = limits
                .maximum_result_items()
                .min(MAXIMUM_REFERENCE_SEARCH_ROWS);
            let page = self
                .reference_search
                .search(
                    query,
                    maximum_rows,
                    context.deadline(),
                    context.cancellation(),
                )
                .await?;
            return build_reference_search_result(page, limits, &context);
        }
        let filters = MarketFilters::parse(&request)?;
        let reference_at = system_timestamp()?;
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
                let display_instrument_ids =
                    load_display_instrument_ids(self.registry.as_ref(), &filters, &context).await?;
                let market_instrument_ids =
                    load_market_instrument_ids(self.registry.as_ref(), &filters, &context).await?;
                let display_batches = load_display_snapshots(
                    self.registry.as_ref(),
                    &display_instrument_ids,
                    reference_at,
                    &context,
                )
                .await?;
                let display_snapshots = display_snapshot_refs(&display_batches, &filters)?;
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
                    &context,
                )?;
                let market_data_definitions = load_market_data_instrument_definitions(
                    &self.market_data_instruments,
                    &display_instrument_ids,
                    &context,
                )?;
                let order_level = load_order_level_snapshots(
                    self.registry.as_ref(),
                    &streams,
                    &kraken_price_projections,
                    &context,
                )
                .await?;
                let surface_policies = build_surface_policies(
                    &snapshots,
                    &display_snapshots,
                    &kraken_projection_refs,
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
                    &market_data_definitions,
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
    use crate::application::domain_support::encode_hex;

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
        let quality = match record.quality {
            DataQuality::OfficialDelayed => "official_delayed",
            _ => return Err(ServiceError::InvalidResult),
        };
        values.push(serde_json::json!({
            "referenceId": record.reference_id.as_str(),
            "symbol": record.symbol,
            "name": record.security_name.trim(),
            "venueId": record.venue_id.as_str(),
            "assetClass": match record.asset_class {
                AssetClass::Equity => "equity",
                AssetClass::Fund => "fund",
                _ => return Err(ServiceError::InvalidResult),
            },
            "referenceOnly": true,
            "isEtf": record.is_etf,
            "roundLotSize": record.round_lot_size,
            "directoryPresence": "current_directory",
            "quality": quality,
            "effectiveAt": timestamp_value(record.effective_at),
            "availableAt": timestamp_value(record.available_at),
            "sourceId": record.source_id.as_str(),
            "providerId": record.provider_id.as_str(),
            "sourcePayloadSha256": encode_hex(record.source_payload_digest.bytes()),
            "matchKind": record.match_kind.as_str(),
            "quoteAvailability": "account_required",
        }));
    }
    results::bounded_result(
        &values,
        available,
        serde_json::json!({
            "complete": !has_more,
            "referenceOnly": true,
            "provider": "nasdaq-trader-symbol-directory",
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
    context: &RequestContext,
) -> Result<Vec<InstrumentDefinition>, ServiceError> {
    let mut instrument_ids = Vec::new();
    instrument_ids
        .try_reserve_exact(
            streams
                .len()
                .checked_add(kraken.len())
                .ok_or(ServiceError::ResourceExhausted)?,
        )
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    instrument_ids.extend(streams.iter().map(|view| view.route.route().instrument()));
    instrument_ids.extend(kraken.iter().map(|snapshot| snapshot.key().instrument_id()));
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
    reference_at: Timestamp,
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
        batches.push(
            registry
                .display_snapshots_for_instrument(
                    *instrument_id,
                    maximum_sources,
                    reference_at,
                    context.deadline(),
                    context.cancellation(),
                )
                .await?,
        );
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

fn load_market_data_instrument_definitions(
    reader: &MarketDataInstrumentReadCapability,
    instrument_ids: &[InstrumentId],
    context: &RequestContext,
) -> Result<Vec<MarketDataInstrumentDefinition>, ServiceError> {
    let mut definitions = Vec::new();
    definitions
        .try_reserve_exact(instrument_ids.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for instrument_id in instrument_ids {
        ensure_live(context)?;
        let record = reader
            .latest(*instrument_id, context.deadline(), context.cancellation())
            .map_err(|error| {
                tracing::error!(%error, "unified Markets market-data definition read failed");
                ServiceError::Unavailable
            })?
            .ok_or(ServiceError::Unavailable)?;
        definitions.push(record.definition().clone());
    }
    if definitions
        .windows(2)
        .any(|pair| pair[0].instrument_id() >= pair[1].instrument_id())
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(definitions)
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
            .order_level_snapshot(
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
            .order_level_snapshot(
                key.source_id(),
                key.venue_id(),
                key.instrument_id(),
                key.generation(),
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
