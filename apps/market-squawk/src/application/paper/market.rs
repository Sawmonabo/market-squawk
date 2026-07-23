//! Bounded current-state Market domain over the paper runtime's live owner.

use std::{cmp::Ordering, fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use market_squawk_domain::{DataQuality, InstrumentId, Timestamp};
use market_squawk_live::{
    LastTradeSnapshot, LiveRuntimeSnapshotLease, RouteSnapshot, ShardSnapshot,
    SnapshotCompleteness, SnapshotDimension, SnapshotReadError, StreamPhaseSnapshot,
    StreamSnapshot,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, ServiceLimits, ToolResultMetadata,
    TypedToolRequest, TypedToolResult,
};
use serde_json::{Value, json};

use super::{PaperController, PaperState, bounded_lock, ensure_live};
use crate::application::{
    ApplicationDomainService, domain_support::encode_hex, effective_service_limits,
};

const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_TRADES: &str = "Market.GetTrades";
const MARKET_GET_QUOTES: &str = "Market.GetQuotes";
const MARKET_GET_BOOKS: &str = "Market.GetBooks";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const MARKET_GET_COMPARISONS: &str = "Market.GetComparisons";
const MAXIMUM_LISTED_EVIDENCE_IDENTITIES: usize = 8;
const CONSERVATIVE_LEVEL_JSON_BYTES: usize = 64;

/// Current-state Market service sharing the sole paper live-runtime owner.
pub(super) struct MarketDomainService {
    controller: Arc<PaperController>,
}

impl MarketDomainService {
    pub(super) const fn new(controller: Arc<PaperController>) -> Self {
        Self { controller }
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
        let filters = MarketFilters::parse(&request)?;
        let reference_at = system_timestamp()?;
        let lease = self.controller.market_snapshots(&context).await?;
        let streams = collect_streams(&lease, &filters, &context)?;
        if streams.is_empty() {
            return Err(ServiceError::NotFound);
        }

        let source_coverage = source_coverage_value(&streams, &filters);
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
            _ => Err(ServiceError::NotFound),
        }?;
        ensure_live(&context)?;
        Ok(output)
    }

    fn begin_shutdown(&self) {
        self.controller.begin_shutdown();
    }

    async fn finish_shutdown(&self, deadline: Instant) -> Result<(), ServiceError> {
        self.controller.finish_shutdown(deadline).await
    }
}

impl PaperController {
    async fn market_snapshots(
        &self,
        context: &RequestContext,
    ) -> Result<LiveRuntimeSnapshotLease, ServiceError> {
        ensure_live(context)?;
        let reader = {
            let state =
                bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
            let PaperState::Running(runtime) = &*state else {
                return Err(ServiceError::Unavailable);
            };
            runtime.snapshots()
        };
        ensure_live(context)?;
        reader.try_load_all().map_err(map_snapshot_read_error)
    }
}

const fn map_snapshot_read_error(error: SnapshotReadError) -> ServiceError {
    match error {
        SnapshotReadError::ReaderLimitReached | SnapshotReadError::CapacityOverflow => {
            ServiceError::ResourceExhausted
        }
        SnapshotReadError::UnknownShard | SnapshotReadError::Closed => ServiceError::Unavailable,
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
                    .is_ok())
    }

    fn matches_time(&self, timestamp: Timestamp) -> bool {
        self.time_range
            .is_none_or(|(start, end)| timestamp >= start && timestamp <= end)
    }
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
    shard: &'snapshot ShardSnapshot,
    route: &'snapshot RouteSnapshot,
    stream: &'snapshot StreamSnapshot,
}

fn collect_streams<'snapshot>(
    lease: &'snapshot LiveRuntimeSnapshotLease,
    filters: &MarketFilters<'_>,
    context: &RequestContext,
) -> Result<Vec<StreamView<'snapshot>>, ServiceError> {
    let mut count = 0_usize;
    for shard in lease.snapshots() {
        require_complete(shard.route_dimension())?;
        for route in shard.routes() {
            require_complete(route.stream_dimension())?;
            count = count
                .checked_add(route.streams().len())
                .ok_or(ServiceError::ResourceExhausted)?;
        }
    }
    let mut streams = Vec::new();
    streams
        .try_reserve_exact(count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for shard in lease.snapshots() {
        ensure_live(context)?;
        for route in shard.routes() {
            for stream in route.streams() {
                let view = StreamView {
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
    streams.sort_unstable_by(compare_streams);
    Ok(streams)
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

fn build_snapshot_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    build_per_stream_result(
        streams,
        filters,
        reference_at,
        source_coverage,
        limits,
        context,
        snapshot_value,
        |view| view.shard.published_at(),
        true,
    )
}

fn build_book_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    build_per_stream_result(
        streams,
        filters,
        reference_at,
        source_coverage,
        limits,
        context,
        |view, levels, at| {
            let mut value = identity_value(view);
            value["asOf"] = Value::String(timestamp_value(view.shard.published_at()));
            value["stateEvaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
            value["book"] = book_value(view.stream, levels);
            value["currentDisplayQuality"] = json!(current_display_quality(view.stream, at));
            value
        },
        |view| view.stream.evaluated_at(),
        true,
    )
}

fn build_quality_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    build_per_stream_result(
        streams,
        filters,
        reference_at,
        source_coverage,
        limits,
        context,
        |view, _levels, at| quality_value(view, at),
        |view| view.stream.evaluated_at(),
        false,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "the bounded result, time, and evidence contracts remain explicit"
)]
fn build_per_stream_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
    encode: impl Fn(&StreamView<'_>, usize, Timestamp) -> Value,
    timestamp: impl Fn(&StreamView<'_>) -> Timestamp,
    includes_levels: bool,
) -> Result<TypedToolResult, ServiceError> {
    let available = streams
        .iter()
        .filter(|view| filters.matches_time(timestamp(view)))
        .count();
    let build_count = available.min(limits.maximum_result_items());
    let level_limit = if includes_levels && build_count > 0 {
        limits
            .maximum_result_bytes()
            .checked_div(CONSERVATIVE_LEVEL_JSON_BYTES)
            .unwrap_or(0)
            .checked_div(build_count)
            .unwrap_or(0)
    } else {
        0
    };
    let mut values = Vec::new();
    values
        .try_reserve_exact(build_count)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut quality = QualitySummary::new(reference_at);
    for view in streams {
        ensure_live(context)?;
        if !filters.matches_time(timestamp(view)) {
            continue;
        }
        quality.observe_stream(view.stream);
        if values.len() < build_count {
            values.push(encode(view, level_limit, reference_at));
        }
    }
    bounded_result(
        &values,
        available,
        with_availability(source_coverage, available),
        quality.into_value(),
        limits,
        context,
    )
}

fn build_trade_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let mut available = 0_usize;
    let mut values = Vec::new();
    values
        .try_reserve_exact(limits.maximum_result_items().min(streams.len()))
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut quality = QualitySummary::new(reference_at);
    for view in streams {
        ensure_live(context)?;
        let Some(trade) = view.stream.last_trade() else {
            continue;
        };
        if !filters.matches_time(trade.available_at()) {
            continue;
        }
        available = available
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        quality.observe_trade(trade);
        if values.len() < limits.maximum_result_items() {
            values.push(trade_value(view, trade, reference_at));
        }
    }
    bounded_result(
        &values,
        available,
        with_availability(source_coverage, available),
        quality.into_value(),
        limits,
        context,
    )
}

fn build_quote_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let mut available = 0_usize;
    let mut values = Vec::new();
    values
        .try_reserve_exact(limits.maximum_result_items().min(streams.len()))
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut quality = QualitySummary::new(reference_at);
    for view in streams {
        ensure_live(context)?;
        if !filters.matches_time(view.stream.evaluated_at()) {
            continue;
        }
        require_observable_top(view.stream)?;
        if view.stream.bids().is_empty() && view.stream.asks().is_empty() {
            continue;
        }
        available = available
            .checked_add(1)
            .ok_or(ServiceError::ResourceExhausted)?;
        quality.observe_stream(view.stream);
        if values.len() < limits.maximum_result_items() {
            values.push(quote_value(view, reference_at));
        }
    }
    bounded_result(
        &values,
        available,
        with_availability(source_coverage, available),
        quality.into_value(),
        limits,
        context,
    )
}

fn build_comparison_result(
    streams: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    source_coverage: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let mut available = 0_usize;
    let mut values = Vec::new();
    values
        .try_reserve_exact(limits.maximum_result_items().min(streams.len()))
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    let mut quality = QualitySummary::new(reference_at);
    let mut start = 0_usize;
    while start < streams.len() {
        ensure_live(context)?;
        let instrument = streams[start].route.route().instrument();
        let mut end = start + 1;
        while end < streams.len() && streams[end].route.route().instrument() == instrument {
            end += 1;
        }
        let candidates = &streams[start..end];
        for view in candidates
            .iter()
            .filter(|view| filters.matches_time(view.stream.evaluated_at()))
        {
            ensure_live(context)?;
            require_observable_top(view.stream)?;
        }
        let comparable = candidates.iter().filter(|view| {
            filters.matches_time(view.stream.evaluated_at())
                && !(view.stream.bids().is_empty() && view.stream.asks().is_empty())
        });
        let count = comparable.clone().count();
        if count > 0 {
            available = available
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            for view in comparable {
                quality.observe_stream(view.stream);
            }
            if values.len() < limits.maximum_result_items() {
                values.push(comparison_value(
                    instrument,
                    candidates,
                    filters,
                    reference_at,
                    count,
                ));
            }
        }
        start = end;
    }
    bounded_result(
        &values,
        available,
        with_availability(source_coverage, available),
        quality.into_value(),
        limits,
        context,
    )
}

fn bounded_result(
    values: &[Value],
    available: usize,
    source_coverage: Value,
    data_quality: Value,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let attempt = |returned: usize| -> Result<Option<TypedToolResult>, ServiceError> {
        ensure_live(context)?;
        let metadata = if returned < available {
            ToolResultMetadata::try_truncated(
                available,
                source_coverage.clone(),
                data_quality.clone(),
            )
        } else {
            ToolResultMetadata::try_complete(source_coverage.clone(), data_quality.clone())
        }
        .map_err(|_error| ServiceError::InvalidResult)?;
        let content = if returned == 0 {
            Value::Null
        } else {
            Value::Array(values[..returned].to_vec())
        };
        let result = TypedToolResult::try_new(content, returned, metadata, limits).ok();
        ensure_live(context)?;
        Ok(result)
    };

    let mut low = 0_usize;
    let mut high = values.len();
    let mut best = None;
    while low <= high {
        let midpoint = low + (high - low) / 2;
        if let Some(result) = attempt(midpoint)? {
            best = Some(result);
            low = midpoint.saturating_add(1);
        } else if midpoint == 0 {
            break;
        } else {
            high = midpoint - 1;
        }
    }
    best.ok_or(ServiceError::ResourceExhausted)
}

fn identity_value(view: &StreamView<'_>) -> Value {
    json!({
        "sourceId": view.stream.source().as_str(),
        "venueId": view.route.route().venue().as_str(),
        "instrumentId": view.route.route().instrument().to_string(),
        "providerProduct": view
            .stream
            .provider_product()
            .as_source_identifier()
            .as_str(),
        "providerChannel": view
            .stream
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "connectionGeneration": view.stream.connection_generation().get(),
        "stateRevision": view.stream.state_revision(),
        "shardId": view.shard.shard_id().to_string(),
        "shardSnapshotRevision": view.shard.snapshot_revision().get()
    })
}

fn snapshot_value(view: &StreamView<'_>, level_limit: usize, reference_at: Timestamp) -> Value {
    let mut value = identity_value(view);
    value["phase"] = json!(view.stream.phase());
    value["lastSequence"] = json!(view.stream.last_sequence().map(|value| value.get()));
    value["snapshotOriginRevision"] = json!(view.stream.snapshot_origin_revision());
    value["snapshotInitialized"] = Value::Bool(view.stream.snapshot_initialized());
    value["generationCurrent"] = Value::Bool(view.stream.generation_current());
    value["healthEpoch"] = Value::from(view.stream.health_epoch());
    value["sourceTimestamp"] = json!(view.stream.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(view.stream.received_at()));
    value["evaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["publishedAt"] = Value::String(timestamp_value(view.shard.published_at()));
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["sourceValidUntil"] = Value::String(timestamp_value(view.stream.source_valid_until()));
    value["freshAtReference"] = Value::Bool(reference_at <= view.stream.source_valid_until());
    value["tradingStatus"] = json!(view.stream.trading_status());
    value["tradingStatusRevision"] = json!(view.stream.trading_status_revision());
    value["book"] = book_value(view.stream, level_limit);
    value["lastTrade"] = view
        .stream
        .last_trade()
        .map(|trade| trade_value(view, trade, reference_at))
        .unwrap_or(Value::Null);
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

fn quality_value(view: &StreamView<'_>, reference_at: Timestamp) -> Value {
    let best_bid = view.stream.bids().first().map(|level| level.price());
    let best_ask = view.stream.asks().first().map(|level| level.price());
    let mut value = identity_value(view);
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["phase"] = json!(view.stream.phase());
    value["generationCurrent"] = Value::Bool(view.stream.generation_current());
    value["snapshotInitialized"] = Value::Bool(view.stream.snapshot_initialized());
    value["lastSequence"] = json!(view.stream.last_sequence().map(|value| value.get()));
    value["sourceTimestamp"] = json!(view.stream.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(view.stream.received_at()));
    value["evaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["sourceValidUntil"] = Value::String(timestamp_value(view.stream.source_valid_until()));
    value["referenceAt"] = Value::String(timestamp_value(reference_at));
    value["freshAtReference"] = Value::Bool(reference_at <= view.stream.source_valid_until());
    value["tradingStatus"] = json!(view.stream.trading_status());
    value["tradingStatusRevision"] = json!(view.stream.trading_status_revision());
    value["stateBidDepth"] = Value::from(view.stream.state_bid_depth());
    value["stateAskDepth"] = Value::from(view.stream.state_ask_depth());
    value["bidDimension"] = dimension_value(view.stream.bid_dimension());
    value["askDimension"] = dimension_value(view.stream.ask_dimension());
    value["crossedBook"] = Value::Bool(best_bid.zip(best_ask).is_some_and(|(bid, ask)| bid >= ask));
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

fn trade_value(view: &StreamView<'_>, trade: &LastTradeSnapshot, reference_at: Timestamp) -> Value {
    let mut value = identity_value(view);
    value["sourceIdentifier"] = Value::String(trade.source_identifier().as_str().to_owned());
    value["stableTradeId"] = Value::String(trade.stable_trade_id().as_str().to_owned());
    value["tradeConnectionGeneration"] = Value::from(trade.connection_generation().get());
    value["priceTicks"] = Value::from(trade.price().get());
    value["quantityLots"] = Value::from(trade.quantity().get());
    value["aggressorSide"] = json!(trade.aggressor_side());
    value["sourceTimestamp"] = json!(trade.source_timestamp().map(timestamp_value));
    value["receivedAt"] = Value::String(timestamp_value(trade.received_at()));
    value["availableAt"] = Value::String(timestamp_value(trade.available_at()));
    value["ingestedAt"] = Value::String(timestamp_value(trade.ingested_at()));
    value["recordedQuality"] = json!(trade.recorded_quality());
    value["currentDisplayQuality"] = json!(current_trade_display_quality(trade, reference_at));
    value["recordedCoverage"] = json!(trade.recorded_coverage());
    value["assessmentId"] = Value::String(trade.assessment_id().as_str().to_owned());
    value["qualificationEvaluatedAt"] =
        Value::String(timestamp_value(trade.qualification_evaluated_at()));
    value["qualificationValidUntil"] =
        Value::String(timestamp_value(trade.qualification_valid_until()));
    value["freshAtReference"] = Value::Bool(reference_at <= trade.qualification_valid_until());
    value["payloadDigest"] = json!({
        "algorithm": trade.payload_digest().algorithm(),
        "bytes": encode_hex(trade.payload_digest().bytes())
    });
    value["bindingDigest"] = Value::String(encode_hex(trade.binding_digest()));
    value["tradeTradingStatus"] = json!(trade.trading_status());
    value["committedStateRevision"] = Value::from(trade.committed_state_revision());
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

fn quote_value(view: &StreamView<'_>, reference_at: Timestamp) -> Value {
    let bid = view.stream.bids().first().copied();
    let ask = view.stream.asks().first().copied();
    let mut value = identity_value(view);
    value["bid"] = bid.map(level_value).unwrap_or(Value::Null);
    value["ask"] = ask.map(level_value).unwrap_or(Value::Null);
    value["sourceTimestamp"] = Value::Null;
    value["asOf"] = Value::String(timestamp_value(view.shard.published_at()));
    value["stateEvaluatedAt"] = Value::String(timestamp_value(view.stream.evaluated_at()));
    value["recordedQuality"] = json!(view.stream.quality());
    value["currentDisplayQuality"] = json!(current_display_quality(view.stream, reference_at));
    value["crossed"] = Value::Bool(
        bid.zip(ask)
            .is_some_and(|(bid, ask)| bid.price() >= ask.price()),
    );
    value["authority"] = Value::String("not_exposed".to_owned());
    value
}

fn comparison_value(
    instrument: InstrumentId,
    candidates: &[StreamView<'_>],
    filters: &MarketFilters<'_>,
    reference_at: Timestamp,
    count: usize,
) -> Value {
    let observations = candidates
        .iter()
        .filter(|view| {
            filters.matches_time(view.stream.evaluated_at())
                && !(view.stream.bids().is_empty() && view.stream.asks().is_empty())
        })
        .map(|view| {
            let bid = view.stream.bids().first().copied();
            let ask = view.stream.asks().first().copied();
            let midpoint = bid.zip(ask).map(|(bid, ask)| {
                json!({
                    "numeratorTicks": (
                        i128::from(bid.price().get()) + i128::from(ask.price().get())
                    ).to_string(),
                    "denominator": 2
                })
            });
            json!({
                "sourceId": view.stream.source().as_str(),
                "venueId": view.route.route().venue().as_str(),
                "providerProduct": view
                    .stream
                    .provider_product()
                    .as_source_identifier()
                    .as_str(),
                "providerChannel": view
                    .stream
                    .provider_channel()
                    .as_source_identifier()
                    .as_str(),
                "bid": bid.map(level_value),
                "ask": ask.map(level_value),
                "midpoint": midpoint,
                "asOf": timestamp_value(view.shard.published_at()),
                "stateEvaluatedAt": timestamp_value(view.stream.evaluated_at()),
                "recordedQuality": view.stream.quality(),
                "currentDisplayQuality": current_display_quality(view.stream, reference_at)
            })
        })
        .collect::<Vec<_>>();
    json!({
        "instrumentId": instrument.to_string(),
        "observationCount": count,
        "comparable": count >= 2,
        "observations": observations,
        "authority": "not_exposed"
    })
}

fn book_value(stream: &StreamSnapshot, level_limit: usize) -> Value {
    let (bid_count, ask_count) =
        balanced_level_counts(stream.bids().len(), stream.asks().len(), level_limit);
    let bids = stream.bids()[..bid_count]
        .iter()
        .copied()
        .map(level_value)
        .collect::<Vec<_>>();
    let asks = stream.asks()[..ask_count]
        .iter()
        .copied()
        .map(level_value)
        .collect::<Vec<_>>();
    json!({
        "configuredDepth": stream.configured_depth(),
        "stateBidDepth": stream.state_bid_depth(),
        "stateAskDepth": stream.state_ask_depth(),
        "snapshotBidDimension": dimension_value(stream.bid_dimension()),
        "snapshotAskDimension": dimension_value(stream.ask_dimension()),
        "resultBidDimension": count_dimension(stream.bids().len(), bid_count, level_limit),
        "resultAskDimension": count_dimension(stream.asks().len(), ask_count, level_limit),
        "bids": bids,
        "asks": asks
    })
}

fn balanced_level_counts(bids: usize, asks: usize, limit: usize) -> (usize, usize) {
    let mut bid_count = bids.min(limit.saturating_add(1) / 2);
    let mut ask_count = asks.min(limit.saturating_sub(bid_count));
    bid_count = bids.min(limit.saturating_sub(ask_count));
    ask_count = asks.min(limit.saturating_sub(bid_count));
    (bid_count, ask_count)
}

fn count_dimension(available: usize, returned: usize, configured_limit: usize) -> Value {
    let completeness = if returned == available {
        "complete"
    } else if returned == 0 {
        "unavailable"
    } else {
        "truncated"
    };
    json!({
        "completeness": completeness,
        "available": available,
        "returned": returned,
        "configuredLimit": configured_limit
    })
}

fn dimension_value(dimension: &SnapshotDimension) -> Value {
    json!({
        "completeness": dimension.completeness(),
        "available": dimension.available(),
        "returned": dimension.returned(),
        "configuredLimit": dimension.configured_limit()
    })
}

fn level_value(level: market_squawk_live::BookLevelSnapshot) -> Value {
    json!({
        "priceTicks": level.price().get(),
        "quantityLots": level.quantity().get()
    })
}

fn source_coverage_value(streams: &[StreamView<'_>], filters: &MarketFilters<'_>) -> Value {
    let mut sources = streams
        .iter()
        .map(|view| view.stream.source().as_str())
        .collect::<Vec<_>>();
    sources.sort_unstable();
    sources.dedup();
    let source_count = sources.len();
    sources.truncate(MAXIMUM_LISTED_EVIDENCE_IDENTITIES);

    let mut venues = streams
        .iter()
        .map(|view| view.route.route().venue().as_str())
        .collect::<Vec<_>>();
    venues.sort_unstable();
    venues.dedup();
    let venue_count = venues.len();
    venues.truncate(MAXIMUM_LISTED_EVIDENCE_IDENTITIES);

    json!({
        "mode": "current_live_runtime",
        "consistency": "per_shard_current_non_atomic",
        "historicalDataset": Value::Null,
        "requestedSourceCount": filters.sources.len(),
        "listedRequestedSources": filters
            .sources
            .iter()
            .take(MAXIMUM_LISTED_EVIDENCE_IDENTITIES)
            .copied()
            .collect::<Vec<_>>(),
        "listedRequestedSourcesComplete":
            filters.sources.len() <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "observedSourceCount": source_count,
        "listedSources": sources,
        "listedSourcesComplete": source_count <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "observedVenueCount": venue_count,
        "listedVenues": venues,
        "listedVenuesComplete": venue_count <= MAXIMUM_LISTED_EVIDENCE_IDENTITIES,
        "streamIdentityScope": "complete",
        "bookDepthScope": "per_record_explicit"
    })
}

fn with_availability(mut coverage: Value, available: usize) -> Value {
    coverage["availability"] = Value::String(if available == 0 {
        "no_current_observation".to_owned()
    } else {
        "current".to_owned()
    });
    coverage
}

struct QualitySummary {
    reference_at: Timestamp,
    recorded: [usize; 9],
    current: [usize; 9],
    fresh: usize,
    stale: usize,
}

impl QualitySummary {
    const fn new(reference_at: Timestamp) -> Self {
        Self {
            reference_at,
            recorded: [0; 9],
            current: [0; 9],
            fresh: 0,
            stale: 0,
        }
    }

    fn observe_stream(&mut self, stream: &StreamSnapshot) {
        let current = current_display_quality(stream, self.reference_at);
        self.recorded[quality_index(stream.quality())] += 1;
        self.current[quality_index(current)] += 1;
        if self.reference_at <= stream.source_valid_until() {
            self.fresh += 1;
        } else {
            self.stale += 1;
        }
    }

    fn observe_trade(&mut self, trade: &LastTradeSnapshot) {
        let current = current_trade_display_quality(trade, self.reference_at);
        self.recorded[quality_index(trade.recorded_quality())] += 1;
        self.current[quality_index(current)] += 1;
        if self.reference_at <= trade.qualification_valid_until() {
            self.fresh += 1;
        } else {
            self.stale += 1;
        }
    }

    fn into_value(self) -> Value {
        json!({
            "referenceAt": timestamp_value(self.reference_at),
            "recordedClassifications": quality_counts(&self.recorded),
            "currentDisplayClassifications": quality_counts(&self.current),
            "freshObservations": self.fresh,
            "staleObservations": self.stale,
            "authority": "not_exposed"
        })
    }
}

fn quality_counts(counts: &[usize; 9]) -> Value {
    const QUALITIES: [DataQuality; 9] = [
        DataQuality::DirectVerified,
        DataQuality::DirectUnverified,
        DataQuality::OfficialDelayed,
        DataQuality::Aggregated,
        DataQuality::Indicative,
        DataQuality::Modeled,
        DataQuality::Estimated,
        DataQuality::Stale,
        DataQuality::Quarantined,
    ];
    Value::Array(
        QUALITIES
            .into_iter()
            .zip(counts)
            .filter(|(_, count)| **count > 0)
            .map(|(quality, count)| json!({"quality": quality, "count": count}))
            .collect(),
    )
}

const fn quality_index(quality: DataQuality) -> usize {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

fn require_observable_top(stream: &StreamSnapshot) -> Result<(), ServiceError> {
    if [stream.bid_dimension(), stream.ask_dimension()]
        .into_iter()
        .any(|dimension| dimension.available() > 0 && dimension.returned() == 0)
    {
        Err(ServiceError::Unavailable)
    } else {
        Ok(())
    }
}

fn current_trade_display_quality(
    trade: &LastTradeSnapshot,
    reference_at: Timestamp,
) -> DataQuality {
    if reference_at <= trade.qualification_valid_until() {
        trade.recorded_quality()
    } else {
        DataQuality::Stale
    }
}

fn current_display_quality(stream: &StreamSnapshot, reference_at: Timestamp) -> DataQuality {
    if !stream.generation_current()
        || stream.phase() != StreamPhaseSnapshot::Healthy
        || stream.quality() == DataQuality::Quarantined
    {
        DataQuality::Quarantined
    } else if reference_at > stream.source_valid_until() {
        DataQuality::Stale
    } else {
        stream.quality()
    }
}

fn timestamp_value(timestamp: Timestamp) -> String {
    DateTime::<Utc>::from_timestamp_nanos(timestamp.unix_nanos())
        .to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}
