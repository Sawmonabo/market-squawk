//! Bounded current-state Market domain over the paper runtime's live owner.

mod results;
mod serialization;

use std::{cmp::Ordering, fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use market_squawk_domain::{InstrumentId, Timestamp};
use market_squawk_live::{
    LiveRuntimeSnapshotLease, RouteSnapshot, ShardSnapshot, SnapshotCompleteness,
    SnapshotDimension, SnapshotReadError, StreamSnapshot,
};
use market_squawk_services::{
    RequestContext, ServiceDomain, ServiceError, TypedToolRequest, TypedToolResult,
};
use serde_json::Value;

use super::{PaperController, PaperState, bounded_lock, ensure_live};
use crate::application::{ApplicationDomainService, effective_service_limits};
use results::{
    build_book_result, build_comparison_result, build_quality_result, build_quote_result,
    build_snapshot_result, build_trade_result,
};
use serialization::source_coverage_value;

const MARKET_GET_SNAPSHOT: &str = "Market.GetSnapshot";
const MARKET_GET_TRADES: &str = "Market.GetTrades";
const MARKET_GET_QUOTES: &str = "Market.GetQuotes";
const MARKET_GET_BOOKS: &str = "Market.GetBooks";
const MARKET_GET_QUALITY: &str = "Market.GetQuality";
const MARKET_GET_COMPARISONS: &str = "Market.GetComparisons";

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
        let streams = lease
            .as_ref()
            .map(|lease| collect_streams(lease, &filters, &context))
            .transpose()?
            .unwrap_or_default();

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
    ) -> Result<Option<LiveRuntimeSnapshotLease>, ServiceError> {
        ensure_live(context)?;
        let reader = {
            let state =
                bounded_lock(&self.state, context.deadline(), context.cancellation()).await?;
            match &*state {
                PaperState::LiveOnly {
                    runtime,
                    exports,
                    cancellation,
                    ..
                } => {
                    if cancellation.is_cancelled() || !runtime.is_healthy() || !exports.is_healthy()
                    {
                        return Ok(None);
                    }
                    runtime.snapshots()
                }
                PaperState::Running {
                    runtime,
                    exports,
                    cancellation,
                    ..
                } => {
                    if cancellation.is_cancelled()
                        || !runtime.source_is_healthy()
                        || !exports.is_healthy()
                    {
                        return Ok(None);
                    }
                    runtime.snapshots()
                }
                PaperState::Stopped { .. } => return Ok(None),
                PaperState::LiveStarting { .. }
                | PaperState::Starting { .. }
                | PaperState::Stopping => return Err(ServiceError::Unavailable),
            }
        };
        ensure_live(context)?;
        reader
            .try_load_all()
            .map(Some)
            .map_err(map_snapshot_read_error)
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

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    Utc::now()
        .timestamp_nanos_opt()
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}
