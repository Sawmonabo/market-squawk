//! Bounded current-state Market result construction.

use market_squawk_domain::Timestamp;
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolResult,
};
use serde_json::{Value, json};

use super::serialization::{
    QualitySummary, book_value, comparison_value, current_display_quality, identity_value,
    quality_value, quote_value, require_observable_top, snapshot_value, timestamp_value,
    trade_value, with_availability,
};
use super::{MarketFilters, StreamView, ensure_live};

const CONSERVATIVE_LEVEL_JSON_BYTES: usize = 64;

pub(super) fn build_snapshot_result(
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

pub(super) fn build_book_result(
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

pub(super) fn build_quality_result(
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

pub(super) fn build_trade_result(
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

pub(super) fn build_quote_result(
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

pub(super) fn build_comparison_result(
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
