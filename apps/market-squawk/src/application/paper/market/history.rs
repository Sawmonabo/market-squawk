//! Provider-neutral immutable history projection for ordinary Market consumers.

use market_squawk_domain::{DataQuality, InstrumentId};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::{ensure_live, serialization::timestamp_value, system_timestamp};
use crate::application::research::{
    LatestMarketHistoryReadRequest, MarketHistoryAdjustmentPolicy, MarketHistoryBar,
    MarketHistoryMissingReason, MarketHistoryPartialReason, MarketHistoryQuality,
    MarketHistoryReadCapability, MarketHistoryReadLimit, MarketHistoryReadOutcome,
    MarketHistorySeries, MarketHistorySessionPolicy, MarketHistoryTimeframe,
    MarketHistoryUnavailableReason,
};

const PRODUCT_PERIOD: &str = "daily";
const PRODUCT_RANGE: &str = "latest_complete_window";
const PRODUCT_SESSION: &str = "completed_trading_sessions";
const PRODUCT_ADJUSTMENT: &str = "fully_adjusted";
const MAXIMUM_PRODUCT_HISTORY_BARS: usize = 1_000;

/// Reads one opaque-token-resolved investment without returning its canonical identity.
pub(super) async fn build_product_market_history_result(
    reader: &MarketHistoryReadCapability,
    instrument_id: InstrumentId,
    history_token: &str,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let maximum_bars = limits
        .maximum_result_items()
        .min(MAXIMUM_PRODUCT_HISTORY_BARS);
    let limit = u32::try_from(maximum_bars)
        .ok()
        .and_then(|value| MarketHistoryReadLimit::try_new(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let outcome = reader
        .read_latest(
            LatestMarketHistoryReadRequest::new(
                instrument_id,
                MarketHistoryTimeframe::Daily,
                MarketHistorySessionPolicy::CompletedTradingSessions,
                MarketHistoryAdjustmentPolicy::FullyAdjusted,
                system_timestamp()?,
                limit,
            ),
            context.deadline(),
            context.cancellation().clone(),
        )
        .await;
    ensure_live(context)?;
    match outcome {
        MarketHistoryReadOutcome::Complete(series) => {
            product_series_result(series, history_token, false, limits, context)
        }
        MarketHistoryReadOutcome::Partial { series, .. } => {
            product_series_result(series, history_token, true, limits, context)
        }
        MarketHistoryReadOutcome::Missing(_) => {
            product_unavailable_result("not_available", limits, context)
        }
        MarketHistoryReadOutcome::Unavailable(reason) => product_unavailable_result(
            match reason {
                MarketHistoryUnavailableReason::Cancelled
                | MarketHistoryUnavailableReason::DeadlineExceeded
                | MarketHistoryUnavailableReason::CapacityExceeded
                | MarketHistoryUnavailableReason::StorageUnavailable => "temporarily_unavailable",
                MarketHistoryUnavailableReason::IntegrityUnproven => "not_available",
            },
            limits,
            context,
        ),
    }
}

fn product_series_result(
    series: MarketHistorySeries,
    history_token: &str,
    partial: bool,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    validate_series(&series)?;
    let mut bars = Vec::new();
    bars.try_reserve_exact(series.bars().len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for bar in series.bars() {
        bars.push(json!({
            "startsAt": timestamp_value(bar.period_start()),
            "endsAt": timestamp_value(bar.period_end_exclusive()),
                "open": bar.open().amount().normalize().to_string(),
                "high": bar.high().amount().normalize().to_string(),
                "low": bar.low().amount().normalize().to_string(),
                "close": bar.close().amount().normalize().to_string(),
                "volume": bar.volume().normalize().to_string(),
        }));
    }
    let count = bars.len();
    let content = json!({
        "data": {
            "historyToken": history_token,
            "currency": series.currency().as_str(),
            "bars": bars,
            "partial": partial,
        },
        "unavailableReason": Value::Null,
    });
    let metadata = if partial {
        ToolResultMetadata::try_truncated(
            series.coverage().materialized_bars(),
            json!({"availability": "available"}),
            json!({"quality": "verified"}),
        )
    } else {
        ToolResultMetadata::try_complete(
            json!({"availability": "available"}),
            json!({"quality": "verified"}),
        )
    }
    .map_err(|_error| ServiceError::InvalidResult)?;
    let result = TypedToolResult::try_new(content, count, metadata, limits)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    ensure_live(context)?;
    Ok(result)
}

fn product_unavailable_result(
    reason: &'static str,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let metadata = ToolResultMetadata::try_complete(
        json!({"availability": "unavailable"}),
        json!({"quality": "unavailable"}),
    )
    .map_err(|_error| ServiceError::InvalidResult)?;
    let result = TypedToolResult::try_new(
        json!({"data": Value::Null, "unavailableReason": reason}),
        0,
        metadata,
        limits,
    )
    .map_err(|_error| ServiceError::ResourceExhausted)?;
    ensure_live(context)?;
    Ok(result)
}

pub(super) async fn build_market_history_result(
    reader: &MarketHistoryReadCapability,
    request: &TypedToolRequest,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let instrument_id = parse_request(request)?;
    let limit = u32::try_from(limits.maximum_result_items())
        .ok()
        .and_then(|value| MarketHistoryReadLimit::try_new(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let cutoff = system_timestamp()?;
    let outcome = reader
        .read_latest(
            LatestMarketHistoryReadRequest::new(
                instrument_id,
                MarketHistoryTimeframe::Daily,
                MarketHistorySessionPolicy::CompletedTradingSessions,
                MarketHistoryAdjustmentPolicy::FullyAdjusted,
                cutoff,
                limit,
            ),
            context.deadline(),
            context.cancellation().clone(),
        )
        .await;
    ensure_live(context)?;
    match outcome {
        MarketHistoryReadOutcome::Complete(series) => {
            series_result(series, "complete", None, limits, context)
        }
        MarketHistoryReadOutcome::Partial { series, reason } => {
            let reason = match reason {
                MarketHistoryPartialReason::OutputLimit => "result_limit",
            };
            series_result(series, "partial", Some(reason), limits, context)
        }
        MarketHistoryReadOutcome::Missing(reason) => status_result(
            instrument_id,
            "missing",
            missing_reason(reason),
            "missing",
            limits,
            context,
        ),
        MarketHistoryReadOutcome::Unavailable(reason) => status_result(
            instrument_id,
            "unavailable",
            unavailable_reason(reason),
            "unavailable",
            limits,
            context,
        ),
    }
}

fn parse_request(request: &TypedToolRequest) -> Result<InstrumentId, ServiceError> {
    let instruments = request
        .arguments()
        .get("instrumentIds")
        .and_then(Value::as_array)
        .filter(|values| values.len() == 1)
        .ok_or(ServiceError::InvalidRequest)?;
    let instrument_id = instruments[0]
        .as_str()
        .ok_or(ServiceError::InvalidRequest)?
        .parse()
        .map_err(|_error| ServiceError::InvalidRequest)?;
    if request.arguments().get("period").and_then(Value::as_str) != Some(PRODUCT_PERIOD)
        || request.arguments().get("range").and_then(Value::as_str) != Some(PRODUCT_RANGE)
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(instrument_id)
}

fn series_result(
    series: MarketHistorySeries,
    kind: &'static str,
    reason: Option<&'static str>,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    validate_series(&series)?;
    let coverage = series.coverage();
    if kind == "partial" && coverage.materialized_bars() <= coverage.returned_bars() {
        return Err(ServiceError::InvalidResult);
    }
    let quality = quality_value(series.quality())?;
    let mut content = json!({
        "kind": kind,
        "instrumentId": series.instrument_id().to_string(),
        "period": PRODUCT_PERIOD,
        "range": PRODUCT_RANGE,
        "session": PRODUCT_SESSION,
        "adjustment": PRODUCT_ADJUSTMENT,
        "currency": series.currency().as_str(),
        "coverage": {
            "selectedStart": timestamp_value(coverage.requested().start()),
            "selectedEndExclusive": timestamp_value(coverage.requested().end_exclusive()),
            "materializedStart": timestamp_value(coverage.materialized().start()),
            "materializedEndExclusive": timestamp_value(
                coverage.materialized().end_exclusive()
            ),
            "returnedStart": timestamp_value(coverage.returned().start()),
            "returnedEndExclusive": timestamp_value(coverage.returned().end_exclusive()),
            "materializedBars": coverage.materialized_bars(),
            "returnedBars": coverage.returned_bars(),
        },
        "quality": quality,
        "bars": series.bars().iter().map(bar_value).collect::<Vec<_>>(),
    });
    if let Some(reason) = reason {
        content["reason"] = Value::String(reason.to_owned());
    }
    let coverage_metadata = json!({
        "availability": "available",
        "completeTradingSessions": true,
        "materializedBars": coverage.materialized_bars(),
        "returnedBars": coverage.returned_bars(),
    });
    let metadata = if kind == "partial" {
        ToolResultMetadata::try_truncated(
            coverage.materialized_bars(),
            coverage_metadata,
            quality_value(series.quality())?,
        )
    } else {
        ToolResultMetadata::try_complete(coverage_metadata, quality_value(series.quality())?)
    }
    .map_err(|_error| ServiceError::InvalidResult)?;
    let result = TypedToolResult::try_new(content, coverage.returned_bars(), metadata, limits)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    ensure_live(context)?;
    Ok(result)
}

fn status_result(
    instrument_id: InstrumentId,
    kind: &'static str,
    reason: &'static str,
    availability: &'static str,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let content = json!({
        "kind": kind,
        "instrumentId": instrument_id.to_string(),
        "period": PRODUCT_PERIOD,
        "range": PRODUCT_RANGE,
        "session": PRODUCT_SESSION,
        "adjustment": PRODUCT_ADJUSTMENT,
        "reason": reason,
    });
    let metadata = ToolResultMetadata::try_complete(
        json!({"availability": availability}),
        json!({
            "charts": false,
            "currentResearch": false,
            "pointInTimeBacktests": false,
            "retrospectiveTraining": false,
        }),
    )
    .map_err(|_error| ServiceError::InvalidResult)?;
    let result = TypedToolResult::try_new(content, 0, metadata, limits)
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    ensure_live(context)?;
    Ok(result)
}

fn validate_series(series: &MarketHistorySeries) -> Result<(), ServiceError> {
    let coverage = series.coverage();
    let quality = series.quality();
    if series.timeframe() != MarketHistoryTimeframe::Daily
        || series.session() != MarketHistorySessionPolicy::CompletedTradingSessions
        || series.adjustment() != MarketHistoryAdjustmentPolicy::FullyAdjusted
        || series.bars().is_empty()
        || coverage.returned_bars() != series.bars().len()
        || coverage.materialized_bars() < coverage.returned_bars()
        || !quality.complete_trading_sessions()
        || !quality.current_research_eligible()
        || quality.point_in_time_backtest_eligible()
        || quality.retrospective_training_eligible()
        || quality.observation_quality() == DataQuality::Quarantined
        || series.bars().windows(2).any(|pair| {
            pair[0].period_start() >= pair[1].period_start()
                || pair[0].period_end_exclusive() > pair[1].period_start()
        })
        || series.bars().iter().any(|bar| {
            bar.period_start() >= bar.period_end_exclusive()
                || bar.open().currency() != series.currency()
                || bar.high().currency() != series.currency()
                || bar.low().currency() != series.currency()
                || bar.close().currency() != series.currency()
                || bar
                    .vwap()
                    .is_some_and(|value| value.currency() != series.currency())
        })
    {
        return Err(ServiceError::InvalidResult);
    }
    let first = series.bars().first().ok_or(ServiceError::InvalidResult)?;
    let last = series.bars().last().ok_or(ServiceError::InvalidResult)?;
    if coverage.returned().start() != first.period_start()
        || coverage.returned().end_exclusive() != last.period_end_exclusive()
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn bar_value(bar: &MarketHistoryBar) -> Value {
    json!({
        "periodStart": timestamp_value(bar.period_start()),
        "periodEndExclusive": timestamp_value(bar.period_end_exclusive()),
        "open": decimal_text(bar.open().amount()),
        "high": decimal_text(bar.high().amount()),
        "low": decimal_text(bar.low().amount()),
        "close": decimal_text(bar.close().amount()),
        "volume": decimal_text(bar.volume()),
        "tradeCount": bar.trade_count(),
        "vwap": bar.vwap().map(|value| decimal_text(value.amount())),
    })
}

fn decimal_text(value: Decimal) -> String {
    value.normalize().to_string()
}

fn quality_value(quality: MarketHistoryQuality) -> Result<Value, ServiceError> {
    let confidence = match quality.observation_quality() {
        DataQuality::DirectVerified => "high",
        DataQuality::DirectUnverified | DataQuality::OfficialDelayed | DataQuality::Aggregated => {
            "moderate"
        }
        DataQuality::Indicative | DataQuality::Modeled | DataQuality::Estimated => "limited",
        DataQuality::Stale => "stale",
        DataQuality::Quarantined => return Err(ServiceError::InvalidResult),
    };
    Ok(json!({
        "confidence": confidence,
        "completeTradingSessions": quality.complete_trading_sessions(),
        "use": {
            "charts": true,
            "currentResearch": quality.current_research_eligible(),
            "pointInTimeBacktests": quality.point_in_time_backtest_eligible(),
            "retrospectiveTraining": quality.retrospective_training_eligible(),
        }
    }))
}

const fn missing_reason(reason: MarketHistoryMissingReason) -> &'static str {
    match reason {
        MarketHistoryMissingReason::PolicyNotMaterialized => "requested_history_not_available",
        MarketHistoryMissingReason::NoCompleteWindowAtKnowledgeCutoff => "no_complete_history",
    }
}

const fn unavailable_reason(reason: MarketHistoryUnavailableReason) -> &'static str {
    match reason {
        MarketHistoryUnavailableReason::Cancelled => "request_cancelled",
        MarketHistoryUnavailableReason::DeadlineExceeded
        | MarketHistoryUnavailableReason::CapacityExceeded
        | MarketHistoryUnavailableReason::StorageUnavailable => "temporarily_unavailable",
        MarketHistoryUnavailableReason::IntegrityUnproven => "history_could_not_be_verified",
    }
}
