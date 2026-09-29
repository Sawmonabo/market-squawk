//! Provider-neutral immutable history projection for ordinary Market consumers.

use market_squawk_domain::{BarTimeSemantics, CalendarDate, DataQuality, InstrumentId};
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::{ensure_live, serialization::timestamp_value, system_timestamp};
use crate::application::research::{
    LatestMarketHistoryReadRequest, MarketHistoryAdjustmentPolicy, MarketHistoryBar,
    MarketHistoryInterval, MarketHistoryMissingReason, MarketHistoryPartialReason,
    MarketHistoryQuality, MarketHistoryReadCapability, MarketHistoryReadLimit,
    MarketHistoryReadOutcome, MarketHistorySeries, MarketHistorySessionPolicy,
    MarketHistoryTimeframe, MarketHistoryUnavailableReason, MarketHistoryViewport,
};

const PRODUCT_PERIOD: &str = "daily";
const PRODUCT_RANGE: &str = "latest_complete_window";
const PRODUCT_SESSION: &str = "completed_trading_sessions";
const PRODUCT_ADJUSTMENT: &str = "fully_adjusted";

/// Reads one opaque-token-resolved investment without returning its canonical identity.
pub(super) async fn build_product_market_history_result(
    reader: &MarketHistoryReadCapability,
    research: &crate::ResearchService,
    instrument_id: InstrumentId,
    history_token: &str,
    request: &TypedToolRequest,
    limits: ServiceLimits,
    context: &RequestContext,
) -> Result<TypedToolResult, ServiceError> {
    let viewport = product_viewport(request)?;
    let limit = u32::try_from(viewport.point_limit)
        .ok()
        .and_then(|value| MarketHistoryReadLimit::try_new(value).ok())
        .ok_or(ServiceError::InvalidRequest)?;
    let data = reader
        .read_latest_viewport(
            research,
            LatestMarketHistoryReadRequest::new(
                instrument_id,
                MarketHistoryTimeframe::Daily,
                MarketHistorySessionPolicy::CompletedTradingSessions,
                MarketHistoryAdjustmentPolicy::FullyAdjusted,
                system_timestamp()?,
                limit,
            ),
            viewport,
            context,
        )
        .await?;
    ensure_live(context)?;
    let Some(mut data) = data else {
        return product_unavailable_result("not_available", limits, context);
    };
    let bars = data
        .get("bars")
        .and_then(Value::as_array)
        .ok_or(ServiceError::InvalidResult)?;
    let count = bars.len();
    let generation = data
        .get("generationToken")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)?
        .to_owned();
    let display_digest = data
        .get("display")
        .and_then(|display| display.get("projectionDigest"))
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)?
        .to_owned();
    data.as_object_mut()
        .ok_or(ServiceError::InvalidResult)?
        .insert("historyToken".to_owned(), json!(history_token));
    let metadata = ToolResultMetadata::try_complete(
        json!({"availability":"available", "generationToken":generation, "projectionDigest":display_digest}),
        json!({"quality":"verified", "originalBarsRetained":true, "displayOnly":true}))
        .map_err(|_| ServiceError::InvalidResult)?;
    let result = TypedToolResult::try_new(
        json!({"data":data,"unavailableReason":Value::Null}),
        count,
        metadata,
        limits,
    )
    .map_err(Into::<ServiceError>::into)?;
    ensure_live(context)?;
    Ok(result)
}

fn product_viewport(request: &TypedToolRequest) -> Result<MarketHistoryViewport, ServiceError> {
    let text = |name: &str| {
        request
            .arguments()
            .get(name)
            .filter(|value| !value.is_null())
            .map(|value| value.as_str().ok_or(ServiceError::InvalidRequest))
            .transpose()
    };
    let nanos = |name: &str| {
        text(name)?
            .map(|value| {
                value
                    .parse::<i64>()
                    .ok()
                    .filter(|parsed| parsed.to_string() == value)
                    .ok_or(ServiceError::InvalidRequest)
            })
            .transpose()
    };
    let date = |name: &str| {
        text(name)?
            .map(|value| {
                if value.len() != 10 {
                    return Err(ServiceError::InvalidRequest);
                }
                let mut parts = value.split('-');
                let year = parts
                    .next()
                    .and_then(|value| value.parse::<u16>().ok())
                    .ok_or(ServiceError::InvalidRequest)?;
                let month = parts
                    .next()
                    .and_then(|value| value.parse::<u8>().ok())
                    .ok_or(ServiceError::InvalidRequest)?;
                let day = parts
                    .next()
                    .and_then(|value| value.parse::<u8>().ok())
                    .ok_or(ServiceError::InvalidRequest)?;
                let date = CalendarDate::new(year, month, day)
                    .map_err(|_| ServiceError::InvalidRequest)?;
                if parts.next().is_some() || date.to_string() != value {
                    return Err(ServiceError::InvalidRequest);
                }
                Ok(date)
            })
            .transpose()
    };
    let start_unix_nanos = nanos("startUnixNanos")?;
    let end_unix_nanos = nanos("endUnixNanos")?;
    let start_date = date("startDate")?;
    let end_date = date("endDate")?;
    let point_limit = request
        .arguments()
        .get("pointLimit")
        .filter(|value| !value.is_null())
        .map(|value| {
            value
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(ServiceError::InvalidRequest)
        })
        .transpose()?
        .unwrap_or(1000);
    let generation_token = text("generationToken")?.map(str::to_owned);
    if !(8..=4096).contains(&point_limit)
        || start_unix_nanos
            .zip(end_unix_nanos)
            .is_some_and(|(start, end)| start > end)
        || start_date
            .zip(end_date)
            .is_some_and(|(start, end)| start > end)
        || ((start_unix_nanos.is_some() || end_unix_nanos.is_some())
            && (start_date.is_some() || end_date.is_some()))
        || generation_token.as_ref().is_some_and(|value| {
            value.len() != 64
                || value
                    .bytes()
                    .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        })
    {
        return Err(ServiceError::InvalidRequest);
    }
    Ok(MarketHistoryViewport {
        start_unix_nanos,
        end_unix_nanos,
        start_date,
        end_date,
        point_limit,
        generation_token,
    })
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
            "requested": interval_value(coverage.requested()),
            "materialized": interval_value(coverage.materialized()),
            "returned": interval_value(coverage.returned()),
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
            match (pair[0].time_semantics(), pair[1].time_semantics()) {
                (
                    BarTimeSemantics::TimestampedPeriod(first),
                    BarTimeSemantics::TimestampedPeriod(second),
                ) => {
                    first.period_start() >= second.period_start()
                        || first.period_end_exclusive() > second.period_start()
                }
                (
                    BarTimeSemantics::NominalDailyDate(first),
                    BarTimeSemantics::NominalDailyDate(second),
                ) => first.date() >= second.date(),
                _ => true,
            }
        })
        || series.bars().iter().any(|bar| {
            bar.time_semantics()
                .timestamped_period()
                .is_some_and(|period| period.period_start() >= period.period_end_exclusive())
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
    let returned_matches = match coverage.returned() {
        MarketHistoryInterval::Timestamped {
            start,
            end_exclusive,
        } => {
            first.period_start() == Some(start)
                && last.period_end_exclusive() == Some(end_exclusive)
                && first.nominal_date().is_none()
                && last.nominal_date().is_none()
        }
        MarketHistoryInterval::NominalDates {
            start,
            end_inclusive,
        } => {
            first.nominal_date() == Some(start)
                && last.nominal_date() == Some(end_inclusive)
                && first.period_start().is_none()
                && last.period_end_exclusive().is_none()
        }
    };
    if !returned_matches {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn bar_value(bar: &MarketHistoryBar) -> Value {
    json!({
        "time": bar_time_value(bar),
        "open": decimal_text(bar.open().amount()),
        "high": decimal_text(bar.high().amount()),
        "low": decimal_text(bar.low().amount()),
        "close": decimal_text(bar.close().amount()),
        "volume": decimal_text(bar.volume()),
        "tradeCount": bar.trade_count(),
        "vwap": bar.vwap().map(|value| decimal_text(value.amount())),
    })
}

fn bar_time_value(bar: &MarketHistoryBar) -> Value {
    match bar.time_semantics() {
        BarTimeSemantics::TimestampedPeriod(period) => json!({
            "precision": "timestamped_period", "startsAt": timestamp_value(period.period_start()),
            "endsAt": timestamp_value(period.period_end_exclusive()),
        }),
        BarTimeSemantics::NominalDailyDate(date) => json!({
            "precision": "nominal_date", "date": date.date().to_string(),
        }),
    }
}

fn interval_value(interval: MarketHistoryInterval) -> Value {
    match interval {
        MarketHistoryInterval::Timestamped {
            start,
            end_exclusive,
        } => json!({
            "precision": "timestamped_period", "startsAt": timestamp_value(start),
            "endsAt": timestamp_value(end_exclusive),
        }),
        MarketHistoryInterval::NominalDates {
            start,
            end_inclusive,
        } => json!({
            "precision": "nominal_dates", "startDate": start.to_string(),
            "endDateInclusive": end_inclusive.to_string(),
        }),
    }
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
