//! Immutable Market viewport projections over the canonical verified history cursor.
use super::*;
use crate::{
    ResearchService,
    application::model::forecast::{
        authorize_projection_parents, chart_storage_error, read_chart_display,
        recheck_projection_parents,
    },
};
use market_squawk_data::{ChartProjectionError, ChartProjectionRow};
use market_squawk_modeling::ForecastArtifactManifestRecord;
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

pub(crate) struct MarketHistoryViewport {
    pub(crate) start_unix_nanos: Option<i64>,
    pub(crate) end_unix_nanos: Option<i64>,
    pub(crate) start_date: Option<CalendarDate>,
    pub(crate) end_date: Option<CalendarDate>,
    pub(crate) point_limit: usize,
    pub(crate) generation_token: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Metadata {
    instrument: InstrumentId,
    currency: Currency,
    nominal: bool,
    adjustment: String,
    parents: Vec<ForecastArtifactManifestRecord>,
}
impl MarketHistoryReadCapability {
    pub(crate) async fn read_latest_viewport(
        &self,
        research: &ResearchService,
        request: LatestMarketHistoryReadRequest,
        viewport: MarketHistoryViewport,
        context: &RequestContext,
    ) -> Result<Option<Value>, ServiceError> {
        if !(8..=4096).contains(&viewport.point_limit)
            || viewport
                .start_unix_nanos
                .zip(viewport.end_unix_nanos)
                .is_some_and(|(a, b)| a > b)
            || viewport
                .start_date
                .zip(viewport.end_date)
                .is_some_and(|(a, b)| a > b)
            || (viewport.start_date.is_some() || viewport.end_date.is_some())
                && (viewport.start_unix_nanos.is_some() || viewport.end_unix_nanos.is_some())
        {
            return Err(ServiceError::InvalidRequest);
        }
        if !request.supported_by_current_catalog_policy() {
            return Ok(None);
        }
        let projections = research.chart_projections();
        let adjustment = format!("{:?}", request.adjustment);
        let reference = if let Some(token) = &viewport.generation_token {
            projections
                .reference(
                    parse_digest(token)?,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(chart_storage_error)?
                .ok_or(ServiceError::NotFound)?
        } else {
            let policy = request
                .adjustment
                .selection_policy()
                .ok_or(ServiceError::InvalidRequest)?;
            let lookup = LatestCanonicalMarketBarHistoryWindowRequest::try_new(
                request.instrument_id,
                policy,
                request.knowledge_cutoff,
            )
            .map_err(|_| ServiceError::InvalidRequest)?;
            let Some(selection) = self
                .reader
                .select_latest_canonical_market_bar_history_window(
                    lookup,
                    context.deadline(),
                    context.cancellation(),
                )
                .map_err(read_error)?
            else {
                return Ok(None);
            };
            let manifest = selection
                .exact_request()
                .exact_manifest()
                .ok_or(ServiceError::InvalidResult)?;
            let mut source = Sha256::new();
            source.update(b"market-squawk/market-chart-original-generation/v1\0");
            source.update(
                serde_json::to_vec(&ForecastArtifactManifestRecord::from_manifest(manifest))
                    .map_err(|_| ServiceError::InvalidResult)?,
            );
            source.update(request.instrument_id.to_string().as_bytes());
            source.update(adjustment.as_bytes());
            source.update(
                serde_json::to_vec(&(
                    selection.exact_request().requested_range(),
                    selection.exact_request().requested_dates(),
                ))
                .map_err(|_| ServiceError::InvalidResult)?,
            );
            let source = source.finalize().into();
            if let Some(reference) = projections
                .reference(source, context.deadline(), context.cancellation())
                .map_err(chart_storage_error)?
            {
                reference
            } else {
                let Some(history) = self
                    .reader
                    .read_canonical_market_bar_history_cursor(
                        selection.into_exact_request(),
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(read_error)?
                else {
                    return Ok(None);
                };
                let receipt = history.selection().receipt();
                if !receipt.current_research_eligible()
                    || receipt.instrument_id() != request.instrument_id
                {
                    return Err(ServiceError::InvalidResult);
                };
                let mut parents = vec![history.selection().pinned().manifest().clone()];
                if !parents.contains(history.read_receipt().origin_manifest()) {
                    parents.push(history.read_receipt().origin_manifest().clone());
                }
                let permit = authorize_projection_parents(
                    research,
                    &parents,
                    request.knowledge_cutoff,
                    context,
                )
                .await?;
                let metadata = Metadata {
                    instrument: request.instrument_id,
                    currency: receipt.currency(),
                    nominal: receipt.requested_dates().is_some(),
                    adjustment: adjustment.clone(),
                    parents: parents
                        .iter()
                        .map(ForecastArtifactManifestRecord::from_manifest)
                        .collect(),
                };
                let bytes =
                    serde_json::to_vec(&metadata).map_err(|_| ServiceError::InvalidResult)?;
                let rows=history.bars().map(|bar| {
                    let bar=bar.map_err(|error|match error {AnalyticalReadError::Query(QueryError::Cancelled)=>ChartProjectionError::Cancelled,AnalyticalReadError::Query(QueryError::DeadlineExceeded)=>ChartProjectionError::DeadlineExceeded,_=>ChartProjectionError::Invalid})?;
                    let (key,time)=match bar.time_semantics() {
                        BarTimeSemantics::TimestampedPeriod(period)=>(period.period_start().unix_nanos(),json!({"precision":"timestamped_period","startsAt":timestamp(period.period_start()),"endsAt":timestamp(period.period_end_exclusive())})),
                        BarTimeSemantics::NominalDailyDate(date)=>(date_key(date.date()),json!({"precision":"nominal_date","date":date.date().to_string()})),
                    };
                    Ok(ChartProjectionRow{time_nanos:key,values:vec![Some(bar.low().amount().into()),Some(bar.high().amount().into()),Some(bar.close().amount().into())],
                        point:json!({"time":time,"open":bar.open().amount().normalize().to_string(),"high":bar.high().amount().normalize().to_string(),"low":bar.low().amount().normalize().to_string(),"close":bar.close().amount().normalize().to_string(),"volume":bar.volume().normalize().to_string()})})
                });
                let reference = projections
                    .publish(
                        source,
                        &bytes,
                        3,
                        rows,
                        context.deadline(),
                        context.cancellation(),
                    )
                    .map_err(chart_storage_error)?;
                recheck_projection_parents(research, permit, context).await?;
                reference
            }
        };
        let bytes = projections
            .metadata(&reference, context.deadline(), context.cancellation())
            .map_err(chart_storage_error)?;
        let metadata: Metadata =
            serde_json::from_slice(&bytes).map_err(|_| ServiceError::InvalidResult)?;
        if metadata.instrument != request.instrument_id || metadata.adjustment != adjustment {
            return Err(ServiceError::InvalidRequest);
        };
        if metadata.nominal
            && (viewport.start_unix_nanos.is_some() || viewport.end_unix_nanos.is_some())
            || !metadata.nominal && (viewport.start_date.is_some() || viewport.end_date.is_some())
        {
            return Err(ServiceError::InvalidRequest);
        };
        let parents = metadata
            .parents
            .iter()
            .map(|parent| parent.typed().map_err(|_| ServiceError::InvalidResult))
            .collect::<Result<Vec<_>, _>>()?;
        let permit =
            authorize_projection_parents(research, &parents, request.knowledge_cutoff, context)
                .await?;
        let (start, end) = if metadata.nominal {
            (
                viewport.start_date.map(date_key),
                viewport.end_date.map(date_key),
            )
        } else {
            (viewport.start_unix_nanos, viewport.end_unix_nanos)
        };
        let (bars, mut display) = read_chart_display(
            research,
            &reference,
            start,
            end,
            viewport.point_limit,
            context,
        )?;
        if metadata.nominal {
            display["firstTimeUnixNanos"] = Value::Null;
            display["lastTimeUnixNanos"] = Value::Null;
        }
        recheck_projection_parents(research, permit, context).await?;
        Ok(Some(
            json!({"currency":metadata.currency.as_str(),"bars":bars,"partial":display["reduced"],"display":display,
            "generationToken":hex(reference.source_sha256),
            "viewport":{"startUnixNanos":viewport.start_unix_nanos.map(|v|v.to_string()),"endUnixNanos":viewport.end_unix_nanos.map(|v|v.to_string()),
                "startDate":viewport.start_date.map(|v|v.to_string()),"endDate":viewport.end_date.map(|v|v.to_string()),"pointLimit":viewport.point_limit,
                "fullStartUnixNanos":if metadata.nominal {None}else{reference.first_time.map(|v|v.to_string())},
                "fullEndUnixNanos":if metadata.nominal {None}else{reference.last_time.map(|v|v.to_string())},
                "fullStartDate":if metadata.nominal {reference.first_time.map(date_text)}else{None},
                "fullEndDate":if metadata.nominal {reference.last_time.map(date_text)}else{None}}}),
        ))
    }
}
fn date_key(date: CalendarDate) -> i64 {
    i64::from(date.year()) * 10_000 + i64::from(date.month()) * 100 + i64::from(date.day())
}
fn date_text(key: i64) -> String {
    format!(
        "{:04}-{:02}-{:02}",
        key / 10_000,
        key / 100 % 100,
        key % 100
    )
}
fn timestamp(value: Timestamp) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(value.unix_nanos())
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}
fn hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut result, byte| {
            let _ = write!(result, "{byte:02x}");
            result
        })
}
fn parse_digest(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ServiceError::InvalidRequest);
    };
    let mut result = [0; 32];
    for (slot, pair) in result.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        let nibble = |byte: u8| {
            if byte <= b'9' {
                byte - b'0'
            } else {
                byte - b'a' + 10
            }
        };
        *slot = nibble(pair[0]) * 16 + nibble(pair[1]);
    }
    Ok(result)
}
fn read_error(error: AnalyticalReadError) -> ServiceError {
    match unavailable_reason(&error) {
        MarketHistoryUnavailableReason::Cancelled => ServiceError::Cancelled,
        MarketHistoryUnavailableReason::DeadlineExceeded => ServiceError::DeadlineExceeded,
        MarketHistoryUnavailableReason::CapacityExceeded => ServiceError::ResourceExhausted,
        MarketHistoryUnavailableReason::StorageUnavailable => ServiceError::Unavailable,
        MarketHistoryUnavailableReason::IntegrityUnproven => ServiceError::InvalidResult,
    }
}
