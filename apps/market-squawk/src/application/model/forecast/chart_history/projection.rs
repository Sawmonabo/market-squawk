//! Bounded display reduction over immutable, source-authenticated original observations.

use super::*;
use market_squawk_data::{
    ChartProjectionError, ChartProjectionReference, ChartProjectionRow, Sha256Digest,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub(crate) struct SavedChartProjection {
    pub(crate) frame: SavedForecastChart,
    pub(crate) history: Value,
    pub(crate) forecast: Value,
}

impl SavedForecastChart {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument
    }
    pub(crate) const fn origin_price(&self) -> Money {
        self.origin_price
    }
    pub(crate) const fn origin_at(&self) -> Timestamp {
        self.origin_at
    }
    pub(crate) const fn source_cutoff(&self) -> Timestamp {
        self.source_cutoff
    }
    pub(crate) fn basis_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.basis)
    }
    pub(crate) fn history_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.history)
    }
    pub(crate) fn source_read_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.source_read)
    }
    pub(crate) fn calendar_identity(&self) -> Sha256Digest {
        Sha256Digest::new(self.calendar)
    }
    pub(crate) fn selected_manifest(
        &self,
    ) -> Result<market_squawk_data::DatasetManifestRef, ServiceError> {
        self.selected_manifest
            .typed()
            .map_err(|_| ServiceError::InvalidResult)
    }
    pub(crate) fn origin_manifest(
        &self,
    ) -> Result<market_squawk_data::DatasetManifestRef, ServiceError> {
        self.origin_manifest
            .typed()
            .map_err(|_| ServiceError::InvalidResult)
    }

    pub(crate) async fn read_projection(
        &self,
        research: &crate::ResearchService,
        start: Option<i64>,
        end: Option<i64>,
        point_limit: usize,
        context: &RequestContext,
    ) -> Result<SavedChartProjection, ServiceError> {
        self.read_projection_bound(research, start, end, point_limit, true, context)
            .await
    }
    pub(crate) async fn read_metadata(
        &self,
        research: &crate::ResearchService,
        context: &RequestContext,
    ) -> Result<SavedChartProjection, ServiceError> {
        self.read_projection_bound(research, None, None, 8, false, context)
            .await
    }
    async fn read_projection_bound(
        &self,
        research: &crate::ResearchService,
        start: Option<i64>,
        end: Option<i64>,
        point_limit: usize,
        rows: bool,
        context: &RequestContext,
    ) -> Result<SavedChartProjection, ServiceError> {
        self.validate()?;
        let reference = self
            .projection
            .as_ref()
            .ok_or(ServiceError::InvalidResult)?;
        if reference.source_sha256 != self.projection_source()? {
            return Err(ServiceError::InvalidResult);
        }
        let parents = self
            .parents
            .iter()
            .map(|record| record.typed().map_err(|_| ServiceError::InvalidResult))
            .collect::<Result<Vec<_>, _>>()?;
        let permit =
            authorize_projection_parents(research, &parents, self.source_cutoff, context).await?;
        let (points, display) = if rows {
            read_chart_display(research, reference, start, end, point_limit, context)?
        } else {
            research
                .chart_projections()
                .metadata(reference, context.deadline(), context.cancellation())
                .map_err(storage_error)?;
            (
                Vec::new(),
                json!({"method":"first_last_min_max", "originalPointCount":reference.row_count.to_string(),
                "visibleOriginalPointCount":"0", "returnedPointCount":0,
                "firstTimeUnixNanos":reference.first_time.map(|v|v.to_string()),"lastTimeUnixNanos":reference.last_time.map(|v|v.to_string()),
                "projectionDigest":super::super::persistence::hex(reference.projection_sha256),"reduced":false}),
            )
        };
        check(context)?;
        recheck_projection_parents(research, permit, context).await?;
        Ok(SavedChartProjection {
            frame: self.clone(),
            history: json!({"state":"available", "basis":"split_adjusted_price", "points":points,
                "display":display,
                "summary":"Original daily closing prices in the saved forecast's split-adjusted share units. Empty observations are genuine gaps; dates identify market sessions."}),
            forecast: self.forecast.clone(),
        })
    }
    fn projection_source(&self) -> Result<[u8; 32], ServiceError> {
        let mut original = self.clone();
        original.projection = None;
        Ok(
            Sha256::digest(serde_json::to_vec(&original).map_err(|_| ServiceError::InvalidResult)?)
                .into(),
        )
    }
}

pub(super) fn publish_history(
    record: &mut SavedForecastChart,
    proof: &ForecastBasisHistory,
    research: &crate::ResearchService,
    context: &RequestContext,
) -> Result<(), ServiceError> {
    let source = record.projection_source()?;
    let rows = proof.rows().map(|row| {
        let row = row.map_err(|error| match error {
            DatasetBuildError::Cancelled => ChartProjectionError::Cancelled,
            DatasetBuildError::DeadlineExceeded => ChartProjectionError::DeadlineExceeded,
            _ => ChartProjectionError::Invalid,
        })?;
        let point = json!({
            "coordinate":coordinate(&row),
            "availableAtUnixNanos":row.available_at.map(nanos),
            "value":row.prices.as_ref().map(|prices|prices.close.amount().normalize().to_string()),
            "quality":row.quality.map(quality),
        });
        Ok(ChartProjectionRow {
            time_nanos: row.observed_at.unix_nanos(),
            values: vec![
                row.prices
                    .as_ref()
                    .map(|prices| prices.close.amount().into()),
            ],
            point,
        })
    });
    record.projection = Some(
        research
            .chart_projections()
            .publish(
                source,
                b"{}",
                1,
                rows,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(storage_error)?,
    );
    Ok(())
}

pub(super) fn original_origin(proof: &ForecastBasisHistory) -> Result<Value, ServiceError> {
    let row = proof.last_row().ok_or(ServiceError::InvalidResult)?;
    let close = row
        .prices
        .as_ref()
        .ok_or(ServiceError::InvalidResult)?
        .close;
    let source_quality = row.quality.ok_or(ServiceError::InvalidResult)?;
    if row.observed_at != proof.origin_at() || close != proof.origin_price() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(
        json!({"state":"available", "basis":"split_adjusted_price", "coordinate":coordinate(row),
        "value":close.amount().normalize().to_string(), "quality":quality(source_quality),
        "summary":"Original observed close in the same split-adjusted share units as the saved forecast. Cash dividends are excluded."}),
    )
}

pub(super) fn original_forecast(
    price: &SelectedPriceForecast,
    origin: &Value,
) -> Result<Value, ServiceError> {
    let amount = |value: market_squawk_modeling::ForecastValue| {
        Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
            .map(|value| value.normalize().to_string())
            .map_err(|_| ServiceError::InvalidResult)
    };
    let interval = |value: super::super::SelectedPriceInterval| -> Result<Value, ServiceError> {
        Ok(json!({"lower":amount(value.lower())?, "upper":amount(value.upper())?}))
    };
    let points = price
        .points()
        .iter()
        .map(|point| {
            Ok(json!({"timeUnixNanos":nanos(point.target_at()),
                "central":amount(point.central())?,
                "interval50":point.intervals().map(|v|interval(v.interval_50())).transpose()?,
                "interval80":point.intervals().map(|v|interval(v.interval_80())).transpose()?,
                "interval95":point.intervals().map(|v|interval(v.interval_95())).transpose()?,
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    Ok(
        json!({"state":"available", "basis":"saved_price_projection", "observedThroughUnixNanos":nanos(price.observed_through()),
        "origin":origin,"points":points,
        "summary":"Saved expected prices at the selected horizons with calibrated ranges. No intermediate future path was estimated."}),
    )
}

/// Renews only source-use authority. Immutable financial values are never regenerated on reads.
pub(crate) async fn authorize_projection_parents(
    research: &crate::ResearchService,
    parents: &[market_squawk_data::DatasetManifestRef],
    source_cutoff: Timestamp,
    context: &RequestContext,
) -> Result<std::sync::Arc<market_squawk_data::AuthorizedResearchRead>, ServiceError> {
    use market_squawk_data::{ResearchUse, ResearchUseLimits, ResearchUseRequest};
    check(context)?;
    let duration = context
        .deadline()
        .saturating_duration_since(std::time::Instant::now())
        .min(std::time::Duration::from_secs(
            market_squawk_data::MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS,
        ));
    if duration.is_zero() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let limits = ResearchUseLimits::try_new(
        parents.len(),
        market_squawk_data::MAX_RESEARCH_USE_GRAPH_NODES,
        market_squawk_data::MAX_RESEARCH_USE_EDGES,
        market_squawk_data::MAX_RESEARCH_USE_SOURCES,
        market_squawk_data::MAX_RESEARCH_USE_RETAINED_BYTES,
        duration,
        std::time::Duration::from_secs(market_squawk_data::MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    let request = ResearchUseRequest::try_new(parents.to_vec(), ResearchUse::Display, limits)
        .map_err(|_| ServiceError::InvalidResult)?;
    let authorized = research
        .authorize_research_display(request, context.deadline(), context.cancellation())
        .await
        .map_err(crate::application::research::corporate_actions::map_research_error)?
        .map_err(crate::application::research::map_research_use_error)?;
    check(context)?;
    let now = wall_now()?;
    if authorized.research_use() != ResearchUse::Display
        || now < source_cutoff
        || now >= authorized.expires_at()
        || authorized.graph().roots().len() != parents.len()
        || parents.iter().any(|parent| {
            !authorized.graph().roots().contains(parent)
                || !authorized
                    .graph()
                    .nodes()
                    .iter()
                    .any(|node| node.manifest() == parent)
        })
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(authorized)
}

pub(crate) async fn recheck_projection_parents(
    research: &crate::ResearchService,
    receipt: std::sync::Arc<market_squawk_data::AuthorizedResearchRead>,
    context: &RequestContext,
) -> Result<(), ServiceError> {
    check(context)?;
    research
        .recheck_research_display(receipt, context.deadline(), context.cancellation())
        .await
        .map_err(crate::application::research::corporate_actions::map_research_error)?
        .map_err(crate::application::research::map_research_use_error)?;
    check(context)
}

#[derive(Clone)]
struct DisplayPoint {
    ordinal: u64,
    row: ChartProjectionRow,
    gaps: [u64; 3],
}
#[derive(Default)]
struct Bucket {
    first: Option<DisplayPoint>,
    last: Option<DisplayPoint>,
    minima: [Option<DisplayPoint>; 3],
    maxima: [Option<DisplayPoint>; 3],
}
impl Bucket {
    fn push(&mut self, point: DisplayPoint) {
        self.first.get_or_insert_with(|| point.clone());
        for (series, value) in point.row.values.iter().enumerate() {
            let Some(value) = value else {
                continue;
            };
            if self.minima[series]
                .as_ref()
                .and_then(|p| p.row.values[series])
                .is_none_or(|prior| *value < prior)
            {
                self.minima[series] = Some(point.clone());
            }
            if self.maxima[series]
                .as_ref()
                .and_then(|p| p.row.values[series])
                .is_none_or(|prior| *value > prior)
            {
                self.maxima[series] = Some(point.clone());
            }
        }
        self.last = Some(point);
    }
    fn drain(&mut self, into: &mut BTreeMap<u64, DisplayPoint>) {
        for point in self
            .first
            .take()
            .into_iter()
            .chain(self.last.take())
            .chain(self.minima.iter_mut().filter_map(Option::take))
            .chain(self.maxima.iter_mut().filter_map(Option::take))
        {
            into.insert(point.ordinal, point);
        }
    }
}

/// First/last and exact extrema per display bucket. Gap counters prohibit drawing across omitted
/// gaps. Every selected point carries its original ordinal; zoom to one time returns original data.
pub(crate) fn read_chart_display(
    research: &crate::ResearchService,
    reference: &ChartProjectionReference,
    start: Option<i64>,
    end: Option<i64>,
    point_limit: usize,
    context: &RequestContext,
) -> Result<(Vec<Value>, Value), ServiceError> {
    read_chart_display_from_catalog(
        &research.chart_projections(),
        reference,
        start,
        end,
        point_limit,
        context,
    )
}

pub(crate) fn read_chart_display_from_catalog(
    catalog: &market_squawk_data::ChartProjectionCatalogCapability,
    reference: &ChartProjectionReference,
    start: Option<i64>,
    end: Option<i64>,
    point_limit: usize,
    context: &RequestContext,
) -> Result<(Vec<Value>, Value), ServiceError> {
    if !(8..=4096).contains(&point_limit) {
        return Err(ServiceError::InvalidRequest);
    }
    let start = start.unwrap_or(i64::MIN);
    let end = end.unwrap_or(i64::MAX);
    if start > end {
        return Err(ServiceError::InvalidRequest);
    }
    let first = reference.first_time.unwrap_or(start).max(start);
    let last = reference.last_time.unwrap_or(end).min(end);
    let span = (i128::from(last) - i128::from(first) + 1).max(1);
    let buckets = (point_limit / (2 + reference.series_count * 2)).max(1);
    let mut current = 0_u128;
    let mut bucket = Bucket::default();
    let mut retained = BTreeMap::new();
    let mut gaps = [0_u64; 3];
    let mut originals = Some(Vec::with_capacity(point_limit));
    let count = catalog
        .scan(
            reference,
            start,
            end,
            context.deadline(),
            context.cancellation(),
            |ordinal, row| {
                let index = ((i128::from(row.time_nanos) - i128::from(first)).max(0) as u128)
                    * (buckets as u128)
                    / (span as u128);
                if index != current {
                    bucket.drain(&mut retained);
                    current = index;
                }
                for (series, value) in row.values.iter().enumerate() {
                    if value.is_none() {
                        gaps[series] += 1;
                    }
                }
                let point = DisplayPoint { ordinal, row, gaps };
                if let Some(points) = originals.as_mut() {
                    if points.len() < point_limit {
                        points.push(point.clone());
                    } else {
                        originals = None;
                    }
                }
                bucket.push(point);
                Ok(())
            },
        )
        .map_err(storage_error)?;
    bucket.drain(&mut retained);
    if let Some(originals) = originals {
        retained = originals
            .into_iter()
            .map(|point| (point.ordinal, point))
            .collect();
    }
    let points = display_points(retained, reference.series_count)?;
    let display = json!({"method":"first_last_min_max", "originalPointCount":reference.row_count.to_string(),
        "visibleOriginalPointCount":count.to_string(),"returnedPointCount":points.len(),
        "firstTimeUnixNanos":reference.first_time.map(|v|v.to_string()),"lastTimeUnixNanos":reference.last_time.map(|v|v.to_string()),
        "projectionDigest":super::super::persistence::hex(reference.projection_sha256),
        "reduced":count>points.len() as u64});
    Ok((points, display))
}

fn display_points(
    retained: BTreeMap<u64, DisplayPoint>,
    series_count: usize,
) -> Result<Vec<Value>, ServiceError> {
    let mut previous_gaps = [0_u64; 3];
    let mut previous_missing = [false; 3];
    let mut points = Vec::with_capacity(retained.len());
    for (_, point) in retained {
        let mut value = point.row.point;
        let object = value.as_object_mut().ok_or(ServiceError::InvalidResult)?;
        object.insert("originalOrdinal".into(), json!(point.ordinal.to_string()));
        object.insert(
            "breakBefore".into(),
            json!(
                (0..series_count)
                    .map(|i| point.gaps[i] != previous_gaps[i] || previous_missing[i])
                    .collect::<Vec<_>>()
            ),
        );
        previous_gaps = point.gaps;
        for (series, value) in point.row.values.iter().enumerate() {
            previous_missing[series] = value.is_none();
        }
        points.push(value);
    }
    Ok(points)
}

pub(crate) fn storage_error(error: ChartProjectionError) -> ServiceError {
    match error {
        ChartProjectionError::Cancelled => ServiceError::Cancelled,
        ChartProjectionError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ChartProjectionError::NotFound => ServiceError::NotFound,
        ChartProjectionError::Invalid => ServiceError::InvalidResult,
        ChartProjectionError::Unavailable | ChartProjectionError::Storage(_) => {
            ServiceError::Unavailable
        }
    }
}
pub(crate) fn wall_now() -> Result<Timestamp, ServiceError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|v| i64::try_from(v.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ServiceError::Internal)
}
fn coordinate(row: &market_squawk_data::ForecastBasisHistoryRow) -> Value {
    if row.provider_timestamp.is_some() {
        json!({"kind":"timestamp","timeUnixNanos":nanos(row.observed_at)})
    } else {
        json!({"kind":"session_date","date":row.native_date.to_string(),"sessionCloseUnixNanos":nanos(row.session_close)})
    }
}
fn nanos(value: Timestamp) -> String {
    value.unix_nanos().to_string()
}
pub(crate) fn quality(value: market_squawk_domain::DataQuality) -> &'static str {
    use market_squawk_domain::DataQuality::*;
    match value {
        DirectVerified => "direct_verified",
        DirectUnverified => "direct_unverified",
        OfficialDelayed => "official_delayed",
        Aggregated => "aggregated",
        Indicative => "indicative",
        Modeled => "modeled",
        Estimated => "estimated",
        Stale => "stale",
        Quarantined => "quarantined",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displayed_originals_break_after_retained_and_omitted_gaps() -> Result<(), ServiceError> {
        let point = |ordinal: u64, value: Option<i64>, gap_count: u64| DisplayPoint {
            ordinal,
            row: ChartProjectionRow {
                time_nanos: ordinal as i64,
                values: vec![value.map(|value| Decimal::from(value).into())],
                point: json!({"value":value}),
            },
            gaps: [gap_count, 0, 0],
        };
        let retained = BTreeMap::from([
            (0, point(0, Some(10), 0)),
            (1, point(1, None, 1)),
            (2, point(2, Some(20), 1)),
        ]);
        let points = display_points(retained, 1)?;
        assert_eq!(points[2]["breakBefore"], json!([true]));
        assert_eq!(points[2]["originalOrdinal"], "2");
        let reduced = BTreeMap::from([(0, point(0, Some(10), 0)), (2, point(2, Some(20), 1))]);
        assert_eq!(display_points(reduced, 1)?[1]["breakBefore"], json!([true]));
        Ok(())
    }
}
