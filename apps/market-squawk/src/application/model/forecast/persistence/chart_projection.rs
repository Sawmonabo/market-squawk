//! Typed forecast history and horizon projections. Original value meaning is never coerced to price.
use super::*;
use market_squawk_data::{
    ChartProjectionCatalogCapability, ChartProjectionError, ChartProjectionReference,
    ChartProjectionRow, ChartProjectionValue,
};
use market_squawk_services::{ArtifactPublicationContext, RequestContext, ServiceError};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::model) struct ForecastChartReferences {
    pub(super) observed: ChartProjectionReference,
    pub(super) estimates: ChartProjectionReference,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::application::model) struct ForecastChartViewport {
    pub(in crate::application::model) forecast_token: String,
    start_unix_nanos: Option<String>,
    end_unix_nanos: Option<String>,
    start_fiscal_ordinal: Option<u32>,
    end_fiscal_ordinal: Option<u32>,
    point_limit: Option<usize>,
}

impl VintageRecord {
    pub(in crate::application::model) fn publish_chart(
        &self,
        catalog: &ChartProjectionCatalogCapability,
        context: &ArtifactPublicationContext,
    ) -> Result<ForecastChartReferences, ForecastApplicationError> {
        if !self.validate() {
            return Err(ForecastApplicationError::InvalidRecord);
        };
        let payload = &self.payload;
        let fiscal = payload
            .points
            .first()
            .is_some_and(|point| point.financial_target.is_some());
        let metadata=serde_json::to_vec(&json!({"forecastToken":self.product_token()?,"target":payload.output_binding.product_value()?,
            "coordinateKind":if fiscal {"fiscal_period"}else{"timestamp"},
            "observedThroughUnixNanos":payload.observed_through_unix_nanos.map(|v|v.to_string()),
            "sourceArtifactSha256":self.controlled_artifact.sha256})).map_err(|_|ForecastApplicationError::InvalidRecord)?;
        let observed = payload.observed_history.iter().map(|point| {
            Ok(ChartProjectionRow {
                time_nanos: point.observed_at_unix_nanos,
                values: vec![Some(metric(&point.mantissa, point.decimal_scale)?)],
                point: point
                    .product_value(&payload.output_binding)
                    .map_err(|_| ChartProjectionError::Invalid)?,
            })
        });
        let observed = catalog
            .publish(
                chart_source(&self.vintage_id, b"observed"),
                &metadata,
                1,
                observed,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        let estimates = payload.points.iter().map(|point| {
            let key = match (point.target_at_unix_nanos, point.financial_target) {
                (Some(at), None) => at,
                (None, Some(period)) => i64::from(period.ordinal),
                _ => return Err(ChartProjectionError::Invalid),
            };
            Ok(ChartProjectionRow {
                time_nanos: key,
                values: vec![Some(metric(&point.central_mantissa, point.decimal_scale)?)],
                point: point
                    .product_value(&payload.output_binding)
                    .map_err(|_| ChartProjectionError::Invalid)?,
            })
        });
        let estimates = catalog
            .publish(
                chart_source(&self.vintage_id, b"estimates"),
                &metadata,
                1,
                estimates,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        Ok(ForecastChartReferences {
            observed,
            estimates,
        })
    }
}
impl StoredVintageRecord {
    pub(in crate::application::model) fn verify_chart(
        &self,
        catalog: &ChartProjectionCatalogCapability,
        context: &market_squawk_services::ArtifactReadContext,
    ) -> Result<(), ForecastApplicationError> {
        self.validate_chart(catalog, context)?;
        catalog
            .verify(
                &self.chart.observed,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        catalog
            .verify(
                &self.chart.estimates,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        context.ensure_live()?;
        Ok(())
    }

    pub(in crate::application::model) fn validate_chart(
        &self,
        catalog: &ChartProjectionCatalogCapability,
        context: &market_squawk_services::ArtifactReadContext,
    ) -> Result<(), ForecastApplicationError> {
        context.ensure_live()?;
        if self.chart.observed.source_sha256 != chart_source(&self.vintage_id, b"observed")
            || self.chart.estimates.source_sha256 != chart_source(&self.vintage_id, b"estimates")
        {
            return Err(ForecastApplicationError::CorruptIndex);
        };
        let observed = catalog
            .metadata(
                &self.chart.observed,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        let estimates = catalog
            .metadata(
                &self.chart.estimates,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(publication_error)?;
        if observed != estimates {
            return Err(ForecastApplicationError::CorruptIndex);
        };
        let metadata: Value = serde_json::from_slice(&observed)
            .map_err(|_| ForecastApplicationError::CorruptIndex)?;
        if metadata.get("sourceArtifactSha256").and_then(Value::as_str)
            != Some(self.controlled_artifact.sha256.as_str())
            || metadata.get("target") != self.summary.get("target")
        {
            return Err(ForecastApplicationError::CorruptIndex);
        };
        context.ensure_live()?;
        Ok(())
    }
    pub(in crate::application::model) fn chart_viewport(
        &self,
        catalog: &ChartProjectionCatalogCapability,
        viewport: &ForecastChartViewport,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        use crate::application::model::forecast::{
            chart_storage_error, read_chart_display_from_catalog,
        };
        self.validate_chart(
            catalog,
            &market_squawk_services::ArtifactReadContext::new(
                context.cancellation().clone(),
                context.deadline(),
            ),
        )
        .map_err(crate::application::model::map_forecast_selection_error)?;
        let token = product_token(b"market-squawk/product-forecast/v1\0", &self.vintage_id)
            .map_err(|_| ServiceError::InvalidResult)?;
        if viewport.forecast_token != token.to_string() {
            return Err(ServiceError::InvalidRequest);
        };
        if self.chart.observed.source_sha256 != chart_source(&self.vintage_id, b"observed")
            || self.chart.estimates.source_sha256 != chart_source(&self.vintage_id, b"estimates")
        {
            return Err(ServiceError::InvalidResult);
        };
        let parse = |value: &str| {
            value
                .parse::<i64>()
                .ok()
                .filter(|v| v.to_string() == value)
                .ok_or(ServiceError::InvalidRequest)
        };
        let start = viewport
            .start_unix_nanos
            .as_deref()
            .map(parse)
            .transpose()?;
        let end = viewport.end_unix_nanos.as_deref().map(parse).transpose()?;
        let limit = viewport.point_limit.unwrap_or(1000);
        if !(8..=4096).contains(&limit)
            || start.zip(end).is_some_and(|(a, b)| a > b)
            || viewport
                .start_fiscal_ordinal
                .zip(viewport.end_fiscal_ordinal)
                .is_some_and(|(a, b)| a > b)
        {
            return Err(ServiceError::InvalidRequest);
        };
        let bytes = catalog
            .metadata(
                &self.chart.estimates,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(chart_storage_error)?;
        let metadata: Value =
            serde_json::from_slice(&bytes).map_err(|_| ServiceError::InvalidResult)?;
        if metadata.get("sourceArtifactSha256").and_then(Value::as_str)
            != Some(self.controlled_artifact.sha256.as_str())
        {
            return Err(ServiceError::InvalidResult);
        };
        let fiscal =
            metadata.get("coordinateKind").and_then(Value::as_str) == Some("fiscal_period");
        if fiscal && (start.is_some() || end.is_some())
            || !fiscal
                && (viewport.start_fiscal_ordinal.is_some()
                    || viewport.end_fiscal_ordinal.is_some())
        {
            return Err(ServiceError::InvalidRequest);
        };
        let (observed, history_display) = read_chart_display_from_catalog(
            catalog,
            &self.chart.observed,
            start,
            end,
            limit,
            context,
        )?;
        let (estimate_start, estimate_end) = if fiscal {
            (
                viewport.start_fiscal_ordinal.map(i64::from),
                viewport.end_fiscal_ordinal.map(i64::from),
            )
        } else {
            (start, end)
        };
        // Horizon points carry their own calibrated intervals and fiscal identities. Keep every
        // original point in the requested range so reducing history cannot flatten a narrow band.
        let mut estimates = Vec::new();
        catalog
            .scan(
                &self.chart.estimates,
                estimate_start.unwrap_or(i64::MIN),
                estimate_end.unwrap_or(i64::MAX),
                context.deadline(),
                context.cancellation(),
                |ordinal, row| {
                    let mut point = row.point;
                    point
                        .as_object_mut()
                        .ok_or(ChartProjectionError::Invalid)?
                        .insert("originalOrdinal".into(), json!(ordinal.to_string()));
                    estimates.push(point);
                    Ok(())
                },
            )
            .map_err(chart_storage_error)?;
        let full_start = self.chart.observed.first_time.or(if fiscal {
            None
        } else {
            self.chart.estimates.first_time
        });
        let full_end = if fiscal {
            self.chart.observed.last_time
        } else {
            self.chart
                .estimates
                .last_time
                .or(self.chart.observed.last_time)
        };
        Ok(
            json!({"forecastToken":token,"target":metadata["target"],"coordinateKind":metadata["coordinateKind"],
            "observedThroughUnixNanos":metadata["observedThroughUnixNanos"],"observedHistory":observed,"estimates":estimates,"display":history_display,
            "viewport":{"startUnixNanos":viewport.start_unix_nanos,"endUnixNanos":viewport.end_unix_nanos,
                "startFiscalOrdinal":viewport.start_fiscal_ordinal,"endFiscalOrdinal":viewport.end_fiscal_ordinal,"pointLimit":limit,
                "fullStartUnixNanos":full_start.map(|v|v.to_string()),"fullEndUnixNanos":full_end.map(|v|v.to_string()),
                "fullStartFiscalOrdinal":if fiscal {self.chart.estimates.first_time}else{None},"fullEndFiscalOrdinal":if fiscal{self.chart.estimates.last_time}else{None}}}),
        )
    }
}
fn metric(mantissa: &str, scale: u8) -> Result<ChartProjectionValue, ChartProjectionError> {
    ChartProjectionValue::try_new(
        mantissa
            .parse()
            .map_err(|_| ChartProjectionError::Invalid)?,
        scale,
    )
}
fn chart_source(vintage: &str, role: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/typed-forecast-chart/v1\0");
    hash.update(vintage.as_bytes());
    hash.update(role);
    hash.finalize().into()
}
fn publication_error(error: ChartProjectionError) -> ForecastApplicationError {
    match error {
        ChartProjectionError::Cancelled => market_squawk_services::ArtifactError::Cancelled.into(),
        ChartProjectionError::DeadlineExceeded => {
            market_squawk_services::ArtifactError::DeadlineExceeded.into()
        }
        ChartProjectionError::Invalid => ForecastApplicationError::InvalidRecord,
        _ => ForecastApplicationError::Unavailable,
    }
}

impl crate::application::model::ModelDomainService {
    pub(in crate::application::model) async fn get_forecast_chart(
        &self,
        request: &market_squawk_services::TypedToolRequest,
        context: &RequestContext,
    ) -> Result<market_squawk_services::TypedToolResult, ServiceError> {
        use crate::application::model::{map_forecast_selection_error, one_result};
        let mut arguments = request.arguments().clone();
        arguments.remove("resultLimits");
        let viewport: ForecastChartViewport = serde_json::from_value(Value::Object(arguments))
            .map_err(|_| ServiceError::InvalidRequest)?;
        let token =
            Uuid::parse_str(&viewport.forecast_token).map_err(|_| ServiceError::InvalidRequest)?;
        if token.to_string() != viewport.forecast_token {
            return Err(ServiceError::InvalidRequest);
        };
        crate::application::domain_support::ensure_request_live(context, &self.lifecycle)?;
        let forecasts = self.forecasts.as_ref().ok_or(ServiceError::Unavailable)?;
        let head = forecasts
            .catalog
            .head()
            .map_err(|_| ServiceError::Unavailable)?;
        let bytes = forecasts
            .catalog
            .get(
                head,
                market_squawk_data::ForecastInventoryLookup::Token(&viewport.forecast_token),
            )
            .map_err(|_| ServiceError::Unavailable)?
            .ok_or(ServiceError::NotFound)?;
        let stored = StoredVintageRecord::decode(&bytes).map_err(map_forecast_selection_error)?;
        // Probability contracts have no observed-history corpus. Preserve their existing exact
        // event-input/outcome revalidation before exposing the retained probability horizon.
        if stored
            .summary
            .get("target")
            .and_then(|value| value.get("valueKind"))
            .and_then(Value::as_str)
            == Some("probability")
        {
            super::super::outcome::validate_event_for_read(self, token, context)
                .await
                .map_err(map_forecast_selection_error)?;
        }
        let result = stored.chart_viewport(&forecasts.charts, &viewport, context)?;
        crate::application::domain_support::ensure_request_live(context, &self.lifecycle)?;
        one_result(result, request, context)
    }
}
